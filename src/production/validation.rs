//! Coordinate reference loading, replay observations and final reporting.
//! Market rules and field decoding live in focused child modules; books are read-only here.
use std::collections::{BTreeMap, HashMap};

mod candidate;
use candidate::{CandidateCache, CandidateContext, CandidateCounters, WindowConfig};
mod close_range;
pub use close_range::ClosePriceBandAudit;
use close_range::DayLimits;
mod columns;
mod loader;
use loader::{LoadedReferences, load_references};
mod phases;
pub use phases::{ReferenceCoverage, SelectionAudit, SelectionSample, SkipReason};
mod precision;
pub use precision::{TurnoverPrecisionAudit, UpperLimitNormalizationAudit};
mod reference;
use reference::ReferenceBookView;
mod sz_close_price;
use sz_close_price::SzClosePriceTracker;
#[cfg(feature = "profiling")]
pub(crate) mod profiling;
mod report;
use super::replay::{
    ObservationPoint, ReplayReport, StateObserver, run_market_day, run_market_day_before,
};
use super::types::{is_chinext_symbol, is_etf_symbol};
use super::{ProductionError, SymbolMap, ValidationAnchor, ValidationConfig};
use crate::{Market, OrderBook, Side};
pub use report::{
    CoverageCounts, CoverageSummary, FieldDifference, RunOutcome, ValidationCounts,
    ValidationOutcome, ValidationRecord, ValidationReport,
};

#[derive(Clone, Debug)]
struct ReferenceSnapshot {
    time_ns: i64,
    view: Option<ReferenceBookView>,
    load_error: Option<String>,
    source_row_no: u64,
    missing_reception: bool,
    pre_close_price_units: Option<i64>,
    state: AnchorState,
}

#[derive(Clone, Debug, Default)]
struct SymbolReferences {
    pre_open: Option<ReferenceSnapshot>,
    continuous_trading: Vec<ReferenceSnapshot>,
    market_close: Option<ReferenceSnapshot>,
}

#[cfg(test)]
impl SymbolReferences {
    fn get_single(&self, anchor: ValidationAnchor) -> Option<&ReferenceSnapshot> {
        match anchor {
            ValidationAnchor::PreOpen => self.pre_open.as_ref(),
            ValidationAnchor::ContinuousTrading => None,
            ValidationAnchor::MarketClose => self.market_close.as_ref(),
        }
    }

    fn get(&self, anchor: ValidationAnchor, time_ns: i64) -> Option<&ReferenceSnapshot> {
        match anchor {
            ValidationAnchor::ContinuousTrading => self
                .continuous_trading
                .binary_search_by_key(&time_ns, |reference| reference.time_ns)
                .ok()
                .map(|index| &self.continuous_trading[index]),
            ValidationAnchor::PreOpen | ValidationAnchor::MarketClose => self
                .get_single(anchor)
                .filter(|reference| reference.time_ns == time_ns),
        }
    }

    fn get_mut(
        &mut self,
        anchor: ValidationAnchor,
        time_ns: i64,
    ) -> Option<&mut ReferenceSnapshot> {
        match anchor {
            ValidationAnchor::ContinuousTrading => self
                .continuous_trading
                .binary_search_by_key(&time_ns, |reference| reference.time_ns)
                .ok()
                .map(|index| &mut self.continuous_trading[index]),
            ValidationAnchor::PreOpen => self
                .pre_open
                .as_mut()
                .filter(|reference| reference.time_ns == time_ns),
            ValidationAnchor::MarketClose => self
                .market_close
                .as_mut()
                .filter(|reference| reference.time_ns == time_ns),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct AnchorState {
    matched: bool,
    finalized: bool,
    matched_candidate_time_ns: Option<i64>,
    matched_candidate_raw_sequence: Option<u64>,
    matched_candidate_apply_sequence: Option<u64>,
    best_candidate_time_ns: Option<i64>,
    best_candidate_raw_sequence: Option<u64>,
    match_tag: Option<&'static str>,
    diagnostics: Option<Box<AnchorDiagnostics>>,
}

#[derive(Clone, Debug, Default)]
struct AnchorDiagnostics {
    comparison_error: Option<String>,
    close_price_band: Option<ClosePriceBandAudit>,
    best_differences: Option<Vec<FieldDifference>>,
    turnover_precision: Option<TurnoverPrecisionAudit>,
}

impl AnchorState {
    fn best_differences(&self) -> Option<&Vec<FieldDifference>> {
        self.diagnostics
            .as_ref()
            .and_then(|d| d.best_differences.as_ref())
    }

    fn diagnostics_mut(&mut self) -> &mut AnchorDiagnostics {
        self.diagnostics.get_or_insert_with(Default::default)
    }
}

#[derive(Clone, Debug, Default)]
struct SymbolValidationState {
    rules: Option<SymbolValidationRules>,
    limits: Option<DayLimits>,
    cache: CandidateCache,
    references: SymbolReferences,
    continuous_position: usize,
    seen: bool,
    close_price: Option<SzClosePriceTracker>,
    channel: Option<u32>,
}

/// Configuration-only values, initialized on first observation after overrides.
/// This cache never depends on the evolving order book or reference frames.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SymbolValidationRules {
    is_etf: bool,
    price_quantum: i64,
    lookahead_ns: i64,
}

impl SymbolValidationRules {
    fn new(market: Market, symbol: &str, lookahead_ns: i64, lookahead_override: bool) -> Self {
        let is_etf = is_etf_symbol(market, symbol);
        Self {
            is_etf,
            price_quantum: validation_price_quantum(market, symbol),
            lookahead_ns: if !lookahead_override && market == Market::Szse {
                if is_etf {
                    1_100 * REFERENCE_MILLISECOND_NS
                } else if is_chinext_symbol(symbol) {
                    3 * REFERENCE_SECOND_NS
                } else {
                    lookahead_ns
                }
            } else {
                lookahead_ns
            },
        }
    }
}

impl From<SymbolReferences> for SymbolValidationState {
    fn from(references: SymbolReferences) -> Self {
        Self {
            references,
            ..Self::default()
        }
    }
}

const REFERENCE_MILLISECOND_NS: i64 = 1_000_000;
const REFERENCE_SECOND_NS: i64 = 1_000_000_000;

const fn default_continuous_lookahead_ms() -> i64 {
    1_000
}
struct ValidationObserver {
    counters: CandidateCounters,
    market: Market,
    symbols: SymbolMap<SymbolValidationState>,
    pre_open_only: bool,
    continuous_lookback_ns: i64,
    continuous_lookahead_ns: i64,
    lookahead_override: bool,
    diagnostic_window_override: bool,
    selection_audit: BTreeMap<String, SelectionAudit>,
    max_detail_records: Option<usize>,
}

impl ValidationObserver {
    fn new(market: Market, references: HashMap<String, SymbolReferences>) -> Self {
        let continuous_lookback_ns = if market == Market::Sse {
            REFERENCE_SECOND_NS
        } else {
            0
        };
        Self {
            counters: CandidateCounters::default(),
            market,
            symbols: references
                .into_iter()
                .map(|(symbol, references)| (symbol, references.into()))
                .collect(),
            pre_open_only: false,
            continuous_lookback_ns,
            continuous_lookahead_ns: REFERENCE_SECOND_NS,
            lookahead_override: false,
            diagnostic_window_override: false,
            selection_audit: BTreeMap::new(),
            max_detail_records: None,
        }
    }

    fn symbol_state_mut(&mut self, symbol: &str) -> &mut SymbolValidationState {
        self.symbols.entry_ref(symbol).or_default()
    }

    fn set_close_limits(&mut self, limits: HashMap<String, DayLimits>) {
        for (symbol, limits) in limits {
            self.symbol_state_mut(&symbol).limits = Some(limits);
        }
    }

    fn observe_trade_inner(
        &mut self,
        symbol: &str,
        sequence: Option<u64>,
        quote_time_ns: i64,
        price_units: i64,
        quantity: u64,
    ) -> Result<(), ProductionError> {
        if self.market == Market::Szse && !self.pre_open_only {
            let tracker = self
                .symbol_state_mut(symbol)
                .close_price
                .get_or_insert_with(SzClosePriceTracker::default);
            tracker.observe(quote_time_ns, price_units, quantity)?;
            if let Some(base) = tracker.range_base.as_mut() {
                if base.raw_sequence.is_none() {
                    base.raw_sequence = sequence;
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn anchor_state(
        &self,
        symbol: &str,
        anchor: ValidationAnchor,
        time_ns: i64,
    ) -> Option<&AnchorState> {
        self.symbols
            .get(symbol)
            .and_then(|state| state.references.get(anchor, time_ns))
            .map(|reference| &reference.state)
    }

    fn with_continuous_lookback(
        mut self,
        lookback: Option<std::time::Duration>,
    ) -> Result<Self, ProductionError> {
        if let Some(duration) = lookback {
            self.diagnostic_window_override = true;
            self.continuous_lookback_ns = i64::try_from(duration.as_nanos())
                .map_err(|_| ProductionError::Arithmetic("continuous lookback"))?;
        }
        if self.continuous_lookback_ns > REFERENCE_SECOND_NS {
            return Err(ProductionError::InvalidRequest(
                "continuous lookback cannot exceed 1s".to_owned(),
            ));
        }
        Ok(self)
    }

    fn with_continuous_lookahead(
        mut self,
        lookahead: Option<std::time::Duration>,
    ) -> Result<Self, ProductionError> {
        if let Some(duration) = lookahead {
            self.lookahead_override = true;
            self.diagnostic_window_override = true;
            self.continuous_lookahead_ns = i64::try_from(duration.as_nanos())
                .map_err(|_| ProductionError::Arithmetic("continuous lookahead"))?;
        }
        if self.continuous_lookahead_ns < REFERENCE_MILLISECOND_NS
            || self.continuous_lookahead_ns > 3 * REFERENCE_SECOND_NS
        {
            return Err(ProductionError::InvalidRequest(
                "continuous lookahead must be within [1ms, 3s]".to_owned(),
            ));
        }
        for state in self.symbols.values_mut() {
            state.rules = None;
        }
        Ok(self)
    }

    const fn with_max_detail_records(mut self, limit: Option<usize>) -> Self {
        self.max_detail_records = limit;
        self
    }

    fn lookahead_ns(&self, symbol: &str) -> i64 {
        self.rules_for(symbol).lookahead_ns
    }

    fn rules_for(&self, symbol: &str) -> SymbolValidationRules {
        SymbolValidationRules::new(
            self.market,
            symbol,
            self.continuous_lookahead_ns,
            self.lookahead_override,
        )
    }

    fn pre_open_only(market: Market, references: HashMap<String, SymbolReferences>) -> Self {
        Self {
            pre_open_only: true,
            ..Self::new(market, references)
        }
    }

    #[cfg(test)]
    fn compare_candidate(
        &mut self,
        symbol: &str,
        book: &OrderBook,
        anchor: ValidationAnchor,
        reference_time_ns: i64,
        candidate_time_ns: Option<i64>,
    ) -> Result<(), ProductionError> {
        let rules = self.rules_for(symbol);
        let Some(state) = self.symbols.get_mut(symbol) else {
            return Ok(());
        };
        let Some(reference) = state.references.get_mut(anchor, reference_time_ns) else {
            return Ok(());
        };
        let ctx = CandidateContext {
            market: self.market,
            rules,
            symbol,
            book,
            limits: state.limits.as_ref(),
            close_price: state.close_price.as_ref(),
        };
        candidate::compare_candidate(
            ctx,
            reference,
            &mut state.cache,
            &mut self.counters,
            anchor,
            candidate_time_ns,
        )
    }

    fn into_report(self, replay: ReplayReport, retain_matched_records: bool) -> ValidationReport {
        report::build_report(self, replay, retain_matched_records)
    }
}

impl StateObserver for ValidationObserver {
    fn observe_trade_with_sequence(
        &mut self,
        _channel: u32,
        symbol: &str,
        raw_sequence: u64,
        quote_time_ns: i64,
        price_units: i64,
        quantity: u64,
    ) -> Result<(), ProductionError> {
        self.observe_trade_inner(
            symbol,
            Some(raw_sequence),
            quote_time_ns,
            price_units,
            quantity,
        )
    }
    fn observe_trade(
        &mut self,
        _channel: u32,
        symbol: &str,
        quote_time_ns: i64,
        price_units: i64,
        quantity: u64,
    ) -> Result<(), ProductionError> {
        self.observe_trade_inner(symbol, None, quote_time_ns, price_units, quantity)
    }

    fn observe(
        &mut self,
        channel: u32,
        symbol: &str,
        book: &OrderBook,
        point: ObservationPoint,
    ) -> Result<(), ProductionError> {
        let state = self.symbols.entry_ref(symbol).or_default();
        let rules = *state.rules.get_or_insert_with(|| {
            SymbolValidationRules::new(
                self.market,
                symbol,
                self.continuous_lookahead_ns,
                self.lookahead_override,
            )
        });
        let windows = WindowConfig {
            pre_open_only: self.pre_open_only,
            lookback_ns: self.continuous_lookback_ns,
            lookahead_ns: rules.lookahead_ns,
        };
        state.seen = true;
        state.channel = Some(channel);
        state.observe_candidates(
            CandidateContext {
                market: self.market,
                rules,
                symbol,
                book,
                limits: None, // Borrowed from the same symbol state inside observe_candidates.
                close_price: None,
            },
            point,
            windows,
            &mut self.counters,
        )
    }
}

pub fn validate_market_day(config: &ValidationConfig) -> Result<ValidationReport, ProductionError> {
    let references = load_references(&config.request, false)?;
    let mut observer = full_day_observer(config, references)?;
    let mut request = config.request.clone();
    request.snapshots = None;
    let replay = run_market_day(&request, &mut observer)?;
    Ok(observer.into_report(replay, config.retain_matched_records))
}

fn full_day_observer(
    config: &ValidationConfig,
    references: LoadedReferences,
) -> Result<ValidationObserver, ProductionError> {
    let mut observer = ValidationObserver::new(config.request.market, references.books)
        .with_continuous_lookback(config.continuous_lookback)?
        .with_continuous_lookahead(config.continuous_lookahead)?
        .with_max_detail_records(config.max_detail_records);
    observer.selection_audit = references.selection_audit;
    observer.set_close_limits(references.sz_close_limits);
    Ok(observer)
}

/// Validates only the post-opening-auction (`PreOpen`) state and stops replay
/// after the last selected opening candidate window. The raw Parquet files are scanned once so validation keeps
/// the same source coverage and schema checks as a full-day run.
pub fn validate_pre_open_market_day(
    config: &ValidationConfig,
) -> Result<ValidationReport, ProductionError> {
    let references = load_references(&config.request, true)?;
    let cutoff = references
        .books
        .values()
        .filter_map(|r| r.pre_open.as_ref())
        .map(|r| {
            r.time_ns
                .checked_add(REFERENCE_SECOND_NS)
                .ok_or(ProductionError::Arithmetic("opening cutoff"))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(super::parse_market_timestamp(
            config.request.trading_day,
            "09:30:00.000",
        )?);
    let mut observer = ValidationObserver::pre_open_only(config.request.market, references.books)
        .with_continuous_lookback(config.continuous_lookback)?
        .with_continuous_lookahead(config.continuous_lookahead)?
        .with_max_detail_records(config.max_detail_records);
    observer.selection_audit = references.selection_audit;
    let mut request = config.request.clone();
    request.snapshots = None;
    let replay = run_market_day_before(&request, cutoff, &mut observer)?;
    Ok(observer.into_report(replay, config.retain_matched_records))
}

const fn timestamp_ns_to_ms(timestamp_ns: i64) -> i64 {
    timestamp_ns.div_euclid(REFERENCE_MILLISECOND_NS)
}

fn validation_price(market: Market, symbol: &str, value: Option<i64>) -> Option<i64> {
    quantize_validation_price(validation_price_quantum(market, symbol), value)
}

fn quantize_validation_price(quantum: i64, value: Option<i64>) -> Option<i64> {
    value.map(|units| (units + quantum / 2) / quantum * quantum)
}

fn validation_price_quantum(market: Market, symbol: &str) -> i64 {
    match market {
        Market::Sse => 10,
        Market::Szse if is_etf_symbol(market, symbol) => 10,
        Market::Szse => 100,
    }
}

#[cfg(test)]
fn published_weighted_price(
    book: &OrderBook,
    side: Side,
    market: Market,
    symbol: &str,
) -> Result<Option<i64>, ProductionError> {
    published_weighted_price_with_quantum(book, side, validation_price_quantum(market, symbol))
}

fn published_weighted_price_with_quantum(
    book: &OrderBook,
    side: Side,
    price_quantum: i64,
) -> Result<Option<i64>, ProductionError> {
    let (total, weighted) = book.visible_aggregate(side);
    if total == 0 {
        return Ok(None);
    }
    let total = u128::from(total);
    let quantum = u128::try_from(price_quantum)
        .map_err(|_| ProductionError::Arithmetic("published weighted quantum"))?;
    let rounded_units = round_weighted_to_quantum(weighted, total, quantum)?;
    i64::try_from(rounded_units)
        .map(Some)
        .map_err(|_| ProductionError::Arithmetic("published weighted conversion"))
}

fn round_weighted_to_quantum(
    weighted: u128,
    total: u128,
    quantum: u128,
) -> Result<u128, ProductionError> {
    let divisor = total
        .checked_mul(quantum)
        .ok_or(ProductionError::Arithmetic("published weighted divisor"))?;
    let rounded_quanta = weighted
        .checked_add(divisor / 2)
        .ok_or(ProductionError::Arithmetic("published weighted rounding"))?
        / divisor;
    rounded_quanta
        .checked_mul(quantum)
        .ok_or(ProductionError::Arithmetic("published weighted units"))
}

const fn anchor_text(anchor: ValidationAnchor) -> &'static str {
    match anchor {
        ValidationAnchor::PreOpen => "pre_open",
        ValidationAnchor::ContinuousTrading => "continuous_trading",
        ValidationAnchor::MarketClose => "market_close",
    }
}

#[cfg(test)]
mod tests;
