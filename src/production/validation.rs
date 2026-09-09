use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{
    Array, Decimal128Array, Int64Array, LargeStringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

mod close_range;
mod columns;
use columns::RawSnapshotColumns;
mod phases;
mod reference;
pub use close_range::ClosePriceBandAudit;
use close_range::{DayLimits, RangeBase};
use reference::ReferenceBookView;
#[cfg(feature = "profiling")]
pub(crate) mod profiling;
use phases::PhaseTracker;
pub use phases::{PhaseAudit, PhaseIssue};

use crate::{Market, OrderBook, QuoteTimestampNs, Side};

use super::replay::{
    ObservationPoint, ReplayReport, StateObserver, run_market_day, run_market_day_before,
};
use super::types::{is_chinext_symbol, is_etf_symbol, is_supported_symbol};
use super::{
    MarketDayRequest, ProductionError, SnapshotBookView, SnapshotLevel, SnapshotLevels,
    ValidationAnchor, ValidationConfig,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldDifference {
    pub field: String,
    pub expected: String,
    pub actual: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationOutcome {
    Matched,
    Mismatched,
    ExcludedByStatus,
    DataError,
    MissingSource,
    /// Kept for reading historical reports; new reports use explicit categories.
    NotComparable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ValidationRecord {
    /// Validation-only projection context; not a waiver or a matching tag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_price_band: Option<ClosePriceBandAudit>,
    pub market: String,
    pub symbol: String,
    pub anchor: ValidationAnchor,
    pub outcome: ValidationOutcome,
    pub reference_time_ms: Option<i64>,
    #[serde(default)]
    pub matched_candidate_time_ms: Option<i64>,
    #[serde(default)]
    pub matched_candidate_raw_sequence: Option<u64>,
    #[serde(default)]
    pub matched_candidate_apply_sequence: Option<u64>,
    #[serde(default)]
    pub channel_id: Option<u32>,
    #[serde(default)]
    pub best_candidate_time_ms: Option<i64>,
    #[serde(default)]
    pub best_candidate_raw_sequence: Option<u64>,
    /// Explains a rule-based semantic normalization used for a successful match.
    #[serde(default)]
    pub match_tag: Option<String>,
    pub reason: Option<String>,
    pub differences: Vec<FieldDifference>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationReport {
    pub replay: ReplayReport,
    /// Milliseconds inspected before each continuous reference timestamp.
    #[serde(default)]
    pub continuous_lookback_ms: i64,
    /// Base horizon in milliseconds; the effective per-symbol horizons are in
    /// `continuous_lookahead_ms_by_symbol` (SZ ETFs and ChiNext differ by default).
    #[serde(default = "default_continuous_lookahead_ms")]
    pub continuous_lookahead_ms: i64,
    /// Explicit window overrides are diagnostics, never standard-rule acceptance.
    #[serde(default)]
    pub diagnostic_window_override: bool,
    #[serde(default)]
    pub continuous_lookahead_ms_by_symbol: BTreeMap<String, i64>,
    #[serde(default)]
    pub phase_audit: Vec<PhaseAudit>,
    pub total_anchors: u64,
    pub comparable_anchors: u64,
    pub matched: u64,
    pub mismatched: u64,
    pub not_comparable: u64,
    #[serde(default)]
    pub excluded_by_status: u64,
    #[serde(default)]
    pub data_errors: u64,
    #[serde(default)]
    pub missing_source: u64,
    pub match_rate: Option<f64>,
    pub not_comparable_rate: Option<f64>,
    pub mismatch_fields: BTreeMap<String, u64>,
    pub mismatch_reasons: BTreeMap<String, u64>,
    pub not_comparable_reasons: BTreeMap<String, u64>,
    /// Number of distinct symbols with at least one mismatched frame.
    #[serde(default)]
    pub mismatched_symbols: u64,
    /// Distinct mismatched symbols grouped by their three-digit prefix.
    #[serde(default)]
    pub mismatch_symbol_prefixes: BTreeMap<String, u64>,
    /// Counts successful rule-based semantic matches, keyed by a stable tag.
    #[serde(default)]
    pub match_tags: BTreeMap<String, u64>,
    /// Counts for every evaluated frame, keyed by `<stock|etf>.<anchor>`.
    #[serde(default)]
    pub breakdown: BTreeMap<String, ValidationCounts>,
    /// Successful records excluded from `records` in compact-report mode.
    #[serde(default)]
    pub omitted_matched_records: u64,
    #[serde(default)]
    pub omitted_mismatched_records: u64,
    #[serde(default)]
    pub omitted_not_comparable_records: u64,
    pub records: Vec<ValidationRecord>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ValidationCounts {
    pub total: u64,
    pub comparable: u64,
    pub matched: u64,
    pub mismatched: u64,
    pub not_comparable: u64,
    #[serde(default)]
    pub excluded_by_status: u64,
    #[serde(default)]
    pub data_errors: u64,
    #[serde(default)]
    pub missing_source: u64,
}

impl ValidationCounts {
    fn observe(&mut self, outcome: &ValidationOutcome) {
        self.total += 1;
        match outcome {
            ValidationOutcome::Matched => {
                self.comparable += 1;
                self.matched += 1;
            }
            ValidationOutcome::Mismatched => {
                self.comparable += 1;
                self.mismatched += 1;
            }
            ValidationOutcome::NotComparable => self.not_comparable += 1,
            ValidationOutcome::ExcludedByStatus => {
                self.not_comparable += 1;
                self.excluded_by_status += 1;
            }
            ValidationOutcome::DataError => {
                self.not_comparable += 1;
                self.data_errors += 1;
            }
            ValidationOutcome::MissingSource => {
                self.not_comparable += 1;
                self.missing_source += 1;
            }
        }
    }
}

impl ValidationReport {
    #[must_use]
    pub const fn is_success(&self) -> bool {
        self.mismatched == 0
            && self.data_errors == 0
            && self.missing_source == 0
            && self.total_anchors > 0
            && self.replay.sz_after_close_events == 0
    }

    /// A diagnostic override may have no mismatches but is not standard acceptance.
    #[must_use]
    pub const fn is_standard_acceptance(&self) -> bool {
        self.is_success()
            && self.replay.sz_pending_resolution_version == 1
            && !self.diagnostic_window_override
            && matches!(
                self.replay.sz_market_order_policy,
                super::SzMarketOrderPolicy::RequireEvidence
            )
    }
}

#[derive(Clone, Debug)]
struct ReferenceSnapshot {
    time_ns: i64,
    view: Option<ReferenceBookView>,
    load_error: Option<String>,
    not_comparable_reason: Option<String>,
    pre_close_price_units: Option<i64>,
    state: AnchorState,
}

#[derive(Clone, Debug, Default)]
struct SymbolReferences {
    pre_open: Option<ReferenceSnapshot>,
    continuous_trading: Vec<ReferenceSnapshot>,
    market_close: Option<ReferenceSnapshot>,
}

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
    references: SymbolReferences,
    continuous_position: usize,
    seen: bool,
    close_price: Option<SzClosePriceTracker>,
    channel: Option<u32>,
}

impl From<SymbolReferences> for SymbolValidationState {
    fn from(references: SymbolReferences) -> Self {
        Self {
            references,
            ..Self::default()
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct TradeSample {
    quote_time_ns: i64,
    price_units: u128,
    quantity: u128,
}

#[derive(Clone, Debug, Default)]
struct SzClosePriceTracker {
    range_base: Option<RangeBase>,
    last_minute: VecDeque<TradeSample>,
    weighted_price_quantity: u128,
    quantity: u128,
    has_closing_auction_trade: bool,
}

impl SzClosePriceTracker {
    fn observe(
        &mut self,
        quote_time_ns: i64,
        price_units: i64,
        quantity: u64,
    ) -> Result<(), ProductionError> {
        let price_units = u128::try_from(price_units)
            .map_err(|_| ProductionError::Arithmetic("SZ close trade price"))?;
        let quantity = u128::from(quantity);
        let weighted = price_units
            .checked_mul(quantity)
            .ok_or(ProductionError::Arithmetic("SZ close trade value"))?;
        if self
            .last_minute
            .back()
            .is_some_and(|sample| quote_time_ns < sample.quote_time_ns)
        {
            return Err(ProductionError::Validation(
                "SZ per-symbol trade time regressed while calculating close price".to_owned(),
            ));
        }
        self.last_minute.push_back(TradeSample {
            quote_time_ns,
            price_units,
            quantity,
        });
        self.weighted_price_quantity = self
            .weighted_price_quantity
            .checked_add(weighted)
            .ok_or(ProductionError::Arithmetic("SZ close weighted value"))?;
        self.quantity = self
            .quantity
            .checked_add(quantity)
            .ok_or(ProductionError::Arithmetic("SZ close quantity"))?;

        let cutoff = quote_time_ns
            .checked_sub(60_000_000_000)
            .ok_or(ProductionError::Arithmetic("SZ close one-minute cutoff"))?;
        while self
            .last_minute
            .front()
            .is_some_and(|sample| sample.quote_time_ns < cutoff)
        {
            let sample = self.last_minute.pop_front().ok_or_else(|| {
                ProductionError::Validation("missing SZ close trade sample".to_owned())
            })?;
            self.weighted_price_quantity = self
                .weighted_price_quantity
                .checked_sub(
                    sample
                        .price_units
                        .checked_mul(sample.quantity)
                        .ok_or(ProductionError::Arithmetic("SZ close evicted trade value"))?,
                )
                .ok_or(ProductionError::Arithmetic("SZ close weighted subtraction"))?;
            self.quantity = self
                .quantity
                .checked_sub(sample.quantity)
                .ok_or(ProductionError::Arithmetic("SZ close quantity subtraction"))?;
        }

        let time = super::time::time_of_day_nanos(QuoteTimestampNs::from_nanos(quote_time_ns));
        const CLOSE_CALL_START: i64 = 14 * 3_600_000_000_000 + 57 * 60_000_000_000;
        const MARKET_CLOSE: i64 = 15 * 3_600_000_000_000;
        if time < CLOSE_CALL_START {
            self.range_base = Some(RangeBase {
                price: i64::try_from(price_units)
                    .map_err(|_| ProductionError::Arithmetic("SZ range base"))?,
                time_ms: timestamp_ns_to_ms(quote_time_ns),
                raw_sequence: None,
            });
        }
        if (CLOSE_CALL_START..=MARKET_CLOSE).contains(&time) {
            self.has_closing_auction_trade = true;
        }
        Ok(())
    }

    fn average_close_price(&self, quantum: i64) -> Result<Option<i64>, ProductionError> {
        if self.quantity == 0 {
            return Ok(None);
        }
        let quantum = u128::try_from(quantum)
            .map_err(|_| ProductionError::Arithmetic("SZ close price quantum"))?;
        let units =
            round_weighted_to_quantum(self.weighted_price_quantity, self.quantity, quantum)?;
        i64::try_from(units)
            .map(Some)
            .map_err(|_| ProductionError::Arithmetic("SZ close price conversion"))
    }
}

const REFERENCE_MILLISECOND_NS: i64 = 1_000_000;
const REFERENCE_SECOND_NS: i64 = 1_000_000_000;

const fn default_continuous_lookahead_ms() -> i64 {
    1_000
}
const SZ_STOCK_AVG_CLOSE_PRICE_TAG: &str = "SZ_STOCK_AVG_CLOSE_PRICE";
const SZ_ETF_AVG_CLOSE_PRICE_TAG: &str = "SZ_ETF_AVG_CLOSE_PRICE";
const SZ_STOCK_PRE_CLOSE_PRICE_TAG: &str = "SZ_STOCK_PRE_CLOSE_PRICE";
const SZ_ETF_PRE_CLOSE_PRICE_TAG: &str = "SZ_ETF_PRE_CLOSE_PRICE";

struct ValidationObserver {
    #[cfg(feature = "profiling")]
    scalar_rejected_candidates: u64,
    #[cfg(feature = "profiling")]
    depth_materializations: u64,
    sz_close_limits: HashMap<String, DayLimits>,
    market: Market,
    symbols: HashMap<String, SymbolValidationState>,
    pre_open_only: bool,
    continuous_lookback_ns: i64,
    continuous_lookahead_ns: i64,
    lookahead_override: bool,
    diagnostic_window_override: bool,
    phase_audit: BTreeMap<String, PhaseAudit>,
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
            sz_close_limits: HashMap::new(),
            #[cfg(feature = "profiling")]
            scalar_rejected_candidates: 0,
            #[cfg(feature = "profiling")]
            depth_materializations: 0,
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
            phase_audit: BTreeMap::new(),
            max_detail_records: None,
        }
    }

    #[allow(clippy::map_entry)] // Avoid allocating a String on the hot-path lookup.
    fn symbol_state_mut(&mut self, symbol: &str) -> &mut SymbolValidationState {
        if !self.symbols.contains_key(symbol) {
            self.symbols
                .insert(symbol.to_owned(), SymbolValidationState::default());
        }
        match self.symbols.get_mut(symbol) {
            Some(state) => state,
            None => std::process::abort(),
        }
    }

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

    fn anchor_state_mut(
        &mut self,
        symbol: &str,
        anchor: ValidationAnchor,
        time_ns: i64,
    ) -> &mut AnchorState {
        let symbol_state = match self.symbols.get_mut(symbol) {
            Some(state) => state,
            None => std::process::abort(),
        };
        match symbol_state.references.get_mut(anchor, time_ns) {
            Some(reference) => &mut reference.state,
            None => std::process::abort(),
        }
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
        Ok(self)
    }

    const fn with_max_detail_records(mut self, limit: Option<usize>) -> Self {
        self.max_detail_records = limit;
        self
    }

    fn lookahead_ns(&self, symbol: &str) -> i64 {
        if !self.lookahead_override && self.market == Market::Szse {
            if is_etf_symbol(self.market, symbol) {
                return 1_100 * REFERENCE_MILLISECOND_NS;
            }
            if is_chinext_symbol(symbol) {
                return 3 * REFERENCE_SECOND_NS;
            }
        }
        self.continuous_lookahead_ns
    }

    fn pre_open_only(market: Market, references: HashMap<String, SymbolReferences>) -> Self {
        Self {
            pre_open_only: true,
            ..Self::new(market, references)
        }
    }

    fn compare_candidate(
        &mut self,
        symbol: &str,
        book: &OrderBook,
        anchor: ValidationAnchor,
        reference_time_ns: i64,
        candidate_time_ns: Option<i64>,
    ) -> Result<(), ProductionError> {
        let Some(reference) = self
            .symbols
            .get(symbol)
            .and_then(|state| state.references.get(anchor, reference_time_ns))
        else {
            return Ok(());
        };
        if reference.not_comparable_reason.is_some() {
            return Ok(());
        }
        if self
            .anchor_state(symbol, anchor, reference_time_ns)
            .is_some_and(|state| state.matched || state.finalized)
        {
            return Ok(());
        }
        let Some(expected) = reference.view.as_ref() else {
            return Ok(());
        };
        let projection = if self.market == Market::Szse
            && !is_etf_symbol(self.market, symbol)
            && anchor == ValidationAnchor::MarketClose
        {
            let metadata = self
                .sz_close_limits
                .get(symbol)
                .cloned()
                .unwrap_or_default();
            match metadata.unlimited() {
                Ok(false) => Ok(None),
                Ok(true) => self
                    .symbols
                    .get(symbol)
                    .and_then(|state| state.close_price.as_ref())
                    .and_then(|tracker| tracker.range_base.as_ref())
                    .ok_or_else(|| "missing pre-14:57 successful trade for SZ E0 range".to_owned())
                    .and_then(|base| {
                        close_range::project(book, base, &metadata)
                            .map(Some)
                            .map_err(|e| e.to_string())
                    }),
                Err(error) => Err(error),
            }
        } else {
            Ok(None)
        };
        let (mut actual, close_price_band) = match projection {
            Ok(Some((view, audit))) => (view, Some(audit)),
            Ok(None) => {
                let mut view = SnapshotBookView::from_book_scalars(book)?;
                view.weighted_bid_price_units =
                    published_weighted_price(book, Side::Buy, self.market, symbol)?;
                view.weighted_ask_price_units =
                    published_weighted_price(book, Side::Sell, self.market, symbol)?;
                (view, None)
            }
            Err(error) => {
                let state = self.anchor_state_mut(symbol, anchor, reference_time_ns);
                state.diagnostics_mut().comparison_error = Some(error);
                state.finalized = true;
                return Ok(());
            }
        };
        if close_price_band.is_none() {
            let scalar_mask = compare_reference_scalars(self.market, symbol, expected, &actual);
            // Depth contributes at most two additional field differences. Only
            // skip when no depth result can improve the first best candidate.
            // E0 reconciliation requires its complete mask (LastPrice-only).
            if anchor != ValidationAnchor::MarketClose
                && !scalar_mask.is_empty()
                && reference
                    .state
                    .best_differences()
                    .is_some_and(|best| scalar_mask.count() >= best.len())
            {
                #[cfg(feature = "profiling")]
                {
                    self.scalar_rejected_candidates += 1;
                }
                return Ok(());
            }
            actual.fill_depth(book, 10)?;
        }
        #[cfg(feature = "profiling")]
        {
            self.depth_materializations += 1;
        }
        let expected_view = expected.expand();
        let expected = &expected_view;
        let mut difference_mask = compare_view_mask(self.market, symbol, expected, &actual);
        let match_tag = if self.market == Market::Szse && anchor == ValidationAnchor::MarketClose {
            reconcile_sz_market_close_price(
                symbol,
                reference.pre_close_price_units,
                self.symbols
                    .get(symbol)
                    .and_then(|state| state.close_price.as_ref()),
                expected,
                &mut actual,
                difference_mask,
            )?
        } else {
            None
        };
        if match_tag.is_some() {
            difference_mask = compare_view_mask(self.market, symbol, expected, &actual);
        }
        let best_differences = if !difference_mask.is_empty()
            && self
                .anchor_state(symbol, anchor, reference_time_ns)
                .and_then(|state| state.best_differences())
                .is_none_or(|best| difference_mask.count() < best.len())
        {
            Some(compare_views(self.market, symbol, expected, &actual))
        } else {
            None
        };
        let state = self.anchor_state_mut(symbol, anchor, reference_time_ns);
        if close_price_band.is_some() {
            state.diagnostics_mut().close_price_band = close_price_band;
        }
        if difference_mask.is_empty() {
            state.matched = true;
            state.finalized = true;
            if let Some(diagnostics) = state.diagnostics.as_mut() {
                diagnostics.best_differences = None;
            }
            if state
                .diagnostics
                .as_ref()
                .is_some_and(|d| d.close_price_band.is_none() && d.comparison_error.is_none())
            {
                state.diagnostics = None;
            }
            state.matched_candidate_time_ns = candidate_time_ns;
            state.matched_candidate_raw_sequence =
                book.last_applied_meta().map(|m| m.raw_sequence.get());
            state.matched_candidate_apply_sequence =
                book.last_applied_meta().map(|m| m.apply_sequence.get());
            state.match_tag = match_tag;
        } else if let Some(differences) = best_differences {
            state.diagnostics_mut().best_differences = Some(differences);
            state.best_candidate_time_ns = candidate_time_ns;
            state.best_candidate_raw_sequence =
                book.last_applied_meta().map(|m| m.raw_sequence.get());
        }
        Ok(())
    }

    fn observe_timed_anchor(
        &mut self,
        symbol: &str,
        book: &OrderBook,
        anchor: ValidationAnchor,
        point: ObservationPoint,
    ) -> Result<(), ProductionError> {
        let Some(reference_time) = self
            .symbols
            .get(symbol)
            .and_then(|state| state.references.get_single(anchor))
            .map(|reference| reference.time_ns)
        else {
            return Ok(());
        };
        let (point_time, is_before_event) = match point {
            ObservationPoint::BeforeEvent(value) => (value, true),
            ObservationPoint::AfterEvent(value) => (value, false),
            ObservationPoint::MarketClose(_) | ObservationPoint::ChannelFinished => return Ok(()),
        };
        let window_ns = match anchor {
            // Raw market snapshots encode stage time at whole-second
            // granularity (for example 09:25:01.000 can include auction
            // trades stamped 09:25:01.010), so PreOpen uses the same
            // half-open second-bucket candidate rule as periodic frames.
            ValidationAnchor::PreOpen => REFERENCE_SECOND_NS,
            ValidationAnchor::ContinuousTrading | ValidationAnchor::MarketClose => return Ok(()),
        };
        let window_end = reference_time
            .checked_add(window_ns)
            .ok_or(ProductionError::Arithmetic("reference candidate window"))?;

        // The first BeforeEvent at or beyond window_end is the state after every
        // event in the half-open bucket and is therefore still a valid candidate,
        // even when the next event is much later. AfterEvent(window_end) already
        // includes an event outside the bucket and must not be compared.
        let inside_window =
            point_time >= reference_time && (point_time < window_end || is_before_event);
        if inside_window {
            let candidate_time = if point_time >= window_end {
                window_end
                    .checked_sub(REFERENCE_MILLISECOND_NS)
                    .ok_or(ProductionError::Arithmetic("reference candidate time"))?
            } else {
                point_time
            };
            self.compare_candidate(symbol, book, anchor, reference_time, Some(candidate_time))?;
        }
        if point_time >= window_end {
            self.anchor_state_mut(symbol, anchor, reference_time)
                .finalized = true;
        }
        Ok(())
    }

    fn observe_continuous_trading(
        &mut self,
        symbol: &str,
        book: &OrderBook,
        point: ObservationPoint,
    ) -> Result<(), ProductionError> {
        let (point_time, is_before_event) = match point {
            ObservationPoint::BeforeEvent(value) => (value, true),
            ObservationPoint::AfterEvent(value) => (value, false),
            ObservationPoint::MarketClose(_) | ObservationPoint::ChannelFinished => return Ok(()),
        };
        let mut position = self
            .symbols
            .get(symbol)
            .map(|state| state.continuous_position)
            .unwrap_or_default();
        let mut first_pending = None;
        loop {
            let Some(reference_time) = self
                .symbols
                .get(symbol)
                .and_then(|state| state.references.continuous_trading.get(position))
                .map(|reference| reference.time_ns)
            else {
                break;
            };
            // Raw snapshots expose a whole-second stage timestamp, while the
            // independently received tick stream can place the matching book
            // prefix earlier. This tolerance affects validation only; replay
            // ordering is still the native market sequence.
            let window_start = reference_time
                .checked_sub(self.continuous_lookback_ns)
                .ok_or(ProductionError::Arithmetic(
                    "continuous reference window start",
                ))?;
            if point_time < window_start {
                break;
            }
            let window_end = reference_time
                .checked_add(self.lookahead_ns(symbol))
                .ok_or(ProductionError::Arithmetic("continuous reference second"))?;
            if point_time < window_end || is_before_event {
                let candidate_time = if point_time >= window_end {
                    window_end.checked_sub(REFERENCE_MILLISECOND_NS).ok_or(
                        ProductionError::Arithmetic("continuous reference candidate time"),
                    )?
                } else {
                    point_time
                };
                self.compare_candidate(
                    symbol,
                    book,
                    ValidationAnchor::ContinuousTrading,
                    reference_time,
                    Some(candidate_time),
                )?;
            }
            if point_time >= window_end {
                self.anchor_state_mut(
                    symbol,
                    ValidationAnchor::ContinuousTrading,
                    reference_time,
                )
                .finalized = true;
            }
            let finalized = self
                .anchor_state(symbol, ValidationAnchor::ContinuousTrading, reference_time)
                .is_some_and(|state| state.finalized);
            if !finalized {
                first_pending.get_or_insert(position);
            }
            position += 1;
        }
        self.symbol_state_mut(symbol).continuous_position = first_pending.unwrap_or(position);
        Ok(())
    }

    fn finish_continuous_trading(
        &mut self,
        symbol: &str,
        book: &OrderBook,
    ) -> Result<(), ProductionError> {
        let mut position = self
            .symbols
            .get(symbol)
            .map(|state| state.continuous_position)
            .unwrap_or_default();
        loop {
            let Some(reference_time) = self
                .symbols
                .get(symbol)
                .and_then(|state| state.references.continuous_trading.get(position))
                .map(|reference| reference.time_ns)
            else {
                break;
            };
            let candidate_time = reference_time
                .checked_add(self.lookahead_ns(symbol) - REFERENCE_MILLISECOND_NS)
                .ok_or(ProductionError::Arithmetic(
                    "continuous reference candidate time",
                ))?;
            self.compare_candidate(
                symbol,
                book,
                ValidationAnchor::ContinuousTrading,
                reference_time,
                Some(candidate_time),
            )?;
            self.anchor_state_mut(symbol, ValidationAnchor::ContinuousTrading, reference_time)
                .finalized = true;
            position += 1;
        }
        self.symbol_state_mut(symbol).continuous_position = position;
        Ok(())
    }

    fn into_report(
        mut self,
        replay: ReplayReport,
        retain_matched_records: bool,
    ) -> ValidationReport {
        let market = market_text(self.market).to_owned();
        let mut symbols = self.symbols.keys().cloned().collect::<Vec<_>>();
        symbols.sort_unstable();
        let mut records = Vec::new();
        let mut totals = ValidationCounts::default();
        let mut breakdown = BTreeMap::<String, ValidationCounts>::new();
        let mut mismatch_fields = BTreeMap::new();
        let mut mismatch_reasons = BTreeMap::new();
        let mut not_comparable_reasons = BTreeMap::new();
        let mut mismatched_symbols = BTreeSet::new();
        let mut match_tags = BTreeMap::new();
        let mut omitted_matched_records = 0_u64;
        let mut omitted_mismatched_records = 0_u64;
        let mut omitted_not_comparable_records = 0_u64;
        let continuous_lookahead_ms_by_symbol = symbols
            .iter()
            .map(|symbol| {
                (
                    symbol.clone(),
                    self.lookahead_ns(symbol) / REFERENCE_MILLISECOND_NS,
                )
            })
            .collect();
        for symbol in symbols {
            let mut symbol_state = self.symbols.remove(&symbol).unwrap_or_default();
            let references = std::mem::take(&mut symbol_state.references);
            let mut cases = Vec::with_capacity(references.continuous_trading.len() + 2);
            cases.push((ValidationAnchor::PreOpen, references.pre_open));
            if !self.pre_open_only {
                if references.continuous_trading.is_empty() {
                    cases.push((ValidationAnchor::ContinuousTrading, None));
                } else {
                    cases.extend(
                        references.continuous_trading.into_iter().map(|reference| {
                            (ValidationAnchor::ContinuousTrading, Some(reference))
                        }),
                    );
                }
                cases.push((ValidationAnchor::MarketClose, references.market_close));
            }

            for (anchor, mut reference) in cases {
                let reference_time_ns = reference.as_ref().map(|reference| reference.time_ns);
                let state = reference
                    .as_mut()
                    .map(|reference| std::mem::take(&mut reference.state));
                let mut matched_candidate_time_ms = state
                    .as_ref()
                    .and_then(|state| state.matched_candidate_time_ns)
                    .map(timestamp_ns_to_ms);
                let mut matched_candidate_raw_sequence = state
                    .as_ref()
                    .and_then(|s| s.matched_candidate_raw_sequence);
                let mut matched_candidate_apply_sequence = state
                    .as_ref()
                    .and_then(|s| s.matched_candidate_apply_sequence);
                let best_candidate_time_ms = state
                    .as_ref()
                    .and_then(|s| s.best_candidate_time_ns)
                    .map(timestamp_ns_to_ms);
                let best_candidate_raw_sequence =
                    state.as_ref().and_then(|s| s.best_candidate_raw_sequence);
                let mut match_tag = state
                    .as_ref()
                    .and_then(|state| state.match_tag)
                    .map(str::to_owned);
                let close_price_band = state
                    .as_ref()
                    .and_then(|s| s.diagnostics.as_ref())
                    .and_then(|d| d.close_price_band.clone());
                let (outcome, reason, differences) = match (reference.as_ref(), state) {
                    _ if self
                        .phase_audit
                        .get(&symbol)
                        .and_then(|audit| audit.issue(anchor))
                        .is_some() =>
                    {
                        let issue = &self.phase_audit[&symbol].issues[anchor_text(anchor)];
                        (
                            ValidationOutcome::DataError,
                            Some(format!(
                                "{}: {:?} -> {} at {} source_row={}",
                                issue.reason,
                                issue.previous_status,
                                issue.status,
                                issue.time_ms,
                                issue.source_row_no
                            )),
                            Vec::new(),
                        )
                    }
                    (None, _)
                        if self
                            .phase_audit
                            .get(&symbol)
                            .and_then(|audit| audit.excluded_reason(anchor))
                            .is_some() =>
                    {
                        (
                            ValidationOutcome::ExcludedByStatus,
                            self.phase_audit[&symbol].excluded_reason(anchor).cloned(),
                            Vec::new(),
                        )
                    }
                    (None, _) if anchor == ValidationAnchor::MarketClose && symbol_state.seen => (
                        ValidationOutcome::MissingSource,
                        Some("required market-close reference is missing".to_owned()),
                        Vec::new(),
                    ),
                    (None, _) => (
                        ValidationOutcome::MissingSource,
                        Some("reference anchor is missing".to_owned()),
                        Vec::new(),
                    ),
                    (Some(_), state)
                        if self.market == Market::Szse
                            && anchor == ValidationAnchor::MarketClose
                            && replay.sz_after_close_events_by_symbol.contains_key(&symbol) =>
                    {
                        (
                            ValidationOutcome::DataError,
                            Some(format!(
                                "unclassified SZ events after 15:00:00.000: {}; phase review required before E0 acceptance",
                                replay.sz_after_close_events_by_symbol[&symbol]
                            )),
                            state
                                .and_then(|state| state.diagnostics)
                                .and_then(|d| d.best_differences)
                                .unwrap_or_default(),
                        )
                    }
                    (Some(reference), _) if reference.not_comparable_reason.is_some() => (
                        ValidationOutcome::ExcludedByStatus,
                        reference.not_comparable_reason.clone(),
                        Vec::new(),
                    ),
                    (Some(reference), _) if reference.load_error.is_some() => (
                        ValidationOutcome::DataError,
                        reference.load_error.clone(),
                        Vec::new(),
                    ),
                    (Some(_), _) if !symbol_state.seen => (
                        ValidationOutcome::MissingSource,
                        Some("no selected raw order/trade events were observed".to_owned()),
                        Vec::new(),
                    ),
                    (Some(_), Some(state))
                        if state
                            .diagnostics
                            .as_ref()
                            .is_some_and(|d| d.comparison_error.is_some()) =>
                    {
                        (
                            ValidationOutcome::DataError,
                            state.diagnostics.and_then(|d| d.comparison_error),
                            Vec::new(),
                        )
                    }
                    (Some(_), Some(state)) if state.matched => {
                        (ValidationOutcome::Matched, None, Vec::new())
                    }
                    (Some(_), Some(state)) => (
                        ValidationOutcome::Mismatched,
                        Some(
                            "no reconstructed full-state candidate matched the reference"
                                .to_owned(),
                        ),
                        state
                            .diagnostics
                            .and_then(|d| d.best_differences)
                            .unwrap_or_default(),
                    ),
                    (Some(_), None) => (
                        ValidationOutcome::MissingSource,
                        Some("replay did not reach the reference anchor".to_owned()),
                        Vec::new(),
                    ),
                };
                if outcome != ValidationOutcome::Matched {
                    matched_candidate_time_ms = None;
                    matched_candidate_raw_sequence = None;
                    matched_candidate_apply_sequence = None;
                    match_tag = None;
                }
                totals.observe(&outcome);
                let class = if is_etf_symbol(self.market, &symbol) {
                    "etf"
                } else {
                    "stock"
                };
                let key = format!("{class}.{}", anchor_text(anchor));
                breakdown.entry(key).or_default().observe(&outcome);
                if outcome == ValidationOutcome::Mismatched {
                    for difference in &differences {
                        *mismatch_fields.entry(difference.field.clone()).or_default() += 1;
                    }
                    if let Some(reason) = reason.as_ref() {
                        *mismatch_reasons.entry(reason.clone()).or_default() += 1;
                    }
                } else if outcome != ValidationOutcome::Matched {
                    if let Some(reason) = reason.as_ref() {
                        *not_comparable_reasons.entry(reason.clone()).or_default() += 1;
                    }
                }
                if outcome == ValidationOutcome::Matched {
                    if let Some(tag) = match_tag.as_ref() {
                        *match_tags.entry(tag.clone()).or_default() += 1;
                    }
                }
                let record = ValidationRecord {
                    close_price_band,
                    market: market.clone(),
                    symbol: symbol.clone(),
                    anchor,
                    outcome: outcome.clone(),
                    reference_time_ms: reference_time_ns.map(timestamp_ns_to_ms),
                    matched_candidate_time_ms,
                    matched_candidate_raw_sequence,
                    matched_candidate_apply_sequence,
                    channel_id: symbol_state.channel,
                    best_candidate_time_ms,
                    best_candidate_raw_sequence,
                    match_tag,
                    reason,
                    differences,
                };
                if outcome == ValidationOutcome::Mismatched {
                    mismatched_symbols.insert(symbol.clone());
                }
                let keep_detail = self
                    .max_detail_records
                    .is_none_or(|limit| records.len() < limit);
                if (outcome == ValidationOutcome::Matched && retain_matched_records)
                    || (outcome != ValidationOutcome::Matched && keep_detail)
                {
                    records.push(record);
                } else {
                    match outcome {
                        ValidationOutcome::Matched => omitted_matched_records += 1,
                        ValidationOutcome::Mismatched => omitted_mismatched_records += 1,
                        _ => omitted_not_comparable_records += 1,
                    }
                }
            }
        }
        let mut mismatch_symbol_prefixes = BTreeMap::new();
        for symbol in &mismatched_symbols {
            let prefix = symbol.get(..3).unwrap_or(symbol).to_owned();
            *mismatch_symbol_prefixes.entry(prefix).or_insert(0) += 1;
        }
        records.sort_by(|left, right| {
            left.symbol
                .cmp(&right.symbol)
                .then_with(|| anchor_rank(left.anchor).cmp(&anchor_rank(right.anchor)))
                .then_with(|| left.reference_time_ms.cmp(&right.reference_time_ms))
        });
        ValidationReport {
            replay,
            continuous_lookback_ms: self.continuous_lookback_ns / REFERENCE_MILLISECOND_NS,
            continuous_lookahead_ms: self.continuous_lookahead_ns / REFERENCE_MILLISECOND_NS,
            diagnostic_window_override: self.diagnostic_window_override,
            continuous_lookahead_ms_by_symbol,
            phase_audit: self.phase_audit.into_values().collect(),
            total_anchors: totals.total,
            comparable_anchors: totals.comparable,
            matched: totals.matched,
            mismatched: totals.mismatched,
            not_comparable: totals.not_comparable,
            excluded_by_status: totals.excluded_by_status,
            data_errors: totals.data_errors,
            missing_source: totals.missing_source,
            match_rate: ratio(totals.matched, totals.comparable),
            not_comparable_rate: ratio(totals.not_comparable, totals.total),
            mismatch_fields,
            mismatch_reasons,
            not_comparable_reasons,
            mismatched_symbols: mismatched_symbols.len() as u64,
            mismatch_symbol_prefixes,
            match_tags,
            breakdown,
            omitted_matched_records,
            omitted_mismatched_records,
            omitted_not_comparable_records,
            records,
        }
    }
}

impl StateObserver for ValidationObserver {
    fn observe_trade_with_sequence(
        &mut self,
        channel: u32,
        symbol: &str,
        raw_sequence: u64,
        quote_time_ns: i64,
        price_units: i64,
        quantity: u64,
    ) -> Result<(), ProductionError> {
        self.observe_trade(channel, symbol, quote_time_ns, price_units, quantity)?;
        if let Some(base) = self
            .symbols
            .get_mut(symbol)
            .and_then(|state| state.close_price.as_mut())
            .and_then(|tracker| tracker.range_base.as_mut())
        {
            if base.raw_sequence.is_none() {
                base.raw_sequence = Some(raw_sequence);
            }
        }
        Ok(())
    }
    fn observe_trade(
        &mut self,
        _channel: u32,
        symbol: &str,
        quote_time_ns: i64,
        price_units: i64,
        quantity: u64,
    ) -> Result<(), ProductionError> {
        if self.market == Market::Szse && !self.pre_open_only {
            self.symbol_state_mut(symbol)
                .close_price
                .get_or_insert_with(SzClosePriceTracker::default)
                .observe(quote_time_ns, price_units, quantity)?;
        }
        Ok(())
    }

    fn observe(
        &mut self,
        channel: u32,
        symbol: &str,
        book: &OrderBook,
        point: ObservationPoint,
    ) -> Result<(), ProductionError> {
        let state = self.symbol_state_mut(symbol);
        state.seen = true;
        state.channel = Some(channel);
        self.observe_timed_anchor(symbol, book, ValidationAnchor::PreOpen, point)?;
        if !self.pre_open_only {
            self.observe_continuous_trading(symbol, book, point)?;
        }
        if !self.pre_open_only {
            if let ObservationPoint::MarketClose(boundary_time_ns) = point {
                if let Some(reference_time) = self
                    .symbols
                    .get(symbol)
                    .and_then(|state| state.references.market_close.as_ref())
                    .map(|reference| reference.time_ns)
                {
                    self.compare_candidate(
                        symbol,
                        book,
                        ValidationAnchor::MarketClose,
                        reference_time,
                        Some(boundary_time_ns),
                    )?;
                    self.anchor_state_mut(symbol, ValidationAnchor::MarketClose, reference_time)
                        .finalized = true;
                }
            }
        }
        if point == ObservationPoint::ChannelFinished {
            if let Some(reference_time) = self
                .symbols
                .get(symbol)
                .and_then(|state| state.references.pre_open.as_ref())
                .map(|reference| reference.time_ns)
            {
                self.compare_candidate(
                    symbol,
                    book,
                    ValidationAnchor::PreOpen,
                    reference_time,
                    Some(
                        reference_time
                            .checked_add(REFERENCE_SECOND_NS - REFERENCE_MILLISECOND_NS)
                            .ok_or(ProductionError::Arithmetic("opening window end"))?,
                    ),
                )?;
                self.anchor_state_mut(symbol, ValidationAnchor::PreOpen, reference_time)
                    .finalized = true;
            }
            if !self.pre_open_only {
                self.finish_continuous_trading(symbol, book)?;
            }
        }
        Ok(())
    }
}

pub fn validate_market_day(config: &ValidationConfig) -> Result<ValidationReport, ProductionError> {
    let references = load_references(&config.request, false)?;
    let mut observer = ValidationObserver::new(config.request.market, references.books)
        .with_continuous_lookback(config.continuous_lookback)?
        .with_continuous_lookahead(config.continuous_lookahead)?
        .with_max_detail_records(config.max_detail_records);
    observer.phase_audit = references.phase_audit;
    observer.sz_close_limits = references.sz_close_limits;
    let mut request = config.request.clone();
    request.snapshots = None;
    let replay = run_market_day(&request, &mut observer)?;
    Ok(observer.into_report(replay, config.retain_matched_records))
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
    observer.phase_audit = references.phase_audit;
    let mut request = config.request.clone();
    request.snapshots = None;
    let replay = run_market_day_before(&request, cutoff, &mut observer)?;
    Ok(observer.into_report(replay, config.retain_matched_records))
}

fn ratio(numerator: u64, denominator: u64) -> Option<f64> {
    (denominator != 0).then_some(numerator as f64 / denominator as f64)
}

const fn timestamp_ns_to_ms(timestamp_ns: i64) -> i64 {
    timestamp_ns.div_euclid(REFERENCE_MILLISECOND_NS)
}

struct LoadedReferences {
    sz_close_limits: HashMap<String, DayLimits>,
    books: HashMap<String, SymbolReferences>,
    phase_audit: BTreeMap<String, PhaseAudit>,
}

fn load_references(
    request: &MarketDayRequest,
    pre_open_only: bool,
) -> Result<LoadedReferences, ProductionError> {
    let path = raw_snapshot_reference_path(request);
    let file = File::open(&path).map_err(|error| ProductionError::io(&path, error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| ProductionError::parquet(&path, error))?;
    let projection = columns::projection(&builder, request.market, &path)?;
    let timestamp_parser = super::time::MarketTimestampParser::new(request.trading_day)?;
    let reader = builder
        .with_projection(projection)
        .with_batch_size(request.batch_size)
        .build()
        .map_err(|error| ProductionError::parquet(&path, error))?;
    let opening_start = super::parse_market_timestamp(request.trading_day, "09:25:00.000")?;
    let continuous_start = super::parse_market_timestamp(request.trading_day, "09:30:00.000")?;
    let mut books: HashMap<String, SymbolReferences> = HashMap::new();
    let mut sz_close_limits: HashMap<String, DayLimits> = HashMap::new();
    let mut trackers: BTreeMap<String, PhaseTracker> = BTreeMap::new();
    let status_field = if request.market == Market::Sse {
        "InstruStatus"
    } else {
        "TradingPhaseCode"
    };
    for batch in reader {
        let batch = batch.map_err(|source| ProductionError::Arrow {
            context: "raw snapshot batch",
            source,
        })?;
        let columns = RawSnapshotColumns::bind(request.market, &path, &batch)?;
        let symbols = large_string(&path, &batch, "SecurityID")?;
        let times = large_string(&path, &batch, "UpdateTime")?;
        let statuses = large_string(&path, &batch, status_field)?;
        let rows = uint64(&path, &batch, "source_row_no")?;
        for row in 0..batch.num_rows() {
            if symbols.is_null(row)
                || times.is_null(row)
                || statuses.is_null(row)
                || rows.is_null(row)
            {
                return Err(ProductionError::Schema {
                    path: path.clone(),
                    detail: format!("null snapshot key at batch row {row}"),
                });
            }
            let symbol = symbols.value(row).trim();
            if !is_supported_symbol(request.market, symbol)
                || !request.targets.contains(request.market, symbol)
            {
                continue;
            }
            let time_ns = timestamp_parser.parse(times.value(row))?;
            if !pre_open_only
                && request.market == Market::Szse
                && !is_etf_symbol(request.market, symbol)
            {
                let values = columns.limits(row);
                sz_close_limits
                    .entry(symbol.to_owned())
                    .or_default()
                    .observe(
                        values,
                        format!("{}#source_row_no={}", path.display(), rows.value(row)),
                    );
            }
            let tracker = trackers
                .entry(symbol.to_owned())
                .or_insert_with(|| PhaseTracker::new(request.market, request.trading_day, symbol));
            let references = books.entry(symbol.to_owned()).or_default();
            let Some(anchor) = tracker.observe(
                request.market,
                (time_ns, rows.value(row)),
                statuses.value(row).trim(),
                opening_start,
                continuous_start,
            ) else {
                continue;
            };
            if pre_open_only && anchor != ValidationAnchor::PreOpen {
                continue;
            }
            let (view, load_error) = load_raw_snapshot_view(&columns, &path, row)?;
            let candidate = ReferenceSnapshot {
                time_ns,
                view,
                load_error,
                not_comparable_reason: None,
                pre_close_price_units: if request.market == Market::Szse
                    && anchor == ValidationAnchor::MarketClose
                {
                    columns.pre_close(row)?
                } else {
                    None
                },
                state: AnchorState::default(),
            };
            match anchor {
                ValidationAnchor::ContinuousTrading => {
                    references.continuous_trading.push(candidate)
                }
                ValidationAnchor::PreOpen | ValidationAnchor::MarketClose => {
                    let target = if anchor == ValidationAnchor::PreOpen {
                        &mut references.pre_open
                    } else {
                        &mut references.market_close
                    };
                    if let Some(first) = target.as_mut() {
                        // Compare normalized reference fields exactly; the book's
                        // weighted-price tolerance does not apply to reference duplicates.
                        if first.load_error.is_none()
                            && (first.view != candidate.view || candidate.load_error.is_some())
                        {
                            first.load_error = Some(format!(
                                "repeated {} {} frames differ for {symbol}: first={} candidate={} source_row={}",
                                market_text(request.market),
                                anchor_text(anchor),
                                timestamp_ns_to_ms(first.time_ns),
                                timestamp_ns_to_ms(time_ns),
                                rows.value(row)
                            ));
                        }
                    } else {
                        *target = Some(candidate);
                    }
                }
            }
        }
    }
    // Duplicate periodic timestamps are source errors, not frames to silently deduplicate.
    for (symbol, references) in &mut books {
        references
            .continuous_trading
            .sort_by_key(|reference| reference.time_ns);
        let duplicates = references
            .continuous_trading
            .windows(2)
            .filter(|w| w[0].time_ns == w[1].time_ns)
            .map(|w| w[0].time_ns)
            .collect::<HashSet<_>>();
        for reference in &mut references.continuous_trading {
            if duplicates.contains(&reference.time_ns) {
                reference.load_error = Some(format!(
                    "duplicate continuous reference timestamp for {symbol}"
                ));
            }
        }
    }
    if let super::TargetUniverse::Symbols(symbols) = &request.targets {
        for symbol in symbols {
            books.entry(symbol.to_string()).or_default();
        }
    }
    Ok(LoadedReferences {
        sz_close_limits,
        books,
        phase_audit: trackers
            .into_iter()
            .map(|(symbol, tracker)| (symbol, tracker.audit))
            .collect(),
    })
}

fn load_raw_snapshot_view(
    columns: &RawSnapshotColumns<'_>,
    path: &Path,
    row: usize,
) -> Result<(Option<ReferenceBookView>, Option<String>), ProductionError> {
    match columns.view(path, row) {
        Ok(view) => Ok((Some(view.try_into()?), None)),
        Err(ProductionError::Validation(detail)) => Ok((None, Some(detail))),
        Err(error) => Err(error),
    }
}

fn raw_snapshot_reference_path(request: &MarketDayRequest) -> PathBuf {
    request
        .raw_root
        .join(format!("date={}", request.trading_day.as_yyyymmdd()))
        .join(match request.market {
            Market::Sse => "MarketData",
            Market::Szse => "mdl_6_28_0",
        })
        .join("part-0.parquet")
}

fn market_text(market: Market) -> &'static str {
    match market {
        Market::Sse => "SH",
        Market::Szse => "SZ",
    }
}

const DIFF_BIDS: u16 = 1 << 0;
const DIFF_ASKS: u16 = 1 << 1;
const DIFF_TOTAL_BID_QUANTITY: u16 = 1 << 2;
const DIFF_WEIGHTED_BID_PRICE: u16 = 1 << 3;
const DIFF_TOTAL_ASK_QUANTITY: u16 = 1 << 4;
const DIFF_WEIGHTED_ASK_PRICE: u16 = 1 << 5;
const DIFF_LAST_PRICE: u16 = 1 << 6;
const DIFF_HIGH_PRICE: u16 = 1 << 7;
const DIFF_LOW_PRICE: u16 = 1 << 8;
const DIFF_TRADE_COUNT: u16 = 1 << 9;
const DIFF_TRADE_QUANTITY: u16 = 1 << 10;
const DIFF_TURNOVER: u16 = 1 << 11;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DifferenceMask(u16);

impl DifferenceMask {
    fn record(&mut self, bit: u16, differs: bool) {
        if differs {
            self.0 |= bit;
        }
    }

    const fn is_empty(self) -> bool {
        self.0 == 0
    }

    fn count(self) -> usize {
        self.0.count_ones() as usize
    }

    const fn is_only(self, bit: u16) -> bool {
        self.0 == bit
    }
}

fn compare_reference_scalars(
    market: Market,
    symbol: &str,
    expected: &ReferenceBookView,
    actual: &SnapshotBookView,
) -> DifferenceMask {
    let mut mask = DifferenceMask::default();
    mask.record(
        DIFF_TOTAL_BID_QUANTITY,
        expected.total_bid_quantity != actual.total_bid_quantity,
    );
    mask.record(
        DIFF_WEIGHTED_BID_PRICE,
        !weighted_prices_match(
            expected.weighted_bid_price_units,
            validation_price(market, symbol, actual.weighted_bid_price_units),
        ),
    );
    mask.record(
        DIFF_TOTAL_ASK_QUANTITY,
        expected.total_ask_quantity != actual.total_ask_quantity,
    );
    mask.record(
        DIFF_WEIGHTED_ASK_PRICE,
        !weighted_prices_match(
            expected.weighted_ask_price_units,
            validation_price(market, symbol, actual.weighted_ask_price_units),
        ),
    );
    mask.record(
        DIFF_LAST_PRICE,
        expected.last_price_units != actual.last_price_units,
    );
    mask.record(
        DIFF_HIGH_PRICE,
        expected.high_price_units != actual.high_price_units,
    );
    mask.record(
        DIFF_LOW_PRICE,
        expected.low_price_units != actual.low_price_units,
    );
    mask.record(DIFF_TRADE_COUNT, expected.trade_count != actual.trade_count);
    mask.record(
        DIFF_TRADE_QUANTITY,
        expected.trade_quantity != actual.trade_quantity,
    );
    mask.record(
        DIFF_TURNOVER,
        expected.turnover_units != actual.turnover_units,
    );
    mask
}

fn compare_view_mask(
    market: Market,
    symbol: &str,
    expected: &SnapshotBookView,
    actual: &SnapshotBookView,
) -> DifferenceMask {
    let mut mask = DifferenceMask::default();
    mask.record(DIFF_BIDS, expected.bids != actual.bids);
    mask.record(DIFF_ASKS, expected.asks != actual.asks);
    mask.record(
        DIFF_TOTAL_BID_QUANTITY,
        expected.total_bid_quantity != actual.total_bid_quantity,
    );
    mask.record(
        DIFF_WEIGHTED_BID_PRICE,
        !weighted_prices_match(
            expected.weighted_bid_price_units,
            validation_price(market, symbol, actual.weighted_bid_price_units),
        ),
    );
    mask.record(
        DIFF_TOTAL_ASK_QUANTITY,
        expected.total_ask_quantity != actual.total_ask_quantity,
    );
    mask.record(
        DIFF_WEIGHTED_ASK_PRICE,
        !weighted_prices_match(
            expected.weighted_ask_price_units,
            validation_price(market, symbol, actual.weighted_ask_price_units),
        ),
    );
    mask.record(
        DIFF_LAST_PRICE,
        expected.last_price_units != actual.last_price_units,
    );
    mask.record(
        DIFF_HIGH_PRICE,
        expected.high_price_units != actual.high_price_units,
    );
    mask.record(
        DIFF_LOW_PRICE,
        expected.low_price_units != actual.low_price_units,
    );
    mask.record(DIFF_TRADE_COUNT, expected.trade_count != actual.trade_count);
    mask.record(
        DIFF_TRADE_QUANTITY,
        expected.trade_quantity != actual.trade_quantity,
    );
    mask.record(
        DIFF_TURNOVER,
        expected.turnover_units != actual.turnover_units,
    );
    mask
}

fn compare_views(
    market: Market,
    symbol: &str,
    expected: &SnapshotBookView,
    actual: &SnapshotBookView,
) -> Vec<FieldDifference> {
    let mut differences = Vec::new();
    compare(&mut differences, "bids", &expected.bids, &actual.bids);
    compare(&mut differences, "asks", &expected.asks, &actual.asks);
    compare(
        &mut differences,
        "total_bid_quantity",
        &expected.total_bid_quantity,
        &actual.total_bid_quantity,
    );
    compare_weighted_price(
        &mut differences,
        "weighted_bid_price_units",
        expected.weighted_bid_price_units,
        validation_price(market, symbol, actual.weighted_bid_price_units),
    );
    compare(
        &mut differences,
        "total_ask_quantity",
        &expected.total_ask_quantity,
        &actual.total_ask_quantity,
    );
    compare_weighted_price(
        &mut differences,
        "weighted_ask_price_units",
        expected.weighted_ask_price_units,
        validation_price(market, symbol, actual.weighted_ask_price_units),
    );
    for (field, expected_value, actual_value) in [
        (
            "last_price_units",
            expected.last_price_units,
            actual.last_price_units,
        ),
        (
            "high_price_units",
            expected.high_price_units,
            actual.high_price_units,
        ),
        (
            "low_price_units",
            expected.low_price_units,
            actual.low_price_units,
        ),
    ] {
        compare(&mut differences, field, &expected_value, &actual_value);
    }
    compare(
        &mut differences,
        "trade_count",
        &expected.trade_count,
        &actual.trade_count,
    );
    compare(
        &mut differences,
        "trade_quantity",
        &expected.trade_quantity,
        &actual.trade_quantity,
    );
    compare(
        &mut differences,
        "turnover_units",
        &expected.turnover_units,
        &actual.turnover_units,
    );
    differences
}

fn reconcile_sz_market_close_price(
    symbol: &str,
    pre_close_price_units: Option<i64>,
    tracker: Option<&SzClosePriceTracker>,
    expected: &SnapshotBookView,
    actual: &mut SnapshotBookView,
    differences: DifferenceMask,
) -> Result<Option<&'static str>, ProductionError> {
    if !differences.is_only(DIFF_LAST_PRICE) {
        return Ok(None);
    }

    let is_etf = is_etf_symbol(Market::Szse, symbol);
    let (official_close_price, tag) = match tracker {
        Some(tracker) if tracker.has_closing_auction_trade => return Ok(None),
        Some(tracker) => (
            tracker.average_close_price(validation_price_quantum(Market::Szse, symbol))?,
            if is_etf {
                SZ_ETF_AVG_CLOSE_PRICE_TAG
            } else {
                SZ_STOCK_AVG_CLOSE_PRICE_TAG
            },
        ),
        None => (
            pre_close_price_units,
            if is_etf {
                SZ_ETF_PRE_CLOSE_PRICE_TAG
            } else {
                SZ_STOCK_PRE_CLOSE_PRICE_TAG
            },
        ),
    };
    if official_close_price != expected.last_price_units {
        return Ok(None);
    }
    actual.last_price_units = official_close_price;
    Ok(Some(tag))
}

/// One internal price unit is CNY 0.0001, so ten units are CNY 0.001.
const WEIGHTED_PRICE_TOLERANCE_UNITS: u64 = 10;

fn compare_weighted_price(
    differences: &mut Vec<FieldDifference>,
    field: &str,
    expected: Option<i64>,
    actual: Option<i64>,
) {
    if !weighted_prices_match(expected, actual) {
        differences.push(FieldDifference {
            field: field.to_owned(),
            expected: format!(
                "{expected:?} (absolute tolerance: {} units / CNY 0.001)",
                WEIGHTED_PRICE_TOLERANCE_UNITS
            ),
            actual: format!("{actual:?}"),
        });
    }
}

fn weighted_prices_match(expected: Option<i64>, actual: Option<i64>) -> bool {
    match (expected, actual) {
        (None, None) => true,
        (Some(expected), Some(actual)) => {
            expected.abs_diff(actual) <= WEIGHTED_PRICE_TOLERANCE_UNITS
        }
        _ => false,
    }
}

fn validation_price(market: Market, symbol: &str, value: Option<i64>) -> Option<i64> {
    let quantum = validation_price_quantum(market, symbol);
    value.map(|units| (units + quantum / 2) / quantum * quantum)
}

fn validation_price_quantum(market: Market, symbol: &str) -> i64 {
    match market {
        Market::Sse => 10,
        Market::Szse if is_etf_symbol(market, symbol) => 10,
        Market::Szse => 100,
    }
}

fn published_weighted_price(
    book: &OrderBook,
    side: Side,
    market: Market,
    symbol: &str,
) -> Result<Option<i64>, ProductionError> {
    let (total, weighted) = book.visible_aggregate(side);
    if total == 0 {
        return Ok(None);
    }
    let total = u128::from(total);
    let quantum = u128::try_from(validation_price_quantum(market, symbol))
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

fn compare<T: std::fmt::Debug + PartialEq>(
    differences: &mut Vec<FieldDifference>,
    field: &str,
    expected: &T,
    actual: &T,
) {
    if expected != actual {
        differences.push(FieldDifference {
            field: field.to_owned(),
            expected: format!("{expected:?}"),
            actual: format!("{actual:?}"),
        });
    }
}

fn column_index(path: &Path, batch: &RecordBatch, name: &str) -> Result<usize, ProductionError> {
    batch
        .schema()
        .index_of(name)
        .map_err(|_| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("missing reference field {name}"),
        })
}

fn large_string<'a>(
    path: &Path,
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a LargeStringArray, ProductionError> {
    let index = column_index(path, batch, name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or_else(|| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("reference field {name} is not LargeUtf8"),
        })
}

fn int64<'a>(
    path: &Path,
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Int64Array, ProductionError> {
    let index = column_index(path, batch, name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or_else(|| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("reference field {name} is not Int64"),
        })
}

fn uint32<'a>(
    path: &Path,
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a UInt32Array, ProductionError> {
    let index = column_index(path, batch, name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or_else(|| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("reference field {name} is not UInt32"),
        })
}

fn uint64<'a>(
    path: &Path,
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a UInt64Array, ProductionError> {
    let index = column_index(path, batch, name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or_else(|| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("reference field {name} is not UInt64"),
        })
}

fn decimal128<'a>(
    path: &Path,
    batch: &'a RecordBatch,
    name: &str,
    expected_scale: i8,
) -> Result<&'a Decimal128Array, ProductionError> {
    let index = column_index(path, batch, name)?;
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("reference field {name} is not Decimal128"),
        })?;
    match array.data_type() {
        DataType::Decimal128(_, scale) if *scale == expected_scale => Ok(array),
        DataType::Decimal128(_, scale) => Err(ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!(
                "reference field {name} has decimal scale {scale}, expected {expected_scale}"
            ),
        }),
        _ => Err(ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("reference field {name} is not Decimal128"),
        }),
    }
}

fn rescale_nonnegative_decimal(value: i128, source_scale: i8, target_scale: i8) -> Option<i128> {
    if value < 0 {
        return None;
    }
    match source_scale.cmp(&target_scale) {
        std::cmp::Ordering::Less => {
            let exponent = u32::try_from(target_scale - source_scale).ok()?;
            value.checked_mul(10_i128.checked_pow(exponent)?)
        }
        std::cmp::Ordering::Equal => Some(value),
        std::cmp::Ordering::Greater => {
            let exponent = u32::try_from(source_scale - target_scale).ok()?;
            let divisor = 10_i128.checked_pow(exponent)?;
            value
                .checked_add(divisor / 2)
                .map(|scaled| scaled / divisor)
        }
    }
}

const fn anchor_rank(anchor: ValidationAnchor) -> u8 {
    match anchor {
        ValidationAnchor::PreOpen => 0,
        ValidationAnchor::ContinuousTrading => 1,
        ValidationAnchor::MarketClose => 2,
    }
}

const fn anchor_text(anchor: ValidationAnchor) -> &'static str {
    match anchor {
        ValidationAnchor::PreOpen => "pre_open",
        ValidationAnchor::ContinuousTrading => "continuous_trading",
        ValidationAnchor::MarketClose => "market_close",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::{
        AnchorState, ObservationPoint, ReferenceSnapshot, ReplayReport, StateObserver,
        SymbolReferences, ValidationAnchor, ValidationObserver, ValidationOutcome,
    };
    use crate::{
        AddOrder, ApplySequence, BookConfig, BookEvent, BookKey, ChannelId, CrossingBehavior,
        EventMeta, LocalTimestampNs, Market, OrderBook, OrderId, OrderKey, Price, PriceScale,
        PricingInstruction, Quantity, QuoteTimestampNs, RawSequence, Side, Symbol, TradingDay,
    };

    fn some_or_abort<T>(value: Option<T>) -> T {
        match value {
            Some(value) => value,
            None => std::process::abort(),
        }
    }

    fn empty_book() -> OrderBook {
        let key = BookKey {
            market: Market::Sse,
            trading_day: some_or_abort(TradingDay::from_yyyymmdd(20_260_828)),
            symbol: Symbol::from("600000"),
        };
        OrderBook::new(BookConfig::new(
            key,
            some_or_abort(PriceScale::from_decimal_places(4)),
        ))
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn e0_projection_is_deterministic_and_never_filters_reference_or_mutates_book() {
        for case in [
            "matched",
            "reference_outside",
            "missing_base",
            "missing_metadata",
            "conflict",
            "etf",
        ] {
            let symbol = if case == "etf" { "159001" } else { "000001" };
            let mut book = empty_book();
            add_order(&mut book);
            let before = book.summary();
            let full = super::SnapshotBookView::from_book(&book, 10).expect("view");
            let mut expected = full.clone();
            if case != "reference_outside" && case != "etf" {
                expected.bids.clear();
                expected.total_bid_quantity = 0;
                expected.weighted_bid_price_units = None;
            }
            let refs = HashMap::from([(
                symbol.to_owned(),
                SymbolReferences {
                    market_close: Some(ReferenceSnapshot {
                        time_ns: timestamp("15:00:00.000"),
                        view: Some(
                            expected
                                .clone()
                                .try_into()
                                .unwrap_or_else(|_| std::process::abort()),
                        ),
                        load_error: None,
                        not_comparable_reason: None,
                        pre_close_price_units: None,
                        state: AnchorState::default(),
                    }),
                    ..SymbolReferences::default()
                },
            )]);
            let mut observer = ValidationObserver::new(Market::Szse, refs);
            if case != "missing_metadata" {
                let mut limits = super::DayLimits::default();
                limits.observe(
                    Ok((super::close_range::NO_UPPER_LIMIT, 100)),
                    "row1".to_owned(),
                );
                if case == "conflict" {
                    limits.observe(Ok((120000, 80000)), "row2".to_owned());
                }
                observer.sz_close_limits.insert(symbol.to_owned(), limits);
            }
            if case != "missing_base" {
                observer
                    .observe_trade_with_sequence(1, symbol, 7, timestamp("14:56:59.990"), 1000, 10)
                    .expect("trade");
                // The final trade must not reset the independently frozen range.
                observer
                    .observe_trade_with_sequence(1, symbol, 8, timestamp("15:00:00.000"), 2000, 10)
                    .expect("trade");
            }
            observer
                .observe(
                    1,
                    symbol,
                    &book,
                    ObservationPoint::MarketClose(timestamp("15:00:00.000")),
                )
                .expect("close");
            assert_eq!(
                observer.symbols[symbol]
                    .references
                    .market_close
                    .as_ref()
                    .expect("reference")
                    .view,
                Some(
                    expected
                        .try_into()
                        .unwrap_or_else(|_| std::process::abort())
                )
            );
            let report = observer.into_report(ReplayReport::default(), true);
            let close = report
                .records
                .iter()
                .find(|r| r.anchor == ValidationAnchor::MarketClose)
                .expect("close");
            let outcome = match case {
                "matched" | "etf" => ValidationOutcome::Matched,
                "reference_outside" => ValidationOutcome::Mismatched,
                _ => ValidationOutcome::DataError,
            };
            assert_eq!(close.outcome, outcome, "{case}");
            if case == "matched" || case == "reference_outside" {
                let band = close
                    .close_price_band
                    .as_ref()
                    .expect("rule context even on mismatch");
                assert_eq!(band.base_price_units, 1000);
                assert_eq!(band.base_raw_sequence, 7);
            } else {
                assert!(close.close_price_band.is_none());
            }
            assert_eq!(book.summary(), before);
            assert_eq!(
                super::SnapshotBookView::from_book(&book, 10).expect("view"),
                full
            );
        }
    }

    fn timestamp(value: &str) -> i64 {
        match super::super::parse_market_timestamp(
            some_or_abort(TradingDay::from_yyyymmdd(20_260_828)),
            value,
        ) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        }
    }

    fn sz_close_references(
        symbol: &str,
        last_price_units: i64,
        pre_close_price_units: i64,
    ) -> HashMap<String, SymbolReferences> {
        let mut view = match super::SnapshotBookView::from_book(&empty_book(), 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        view.last_price_units = Some(last_price_units);
        HashMap::from([(
            symbol.to_owned(),
            SymbolReferences {
                pre_open: None,
                continuous_trading: Vec::new(),
                market_close: Some(ReferenceSnapshot {
                    time_ns: timestamp("15:00:00.000"),
                    view: Some(view.try_into().unwrap_or_else(|_| std::process::abort())),
                    load_error: None,
                    not_comparable_reason: None,
                    pre_close_price_units: Some(pre_close_price_units),
                    state: AnchorState::default(),
                }),
            },
        )])
    }

    fn add_order(book: &mut OrderBook) {
        let key = OrderKey {
            channel_id: some_or_abort(ChannelId::new(1)),
            side: Side::Buy,
            order_id: some_or_abort(OrderId::new(1)),
        };
        let event = BookEvent::AddOrder(AddOrder {
            meta: EventMeta {
                book_key: book.config().book_key.clone(),
                raw_sequence: some_or_abort(RawSequence::new(1)),
                apply_sequence: some_or_abort(ApplySequence::new(1)),
                local_time: LocalTimestampNs::from_nanos(11),
                quote_time: QuoteTimestampNs::from_nanos(10),
            },
            order_key: key,
            pricing: PricingInstruction::Provided(some_or_abort(Price::from_units(100_000))),
            crossing: CrossingBehavior::Rest,
            quantity: some_or_abort(Quantity::new(100)),
        });
        if book.apply(event).is_err() {
            std::process::abort();
        }
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn scalar_pruning_preserves_first_match_and_best_difference_context() {
        let mut book = empty_book();
        let mut candidates = vec![book.clone()];
        for seq in 1..=24 {
            book.apply(BookEvent::AddOrder(AddOrder {
                meta: EventMeta {
                    book_key: book.config().book_key.clone(),
                    raw_sequence: RawSequence::new(seq).expect("raw"),
                    apply_sequence: ApplySequence::new(seq).expect("apply"),
                    local_time: LocalTimestampNs::from_nanos(seq as i64),
                    quote_time: QuoteTimestampNs::from_nanos(seq as i64),
                },
                order_key: OrderKey {
                    channel_id: ChannelId::new(1).expect("channel"),
                    side: if seq % 2 == 0 { Side::Buy } else { Side::Sell },
                    order_id: OrderId::new(seq).expect("id"),
                },
                pricing: PricingInstruction::Provided(
                    Price::from_units(100_000 + seq as i64 * 100).expect("price"),
                ),
                crossing: CrossingBehavior::Rest,
                quantity: Quantity::new(seq * 100).expect("qty"),
            }))
            .expect("apply");
            candidates.push(book.clone());
        }
        for market in [Market::Sse, Market::Szse] {
            for missing_match in [false, true] {
                let mut expected =
                    super::SnapshotBookView::from_book(&candidates[12], 10).expect("view");
                expected.weighted_bid_price_units =
                    super::published_weighted_price(&candidates[12], Side::Buy, market, "600000")
                        .expect("bid");
                expected.weighted_ask_price_units =
                    super::published_weighted_price(&candidates[12], Side::Sell, market, "600000")
                        .expect("ask");
                if missing_match {
                    expected.trade_count = 1;
                }
                let refs = HashMap::from([(
                    "600000".to_owned(),
                    SymbolReferences {
                        continuous_trading: vec![ReferenceSnapshot {
                            time_ns: 10,
                            view: Some(expected.clone().try_into().expect("compact")),
                            load_error: None,
                            not_comparable_reason: None,
                            pre_close_price_units: None,
                            state: AnchorState::default(),
                        }],
                        ..SymbolReferences::default()
                    },
                )]);
                let mut observer = ValidationObserver::new(market, refs);
                let mut best: Option<Vec<super::FieldDifference>> = None;
                let mut best_time = None;
                let mut matched_time = None;
                for (i, candidate) in candidates.iter().chain(candidates.iter().rev()).enumerate() {
                    let time = i as i64;
                    if matched_time.is_none() {
                        let mut actual =
                            super::SnapshotBookView::from_book(candidate, 10).expect("view");
                        actual.weighted_bid_price_units =
                            super::published_weighted_price(candidate, Side::Buy, market, "600000")
                                .expect("bid");
                        actual.weighted_ask_price_units = super::published_weighted_price(
                            candidate,
                            Side::Sell,
                            market,
                            "600000",
                        )
                        .expect("ask");
                        let diffs = super::compare_views(market, "600000", &expected, &actual);
                        if diffs.is_empty() {
                            matched_time = Some(time);
                            best = None;
                        } else if best.as_ref().is_none_or(|b| diffs.len() < b.len()) {
                            best = Some(diffs);
                            best_time = Some(time);
                        }
                    }
                    observer
                        .compare_candidate(
                            "600000",
                            candidate,
                            ValidationAnchor::ContinuousTrading,
                            10,
                            Some(time),
                        )
                        .expect("compare");
                    let state = observer
                        .anchor_state("600000", ValidationAnchor::ContinuousTrading, 10)
                        .expect("state");
                    assert_eq!(state.matched_candidate_time_ns, matched_time);
                    assert_eq!(state.best_candidate_time_ns, best_time);
                    assert_eq!(state.best_differences(), best.as_ref());
                    if state.matched {
                        assert!(state.diagnostics.is_none());
                    }
                }
                #[cfg(feature = "profiling")]
                if missing_match {
                    assert!(observer.scalar_rejected_candidates > 0);
                }
            }
        }
    }

    #[test]
    fn pre_open_accepts_a_state_inside_the_reference_second() {
        let mut expected_book = empty_book();
        add_order(&mut expected_book);
        let expected = match super::SnapshotBookView::from_book(&expected_book, 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let references = HashMap::from([(
            "600000".to_owned(),
            SymbolReferences {
                pre_open: Some(ReferenceSnapshot {
                    time_ns: 10_000_000,
                    view: Some(
                        expected
                            .try_into()
                            .unwrap_or_else(|_| std::process::abort()),
                    ),
                    load_error: None,
                    not_comparable_reason: None,
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }),
                continuous_trading: Vec::new(),
                market_close: None,
            },
        )]);
        let compact_references = references.clone();
        let mut observer = ValidationObserver::new(Market::Sse, references);
        let mut actual = empty_book();
        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &actual,
                    ObservationPoint::BeforeEvent(10_000_000),
                )
                .is_ok()
        );
        add_order(&mut actual);
        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &actual,
                    ObservationPoint::AfterEvent(170_000_000),
                )
                .is_ok()
        );
        let report = observer.into_report(ReplayReport::default(), true);
        let pre_open = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::PreOpen);
        assert!(matches!(
            pre_open,
            Some(record) if record.outcome == ValidationOutcome::Matched
                && record.matched_candidate_time_ms == Some(170)
        ));
        assert_eq!(report.matched, 1);
        assert_eq!(report.mismatched, 0);
        assert_eq!(report.missing_source, 2);
        assert_eq!(report.not_comparable, 2);
        assert_eq!(report.match_rate, Some(1.0));

        let mut compact = ValidationObserver::new(Market::Sse, compact_references);
        assert!(
            compact
                .observe(
                    1,
                    "600000",
                    &actual,
                    ObservationPoint::AfterEvent(170_000_000),
                )
                .is_ok()
        );
        let compact_report = compact.into_report(ReplayReport::default(), false);
        assert_eq!(compact_report.omitted_matched_records, 1);
        assert_eq!(compact_report.records.len(), 2);
        assert!(
            compact_report
                .records
                .iter()
                .all(|record| record.outcome != ValidationOutcome::Matched)
        );
        assert_eq!(
            compact_report
                .breakdown
                .get("stock.pre_open")
                .map(|counts| counts.matched),
            Some(1)
        );
    }

    #[test]
    fn continuous_anchor_accepts_any_state_inside_the_reference_second() {
        let mut expected_book = empty_book();
        add_order(&mut expected_book);
        let expected = match super::SnapshotBookView::from_book(&expected_book, 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let reference_time = 10_000_000_000;
        let references = HashMap::from([(
            "600000".to_owned(),
            SymbolReferences {
                pre_open: None,
                continuous_trading: vec![ReferenceSnapshot {
                    time_ns: reference_time,
                    view: Some(
                        expected
                            .try_into()
                            .unwrap_or_else(|_| std::process::abort()),
                    ),
                    load_error: None,
                    not_comparable_reason: None,
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }],
                market_close: None,
            },
        )]);
        let mut observer = ValidationObserver::new(Market::Sse, references);
        let mut actual = empty_book();
        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &actual,
                    ObservationPoint::BeforeEvent(reference_time),
                )
                .is_ok()
        );
        add_order(&mut actual);
        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &actual,
                    ObservationPoint::AfterEvent(reference_time + 999_000_000),
                )
                .is_ok()
        );

        let report = observer.into_report(ReplayReport::default(), true);
        let continuous = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::ContinuousTrading);
        assert!(matches!(
            continuous,
            Some(record) if record.outcome == ValidationOutcome::Matched
                && record.matched_candidate_time_ms == Some(10_999)
        ));
    }

    #[test]
    fn old_and_assumed_market_order_reports_are_not_standard_acceptance() {
        let observer = ValidationObserver::new(Market::Szse, HashMap::new());
        let mut report = observer.into_report(ReplayReport::default(), true);
        report.total_anchors = 1;
        report.comparable_anchors = 1;
        report.matched = 1;
        report.omitted_matched_records = 1;
        assert!(report.is_success());
        assert!(!report.is_standard_acceptance());
        report.replay.sz_pending_resolution_version = 1;
        assert!(report.is_standard_acceptance());
        report.replay.sz_market_order_policy = crate::SzMarketOrderPolicy::AssumeContiguous;
        assert!(!report.is_standard_acceptance());
        report.replay.sz_market_order_policy = crate::SzMarketOrderPolicy::RestAtLastTradePrice;
        assert!(report.is_success());
        assert!(!report.is_standard_acceptance());
    }

    #[test]
    fn missing_historical_policy_does_not_imply_practical_replay() {
        let report: ReplayReport = match serde_json::from_str("{}") {
            Ok(report) => report,
            Err(_) => std::process::abort(),
        };
        assert_eq!(report.sz_pending_resolution_version, 0);
        assert_eq!(
            report.sz_market_order_policy,
            crate::SzMarketOrderPolicy::RequireEvidence
        );
    }

    #[test]
    fn sse_continuous_anchor_accepts_a_state_from_the_previous_second() {
        let expected_book = empty_book();
        let expected = match super::SnapshotBookView::from_book(&expected_book, 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let reference_time = 10_000_000_000;
        let references = HashMap::from([(
            "600000".to_owned(),
            SymbolReferences {
                pre_open: None,
                continuous_trading: vec![ReferenceSnapshot {
                    time_ns: reference_time,
                    view: Some(
                        expected
                            .try_into()
                            .unwrap_or_else(|_| std::process::abort()),
                    ),
                    load_error: None,
                    not_comparable_reason: None,
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }],
                market_close: None,
            },
        )]);
        let mut observer = ValidationObserver::new(Market::Sse, references);
        let mut actual = empty_book();
        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &actual,
                    ObservationPoint::BeforeEvent(reference_time - 500_000_000),
                )
                .is_ok()
        );
        add_order(&mut actual);
        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &actual,
                    ObservationPoint::BeforeEvent(reference_time + 1_000_000_000),
                )
                .is_ok()
        );

        let report = observer.into_report(ReplayReport::default(), true);
        let continuous = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::ContinuousTrading);
        assert!(matches!(
            continuous,
            Some(record) if record.outcome == ValidationOutcome::Matched
                && record.matched_candidate_time_ms == Some(9_500)
        ));
    }

    #[test]
    fn sz_continuous_anchor_does_not_use_the_previous_second() {
        let expected_book = empty_book();
        let expected = match super::SnapshotBookView::from_book(&expected_book, 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let reference_time = 10_000_000_000;
        let references = HashMap::from([(
            "000001".to_owned(),
            SymbolReferences {
                pre_open: None,
                continuous_trading: vec![ReferenceSnapshot {
                    time_ns: reference_time,
                    view: Some(
                        expected
                            .try_into()
                            .unwrap_or_else(|_| std::process::abort()),
                    ),
                    load_error: None,
                    not_comparable_reason: None,
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }],
                market_close: None,
            },
        )]);
        let mut observer = ValidationObserver::new(Market::Szse, references);
        let mut actual = empty_book();
        assert!(
            observer
                .observe(
                    1,
                    "000001",
                    &actual,
                    ObservationPoint::BeforeEvent(reference_time - 500_000_000),
                )
                .is_ok()
        );
        add_order(&mut actual);
        assert!(
            observer
                .observe(
                    1,
                    "000001",
                    &actual,
                    ObservationPoint::BeforeEvent(reference_time + 1_000_000_000),
                )
                .is_ok()
        );

        let report = observer.into_report(ReplayReport::default(), true);
        let continuous = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::ContinuousTrading);
        assert!(matches!(
            continuous,
            Some(record) if record.outcome == ValidationOutcome::Mismatched
                && record.matched_candidate_time_ms.is_none()
        ));
    }

    #[test]
    fn configured_sz_lookback_accepts_a_state_from_the_previous_second() {
        let expected_book = empty_book();
        let expected = match super::SnapshotBookView::from_book(&expected_book, 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let reference_time = 10_000_000_000;
        let references = HashMap::from([(
            "000001".to_owned(),
            SymbolReferences {
                pre_open: None,
                continuous_trading: vec![ReferenceSnapshot {
                    time_ns: reference_time,
                    view: Some(
                        expected
                            .try_into()
                            .unwrap_or_else(|_| std::process::abort()),
                    ),
                    load_error: None,
                    not_comparable_reason: None,
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }],
                market_close: None,
            },
        )]);
        let mut observer = match ValidationObserver::new(Market::Szse, references)
            .with_continuous_lookback(Some(Duration::from_secs(1)))
        {
            Ok(observer) => observer,
            Err(_) => std::process::abort(),
        };
        let mut actual = empty_book();
        assert!(
            observer
                .observe(
                    1,
                    "000001",
                    &actual,
                    ObservationPoint::BeforeEvent(reference_time - 500_000_000),
                )
                .is_ok()
        );
        add_order(&mut actual);
        assert!(
            observer
                .observe(
                    1,
                    "000001",
                    &actual,
                    ObservationPoint::BeforeEvent(reference_time + 1_000_000_000),
                )
                .is_ok()
        );

        let report = observer.into_report(ReplayReport::default(), true);
        let continuous = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::ContinuousTrading);
        assert!(matches!(
            continuous,
            Some(record) if record.outcome == ValidationOutcome::Matched
                && record.matched_candidate_time_ms == Some(9_500)
        ));
        assert_eq!(report.continuous_lookback_ms, 1_000);
    }

    #[test]
    fn configured_sz_lookahead_accepts_a_state_later_in_the_three_second_frame() {
        let mut expected_book = empty_book();
        add_order(&mut expected_book);
        let expected = match super::SnapshotBookView::from_book(&expected_book, 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let reference_time = 10_000_000_000;
        let references = HashMap::from([(
            "300001".to_owned(),
            SymbolReferences {
                pre_open: None,
                continuous_trading: vec![ReferenceSnapshot {
                    time_ns: reference_time,
                    view: Some(
                        expected
                            .try_into()
                            .unwrap_or_else(|_| std::process::abort()),
                    ),
                    load_error: None,
                    not_comparable_reason: None,
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }],
                market_close: None,
            },
        )]);
        let mut observer = match ValidationObserver::new(Market::Szse, references)
            .with_continuous_lookahead(Some(Duration::from_secs(3)))
        {
            Ok(observer) => observer,
            Err(_) => std::process::abort(),
        };
        let mut actual = empty_book();
        add_order(&mut actual);
        assert!(
            observer
                .observe(
                    1,
                    "300001",
                    &actual,
                    ObservationPoint::AfterEvent(reference_time + 2_500_000_000),
                )
                .is_ok()
        );

        let report = observer.into_report(ReplayReport::default(), true);
        let continuous = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::ContinuousTrading);
        assert!(matches!(
            continuous,
            Some(record) if record.outcome == ValidationOutcome::Matched
                && record.matched_candidate_time_ms == Some(12_500)
        ));
        assert_eq!(report.continuous_lookahead_ms, 3_000);
    }

    #[test]
    fn mixed_sz_universe_automatically_uses_board_specific_windows_and_audits_sequence() {
        let mut expected_book = empty_book();
        add_order(&mut expected_book);
        let expected = super::SnapshotBookView::from_book(&expected_book, 10)
            .unwrap_or_else(|_| std::process::abort());
        let symbols = ["000001", "159501", "300001", "301001", "302132"];
        let references = symbols
            .into_iter()
            .map(|symbol| {
                (
                    symbol.to_owned(),
                    SymbolReferences {
                        pre_open: None,
                        continuous_trading: vec![ReferenceSnapshot {
                            time_ns: 10_000_000_000,
                            view: Some(
                                expected
                                    .clone()
                                    .try_into()
                                    .unwrap_or_else(|_| std::process::abort()),
                            ),
                            load_error: None,
                            not_comparable_reason: None,
                            pre_close_price_units: None,
                            state: AnchorState::default(),
                        }],
                        market_close: None,
                    },
                )
            })
            .collect();
        let mut observer = ValidationObserver::new(Market::Szse, references);
        for symbol in symbols {
            observer
                .observe(
                    1,
                    symbol,
                    &empty_book(),
                    ObservationPoint::BeforeEvent(12_000_000_000),
                )
                .unwrap_or_else(|_| std::process::abort());
            observer
                .observe(
                    1,
                    symbol,
                    &expected_book,
                    ObservationPoint::AfterEvent(12_000_000_000),
                )
                .unwrap_or_else(|_| std::process::abort());
        }
        let report = observer.into_report(ReplayReport::default(), true);
        assert!(!report.diagnostic_window_override);
        for record in report
            .records
            .iter()
            .filter(|r| r.anchor == ValidationAnchor::ContinuousTrading)
        {
            let chinext = super::is_chinext_symbol(&record.symbol);
            assert_eq!(
                record.outcome,
                if chinext {
                    ValidationOutcome::Matched
                } else {
                    ValidationOutcome::Mismatched
                }
            );
            assert_eq!(
                report.continuous_lookahead_ms_by_symbol[&record.symbol],
                if chinext {
                    3000
                } else if super::is_etf_symbol(Market::Szse, &record.symbol) {
                    1100
                } else {
                    1000
                }
            );
            if chinext {
                assert_eq!(record.matched_candidate_time_ms, Some(12000));
                assert_eq!(
                    record.matched_candidate_raw_sequence,
                    expected_book
                        .last_applied_meta()
                        .map(|m| m.raw_sequence.get())
                );
                assert_eq!(record.channel_id, Some(1));
            }
        }
    }

    #[test]
    fn sz_etf_default_window_accepts_1099ms_but_excludes_1100ms_events() {
        for offset_ms in [0, 999, 1000, 1040, 1099, 1100, 1101] {
            let mut actual = empty_book();
            let mut expected_book = empty_book();
            add_order(&mut expected_book);
            let reference_time = 10 * super::REFERENCE_SECOND_NS;
            let reference = ReferenceSnapshot {
                time_ns: reference_time,
                view: Some(
                    super::SnapshotBookView::from_book(&expected_book, 10)
                        .and_then(TryInto::try_into)
                        .unwrap_or_else(|_| std::process::abort()),
                ),
                load_error: None,
                not_comparable_reason: None,
                pre_close_price_units: None,
                state: AnchorState::default(),
            };
            let references = HashMap::from([(
                "159501".to_owned(),
                SymbolReferences {
                    pre_open: Some(reference.clone()),
                    continuous_trading: vec![reference],
                    market_close: None,
                },
            )]);
            let mut observer = ValidationObserver::new(Market::Szse, references);
            let event_time = reference_time + offset_ms * super::REFERENCE_MILLISECOND_NS;
            observer
                .observe(
                    1,
                    "159501",
                    &actual,
                    ObservationPoint::BeforeEvent(event_time),
                )
                .unwrap_or_else(|_| std::process::abort());
            add_order(&mut actual);
            observer
                .observe(
                    1,
                    "159501",
                    &actual,
                    ObservationPoint::AfterEvent(event_time),
                )
                .unwrap_or_else(|_| std::process::abort());
            let report = observer.into_report(ReplayReport::default(), true);
            assert!(!report.diagnostic_window_override);
            assert_eq!(report.continuous_lookback_ms, 0);
            assert_eq!(report.continuous_lookahead_ms_by_symbol["159501"], 1100);
            for (anchor, end_ms) in [
                (ValidationAnchor::ContinuousTrading, 1100),
                (ValidationAnchor::PreOpen, 1000),
            ] {
                let record = report
                    .records
                    .iter()
                    .find(|r| r.anchor == anchor)
                    .unwrap_or_else(|| std::process::abort());
                assert_eq!(
                    record.outcome,
                    if offset_ms < end_ms {
                        ValidationOutcome::Matched
                    } else {
                        ValidationOutcome::Mismatched
                    },
                    "{anchor:?}, offset={offset_ms}"
                );
            }
        }
    }

    #[test]
    fn explicit_horizon_overrides_sz_etf_default_but_not_other_market_defaults() {
        let default = ValidationObserver::new(Market::Szse, HashMap::new());
        for (symbol, millis) in [("000001", 1000), ("159501", 1100), ("300001", 3000)] {
            assert_eq!(
                default.lookahead_ns(symbol),
                millis * super::REFERENCE_MILLISECOND_NS
            );
        }
        for millis in [1000, 1100, 3000] {
            let observer = ValidationObserver::new(Market::Szse, HashMap::new())
                .with_continuous_lookahead(Some(Duration::from_millis(millis)))
                .unwrap_or_else(|_| std::process::abort());
            assert!(observer.diagnostic_window_override);
            for symbol in ["000001", "159501", "300001"] {
                assert_eq!(
                    observer.lookahead_ns(symbol),
                    i64::try_from(millis).unwrap_or_else(|_| std::process::abort())
                        * super::REFERENCE_MILLISECOND_NS
                );
            }
        }
        let sh = ValidationObserver::new(Market::Sse, HashMap::new());
        for symbol in ["600000", "510300"] {
            assert_eq!(sh.lookahead_ns(symbol), super::REFERENCE_SECOND_NS);
        }
    }

    #[test]
    fn overlapping_windows_do_not_block_a_later_matching_frame() {
        let mut populated = empty_book();
        add_order(&mut populated);
        let references = HashMap::from([(
            "600000".to_owned(),
            SymbolReferences {
                pre_open: None,
                continuous_trading: [
                    (10_000_000_000, &populated),
                    (11_000_000_000, &empty_book()),
                ]
                .into_iter()
                .map(|(time_ns, book)| ReferenceSnapshot {
                    time_ns,
                    view: Some(
                        super::SnapshotBookView::from_book(book, 10)
                            .and_then(TryInto::try_into)
                            .unwrap_or_else(|_| std::process::abort()),
                    ),
                    load_error: None,
                    not_comparable_reason: None,
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                })
                .collect(),
                market_close: None,
            },
        )]);
        let mut observer = ValidationObserver::new(Market::Sse, references);
        observer
            .observe(
                1,
                "600000",
                &empty_book(),
                ObservationPoint::AfterEvent(10_500_000_000),
            )
            .unwrap_or_else(|_| std::process::abort());
        let report = observer.into_report(ReplayReport::default(), true);
        let later = report
            .records
            .iter()
            .find(|r| r.reference_time_ms == Some(11_000));
        assert!(
            matches!(later, Some(record) if record.outcome == ValidationOutcome::Matched
            && record.matched_candidate_time_ms == Some(10_500))
        );
    }

    #[test]
    fn continuous_lookahead_rejects_overlapping_reference_frames() {
        let result = ValidationObserver::new(Market::Szse, HashMap::new())
            .with_continuous_lookahead(Some(Duration::from_millis(3_001)));
        assert!(matches!(
            result,
            Err(super::ProductionError::InvalidRequest(_))
        ));
    }

    #[test]
    fn detail_cap_preserves_aggregate_mismatch_and_symbol_counts() {
        let mut expected_book = empty_book();
        add_order(&mut expected_book);
        let expected = match super::SnapshotBookView::from_book(&expected_book, 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let empty = match super::SnapshotBookView::from_book(&empty_book(), 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let references = HashMap::from([(
            "600000".to_owned(),
            SymbolReferences {
                pre_open: Some(ReferenceSnapshot {
                    time_ns: 0,
                    view: Some(
                        empty
                            .clone()
                            .try_into()
                            .unwrap_or_else(|_| std::process::abort()),
                    ),
                    load_error: None,
                    not_comparable_reason: None,
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }),
                continuous_trading: vec![
                    ReferenceSnapshot {
                        time_ns: 10_000_000_000,
                        view: Some(
                            expected
                                .clone()
                                .try_into()
                                .unwrap_or_else(|_| std::process::abort()),
                        ),
                        load_error: None,
                        not_comparable_reason: None,
                        pre_close_price_units: None,
                        state: AnchorState::default(),
                    },
                    ReferenceSnapshot {
                        time_ns: 12_000_000_000,
                        view: Some(
                            expected
                                .try_into()
                                .unwrap_or_else(|_| std::process::abort()),
                        ),
                        load_error: None,
                        not_comparable_reason: None,
                        pre_close_price_units: None,
                        state: AnchorState::default(),
                    },
                ],
                market_close: Some(ReferenceSnapshot {
                    time_ns: 14_000_000_000,
                    view: Some(empty.try_into().unwrap_or_else(|_| std::process::abort())),
                    load_error: None,
                    not_comparable_reason: None,
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }),
            },
        )]);
        let mut observer =
            ValidationObserver::new(Market::Sse, references).with_max_detail_records(Some(1));
        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &empty_book(),
                    ObservationPoint::BeforeEvent(14_000_000_000),
                )
                .is_ok()
        );

        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &empty_book(),
                    ObservationPoint::MarketClose(14_000_000_000)
                )
                .is_ok()
        );
        let report = observer.into_report(ReplayReport::default(), false);
        assert_eq!(report.mismatched, 2);
        assert_eq!(report.records.len(), 1);
        assert_eq!(report.omitted_mismatched_records, 1);
        assert_eq!(report.mismatched_symbols, 1);
        assert_eq!(report.mismatch_symbol_prefixes.get("600"), Some(&1));
    }

    #[test]
    fn validates_every_continuous_trading_reference_frame() {
        let mut book = empty_book();
        add_order(&mut book);
        let expected = match super::SnapshotBookView::from_book(&book, 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let first_time = 10_000_000_000;
        let second_time = 12_000_000_000;
        let references = HashMap::from([(
            "600000".to_owned(),
            SymbolReferences {
                pre_open: None,
                continuous_trading: vec![
                    ReferenceSnapshot {
                        time_ns: first_time,
                        view: Some(
                            expected
                                .clone()
                                .try_into()
                                .unwrap_or_else(|_| std::process::abort()),
                        ),
                        load_error: None,
                        not_comparable_reason: None,
                        pre_close_price_units: None,
                        state: AnchorState::default(),
                    },
                    ReferenceSnapshot {
                        time_ns: second_time,
                        view: Some(
                            expected
                                .try_into()
                                .unwrap_or_else(|_| std::process::abort()),
                        ),
                        load_error: None,
                        not_comparable_reason: None,
                        pre_close_price_units: None,
                        state: AnchorState::default(),
                    },
                ],
                market_close: None,
            },
        )]);
        let mut observer = ValidationObserver::new(Market::Sse, references);
        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &book,
                    ObservationPoint::BeforeEvent(first_time),
                )
                .is_ok()
        );
        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &book,
                    ObservationPoint::BeforeEvent(second_time),
                )
                .is_ok()
        );

        let report = observer.into_report(ReplayReport::default(), true);
        let continuous = report
            .records
            .iter()
            .filter(|record| record.anchor == ValidationAnchor::ContinuousTrading)
            .collect::<Vec<_>>();
        assert_eq!(continuous.len(), 2);
        assert!(
            continuous
                .iter()
                .all(|record| record.outcome == ValidationOutcome::Matched)
        );
        assert_eq!(continuous[0].reference_time_ms, Some(10_000));
        assert_eq!(continuous[1].reference_time_ms, Some(12_000));
    }

    #[test]
    fn continuous_anchor_excludes_events_at_the_next_second() {
        let mut expected_book = empty_book();
        add_order(&mut expected_book);
        let expected = match super::SnapshotBookView::from_book(&expected_book, 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let reference_time = 10_000_000_000;
        let references = HashMap::from([(
            "600000".to_owned(),
            SymbolReferences {
                pre_open: None,
                continuous_trading: vec![ReferenceSnapshot {
                    time_ns: reference_time,
                    view: Some(
                        expected
                            .try_into()
                            .unwrap_or_else(|_| std::process::abort()),
                    ),
                    load_error: None,
                    not_comparable_reason: None,
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }],
                market_close: None,
            },
        )]);
        let mut observer = ValidationObserver::new(Market::Sse, references);
        let mut actual = empty_book();
        let next_second = reference_time + 1_000_000_000;
        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &actual,
                    ObservationPoint::BeforeEvent(next_second),
                )
                .is_ok()
        );
        add_order(&mut actual);
        assert!(
            observer
                .observe(
                    1,
                    "600000",
                    &actual,
                    ObservationPoint::AfterEvent(next_second),
                )
                .is_ok()
        );

        let report = observer.into_report(ReplayReport::default(), true);
        let continuous = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::ContinuousTrading);
        assert!(matches!(
            continuous,
            Some(record) if record.outcome == ValidationOutcome::Mismatched
                && record.matched_candidate_time_ms.is_none()
        ));
    }

    #[test]
    fn all_anchors_compare_full_state_with_weighted_price_tolerance() {
        let mut book = empty_book();
        add_order(&mut book);
        let expected = match super::SnapshotBookView::from_book(&book, 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };

        let mut actual = expected.clone();
        actual.weighted_bid_price_units = actual.weighted_bid_price_units.map(|value| value + 10);
        assert!(super::compare_views(Market::Sse, "600000", &expected, &actual).is_empty());
        assert!(super::compare_view_mask(Market::Sse, "600000", &expected, &actual).is_empty());

        actual.weighted_bid_price_units = expected.weighted_bid_price_units.map(|value| value + 20);
        let differences = super::compare_views(Market::Sse, "600000", &expected, &actual);
        assert_eq!(differences.len(), 1);
        assert_eq!(
            super::compare_view_mask(Market::Sse, "600000", &expected, &actual).count(),
            differences.len()
        );
        assert_eq!(differences[0].field, "weighted_bid_price_units");

        let mut actual = expected.clone();
        actual.bids[0].order_count += 1;
        actual.total_bid_quantity += 1;
        actual.last_price_units = Some(100_000);
        actual.trade_count += 1;
        let differences = super::compare_views(Market::Sse, "600000", &expected, &actual);
        assert_eq!(differences.len(), 4);
        assert_eq!(
            super::compare_view_mask(Market::Sse, "600000", &expected, &actual).count(),
            differences.len()
        );
        assert_eq!(differences[0].field, "bids");
        assert_eq!(differences[1].field, "total_bid_quantity");
        assert_eq!(differences[2].field, "last_price_units");
        assert_eq!(differences[3].field, "trade_count");
    }

    #[test]
    fn weighted_price_requires_matching_presence() {
        let mut differences = Vec::new();
        super::compare_weighted_price(
            &mut differences,
            "weighted_bid_price_units",
            Some(100_000),
            None,
        );
        assert_eq!(differences.len(), 1);
    }

    #[test]
    fn rounds_weighted_prices_to_reference_feed_precision() {
        assert_eq!(
            super::validation_price(Market::Sse, "600000", Some(89_086)),
            Some(89_090)
        );
        assert_eq!(
            super::validation_price(Market::Szse, "000001", Some(113_222)),
            Some(113_200)
        );
        assert_eq!(
            super::validation_price(Market::Szse, "159001", Some(997_194)),
            Some(997_190)
        );
        assert!(matches!(
            super::round_weighted_to_quantum(69_274_900, 1_000, 10),
            Ok(69_270)
        ));
    }

    #[test]
    fn production_price_multiplier_matches_four_decimal_places() {
        let scale = some_or_abort(PriceScale::from_decimal_places(
            super::super::PRODUCTION_PRICE_DECIMAL_PLACES,
        ));
        assert_eq!(
            scale.multiplier(),
            super::super::PRODUCTION_PRICE_MULTIPLIER
        );
    }

    #[test]
    fn rescales_raw_decimal_references_without_float64() {
        assert_eq!(
            super::rescale_nonnegative_decimal(7_994, 3, 4),
            Some(79_940)
        );
        assert_eq!(
            super::rescale_nonnegative_decimal(12_345_649, 6, 4),
            Some(123_456)
        );
        assert_eq!(
            super::rescale_nonnegative_decimal(12_345_650, 6, 4),
            Some(123_457)
        );
        assert_eq!(super::rescale_nonnegative_decimal(-1, 6, 4), None);
    }

    #[test]
    fn shenzhen_e0_accepts_verified_one_minute_average_close_price() {
        let symbol = "159001";
        let references = sz_close_references(symbol, 10_010, 9_900);
        let mut observer = ValidationObserver::new(Market::Szse, references);
        assert!(
            observer
                .observe_trade(1, symbol, timestamp("14:55:30.000"), 10_000, 100)
                .is_ok()
        );
        assert!(
            observer
                .observe_trade(1, symbol, timestamp("14:56:00.000"), 10_020, 100)
                .is_ok()
        );
        assert!(
            observer
                .observe(
                    1,
                    symbol,
                    &empty_book(),
                    ObservationPoint::MarketClose(timestamp("15:00:00.000")),
                )
                .is_ok()
        );

        let report = observer.into_report(ReplayReport::default(), true);
        let close = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::MarketClose);
        assert!(matches!(
            close,
            Some(record) if record.outcome == ValidationOutcome::Matched
                && record.match_tag.as_deref() == Some(super::SZ_ETF_AVG_CLOSE_PRICE_TAG)
        ));
        assert_eq!(
            report.match_tags.get(super::SZ_ETF_AVG_CLOSE_PRICE_TAG),
            Some(&1)
        );
    }

    #[test]
    fn shenzhen_stock_average_close_uses_the_stock_price_quantum() {
        let symbol = "000001";
        let references = sz_close_references(symbol, 10_000, 9_900);
        let mut observer = ValidationObserver::new(Market::Szse, references);
        observer.sz_close_limits.insert(
            symbol.to_owned(),
            super::DayLimits {
                values: Some((200_000, 100)),
                source: "synthetic limited stock".to_owned(),
                error: None,
            },
        );
        assert!(
            observer
                .observe_trade(1, symbol, timestamp("14:55:30.000"), 10_000, 100)
                .is_ok()
        );
        assert!(
            observer
                .observe_trade(1, symbol, timestamp("14:56:00.000"), 10_060, 100)
                .is_ok()
        );
        assert!(
            observer
                .observe(
                    1,
                    symbol,
                    &empty_book(),
                    ObservationPoint::MarketClose(timestamp("15:00:00.000")),
                )
                .is_ok()
        );

        let report = observer.into_report(ReplayReport::default(), true);
        let close = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::MarketClose);
        assert!(matches!(
            close,
            Some(record) if record.outcome == ValidationOutcome::Matched
                && record.match_tag.as_deref() == Some(super::SZ_STOCK_AVG_CLOSE_PRICE_TAG)
        ));
    }

    #[test]
    fn shenzhen_average_close_uses_an_inclusive_sixty_second_window() {
        let mut tracker = super::SzClosePriceTracker::default();
        assert!(
            tracker
                .observe(timestamp("14:54:59.999"), 10_000, 100)
                .is_ok()
        );
        assert!(
            tracker
                .observe(timestamp("14:55:00.000"), 10_020, 100)
                .is_ok()
        );
        assert!(
            tracker
                .observe(timestamp("14:56:00.000"), 10_040, 100)
                .is_ok()
        );
        assert_eq!(tracker.last_minute.len(), 2);
        assert_eq!(tracker.average_close_price(10).ok().flatten(), Some(10_030));
        assert_eq!(
            tracker.average_close_price(100).ok().flatten(),
            Some(10_000)
        );
        assert!(!tracker.has_closing_auction_trade);
    }

    #[test]
    fn shenzhen_e0_uses_previous_close_when_the_day_has_no_trade() {
        let symbol = "000001";
        let references = sz_close_references(symbol, 123_400, 123_400);
        let mut observer = ValidationObserver::new(Market::Szse, references);
        observer.sz_close_limits.insert(
            symbol.to_owned(),
            super::DayLimits {
                values: Some((200_000, 100)),
                source: "synthetic limited stock".to_owned(),
                error: None,
            },
        );
        assert!(
            observer
                .observe(
                    1,
                    symbol,
                    &empty_book(),
                    ObservationPoint::MarketClose(timestamp("15:00:00.000")),
                )
                .is_ok()
        );

        let report = observer.into_report(ReplayReport::default(), true);
        let close = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::MarketClose);
        assert!(matches!(
            close,
            Some(record) if record.outcome == ValidationOutcome::Matched
                && record.match_tag.as_deref() == Some(super::SZ_STOCK_PRE_CLOSE_PRICE_TAG)
        ));
    }

    #[test]
    fn shenzhen_e0_does_not_replace_a_closing_auction_price() {
        let symbol = "159001";
        let references = sz_close_references(symbol, 10_010, 9_900);
        let mut observer = ValidationObserver::new(Market::Szse, references);
        assert!(
            observer
                .observe_trade(1, symbol, timestamp("14:57:00.000"), 10_000, 100)
                .is_ok()
        );
        assert!(
            observer
                .observe(
                    1,
                    symbol,
                    &empty_book(),
                    ObservationPoint::MarketClose(timestamp("15:00:00.000")),
                )
                .is_ok()
        );

        let report = observer.into_report(ReplayReport::default(), true);
        let close = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::MarketClose);
        assert!(matches!(
            close,
            Some(record) if record.outcome == ValidationOutcome::Mismatched
                && record.match_tag.is_none()
        ));
        assert!(report.match_tags.is_empty());
    }

    #[test]
    fn suspension_reason_precedes_missing_tick_reason() {
        let references = HashMap::from([(
            "000635".to_owned(),
            SymbolReferences {
                pre_open: Some(ReferenceSnapshot {
                    time_ns: 100,
                    view: None,
                    load_error: None,
                    not_comparable_reason: Some("suspended PreOpen phase status B1".to_owned()),
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }),
                continuous_trading: vec![ReferenceSnapshot {
                    time_ns: 200,
                    view: None,
                    load_error: None,
                    not_comparable_reason: Some("suspended continuous phase status T1".to_owned()),
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }],
                market_close: Some(ReferenceSnapshot {
                    time_ns: 300,
                    view: None,
                    load_error: None,
                    not_comparable_reason: Some("suspended close phase status E1".to_owned()),
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                }),
            },
        )]);
        let observer = ValidationObserver::new(Market::Szse, references);
        let report = observer.into_report(ReplayReport::default(), true);
        assert_eq!(report.not_comparable, 3);
        assert!(report.records.iter().all(|record| {
            record.outcome == ValidationOutcome::ExcludedByStatus
                && record
                    .reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("suspended"))
        }));
    }
}
