//! Candidate extraction is cached, never candidate eligibility or match timing.
use super::close_range::{self, DayLimits};
use super::precision::turnover_precision;
use super::reference::ReferenceBookView;
use super::report::FieldDifference;
use super::sz_close_price::{SzClosePriceTracker, reconcile_sz_market_close_price};
use super::{
    REFERENCE_MILLISECOND_NS, REFERENCE_SECOND_NS, ReferenceSnapshot, SymbolValidationRules,
    SymbolValidationState, published_weighted_price_with_quantum, quantize_validation_price,
    validation_price,
};
use crate::production::replay::ObservationPoint;
use crate::{
    Market, OrderBook, ProductionError, Side, SnapshotBookView, SnapshotLevels, ValidationAnchor,
};

#[derive(Clone, Debug, Default)]
pub(super) struct CandidateCache {
    // Owned by one symbol's validator for one BookKey lifecycle. Revision is
    // not a global identifier and must not be shared across book instances.
    revision: Option<u64>,
    view: Option<SnapshotBookView>,
    depth_ready: bool,
}

#[cfg(test)]
pub(super) mod tests;

#[derive(Default)]
pub(super) struct CandidateCounters {
    #[cfg(feature = "profiling")]
    pub scalar_rejected_candidates: u64,
    #[cfg(feature = "profiling")]
    pub depth_materializations: u64,
    #[cfg(feature = "profiling")]
    pub candidate_cache_hits: u64,
}

#[derive(Clone, Copy)]
pub(super) struct CandidateContext<'a> {
    pub market: Market,
    pub rules: SymbolValidationRules,
    pub symbol: &'a str,
    pub book: &'a OrderBook,
    pub limits: Option<&'a DayLimits>,
    pub close_price: Option<&'a SzClosePriceTracker>,
}

fn scalars(ctx: CandidateContext<'_>) -> Result<SnapshotBookView, ProductionError> {
    let stats = ctx.book.statistics();
    Ok(SnapshotBookView {
        bids: SnapshotLevels::new(),
        asks: SnapshotLevels::new(),
        total_bid_quantity: ctx.book.visible_aggregate(Side::Buy).0,
        total_ask_quantity: ctx.book.visible_aggregate(Side::Sell).0,
        weighted_bid_price_units: published_weighted_price_with_quantum(
            ctx.book,
            Side::Buy,
            ctx.rules.price_quantum,
        )?,
        weighted_ask_price_units: published_weighted_price_with_quantum(
            ctx.book,
            Side::Sell,
            ctx.rules.price_quantum,
        )?,
        last_price_units: stats.last_price.map(crate::Price::units),
        high_price_units: stats.high_price.map(crate::Price::units),
        low_price_units: stats.low_price.map(crate::Price::units),
        trade_count: stats.trade_count,
        trade_quantity: stats.total_quantity,
        turnover_units: stats.total_turnover_units,
    })
}

impl CandidateCache {
    fn prepare(
        &mut self,
        ctx: CandidateContext<'_>,
        counters: &mut CandidateCounters,
    ) -> Result<(), ProductionError> {
        let revision = ctx.book.cache_revision();
        if revision.is_some() && self.revision == revision && self.view.is_some() {
            #[cfg(feature = "profiling")]
            {
                counters.candidate_cache_hits += 1;
            }
        } else {
            self.view = Some(scalars(ctx)?);
            self.revision = revision;
            self.depth_ready = false;
        }
        let _ = counters;
        Ok(())
    }
}

pub(super) const DIFF_BIDS: u16 = 1 << 0;
pub(super) const DIFF_ASKS: u16 = 1 << 1;
const DIFF_TOTAL_BID_QUANTITY: u16 = 1 << 2;
const DIFF_WEIGHTED_BID_PRICE: u16 = 1 << 3;
const DIFF_TOTAL_ASK_QUANTITY: u16 = 1 << 4;
const DIFF_WEIGHTED_ASK_PRICE: u16 = 1 << 5;
pub(super) const DIFF_LAST_PRICE: u16 = 1 << 6;
const DIFF_HIGH_PRICE: u16 = 1 << 7;
const DIFF_LOW_PRICE: u16 = 1 << 8;
const DIFF_TRADE_COUNT: u16 = 1 << 9;
const DIFF_TRADE_QUANTITY: u16 = 1 << 10;
const DIFF_TURNOVER: u16 = 1 << 11;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct DifferenceMask(u16);

impl DifferenceMask {
    pub(super) fn record(&mut self, bit: u16, differs: bool) {
        if differs {
            self.0 |= bit;
        }
    }

    const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub(super) fn count(self) -> usize {
        self.0.count_ones() as usize
    }

    pub(super) const fn is_only(self, bit: u16) -> bool {
        self.0 == bit
    }
}

fn compare_reference_scalars(
    price_quantum: i64,
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
            quantize_validation_price(price_quantum, actual.weighted_bid_price_units),
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
            quantize_validation_price(price_quantum, actual.weighted_ask_price_units),
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

#[cfg(test)]
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

pub(super) fn compare_candidate(
    ctx: CandidateContext<'_>,
    reference: &mut ReferenceSnapshot,
    cache: &mut CandidateCache,
    counters: &mut CandidateCounters,
    anchor: ValidationAnchor,
    candidate_time_ns: Option<i64>,
) -> Result<(), ProductionError> {
    if reference.state.matched || reference.state.finalized {
        return Ok(());
    }
    let Some(expected) = reference.view.as_ref() else {
        return Ok(());
    };
    // Close projections and closing-price reconciliation have independent
    // audit state. They neither use nor modify the ordinary candidate cache.
    let is_close = anchor == ValidationAnchor::MarketClose;
    let projection = if is_close && ctx.market == Market::Szse && !ctx.rules.is_etf {
        let missing = DayLimits::default();
        let limits = ctx.limits.unwrap_or(&missing);
        match limits.unlimited() {
            Ok(false) => Ok(None),
            Ok(true) => ctx
                .close_price
                .and_then(|tracker| tracker.range_base.as_ref())
                .ok_or_else(|| "missing pre-14:57 successful trade for SZ E0 range".to_owned())
                .and_then(|base| {
                    close_range::project(ctx.book, base, limits)
                        .map(Some)
                        .map_err(|e| e.to_string())
                }),
            Err(error) => Err(error),
        }
    } else {
        Ok(None)
    };
    let (mut close_view, close_price_band, projected) = match projection {
        Ok(Some((view, audit))) => (Some(view), Some(audit), true),
        Ok(None) if is_close => (Some(scalars(ctx)?), None, false),
        Ok(None) => (None, None, false),
        Err(error) => {
            reference.state.diagnostics_mut().comparison_error = Some(error);
            reference.state.finalized = true;
            return Ok(());
        }
    };
    if !is_close {
        cache.prepare(ctx, counters)?;
    }
    let actual = if let Some(view) = close_view.as_mut() {
        view
    } else {
        cache
            .view
            .as_mut()
            .ok_or(ProductionError::Arithmetic("missing candidate cache"))?
    };
    let mut mask = compare_reference_scalars(ctx.rules.price_quantum, expected, actual);
    let turnover_precision = turnover_precision(
        reference.missing_reception,
        expected.turnover_units,
        actual.turnover_units,
    );
    if turnover_precision.is_some() {
        mask.0 &= !DIFF_TURNOVER;
    }
    if !is_close
        && !mask.is_empty()
        && reference
            .state
            .best_differences()
            .is_some_and(|best| mask.count() >= best.len())
    {
        #[cfg(feature = "profiling")]
        {
            counters.scalar_rejected_candidates += 1;
        }
        return Ok(());
    }
    if is_close || !cache.depth_ready {
        if !projected {
            actual.fill_depth(ctx.book, 10)?;
        }
        #[cfg(feature = "profiling")]
        {
            counters.depth_materializations += 1;
        }
        if !is_close {
            cache.depth_ready = true;
        }
    }
    mask.0 |= expected.depth_differences(actual).0;
    let match_tag = if is_close && ctx.market == Market::Szse {
        reconcile_sz_market_close_price(
            ctx.symbol,
            reference.pre_close_price_units,
            ctx.close_price,
            &expected.expand(),
            actual,
            mask,
        )?
    } else {
        None
    };
    if match_tag.is_some() {
        mask = compare_reference_scalars(ctx.rules.price_quantum, expected, actual);
        if turnover_precision.is_some() {
            mask.0 &= !DIFF_TURNOVER;
        }
        mask.0 |= expected.depth_differences(actual).0;
    }
    let best = if !mask.is_empty()
        && reference
            .state
            .best_differences()
            .is_none_or(|best| mask.count() < best.len())
    {
        let mut differences = compare_views(ctx.market, ctx.symbol, &expected.expand(), actual);
        if turnover_precision.is_some() {
            differences.retain(|d| d.field != "turnover_units");
        }
        Some(differences)
    } else {
        None
    };
    let state = &mut reference.state;
    if close_price_band.is_some() {
        state.diagnostics_mut().close_price_band = close_price_band;
    }
    if mask.is_empty() {
        if let Some(audit) = turnover_precision {
            state.diagnostics_mut().turnover_precision = Some(audit);
        }
        state.matched = true;
        state.finalized = true;
        if let Some(d) = state.diagnostics.as_mut() {
            d.best_differences = None;
        }
        if state.diagnostics.as_ref().is_some_and(|d| {
            d.close_price_band.is_none()
                && d.comparison_error.is_none()
                && d.turnover_precision.is_none()
        }) {
            state.diagnostics = None;
        }
        state.matched_candidate_time_ns = candidate_time_ns;
        state.matched_candidate_raw_sequence =
            ctx.book.last_applied_meta().map(|m| m.raw_sequence.get());
        state.matched_candidate_apply_sequence =
            ctx.book.last_applied_meta().map(|m| m.apply_sequence.get());
        state.match_tag = match_tag;
    } else if let Some(best) = best {
        state.diagnostics_mut().best_differences = Some(best);
        state.best_candidate_time_ns = candidate_time_ns;
        state.best_candidate_raw_sequence =
            ctx.book.last_applied_meta().map(|m| m.raw_sequence.get());
    }
    Ok(())
}

pub(super) struct WindowConfig {
    pub pre_open_only: bool,
    pub lookback_ns: i64,
    pub lookahead_ns: i64,
}

impl SymbolValidationState {
    pub fn observe_candidates(
        &mut self,
        ctx: CandidateContext<'_>,
        point: ObservationPoint,
        windows: WindowConfig,
        counters: &mut CandidateCounters,
    ) -> Result<(), ProductionError> {
        let ctx = CandidateContext {
            limits: self.limits.as_ref(),
            close_price: self.close_price.as_ref(),
            ..ctx
        };
        let timed = match point {
            ObservationPoint::BeforeEvent(q) => Some((q, true)),
            ObservationPoint::AfterEvent(q) => Some((q, false)),
            _ => None,
        };
        if let Some(reference) = self.references.pre_open.as_mut() {
            let end = reference
                .time_ns
                .checked_add(REFERENCE_SECOND_NS)
                .ok_or(ProductionError::Arithmetic("reference candidate window"))?;
            if let Some((q, before)) = timed {
                if q >= reference.time_ns && (q < end || before) {
                    let time = if q >= end {
                        end - REFERENCE_MILLISECOND_NS
                    } else {
                        q
                    };
                    compare_candidate(
                        ctx,
                        reference,
                        &mut self.cache,
                        counters,
                        ValidationAnchor::PreOpen,
                        Some(time),
                    )?;
                }
                if q >= end {
                    reference.state.finalized = true;
                }
            } else if point == ObservationPoint::ChannelFinished {
                compare_candidate(
                    ctx,
                    reference,
                    &mut self.cache,
                    counters,
                    ValidationAnchor::PreOpen,
                    Some(end - REFERENCE_MILLISECOND_NS),
                )?;
                reference.state.finalized = true;
            }
        }
        if windows.pre_open_only {
            return Ok(());
        }
        if let Some((q, before)) = timed {
            let mut first_pending = None;
            let mut position = self.continuous_position;
            for reference in &mut self.references.continuous_trading[self.continuous_position..] {
                let start = reference.time_ns.checked_sub(windows.lookback_ns).ok_or(
                    ProductionError::Arithmetic("continuous reference window start"),
                )?;
                if q < start {
                    break;
                }
                let end = reference
                    .time_ns
                    .checked_add(windows.lookahead_ns)
                    .ok_or(ProductionError::Arithmetic("continuous reference second"))?;
                if q < end || before {
                    let time = if q >= end {
                        end - REFERENCE_MILLISECOND_NS
                    } else {
                        q
                    };
                    compare_candidate(
                        ctx,
                        reference,
                        &mut self.cache,
                        counters,
                        ValidationAnchor::ContinuousTrading,
                        Some(time),
                    )?;
                }
                if q >= end {
                    reference.state.finalized = true;
                }
                if !reference.state.finalized {
                    first_pending.get_or_insert(position);
                }
                position += 1;
            }
            self.continuous_position = first_pending.unwrap_or(position);
        } else if let ObservationPoint::MarketClose(boundary) = point {
            if let Some(reference) = self.references.market_close.as_mut() {
                compare_candidate(
                    ctx,
                    reference,
                    &mut self.cache,
                    counters,
                    ValidationAnchor::MarketClose,
                    Some(boundary),
                )?;
                reference.state.finalized = true;
            }
        } else if point == ObservationPoint::ChannelFinished {
            for reference in &mut self.references.continuous_trading[self.continuous_position..] {
                let time = reference
                    .time_ns
                    .checked_add(windows.lookahead_ns - REFERENCE_MILLISECOND_NS)
                    .ok_or(ProductionError::Arithmetic(
                        "continuous reference candidate time",
                    ))?;
                compare_candidate(
                    ctx,
                    reference,
                    &mut self.cache,
                    counters,
                    ValidationAnchor::ContinuousTrading,
                    Some(time),
                )?;
                reference.state.finalized = true;
            }
            self.continuous_position = self.references.continuous_trading.len();
        }
        Ok(())
    }
}
