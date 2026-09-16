use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::{
    AddOrder, BookConfig, BookEvent, BookKey, ChannelId, CrossingBehavior, EventMeta,
    LocalTimestampNs, Market, OrderBook, OrderCancel, OrderId, OrderKey, OrderReference, Price,
    PriceScale, PricingInstruction, Quantity, QuoteTimestampNs, RawSequence, Side, Symbol, Trade,
    UnknownTradePolicy,
};

use super::input::{IngestStats, spool_inputs, spool_inputs_before};
use super::spool::{
    Aggressor, FinishedSpool, SseKind, SseRow, SzExecutionKind, SzExecutionRow, SzOrderKind,
    SzOrderRow, SzSide,
};
use super::writer::SnapshotWriter;
use super::{
    BookSnapshot, MarketDayRequest, PRODUCTION_PRICE_DECIMAL_PLACES, ProductionError, SnapshotKind,
    SnapshotSchedule, SymbolMap, parse_market_timestamp,
};
use crate::production::snapshot::SnapshotCursor;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplayReport {
    /// Original-input inversions, audited before universe filtering.
    #[serde(default)]
    pub sz_sequence_regressions: Vec<super::SequenceRegression>,
    /// Selected channel streams repaired before the existing two-way merge.
    #[serde(default)]
    pub sz_sequence_repairs: Vec<super::SequenceRepair>,
    /// Zero in historical reports; one uses bounded pending-order resolution.
    pub sz_pending_resolution_version: u32,
    pub sz_market_order_policy: super::SzMarketOrderPolicy,
    pub sz_pending_groups: u64,
    pub sz_inferred_market_remainders: u64,
    pub sz_empty_same_side_cancellations: u64,
    pub input_rows: u64,
    pub selected_rows: u64,
    pub excluded_rows: u64,
    pub excluded_after_cutoff_rows: u64,
    pub excluded_non_stock_rows: u64,
    pub excluded_unselected_stock_rows: u64,
    pub channels: usize,
    pub symbols: u64,
    pub applied_events: u64,
    pub status_events: u64,
    pub same_second_quote_time_regressions: u64,
    pub scheduled_snapshots: u64,
    pub market_close_snapshots: u64,
    pub output_rows: u64,
    /// Legacy phase-assisted replay counters, retained for historical report compatibility.
    /// New replay does not read reference snapshots; these four fields stay false/zero.
    pub sz_phase_source_available: bool,
    pub sz_phase_rows: u64,
    pub sz_resumption_checkpoints: usize,
    pub sz_resumption_rest_orders: u64,
    /// Successfully applied SZ limit orders, directly resting irrespective of crossing or phase.
    /// Zero in older reports that used phase-assisted HideIfCrossing.
    pub sz_direct_rest_limit_orders: u64,
    /// Successfully applied SZ events strictly later than 15:00 (quote time, not receipt time).
    pub sz_after_close_events: u64,
    /// Per-symbol counts requiring phase review before E0 validation can pass.
    pub sz_after_close_events_by_symbol: BTreeMap<String, u64>,
}

impl ReplayReport {
    fn observe_sz_applied_event(
        &mut self,
        symbol: &str,
        quote_time_ns: i64,
    ) -> Result<(), ProductionError> {
        const CLOSE: i64 = 15 * 3_600_000_000_000;
        if super::time::time_of_day_nanos(QuoteTimestampNs::from_nanos(quote_time_ns)) > CLOSE {
            self.sz_after_close_events = self
                .sz_after_close_events
                .checked_add(1)
                .ok_or(ProductionError::Arithmetic("SZ after-close event count"))?;
            let count = self
                .sz_after_close_events_by_symbol
                .entry(symbol.to_owned())
                .or_default();
            *count = count.checked_add(1).ok_or(ProductionError::Arithmetic(
                "SZ symbol after-close event count",
            ))?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObservationPoint {
    BeforeEvent(i64),
    AfterEvent(i64),
    MarketClose(i64),
    ChannelFinished,
}

pub(crate) trait StateObserver {
    // Audit metadata only. Pending groups may end in a cancellation, so the
    // final book metadata cannot identify the last successful trade's sequence.
    fn observe_trade_with_sequence(
        &mut self,
        channel: u32,
        symbol: &str,
        _raw_sequence: u64,
        quote_time_ns: i64,
        price_units: i64,
        quantity: u64,
    ) -> Result<(), ProductionError> {
        self.observe_trade(channel, symbol, quote_time_ns, price_units, quantity)
    }
    fn observe_trade(
        &mut self,
        _channel: u32,
        _symbol: &str,
        _quote_time_ns: i64,
        _price_units: i64,
        _quantity: u64,
    ) -> Result<(), ProductionError> {
        Ok(())
    }

    fn observe(
        &mut self,
        channel: u32,
        symbol: &str,
        book: &OrderBook,
        point: ObservationPoint,
    ) -> Result<(), ProductionError>;
}

struct NullObserver;

impl StateObserver for NullObserver {
    fn observe(
        &mut self,
        _channel: u32,
        _symbol: &str,
        _book: &OrderBook,
        _point: ObservationPoint,
    ) -> Result<(), ProductionError> {
        Ok(())
    }
}

pub fn replay_market_day(request: &MarketDayRequest) -> Result<ReplayReport, ProductionError> {
    run_market_day(request, &mut NullObserver)
}

pub(crate) fn run_market_day(
    request: &MarketDayRequest,
    observer: &mut dyn StateObserver,
) -> Result<ReplayReport, ProductionError> {
    let (spool, ingest) = spool_inputs(request)?;
    let result = process_spool(request, &spool, ingest, observer, true);
    finish_spool(result, spool)
}

pub(crate) fn run_market_day_before(
    request: &MarketDayRequest,
    quote_time_exclusive: i64,
    observer: &mut dyn StateObserver,
) -> Result<ReplayReport, ProductionError> {
    let (spool, ingest) = spool_inputs_before(request, Some(quote_time_exclusive))?;
    let result = process_spool(request, &spool, ingest, observer, false);
    finish_spool(result, spool)
}

/// Benchmark-only phase boundaries; the default production API has no clocks.
#[cfg(feature = "profiling")]
pub(crate) fn profile_run_market_day(
    request: &MarketDayRequest,
    observer: &mut dyn StateObserver,
) -> Result<(ReplayReport, [std::time::Duration; 3]), ProductionError> {
    let start = std::time::Instant::now();
    let (spool, ingest) = spool_inputs(request)?;
    let input_elapsed = start.elapsed();
    let start = std::time::Instant::now();
    let result = process_spool(request, &spool, ingest, observer, true);
    let loop_elapsed = start.elapsed();
    let start = std::time::Instant::now();
    let report = finish_spool(result, spool)?;
    Ok((report, [input_elapsed, loop_elapsed, start.elapsed()]))
}

fn finish_spool(
    result: Result<ReplayReport, ProductionError>,
    spool: FinishedSpool,
) -> Result<ReplayReport, ProductionError> {
    match result {
        Ok(report) => {
            spool.cleanup()?;
            Ok(report)
        }
        Err(source) => Err(ProductionError::ReplayFailed {
            spool_path: spool.root().to_path_buf(),
            source: Box::new(source),
        }),
    }
}

fn process_spool(
    request: &MarketDayRequest,
    spool: &FinishedSpool,
    ingest: IngestStats,
    observer: &mut dyn StateObserver,
    complete_day: bool,
) -> Result<ReplayReport, ProductionError> {
    let mut report = ReplayReport {
        sz_sequence_regressions: spool.regressions.clone(),
        sz_sequence_repairs: spool.repairs.clone(),
        sz_pending_resolution_version: 1,
        sz_market_order_policy: request.sz_market_order_policy,
        input_rows: ingest.input_rows,
        selected_rows: ingest.selected_rows,
        excluded_rows: ingest.excluded_rows,
        excluded_after_cutoff_rows: ingest.excluded_after_cutoff_rows,
        excluded_non_stock_rows: ingest.excluded_non_stock_rows,
        excluded_unselected_stock_rows: ingest.excluded_unselected_stock_rows,
        channels: ingest.channels,
        ..ReplayReport::default()
    };
    for channel in spool.channels()? {
        match request.market {
            Market::Sse => process_sse_channel(request, spool, channel, observer, &mut report)?,
            Market::Szse => {
                process_sz_channel(request, spool, channel, observer, &mut report, complete_day)?
            }
        }
    }
    Ok(report)
}

struct BookRuntime {
    book: OrderBook,
    references: HashMap<(Side, u64), OrderKey>,
    cursor: Option<SnapshotCursor>,
    // Product-status timestamps do not establish a business-event watermark.
    last_business_quote_time_ns: Option<i64>,
    market_close_emitted: bool,
    // Capture SH close at its native position, write after EOF drains Scheduled.
    pending_close_snapshot: Option<Box<BookSnapshot>>,
}

impl BookRuntime {
    fn new(
        request: &MarketDayRequest,
        symbol: &str,
        schedule: Option<&SnapshotSchedule>,
    ) -> Result<Self, ProductionError> {
        let book_key = BookKey {
            market: request.market,
            trading_day: request.trading_day,
            symbol: Symbol::from(symbol),
        };
        let price_scale = PriceScale::from_decimal_places(PRODUCTION_PRICE_DECIMAL_PLACES)
            .ok_or_else(|| ProductionError::InvalidRequest("invalid price scale".to_owned()))?;
        let policy = match request.market {
            Market::Sse => UnknownTradePolicy::UpdateKnownAndStatistics,
            Market::Szse => UnknownTradePolicy::Reject,
        };
        Ok(Self {
            book: OrderBook::new(
                BookConfig::new(book_key, price_scale).with_unknown_trade_policy(policy),
            ),
            references: HashMap::new(),
            cursor: schedule
                .map(|schedule| SnapshotCursor::new(request.trading_day, schedule))
                .transpose()?,
            last_business_quote_time_ns: None,
            market_close_emitted: false,
            pending_close_snapshot: None,
        })
    }

    fn check_business_quote_time(
        &mut self,
        symbol: &str,
        quote_time_ns: i64,
    ) -> Result<bool, ProductionError> {
        if let Some(previous) = self.last_business_quote_time_ns {
            if quote_time_ns < previous {
                if !same_second_regression_is_allowed(
                    previous,
                    quote_time_ns,
                    self.cursor.is_some(),
                ) {
                    return Err(ProductionError::QuoteTimeRegression {
                        symbol: Symbol::from(symbol),
                        previous,
                        current: quote_time_ns,
                    });
                }
                return Ok(true);
            }
        }
        self.last_business_quote_time_ns = Some(quote_time_ns);
        Ok(false)
    }

    fn resolve_history(&self, side: Side, order_no: u64) -> Option<OrderKey> {
        self.references.get(&(side, order_no)).copied()
    }

    fn active_reference(&self, side: Side, order_no: u64) -> Option<OrderKey> {
        self.resolve_history(side, order_no)
            .filter(|key| self.book.order(key).is_some())
    }

    fn active_reference_with_remaining(
        &self,
        side: Side,
        order_no: u64,
    ) -> Option<(OrderKey, u64)> {
        let key = self.resolve_history(side, order_no)?;
        let order = self.book.order(&key)?;
        Some((key, order.remaining_quantity))
    }
}

fn same_second_regression_is_allowed(
    previous_time_ns: i64,
    current_time_ns: i64,
    has_snapshot_cursor: bool,
) -> bool {
    !has_snapshot_cursor
        && current_time_ns < previous_time_ns
        && previous_time_ns.div_euclid(1_000_000_000) == current_time_ns.div_euclid(1_000_000_000)
}

fn process_sse_channel(
    request: &MarketDayRequest,
    spool: &FinishedSpool,
    channel: u32,
    observer: &mut dyn StateObserver,
    report: &mut ReplayReport,
) -> Result<(), ProductionError> {
    let mut reader = spool.sse_reader(channel)?;
    let mut runtimes: SymbolMap<BookRuntime> = SymbolMap::default();
    let mut writer = snapshot_writer(request, channel)?;
    while let Some(row) = reader.next_row()? {
        let runtime = runtime(request, &mut runtimes, &row.symbol)?;
        match row.kind {
            SseKind::Status => {
                // Auction orders can be published after CCALL with an earlier
                // TickTime. Keep native BizIndex order, but never let S advance
                // scheduled snapshots or expire ordinary validation windows.
                report.status_events += 1;
                if row.status == 2 && !runtime.market_close_emitted {
                    emit_market_close(
                        request,
                        channel,
                        &row.symbol,
                        row.quote_time_ns,
                        runtime,
                        writer.as_mut(),
                        observer,
                        report,
                    )?;
                }
            }
            SseKind::Add | SseKind::Delete | SseKind::Trade => {
                if runtime.market_close_emitted {
                    return Err(normalize_error(
                        &row.symbol,
                        row.sequence,
                        "business event after Shanghai CLOSE checkpoint",
                    ));
                }
                before_business_event(
                    request,
                    channel,
                    &row.symbol,
                    row.quote_time_ns,
                    runtime,
                    writer.as_mut(),
                    observer,
                    report,
                )?;
                apply_sse_row(runtime, &row)?;
                report.applied_events += 1;
                observer.observe(
                    channel,
                    &row.symbol,
                    &runtime.book,
                    ObservationPoint::AfterEvent(row.quote_time_ns),
                )?;
            }
        }
    }
    finish_channel(
        request,
        channel,
        &mut runtimes,
        writer.as_mut(),
        observer,
        report,
        false,
    )?;
    report.symbols = report
        .symbols
        .checked_add(runtimes.len() as u64)
        .ok_or(ProductionError::Arithmetic("symbol count"))?;
    if let Some(writer) = writer {
        report.output_rows = report
            .output_rows
            .checked_add(writer.close()?)
            .ok_or(ProductionError::Arithmetic("output row count"))?;
    }
    Ok(())
}

fn process_sz_channel(
    request: &MarketDayRequest,
    spool: &FinishedSpool,
    channel: u32,
    observer: &mut dyn StateObserver,
    report: &mut ReplayReport,
    complete_day: bool,
) -> Result<(), ProductionError> {
    let mut orders = spool.sz_order_reader(channel)?;
    let mut executions = spool.sz_execution_reader(channel)?;
    let mut order = orders
        .as_mut()
        .map(|reader| reader.next_row())
        .transpose()?
        .flatten();
    let mut execution = executions
        .as_mut()
        .map(|reader| reader.next_row())
        .transpose()?
        .flatten();
    let mut runtimes: SymbolMap<BookRuntime> = SymbolMap::default();
    let mut writer = snapshot_writer(request, channel)?;
    while order.is_some() || execution.is_some() {
        let take_order = match (&order, &execution) {
            (Some(left), Some(right)) if left.sequence < right.sequence => true,
            (Some(left), Some(right)) if left.sequence > right.sequence => false,
            (Some(left), Some(_)) => {
                return Err(ProductionError::AmbiguousSequence {
                    channel,
                    sequence: left.sequence,
                });
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_order {
            let row = order.take().ok_or_else(|| {
                ProductionError::InvalidRequest("missing Shenzhen order cursor".to_owned())
            })?;
            order = orders
                .as_mut()
                .map(|reader| reader.next_row())
                .transpose()?
                .flatten();
            let runtime = runtime(request, &mut runtimes, &row.symbol)?;
            let side = sz_side(row.side);
            let same_side_price_available =
                row.kind == SzOrderKind::SameSideBest && runtime.book.best_level(side).is_some();
            if (row.kind == SzOrderKind::Market
                && request.sz_market_order_policy
                    != super::SzMarketOrderPolicy::RestAtLastTradePrice)
                || (row.kind == SzOrderKind::SameSideBest && !same_side_price_available)
            {
                let opposite_best = runtime.book.best_level(match side {
                    Side::Buy => Side::Sell,
                    Side::Sell => Side::Buy,
                });
                let mut pending = super::sz_pending::PendingOrder::new(
                    &row,
                    opposite_best.map(|p| p.price.units()),
                );
                let mut responses = Vec::new();
                while let Some(next) = execution.as_ref() {
                    if order.as_ref().is_some_and(|o| o.sequence == next.sequence) {
                        return Err(ProductionError::AmbiguousSequence {
                            channel,
                            sequence: next.sequence,
                        });
                    }
                    if pending.terminal()
                        || order.as_ref().is_some_and(|o| o.sequence < next.sequence)
                        || !pending.references(next)
                    {
                        break;
                    }
                    pending.observe(next)?;
                    responses.push(
                        execution
                            .take()
                            .ok_or_else(|| pending.error("missing execution cursor"))?,
                    );
                    execution = executions
                        .as_mut()
                        .map(|reader| reader.next_row())
                        .transpose()?
                        .flatten();
                }
                let next_sequence = order
                    .as_ref()
                    .map(|o| o.sequence)
                    .into_iter()
                    .chain(execution.as_ref().map(|e| e.sequence))
                    .min();
                let disposition = pending.finish(request.sz_market_order_policy, next_sequence)?;
                // Classification happens before any book mutation. All responses
                // have the order's quote time; do not expose speculative states.
                before_business_event(
                    request,
                    channel,
                    &row.symbol,
                    row.quote_time_ns,
                    runtime,
                    writer.as_mut(),
                    observer,
                    report,
                )?;
                process_sz_order(
                    request,
                    channel,
                    &row,
                    runtime,
                    same_side_price_available,
                    None,
                    &mut NullObserver,
                    report,
                )?;
                for response in &responses {
                    process_sz_execution(
                        request,
                        channel,
                        response,
                        runtime,
                        None,
                        &mut NullObserver,
                        report,
                    )?;
                }
                if let super::sz_pending::Disposition::Rest(units) = disposition {
                    let key = order_key(channel, side, row.sequence, &row.symbol, row.sequence)?;
                    runtime
                        .book
                        .rest_pending_order(key, price(units, &row.symbol, row.sequence)?)
                        .map_err(|source| ProductionError::Apply {
                            symbol: Symbol::from(row.symbol.as_str()),
                            sequence: row.sequence,
                            source,
                        })?;
                    report.sz_inferred_market_remainders += 1;
                }
                // Close-price audit observes trades only after group success.
                for response in &responses {
                    if response.kind == SzExecutionKind::Trade {
                        observer.observe_trade_with_sequence(
                            channel,
                            &row.symbol,
                            response.sequence,
                            response.quote_time_ns,
                            response.price_units,
                            response.quantity,
                        )?;
                    }
                }
                report.sz_pending_groups += 1;
                if row.kind == SzOrderKind::SameSideBest {
                    report.sz_empty_same_side_cancellations += 1;
                }
                observer.observe(
                    channel,
                    &row.symbol,
                    &runtime.book,
                    ObservationPoint::AfterEvent(row.quote_time_ns),
                )?;
            } else {
                process_sz_order(
                    request,
                    channel,
                    &row,
                    runtime,
                    same_side_price_available,
                    writer.as_mut(),
                    observer,
                    report,
                )?;
            }
        } else {
            let row = execution.take().ok_or_else(|| {
                ProductionError::InvalidRequest("missing Shenzhen execution cursor".to_owned())
            })?;
            process_sz_execution(
                request,
                channel,
                &row,
                runtime(request, &mut runtimes, &row.symbol)?,
                writer.as_mut(),
                observer,
                report,
            )?;
            execution = executions
                .as_mut()
                .map(|reader| reader.next_row())
                .transpose()?
                .flatten();
        }
    }
    finish_channel(
        request,
        channel,
        &mut runtimes,
        writer.as_mut(),
        observer,
        report,
        complete_day,
    )?;
    report.symbols = report
        .symbols
        .checked_add(runtimes.len() as u64)
        .ok_or(ProductionError::Arithmetic("symbol count"))?;
    if let Some(writer) = writer {
        report.output_rows = report
            .output_rows
            .checked_add(writer.close()?)
            .ok_or(ProductionError::Arithmetic("output row count"))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn process_sz_order(
    request: &MarketDayRequest,
    channel: u32,
    row: &SzOrderRow,
    runtime: &mut BookRuntime,
    same_side_price_available: bool,
    writer: Option<&mut SnapshotWriter>,
    observer: &mut dyn StateObserver,
    report: &mut ReplayReport,
) -> Result<(), ProductionError> {
    before_business_event(
        request,
        channel,
        &row.symbol,
        row.quote_time_ns,
        runtime,
        writer,
        observer,
        report,
    )?;
    let side = sz_side(row.side);
    let key = order_key(channel, side, row.sequence, &row.symbol, row.sequence)?;
    let pricing = match row.kind {
        SzOrderKind::Market => PricingInstruction::Unpriced,
        SzOrderKind::Limit => {
            PricingInstruction::Provided(price(row.price_units, &row.symbol, row.sequence)?)
        }
        SzOrderKind::SameSideBest if same_side_price_available => PricingInstruction::SameSideBest,
        SzOrderKind::SameSideBest => PricingInstruction::Unpriced,
    };
    let crossing = match row.kind {
        SzOrderKind::Market
            if request.sz_market_order_policy
                == super::SzMarketOrderPolicy::RestAtLastTradePrice =>
        {
            CrossingBehavior::RestAtLastTradePrice
        }
        SzOrderKind::Market => CrossingBehavior::AlwaysHide,
        SzOrderKind::SameSideBest if !same_side_price_available => CrossingBehavior::AlwaysHide,
        SzOrderKind::SameSideBest => CrossingBehavior::Rest,
        // A crossing price is not evidence of a fill or of invisibility. Keep
        // every priced limit order until source executions/cancellations reduce
        // it, including untraded resumption-auction orders. Intermediate replay
        // states may cross; reference snapshot phases must not drive this ledger.
        SzOrderKind::Limit => CrossingBehavior::Rest,
    };
    let event = BookEvent::AddOrder(AddOrder {
        meta: meta(runtime, row.sequence, row.local_time_ns, row.quote_time_ns)?,
        order_key: key,
        pricing,
        crossing,
        quantity: quantity(row.quantity, &row.symbol, row.sequence)?,
    });
    apply(runtime, event, &row.symbol, row.sequence)?;
    report.observe_sz_applied_event(&row.symbol, row.quote_time_ns)?;
    if row.kind == SzOrderKind::Limit {
        report.sz_direct_rest_limit_orders += 1;
    }
    runtime.references.insert((side, row.sequence), key);
    report.applied_events += 1;
    observer.observe(
        channel,
        &row.symbol,
        &runtime.book,
        ObservationPoint::AfterEvent(row.quote_time_ns),
    )
}

fn process_sz_execution(
    request: &MarketDayRequest,
    channel: u32,
    row: &SzExecutionRow,
    runtime: &mut BookRuntime,
    writer: Option<&mut SnapshotWriter>,
    observer: &mut dyn StateObserver,
    report: &mut ReplayReport,
) -> Result<(), ProductionError> {
    before_business_event(
        request,
        channel,
        &row.symbol,
        row.quote_time_ns,
        runtime,
        writer,
        observer,
        report,
    )?;
    let event = match row.kind {
        SzExecutionKind::Trade => BookEvent::Trade(Trade {
            meta: meta(runtime, row.sequence, row.local_time_ns, row.quote_time_ns)?,
            bid_order: sz_reference(runtime, Side::Buy, row.bid_order_no),
            ask_order: sz_reference(runtime, Side::Sell, row.ask_order_no),
            price: price(row.price_units, &row.symbol, row.sequence)?,
            quantity: quantity(row.quantity, &row.symbol, row.sequence)?,
        }),
        SzExecutionKind::Cancel => {
            let (side, order_no) = one_sided_reference(
                row.bid_order_no,
                row.ask_order_no,
                &row.symbol,
                row.sequence,
            )?;
            let (key, remaining) = runtime
                .active_reference_with_remaining(side, order_no)
                .ok_or_else(|| {
                    normalize_error(&row.symbol, row.sequence, "unknown cancellation target")
                })?;
            if remaining != row.quantity {
                return Err(normalize_error(
                    &row.symbol,
                    row.sequence,
                    format!(
                        "cancellation quantity {} does not equal remaining {remaining}",
                        row.quantity
                    ),
                ));
            }
            BookEvent::OrderCancel(OrderCancel {
                meta: meta(runtime, row.sequence, row.local_time_ns, row.quote_time_ns)?,
                order_key: key,
            })
        }
    };
    apply(runtime, event, &row.symbol, row.sequence)?;
    report.observe_sz_applied_event(&row.symbol, row.quote_time_ns)?;
    if row.kind == SzExecutionKind::Trade {
        observer.observe_trade_with_sequence(
            channel,
            &row.symbol,
            row.sequence,
            row.quote_time_ns,
            row.price_units,
            row.quantity,
        )?;
    }
    report.applied_events += 1;
    observer.observe(
        channel,
        &row.symbol,
        &runtime.book,
        ObservationPoint::AfterEvent(row.quote_time_ns),
    )
}

#[allow(clippy::too_many_arguments)]
fn before_business_event(
    request: &MarketDayRequest,
    channel: u32,
    symbol: &str,
    quote_time_ns: i64,
    runtime: &mut BookRuntime,
    mut writer: Option<&mut SnapshotWriter>,
    observer: &mut dyn StateObserver,
    report: &mut ReplayReport,
) -> Result<(), ProductionError> {
    if runtime.check_business_quote_time(symbol, quote_time_ns)? {
        report.same_second_quote_time_regressions = report
            .same_second_quote_time_regressions
            .checked_add(1)
            .ok_or(ProductionError::Arithmetic(
                "same-second quote-time regression count",
            ))?;
    }
    if let (Some(cursor), Some(output)) = (&mut runtime.cursor, writer.as_mut()) {
        let depth = request
            .snapshots
            .as_ref()
            .map_or(10, |schedule| schedule.depth);
        for boundary in cursor.drain_due(quote_time_ns)? {
            output.push(BookSnapshot::capture(
                &runtime.book,
                channel,
                SnapshotKind::Scheduled,
                boundary,
                depth,
            )?)?;
            report.scheduled_snapshots += 1;
        }
    }
    observer.observe(
        channel,
        symbol,
        &runtime.book,
        ObservationPoint::BeforeEvent(quote_time_ns),
    )
}

fn finish_channel(
    request: &MarketDayRequest,
    channel: u32,
    runtimes: &mut SymbolMap<BookRuntime>,
    mut writer: Option<&mut SnapshotWriter>,
    observer: &mut dyn StateObserver,
    report: &mut ReplayReport,
    emit_sz_close: bool,
) -> Result<(), ProductionError> {
    let depth = request
        .snapshots
        .as_ref()
        .map_or(10, |schedule| schedule.depth);
    let close = parse_market_timestamp(request.trading_day, "15:00:00.000")?;
    let mut symbols = runtimes.keys().cloned().collect::<Vec<_>>();
    symbols.sort_unstable();
    for symbol in symbols {
        let runtime = runtimes.get_mut(&symbol).ok_or_else(|| {
            ProductionError::InvalidRequest("missing order-book runtime at channel end".to_owned())
        })?;
        if let (Some(cursor), Some(output)) = (&mut runtime.cursor, writer.as_deref_mut()) {
            for boundary in cursor.finish()? {
                output.push(BookSnapshot::capture(
                    &runtime.book,
                    channel,
                    SnapshotKind::Scheduled,
                    boundary,
                    depth,
                )?)?;
                report.scheduled_snapshots += 1;
            }
        }
        if emit_sz_close && !runtime.market_close_emitted {
            // E0 is on a separate feed, so only exhausting both native-sequence
            // streams completes this offline replay. Never finalize before a
            // later event. A post-15:00 final state requires phase review.
            let boundary = close.max(runtime.last_business_quote_time_ns.unwrap_or(close));
            emit_market_close(
                request,
                channel,
                &symbol,
                boundary,
                runtime,
                writer.as_deref_mut(),
                observer,
                report,
            )?;
        }
        if let (Some(snapshot), Some(output)) =
            (runtime.pending_close_snapshot.take(), writer.as_deref_mut())
        {
            output.push(*snapshot)?;
        }
        observer.observe(
            channel,
            &symbol,
            &runtime.book,
            ObservationPoint::ChannelFinished,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_market_close(
    request: &MarketDayRequest,
    channel: u32,
    symbol: &str,
    boundary_time_ns: i64,
    runtime: &mut BookRuntime,
    writer: Option<&mut SnapshotWriter>,
    observer: &mut dyn StateObserver,
    report: &mut ReplayReport,
) -> Result<(), ProductionError> {
    if runtime.market_close_emitted {
        return Ok(());
    }
    if let Some(output) = writer {
        let depth = request
            .snapshots
            .as_ref()
            .map_or(10, |schedule| schedule.depth);
        let snapshot = BookSnapshot::capture(
            &runtime.book,
            channel,
            SnapshotKind::MarketClose,
            boundary_time_ns,
            depth,
        )?;
        if request.market == Market::Sse {
            runtime.pending_close_snapshot = Some(Box::new(snapshot));
        } else {
            output.push(snapshot)?;
        }
    }
    observer.observe(
        channel,
        symbol,
        &runtime.book,
        ObservationPoint::MarketClose(boundary_time_ns),
    )?;
    runtime.market_close_emitted = true;
    report.market_close_snapshots += 1;
    Ok(())
}

fn apply_sse_row(runtime: &mut BookRuntime, row: &SseRow) -> Result<(), ProductionError> {
    let event = match row.kind {
        SseKind::Add => {
            let (side, order_no) = one_sided_reference(
                row.buy_order_no,
                row.sell_order_no,
                &row.symbol,
                row.sequence,
            )?;
            if !matches!(
                (side, row.flag),
                (Side::Buy, Aggressor::Buy) | (Side::Sell, Aggressor::Sell)
            ) {
                return Err(normalize_error(
                    &row.symbol,
                    row.sequence,
                    "add side does not match TickBSFlag",
                ));
            }
            let key = order_key(row.channel, side, order_no, &row.symbol, row.sequence)?;
            let event = BookEvent::AddOrder(AddOrder {
                meta: meta(runtime, row.sequence, row.local_time_ns, row.quote_time_ns)?,
                order_key: key,
                pricing: PricingInstruction::Provided(price(
                    row.price_units,
                    &row.symbol,
                    row.sequence,
                )?),
                crossing: CrossingBehavior::Rest,
                quantity: quantity(row.quantity, &row.symbol, row.sequence)?,
            });
            apply(runtime, event, &row.symbol, row.sequence)?;
            runtime.references.insert((side, order_no), key);
            return Ok(());
        }
        SseKind::Delete => {
            let (side, order_no) = one_sided_reference(
                row.buy_order_no,
                row.sell_order_no,
                &row.symbol,
                row.sequence,
            )?;
            let key = runtime.active_reference(side, order_no).ok_or_else(|| {
                normalize_error(&row.symbol, row.sequence, "unknown deletion target")
            })?;
            BookEvent::OrderCancel(OrderCancel {
                meta: meta(runtime, row.sequence, row.local_time_ns, row.quote_time_ns)?,
                order_key: key,
            })
        }
        SseKind::Trade => BookEvent::Trade(Trade {
            meta: meta(runtime, row.sequence, row.local_time_ns, row.quote_time_ns)?,
            bid_order: sse_trade_reference(runtime, Side::Buy, row, row.buy_order_no)?,
            ask_order: sse_trade_reference(runtime, Side::Sell, row, row.sell_order_no)?,
            price: price(row.price_units, &row.symbol, row.sequence)?,
            quantity: quantity(row.quantity, &row.symbol, row.sequence)?,
        }),
        SseKind::Status => {
            return Err(normalize_error(
                &row.symbol,
                row.sequence,
                "status row cannot be applied",
            ));
        }
    };
    apply(runtime, event, &row.symbol, row.sequence)
}

fn sse_trade_reference(
    runtime: &BookRuntime,
    side: Side,
    row: &SseRow,
    order_no: u64,
) -> Result<OrderReference, ProductionError> {
    if order_no == 0 {
        return if matches!(
            (side, row.flag),
            (Side::Buy, Aggressor::Buy) | (Side::Sell, Aggressor::Sell)
        ) {
            Ok(OrderReference::Absent)
        } else {
            Err(normalize_error(
                &row.symbol,
                row.sequence,
                "passive or neutral trade reference is absent",
            ))
        };
    }
    if let Some(key) = runtime.active_reference(side, order_no) {
        return Ok(OrderReference::Resolved(key));
    }
    let aggressive = matches!(
        (side, row.flag),
        (Side::Buy, Aggressor::Buy) | (Side::Sell, Aggressor::Sell)
    );
    if aggressive {
        let id = OrderId::new(order_no).ok_or_else(|| {
            normalize_error(&row.symbol, row.sequence, "trade order id must be positive")
        })?;
        Ok(OrderReference::Unresolved { side, order_id: id })
    } else {
        Err(normalize_error(
            &row.symbol,
            row.sequence,
            format!("unknown passive {side:?} order {order_no}"),
        ))
    }
}

fn sz_reference(runtime: &BookRuntime, side: Side, order_no: u64) -> OrderReference {
    if order_no == 0 {
        OrderReference::Absent
    } else if let Some(key) = runtime.resolve_history(side, order_no) {
        OrderReference::Resolved(key)
    } else {
        OrderId::new(order_no).map_or(OrderReference::Absent, |order_id| {
            OrderReference::Unresolved { side, order_id }
        })
    }
}

fn runtime<'a>(
    request: &MarketDayRequest,
    runtimes: &'a mut SymbolMap<BookRuntime>,
    symbol: &str,
) -> Result<&'a mut BookRuntime, ProductionError> {
    use hashbrown::hash_map::EntryRef;

    match runtimes.entry_ref(symbol) {
        EntryRef::Occupied(entry) => Ok(entry.into_mut()),
        EntryRef::Vacant(entry) => {
            let runtime = BookRuntime::new(request, symbol, request.snapshots.as_ref())?;
            Ok(entry.insert(runtime))
        }
    }
}

fn snapshot_writer(
    request: &MarketDayRequest,
    channel: u32,
) -> Result<Option<SnapshotWriter>, ProductionError> {
    request
        .snapshots
        .as_ref()
        .map(|schedule| {
            SnapshotWriter::create(
                &request.output_root,
                request.trading_day,
                request.market,
                channel,
                schedule.depth,
            )
        })
        .transpose()
}

fn meta(
    runtime: &BookRuntime,
    raw_sequence: u64,
    local_time_ns: i64,
    quote_time_ns: i64,
) -> Result<EventMeta, ProductionError> {
    Ok(EventMeta {
        book_key: runtime.book.config().book_key.clone(),
        raw_sequence: RawSequence::new(raw_sequence).ok_or_else(|| {
            normalize_error(
                runtime.book.config().book_key.symbol.as_str(),
                raw_sequence,
                "raw sequence must be positive",
            )
        })?,
        apply_sequence: runtime.book.next_apply_sequence().map_err(|source| {
            ProductionError::Apply {
                symbol: runtime.book.config().book_key.symbol.clone(),
                sequence: raw_sequence,
                source,
            }
        })?,
        local_time: LocalTimestampNs::from_nanos(local_time_ns),
        quote_time: QuoteTimestampNs::from_nanos(quote_time_ns),
    })
}

fn apply(
    runtime: &mut BookRuntime,
    event: BookEvent,
    symbol: &str,
    sequence: u64,
) -> Result<(), ProductionError> {
    runtime
        .book
        .apply(event)
        .map(|_| ())
        .map_err(|source| ProductionError::Apply {
            symbol: Symbol::from(symbol),
            sequence,
            source,
        })
}

fn order_key(
    channel: u32,
    side: Side,
    order_no: u64,
    symbol: &str,
    sequence: u64,
) -> Result<OrderKey, ProductionError> {
    Ok(OrderKey {
        channel_id: ChannelId::new(channel)
            .ok_or_else(|| normalize_error(symbol, sequence, "channel must be positive"))?,
        side,
        order_id: OrderId::new(order_no)
            .ok_or_else(|| normalize_error(symbol, sequence, "order id must be positive"))?,
    })
}

fn one_sided_reference(
    bid: u64,
    ask: u64,
    symbol: &str,
    sequence: u64,
) -> Result<(Side, u64), ProductionError> {
    match (bid, ask) {
        (value, 0) if value > 0 => Ok((Side::Buy, value)),
        (0, value) if value > 0 => Ok((Side::Sell, value)),
        _ => Err(normalize_error(
            symbol,
            sequence,
            "expected exactly one nonzero order reference",
        )),
    }
}

fn price(value: i64, symbol: &str, sequence: u64) -> Result<Price, ProductionError> {
    Price::from_units(value).ok_or_else(|| {
        normalize_error(symbol, sequence, format!("price must be positive: {value}"))
    })
}

fn quantity(value: u64, symbol: &str, sequence: u64) -> Result<Quantity, ProductionError> {
    Quantity::new(value)
        .ok_or_else(|| normalize_error(symbol, sequence, "quantity must be positive"))
}

fn normalize_error(symbol: &str, sequence: u64, detail: impl Into<String>) -> ProductionError {
    ProductionError::Normalize {
        symbol: Symbol::from(symbol),
        sequence,
        detail: detail.into(),
    }
}

fn sz_side(side: SzSide) -> Side {
    match side {
        SzSide::Buy => Side::Buy,
        SzSide::Sell => Side::Sell,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use super::{SymbolMap, one_sided_reference, runtime, same_second_regression_is_allowed};
    use crate::{
        Market, MarketDayRequest, ProductionError, Side, SnapshotSchedule, SzMarketOrderPolicy,
        TargetUniverse, TradingDay,
    };

    fn runtime_request() -> MarketDayRequest {
        MarketDayRequest {
            raw_root: Default::default(),
            output_root: Default::default(),
            temp_root: Default::default(),
            trading_day: TradingDay::from_yyyymmdd(20_260_828).expect("valid test day"),
            market: Market::Szse,
            targets: TargetUniverse::AllStocks,
            snapshots: None,
            batch_size: 1,
            sz_market_order_policy: SzMarketOrderPolicy::RequireEvidence,
        }
    }

    #[test]
    fn runtime_reuses_existing_state_and_keeps_symbols_independent() {
        let request = runtime_request();
        let mut runtimes = SymbolMap::default();
        let first = runtime(&request, &mut runtimes, "000001").expect("create runtime");
        first.last_business_quote_time_ns = Some(123);
        first.market_close_emitted = true;

        let existing = runtime(&request, &mut runtimes, "000001").expect("reuse runtime");
        assert_eq!(existing.last_business_quote_time_ns, Some(123));
        assert!(existing.market_close_emitted);
        let second = runtime(&request, &mut runtimes, "000002").expect("independent runtime");
        assert_eq!(second.last_business_quote_time_ns, None);
        assert!(!second.market_close_emitted);
        assert_eq!(runtimes.len(), 2);
    }

    #[test]
    fn runtime_creation_failure_does_not_insert_or_reinitialize_existing_state() {
        let mut request = runtime_request();
        let mut runtimes = SymbolMap::default();
        runtime(&request, &mut runtimes, "000001")
            .expect("create runtime")
            .last_business_quote_time_ns = Some(123);
        // Public fields allow this invalid schedule; cursor creation must fail.
        request.snapshots = Some(SnapshotSchedule {
            interval: Duration::from_secs(u64::MAX),
            depth: 10,
        });
        assert!(matches!(
            runtime(&request, &mut runtimes, "000002"),
            Err(ProductionError::Arithmetic(_))
        ));
        assert_eq!(runtimes.len(), 1);
        assert!(!runtimes.contains_key("000002"));
        let existing = runtime(&request, &mut runtimes, "000001").expect("reuse runtime");
        assert_eq!(existing.last_business_quote_time_ns, Some(123));
        assert!(existing.cursor.is_none());

        request.snapshots =
            Some(SnapshotSchedule::new(Duration::from_secs(30), 10).expect("valid schedule"));
        assert!(
            runtime(&request, &mut runtimes, "000002")
                .expect("retry creation")
                .cursor
                .is_some()
        );
        assert_eq!(runtimes.len(), 2);
    }

    #[test]
    fn sz_cancel_lookup_retains_unknown_target_errors_and_current_remaining() {
        let request = runtime_request();
        let mut runtimes = SymbolMap::default();
        let runtime = runtime(&request, &mut runtimes, "000001").expect("runtime");
        let mut report = super::ReplayReport::default();
        let row = super::SzOrderRow {
            source_row: 1,
            sequence: 1,
            channel: 1,
            symbol: "000001".try_into().expect("symbol"),
            quote_time_ns: 100,
            local_time_ns: 200,
            price_units: 100_000,
            quantity: 10,
            side: super::SzSide::Buy,
            kind: super::SzOrderKind::Limit,
        };
        super::process_sz_order(
            &request,
            1,
            &row,
            runtime,
            false,
            None,
            &mut super::NullObserver,
            &mut report,
        )
        .expect("add");
        let (key, remaining) = runtime
            .active_reference_with_remaining(Side::Buy, 1)
            .expect("active");
        assert_eq!(remaining, 10);
        assert_eq!(runtime.active_reference(Side::Buy, 1), Some(key));
        let mut cancel = super::SzExecutionRow {
            source_row: 2,
            sequence: 2,
            channel: 1,
            symbol: row.symbol,
            quote_time_ns: 100,
            local_time_ns: 201,
            bid_order_no: 1,
            ask_order_no: 0,
            price_units: 0,
            quantity: 9,
            kind: super::SzExecutionKind::Cancel,
        };
        let err = super::process_sz_execution(
            &request,
            1,
            &cancel,
            runtime,
            None,
            &mut super::NullObserver,
            &mut report,
        )
        .expect_err("wrong quantity");
        assert!(
            err.to_string()
                .contains("cancellation quantity 9 does not equal remaining 10")
        );
        assert_eq!(
            runtime.active_reference_with_remaining(Side::Buy, 1),
            Some((key, 10))
        );
        cancel.quantity = 10;
        super::process_sz_execution(
            &request,
            1,
            &cancel,
            runtime,
            None,
            &mut super::NullObserver,
            &mut report,
        )
        .expect("cancel");
        assert_eq!(runtime.resolve_history(Side::Buy, 1), Some(key));
        assert_eq!(runtime.active_reference_with_remaining(Side::Buy, 1), None);
        for order_no in [1, 999] {
            cancel.bid_order_no = order_no;
            cancel.sequence = 3;
            let err = super::process_sz_execution(
                &request,
                1,
                &cancel,
                runtime,
                None,
                &mut super::NullObserver,
                &mut report,
            )
            .expect_err("inactive or missing");
            assert!(err.to_string().contains("unknown cancellation target"));
        }
        assert_eq!(report.applied_events, 2);
    }

    #[test]
    fn requires_one_cancellation_reference() {
        assert_eq!(
            one_sided_reference(10, 0, "000001", 1).ok(),
            Some((Side::Buy, 10))
        );
        assert!(one_sided_reference(0, 0, "000001", 1).is_err());
        assert!(one_sided_reference(10, 20, "000001", 1).is_err());
    }

    #[test]
    fn only_unscheduled_same_second_quote_time_regressions_are_allowed() {
        let previous = 13 * 3_600_000_000_000 + 10 * 60_000_000_000 + 60_000_000;
        let current = previous - 10_000_000;
        assert!(same_second_regression_is_allowed(previous, current, false));
        assert!(!same_second_regression_is_allowed(previous, current, true));
        assert!(!same_second_regression_is_allowed(
            13 * 3_600_000_000_000,
            13 * 3_600_000_000_000 - 1_000_000,
            false,
        ));
    }

    #[derive(Default)]
    struct RecordingObserver(Vec<(super::ObservationPoint, crate::BookSnapshot)>);

    impl super::StateObserver for RecordingObserver {
        fn observe(
            &mut self,
            channel: u32,
            _symbol: &str,
            book: &crate::OrderBook,
            point: super::ObservationPoint,
        ) -> Result<(), ProductionError> {
            self.0.push((
                point,
                crate::BookSnapshot::capture(book, channel, crate::SnapshotKind::Scheduled, 0, 10)?,
            ));
            Ok(())
        }
    }

    fn replay_sse_rows(
        rows: &[(super::SseKind, &str, u8)],
    ) -> (
        Result<(), ProductionError>,
        super::ReplayReport,
        RecordingObserver,
    ) {
        let root = tempfile::TempDir::new().expect("temporary directory");
        let mut request = runtime_request();
        request.market = Market::Sse;
        request.temp_root = root.path().to_path_buf();
        let mut spool =
            crate::production::spool::SpoolSet::create(root.path(), "status-test").expect("spool");
        for (index, &(kind, time, status)) in rows.iter().enumerate() {
            let sequence = index as u64 + 1;
            let time = super::parse_market_timestamp(request.trading_day, time).expect("time");
            spool
                .write_sse(&super::SseRow {
                    source_row: sequence,
                    sequence,
                    channel: 1,
                    symbol: "600000".try_into().expect("symbol"),
                    quote_time_ns: time,
                    local_time_ns: time + 123,
                    kind,
                    buy_order_no: sequence,
                    sell_order_no: 0,
                    price_units: 100_000,
                    quantity: 10,
                    flag: super::Aggressor::Buy,
                    status,
                })
                .expect("row");
        }
        let spool = spool.finish().expect("finished spool");
        let mut report = super::ReplayReport::default();
        let mut observer = RecordingObserver::default();
        let result = super::process_sse_channel(&request, &spool, 1, &mut observer, &mut report);
        (result, report, observer)
    }

    #[test]
    fn sse_status_only_and_repeated_close_do_not_emit_business_observations() {
        use super::{ObservationPoint, SseKind::Status};
        let (result, report, observer) = replay_sse_rows(&[
            (Status, "14:57:01.000", 1),
            (Status, "15:00:01.000", 2),
            (Status, "15:00:02.000", 2),
        ]);
        result.expect("status-only stream");
        assert_eq!(report.status_events, 3);
        assert_eq!(report.applied_events, 0);
        assert_eq!(report.market_close_snapshots, 1);
        assert_eq!(observer.0.len(), 2);
        assert!(matches!(observer.0[0].0, ObservationPoint::MarketClose(_)));
        assert_eq!(observer.0[1].0, ObservationPoint::ChannelFinished);
        for (_, snapshot) in observer.0 {
            assert_eq!(snapshot.last_quote_time_ns, None);
            assert_eq!(snapshot.last_local_time_ns, None);
            assert!(snapshot.book.bids.is_empty());
        }
    }

    #[test]
    fn sse_business_after_close_is_rejected_before_observation_or_mutation() {
        use super::{
            ObservationPoint,
            SseKind::{Add, Status},
        };
        let (result, report, observer) = replay_sse_rows(&[
            (Add, "14:56:00.000", 0),
            (Status, "15:00:01.000", 2),
            (Add, "15:00:02.000", 0),
        ]);
        assert!(matches!(
            result,
            Err(ProductionError::Normalize { sequence: 3, .. })
        ));
        assert_eq!(report.applied_events, 1);
        assert_eq!(observer.0.len(), 3);
        let (point, snapshot) = observer.0.last().expect("close");
        assert!(matches!(point, ObservationPoint::MarketClose(_)));
        assert_eq!(snapshot.book.total_bid_quantity, 10);
        let time = super::parse_market_timestamp(runtime_request().trading_day, "14:56:00.000")
            .expect("time");
        assert_eq!(snapshot.last_quote_time_ns, Some(time));
        assert_eq!(snapshot.last_local_time_ns, Some(time + 123));
    }

    #[test]
    fn sse_cross_second_business_regression_still_fails_without_mutation() {
        use super::{
            ObservationPoint,
            SseKind::{Add, Status},
        };
        let (result, report, observer) = replay_sse_rows(&[
            (Add, "14:57:01.000", 0),
            (Status, "14:57:02.000", 1),
            (Add, "14:57:00.990", 0),
        ]);
        assert!(matches!(
            result,
            Err(ProductionError::QuoteTimeRegression { .. })
        ));
        assert_eq!(report.applied_events, 1);
        assert_eq!(observer.0.len(), 2);
        let (point, snapshot) = observer.0.last().expect("last successful event");
        assert!(matches!(point, ObservationPoint::AfterEvent(_)));
        assert_eq!(snapshot.book.total_bid_quantity, 10);
    }
}
