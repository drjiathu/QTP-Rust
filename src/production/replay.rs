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
    SnapshotSchedule, parse_market_timestamp,
};
use crate::production::snapshot::SnapshotCursor;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplayReport {
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
    pub sz_phase_source_available: bool,
    pub sz_phase_rows: u64,
    pub sz_resumption_checkpoints: usize,
    pub sz_resumption_rest_orders: u64,
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
        input_rows: ingest.input_rows,
        selected_rows: ingest.selected_rows,
        excluded_rows: ingest.excluded_rows,
        excluded_after_cutoff_rows: ingest.excluded_after_cutoff_rows,
        excluded_non_stock_rows: ingest.excluded_non_stock_rows,
        excluded_unselected_stock_rows: ingest.excluded_unselected_stock_rows,
        channels: ingest.channels,
        sz_phase_source_available: ingest.sz_phase_source_available,
        sz_phase_rows: ingest.sz_phase_rows,
        sz_resumption_checkpoints: ingest.sz_resumptions.values().map(Vec::len).sum(),
        ..ReplayReport::default()
    };
    for channel in spool.channels()? {
        match request.market {
            Market::Sse => process_sse_channel(request, spool, channel, observer, &mut report)?,
            Market::Szse => process_sz_channel(
                request,
                spool,
                channel,
                observer,
                &mut report,
                complete_day,
                &ingest.sz_resumptions,
            )?,
        }
    }
    Ok(report)
}

struct BookRuntime {
    book: OrderBook,
    references: HashMap<(Side, u64), OrderKey>,
    cursor: Option<SnapshotCursor>,
    last_quote_time_ns: Option<i64>,
    market_close_emitted: bool,
    resumption_times: Option<Vec<i64>>,
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
            last_quote_time_ns: None,
            market_close_emitted: false,
            resumption_times: None,
        })
    }

    fn check_quote_time(
        &mut self,
        symbol: &str,
        quote_time_ns: i64,
    ) -> Result<bool, ProductionError> {
        if let Some(previous) = self.last_quote_time_ns {
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
        self.last_quote_time_ns = Some(quote_time_ns);
        Ok(false)
    }

    fn resolve_history(&self, side: Side, order_no: u64) -> Option<OrderKey> {
        self.references.get(&(side, order_no)).copied()
    }

    fn active_reference(&self, side: Side, order_no: u64) -> Option<OrderKey> {
        self.resolve_history(side, order_no)
            .filter(|key| self.book.order(key).is_some())
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
    let mut runtimes: HashMap<String, BookRuntime> = HashMap::new();
    let mut writer = snapshot_writer(request, channel)?;
    while let Some(row) = reader.next_row()? {
        let runtime = runtime(request, &mut runtimes, &row.symbol)?;
        before_row(
            request,
            channel,
            &row.symbol,
            row.quote_time_ns,
            runtime,
            writer.as_mut(),
            observer,
            report,
        )?;
        match row.kind {
            SseKind::Status => {
                report.status_events += 1;
                observer.observe(
                    channel,
                    &row.symbol,
                    &runtime.book,
                    ObservationPoint::AfterEvent(row.quote_time_ns),
                )?;
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
                apply_sse_row(runtime, request, &row)?;
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
    resumptions: &HashMap<String, Vec<i64>>,
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
    let mut runtimes: HashMap<String, BookRuntime> = HashMap::new();
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
            // An unreferenced trade may have created the runtime before its first order.
            runtime(request, &mut runtimes, &row.symbol)?
                .resumption_times
                .get_or_insert_with(|| resumptions.get(&row.symbol).cloned().unwrap_or_default());
            process_sz_order(
                request,
                channel,
                &row,
                &mut runtimes,
                writer.as_mut(),
                observer,
                report,
            )?;
            order = orders
                .as_mut()
                .map(|reader| reader.next_row())
                .transpose()?
                .flatten();
        } else {
            let row = execution.take().ok_or_else(|| {
                ProductionError::InvalidRequest("missing Shenzhen execution cursor".to_owned())
            })?;
            process_sz_execution(
                request,
                channel,
                &row,
                &mut runtimes,
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

fn process_sz_order(
    request: &MarketDayRequest,
    channel: u32,
    row: &SzOrderRow,
    runtimes: &mut HashMap<String, BookRuntime>,
    writer: Option<&mut SnapshotWriter>,
    observer: &mut dyn StateObserver,
    report: &mut ReplayReport,
) -> Result<(), ProductionError> {
    let runtime = runtime(request, runtimes, &row.symbol)?;
    before_row(
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
    let same_side_price_available = match side {
        Side::Buy => runtime.book.summary().best_bid.is_some(),
        Side::Sell => runtime.book.summary().best_ask.is_some(),
    };
    let pricing = match row.kind {
        SzOrderKind::Market if row.price_units > 0 => {
            PricingInstruction::Provided(price(row.price_units, &row.symbol, row.sequence)?)
        }
        SzOrderKind::Market => PricingInstruction::Unpriced,
        SzOrderKind::Limit => {
            PricingInstruction::Provided(price(row.price_units, &row.symbol, row.sequence)?)
        }
        SzOrderKind::SameSideBest if same_side_price_available => PricingInstruction::SameSideBest,
        SzOrderKind::SameSideBest => PricingInstruction::Unpriced,
    };
    let resumption_limit = row.kind == SzOrderKind::Limit
        && runtime
            .resumption_times
            .as_ref()
            .is_some_and(|times| times.contains(&row.quote_time_ns));
    let crossing = match row.kind {
        SzOrderKind::Market => CrossingBehavior::RestAtLastTradePrice,
        SzOrderKind::SameSideBest if !same_side_price_available => CrossingBehavior::AlwaysHide,
        // The source flushes pre-resumption limit orders at this exact timestamp.
        // They may cross before the auction trades arrive. A hidden order that
        // never trades would otherwise remain invisible for its entire lifetime.
        _ if resumption_limit => CrossingBehavior::Rest,
        _ if is_continuous(row.quote_time_ns) => CrossingBehavior::HideIfCrossing,
        _ => CrossingBehavior::Rest,
    };
    let event = BookEvent::AddOrder(AddOrder {
        meta: meta(
            runtime,
            request,
            row.sequence,
            row.local_time_ns,
            row.quote_time_ns,
        )?,
        order_key: key,
        pricing,
        crossing,
        quantity: quantity(row.quantity, &row.symbol, row.sequence)?,
    });
    apply(runtime, event, &row.symbol, row.sequence)?;
    report.observe_sz_applied_event(&row.symbol, row.quote_time_ns)?;
    if resumption_limit {
        report.sz_resumption_rest_orders += 1;
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
    runtimes: &mut HashMap<String, BookRuntime>,
    writer: Option<&mut SnapshotWriter>,
    observer: &mut dyn StateObserver,
    report: &mut ReplayReport,
) -> Result<(), ProductionError> {
    let runtime = runtime(request, runtimes, &row.symbol)?;
    before_row(
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
            meta: meta(
                runtime,
                request,
                row.sequence,
                row.local_time_ns,
                row.quote_time_ns,
            )?,
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
            let key = runtime.active_reference(side, order_no).ok_or_else(|| {
                normalize_error(&row.symbol, row.sequence, "unknown cancellation target")
            })?;
            let remaining = runtime
                .book
                .order(&key)
                .map(|order| order.remaining_quantity)
                .ok_or_else(|| {
                    normalize_error(&row.symbol, row.sequence, "inactive cancellation target")
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
                meta: meta(
                    runtime,
                    request,
                    row.sequence,
                    row.local_time_ns,
                    row.quote_time_ns,
                )?,
                order_key: key,
            })
        }
    };
    apply(runtime, event, &row.symbol, row.sequence)?;
    report.observe_sz_applied_event(&row.symbol, row.quote_time_ns)?;
    if row.kind == SzExecutionKind::Trade {
        observer.observe_trade(
            channel,
            &row.symbol,
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
fn before_row(
    request: &MarketDayRequest,
    channel: u32,
    symbol: &str,
    quote_time_ns: i64,
    runtime: &mut BookRuntime,
    mut writer: Option<&mut SnapshotWriter>,
    observer: &mut dyn StateObserver,
    report: &mut ReplayReport,
) -> Result<(), ProductionError> {
    if runtime.check_quote_time(symbol, quote_time_ns)? {
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
    runtimes: &mut HashMap<String, BookRuntime>,
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
            let boundary = close.max(runtime.last_quote_time_ns.unwrap_or(close));
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
        output.push(BookSnapshot::capture(
            &runtime.book,
            channel,
            SnapshotKind::MarketClose,
            boundary_time_ns,
            depth,
        )?)?;
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

fn apply_sse_row(
    runtime: &mut BookRuntime,
    request: &MarketDayRequest,
    row: &SseRow,
) -> Result<(), ProductionError> {
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
                meta: meta(
                    runtime,
                    request,
                    row.sequence,
                    row.local_time_ns,
                    row.quote_time_ns,
                )?,
                order_key: key,
                pricing: PricingInstruction::Provided(price(
                    row.price_units,
                    &row.symbol,
                    row.sequence,
                )?),
                crossing: CrossingBehavior::Rest,
                quantity: quantity(row.quantity, &row.symbol, row.sequence)?,
            });
            apply(runtime, event.clone(), &row.symbol, row.sequence)?;
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
                meta: meta(
                    runtime,
                    request,
                    row.sequence,
                    row.local_time_ns,
                    row.quote_time_ns,
                )?,
                order_key: key,
            })
        }
        SseKind::Trade => BookEvent::Trade(Trade {
            meta: meta(
                runtime,
                request,
                row.sequence,
                row.local_time_ns,
                row.quote_time_ns,
            )?,
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
    runtimes: &'a mut HashMap<String, BookRuntime>,
    symbol: &str,
) -> Result<&'a mut BookRuntime, ProductionError> {
    if !runtimes.contains_key(symbol) {
        runtimes.insert(
            symbol.to_owned(),
            BookRuntime::new(request, symbol, request.snapshots.as_ref())?,
        );
    }
    runtimes.get_mut(symbol).ok_or_else(|| {
        ProductionError::InvalidRequest("failed to create order-book runtime".to_owned())
    })
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
    _request: &MarketDayRequest,
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

fn is_continuous(timestamp_ns: i64) -> bool {
    const OPEN: i64 = 9 * 3_600_000_000_000 + 30 * 60_000_000_000;
    const CLOSE_CALL: i64 = 14 * 3_600_000_000_000 + 57 * 60_000_000_000;
    let quote = QuoteTimestampNs::from_nanos(timestamp_ns);
    let time = super::time::time_of_day_nanos(quote);
    (OPEN..CLOSE_CALL).contains(&time)
}

#[cfg(test)]
mod tests {
    use super::{one_sided_reference, same_second_regression_is_allowed};
    use crate::Side;

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
}
