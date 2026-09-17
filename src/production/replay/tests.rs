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

fn pending_order() -> super::SzOrderRow {
    super::SzOrderRow {
        source_row: 2,
        sequence: 2,
        channel: 1,
        symbol: "000001".try_into().expect("symbol"),
        quote_time_ns: 100,
        local_time_ns: 200,
        price_units: 123_456,
        quantity: 10,
        side: super::SzSide::Buy,
        kind: super::SzOrderKind::Market,
    }
}

fn pending_response(sequence: u64, quantity: u64, cancel: bool) -> super::SzExecutionRow {
    super::SzExecutionRow {
        source_row: sequence,
        sequence,
        channel: 1,
        symbol: pending_order().symbol,
        quote_time_ns: 100,
        local_time_ns: 200 + sequence as i64,
        bid_order_no: 2,
        ask_order_no: if cancel { 0 } else { 1 },
        price_units: if cancel { 0 } else { 100_000 },
        quantity,
        kind: if cancel {
            super::SzExecutionKind::Cancel
        } else {
            super::SzExecutionKind::Trade
        },
    }
}

#[derive(Default)]
struct PendingObserver(Vec<(&'static str, u64)>);

impl super::StateObserver for PendingObserver {
    fn observe(
        &mut self,
        _: u32,
        _: &str,
        book: &crate::OrderBook,
        point: super::ObservationPoint,
    ) -> Result<(), ProductionError> {
        let label = match point {
            super::ObservationPoint::BeforeEvent(_) => "before",
            super::ObservationPoint::AfterEvent(_) => "after",
            _ => "other",
        };
        self.0.push((
            label,
            book.summary().last_raw_sequence.map_or(0, |seq| seq.get()),
        ));
        Ok(())
    }

    fn observe_trade_with_sequence(
        &mut self,
        _: u32,
        _: &str,
        sequence: u64,
        _: i64,
        _: i64,
        _: u64,
    ) -> Result<(), ProductionError> {
        self.0.push(("trade", sequence));
        Ok(())
    }
}

#[test]
fn pending_application_keeps_observer_order_and_partial_failure_effects() {
    for fail in [false, true] {
        let request = runtime_request();
        let mut runtimes = SymbolMap::default();
        let runtime = runtime(&request, &mut runtimes, "000001").expect("runtime");
        let mut report = super::ReplayReport::default();
        let mut maker = pending_order();
        maker.sequence = 1;
        maker.kind = super::SzOrderKind::Limit;
        maker.side = super::SzSide::Sell;
        maker.price_units = 100_000;
        super::process_sz_order(
            &request,
            1,
            &maker,
            runtime,
            false,
            None,
            &mut super::NullObserver,
            &mut report,
        )
        .expect("maker");
        let mut observer = PendingObserver::default();
        // Bad cancellation exercises an apply failure, independently of collection.
        let group = super::PendingGroup {
            responses: vec![
                pending_response(3, 4, false),
                pending_response(4, if fail { 5 } else { 6 }, true),
            ],
            disposition: crate::production::sz_pending::Disposition::Terminal,
        };
        let result = super::apply_pending_group(
            &request,
            1,
            &pending_order(),
            group,
            runtime,
            false,
            None,
            &mut observer,
            &mut report,
        );
        let summary = runtime.book.summary();
        assert_eq!(summary.statistics.trade_count, 1);
        assert_eq!(summary.statistics.total_quantity, 4);
        assert_eq!(summary.statistics.total_turnover_units, 400_000);
        assert_eq!(
            summary.last_quote_time.map(|time| time.as_nanos()),
            Some(100)
        );
        if fail {
            assert!(
                result
                    .expect_err("quantity mismatch")
                    .to_string()
                    .contains("does not equal remaining 6")
            );
            assert_eq!(observer.0, vec![("before", 1)]);
            assert_eq!(summary.last_raw_sequence.map(|seq| seq.get()), Some(3));
            assert_eq!(summary.last_apply_sequence.map(|seq| seq.get()), Some(3));
            assert_eq!(
                summary.last_local_time.map(|time| time.as_nanos()),
                Some(203)
            );
            assert_eq!(
                runtime
                    .active_reference_with_remaining(Side::Buy, 2)
                    .map(|(_, qty)| qty),
                Some(6)
            );
            assert_eq!(report.applied_events, 3);
            assert_eq!(report.sz_pending_groups, 0);
        } else {
            result.expect("group applied");
            assert_eq!(observer.0, vec![("before", 1), ("trade", 3), ("after", 4)]);
            assert_eq!(summary.last_raw_sequence.map(|seq| seq.get()), Some(4));
            assert_eq!(summary.last_apply_sequence.map(|seq| seq.get()), Some(4));
            assert_eq!(
                summary.last_local_time.map(|time| time.as_nanos()),
                Some(204)
            );
            assert!(runtime.active_reference(Side::Buy, 2).is_none());
            assert_eq!(report.applied_events, 4);
            assert_eq!(report.sz_pending_groups, 1);
        }
    }
}

#[test]
fn pending_collection_preserves_unconsumed_error_and_foreign_rows() {
    let row = pending_order();
    for case in 0..7 {
        let mut next = pending_response(3, 10, true);
        let mut order = None;
        match case {
            0 => {
                let mut other = row.clone();
                other.sequence = 3;
                order = Some(other);
            }
            1 => next.sequence = 4,
            2 => next.quote_time_ns += 1,
            3 => next.channel = 2,
            4 => next.symbol = "000002".try_into().expect("symbol"),
            5 => next.quantity = 9,
            _ => next.bid_order_no = 99,
        }
        let expected = next.clone();
        let mut cursor = Some(next);
        let error = super::collect_pending_group(
            SzMarketOrderPolicy::RequireEvidence,
            1,
            &row,
            Some(100_000),
            &order,
            &mut cursor,
            &mut None,
        )
        .err()
        .expect("rejected boundary");
        if case == 0 {
            assert!(matches!(
                error,
                ProductionError::AmbiguousSequence { sequence: 3, .. }
            ));
        }
        assert_eq!(
            cursor,
            Some(expected),
            "case {case} must not consume failing/foreign row"
        );
    }
    assert!(
        super::collect_pending_group(
            SzMarketOrderPolicy::RequireEvidence,
            1,
            &row,
            Some(100_000),
            &None,
            &mut None,
            &mut None
        )
        .is_err()
    );
    let mut cursor = Some(pending_response(3, 10, true));
    let group = super::collect_pending_group(
        SzMarketOrderPolicy::RequireEvidence,
        1,
        &row,
        Some(100_000),
        &None,
        &mut cursor,
        &mut None,
    )
    .expect("terminal response");
    assert_eq!(group.responses.len(), 1);
    assert!(cursor.is_none());
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
    let time =
        super::parse_market_timestamp(runtime_request().trading_day, "14:56:00.000").expect("time");
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
