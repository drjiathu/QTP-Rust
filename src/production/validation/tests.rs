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

#[test]
#[allow(clippy::expect_used)]
fn borrowed_symbol_entries_keep_independent_limits_and_trade_audit() {
    let mut observer = ValidationObserver::new(Market::Szse, HashMap::new());
    observer.set_close_limits(HashMap::from([
        (
            "000001".to_owned(),
            super::DayLimits {
                values: Some((120000, 80000)),
                source: "first".to_owned(),
                error: None,
                upper_limit_normalization: None,
            },
        ),
        (
            "000002".to_owned(),
            super::DayLimits {
                values: Some((240000, 160000)),
                source: "second".to_owned(),
                error: None,
                upper_limit_normalization: None,
            },
        ),
    ]));
    let key_pointer = observer
        .symbols
        .get_key_value("000001")
        .expect("key")
        .0
        .as_ptr();
    for _ in 0..100 {
        assert!(!observer.symbol_state_mut("000001").seen);
    }
    assert_eq!(
        observer
            .symbols
            .get_key_value("000001")
            .expect("key")
            .0
            .as_ptr(),
        key_pointer
    );
    for (symbol, seq, price) in [("000001", 7, 100000), ("000002", 11, 200000)] {
        observer
            .observe_trade(1, symbol, timestamp("14:55:00.000"), price, 10)
            .expect("trade");
        observer
            .observe_trade_with_sequence(1, symbol, seq, timestamp("14:56:00.000"), price, 10)
            .expect("sequence");
        observer
            .observe_trade_with_sequence(
                1,
                symbol,
                seq + 1,
                timestamp("15:00:00.000"),
                price + 100,
                10,
            )
            .expect("close");
        let state = &observer.symbols[symbol];
        let tracker = state.close_price.as_ref().expect("tracker");
        let base = tracker.range_base.as_ref().expect("base");
        assert_eq!(base.raw_sequence, Some(seq));
        assert_eq!(base.price, price);
        assert!(super::sz_close_price::tests::has_closing_auction_trade(
            tracker
        ));
    }
    assert_eq!(observer.symbols.len(), 2);
    assert_eq!(
        observer.symbols["000001"]
            .limits
            .as_ref()
            .expect("limits")
            .source,
        "first"
    );
    assert_eq!(
        observer.symbols["000002"]
            .limits
            .as_ref()
            .expect("limits")
            .source,
        "second"
    );
}

pub(super) fn empty_book() -> OrderBook {
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
        let full = crate::SnapshotBookView::from_book(&book, 10).expect("view");
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
                    source_row_no: 1,
                    missing_reception: false,
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
            observer.symbol_state_mut(symbol).limits = Some(limits);
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
            crate::SnapshotBookView::from_book(&book, 10).expect("view"),
            full
        );
    }
}

pub(super) fn timestamp(value: &str) -> i64 {
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
    let mut view = match crate::SnapshotBookView::from_book(&empty_book(), 10) {
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
                source_row_no: 1,
                missing_reception: false,
                pre_close_price_units: Some(pre_close_price_units),
                state: AnchorState::default(),
            }),
        },
    )])
}

pub(super) fn add_order(book: &mut OrderBook) {
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
                crate::SnapshotBookView::from_book(&candidates[12], 10).expect("view");
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
                        source_row_no: 1,
                        missing_reception: false,
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
                        crate::SnapshotBookView::from_book(candidate, 10).expect("view");
                    actual.weighted_bid_price_units =
                        super::published_weighted_price(candidate, Side::Buy, market, "600000")
                            .expect("bid");
                    actual.weighted_ask_price_units =
                        super::published_weighted_price(candidate, Side::Sell, market, "600000")
                            .expect("ask");
                    let diffs = super::candidate::tests::reference_differences(
                        market, "600000", &expected, &actual,
                    );
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
                assert!(observer.counters.scalar_rejected_candidates > 0);
            }
        }
    }
}

#[test]
fn pre_open_accepts_a_state_inside_the_reference_second() {
    let mut expected_book = empty_book();
    add_order(&mut expected_book);
    let expected = match crate::SnapshotBookView::from_book(&expected_book, 10) {
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
                source_row_no: 1,
                missing_reception: false,
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
    assert_eq!(report.missing_source, 0);
    assert_eq!(report.selected_references, 1);
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
    assert_eq!(compact_report.records.len(), 0);
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
    let expected = match crate::SnapshotBookView::from_book(&expected_book, 10) {
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
                source_row_no: 1,
                missing_reception: false,
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
    report.run_outcome = super::RunOutcome::Passed;
    report.selected_references = 1;
    report.comparable_references = 1;
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
    let expected = match crate::SnapshotBookView::from_book(&expected_book, 10) {
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
                source_row_no: 1,
                missing_reception: false,
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
    let expected = match crate::SnapshotBookView::from_book(&expected_book, 10) {
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
                source_row_no: 1,
                missing_reception: false,
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
    let expected = match crate::SnapshotBookView::from_book(&expected_book, 10) {
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
                source_row_no: 1,
                missing_reception: false,
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
    let expected = match crate::SnapshotBookView::from_book(&expected_book, 10) {
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
                source_row_no: 1,
                missing_reception: false,
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
    let expected = crate::SnapshotBookView::from_book(&expected_book, 10)
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
                        source_row_no: 1,
                        missing_reception: false,
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
                crate::SnapshotBookView::from_book(&expected_book, 10)
                    .and_then(TryInto::try_into)
                    .unwrap_or_else(|_| std::process::abort()),
            ),
            load_error: None,
            source_row_no: 1,
            missing_reception: false,
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
fn cached_rules_follow_overrides_and_initialize_replay_only_symbols() {
    for (market, symbols) in [
        (Market::Sse, vec!["600000", "510300"]),
        (Market::Szse, vec!["000001", "159915", "300001"]),
    ] {
        let mut observer = ValidationObserver::new(market, HashMap::new());
        for symbol in &symbols {
            observer.symbol_state_mut(symbol); // Also covers limits/trades before observation.
            assert!(observer.symbols[*symbol].rules.is_none());
        }
        for configured_ms in [None, Some(2500), Some(1000)] {
            if let Some(ms) = configured_ms {
                observer = observer
                    .with_continuous_lookahead(Some(Duration::from_millis(ms)))
                    .unwrap_or_else(|_| std::process::abort());
                assert!(observer.symbols.values().all(|state| state.rules.is_none()));
            }
            for symbol in &symbols {
                let expected_lookahead = observer.lookahead_ns(symbol);
                let book = empty_book();
                observer
                    .observe(1, symbol, &book, ObservationPoint::BeforeEvent(10))
                    .unwrap_or_else(|_| std::process::abort());
                let rules = observer.symbols[*symbol]
                    .rules
                    .unwrap_or_else(|| std::process::abort());
                assert_eq!(rules.lookahead_ns, expected_lookahead);
                assert_eq!(
                    rules.price_quantum,
                    super::validation_price_quantum(market, symbol)
                );
                assert_eq!(rules.is_etf, super::is_etf_symbol(market, symbol));
                observer
                    .observe(1, symbol, &book, ObservationPoint::AfterEvent(10))
                    .unwrap_or_else(|_| std::process::abort());
                assert_eq!(observer.symbols[*symbol].rules, Some(rules));
            }
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
                    crate::SnapshotBookView::from_book(book, 10)
                        .and_then(TryInto::try_into)
                        .unwrap_or_else(|_| std::process::abort()),
                ),
                load_error: None,
                source_row_no: 1,
                missing_reception: false,
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
        .find(|r| r.reference_time_ms == 11_000);
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
    let expected = match crate::SnapshotBookView::from_book(&expected_book, 10) {
        Ok(value) => value,
        Err(_) => std::process::abort(),
    };
    let empty = match crate::SnapshotBookView::from_book(&empty_book(), 10) {
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
                source_row_no: 1,
                missing_reception: false,
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
                    source_row_no: 1,
                    missing_reception: false,
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
                    source_row_no: 1,
                    missing_reception: false,
                    pre_close_price_units: None,
                    state: AnchorState::default(),
                },
            ],
            market_close: Some(ReferenceSnapshot {
                time_ns: 14_000_000_000,
                view: Some(empty.try_into().unwrap_or_else(|_| std::process::abort())),
                load_error: None,
                source_row_no: 1,
                missing_reception: false,
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
    assert_eq!(report.omitted_failure_records, 1);
    assert_eq!(report.mismatched_symbols, 1);
    assert_eq!(report.mismatch_symbol_prefixes.get("600"), Some(&1));
}

#[test]
fn mixed_detail_caps_preserve_existing_order_dependent_retention() {
    for retain_matches in [false, true] {
        for cap in [0, 1, 2] {
            let reference = |row: i64, matched| ReferenceSnapshot {
                time_ns: row * 1_000_000_000,
                source_row_no: row as u64,
                view: None,
                load_error: None,
                missing_reception: false,
                pre_close_price_units: None,
                state: AnchorState {
                    matched,
                    ..Default::default()
                },
            };
            let mut observer = ValidationObserver::new(
                Market::Sse,
                HashMap::from([(
                    "600000".to_owned(),
                    SymbolReferences {
                        pre_open: Some(reference(1, true)),
                        continuous_trading: vec![reference(2, false)],
                        market_close: Some(reference(3, true)),
                    },
                )]),
            )
            .with_max_detail_records(Some(cap));
            observer.symbol_state_mut("600000").seen = true;
            let report = observer.into_report(ReplayReport::default(), retain_matches);
            assert_eq!(
                (
                    report.selected_references,
                    report.matched,
                    report.mismatched
                ),
                (3, 2, 1)
            );
            let kept_failure = usize::from(cap > usize::from(retain_matches));
            assert_eq!(
                report.records.len(),
                2 * usize::from(retain_matches) + kept_failure
            );
            assert_eq!(report.omitted_failure_records, 1 - kept_failure as u64);
            assert_eq!(
                report.omitted_matched_records,
                if retain_matches { 0 } else { 2 }
            );
        }
    }
}

#[test]
fn validates_every_continuous_trading_reference_frame() {
    let mut book = empty_book();
    add_order(&mut book);
    let expected = match crate::SnapshotBookView::from_book(&book, 10) {
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
                    source_row_no: 1,
                    missing_reception: false,
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
                    source_row_no: 1,
                    missing_reception: false,
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
    assert_eq!(continuous[0].reference_time_ms, 10_000);
    assert_eq!(continuous[1].reference_time_ms, 12_000);
}

#[test]
fn continuous_anchor_excludes_events_at_the_next_second() {
    let mut expected_book = empty_book();
    add_order(&mut expected_book);
    let expected = match crate::SnapshotBookView::from_book(&expected_book, 10) {
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
                source_row_no: 1,
                missing_reception: false,
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
            && record.match_tag.as_deref() == Some("SZ_ETF_AVG_CLOSE_PRICE")
    ));
    assert_eq!(report.match_tags.get("SZ_ETF_AVG_CLOSE_PRICE"), Some(&1));
}

#[test]
fn shenzhen_stock_average_close_uses_the_stock_price_quantum() {
    let symbol = "000001";
    let references = sz_close_references(symbol, 10_000, 9_900);
    let mut observer = ValidationObserver::new(Market::Szse, references);
    observer.symbol_state_mut(symbol).limits = Some(super::DayLimits {
        values: Some((200_000, 100)),
        source: "synthetic limited stock".to_owned(),
        error: None,
        upper_limit_normalization: None,
    });
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
            && record.match_tag.as_deref() == Some("SZ_STOCK_AVG_CLOSE_PRICE")
    ));
}

#[test]
fn shenzhen_e0_uses_previous_close_when_the_day_has_no_trade() {
    let symbol = "000001";
    let references = sz_close_references(symbol, 123_400, 123_400);
    let mut observer = ValidationObserver::new(Market::Szse, references);
    observer.symbol_state_mut(symbol).limits = Some(super::DayLimits {
        values: Some((200_000, 100)),
        source: "synthetic limited stock".to_owned(),
        error: None,
        upper_limit_normalization: None,
    });
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
            && record.match_tag.as_deref() == Some("SZ_STOCK_PRE_CLOSE_PRICE")
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
#[allow(clippy::expect_used)]
fn no_selected_references_has_no_virtual_results_and_version_is_strict() {
    let refs = HashMap::from([("000635".to_owned(), SymbolReferences::default())]);
    let report =
        ValidationObserver::new(Market::Szse, refs).into_report(ReplayReport::default(), true);
    assert_eq!(report.selected_references, 0);
    assert_eq!(report.match_rate, None);
    assert!(report.records.is_empty());
    assert!(report.is_success());
    assert!(!report.is_standard_acceptance());
    assert_eq!(report.run_outcome, super::RunOutcome::NoEligibleReferences);
    assert_eq!(report.coverage.symbols_without_reference_records, 1);
    let mut json = serde_json::to_value(&report).expect("serialize");
    assert!(serde_json::from_value::<super::ValidationReport>(json.clone()).is_ok());
    json["report_schema_version"] = serde_json::json!(1);
    assert!(serde_json::from_value::<super::ValidationReport>(json.clone()).is_err());
    json.as_object_mut()
        .expect("object")
        .remove("report_schema_version");
    assert!(serde_json::from_value::<super::ValidationReport>(json).is_err());
    let replay = ReplayReport {
        sz_after_close_events: 1,
        ..ReplayReport::default()
    };
    let report = ValidationObserver::new(Market::Szse, HashMap::new()).into_report(replay, true);
    assert_eq!(report.run_outcome, super::RunOutcome::Failed);
    assert!(!report.is_success());
}
