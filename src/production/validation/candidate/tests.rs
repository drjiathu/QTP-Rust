#![allow(clippy::expect_used)]
use super::*;
use crate::{
    AddOrder, ApplySequence, BookConfig, BookEvent, BookKey, ChannelId, CrossingBehavior,
    EventMeta, LocalTimestampNs, OrderId, OrderKey, Price, PriceScale, PricingInstruction,
    Quantity, RawSequence, Symbol, TradingDay,
};

fn book(market: Market, symbol: &str) -> OrderBook {
    OrderBook::new(BookConfig::new(
        BookKey {
            market,
            symbol: Symbol::from(symbol),
            trading_day: TradingDay::from_yyyymmdd(20260828).expect("date"),
        },
        PriceScale::from_decimal_places(4).expect("scale"),
    ))
}

fn add(book: &mut OrderBook, n: u64, crossing: CrossingBehavior) -> OrderKey {
    let key = OrderKey {
        channel_id: ChannelId::new(1).expect("channel"),
        side: Side::Buy,
        order_id: OrderId::new(n).expect("order"),
    };
    book.apply(BookEvent::AddOrder(AddOrder {
        meta: EventMeta {
            book_key: book.config().book_key.clone(),
            raw_sequence: RawSequence::new(n).expect("raw"),
            apply_sequence: ApplySequence::new(n).expect("apply"),
            local_time: LocalTimestampNs::from_nanos(10),
            quote_time: QuoteTimestampNs::from_nanos(10),
        },
        order_key: key,
        pricing: PricingInstruction::Provided(Price::from_units(100_000).expect("price")),
        crossing,
        quantity: Quantity::new(10).expect("quantity"),
    }))
    .expect("add");
    key
}

fn reference(view: SnapshotBookView) -> ReferenceSnapshot {
    ReferenceSnapshot {
        time_ns: 10,
        view: Some(view.try_into().expect("compact")),
        load_error: None,
        source_row_no: 1,
        missing_reception: false,
        pre_close_price_units: None,
        state: AnchorState::default(),
    }
}

fn context<'a>(book: &'a OrderBook, symbol: &'a str) -> CandidateContext<'a> {
    CandidateContext {
        market: book.config().book_key.market,
        rules: SymbolValidationRules::new(
            book.config().book_key.market,
            symbol,
            REFERENCE_SECOND_NS,
            false,
        ),
        symbol,
        book,
        limits: None,
        close_price: None,
    }
}

#[test]
fn turnover_compatibility_is_per_reference_and_preserves_other_differences() {
    for market in [Market::Sse, Market::Szse] {
        let symbol = if market == Market::Sse {
            "510300"
        } else {
            "159915"
        };
        let mut book = book(market, symbol);
        book.apply(BookEvent::Trade(crate::Trade {
            meta: EventMeta {
                book_key: book.config().book_key.clone(),
                raw_sequence: RawSequence::new(1).expect("seq"),
                apply_sequence: ApplySequence::new(1).expect("seq"),
                local_time: LocalTimestampNs::from_nanos(10),
                quote_time: QuoteTimestampNs::from_nanos(10),
            },
            bid_order: crate::OrderReference::Absent,
            ask_order: crate::OrderReference::Absent,
            price: Price::from_units(100_100).expect("price"),
            quantity: Quantity::new(1_000_000_001).expect("quantity"),
        }))
        .expect("trade");
        let original = SnapshotBookView::from_book(&book, 10).expect("view");
        let mut cache = CandidateCache::default();
        let mut counters = CandidateCounters::default();
        for anchor in [
            ValidationAnchor::PreOpen,
            ValidationAnchor::ContinuousTrading,
            ValidationAnchor::MarketClose,
        ] {
            for (missing, extra_difference) in [(true, false), (false, false), (true, true)] {
                let mut expected = original.clone();
                expected.turnover_units = 100_100_000_100_000;
                if extra_difference {
                    expected.total_bid_quantity += 1;
                }
                let mut frame = reference(expected);
                frame.missing_reception = missing;
                compare_candidate(
                    context(&book, symbol),
                    &mut frame,
                    &mut cache,
                    &mut counters,
                    anchor,
                    Some(10),
                )
                .expect("compare");
                assert_eq!(frame.state.matched, missing && !extra_difference);
                if frame.state.matched {
                    let audit = frame
                        .state
                        .diagnostics
                        .as_ref()
                        .expect("diagnostics")
                        .turnover_precision
                        .as_ref()
                        .expect("precision audit");
                    assert_eq!(audit.actual_units, original.turnover_units);
                    assert_eq!(audit.quantum_units, 1000);
                } else {
                    let differences = frame.state.best_differences().expect("differences");
                    assert_eq!(
                        differences.iter().any(|d| d.field == "turnover_units"),
                        !missing
                    );
                }
            }
        }
        assert_eq!(
            SnapshotBookView::from_book(&book, 10).expect("unchanged"),
            original
        );
        assert_eq!(
            cache.view.as_ref().expect("cache").turnover_units,
            original.turnover_units
        );
    }
}

#[test]
fn pending_reentry_invalidates_cache_without_advancing_event_metadata() {
    let mut book = book(Market::Szse, "000001");
    let key = add(&mut book, 1, CrossingBehavior::AlwaysHide);
    let mut target = book.clone();
    let price = Price::from_units(100_000).expect("price");
    target.rest_pending_order(key, price).expect("target");
    let mut reference = reference(SnapshotBookView::from_book(&target, 10).expect("view"));
    let mut cache = CandidateCache::default();
    let mut counters = CandidateCounters::default();
    compare_candidate(
        context(&book, "000001"),
        &mut reference,
        &mut cache,
        &mut counters,
        ValidationAnchor::ContinuousTrading,
        Some(10),
    )
    .expect("before");
    assert!(!reference.state.matched);
    let metadata = book.last_applied_meta().cloned();
    let revision = book.cache_revision();
    book.rest_pending_order(key, price).expect("reentry");
    assert_eq!(book.last_applied_meta(), metadata.as_ref());
    assert_ne!(book.cache_revision(), revision);
    compare_candidate(
        context(&book, "000001"),
        &mut reference,
        &mut cache,
        &mut counters,
        ValidationAnchor::ContinuousTrading,
        Some(11),
    )
    .expect("after");
    assert!(reference.state.matched);
    assert_eq!(reference.state.matched_candidate_time_ns, Some(11));
    assert_eq!(reference.state.matched_candidate_apply_sequence, Some(1));
    let ready = cache.revision;
    assert!(book.rest_pending_order(key, price).is_err());
    cache
        .prepare(context(&book, "000001"), &mut counters)
        .expect("failed mutation");
    assert_eq!(cache.revision, ready);
    assert!(cache.depth_ready);
}

#[test]
fn cached_views_preserve_per_reference_first_match_and_close_isolation() {
    for (market, symbol) in [
        (Market::Sse, "600000"),
        (Market::Sse, "510300"),
        (Market::Szse, "000001"),
        (Market::Szse, "159915"),
    ] {
        let mut book = book(market, symbol);
        add(&mut book, 1, CrossingBehavior::Rest);
        let expected = SnapshotBookView::from_book(&book, 10).expect("view");
        let mut cache = CandidateCache::default();
        let mut counters = CandidateCounters::default();
        for time in [10, 11, 19] {
            let mut frame = reference(expected.clone());
            compare_candidate(
                context(&book, symbol),
                &mut frame,
                &mut cache,
                &mut counters,
                ValidationAnchor::ContinuousTrading,
                Some(time),
            )
            .expect("match");
            assert_eq!(frame.state.matched_candidate_time_ns, Some(time));
            assert_eq!(frame.state.matched_candidate_raw_sequence, Some(1));
        }
        #[cfg(feature = "profiling")]
        {
            assert_eq!(counters.depth_materializations, 1);
            assert_eq!(counters.candidate_cache_hits, 2);
        }
        let cached = cache.view.clone();
        // Close always gets an independent view, including SZ LastPrice
        // normalization, even if the underlying book revision is unchanged.
        let mut frame = reference(expected);
        let limits = DayLimits {
            values: Some((120_000, 80_000)),
            ..DayLimits::default()
        };
        let close_ctx = CandidateContext {
            limits: Some(&limits),
            ..context(&book, symbol)
        };
        compare_candidate(
            close_ctx,
            &mut frame,
            &mut cache,
            &mut counters,
            ValidationAnchor::MarketClose,
            Some(20),
        )
        .expect("close");
        assert!(frame.state.matched);
        assert_eq!(cache.view, cached);
        add(&mut book, 2, CrossingBehavior::Rest); // Same quote_time, different state.
        let mut frame = reference(SnapshotBookView::from_book(&book, 10).expect("new view"));
        compare_candidate(
            context(&book, symbol),
            &mut frame,
            &mut cache,
            &mut counters,
            ValidationAnchor::ContinuousTrading,
            Some(21),
        )
        .expect("new revision");
        assert!(frame.state.matched);
    }
}

#[test]
fn direct_compact_mask_equals_materialized_comparison_for_every_field() {
    for (market, symbol) in [
        (Market::Sse, "600000"),
        (Market::Sse, "510300"),
        (Market::Szse, "000001"),
        (Market::Szse, "159915"),
    ] {
        let mut book = book(market, symbol);
        add(&mut book, 1, CrossingBehavior::Rest);
        let expected = SnapshotBookView::from_book(&book, 10).expect("view");
        let compact: ReferenceBookView = expected.clone().try_into().expect("compact");
        for field in 0..14 {
            let mut actual = expected.clone();
            match field {
                0 => actual.bids[0].price_units += 1,
                1 => actual.bids[0].quantity += 1,
                2 => actual.bids[0].order_count = u64::from(u32::MAX) + 1,
                3 => actual.asks.push(SnapshotLevel {
                    price_units: 110_000,
                    quantity: 1,
                    order_count: 1,
                }),
                4 => actual.total_bid_quantity += 1,
                5 => actual.total_ask_quantity += 1,
                6 => actual.weighted_bid_price_units = Some(999_999),
                7 => actual.weighted_ask_price_units = Some(999_999),
                8 => actual.last_price_units = Some(1),
                9 => actual.high_price_units = Some(2),
                10 => actual.low_price_units = Some(3),
                11 => actual.trade_count += 1,
                12 => actual.trade_quantity += 1,
                _ => actual.turnover_units += 1,
            }
            let mut mask = compare_reference_scalars(
                validation_price_quantum(market, symbol),
                &compact,
                &actual,
            );
            mask.0 |= compact.depth_differences(&actual).0;
            assert_eq!(mask, compare_view_mask(market, symbol, &expected, &actual));
            assert_eq!(
                mask.count(),
                compare_views(market, symbol, &expected, &actual).len()
            );
        }
    }
}

use super::super::tests::{add_order, empty_book};
use super::super::{AnchorState, validation_price_quantum};
use crate::{QuoteTimestampNs, SnapshotLevel};

// The window-engine differential test uses the detailed reference comparator.
// Expose only this test adapter, not production comparison internals.
pub(in crate::production::validation) fn reference_differences(
    market: Market,
    symbol: &str,
    expected: &SnapshotBookView,
    actual: &SnapshotBookView,
) -> Vec<FieldDifference> {
    super::compare_views(market, symbol, expected, actual)
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
