use super::*;
use crate::*;

fn book() -> OrderBook {
    OrderBook::new(BookConfig::new(
        BookKey {
            market: Market::Szse,
            symbol: Symbol::from("001232"),
            trading_day: TradingDay::from_yyyymmdd(20260806).expect("day"),
        },
        PriceScale::from_decimal_places(4).expect("scale"),
    ))
}

fn add(book: &mut OrderBook, seq: u64, side: Side, price: i64, quantity: u64) {
    book.apply(BookEvent::AddOrder(AddOrder {
        meta: EventMeta {
            book_key: book.config().book_key.clone(),
            raw_sequence: RawSequence::new(seq).expect("seq"),
            apply_sequence: ApplySequence::new(seq).expect("seq"),
            local_time: LocalTimestampNs::from_nanos(seq as i64),
            quote_time: QuoteTimestampNs::from_nanos(seq as i64),
        },
        order_key: OrderKey {
            channel_id: ChannelId::new(1).expect("channel"),
            side,
            order_id: OrderId::new(seq).expect("order"),
        },
        pricing: PricingInstruction::Provided(Price::from_units(price).expect("price")),
        crossing: CrossingBehavior::Rest,
        quantity: Quantity::new(quantity).expect("qty"),
    }))
    .expect("apply");
}

#[test]
fn rounded_sentinel_requires_both_missing_and_exact_known_pair() {
    let mut limits = DayLimits::default();
    for source in ["first", "second"] {
        limits.observe_reference(Ok((ROUNDED_NO_UPPER_LIMIT, TICK)), true, || source.into());
    }
    limits.observe_reference(Ok((NO_UPPER_LIMIT, TICK)), false, || "normal".into());
    assert_eq!(limits.unlimited(), Ok(true));
    let audit = limits.upper_limit_normalization.as_ref().expect("audit");
    assert_eq!(audit.records, 2);
    assert_eq!(audit.first_source, "first");
    for (missing, pair) in [
        (false, (ROUNDED_NO_UPPER_LIMIT, TICK)),
        (true, (ROUNDED_NO_UPPER_LIMIT + 1, TICK)),
        (true, (ROUNDED_NO_UPPER_LIMIT, TICK + 1)),
    ] {
        let mut invalid = DayLimits::default();
        invalid.observe_reference(Ok(pair), missing, || "bad".into());
        assert!(invalid.unlimited().is_err());
        invalid.observe_reference(Ok((ROUNDED_NO_UPPER_LIMIT, TICK)), true, || "later".into());
        assert!(
            invalid.unlimited().is_err(),
            "compatibility must not clear earlier errors"
        );
    }
    limits.observe_reference(Ok((120000, 80000)), true, || "conflict".into());
    assert!(limits.unlimited().is_err());
}

#[test]
fn integer_range_rounding_minimum_tick_and_overflow() {
    assert_eq!(bounds(4400).expect("range"), (4000, 4800));
    assert_eq!(bounds(247200).expect("range"), (222500, 271900));
    assert_eq!(bounds(1769900).expect("range"), (1592900, 1946900));
    assert_eq!(bounds(100).expect("range"), (100, 200));
    assert_eq!(bounds(400).expect("range"), (300, 500));
    assert_eq!(bounds(10500).expect("half cent"), (9500, 11600));
    assert!(bounds(0).is_err());
    assert!(bounds(101).is_err());
    assert!(bounds(i64::MAX / 100 * 100).is_err());
}

#[test]
fn metadata_is_explicit_consistent_and_not_inferred_from_depth() {
    let mut metadata = DayLimits::default();
    assert!(metadata.unlimited().is_err());
    metadata.observe(Ok((NO_UPPER_LIMIT, 100)), "first".to_owned());
    assert_eq!(metadata.unlimited(), Ok(true));
    metadata.observe(Ok((NO_UPPER_LIMIT, 100)), "same".to_owned());
    assert_eq!(metadata.source, "first");
    metadata.observe(Ok((120000, 80000)), "conflict".to_owned());
    assert!(metadata.unlimited().is_err());
    for values in [Ok((0, 0)), Ok((120001, 80000)), Err("missing".to_owned())] {
        let mut metadata = DayLimits::default();
        metadata.observe(values, "bad".to_owned());
        assert!(metadata.unlimited().is_err());
    }
}

#[test]
fn lazy_sources_preserve_first_source_and_exact_error_context() {
    use std::cell::Cell;
    let calls = Cell::new(0);
    let source = || {
        calls.set(calls.get() + 1);
        format!("row{}", calls.get())
    };
    let mut limits = DayLimits::default();
    limits.observe_lazy(Ok((120000, 80000)), source);
    for _ in 0..100 {
        limits.observe_lazy(Ok((120000, 80000)), source);
    }
    assert_eq!(calls.get(), 1);
    assert_eq!(limits.source, "row1");
    limits.observe_lazy(Ok((130000, 80000)), source);
    assert_eq!(calls.get(), 2);
    assert_eq!(
        limits.error.as_deref(),
        Some(
            "conflicting SZ daily price limits: (120000, 80000) at row1 vs (130000, 80000) at row2"
        )
    );
    limits.observe_lazy(Err("later error".to_owned()), source);
    assert_eq!(calls.get(), 2);
    for (values, expected) in [
        (Ok((0, 0)), "invalid SZ daily price limits (0, 0) at bad"),
        (Err("missing".to_owned()), "missing at bad"),
    ] {
        let mut invalid = DayLimits::default();
        invalid.observe_lazy(values, || "bad".to_owned());
        assert_eq!(invalid.error.as_deref(), Some(expected));
    }
}

#[test]
fn filters_before_depth_and_aggregates_all_eligible_levels_without_mutation() {
    let mut book = book();
    // Ten out-of-band orders must not consume the ten eligible depth slots.
    for seq in 1..=10 {
        add(&mut book, seq, Side::Buy, 120000 + seq as i64 * 100, 1000);
    }
    for seq in 11..=22 {
        add(
            &mut book,
            seq,
            Side::Buy,
            90000 + (seq - 11) as i64 * 100,
            10,
        );
    }
    add(&mut book, 23, Side::Sell, 110000, 20);
    add(&mut book, 24, Side::Sell, 110100, 30);
    let before = book.summary();
    let levels = book.levels(Side::Buy);
    let base = RangeBase {
        price: 100000,
        time_ms: 1,
        raw_sequence: Some(99),
    };
    let (actual, audit) = project(&book, &base, &DayLimits::default()).expect("project");
    assert_eq!(actual.bids.len(), 10);
    assert_eq!(actual.bids[0].price_units, 91100);
    assert_eq!(actual.total_bid_quantity, 120);
    assert_eq!(actual.weighted_bid_price_units, Some(90600));
    assert_eq!(actual.asks.len(), 1);
    assert_eq!(actual.total_ask_quantity, 20);
    assert_eq!(audit.excluded_bid_quantity, 10000);
    assert_eq!(audit.excluded_ask_quantity, 30);
    assert_eq!(before, book.summary());
    assert_eq!(levels, book.levels(Side::Buy));
    assert!(book.check_invariants().is_ok());
    let (empty, _) = project(
        &book,
        &RangeBase {
            price: 1000,
            ..base
        },
        &DayLimits::default(),
    )
    .expect("empty");
    assert!(empty.bids.is_empty() && empty.asks.is_empty());
    assert_eq!(empty.total_bid_quantity, 0);
    assert_eq!(empty.weighted_bid_price_units, None);
}
