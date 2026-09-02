mod common;

use common::{key, legacy_context, raw_order, raw_trade, some_or_abort};
use qtp_core::{
    ApplySequence, BookEvent, LegacySteadyTimestampNs, MarketDataRecord, NormalizeError,
    OrderReference, OrderReferenceIndex, PricingInstruction, RawOrderSide, RawOrderType,
    RawTradeType, Side, normalize,
};

fn apply_sequence() -> ApplySequence {
    some_or_abort(ApplySequence::new(1))
}

#[test]
fn maps_all_legacy_order_types() {
    let cases = [
        (RawOrderType::LimitPrice, "provided"),
        (RawOrderType::ReverseBestPrice, "provided"),
        (RawOrderType::MarketPrice, "opposite"),
        (RawOrderType::ForwardBestPrice, "same"),
    ];
    for (kind, expected) in cases {
        let mut record = raw_order(1, RawOrderSide::Buy, 1, 101, 10.0, 100);
        record.kind = kind;
        let result = normalize(
            &MarketDataRecord::Order(record),
            apply_sequence(),
            &legacy_context(),
            &OrderReferenceIndex::new(),
        );
        assert!(result.is_ok_and(|event| {
            matches!(
                (event, expected),
                (
                    BookEvent::AddOrder(qtp_core::AddOrder {
                        pricing: PricingInstruction::Provided(_),
                        ..
                    }),
                    "provided"
                ) | (
                    BookEvent::AddOrder(qtp_core::AddOrder {
                        pricing: PricingInstruction::OppositeBest,
                        ..
                    }),
                    "opposite"
                ) | (
                    BookEvent::AddOrder(qtp_core::AddOrder {
                        pricing: PricingInstruction::SameSideBest,
                        ..
                    }),
                    "same"
                )
            )
        }));
    }
}

#[test]
fn order_cancellation_ignores_raw_price_and_quantity() {
    let mut record = raw_order(1, RawOrderSide::Buy, 1, 101, f64::NAN, 0);
    record.kind = RawOrderType::Cancelled;
    let result = normalize(
        &MarketDataRecord::Order(record),
        apply_sequence(),
        &legacy_context(),
        &OrderReferenceIndex::new(),
    );
    assert!(matches!(result, Ok(BookEvent::OrderCancel(_))));
}

#[test]
fn normal_trade_distinguishes_absent_resolved_and_unresolved_references() {
    let mut references = OrderReferenceIndex::new();
    let known = key(Side::Buy, 7, 101);
    references.register(known);
    let record = raw_trade(1, 10.0, 10, 101, 202);
    let result = normalize(
        &MarketDataRecord::Trade(record),
        apply_sequence(),
        &legacy_context(),
        &references,
    );
    assert!(matches!(
        result,
        Ok(BookEvent::Trade(qtp_core::Trade {
            bid_order: OrderReference::Resolved(key),
            ask_order: OrderReference::Unresolved {
                side: Side::Sell,
                ..
            },
            ..
        })) if key == known
    ));

    let absent = raw_trade(2, 10.0, 10, 0, 0);
    assert!(matches!(
        normalize(
            &MarketDataRecord::Trade(absent),
            apply_sequence(),
            &legacy_context(),
            &references,
        ),
        Ok(BookEvent::Trade(qtp_core::Trade {
            bid_order: OrderReference::Absent,
            ask_order: OrderReference::Absent,
            ..
        }))
    ));
}

#[test]
fn trade_cancellation_requires_one_resolved_target() {
    let mut references = OrderReferenceIndex::new();
    let known = key(Side::Sell, 3, 202);
    references.register(known);
    let mut record = raw_trade(1, f64::NAN, 0, 0, 202);
    record.kind = RawTradeType::Cancelled;
    assert!(matches!(
        normalize(
            &MarketDataRecord::Trade(record.clone()),
            apply_sequence(),
            &legacy_context(),
            &references,
        ),
        Ok(BookEvent::OrderCancel(qtp_core::OrderCancel {
            order_key,
            ..
        })) if order_key == known
    ));

    record.bid_order_id = 101;
    assert_eq!(
        normalize(
            &MarketDataRecord::Trade(record),
            apply_sequence(),
            &legacy_context(),
            &references,
        ),
        Err(NormalizeError::AmbiguousCancellation)
    );
}

#[test]
fn rejects_invalid_side_price_symbol_and_signed_identifiers() {
    let mut invalid_side = raw_order(1, RawOrderSide::Borrow, 1, 101, 10.0, 100);
    assert!(matches!(
        normalize(
            &MarketDataRecord::Order(invalid_side.clone()),
            apply_sequence(),
            &legacy_context(),
            &OrderReferenceIndex::new(),
        ),
        Err(NormalizeError::UnsupportedOrderSide(RawOrderSide::Borrow))
    ));

    invalid_side.side = RawOrderSide::Buy;
    invalid_side.price = 10.000_01;
    assert!(matches!(
        normalize(
            &MarketDataRecord::Order(invalid_side.clone()),
            apply_sequence(),
            &legacy_context(),
            &OrderReferenceIndex::new(),
        ),
        Err(NormalizeError::MisalignedPrice { .. })
    ));

    invalid_side.price = 10.0;
    invalid_side.channel_no = 0;
    assert!(matches!(
        normalize(
            &MarketDataRecord::Order(invalid_side.clone()),
            apply_sequence(),
            &legacy_context(),
            &OrderReferenceIndex::new(),
        ),
        Err(NormalizeError::NonPositiveSigned {
            field: "channel_no",
            ..
        })
    ));

    invalid_side.channel_no = 1;
    invalid_side.symbol = qtp_core::Symbol::from("000001.SZ");
    assert!(matches!(
        normalize(
            &MarketDataRecord::Order(invalid_side),
            apply_sequence(),
            &legacy_context(),
            &OrderReferenceIndex::new(),
        ),
        Err(NormalizeError::SymbolMismatch { .. })
    ));
}

#[test]
fn legacy_steady_time_never_reaches_core_event() {
    let mut first = raw_order(1, RawOrderSide::Buy, 1, 101, 10.0, 100);
    let mut second = first.clone();
    first.event_time.steady_time = Some(LegacySteadyTimestampNs::from_nanos(1));
    second.event_time.steady_time = Some(LegacySteadyTimestampNs::from_nanos(999));
    let references = OrderReferenceIndex::new();
    assert_eq!(
        normalize(
            &MarketDataRecord::Order(first),
            apply_sequence(),
            &legacy_context(),
            &references,
        ),
        normalize(
            &MarketDataRecord::Order(second),
            apply_sequence(),
            &legacy_context(),
            &references,
        )
    );
}
