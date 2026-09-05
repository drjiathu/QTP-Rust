mod common;

use common::{
    add, add_with_crossing, cancel, compatibility_book, key, meta, price, strict_book, trade,
};
use qtp_core::{
    AddOrder, ApplyOutcome, BookError, BookEvent, CrossingBehavior, OrderLocation, OrderReference,
    PricingInstruction, Side,
};

#[test]
fn maintains_price_priority_fifo_and_aggregates() {
    let mut book = strict_book();
    let first = key(Side::Buy, 1, 101);
    let second = key(Side::Buy, 1, 102);

    assert!(book.apply(add(1, 1, first, 100_000, 100)).is_ok());
    assert!(book.apply(add(2, 2, second, 100_000, 50)).is_ok());

    let levels = book.levels(Side::Buy);
    assert_eq!(levels.len(), 1);
    assert_eq!(levels[0].total_quantity, 150);
    assert_eq!(levels[0].order_count, 2);
    let orders = book.orders_at(Side::Buy, price(100_000));
    assert_eq!(
        orders.iter().map(|order| order.key).collect::<Vec<_>>(),
        vec![first, second]
    );
    assert!(book.check_invariants().is_ok());
}

#[test]
fn partially_filled_order_is_cancelled_for_all_remaining_quantity() {
    let mut book = strict_book();
    let bid = key(Side::Buy, 1, 201);
    assert!(book.apply(add(1, 1, bid, 100_000, 100)).is_ok());
    assert!(
        book.apply(trade(
            2,
            2,
            OrderReference::Resolved(bid),
            OrderReference::Absent,
            100_000,
            40,
        ))
        .is_ok()
    );

    let outcome = book.apply(cancel(3, 3, bid));
    assert_eq!(
        outcome,
        Ok(ApplyOutcome::Cancelled {
            key: bid,
            cancelled_quantity: 60,
        })
    );
    assert!(book.order(&bid).is_none());
    assert!(book.levels(Side::Buy).is_empty());
}

#[test]
fn hidden_crossing_order_reenters_after_opposite_level_is_consumed() {
    let mut book = strict_book();
    let ask = key(Side::Sell, 1, 301);
    let bid = key(Side::Buy, 1, 302);
    assert!(book.apply(add(1, 1, ask, 100_000, 100)).is_ok());
    let added = book.apply(add_with_crossing(2, 2, bid, 101_000, 150));
    assert!(matches!(
        added,
        Ok(ApplyOutcome::Added {
            location: OrderLocation::Aggressive,
            ..
        })
    ));
    assert!(book.levels(Side::Buy).is_empty());

    assert!(
        book.apply(trade(
            3,
            3,
            OrderReference::Resolved(bid),
            OrderReference::Resolved(ask),
            100_000,
            100,
        ))
        .is_ok()
    );
    let bid_view = book.order(&bid);
    assert!(matches!(
        bid_view,
        Some(view)
            if view.location == OrderLocation::Resting
                && view.remaining_quantity == 50
    ));
    assert_eq!(book.levels(Side::Buy)[0].total_quantity, 50);
    assert!(book.order(&ask).is_none());
}

#[test]
fn always_hidden_order_never_rests_or_reenters() {
    let mut book = strict_book();
    let bid = key(Side::Buy, 1, 303);
    let added = book.apply(BookEvent::AddOrder(AddOrder {
        meta: meta(1, 1),
        order_key: bid,
        pricing: PricingInstruction::Unpriced,
        crossing: CrossingBehavior::AlwaysHide,
        quantity: common::quantity(100),
    }));
    assert!(matches!(
        added,
        Ok(ApplyOutcome::Added {
            effective_price: None,
            location: OrderLocation::Aggressive,
            ..
        })
    ));
    assert!(book.levels(Side::Buy).is_empty());

    assert!(
        book.apply(trade(
            2,
            2,
            OrderReference::Resolved(bid),
            OrderReference::Absent,
            55_000,
            40,
        ))
        .is_ok()
    );
    assert!(matches!(
        book.order(&bid),
        Some(order)
            if order.location == OrderLocation::Aggressive
                && order.remaining_quantity == 60
    ));
    assert!(book.levels(Side::Buy).is_empty());
    assert!(book.apply(cancel(3, 3, bid)).is_ok());
    assert!(book.order(&bid).is_none());
}

#[test]
fn market_remainder_reprices_to_last_trade_and_reenters_after_crossing_ends() {
    let mut book = strict_book();
    let ask = key(Side::Sell, 1, 304);
    let market_bid = key(Side::Buy, 1, 305);
    assert!(book.apply(add(1, 1, ask, 55_000, 100)).is_ok());
    let added = book.apply(BookEvent::AddOrder(AddOrder {
        meta: meta(2, 2),
        order_key: market_bid,
        // The raw market-order boundary is not its eventual resting price.
        pricing: PricingInstruction::Provided(price(60_000)),
        crossing: CrossingBehavior::RestAtLastTradePrice,
        quantity: common::quantity(150),
    }));
    assert!(matches!(
        added,
        Ok(ApplyOutcome::Added {
            effective_price: Some(value),
            location: OrderLocation::Aggressive,
            ..
        }) if value == price(60_000)
    ));

    assert!(
        book.apply(trade(
            3,
            3,
            OrderReference::Resolved(market_bid),
            OrderReference::Resolved(ask),
            55_000,
            40,
        ))
        .is_ok()
    );
    assert!(matches!(
        book.order(&market_bid),
        Some(order)
            if order.location == OrderLocation::Aggressive
                && order.effective_price == Some(price(55_000))
                && order.remaining_quantity == 110
    ));
    assert!(book.levels(Side::Buy).is_empty());

    assert!(
        book.apply(trade(
            4,
            4,
            OrderReference::Resolved(market_bid),
            OrderReference::Resolved(ask),
            55_000,
            60,
        ))
        .is_ok()
    );
    assert!(matches!(
        book.order(&market_bid),
        Some(order)
            if order.location == OrderLocation::Resting
                && order.effective_price == Some(price(55_000))
                && order.remaining_quantity == 50
    ));
    assert_eq!(book.levels(Side::Buy)[0].price, price(55_000));
    assert_eq!(book.levels(Side::Buy)[0].total_quantity, 50);
    assert!(book.check_invariants().is_ok());
}

#[test]
fn unpriced_visible_order_is_rejected_atomically() {
    let mut book = strict_book();
    let bid = key(Side::Buy, 1, 304);
    let event = BookEvent::AddOrder(AddOrder {
        meta: meta(1, 1),
        order_key: bid,
        pricing: PricingInstruction::Unpriced,
        crossing: CrossingBehavior::Rest,
        quantity: common::quantity(100),
    });
    assert_eq!(book.apply(event), Err(BookError::UnpricedVisibleOrder));
    assert!(book.is_empty());
    assert!(book.last_applied_meta().is_none());
}

#[test]
fn unknown_trade_is_atomic_in_strict_mode() {
    let mut book = strict_book();
    let before = book.summary();
    let unknown_id = common::some_or_abort(qtp_core::OrderId::new(999));
    let error = book.apply(trade(
        1,
        1,
        OrderReference::Unresolved {
            side: Side::Buy,
            order_id: unknown_id,
        },
        OrderReference::Absent,
        100_000,
        10,
    ));
    assert_eq!(
        error,
        Err(BookError::UnknownTradeReference {
            side: Side::Buy,
            order_id: unknown_id,
        })
    );
    assert_eq!(book.summary(), before);
}

#[test]
fn compatibility_policy_counts_unknown_trade_without_mutating_orders() {
    let mut book = compatibility_book();
    let unknown_id = common::some_or_abort(qtp_core::OrderId::new(999));
    let outcome = book.apply(trade(
        1,
        1,
        OrderReference::Unresolved {
            side: Side::Buy,
            order_id: unknown_id,
        },
        OrderReference::Absent,
        100_000,
        10,
    ));
    assert_eq!(
        outcome,
        Ok(ApplyOutcome::Traded {
            bid_reduction: 0,
            ask_reduction: 0,
        })
    );
    assert_eq!(book.summary().statistics.trade_count, 1);
    assert_eq!(book.summary().statistics.total_quantity, 10);
}

#[test]
fn overfill_and_duplicate_order_fail_without_advancing_metadata() {
    let mut book = strict_book();
    let bid = key(Side::Buy, 1, 401);
    assert!(book.apply(add(1, 1, bid, 100_000, 20)).is_ok());
    let before = book.summary();
    assert!(matches!(
        book.apply(trade(
            2,
            2,
            OrderReference::Resolved(bid),
            OrderReference::Absent,
            100_000,
            21,
        )),
        Err(BookError::TradeOverfill { .. })
    ));
    assert_eq!(book.summary(), before);

    assert!(book.apply(cancel(2, 2, bid)).is_ok());
    let after_cancel = book.summary();
    assert_eq!(
        book.apply(add(3, 3, bid, 100_000, 20)),
        Err(BookError::DuplicateOrder(bid))
    );
    assert_eq!(book.summary(), after_cancel);
}

#[test]
fn state_dependent_price_requires_the_referenced_side() {
    let mut book = strict_book();
    let bid = key(Side::Buy, 1, 501);
    let event = BookEvent::AddOrder(AddOrder {
        meta: meta(1, 1),
        order_key: bid,
        pricing: PricingInstruction::OppositeBest,
        crossing: CrossingBehavior::Rest,
        quantity: common::quantity(10),
    });
    assert!(matches!(
        book.apply(event),
        Err(BookError::ReferencePriceUnavailable { .. })
    ));
    assert!(book.is_empty());
    assert!(book.last_applied_meta().is_none());
}

#[test]
fn local_time_changes_only_metadata() {
    let bid = key(Side::Buy, 1, 601);
    let mut first_event = add(1, 1, bid, 100_000, 20);
    let mut second_event = first_event.clone();
    if let BookEvent::AddOrder(event) = &mut first_event {
        event.meta.local_time = qtp_core::LocalTimestampNs::from_nanos(100);
    }
    if let BookEvent::AddOrder(event) = &mut second_event {
        event.meta.local_time = qtp_core::LocalTimestampNs::from_nanos(200);
    }
    let mut first = strict_book();
    let mut second = strict_book();
    assert!(first.apply(first_event).is_ok());
    assert!(second.apply(second_event).is_ok());

    assert_eq!(first.depth(10), second.depth(10));
    assert_eq!(first.order(&bid), second.order(&bid));
    assert_eq!(first.summary().statistics, second.summary().statistics);
    assert_ne!(
        first.summary().last_local_time,
        second.summary().last_local_time
    );
}

#[test]
fn cancelling_an_aggressive_order_does_not_change_visible_levels() {
    let mut book = strict_book();
    let ask = key(Side::Sell, 1, 701);
    let hidden_bid = key(Side::Buy, 1, 702);
    assert!(book.apply(add(1, 1, ask, 100_000, 100)).is_ok());
    assert!(
        book.apply(add_with_crossing(2, 2, hidden_bid, 101_000, 50))
            .is_ok()
    );
    let before = book.depth(10);

    assert_eq!(
        book.apply(cancel(3, 3, hidden_bid)),
        Ok(ApplyOutcome::Cancelled {
            key: hidden_bid,
            cancelled_quantity: 50,
        })
    );
    assert_eq!(book.depth(10), before);
    assert!(book.order(&hidden_bid).is_none());
    assert!(book.order(&ask).is_some());
}

#[test]
fn partial_trade_keeps_fifo_position() {
    let mut book = strict_book();
    let first = key(Side::Sell, 1, 801);
    let second = key(Side::Sell, 1, 802);
    assert!(book.apply(add(1, 1, first, 100_000, 100)).is_ok());
    assert!(book.apply(add(2, 2, second, 100_000, 100)).is_ok());
    assert!(
        book.apply(trade(
            3,
            3,
            OrderReference::Absent,
            OrderReference::Resolved(first),
            100_000,
            40,
        ))
        .is_ok()
    );
    assert_eq!(
        book.orders_at(Side::Sell, price(100_000))
            .into_iter()
            .map(|order| order.key)
            .collect::<Vec<_>>(),
        vec![first, second]
    );
}

#[test]
fn unknown_cancel_and_wrong_sequence_are_atomic() {
    let mut book = strict_book();
    let existing = key(Side::Buy, 1, 901);
    let unknown = key(Side::Buy, 1, 999);
    assert!(book.apply(add(1, 1, existing, 100_000, 100)).is_ok());
    let before = book.summary();
    assert_eq!(
        book.apply(cancel(2, 2, unknown)),
        Err(BookError::UnknownCancellation(unknown))
    );
    assert_eq!(book.summary(), before);

    let wrong_sequence = add(3, 3, key(Side::Buy, 1, 902), 99_000, 100);
    assert!(matches!(
        book.apply(wrong_sequence),
        Err(BookError::InvalidApplySequence { .. })
    ));
    assert_eq!(book.summary(), before);
}

#[test]
fn provided_crossing_order_rests_when_crossing_behavior_is_rest() {
    let mut book = strict_book();
    let ask = key(Side::Sell, 1, 1_001);
    let bid = key(Side::Buy, 1, 1_002);
    assert!(book.apply(add(1, 1, ask, 100_000, 100)).is_ok());
    let outcome = book.apply(add(2, 2, bid, 101_000, 100));
    assert!(matches!(
        outcome,
        Ok(ApplyOutcome::Added {
            location: OrderLocation::Resting,
            ..
        })
    ));
    assert_eq!(book.levels(Side::Buy)[0].price, price(101_000));
}
