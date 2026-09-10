#![allow(dead_code)]

use qtp_core::{
    AddOrder, ApplySequence, BookConfig, BookEvent, BookKey, ChannelId, CrossingBehavior,
    EventMeta, LocalTimestampNs, Market, OrderBook, OrderCancel, OrderId, OrderKey, OrderReference,
    Price, PriceScale, PricingInstruction, Quantity, QuoteTimestampNs, RawSequence, Side, Symbol,
    Trade, TradingDay, UnknownTradePolicy,
};

pub fn some_or_abort<T>(value: Option<T>) -> T {
    match value {
        Some(value) => value,
        None => std::process::abort(),
    }
}

pub fn book_key() -> BookKey {
    BookKey {
        market: Market::Sse,
        trading_day: some_or_abort(TradingDay::from_yyyymmdd(20_250_102)),
        symbol: Symbol::from("600000.SH"),
    }
}

pub fn price_scale() -> PriceScale {
    some_or_abort(PriceScale::from_decimal_places(4))
}

pub fn strict_book() -> OrderBook {
    OrderBook::new(BookConfig::new(book_key(), price_scale()))
}

pub fn compatibility_book() -> OrderBook {
    OrderBook::new(
        BookConfig::new(book_key(), price_scale())
            .with_unknown_trade_policy(UnknownTradePolicy::UpdateKnownAndStatistics),
    )
}

pub fn key(side: Side, channel: u32, order_id: u64) -> OrderKey {
    OrderKey {
        channel_id: some_or_abort(ChannelId::new(channel)),
        side,
        order_id: some_or_abort(OrderId::new(order_id)),
    }
}

pub fn price(units: i64) -> Price {
    some_or_abort(Price::from_units(units))
}

pub fn quantity(value: u64) -> Quantity {
    some_or_abort(Quantity::new(value))
}

pub fn meta(apply_sequence: u64, raw_sequence: u64) -> EventMeta {
    EventMeta {
        book_key: book_key(),
        raw_sequence: some_or_abort(RawSequence::new(raw_sequence)),
        apply_sequence: some_or_abort(ApplySequence::new(apply_sequence)),
        local_time: LocalTimestampNs::from_nanos(1_000 + raw_sequence as i64),
        quote_time: QuoteTimestampNs::from_nanos(2_000 + raw_sequence as i64),
    }
}

pub fn add(
    apply_sequence: u64,
    raw_sequence: u64,
    order_key: OrderKey,
    price_units: i64,
    quantity_value: u64,
) -> BookEvent {
    BookEvent::AddOrder(AddOrder {
        meta: meta(apply_sequence, raw_sequence),
        order_key,
        pricing: PricingInstruction::Provided(price(price_units)),
        crossing: CrossingBehavior::Rest,
        quantity: quantity(quantity_value),
    })
}

pub fn add_with_crossing(
    apply_sequence: u64,
    raw_sequence: u64,
    order_key: OrderKey,
    price_units: i64,
    quantity_value: u64,
) -> BookEvent {
    BookEvent::AddOrder(AddOrder {
        meta: meta(apply_sequence, raw_sequence),
        order_key,
        pricing: PricingInstruction::Provided(price(price_units)),
        crossing: CrossingBehavior::HideIfCrossing,
        quantity: quantity(quantity_value),
    })
}

pub fn cancel(apply_sequence: u64, raw_sequence: u64, order_key: OrderKey) -> BookEvent {
    BookEvent::OrderCancel(OrderCancel {
        meta: meta(apply_sequence, raw_sequence),
        order_key,
    })
}

pub fn trade(
    apply_sequence: u64,
    raw_sequence: u64,
    bid: OrderReference,
    ask: OrderReference,
    price_units: i64,
    quantity_value: u64,
) -> BookEvent {
    BookEvent::Trade(Trade {
        meta: meta(apply_sequence, raw_sequence),
        bid_order: bid,
        ask_order: ask,
        price: price(price_units),
        quantity: quantity(quantity_value),
    })
}
