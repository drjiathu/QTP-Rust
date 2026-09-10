//! Shared scalar types and normalized order-book events.

mod types;

pub use types::{
    AddOrder, ApplySequence, BookEvent, BookKey, ChannelId, CrossingBehavior, EventMeta,
    LocalTimestampNs, Market, OrderCancel, OrderId, OrderKey, OrderReference, Price, PriceScale,
    PricingInstruction, Quantity, QuoteTimestampNs, RawSequence, Side, Symbol, Trade, TradingDay,
};
