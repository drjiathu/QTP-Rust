use crate::market_data::{
    ApplySequence, BookKey, LocalTimestampNs, OrderKey, Price, Quantity, QuoteTimestampNs,
    RawSequence, Side,
};

use super::state::OrderLocation;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LevelView {
    pub side: Side,
    pub price: Price,
    pub total_quantity: u64,
    pub order_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderView {
    pub key: OrderKey,
    pub effective_price: Price,
    pub original_quantity: Quantity,
    pub remaining_quantity: u64,
    pub location: OrderLocation,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DepthView {
    pub bids: Vec<LevelView>,
    pub asks: Vec<LevelView>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TradeStatisticsView {
    pub last_price: Option<Price>,
    pub high_price: Option<Price>,
    pub low_price: Option<Price>,
    pub total_quantity: u64,
    pub total_turnover_units: u128,
    pub trade_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookSummary {
    pub book_key: BookKey,
    pub last_raw_sequence: Option<RawSequence>,
    pub last_apply_sequence: Option<ApplySequence>,
    pub last_local_time: Option<LocalTimestampNs>,
    pub last_quote_time: Option<QuoteTimestampNs>,
    pub best_bid: Option<LevelView>,
    pub best_ask: Option<LevelView>,
    pub active_order_count: usize,
    pub statistics: TradeStatisticsView,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Added {
        key: OrderKey,
        effective_price: Price,
        quantity: Quantity,
        location: OrderLocation,
    },
    Cancelled {
        key: OrderKey,
        cancelled_quantity: u64,
    },
    Traded {
        bid_reduction: u64,
        ask_reduction: u64,
    },
}
