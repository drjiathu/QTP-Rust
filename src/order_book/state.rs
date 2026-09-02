use slotmap::{SlotMap, new_key_type};

use crate::market_data::{OrderKey, Price, Quantity, Side};

new_key_type! {
    pub(crate) struct OrderHandle;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderLocation {
    Resting,
    Aggressive,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderState {
    pub key: OrderKey,
    pub effective_price: Price,
    pub original_quantity: Quantity,
    pub remaining_quantity: u64,
    pub previous: Option<OrderHandle>,
    pub next: Option<OrderHandle>,
    pub location: OrderLocation,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PriceLevelState {
    pub total_quantity: u64,
    pub order_count: usize,
    pub head: Option<OrderHandle>,
    pub tail: Option<OrderHandle>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TradeStatistics {
    pub last_price: Option<Price>,
    pub high_price: Option<Price>,
    pub low_price: Option<Price>,
    pub total_quantity: u64,
    pub total_turnover_units: u128,
    pub trade_count: u64,
}

pub(crate) type OrderStorage = SlotMap<OrderHandle, OrderState>;

pub(crate) fn side_matches(state: &OrderState, side: Side) -> bool {
    state.key.side == side
}
