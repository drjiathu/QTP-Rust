use thiserror::Error;

use crate::market_data::{ApplySequence, BookKey, OrderId, OrderKey, Price, Side};

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BookError {
    #[error("event belongs to {actual:?}, expected {expected:?}")]
    BookKeyMismatch { expected: BookKey, actual: BookKey },
    #[error("expected apply sequence {expected:?}, got {actual:?}")]
    InvalidApplySequence {
        expected: ApplySequence,
        actual: ApplySequence,
    },
    #[error("apply sequence overflow")]
    ApplySequenceOverflow,
    #[error("order key has already been used: {0:?}")]
    DuplicateOrder(OrderKey),
    #[error("reference price is unavailable for {side:?} {instruction}")]
    ReferencePriceUnavailable {
        side: Side,
        instruction: &'static str,
    },
    #[error("unknown cancellation target: {0:?}")]
    UnknownCancellation(OrderKey),
    #[error("trade reference side {actual:?} does not match expected side {expected:?}")]
    InvalidTradeReferenceSide { expected: Side, actual: Side },
    #[error("unknown trade reference for {side:?} order {order_id:?}")]
    UnknownTradeReference { side: Side, order_id: OrderId },
    #[error(
        "trade quantity exceeds remaining quantity for {key:?}: remaining={remaining}, trade={trade}"
    )]
    TradeOverfill {
        key: OrderKey,
        remaining: u64,
        trade: u64,
    },
    #[error("arithmetic overflow while updating {0}")]
    ArithmeticOverflow(&'static str),
    #[error("arithmetic underflow while updating {0}")]
    ArithmeticUnderflow(&'static str),
    #[error("internal order-book invariant failed: {0}")]
    InvariantViolation(&'static str),
    #[error("price level does not exist: {side:?} {price:?}")]
    MissingPriceLevel { side: Side, price: Price },
}
