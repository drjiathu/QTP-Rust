//! Deterministic order-book state machine and read-only views.

mod book;
mod error;
mod state;
mod view;

pub use book::{BookConfig, OrderBook, UnknownTradePolicy};
pub use error::BookError;
pub use state::OrderLocation;
pub use view::{ApplyOutcome, BookSummary, DepthView, LevelView, OrderView, TradeStatisticsView};
