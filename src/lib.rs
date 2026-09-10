//! Event-driven order books and Tonglian/Datayes Parquet replay for SH/SZ stocks and ETFs.
//!
//! Use [`replay_market_day`] or [`validate_market_day`] for production files,
//! or apply typed [`BookEvent`] values directly to [`OrderBook`].

#![forbid(unsafe_code)]

pub mod market_data;
pub mod order_book;
pub mod production;

pub use market_data::*;
pub use order_book::*;
pub use production::*;
