//! Rust implementation of the QTP core.
//!
//! The first delivery target reconstructs one A-share order book from legacy
//! order and trade records through explicit normalization and replay layers.

#![forbid(unsafe_code)]

pub mod legacy;
pub mod market_data;
pub mod order_book;
pub mod production;

pub use legacy::{
    LegacyContext, LegacyQtpRules, LegacyReplay, NormalizeError, OrderReferenceIndex, ReplayError,
    ReplayErrorKind, ReplaySource, normalize,
};
pub use market_data::*;
pub use order_book::*;
pub use production::*;
