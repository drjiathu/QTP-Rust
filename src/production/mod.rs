//! Streaming Tonglian/Datayes Parquet replay and snapshot validation.

mod error;
mod input;
mod replay;
mod snapshot;
mod spool;
mod time;
mod types;
mod validation;
mod writer;

pub use error::ProductionError;
pub use replay::{ReplayReport, replay_market_day};
pub use snapshot::{BookSnapshot, SnapshotBookView, SnapshotLevel};
pub use time::{parse_duration, parse_market_timestamp};
pub use types::{
    MarketDayRequest, SnapshotKind, SnapshotSchedule, TargetUniverse, ValidationAnchor,
    ValidationConfig,
};
pub use validation::{
    FieldDifference, ValidationOutcome, ValidationRecord, ValidationReport, validate_market_day,
};

/// Integer price units used by the production Parquet replay and validation pipeline.
///
/// One unit represents CNY 0.0001. This is a storage scale, not an exchange tick size.
pub const PRODUCTION_PRICE_MULTIPLIER: u64 = 10_000;
pub(crate) const PRODUCTION_PRICE_DECIMAL_PLACES: u8 = 4;
