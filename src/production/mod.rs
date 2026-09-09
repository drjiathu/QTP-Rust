//! Streaming Tonglian/Datayes Parquet replay and snapshot validation.

mod error;
mod input;
mod replay;
mod sequence;
mod snapshot;
mod spool;
mod sz_pending;
mod time;
mod types;
mod validation;
mod writer;

pub use error::ProductionError;
pub use replay::{ReplayReport, replay_market_day};
pub use sequence::{SequenceRegression, SequenceRepair};
pub use snapshot::{BookSnapshot, SnapshotBookView, SnapshotLevel, SnapshotLevels};
pub use time::{parse_duration, parse_market_timestamp};
pub use types::{
    MarketDayRequest, SnapshotKind, SnapshotSchedule, SzMarketOrderPolicy, TargetUniverse,
    ValidationAnchor, ValidationConfig, is_chinext_symbol, is_etf_symbol, is_stock_symbol,
    is_supported_symbol,
};
pub use validation::{
    ClosePriceBandAudit, FieldDifference, PhaseAudit, PhaseIssue, ValidationCounts,
    ValidationOutcome, ValidationRecord, ValidationReport, validate_market_day,
    validate_pre_open_market_day,
};

#[cfg(feature = "profiling")]
pub use validation::profiling::{ValidationTimings, profile_validate_market_day};

/// Integer price units used by the production Parquet replay and validation pipeline.
///
/// One unit represents CNY 0.0001. This is a storage scale, not an exchange tick size.
pub const PRODUCTION_PRICE_MULTIPLIER: u64 = 10_000;
pub(crate) const PRODUCTION_PRICE_DECIMAL_PLACES: u8 = 4;

// Borrowed entry lookup allocates an owned key only on insertion. Keep the
// standard randomized hasher; this is not a hash-algorithm optimization.
pub(crate) type SymbolMap<T> =
    hashbrown::HashMap<String, T, std::collections::hash_map::RandomState>;
