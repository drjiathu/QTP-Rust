use std::path::PathBuf;

use thiserror::Error;

use crate::{BookError, Market, Symbol};

#[derive(Debug, Error)]
pub enum ProductionError {
    #[error(
        "unresolved Shenzhen order {order_sequence} for {symbol} in channel {channel} after sequence {last_sequence}: {detail}"
    )]
    UnresolvedSzOrder {
        symbol: String,
        channel: u32,
        order_sequence: u64,
        last_sequence: u64,
        detail: String,
    },
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Parquet error at {path}: {source}")]
    Parquet {
        path: PathBuf,
        #[source]
        source: parquet::errors::ParquetError,
    },
    #[error("Arrow error while processing {context}: {source}")]
    Arrow {
        context: &'static str,
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("invalid {field} in {path} at source row {source_row}: {detail}")]
    InvalidField {
        path: PathBuf,
        source_row: u64,
        field: &'static str,
        detail: String,
    },
    #[error("schema mismatch in {path}: {detail}")]
    Schema { path: PathBuf, detail: String },
    #[error("missing input file: {0}")]
    MissingInput(PathBuf),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error(
        "non-increasing {sequence_name} in {market:?} channel {channel}: {previous} then {current}"
    )]
    NonIncreasingSequence {
        market: Market,
        channel: u32,
        sequence_name: &'static str,
        previous: u64,
        current: u64,
    },
    #[error("ambiguous Shenzhen ApplSeqNum {sequence} in channel {channel}")]
    AmbiguousSequence { channel: u32, sequence: u64 },
    #[error("quote time regressed for {symbol}: {previous} then {current}")]
    QuoteTimeRegression {
        symbol: Symbol,
        previous: i64,
        current: i64,
    },
    #[error("symbol {symbol} appeared in channels {first_channel} and {second_channel}")]
    SymbolChannelConflict {
        symbol: Symbol,
        first_channel: u32,
        second_channel: u32,
    },
    #[error("normalization failed for {symbol} sequence {sequence}: {detail}")]
    Normalize {
        symbol: Symbol,
        sequence: u64,
        detail: String,
    },
    #[error("order-book apply failed for {symbol} sequence {sequence}: {source}")]
    Apply {
        symbol: Symbol,
        sequence: u64,
        #[source]
        source: BookError,
    },
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("replay failed: {source}; retained channel spool at {spool_path}")]
    ReplayFailed {
        spool_path: PathBuf,
        #[source]
        source: Box<Self>,
    },
    #[error("integer arithmetic overflow while computing {0}")]
    Arithmetic(&'static str),
}

impl ProductionError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    pub(crate) fn parquet(path: impl Into<PathBuf>, source: parquet::errors::ParquetError) -> Self {
        Self::Parquet {
            path: path.into(),
            source,
        }
    }
}
