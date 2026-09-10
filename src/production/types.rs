use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{Market, Symbol, TradingDay};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetUniverse {
    AllStocks,
    AllEtfs,
    AllStocksAndEtfs,
    Symbols(Vec<Symbol>),
}

impl TargetUniverse {
    #[must_use]
    pub fn contains(&self, market: Market, symbol: &str) -> bool {
        match self {
            Self::AllStocks => is_stock_symbol(market, symbol),
            Self::AllEtfs => is_etf_symbol(market, symbol),
            Self::AllStocksAndEtfs => is_supported_symbol(market, symbol),
            Self::Symbols(symbols) => symbols.iter().any(|candidate| candidate.as_str() == symbol),
        }
    }
}

#[must_use]
pub fn is_stock_symbol(market: Market, symbol: &str) -> bool {
    if symbol.len() != 6 || !symbol.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    let prefixes: &[&str] = match market {
        Market::Sse => &["600", "601", "603", "605", "688", "689"],
        Market::Szse => &["000", "001", "002", "003", "300", "301", "302"],
    };
    prefixes.iter().any(|prefix| symbol.starts_with(prefix))
}

#[must_use]
pub fn is_etf_symbol(market: Market, symbol: &str) -> bool {
    if symbol.len() != 6 || !symbol.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    let prefixes: &[&str] = match market {
        Market::Sse => &[
            "510", "511", "512", "513", "515", "516", "517", "518", "560", "561", "562", "563",
            "588",
        ],
        Market::Szse => &["159"],
    };
    prefixes.iter().any(|prefix| symbol.starts_with(prefix))
}

#[must_use]
pub fn is_supported_symbol(market: Market, symbol: &str) -> bool {
    is_stock_symbol(market, symbol) || is_etf_symbol(market, symbol)
}

/// Current SZ ChiNext code ranges; not the SSE STAR Market.
#[must_use]
pub fn is_chinext_symbol(symbol: &str) -> bool {
    is_stock_symbol(Market::Szse, symbol)
        && ["300", "301", "302"]
            .iter()
            .any(|prefix| symbol.starts_with(prefix))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotKind {
    Scheduled,
    MarketClose,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationAnchor {
    PreOpen,
    ContinuousTrading,
    MarketClose,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotSchedule {
    pub interval: Duration,
    pub depth: usize,
}

impl SnapshotSchedule {
    pub fn new(interval: Duration, depth: usize) -> Result<Self, String> {
        if interval.is_zero() {
            return Err("snapshot interval must be positive".to_owned());
        }
        if interval.as_nanos() > i64::MAX as u128 {
            return Err("snapshot interval is too large".to_owned());
        }
        if depth == 0 {
            return Err("snapshot depth must be positive".to_owned());
        }
        Ok(Self { interval, depth })
    }
}

/// How to handle market remainders when the source omits execution qualifiers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SzMarketOrderPolicy {
    /// Practical replay: hidden orders rest at their own latest execution price
    /// once they no longer cross. Does not certify exact intermediate states.
    RestAtLastTradePrice,
    /// Reject remainders whose disposition cannot be established from the feed.
    #[default]
    RequireEvidence,
    /// Diagnostic only: assume one order's immediate responses are contiguous.
    /// This is not a verified Tonglian protocol guarantee.
    AssumeContiguous,
}

#[derive(Clone, Debug)]
pub struct MarketDayRequest {
    pub raw_root: PathBuf,
    pub output_root: PathBuf,
    pub temp_root: PathBuf,
    pub trading_day: TradingDay,
    pub market: Market,
    pub targets: TargetUniverse,
    pub snapshots: Option<SnapshotSchedule>,
    pub batch_size: usize,
    pub sz_market_order_policy: SzMarketOrderPolicy,
}

impl MarketDayRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.market != Market::Szse
            && self.sz_market_order_policy != SzMarketOrderPolicy::RequireEvidence
        {
            return Err("SZ market-order policy is only valid for SZ requests".to_owned());
        }
        if self.batch_size == 0 {
            return Err("batch size must be positive".to_owned());
        }
        if let TargetUniverse::Symbols(symbols) = &self.targets {
            if symbols.is_empty() {
                return Err("explicit symbol list must not be empty".to_owned());
            }
            for symbol in symbols {
                if !is_supported_symbol(self.market, symbol.as_str()) {
                    return Err(format!(
                        "symbol {symbol} is not a supported stock or ETF for {:?}",
                        self.market
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ValidationConfig {
    pub request: MarketDayRequest,
    /// Optional lookback before each continuous snapshot timestamp.
    ///
    /// `None` keeps the market default: SSE uses one second and SZSE uses zero.
    /// It shifts only the window start, without changing the per-symbol horizon
    /// or replay ordering.
    pub continuous_lookback: Option<Duration>,
    /// Optional diagnostic horizon after each continuous snapshot timestamp.
    ///
    /// `None` selects the standard per-symbol horizon: SZ ChiNext uses three
    /// seconds, SZ ETFs use 1,100 milliseconds, and other supported securities
    /// use one second. An explicit
    /// override is reported as diagnostic mode, not standard acceptance.
    pub continuous_lookahead: Option<Duration>,
    /// Keep one report record for every successful reference frame.
    ///
    /// Full-market validation should normally disable this: aggregate counts
    /// still include every frame, while `records` retains only mismatches and
    /// non-comparable cases.
    pub retain_matched_records: bool,
    /// Optional cap for retained mismatch/not-comparable detail records.
    /// Aggregate counts always cover every evaluated reference frame.
    pub max_detail_records: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::{SnapshotSchedule, TargetUniverse, is_etf_symbol, is_stock_symbol};
    use crate::{Market, Symbol};
    use std::time::Duration;

    #[test]
    fn recognizes_supported_stock_codes() {
        assert!(is_stock_symbol(Market::Sse, "600000"));
        assert!(is_stock_symbol(Market::Sse, "688001"));
        assert!(!is_stock_symbol(Market::Sse, "510300"));
        assert!(is_etf_symbol(Market::Sse, "510300"));
        assert!(is_stock_symbol(Market::Szse, "000001"));
        assert!(is_stock_symbol(Market::Szse, "300001"));
        assert!(is_stock_symbol(Market::Szse, "302132"));
        assert!(!is_stock_symbol(Market::Szse, "159915"));
        assert!(is_etf_symbol(Market::Szse, "159915"));
    }

    #[test]
    fn explicit_universe_is_exact() {
        let universe = TargetUniverse::Symbols(vec![Symbol::from("600000")]);
        assert!(universe.contains(Market::Sse, "600000"));
        assert!(!universe.contains(Market::Sse, "600001"));
    }

    #[test]
    fn combined_universe_includes_stocks_and_etfs() {
        let universe = TargetUniverse::AllStocksAndEtfs;
        assert!(universe.contains(Market::Sse, "600000"));
        assert!(universe.contains(Market::Sse, "510300"));
        assert!(universe.contains(Market::Szse, "000001"));
        assert!(universe.contains(Market::Szse, "159915"));
        assert!(!universe.contains(Market::Sse, "110059"));
    }

    #[test]
    fn etf_universe_excludes_stocks() {
        let universe = TargetUniverse::AllEtfs;
        assert!(universe.contains(Market::Szse, "159001"));
        assert!(!universe.contains(Market::Szse, "000001"));
    }

    #[test]
    fn validates_snapshot_schedule() {
        assert!(SnapshotSchedule::new(Duration::from_millis(100), 10).is_ok());
        assert!(SnapshotSchedule::new(Duration::ZERO, 10).is_err());
        assert!(SnapshotSchedule::new(Duration::from_secs(1), 0).is_err());
    }
}
