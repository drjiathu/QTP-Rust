use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{Market, Symbol, TradingDay};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetUniverse {
    AllStocks,
    Symbols(Vec<Symbol>),
}

impl TargetUniverse {
    #[must_use]
    pub fn contains(&self, market: Market, symbol: &str) -> bool {
        match self {
            Self::AllStocks => is_stock_symbol(market, symbol),
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
        Market::Szse => &["000", "001", "002", "003", "300", "301"],
    };
    prefixes.iter().any(|prefix| symbol.starts_with(prefix))
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
    ContinuousEnd,
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
}

impl MarketDayRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.batch_size == 0 {
            return Err("batch size must be positive".to_owned());
        }
        if let TargetUniverse::Symbols(symbols) = &self.targets {
            if symbols.is_empty() {
                return Err("explicit symbol list must not be empty".to_owned());
            }
            for symbol in symbols {
                if !is_stock_symbol(self.market, symbol.as_str()) {
                    return Err(format!(
                        "symbol {symbol} is not a supported stock for {:?}",
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
    pub reference_root: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::{SnapshotSchedule, TargetUniverse, is_stock_symbol};
    use crate::{Market, Symbol};
    use std::time::Duration;

    #[test]
    fn recognizes_supported_stock_codes() {
        assert!(is_stock_symbol(Market::Sse, "600000"));
        assert!(is_stock_symbol(Market::Sse, "688001"));
        assert!(!is_stock_symbol(Market::Sse, "510300"));
        assert!(is_stock_symbol(Market::Szse, "000001"));
        assert!(is_stock_symbol(Market::Szse, "300001"));
        assert!(!is_stock_symbol(Market::Szse, "159915"));
    }

    #[test]
    fn explicit_universe_is_exact() {
        let universe = TargetUniverse::Symbols(vec![Symbol::from("600000")]);
        assert!(universe.contains(Market::Sse, "600000"));
        assert!(!universe.contains(Market::Sse, "600001"));
    }

    #[test]
    fn validates_snapshot_schedule() {
        assert!(SnapshotSchedule::new(Duration::from_millis(100), 10).is_ok());
        assert!(SnapshotSchedule::new(Duration::ZERO, 10).is_err());
        assert!(SnapshotSchedule::new(Duration::from_secs(1), 0).is_err());
    }
}
