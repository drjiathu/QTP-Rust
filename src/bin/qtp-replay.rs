use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use qtp_core::{
    Market, MarketDayRequest, SnapshotSchedule, Symbol, TargetUniverse, TradingDay,
    ValidationConfig, parse_duration, replay_market_day, validate_market_day,
    validate_pre_open_market_day,
};

#[derive(Debug, Parser)]
#[command(name = "qtp-replay", version, about = "流式恢复沪深 A 股订单簿")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// 恢复单日订单簿，可选输出固定间隔及收盘截面。
    Replay {
        #[command(flatten)]
        input: InputArgs,
        /// 截面间隔，例如 100ms、30s、1m；省略时仅回放、不写截面。
        #[arg(long)]
        snapshot_interval: Option<String>,
        /// 截面盘口深度。
        #[arg(long, default_value_t = 10)]
        depth: usize,
    },
    /// 与 snapshot 的开盘前、全部连续交易帧和收盘锚点对拍。
    Validate {
        #[command(flatten)]
        input: InputArgs,
        /// 可选的 JSON 报告输出路径；指定后标准输出只显示汇总。
        #[arg(long)]
        report: Option<PathBuf>,
        /// 在 JSON 中保留每一条成功匹配明细；全市场验证通常不应启用。
        #[arg(long)]
        retain_matched_records: bool,
        /// 最多保留多少条失败/不可比较明细；聚合统计不受影响。
        #[arg(long)]
        max_detail_records: Option<usize>,
        /// 仅验证 PreOpen，回放至所选开盘帧候选窗口结束。
        #[arg(long)]
        pre_open_only: bool,
        /// 连续交易参考时间之前的候选回看窗口，例如 1s；省略时沪市为 1s、深市为 0s。
        #[arg(long)]
        continuous_lookback: Option<String>,
        /// 诊断用前向窗口覆盖；默认深市创业板 3s，其余 1s，自动按证券选择。
        #[arg(long)]
        continuous_lookahead: Option<String>,
    },
}

#[derive(Debug, Args)]
struct InputArgs {
    /// 交易日，格式 YYYYMMDD。
    #[arg(long)]
    date: u32,
    /// 市场：SH 或 SZ。
    #[arg(long, value_enum)]
    market: MarketArg,
    /// 清洗后的通联 Raw Parquet 根目录。
    #[arg(long, default_value = "/hdd/data/stock/raw_level2_parquet")]
    raw_root: PathBuf,
    /// 截面输出根目录。
    #[arg(long, default_value = "output")]
    output_root: PathBuf,
    /// 临时通道分片根目录。
    #[arg(long, default_value = "/tmp")]
    temp_root: PathBuf,
    /// 逗号分隔的股票或 ETF 代码；省略时恢复该市场全部 A 股。
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// 未指定代码时，同时选择该市场全部 ETF。
    #[arg(long, conflicts_with = "only_etfs")]
    include_etfs: bool,
    /// 只选择该市场全部 ETF；与 --symbols、--include-etfs 互斥。
    #[arg(long, conflicts_with_all = ["include_etfs", "symbols"])]
    only_etfs: bool,
    /// Arrow 每批读取行数。
    #[arg(long, default_value_t = 65_536)]
    batch_size: usize,
    /// 深市市价策略；省略时使用 rest-at-last-trade-price。
    #[arg(long, value_enum, conflicts_with = "assume_sz_contiguous_responses")]
    sz_market_order_policy: Option<SzMarketOrderPolicyArg>,
    /// 诊断用：假设深市即时成交/撤单连续发布，允许推断市价余量；不属于标准验收。
    #[arg(long)]
    assume_sz_contiguous_responses: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum MarketArg {
    #[value(name = "SH", alias = "sh")]
    Sh,
    #[value(name = "SZ", alias = "sz")]
    Sz,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum SzMarketOrderPolicyArg {
    RestAtLastTradePrice,
    RequireEvidence,
    AssumeContiguous,
}

impl SzMarketOrderPolicyArg {
    const fn policy(self) -> qtp_core::SzMarketOrderPolicy {
        match self {
            Self::RestAtLastTradePrice => qtp_core::SzMarketOrderPolicy::RestAtLastTradePrice,
            Self::RequireEvidence => qtp_core::SzMarketOrderPolicy::RequireEvidence,
            Self::AssumeContiguous => qtp_core::SzMarketOrderPolicy::AssumeContiguous,
        }
    }
}

impl MarketArg {
    const fn market(self) -> Market {
        match self {
            Self::Sh => Market::Sse,
            Self::Sz => Market::Szse,
        }
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse().command {
        Command::Replay {
            input,
            snapshot_interval,
            depth,
        } => {
            let snapshots = snapshot_interval
                .as_deref()
                .map(parse_duration)
                .transpose()?
                .map(|interval| SnapshotSchedule::new(interval, depth))
                .transpose()?;
            let request = input.request(snapshots)?;
            let report = replay_market_day(&request)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Validate {
            input,
            report,
            retain_matched_records,
            max_detail_records,
            pre_open_only,
            continuous_lookback,
            continuous_lookahead,
        } => {
            let request = input.request(None)?;
            let config = ValidationConfig {
                request,
                continuous_lookback: continuous_lookback
                    .as_deref()
                    .map(parse_duration)
                    .transpose()?,
                continuous_lookahead: continuous_lookahead
                    .as_deref()
                    .map(parse_duration)
                    .transpose()?,
                retain_matched_records,
                max_detail_records,
            };
            let validation = if pre_open_only {
                validate_pre_open_market_day(&config)?
            } else {
                validate_market_day(&config)?
            };
            let json = serde_json::to_string_pretty(&validation)?;
            if let Some(path) = report.as_ref() {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(path, json.as_bytes())?;
                println!(
                    "report={} matched={} mismatched={} excluded_by_status={} data_errors={} missing_source={} standard_acceptance={} match_rate={:.2}% match_tags={:?}",
                    path.display(),
                    validation.matched,
                    validation.mismatched,
                    validation.excluded_by_status,
                    validation.data_errors,
                    validation.missing_source,
                    validation.is_standard_acceptance(),
                    validation.match_rate.unwrap_or(0.0) * 100.0,
                    validation.match_tags,
                );
            } else {
                println!("{json}");
            }
            if !validation.is_success() {
                return Err(
                    "snapshot validation contains mismatches, data errors or missing sources"
                        .into(),
                );
            }
        }
    }
    Ok(())
}

impl InputArgs {
    fn request(
        self,
        snapshots: Option<SnapshotSchedule>,
    ) -> Result<MarketDayRequest, Box<dyn std::error::Error>> {
        if self.market.market() != Market::Szse
            && (self.sz_market_order_policy.is_some() || self.assume_sz_contiguous_responses)
        {
            return Err("SZ market-order policy is only valid for SZ requests".into());
        }
        let trading_day = TradingDay::from_yyyymmdd(self.date)
            .ok_or_else(|| format!("invalid trading date: {}", self.date))?;
        let targets = if self.symbols.is_empty() {
            if self.only_etfs {
                TargetUniverse::AllEtfs
            } else if self.include_etfs {
                TargetUniverse::AllStocksAndEtfs
            } else {
                TargetUniverse::AllStocks
            }
        } else {
            TargetUniverse::Symbols(self.symbols.into_iter().map(Symbol::from).collect())
        };
        Ok(MarketDayRequest {
            raw_root: self.raw_root,
            output_root: self.output_root,
            temp_root: self.temp_root,
            trading_day,
            market: self.market.market(),
            targets,
            snapshots,
            batch_size: self.batch_size,
            sz_market_order_policy: if self.assume_sz_contiguous_responses {
                qtp_core::SzMarketOrderPolicy::AssumeContiguous
            } else if let Some(policy) = self.sz_market_order_policy {
                policy.policy()
            } else if self.market.market() == Market::Szse {
                qtp_core::SzMarketOrderPolicy::RestAtLastTradePrice
            } else {
                qtp_core::SzMarketOrderPolicy::RequireEvidence
            },
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn input(args: &[&str]) -> InputArgs {
        let cli = Cli::try_parse_from(args).expect("valid CLI fixture");
        match cli.command {
            Command::Replay { input, .. } | Command::Validate { input, .. } => input,
        }
    }

    #[test]
    fn policy_defaults_are_market_specific() {
        for (market, expected) in [
            ("SZ", qtp_core::SzMarketOrderPolicy::RestAtLastTradePrice),
            ("SH", qtp_core::SzMarketOrderPolicy::RequireEvidence),
        ] {
            let req = input(&[
                "qtp-replay",
                "validate",
                "--date",
                "20260828",
                "--market",
                market,
            ])
            .request(None)
            .expect("request");
            assert_eq!(req.sz_market_order_policy, expected);
        }
    }

    #[test]
    fn policy_options_and_legacy_alias_are_explicit() {
        for (argument, expected) in [
            (
                "rest-at-last-trade-price",
                qtp_core::SzMarketOrderPolicy::RestAtLastTradePrice,
            ),
            (
                "require-evidence",
                qtp_core::SzMarketOrderPolicy::RequireEvidence,
            ),
            (
                "assume-contiguous",
                qtp_core::SzMarketOrderPolicy::AssumeContiguous,
            ),
        ] {
            let args = [
                "qtp-replay",
                "replay",
                "--date",
                "20260828",
                "--market",
                "SZ",
                "--sz-market-order-policy",
                argument,
            ];
            assert_eq!(
                input(&args)
                    .request(None)
                    .expect("request")
                    .sz_market_order_policy,
                expected
            );
            let mut conflict = args.to_vec();
            conflict.push("--assume-sz-contiguous-responses");
            assert!(Cli::try_parse_from(conflict).is_err());
            let mut sh = args;
            sh[5] = "SH";
            assert!(input(&sh).request(None).is_err());
        }
        let args = [
            "qtp-replay",
            "replay",
            "--date",
            "20260828",
            "--market",
            "SZ",
            "--assume-sz-contiguous-responses",
        ];
        assert_eq!(
            input(&args)
                .request(None)
                .expect("request")
                .sz_market_order_policy,
            qtp_core::SzMarketOrderPolicy::AssumeContiguous
        );
    }
}
