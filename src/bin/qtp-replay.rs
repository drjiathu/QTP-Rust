use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use qtp_core::{
    Market, MarketDayRequest, SnapshotSchedule, Symbol, TargetUniverse, TradingDay,
    ValidationConfig, parse_duration, replay_market_day, validate_market_day,
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
    /// 与官方 snapshot 的开盘前、连续交易结束和收盘锚点对拍。
    Validate {
        #[command(flatten)]
        input: InputArgs,
        /// 官方 snapshot 根目录。
        #[arg(long, default_value = "/hdd/data/stock/snapshot")]
        reference_root: PathBuf,
        /// 可选的 JSON 报告输出路径；指定后标准输出只显示汇总。
        #[arg(long)]
        report: Option<PathBuf>,
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
    /// 逗号分隔的股票代码；省略时恢复该市场全部 A 股。
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// Arrow 每批读取行数。
    #[arg(long, default_value_t = 65_536)]
    batch_size: usize,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum MarketArg {
    #[value(name = "SH", alias = "sh")]
    Sh,
    #[value(name = "SZ", alias = "sz")]
    Sz,
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
            reference_root,
            report,
        } => {
            let request = input.request(None)?;
            let validation = validate_market_day(&ValidationConfig {
                request,
                reference_root,
            })?;
            let json = serde_json::to_string_pretty(&validation)?;
            if let Some(path) = report.as_ref() {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(path, json.as_bytes())?;
                println!(
                    "report={} matched={} mismatched={} not_comparable={} match_rate={:.2}%",
                    path.display(),
                    validation.matched,
                    validation.mismatched,
                    validation.not_comparable,
                    validation.match_rate.unwrap_or(0.0) * 100.0,
                );
            } else {
                println!("{json}");
            }
            if !validation.is_success() {
                return Err("snapshot validation contains mismatches".into());
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
        let trading_day = TradingDay::from_yyyymmdd(self.date)
            .ok_or_else(|| format!("invalid trading date: {}", self.date))?;
        let targets = if self.symbols.is_empty() {
            TargetUniverse::AllStocks
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
        })
    }
}
