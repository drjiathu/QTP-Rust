//! Diagnostic executable: cargo run --release --features profiling --example validation_benchmark.
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use qtp_core::{
    Market, MarketDayRequest, Symbol, SzMarketOrderPolicy, TargetUniverse, TradingDay,
    ValidationConfig, profile_validate_market_day,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    date: u32,
    #[arg(long, value_parser = ["SH", "SZ"])]
    market: String,
    #[arg(long, default_value = "/hdd/data/stock/raw_level2_parquet")]
    raw_root: PathBuf,
    #[arg(long, default_value = "target/profile-spool")]
    temp_root: PathBuf,
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    #[arg(long, default_value_t = 262_144)]
    batch_size: usize,
    #[arg(long)]
    report: PathBuf,
    #[arg(long)]
    timings: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let market = if args.market == "SH" {
        Market::Sse
    } else {
        Market::Szse
    };
    let config = ValidationConfig {
        request: MarketDayRequest {
            raw_root: args.raw_root,
            output_root: PathBuf::from("output"),
            temp_root: args.temp_root,
            trading_day: TradingDay::from_yyyymmdd(args.date).ok_or("invalid date")?,
            market,
            targets: if args.symbols.is_empty() {
                TargetUniverse::AllStocksAndEtfs
            } else {
                TargetUniverse::Symbols(args.symbols.into_iter().map(Symbol::from).collect())
            },
            snapshots: None,
            batch_size: args.batch_size,
            sz_market_order_policy: if market == Market::Szse {
                SzMarketOrderPolicy::RestAtLastTradePrice
            } else {
                SzMarketOrderPolicy::RequireEvidence
            },
        },
        continuous_lookback: None,
        continuous_lookahead: None,
        retain_matched_records: false,
        max_detail_records: Some(5000),
    };
    let started = Instant::now();
    let (report, timings) = profile_validate_market_day(&config)?;
    let output_started = Instant::now();
    if let Some(parent) = args.report.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.report, serde_json::to_vec_pretty(&report)?)?;
    let output_seconds = output_started.elapsed().as_secs_f64();
    let measurements = serde_json::json!({
        "timing_schema_version": 1,
        "stages": timings,
        "report_serialization_write_seconds": output_seconds,
        "benchmark_elapsed_before_timing_write_seconds": started.elapsed().as_secs_f64(),
    });
    if let Some(parent) = args.timings.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.timings, serde_json::to_vec_pretty(&measurements)?)?;
    println!(
        "matched={} mismatched={} excluded={} data_errors={} missing_source={}",
        report.matched,
        report.mismatched,
        report.excluded_by_status,
        report.data_errors,
        report.missing_source
    );
    if !report.is_success() {
        return Err("snapshot validation failed".into());
    }
    Ok(())
}
