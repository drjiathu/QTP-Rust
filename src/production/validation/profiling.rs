//! Opt-in benchmark clocks, deliberately absent from default replay reports.
use std::time::{Duration, Instant};

use serde::Serialize;

use super::{ValidationConfig, ValidationObserver, ValidationReport, load_references};
use crate::production::replay::{ObservationPoint, StateObserver, profile_run_market_day};
use crate::{OrderBook, ProductionError};

/// Exclusive wall-time attribution within one validation invocation.
///
/// Replay includes decoding, normalization and book updates, not just `apply`.
/// Observer time includes candidate-book extraction and matching. Clock-read
/// overhead outside each callback stays in replay time. These are instrumented
/// elapsed times, not CPU times or predictions for a standalone replay run.
#[derive(Clone, Debug, Serialize)]
pub struct ValidationTimings {
    pub reference_load_seconds: f64,
    pub input_spool_seconds: f64,
    pub replay_excluding_validation_seconds: f64,
    pub validation_callbacks_seconds: f64,
    pub spool_cleanup_seconds: f64,
    pub report_finalize_seconds: f64,
    pub restore_total_seconds: f64,
    pub validation_total_seconds: f64,
    /// Excludes serialization and output writes performed by the caller.
    pub profiled_total_seconds: f64,
    pub unattributed_seconds: f64,
    pub observation_calls: u64,
}

struct TimedObserver<'a> {
    inner: &'a mut dyn StateObserver,
    elapsed: Duration,
    calls: u64,
}

impl StateObserver for TimedObserver<'_> {
    fn observe_trade_with_sequence(
        &mut self,
        channel: u32,
        symbol: &str,
        raw_sequence: u64,
        quote_time_ns: i64,
        price_units: i64,
        quantity: u64,
    ) -> Result<(), ProductionError> {
        let start = Instant::now();
        let result = self.inner.observe_trade_with_sequence(
            channel,
            symbol,
            raw_sequence,
            quote_time_ns,
            price_units,
            quantity,
        );
        self.elapsed += start.elapsed();
        self.calls += 1;
        result
    }
    fn observe_trade(
        &mut self,
        channel: u32,
        symbol: &str,
        quote_time_ns: i64,
        price_units: i64,
        quantity: u64,
    ) -> Result<(), ProductionError> {
        let start = Instant::now();
        let result =
            self.inner
                .observe_trade(channel, symbol, quote_time_ns, price_units, quantity);
        self.elapsed += start.elapsed();
        self.calls += 1;
        result
    }

    fn observe(
        &mut self,
        channel: u32,
        symbol: &str,
        book: &OrderBook,
        point: ObservationPoint,
    ) -> Result<(), ProductionError> {
        let start = Instant::now();
        let result = self.inner.observe(channel, symbol, book, point);
        self.elapsed += start.elapsed();
        self.calls += 1;
        result
    }
}

/// Run the usual full-day validator with benchmark-only timing enabled.
///
/// Errors retain the usual failed-spool semantics; partial timings are not
/// returned as if a failed day completed. No matching rule is changed.
pub fn profile_validate_market_day(
    config: &ValidationConfig,
) -> Result<(ValidationReport, ValidationTimings), ProductionError> {
    let total_start = Instant::now();
    let start = Instant::now();
    let references = load_references(&config.request, false)?;
    let mut observer = ValidationObserver::new(config.request.market, references.books)
        .with_continuous_lookback(config.continuous_lookback)?
        .with_continuous_lookahead(config.continuous_lookahead)?
        .with_max_detail_records(config.max_detail_records);
    observer.phase_audit = references.phase_audit;
    observer.sz_close_limits = references.sz_close_limits;
    let mut request = config.request.clone();
    request.snapshots = None;
    let reference_load = start.elapsed();
    let mut timed = TimedObserver {
        inner: &mut observer,
        elapsed: Duration::ZERO,
        calls: 0,
    };
    let (replay, [input, replay_loop, cleanup]) = profile_run_market_day(&request, &mut timed)?;
    let callbacks = timed.elapsed;
    let calls = timed.calls;
    let replay_exclusive = replay_loop
        .checked_sub(callbacks)
        .ok_or(ProductionError::Arithmetic("profiling callback partition"))?;
    let start = Instant::now();
    let report = observer.into_report(replay, config.retain_matched_records);
    let finalize = start.elapsed();
    let restore = input + replay_exclusive + cleanup;
    let validation = reference_load + callbacks + finalize;
    let total = total_start.elapsed();
    let residual = total
        .checked_sub(restore + validation)
        .ok_or(ProductionError::Arithmetic("profiling total partition"))?;
    let timings = ValidationTimings {
        reference_load_seconds: reference_load.as_secs_f64(),
        input_spool_seconds: input.as_secs_f64(),
        replay_excluding_validation_seconds: replay_exclusive.as_secs_f64(),
        validation_callbacks_seconds: callbacks.as_secs_f64(),
        spool_cleanup_seconds: cleanup.as_secs_f64(),
        report_finalize_seconds: finalize.as_secs_f64(),
        restore_total_seconds: restore.as_secs_f64(),
        validation_total_seconds: validation.as_secs_f64(),
        profiled_total_seconds: total.as_secs_f64(),
        unattributed_seconds: residual.as_secs_f64(),
        observation_calls: calls,
    };
    Ok((report, timings))
}
