//! Report outcomes, coverage and retained details; does not modify replay state.
use super::TurnoverPrecisionAudit;
use super::default_continuous_lookahead_ms;
use super::precision::{TURNOVER_PRECISION_TAG, UPPER_LIMIT_PRECISION_TAG};
use super::{ClosePriceBandAudit, ReferenceCoverage, SelectionAudit, ValidationAnchor};
use super::{REFERENCE_MILLISECOND_NS, ValidationObserver, anchor_text, timestamp_ns_to_ms};
use crate::Market;
use crate::production::types::is_etf_symbol;
use crate::{ReplayReport, SzMarketOrderPolicy};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldDifference {
    pub field: String,
    pub expected: String,
    pub actual: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationOutcome {
    Matched,
    Mismatched,
    DataError,
    MissingSource,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ValidationRecord {
    /// Present only when both reference reception fields were missing and
    /// 12-significant-digit rounding was necessary for this successful match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turnover_precision: Option<TurnoverPrecisionAudit>,
    /// Validation-only projection context; not a waiver or a matching tag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_price_band: Option<ClosePriceBandAudit>,
    pub market: String,
    pub symbol: String,
    pub anchor: ValidationAnchor,
    pub outcome: ValidationOutcome,
    pub reference_time_ms: i64,
    pub reference_source_row_no: u64,
    #[serde(default)]
    pub matched_candidate_time_ms: Option<i64>,
    #[serde(default)]
    pub matched_candidate_raw_sequence: Option<u64>,
    #[serde(default)]
    pub matched_candidate_apply_sequence: Option<u64>,
    #[serde(default)]
    pub channel_id: Option<u32>,
    #[serde(default)]
    pub best_candidate_time_ms: Option<i64>,
    #[serde(default)]
    pub best_candidate_raw_sequence: Option<u64>,
    /// Explains a rule-based semantic normalization used for a successful match.
    #[serde(default)]
    pub match_tag: Option<String>,
    pub reason: Option<String>,
    pub differences: Vec<FieldDifference>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationReport {
    #[serde(
        deserialize_with = "deserialize_version",
        serialize_with = "serialize_version"
    )]
    pub report_schema_version: u8,
    pub run_outcome: RunOutcome,
    pub coverage: CoverageSummary,
    pub replay: ReplayReport,
    /// Milliseconds inspected before each continuous reference timestamp.
    #[serde(default)]
    pub continuous_lookback_ms: i64,
    /// Base horizon in milliseconds; the effective per-symbol horizons are in
    /// `continuous_lookahead_ms_by_symbol` (SZ ETFs and ChiNext differ by default).
    #[serde(default = "default_continuous_lookahead_ms")]
    pub continuous_lookahead_ms: i64,
    /// Explicit window overrides are diagnostics, never standard-rule acceptance.
    #[serde(default)]
    pub diagnostic_window_override: bool,
    #[serde(default)]
    pub continuous_lookahead_ms_by_symbol: BTreeMap<String, i64>,
    #[serde(default)]
    pub selection_audit: Vec<SelectionAudit>,
    pub selected_references: u64,
    pub comparable_references: u64,
    pub matched: u64,
    pub mismatched: u64,
    #[serde(default)]
    pub data_errors: u64,
    #[serde(default)]
    pub missing_source: u64,
    pub match_rate: Option<f64>,
    pub mismatch_fields: BTreeMap<String, u64>,
    pub mismatch_reasons: BTreeMap<String, u64>,
    pub failure_reasons: BTreeMap<String, u64>,
    /// Number of distinct symbols with at least one mismatched frame.
    #[serde(default)]
    pub mismatched_symbols: u64,
    /// Distinct mismatched symbols grouped by their three-digit prefix.
    #[serde(default)]
    pub mismatch_symbol_prefixes: BTreeMap<String, u64>,
    /// Counts successful rule-based semantic matches, keyed by a stable tag.
    #[serde(default)]
    pub match_tags: BTreeMap<String, u64>,
    /// Counts for every selected frame, keyed by `<stock|etf>.<anchor>`.
    #[serde(default)]
    pub breakdown: BTreeMap<String, ValidationCounts>,
    /// Successful records excluded from `records` in compact-report mode.
    #[serde(default)]
    pub omitted_matched_records: u64,
    #[serde(default)]
    pub omitted_failure_records: u64,
    pub records: Vec<ValidationRecord>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ValidationCounts {
    pub total: u64,
    pub comparable: u64,
    pub matched: u64,
    pub mismatched: u64,
    #[serde(default)]
    pub data_errors: u64,
    #[serde(default)]
    pub missing_source: u64,
}

impl ValidationCounts {
    pub(super) fn observe(&mut self, outcome: &ValidationOutcome) {
        self.total += 1;
        match outcome {
            ValidationOutcome::Matched => {
                self.comparable += 1;
                self.matched += 1;
            }
            ValidationOutcome::Mismatched => {
                self.comparable += 1;
                self.mismatched += 1;
            }
            ValidationOutcome::DataError => {
                self.data_errors += 1;
            }
            ValidationOutcome::MissingSource => {
                self.missing_source += 1;
            }
        }
    }
}

impl ValidationReport {
    #[must_use]
    pub const fn is_success(&self) -> bool {
        !matches!(self.run_outcome, RunOutcome::Failed)
    }

    /// A diagnostic override may have no mismatches but is not standard acceptance.
    #[must_use]
    pub const fn is_standard_acceptance(&self) -> bool {
        matches!(self.run_outcome, RunOutcome::Passed)
            && self.comparable_references > 0
            && self.replay.sz_pending_resolution_version == 1
            && !self.diagnostic_window_override
            && matches!(
                self.replay.sz_market_order_policy,
                SzMarketOrderPolicy::RequireEvidence
            )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    Passed,
    NoEligibleReferences,
    Failed,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CoverageSummary {
    pub symbols: u64,
    pub symbols_with_selected_references: u64,
    pub symbols_without_reference_records: u64,
    pub symbols_without_eligible_references: u64,
    /// Key: market.instrument_class.anchor; includes zero-coverage stages.
    pub by_stage: BTreeMap<String, CoverageCounts>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CoverageCounts {
    pub symbols: u64,
    pub covered_symbols: u64,
    pub selected_references: u64,
}

impl CoverageSummary {
    pub(super) fn observe(
        &mut self,
        market: &str,
        class: &str,
        audit: &SelectionAudit,
        pre_open_only: bool,
    ) {
        self.symbols += 1;
        match audit.coverage {
            ReferenceCoverage::NoReferenceRecords => self.symbols_without_reference_records += 1,
            ReferenceCoverage::NoEligibleReferences => {
                self.symbols_without_eligible_references += 1
            }
            ReferenceCoverage::Selected => self.symbols_with_selected_references += 1,
        }
        for anchor in ["pre_open", "continuous_trading", "market_close"] {
            if pre_open_only && anchor != "pre_open" {
                continue;
            }
            let count = audit.selected_counts.get(anchor).copied().unwrap_or(0);
            let stage = self
                .by_stage
                .entry(format!("{market}.{class}.{anchor}"))
                .or_default();
            stage.symbols += 1;
            stage.covered_symbols += u64::from(count > 0);
            stage.selected_references += count;
        }
    }
}

fn serialize_version<S: serde::Serializer>(version: &u8, serializer: S) -> Result<S::Ok, S::Error> {
    if *version == 2 {
        serializer.serialize_u8(2)
    } else {
        Err(serde::ser::Error::custom(
            "only validation report version 2 can be written",
        ))
    }
}

fn deserialize_version<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u8, D::Error> {
    let version = u8::deserialize(deserializer)?;
    if version == 2 {
        Ok(version)
    } else {
        Err(serde::de::Error::custom(format!(
            "unsupported validation report version {version}; expected 2"
        )))
    }
}

#[derive(Default)]
struct ReportCounts {
    totals: ValidationCounts,
    breakdown: BTreeMap<String, ValidationCounts>,
    mismatch_fields: BTreeMap<String, u64>,
    mismatch_reasons: BTreeMap<String, u64>,
    failure_reasons: BTreeMap<String, u64>,
    mismatched_symbols: BTreeSet<String>,
    match_tags: BTreeMap<String, u64>,
}

impl ReportCounts {
    fn observe(
        &mut self,
        class: &str,
        symbol: &str,
        anchor: ValidationAnchor,
        outcome: &ValidationOutcome,
        reason: &Option<String>,
        differences: &[FieldDifference],
    ) {
        self.totals.observe(outcome);
        self.breakdown
            .entry(format!("{class}.{}", anchor_text(anchor)))
            .or_default()
            .observe(outcome);
        if *outcome == ValidationOutcome::Mismatched {
            self.mismatched_symbols.insert(symbol.to_owned());
            for difference in differences {
                *self
                    .mismatch_fields
                    .entry(difference.field.clone())
                    .or_default() += 1;
            }
            if let Some(reason) = reason {
                *self.mismatch_reasons.entry(reason.clone()).or_default() += 1;
            }
        }
        if *outcome != ValidationOutcome::Matched {
            if let Some(reason) = reason {
                *self.failure_reasons.entry(reason.clone()).or_default() += 1;
            }
        }
    }

    fn observe_match_tags(
        &mut self,
        tag: Option<&str>,
        turnover_precision: Option<&TurnoverPrecisionAudit>,
        close_price_band: Option<&ClosePriceBandAudit>,
    ) {
        if let Some(tag) = tag {
            *self.match_tags.entry(tag.to_owned()).or_default() += 1;
        }
        if turnover_precision.is_some() {
            *self
                .match_tags
                .entry(TURNOVER_PRECISION_TAG.to_owned())
                .or_default() += 1;
        }
        if close_price_band.is_some_and(|audit| audit.upper_limit_normalization.is_some()) {
            *self
                .match_tags
                .entry(UPPER_LIMIT_PRECISION_TAG.to_owned())
                .or_default() += 1;
        }
    }
}

pub(super) fn build_report(
    mut observer: ValidationObserver,
    replay: ReplayReport,
    retain_matched_records: bool,
) -> ValidationReport {
    let market = market_text(observer.market).to_owned();
    let mut symbols = observer.symbols.keys().cloned().collect::<Vec<_>>();
    symbols.sort_unstable();
    let mut records = Vec::new();
    let mut counts = ReportCounts::default();
    let mut coverage = CoverageSummary::default();
    let mut omitted_matched_records = 0_u64;
    let mut omitted_failure_records = 0_u64;
    let continuous_lookahead_ms_by_symbol = symbols
        .iter()
        .map(|symbol| {
            (
                symbol.clone(),
                observer.lookahead_ns(symbol) / REFERENCE_MILLISECOND_NS,
            )
        })
        .collect();
    for symbol in symbols {
        let mut symbol_state = observer.symbols.remove(&symbol).unwrap_or_default();
        let references = std::mem::take(&mut symbol_state.references);
        let class = if is_etf_symbol(observer.market, &symbol) {
            "etf"
        } else {
            "stock"
        };
        let audit = observer
            .selection_audit
            .entry(symbol.clone())
            .or_insert_with(|| SelectionAudit::empty(symbol.clone()));
        coverage.observe(&market, class, audit, observer.pre_open_only);
        let mut cases = Vec::with_capacity(references.continuous_trading.len() + 2);
        if let Some(reference) = references.pre_open {
            cases.push((ValidationAnchor::PreOpen, reference));
        }
        if !observer.pre_open_only {
            cases.extend(
                references
                    .continuous_trading
                    .into_iter()
                    .map(|reference| (ValidationAnchor::ContinuousTrading, reference)),
            );
            if let Some(reference) = references.market_close {
                cases.push((ValidationAnchor::MarketClose, reference));
            }
        }
        for (anchor, mut reference) in cases {
            let mut state = std::mem::take(&mut reference.state);
            let mut diagnostics = state.diagnostics.take();
            let close_price_band = diagnostics.as_mut().and_then(|d| d.close_price_band.take());
            let turnover_precision = diagnostics
                .as_mut()
                .and_then(|d| d.turnover_precision.take());
            let best_candidate_time_ms = state.best_candidate_time_ns.map(timestamp_ns_to_ms);
            let best_candidate_raw_sequence = state.best_candidate_raw_sequence;
            let comparison_error = diagnostics.as_mut().and_then(|d| d.comparison_error.take());
            let differences = diagnostics
                .and_then(|mut d| d.best_differences.take())
                .unwrap_or_default();
            let after_close_events = (observer.market == Market::Szse
                && anchor == ValidationAnchor::MarketClose)
                .then(|| replay.sz_after_close_events_by_symbol.get(&symbol).copied())
                .flatten();
            let (outcome, reason) = classify_reference(
                after_close_events,
                reference.load_error,
                symbol_state.seen,
                comparison_error,
                state.matched,
            );
            counts.observe(class, &symbol, anchor, &outcome, &reason, &differences);
            let matched = outcome == ValidationOutcome::Matched;
            if matched {
                counts.observe_match_tags(
                    state.match_tag,
                    turnover_precision.as_ref(),
                    close_price_band.as_ref(),
                );
            }
            // Keep the established cap semantics: retained matches also occupy
            // records.len(), although retain_matched_records bypasses their cap.
            let keep_detail = observer
                .max_detail_records
                .is_none_or(|limit| records.len() < limit);
            if (matched && retain_matched_records) || (!matched && keep_detail) {
                records.push(ValidationRecord {
                    close_price_band,
                    turnover_precision: if matched { turnover_precision } else { None },
                    market: market.clone(),
                    symbol: symbol.clone(),
                    anchor,
                    outcome,
                    reference_time_ms: timestamp_ns_to_ms(reference.time_ns),
                    reference_source_row_no: reference.source_row_no,
                    matched_candidate_time_ms: if matched {
                        state.matched_candidate_time_ns.map(timestamp_ns_to_ms)
                    } else {
                        None
                    },
                    matched_candidate_raw_sequence: if matched {
                        state.matched_candidate_raw_sequence
                    } else {
                        None
                    },
                    matched_candidate_apply_sequence: if matched {
                        state.matched_candidate_apply_sequence
                    } else {
                        None
                    },
                    channel_id: symbol_state.channel,
                    best_candidate_time_ms,
                    best_candidate_raw_sequence,
                    match_tag: if matched {
                        state.match_tag.map(str::to_owned)
                    } else {
                        None
                    },
                    reason,
                    differences: if matched { Vec::new() } else { differences },
                });
            } else if matched {
                omitted_matched_records += 1;
            } else {
                omitted_failure_records += 1;
            }
        }
    }
    let ReportCounts {
        totals,
        breakdown,
        mismatch_fields,
        mismatch_reasons,
        failure_reasons,
        mismatched_symbols,
        match_tags,
    } = counts;
    let mut mismatch_symbol_prefixes = BTreeMap::new();
    for symbol in &mismatched_symbols {
        *mismatch_symbol_prefixes
            .entry(symbol.get(..3).unwrap_or(symbol).to_owned())
            .or_default() += 1;
    }
    records.sort_by(|a, b| {
        a.symbol
            .cmp(&b.symbol)
            .then_with(|| anchor_rank(a.anchor).cmp(&anchor_rank(b.anchor)))
            .then_with(|| a.reference_time_ms.cmp(&b.reference_time_ms))
    });
    let run_outcome = if totals.mismatched > 0
        || totals.data_errors > 0
        || totals.missing_source > 0
        || replay.sz_after_close_events > 0
    {
        RunOutcome::Failed
    } else if totals.total == 0 {
        RunOutcome::NoEligibleReferences
    } else {
        RunOutcome::Passed
    };
    ValidationReport {
        report_schema_version: 2,
        run_outcome,
        coverage,
        replay,
        continuous_lookback_ms: observer.continuous_lookback_ns / REFERENCE_MILLISECOND_NS,
        continuous_lookahead_ms: observer.continuous_lookahead_ns / REFERENCE_MILLISECOND_NS,
        diagnostic_window_override: observer.diagnostic_window_override,
        continuous_lookahead_ms_by_symbol,
        selection_audit: observer.selection_audit.into_values().collect(),
        selected_references: totals.total,
        comparable_references: totals.comparable,
        matched: totals.matched,
        mismatched: totals.mismatched,
        data_errors: totals.data_errors,
        missing_source: totals.missing_source,
        match_rate: ratio(totals.matched, totals.comparable),
        mismatch_fields,
        mismatch_reasons,
        failure_reasons,
        mismatched_symbols: mismatched_symbols.len() as u64,
        mismatch_symbol_prefixes,
        match_tags,
        breakdown,
        omitted_matched_records,
        omitted_failure_records,
        records,
    }
}

fn ratio(numerator: u64, denominator: u64) -> Option<f64> {
    (denominator != 0).then_some(numerator as f64 / denominator as f64)
}

/// Ordering is part of the report contract: a successful candidate cannot
/// override a source error or an unclassified after-close event.
fn classify_reference(
    after_close_events: Option<u64>,
    load_error: Option<String>,
    seen: bool,
    comparison_error: Option<String>,
    matched: bool,
) -> (ValidationOutcome, Option<String>) {
    if let Some(count) = after_close_events {
        (
            ValidationOutcome::DataError,
            Some(format!(
                "unclassified SZ events after 15:00:00.000: {count}; phase review required before E0 acceptance"
            )),
        )
    } else if let Some(error) = load_error {
        (ValidationOutcome::DataError, Some(error))
    } else if !seen {
        (
            ValidationOutcome::MissingSource,
            Some("no selected raw order/trade events were observed".to_owned()),
        )
    } else if let Some(error) = comparison_error {
        (ValidationOutcome::DataError, Some(error))
    } else if matched {
        (ValidationOutcome::Matched, None)
    } else {
        (
            ValidationOutcome::Mismatched,
            Some("no reconstructed full-state candidate matched the reference".to_owned()),
        )
    }
}

fn market_text(market: Market) -> &'static str {
    match market {
        Market::Sse => "SH",
        Market::Szse => "SZ",
    }
}

const fn anchor_rank(anchor: ValidationAnchor) -> u8 {
    match anchor {
        ValidationAnchor::PreOpen => 0,
        ValidationAnchor::ContinuousTrading => 1,
        ValidationAnchor::MarketClose => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::{ValidationOutcome, classify_reference};

    #[test]
    fn classification_preserves_error_priority_even_after_a_match() {
        let classify = |late, load: Option<&str>, seen, comparison: Option<&str>, matched| {
            classify_reference(
                late,
                load.map(str::to_owned),
                seen,
                comparison.map(str::to_owned),
                matched,
            )
        };
        let (outcome, reason) = classify(Some(0), Some("load"), false, Some("compare"), true);
        assert_eq!(outcome, ValidationOutcome::DataError);
        assert!(
            reason
                .as_deref()
                .is_some_and(|text| text.contains("unclassified SZ events"))
        );
        assert_eq!(
            classify(None, Some("load"), false, Some("compare"), true),
            (ValidationOutcome::DataError, Some("load".to_owned()))
        );
        assert_eq!(
            classify(None, None, false, Some("compare"), true).0,
            ValidationOutcome::MissingSource
        );
        assert_eq!(
            classify(None, None, true, Some("compare"), true),
            (ValidationOutcome::DataError, Some("compare".to_owned()))
        );
        assert_eq!(
            classify(None, None, true, None, true),
            (ValidationOutcome::Matched, None)
        );
        assert_eq!(
            classify(None, None, true, None, false).0,
            ValidationOutcome::Mismatched
        );
    }
}
