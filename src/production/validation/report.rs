use super::TurnoverPrecisionAudit;
use super::default_continuous_lookahead_ms;
use super::{ClosePriceBandAudit, ReferenceCoverage, SelectionAudit, ValidationAnchor};
use crate::{ReplayReport, SzMarketOrderPolicy};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
