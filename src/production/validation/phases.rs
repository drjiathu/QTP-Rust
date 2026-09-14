//! Reference eligibility and bounded audit, independent of book comparison results.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Market, ProductionError, TradingDay};

use super::{ValidationAnchor, anchor_text, is_etf_symbol};

const SH_ETF_CLOSING_AUCTION_START: u32 = 20_260_706;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    StatusNotApplicable,
    MissingNormalPredecessor,
    OutsideSelectionWindow,
    UnexpectedPhaseTransition,
    AdditionalStaticFrame,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SelectionSample {
    pub time_ms: i64,
    pub source_row_no: u64,
    pub previous_status: Option<String>,
    pub status: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceCoverage {
    #[default]
    NoReferenceRecords,
    NoEligibleReferences,
    Selected,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SelectionAudit {
    pub symbol: String,
    pub reference_records: u64,
    pub status_counts: BTreeMap<String, u64>,
    pub selected_counts: BTreeMap<String, u64>,
    pub skipped_counts: BTreeMap<SkipReason, u64>,
    pub first_samples: BTreeMap<SkipReason, SelectionSample>,
    pub coverage: ReferenceCoverage,
}

impl SelectionAudit {
    pub(super) fn empty(symbol: String) -> Self {
        Self {
            symbol,
            ..Self::default()
        }
    }
}

fn increment(counts: &mut BTreeMap<String, u64>, key: &str) {
    if let Some(count) = counts.get_mut(key) {
        *count += 1;
    } else {
        counts.insert(key.to_owned(), 1);
    }
}

#[derive(Default)]
pub(super) struct PhaseTracker {
    pub audit: SelectionAudit,
    sh_continuous_close: bool,
    last_position: Option<(i64, u64)>,
    last_status: Option<String>,
    opening: bool,
    traded: bool,
    closing: bool,
    closing_valid: bool,
    pre_open_selected: bool,
    close_selected: bool,
    pre_open_only: bool,
}

impl PhaseTracker {
    pub fn with_pre_open_only(mut self, enabled: bool) -> Self {
        self.pre_open_only = enabled;
        self
    }
    pub fn new(market: Market, day: TradingDay, symbol: &str) -> Self {
        Self {
            sh_continuous_close: market == Market::Sse
                && is_etf_symbol(market, symbol)
                && day.as_yyyymmdd() < SH_ETF_CLOSING_AUCTION_START,
            audit: SelectionAudit::empty(symbol.to_owned()),
            ..Self::default()
        }
    }

    /// Positions establish native reference order; corrupt positions are not eligibility skips.
    pub fn observe(
        &mut self,
        market: Market,
        position: (i64, u64),
        status: &str,
        opening_start: i64,
        continuous_start: i64,
    ) -> Result<Option<ValidationAnchor>, ProductionError> {
        if self
            .last_position
            .is_some_and(|last| position.0 < last.0 || position.1 <= last.1)
        {
            return Err(ProductionError::Validation(format!(
                "reference time or source row is non-increasing for {} at {} source_row={}",
                self.audit.symbol,
                position.0 / 1_000_000,
                position.1
            )));
        }
        let decision = match market {
            Market::Sse => self.observe_sh(position.0, status, opening_start, continuous_start),
            Market::Szse => self.observe_sz(status),
        }
        .and_then(|anchor| {
            if self.pre_open_only && anchor != ValidationAnchor::PreOpen {
                Err(SkipReason::StatusNotApplicable)
            } else {
                Ok(anchor)
            }
        });
        self.audit.reference_records += 1;
        increment(&mut self.audit.status_counts, status);
        match decision {
            Ok(anchor) => {
                increment(&mut self.audit.selected_counts, anchor_text(anchor));
                self.audit.coverage = ReferenceCoverage::Selected;
            }
            Err(reason) => {
                *self.audit.skipped_counts.entry(reason).or_default() += 1;
                if self.audit.coverage == ReferenceCoverage::NoReferenceRecords {
                    self.audit.coverage = ReferenceCoverage::NoEligibleReferences;
                }
                if matches!(
                    reason,
                    SkipReason::MissingNormalPredecessor
                        | SkipReason::OutsideSelectionWindow
                        | SkipReason::UnexpectedPhaseTransition
                ) {
                    self.audit
                        .first_samples
                        .entry(reason)
                        .or_insert_with(|| SelectionSample {
                            time_ms: position.0 / 1_000_000,
                            source_row_no: position.1,
                            previous_status: self.last_status.clone(),
                            status: status.to_owned(),
                        });
                }
            }
        }
        self.last_position = Some(position);
        if self.last_status.as_deref() != Some(status) {
            self.last_status = Some(status.to_owned());
        }
        Ok(decision.ok())
    }

    fn observe_sh(
        &mut self,
        time: i64,
        status: &str,
        start: i64,
        continuous: i64,
    ) -> Result<ValidationAnchor, SkipReason> {
        use SkipReason::*;
        use ValidationAnchor::*;
        match status {
            "CLOSE" if self.close_selected => Err(AdditionalStaticFrame),
            "TRADE"
                if (start..continuous).contains(&time)
                    && self.pre_open_selected
                    && !self.traded
                    && !self.closing
                    && !self.close_selected =>
            {
                Err(AdditionalStaticFrame)
            }
            "OCALL" if !self.traded && !self.closing && !self.close_selected => {
                self.opening = true;
                Err(StatusNotApplicable)
            }
            "OCALL" => Err(UnexpectedPhaseTransition),
            "TRADE" if self.closing || self.close_selected => Err(UnexpectedPhaseTransition),
            "TRADE" if (start..continuous).contains(&time) => {
                if self.opening && self.last_status.as_deref() == Some("OCALL") {
                    self.pre_open_selected = true;
                    Ok(PreOpen)
                } else {
                    Err(MissingNormalPredecessor)
                }
            }
            "TRADE" if time >= continuous => {
                self.traded = true;
                Ok(ContinuousTrading)
            }
            "TRADE" => Err(OutsideSelectionWindow),
            "CCALL" if !self.close_selected => {
                self.closing = true;
                self.closing_valid = true;
                Err(StatusNotApplicable)
            }
            "CCALL" => Err(UnexpectedPhaseTransition),
            "CLOSE" => {
                let valid = if self.sh_continuous_close {
                    self.traded && !self.closing && self.last_status.as_deref() == Some("TRADE")
                } else {
                    self.closing_valid && self.last_status.as_deref() == Some("CCALL")
                };
                if valid {
                    self.close_selected = true;
                    Ok(MarketClose)
                } else {
                    Err(MissingNormalPredecessor)
                }
            }
            _ => {
                self.opening = false;
                self.closing_valid = false;
                Err(StatusNotApplicable)
            }
        }
    }

    fn observe_sz(&mut self, status: &str) -> Result<ValidationAnchor, SkipReason> {
        use SkipReason::*;
        use ValidationAnchor::*;
        match status {
            "E0" if self.close_selected => Err(AdditionalStaticFrame),
            "B0" if self.pre_open_selected
                && !self.traded
                && !self.closing
                && !self.close_selected =>
            {
                Err(AdditionalStaticFrame)
            }
            "O0" if !self.traded && !self.closing && !self.close_selected => {
                self.opening = true;
                Err(StatusNotApplicable)
            }
            "O0" => Err(UnexpectedPhaseTransition),
            "B0" if self.closing || self.close_selected => Err(UnexpectedPhaseTransition),
            "B0" if !self.traded => {
                if self.opening && self.last_status.as_deref() == Some("O0") {
                    self.pre_open_selected = true;
                    Ok(PreOpen)
                } else {
                    Err(MissingNormalPredecessor)
                }
            }
            "B0" => Err(StatusNotApplicable),
            "T0" if self.closing || self.close_selected => Err(UnexpectedPhaseTransition),
            "T0" => {
                self.traded = true;
                Ok(ContinuousTrading)
            }
            "C0" if !self.close_selected => {
                self.closing = true;
                self.closing_valid = true;
                Err(StatusNotApplicable)
            }
            "C0" => Err(UnexpectedPhaseTransition),
            "E0" => {
                if self.closing_valid && self.last_status.as_deref() == Some("C0") {
                    self.close_selected = true;
                    Ok(MarketClose)
                } else {
                    Err(MissingNormalPredecessor)
                }
            }
            _ => {
                self.opening = false;
                self.closing_valid = false;
                Err(StatusNotApplicable)
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn sequence(
        market: Market,
        day: u32,
        symbol: &str,
        rows: &[(&str, i64)],
    ) -> (SelectionAudit, Vec<Option<ValidationAnchor>>) {
        let mut tracker =
            PhaseTracker::new(market, TradingDay::from_yyyymmdd(day).expect("day"), symbol);
        let actions = rows
            .iter()
            .enumerate()
            .map(|(i, (status, time))| {
                tracker
                    .observe(market, (*time, i as u64 + 1), status, 100, 200)
                    .expect("position")
            })
            .collect();
        assert_eq!(
            tracker.audit.reference_records,
            tracker.audit.selected_counts.values().sum::<u64>()
                + tracker.audit.skipped_counts.values().sum::<u64>()
        );
        assert!(tracker.audit.first_samples.len() <= 3);
        (tracker.audit, actions)
    }

    #[test]
    fn normal_sz_selects_first_static_and_all_continuous() {
        use ValidationAnchor::*;
        let (_, actions) = sequence(
            Market::Szse,
            20260828,
            "000001",
            &[
                ("O0", 10),
                ("B0", 20),
                ("B0", 30),
                ("T0", 40),
                ("B0", 50),
                ("T0", 60),
                ("C0", 70),
                ("E0", 80),
                ("E0", 90),
            ],
        );
        assert_eq!(
            actions,
            [
                None,
                Some(PreOpen),
                None,
                Some(ContinuousTrading),
                None,
                Some(ContinuousTrading),
                None,
                Some(MarketClose),
                None
            ]
        );
    }

    #[test]
    fn halted_and_resumed_sequences_only_select_eligible_frames() {
        use ValidationAnchor::*;
        let (audit, actions) = sequence(
            Market::Szse,
            20260828,
            "000001",
            &[("S0", 1), ("H0", 2), ("E0", 3)],
        );
        assert!(actions.iter().all(Option::is_none));
        assert_eq!(audit.coverage, ReferenceCoverage::NoEligibleReferences);
        assert_eq!(
            audit.first_samples[&SkipReason::MissingNormalPredecessor]
                .previous_status
                .as_deref(),
            Some("H0")
        );
        let (_, actions) = sequence(
            Market::Szse,
            20260828,
            "000001",
            &[("H0", 1), ("T0", 2), ("E0", 3), ("C0", 4), ("E0", 5)],
        );
        assert_eq!(
            actions,
            [None, Some(ContinuousTrading), None, None, Some(MarketClose)]
        );
    }

    #[test]
    fn interrupted_predecessors_and_late_normal_transitions() {
        let (_, actions) = sequence(
            Market::Szse,
            20260828,
            "000001",
            &[
                ("O0", 1),
                ("H0", 2),
                ("B0", 3),
                ("O0", 4),
                ("B0", 5),
                ("T0", 6),
                ("C0", 7),
                ("H0", 8),
                ("E0", 9),
                ("C0", 10),
                ("E0", 11),
            ],
        );
        assert_eq!(actions[2], None);
        assert_eq!(actions[4], Some(ValidationAnchor::PreOpen));
        assert_eq!(actions[8], None);
        assert_eq!(actions[10], Some(ValidationAnchor::MarketClose));
    }

    #[test]
    fn sh_first_static_time_window_and_recovery() {
        use ValidationAnchor::*;
        let (audit, actions) = sequence(
            Market::Sse,
            20260828,
            "600000",
            &[
                ("TRADE", 90),
                ("TRADE", 100),
                ("OCALL", 101),
                ("TRADE", 102),
                ("TRADE", 103),
                ("TRADE", 200),
                ("CLOSE", 201),
                ("CCALL", 202),
                ("SUSP", 203),
                ("CLOSE", 204),
                ("CCALL", 205),
                ("CLOSE", 206),
                ("CLOSE", 207),
            ],
        );
        assert_eq!(actions[0], None);
        assert_eq!(actions[1], None);
        assert_eq!(actions[3], Some(PreOpen));
        assert_eq!(actions[4], None);
        assert_eq!(actions[5], Some(ContinuousTrading));
        assert_eq!(actions[6], None);
        assert_eq!(actions[9], None);
        assert_eq!(actions[11], Some(MarketClose));
        assert_eq!(actions[12], None);
        assert_eq!(audit.selected_counts["market_close"], 1);
    }

    #[test]
    fn historical_sh_etf_close_boundary_is_preserved() {
        for day in [20260703, 20260706, 20260828] {
            for symbol in ["510300", "600000"] {
                for predecessor in ["TRADE", "CCALL"] {
                    let historical = day < 20260706 && symbol == "510300";
                    let (_, actions) = sequence(
                        Market::Sse,
                        day,
                        symbol,
                        &[(predecessor, 200), ("CLOSE", 300), ("CLOSE", 301)],
                    );
                    assert_eq!(actions[1].is_some(), (predecessor == "TRADE") == historical);
                    assert!(actions[2].is_none());
                }
            }
        }
    }

    #[test]
    fn phase_skips_do_not_hide_position_corruption() {
        for position in [(5, 2), (10, 1)] {
            let mut tracker = PhaseTracker::new(
                Market::Szse,
                TradingDay::from_yyyymmdd(20260828).expect("day"),
                "000001",
            );
            tracker
                .observe(Market::Szse, (10, 1), "H0", 100, 200)
                .expect("first");
            assert!(
                tracker
                    .observe(Market::Szse, position, "E0", 100, 200)
                    .is_err()
            );
        }
    }

    #[test]
    fn invalid_phase_positions_skip_without_reopening_completed_stages() {
        for (market, opening, trade, closing, close) in [
            (Market::Szse, "O0", "T0", "C0", "E0"),
            (Market::Sse, "OCALL", "TRADE", "CCALL", "CLOSE"),
        ] {
            let (_, actions) = sequence(
                market,
                20260828,
                "000001",
                &[
                    (trade, 200),
                    (opening, 201),
                    (closing, 202),
                    (trade, 203),
                    (close, 204),
                    (closing, 205),
                    (close, 206),
                ],
            );
            assert_eq!(actions[1], None);
            assert_eq!(actions[3], None);
            assert_eq!(actions[4], None);
            assert_eq!(actions[6], Some(ValidationAnchor::MarketClose));
        }
    }
}
