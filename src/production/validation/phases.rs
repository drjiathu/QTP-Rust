//! Reference-feed phase selection. This never supplies book prices or quantities.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Market, TradingDay};

use super::{ValidationAnchor, anchor_text, is_etf_symbol};

// SSE funds switched from continuous closing trading to a closing auction on
// this trading day (inclusive). Stocks already used the closing auction.
const SH_ETF_CLOSING_AUCTION_START: u32 = 20_260_706;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PhaseIssue {
    pub time_ms: i64,
    pub source_row_no: u64,
    pub previous_status: Option<String>,
    pub status: String,
    pub reason: String,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn sequence(
        market: Market,
        rows: &[(&str, i64)],
    ) -> (PhaseAudit, Vec<Option<ValidationAnchor>>) {
        sequence_for(market, 20_260_828, "test", rows)
    }

    fn sequence_for(
        market: Market,
        day: u32,
        symbol: &str,
        rows: &[(&str, i64)],
    ) -> (PhaseAudit, Vec<Option<ValidationAnchor>>) {
        let mut tracker = PhaseTracker::new(
            market,
            TradingDay::from_yyyymmdd(day).expect("valid test day"),
            symbol,
        );
        let actions = rows
            .iter()
            .enumerate()
            .map(|(i, (code, time))| tracker.observe(market, (*time, i as u64 + 1), code, 100, 200))
            .collect();
        (tracker.audit, actions)
    }

    #[test]
    fn normal_sz_open_repeats_b0_but_lunch_b0_is_not_opening() {
        use ValidationAnchor::{ContinuousTrading, MarketClose, PreOpen};
        let (audit, actions) = sequence(
            Market::Szse,
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
                Some(PreOpen),
                Some(ContinuousTrading),
                None,
                Some(ContinuousTrading),
                None,
                Some(MarketClose),
                Some(MarketClose)
            ]
        );
        assert!(audit.issues.is_empty());
    }

    #[test]
    fn sz_halt_breaks_opening_chain_but_not_later_normal_trading() {
        let (audit, actions) = sequence(
            Market::Szse,
            &[
                ("O0", 10),
                ("H0", 20),
                ("B0", 30),
                ("T0", 40),
                ("C0", 50),
                ("E0", 60),
            ],
        );
        assert_eq!(actions[2], None);
        assert_eq!(actions[3], Some(ValidationAnchor::ContinuousTrading));
        assert_eq!(actions[5], Some(ValidationAnchor::MarketClose));
        assert_eq!(audit.excluded_phase_counts["pre_open"], 2);
    }

    #[test]
    fn opening_cannot_restart_after_trading() {
        for market in [Market::Szse, Market::Sse] {
            let rows = if market == Market::Szse {
                [("T0", 200), ("O0", 210), ("B0", 220)]
            } else {
                [("TRADE", 200), ("OCALL", 210), ("TRADE", 220)]
            };
            let (audit, _) = sequence(market, &rows);
            assert!(audit.issue(ValidationAnchor::PreOpen).is_some());
        }
    }

    #[test]
    fn missing_normal_opening_predecessor_is_a_quality_error() {
        for (market, code) in [(Market::Szse, "B0"), (Market::Sse, "TRADE")] {
            let (audit, _) = sequence(market, &[(code, 100)]);
            assert!(audit.issue(ValidationAnchor::PreOpen).is_some());
        }
    }

    #[test]
    fn sh_opening_time_bound_and_uninterrupted_close_transition() {
        let (audit, actions) = sequence(
            Market::Sse,
            &[
                ("OCALL", 90),
                ("TRADE", 100),
                ("TRADE", 101),
                ("TRADE", 200),
                ("TRADE", 300),
                ("CCALL", 301),
                ("CLOSE", 302),
                ("CLOSE", 303),
            ],
        );
        assert!(audit.issues.is_empty());
        assert_eq!(actions[1], Some(ValidationAnchor::PreOpen));
        assert_eq!(actions[2], None);
        assert_eq!(actions[4], Some(ValidationAnchor::ContinuousTrading));
        assert_eq!(actions[7], Some(ValidationAnchor::MarketClose));
        let (audit, _) = sequence(
            Market::Sse,
            &[("CCALL", 300), ("SUSP", 301), ("CLOSE", 302)],
        );
        assert!(audit.issue(ValidationAnchor::MarketClose).is_none());
        assert!(
            audit
                .excluded_reason(ValidationAnchor::MarketClose)
                .is_some()
        );
    }

    #[test]
    fn normal_trading_cannot_follow_closing_auction() {
        for (market, auction, trade) in
            [(Market::Sse, "CCALL", "TRADE"), (Market::Szse, "C0", "T0")]
        {
            let (audit, actions) = sequence(market, &[(auction, 300), (trade, 301)]);
            assert!(audit.issue(ValidationAnchor::ContinuousTrading).is_some());
            assert_eq!(actions[1], None);
        }
    }

    #[test]
    fn all_day_halt_is_counted_per_phase() {
        let (audit, _) = sequence(
            Market::Szse,
            &[("B1", 10), ("T1", 20), ("T1", 30), ("E1", 40)],
        );
        assert_eq!(audit.excluded_phase_counts["pre_open"], 1);
        assert_eq!(audit.excluded_phase_counts["continuous_trading"], 2);
        assert_eq!(audit.excluded_phase_counts["market_close"], 1);
        assert!(audit.issues.is_empty());
    }

    #[test]
    fn reference_regression_and_duplicate_source_row_are_not_sorted_away() {
        for position in [(5, 2), (10, 1)] {
            let mut tracker = PhaseTracker::new(
                Market::Szse,
                TradingDay::from_yyyymmdd(20_260_828).expect("valid test day"),
                "000001",
            );
            tracker.observe(Market::Szse, (10, 1), "O0", 100, 200);
            tracker.observe(Market::Szse, position, "B0", 100, 200);
            assert_eq!(tracker.audit.issues.len(), 3);
        }
    }

    #[test]
    fn sh_etf_close_policy_changes_on_effective_day_but_stock_policy_does_not() {
        for day in [20_260_601, 20_260_703, 20_260_706, 20_260_707, 20_260_828] {
            for symbol in ["510300", "513100", "588000", "600000", "688981"] {
                let historical_etf =
                    day < SH_ETF_CLOSING_AUCTION_START && is_etf_symbol(Market::Sse, symbol);
                for predecessor in ["TRADE", "CCALL"] {
                    let (audit, actions) = sequence_for(
                        Market::Sse,
                        day,
                        symbol,
                        &[(predecessor, 300), ("CLOSE", 301), ("CLOSE", 302)],
                    );
                    let valid = (predecessor == "TRADE") == historical_etf;
                    assert_eq!(actions[1].is_some(), valid, "{day} {symbol} {predecessor}");
                    assert_eq!(actions[2].is_some(), valid, "repeated CLOSE");
                    assert_eq!(audit.issue(ValidationAnchor::MarketClose).is_none(), valid);
                }
            }
        }
    }

    #[test]
    fn historical_sh_etf_close_requires_uninterrupted_continuous_trading() {
        for rows in [
            vec![("CLOSE", 300)],
            vec![("OCALL", 90), ("TRADE", 100), ("CLOSE", 300)],
            vec![("TRADE", 200), ("ENDTR", 250), ("CLOSE", 300)],
        ] {
            let (audit, actions) = sequence_for(Market::Sse, 20_260_703, "510300", &rows);
            assert_eq!(actions.last(), Some(&None));
            assert!(audit.issue(ValidationAnchor::MarketClose).is_some());
        }
        // A prior halt does not invalidate normal trading after resumption.
        let (audit, actions) = sequence_for(
            Market::Sse,
            20_260_703,
            "513100",
            &[("SUSP", 100), ("TRADE", 250), ("CLOSE", 300)],
        );
        assert_eq!(actions[2], Some(ValidationAnchor::MarketClose));
        assert!(audit.issues.is_empty());
    }

    #[test]
    fn sz_etf_close_is_not_affected_by_sh_effective_day() {
        for day in [20_260_703, 20_260_706] {
            let (audit, actions) = sequence_for(
                Market::Szse,
                day,
                "159915",
                &[("T0", 200), ("C0", 250), ("E0", 300), ("E0", 301)],
            );
            assert!(audit.issues.is_empty());
            assert_eq!(actions[2], Some(ValidationAnchor::MarketClose));
            assert_eq!(actions[3], Some(ValidationAnchor::MarketClose));
        }
    }

    #[test]
    fn sh_suspension_excludes_close_without_inventing_a_normal_transition() {
        let close = 200 + 19_800_000_000_000;
        for (day, symbol) in [
            (20_260_601, "560000"),
            (20_260_706, "560000"),
            (20_260_601, "603721"),
        ] {
            for rows in [
                vec![("SUSP", 90), ("SUSP", 210), ("SUSP", close)],
                vec![
                    ("SUSP", 90),
                    ("SUSP", 210),
                    ("CLOSE", close),
                    ("CLOSE", close + 1),
                    ("ENDTR", close + 2),
                ],
                vec![("TRADE", 200), ("SUSP", 210), ("CLOSE", close)],
            ] {
                let (audit, actions) = sequence_for(Market::Sse, day, symbol, &rows);
                assert!(!actions.contains(&Some(ValidationAnchor::MarketClose)));
                assert!(audit.issue(ValidationAnchor::MarketClose).is_none());
                assert!(
                    audit
                        .excluded_reason(ValidationAnchor::MarketClose)
                        .is_some()
                );
            }
        }
    }

    #[test]
    fn sh_early_halt_or_normal_auction_does_not_hide_missing_close() {
        let close = 200 + 19_800_000_000_000;
        for rows in [
            vec![("SUSP", 90), ("SUSP", 210)],
            vec![("SUSP", 90), ("TRADE", 210), ("CCALL", 300)],
            vec![("CCALL", 300)],
            vec![("SUSP", close), ("TRADE", close + 1), ("CCALL", close + 2)],
            vec![("CCALL", 250), ("SUSP", 260), ("CCALL", 270)],
        ] {
            let (audit, _) = sequence(Market::Sse, &rows);
            assert!(
                audit
                    .excluded_reason(ValidationAnchor::MarketClose)
                    .is_none()
            );
        }
        let (audit, actions) = sequence(
            Market::Sse,
            &[("SUSP", 90), ("TRADE", 210), ("CCALL", 300), ("CLOSE", 301)],
        );
        assert_eq!(actions[3], Some(ValidationAnchor::MarketClose));
        assert!(audit.issue(ValidationAnchor::MarketClose).is_none());
    }
}

/// Full-frame counts are retained even when successful comparison details are omitted.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PhaseAudit {
    pub symbol: String,
    pub status_counts: BTreeMap<String, u64>,
    pub excluded_status_counts: BTreeMap<String, u64>,
    pub excluded_phase_counts: BTreeMap<String, u64>,
    pub excluded_phase_reasons: BTreeMap<String, String>,
    /// First issue per anchor; all phase rows still contribute to status_counts.
    pub issues: BTreeMap<String, PhaseIssue>,
}

impl PhaseAudit {
    pub(super) fn issue(&self, anchor: ValidationAnchor) -> Option<&PhaseIssue> {
        self.issues.get(anchor_text(anchor))
    }

    pub(super) fn excluded_reason(&self, anchor: ValidationAnchor) -> Option<&String> {
        self.excluded_phase_reasons.get(anchor_text(anchor))
    }
}

#[derive(Default)]
pub(super) struct PhaseTracker {
    pub audit: PhaseAudit,
    sh_continuous_close: bool,
    last_position: Option<(i64, u64)>,
    last_status: Option<String>,
    opening: bool,
    opening_blocked: bool,
    pre_open: bool,
    traded: bool,
    closing: bool,
    closing_valid: bool,
    closed: bool,
    sh_suspended: bool,
}

impl PhaseTracker {
    pub fn new(market: Market, trading_day: TradingDay, symbol: &str) -> Self {
        Self {
            sh_continuous_close: market == Market::Sse
                && is_etf_symbol(market, symbol)
                && trading_day.as_yyyymmdd() < SH_ETF_CLOSING_AUCTION_START,
            audit: PhaseAudit {
                symbol: symbol.to_owned(),
                ..PhaseAudit::default()
            },
            ..Self::default()
        }
    }

    fn error(
        &mut self,
        anchor: ValidationAnchor,
        position: (i64, u64),
        status: &str,
        reason: &str,
    ) {
        self.audit
            .issues
            .entry(anchor_text(anchor).to_owned())
            .or_insert_with(|| PhaseIssue {
                time_ms: position.0 / 1_000_000,
                source_row_no: position.1,
                previous_status: self.last_status.clone(),
                status: status.to_owned(),
                reason: reason.to_owned(),
            });
    }

    fn exclude(&mut self, anchor: ValidationAnchor, status: &str) {
        *self
            .audit
            .excluded_status_counts
            .entry(status.to_owned())
            .or_default() += 1;
        *self
            .audit
            .excluded_phase_counts
            .entry(anchor_text(anchor).to_owned())
            .or_default() += 1;
        self.audit
            .excluded_phase_reasons
            .entry(anchor_text(anchor).to_owned())
            .or_insert_with(|| format!("excluded phase status {status}"));
    }

    /// Rows must be visited in native reference-file order, never sorted to hide regressions.
    pub fn observe(
        &mut self,
        market: Market,
        position: (i64, u64),
        status: &str,
        opening_start: i64,
        continuous_start: i64,
    ) -> Option<ValidationAnchor> {
        *self
            .audit
            .status_counts
            .entry(status.to_owned())
            .or_default() += 1;
        if self
            .last_position
            .is_some_and(|last| position.0 < last.0 || position.1 <= last.1)
        {
            for anchor in [
                ValidationAnchor::PreOpen,
                ValidationAnchor::ContinuousTrading,
                ValidationAnchor::MarketClose,
            ] {
                self.error(
                    anchor,
                    position,
                    status,
                    "reference time or source row is non-increasing",
                );
            }
            return None;
        }
        let selected = match market {
            Market::Sse => self.observe_sh(position, status, opening_start, continuous_start),
            Market::Szse => self.observe_sz(position, status),
        };
        self.last_position = Some(position);
        self.last_status = Some(status.to_owned());
        selected
    }

    fn observe_sh(
        &mut self,
        pos: (i64, u64),
        status: &str,
        start: i64,
        continuous: i64,
    ) -> Option<ValidationAnchor> {
        use ValidationAnchor::{ContinuousTrading, MarketClose, PreOpen};
        if matches!(status, "OCALL" | "TRADE" | "CCALL") {
            self.sh_suspended = false;
            // A resumed normal phase must not inherit an old halt exemption
            // when its own required closing reference is missing.
            self.audit
                .excluded_phase_reasons
                .remove(anchor_text(MarketClose));
        }
        match status {
            "OCALL" if !self.traded && !self.closing && !self.closed => {
                self.opening = true;
                self.opening_blocked = false;
            }
            "OCALL" => self.error(
                PreOpen,
                pos,
                status,
                "OCALL after continuous or closing phase",
            ),
            "TRADE" if self.closing || self.closed => {
                self.error(ContinuousTrading, pos, status, "TRADE after closing phase")
            }
            "TRADE" if (start..continuous).contains(&pos.0) => {
                if self.opening && matches!(self.last_status.as_deref(), Some("OCALL" | "TRADE")) {
                    if !self.pre_open {
                        self.pre_open = true;
                        return Some(PreOpen);
                    }
                } else if self.opening_blocked {
                    self.exclude(PreOpen, status);
                } else {
                    self.error(
                        PreOpen,
                        pos,
                        status,
                        "opening TRADE has no normal preceding OCALL",
                    );
                }
            }
            "TRADE" if pos.0 >= continuous => {
                self.traded = true;
                return Some(ContinuousTrading);
            }
            "TRADE" => self.error(
                PreOpen,
                pos,
                status,
                "opening TRADE outside the permitted opening interval",
            ),
            "CCALL" if !self.closed => {
                self.closing = true;
                self.closing_valid = true;
            }
            "CCALL" => self.error(MarketClose, pos, status, "CCALL after CLOSE"),
            "CLOSE" => {
                // Suspended CLOSE (including repeats) is not a normal closing
                // reference. Do not parse its book fields or require CCALL.
                if self.sh_suspended {
                    self.closed = true;
                    self.exclude(MarketClose, "SUSP");
                    return None;
                }
                let valid_transition = if self.sh_continuous_close {
                    (self.traded && !self.closing && self.last_status.as_deref() == Some("TRADE"))
                        || (self.closed && self.last_status.as_deref() == Some("CLOSE"))
                } else {
                    self.closing_valid
                        && matches!(self.last_status.as_deref(), Some("CCALL" | "CLOSE"))
                };
                if valid_transition {
                    self.closed = true;
                    return Some(MarketClose);
                }
                self.error(
                    MarketClose,
                    pos,
                    status,
                    if self.sh_continuous_close {
                        "historical SH ETF CLOSE has no uninterrupted continuous TRADE transition"
                    } else {
                        "CLOSE has no uninterrupted CCALL transition"
                    },
                );
            }
            "START" | "ENDTR" => {}
            _ => {
                self.sh_suspended = status == "SUSP";
                // 15:00 is only evidence for suspension coverage here; it does
                // not select a normal CLOSE frame or cut off tick replay.
                let suspended_at_close =
                    self.sh_suspended && pos.0 >= continuous + 19_800_000_000_000;
                let anchor = if self.closing || self.closed || suspended_at_close {
                    MarketClose
                } else if pos.0 < continuous {
                    PreOpen
                } else {
                    ContinuousTrading
                };
                self.exclude(anchor, status);
                self.opening = false;
                self.opening_blocked = true;
                self.closing_valid = false;
            }
        }
        None
    }

    fn observe_sz(&mut self, pos: (i64, u64), status: &str) -> Option<ValidationAnchor> {
        use ValidationAnchor::{ContinuousTrading, MarketClose, PreOpen};
        match status {
            "O0" if !self.traded && !self.closing && !self.closed => {
                self.opening = true;
                self.opening_blocked = false;
            }
            "O0" => self.error(PreOpen, pos, status, "O0 after continuous or closing phase"),
            "B0" if self.closing || self.closed => {
                self.error(PreOpen, pos, status, "B0 after closing phase")
            }
            "B0" if !self.traded => {
                if self.opening && matches!(self.last_status.as_deref(), Some("O0" | "B0")) {
                    self.pre_open = true;
                    return Some(PreOpen);
                }
                if self.opening_blocked {
                    self.exclude(PreOpen, status);
                } else {
                    self.error(PreOpen, pos, status, "B0 has no normal preceding O0");
                }
            }
            "B0" => {} // Lunch break, not an opening snapshot.
            "T0" if self.closing || self.closed => {
                self.error(ContinuousTrading, pos, status, "T0 after closing phase")
            }
            "T0" => {
                self.traded = true;
                return Some(ContinuousTrading);
            }
            "C0" if !self.closed => {
                self.closing = true;
            }
            "C0" => self.error(MarketClose, pos, status, "C0 after E0"),
            "E0" => {
                self.closed = true;
                return Some(MarketClose);
            }
            "S0" => {}
            _ => {
                let anchor = match status {
                    "E1" | "C1" => MarketClose,
                    "T1" => ContinuousTrading,
                    _ if self.closed || self.closing => MarketClose,
                    _ if self.traded => ContinuousTrading,
                    _ => PreOpen,
                };
                self.exclude(anchor, status);
                self.opening = false;
                self.opening_blocked = true;
            }
        }
        None
    }
}
