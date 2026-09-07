//! Bounded, fail-closed resolution of immediate SZ execution responses.
//!
//! Adjacency is a checked necessary condition, never proof that the source
//! guarantees execution groups. Only explicit diagnostic mode infers a rest.
use super::spool::{SzExecutionKind, SzExecutionRow, SzOrderKind, SzOrderRow, SzSide};
use super::{ProductionError, SzMarketOrderPolicy};

pub(super) const MAX_RESPONSES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Disposition {
    Terminal,
    Rest(i64),
}

pub(super) struct PendingOrder {
    order: SzOrderRow,
    remaining: u64,
    initial_best: Option<i64>,
    first_trade_price: Option<i64>,
    multiple_prices: bool,
    last_sequence: u64,
    responses: usize,
}

impl PendingOrder {
    pub fn new(order: &SzOrderRow, initial_best: Option<i64>) -> Self {
        Self {
            order: order.clone(),
            remaining: order.quantity,
            initial_best,
            first_trade_price: None,
            multiple_prices: false,
            last_sequence: order.sequence,
            responses: 0,
        }
    }

    pub fn error(&self, detail: &str) -> ProductionError {
        ProductionError::UnresolvedSzOrder {
            symbol: self.order.symbol.clone(),
            channel: self.order.channel,
            order_sequence: self.order.sequence,
            last_sequence: self.last_sequence,
            detail: detail.to_owned(),
        }
    }

    pub fn references(&self, execution: &SzExecutionRow) -> bool {
        execution.channel == self.order.channel
            && execution.symbol == self.order.symbol
            && match self.order.side {
                SzSide::Buy => execution.bid_order_no == self.order.sequence,
                SzSide::Sell => execution.ask_order_no == self.order.sequence,
            }
    }

    pub fn observe(&mut self, execution: &SzExecutionRow) -> Result<(), ProductionError> {
        if !self.references(execution) {
            return Err(self.error("response does not reference pending order"));
        }
        if self.last_sequence.checked_add(1) != Some(execution.sequence) {
            return Err(self.error("non-adjacent response; missing or filtered channel events"));
        }
        // With no source execution boundary, do not backfill a state across
        // quote-time cutoffs. Same-millisecond groups are indivisible by < T.
        if execution.quote_time_ns != self.order.quote_time_ns {
            return Err(
                self.error("response crosses quote-time boundary; cannot publish exact snapshots")
            );
        }
        if self.responses == MAX_RESPONSES {
            return Err(self.error("execution group exceeds bounded response buffer"));
        }
        if execution.quantity == 0 || execution.quantity > self.remaining {
            return Err(self.error("zero quantity or response exceeds pending remainder"));
        }
        let mut first_price = self.first_trade_price;
        let mut multiple = self.multiple_prices;
        let remaining = match execution.kind {
            SzExecutionKind::Cancel => {
                let other = match self.order.side {
                    SzSide::Buy => execution.ask_order_no,
                    SzSide::Sell => execution.bid_order_no,
                };
                if other != 0 || execution.quantity != self.remaining {
                    return Err(self
                        .error("cancellation must have one reference and cover exact remainder"));
                }
                0
            }
            SzExecutionKind::Trade => {
                if self.order.kind == SzOrderKind::SameSideBest || self.initial_best.is_none() {
                    return Err(self.error("empty reference book order unexpectedly trades"));
                }
                if execution.price_units <= 0 {
                    return Err(self.error("non-positive execution price"));
                }
                multiple |= first_price.is_some_and(|p| p != execution.price_units);
                first_price.get_or_insert(execution.price_units);
                self.remaining - execution.quantity
            }
        };
        self.remaining = remaining;
        self.first_trade_price = first_price;
        self.multiple_prices = multiple;
        self.last_sequence = execution.sequence;
        self.responses += 1;
        Ok(())
    }

    pub fn terminal(&self) -> bool {
        self.remaining == 0
    }

    pub fn finish(
        &self,
        policy: SzMarketOrderPolicy,
        next_sequence: Option<u64>,
    ) -> Result<Disposition, ProductionError> {
        if self.terminal() {
            return Ok(Disposition::Terminal);
        }
        if self.order.kind == SzOrderKind::SameSideBest {
            return Err(
                self.error("empty same-side best requires source cancellation reconciliation")
            );
        }
        if policy != SzMarketOrderPolicy::AssumeContiguous {
            return Err(self.error(
                "market remainder lacks execution qualifier or verified completion boundary",
            ));
        }
        if next_sequence.is_none() || self.last_sequence.checked_add(1) != next_sequence {
            return Err(
                self.error("EOF or sequence gap cannot prove immediate execution completion")
            );
        }
        if self.multiple_prices {
            return Err(self.error("multiple execution prices with uncancelled remainder"));
        }
        match (self.first_trade_price, self.initial_best) {
            (Some(traded), Some(best)) if traded == best => Ok(Disposition::Rest(best)),
            _ => Err(self
                .error("remainder price is not supported by initial opposite best and executions")),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn order() -> SzOrderRow {
        SzOrderRow {
            source_row: 1,
            sequence: 10,
            channel: 1,
            symbol: "000001".into(),
            quote_time_ns: 100,
            local_time_ns: 999,
            price_units: 123_456,
            quantity: 300,
            side: SzSide::Buy,
            kind: SzOrderKind::Market,
        }
    }
    fn execution(
        sequence: u64,
        quantity: u64,
        price_units: i64,
        kind: SzExecutionKind,
    ) -> SzExecutionRow {
        SzExecutionRow {
            source_row: sequence,
            sequence,
            channel: 1,
            symbol: "000001".into(),
            quote_time_ns: 100,
            local_time_ns: 1000,
            price_units,
            quantity,
            bid_order_no: 10,
            ask_order_no: if kind == SzExecutionKind::Trade { 2 } else { 0 },
            kind,
        }
    }

    #[test]
    fn one_price_is_not_proof_of_rest_but_cancel_closes_ioc() {
        let mut pending = PendingOrder::new(&order(), Some(100_000));
        pending
            .observe(&execution(11, 100, 100_000, SzExecutionKind::Trade))
            .expect("valid test fixture");
        assert!(
            pending
                .finish(SzMarketOrderPolicy::RequireEvidence, Some(12))
                .is_err()
        );
        assert_eq!(
            pending
                .finish(SzMarketOrderPolicy::AssumeContiguous, Some(12))
                .expect("valid test fixture"),
            Disposition::Rest(100_000)
        );
        pending
            .observe(&execution(12, 200, 0, SzExecutionKind::Cancel))
            .expect("valid test fixture");
        assert_eq!(
            pending
                .finish(SzMarketOrderPolicy::RequireEvidence, None)
                .expect("valid test fixture"),
            Disposition::Terminal
        );
    }

    #[test]
    fn multiple_prices_require_terminal_response() {
        let mut p = PendingOrder::new(&order(), Some(100_000));
        p.observe(&execution(11, 100, 100_000, SzExecutionKind::Trade))
            .expect("valid test fixture");
        p.observe(&execution(12, 100, 100_100, SzExecutionKind::Trade))
            .expect("valid test fixture");
        assert!(
            p.finish(SzMarketOrderPolicy::AssumeContiguous, Some(13))
                .is_err()
        );
        p.observe(&execution(13, 100, 0, SzExecutionKind::Cancel))
            .expect("valid test fixture");
        assert!(p.terminal());
    }

    #[test]
    fn full_fill_and_unfilled_cancellation_are_terminal() {
        for row in [
            execution(11, 300, 100_000, SzExecutionKind::Trade),
            execution(11, 300, 0, SzExecutionKind::Cancel),
        ] {
            let mut p = PendingOrder::new(&order(), Some(100_000));
            p.observe(&row).expect("valid test fixture");
            assert_eq!(
                p.finish(SzMarketOrderPolicy::RequireEvidence, None)
                    .expect("valid test fixture"),
                Disposition::Terminal
            );
        }
    }

    #[test]
    fn empty_same_side_never_becomes_a_hidden_lifetime_order() {
        let mut row = order();
        row.kind = SzOrderKind::SameSideBest;
        let mut p = PendingOrder::new(&row, None);
        assert!(
            p.finish(SzMarketOrderPolicy::AssumeContiguous, Some(11))
                .is_err()
        );
        assert!(
            p.observe(&execution(11, 100, 100_000, SzExecutionKind::Trade))
                .is_err()
        );
        p.observe(&execution(11, 300, 0, SzExecutionKind::Cancel))
            .expect("valid test fixture");
        assert!(p.terminal());
    }

    #[test]
    fn gaps_cross_time_overfill_and_partial_cancel_fail_without_advancing() {
        for change in 0..5 {
            let mut p = PendingOrder::new(&order(), Some(100_000));
            let mut e = execution(11, 100, 100_000, SzExecutionKind::Trade);
            match change {
                0 => e.sequence = 12,
                1 => e.quote_time_ns = 101,
                2 => e.quantity = 301,
                3 => {
                    e.kind = SzExecutionKind::Cancel;
                    e.ask_order_no = 0;
                }
                _ => e.symbol = "000002".into(),
            }
            assert!(p.observe(&e).is_err());
            assert_eq!(p.remaining, 300);
            assert_eq!(p.last_sequence, 10);
        }
    }

    #[test]
    fn eof_gap_and_wrong_initial_price_cannot_infer_rest() {
        for best in [100_000, 99_000] {
            let mut p = PendingOrder::new(&order(), Some(best));
            p.observe(&execution(11, 100, 100_000, SzExecutionKind::Trade))
                .expect("valid test fixture");
            assert!(
                p.finish(SzMarketOrderPolicy::AssumeContiguous, None)
                    .is_err()
            );
            assert!(
                p.finish(SzMarketOrderPolicy::AssumeContiguous, Some(13))
                    .is_err()
            );
            if best != 100_000 {
                assert!(
                    p.finish(SzMarketOrderPolicy::AssumeContiguous, Some(12))
                        .is_err()
                );
            }
        }
    }

    #[test]
    fn sell_reference_and_buffer_limit_are_checked() {
        let mut row = order();
        row.side = SzSide::Sell;
        row.quantity = MAX_RESPONSES as u64 + 2;
        let mut p = PendingOrder::new(&row, Some(100_000));
        for i in 0..MAX_RESPONSES {
            let mut e = execution(11 + i as u64, 1, 100_000, SzExecutionKind::Trade);
            e.bid_order_no = 2;
            e.ask_order_no = 10;
            p.observe(&e).expect("sell response");
        }
        let mut overflow = execution(
            11 + MAX_RESPONSES as u64,
            1,
            100_000,
            SzExecutionKind::Trade,
        );
        overflow.bid_order_no = 2;
        overflow.ask_order_no = 10;
        assert!(p.observe(&overflow).is_err());
        assert_eq!(p.remaining, 2);
        assert_eq!(p.responses, MAX_RESPONSES);
    }
}
