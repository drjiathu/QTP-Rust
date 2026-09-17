//! Track successful SZ trades and reconcile the reference closing price.
//! Only a validation view is adjusted; replay and the OrderBook remain untouched.
use super::candidate::{DIFF_LAST_PRICE, DifferenceMask};
use super::close_range::RangeBase;
use super::{round_weighted_to_quantum, timestamp_ns_to_ms, validation_price_quantum};
use crate::production::types::is_etf_symbol;
use crate::{Market, ProductionError, QuoteTimestampNs, SnapshotBookView};
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug)]
struct TradeSample {
    quote_time_ns: i64,
    price_units: u128,
    quantity: u128,
}

#[derive(Clone, Debug, Default)]
pub(super) struct SzClosePriceTracker {
    pub(super) range_base: Option<RangeBase>,
    last_minute: VecDeque<TradeSample>,
    weighted_price_quantity: u128,
    quantity: u128,
    has_closing_auction_trade: bool,
}

impl SzClosePriceTracker {
    pub(super) fn observe(
        &mut self,
        quote_time_ns: i64,
        price_units: i64,
        quantity: u64,
    ) -> Result<(), ProductionError> {
        let price_units = u128::try_from(price_units)
            .map_err(|_| ProductionError::Arithmetic("SZ close trade price"))?;
        let quantity = u128::from(quantity);
        let weighted = price_units
            .checked_mul(quantity)
            .ok_or(ProductionError::Arithmetic("SZ close trade value"))?;
        if self
            .last_minute
            .back()
            .is_some_and(|sample| quote_time_ns < sample.quote_time_ns)
        {
            return Err(ProductionError::Validation(
                "SZ per-symbol trade time regressed while calculating close price".to_owned(),
            ));
        }
        self.last_minute.push_back(TradeSample {
            quote_time_ns,
            price_units,
            quantity,
        });
        self.weighted_price_quantity = self
            .weighted_price_quantity
            .checked_add(weighted)
            .ok_or(ProductionError::Arithmetic("SZ close weighted value"))?;
        self.quantity = self
            .quantity
            .checked_add(quantity)
            .ok_or(ProductionError::Arithmetic("SZ close quantity"))?;

        let cutoff = quote_time_ns
            .checked_sub(60_000_000_000)
            .ok_or(ProductionError::Arithmetic("SZ close one-minute cutoff"))?;
        while self
            .last_minute
            .front()
            .is_some_and(|sample| sample.quote_time_ns < cutoff)
        {
            let sample = self.last_minute.pop_front().ok_or_else(|| {
                ProductionError::Validation("missing SZ close trade sample".to_owned())
            })?;
            self.weighted_price_quantity = self
                .weighted_price_quantity
                .checked_sub(
                    sample
                        .price_units
                        .checked_mul(sample.quantity)
                        .ok_or(ProductionError::Arithmetic("SZ close evicted trade value"))?,
                )
                .ok_or(ProductionError::Arithmetic("SZ close weighted subtraction"))?;
            self.quantity = self
                .quantity
                .checked_sub(sample.quantity)
                .ok_or(ProductionError::Arithmetic("SZ close quantity subtraction"))?;
        }

        let time =
            crate::production::time::time_of_day_nanos(QuoteTimestampNs::from_nanos(quote_time_ns));
        const CLOSE_CALL_START: i64 = 14 * 3_600_000_000_000 + 57 * 60_000_000_000;
        const MARKET_CLOSE: i64 = 15 * 3_600_000_000_000;
        if time < CLOSE_CALL_START {
            self.range_base = Some(RangeBase {
                price: i64::try_from(price_units)
                    .map_err(|_| ProductionError::Arithmetic("SZ range base"))?,
                time_ms: timestamp_ns_to_ms(quote_time_ns),
                raw_sequence: None,
            });
        }
        if (CLOSE_CALL_START..=MARKET_CLOSE).contains(&time) {
            self.has_closing_auction_trade = true;
        }
        Ok(())
    }

    fn average_close_price(&self, quantum: i64) -> Result<Option<i64>, ProductionError> {
        if self.quantity == 0 {
            return Ok(None);
        }
        let quantum = u128::try_from(quantum)
            .map_err(|_| ProductionError::Arithmetic("SZ close price quantum"))?;
        let units =
            round_weighted_to_quantum(self.weighted_price_quantity, self.quantity, quantum)?;
        i64::try_from(units)
            .map(Some)
            .map_err(|_| ProductionError::Arithmetic("SZ close price conversion"))
    }
}

const SZ_STOCK_AVG_CLOSE_PRICE_TAG: &str = "SZ_STOCK_AVG_CLOSE_PRICE";
const SZ_ETF_AVG_CLOSE_PRICE_TAG: &str = "SZ_ETF_AVG_CLOSE_PRICE";
const SZ_STOCK_PRE_CLOSE_PRICE_TAG: &str = "SZ_STOCK_PRE_CLOSE_PRICE";
const SZ_ETF_PRE_CLOSE_PRICE_TAG: &str = "SZ_ETF_PRE_CLOSE_PRICE";

pub(super) fn reconcile_sz_market_close_price(
    symbol: &str,
    pre_close_price_units: Option<i64>,
    tracker: Option<&SzClosePriceTracker>,
    expected: &SnapshotBookView,
    actual: &mut SnapshotBookView,
    differences: DifferenceMask,
) -> Result<Option<&'static str>, ProductionError> {
    if !differences.is_only(DIFF_LAST_PRICE) {
        return Ok(None);
    }

    let is_etf = is_etf_symbol(Market::Szse, symbol);
    let (official_close_price, tag) = match tracker {
        Some(tracker) if tracker.has_closing_auction_trade => return Ok(None),
        Some(tracker) => (
            tracker.average_close_price(validation_price_quantum(Market::Szse, symbol))?,
            if is_etf {
                SZ_ETF_AVG_CLOSE_PRICE_TAG
            } else {
                SZ_STOCK_AVG_CLOSE_PRICE_TAG
            },
        ),
        None => (
            pre_close_price_units,
            if is_etf {
                SZ_ETF_PRE_CLOSE_PRICE_TAG
            } else {
                SZ_STOCK_PRE_CLOSE_PRICE_TAG
            },
        ),
    };
    if official_close_price != expected.last_price_units {
        return Ok(None);
    }
    actual.last_price_units = official_close_price;
    Ok(Some(tag))
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::tests::timestamp;

    pub(in crate::production::validation) fn has_closing_auction_trade(
        tracker: &super::SzClosePriceTracker,
    ) -> bool {
        tracker.has_closing_auction_trade
    }

    #[test]
    fn shenzhen_average_close_uses_an_inclusive_sixty_second_window() {
        let mut tracker = super::SzClosePriceTracker::default();
        assert!(
            tracker
                .observe(timestamp("14:54:59.999"), 10_000, 100)
                .is_ok()
        );
        assert!(
            tracker
                .observe(timestamp("14:55:00.000"), 10_020, 100)
                .is_ok()
        );
        assert!(
            tracker
                .observe(timestamp("14:56:00.000"), 10_040, 100)
                .is_ok()
        );
        assert_eq!(tracker.last_minute.len(), 2);
        assert_eq!(tracker.average_close_price(10).ok().flatten(), Some(10_030));
        assert_eq!(
            tracker.average_close_price(100).ok().flatten(),
            Some(10_000)
        );
        assert!(!tracker.has_closing_auction_trade);
    }
}
