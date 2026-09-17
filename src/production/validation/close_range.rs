//! Validation-only E0 projection. Never changes an OrderBook or replay output.
use super::UpperLimitNormalizationAudit;
use super::round_weighted_to_quantum;
use crate::{OrderBook, ProductionError, Side};
use crate::{SnapshotBookView, SnapshotLevel, SnapshotLevels};
use serde::{Deserialize, Serialize};

pub(super) const NO_UPPER_LIMIT: i64 = 9_999_999_999_999;
const ROUNDED_NO_UPPER_LIMIT: i64 = 10_000_000_000_000;
const TICK: i64 = 100;

#[derive(Clone, Debug, Default)]
pub(super) struct DayLimits {
    pub values: Option<(i64, i64)>,
    pub source: String,
    pub error: Option<String>,
    pub upper_limit_normalization: Option<UpperLimitNormalizationAudit>,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests;

impl DayLimits {
    pub fn observe_reference(
        &mut self,
        values: Result<(i64, i64), String>,
        missing_reception: bool,
        source: impl FnOnce() -> String,
    ) {
        if self.error.is_some() {
            return;
        }
        if missing_reception && values == Ok((ROUNDED_NO_UPPER_LIMIT, TICK)) {
            let source = source();
            self.observe_lazy(Ok((NO_UPPER_LIMIT, TICK)), || source.clone());
            if self.error.is_none() {
                let audit =
                    self.upper_limit_normalization
                        .get_or_insert(UpperLimitNormalizationAudit {
                            records: 0,
                            first_source: source,
                            raw_upper_units: ROUNDED_NO_UPPER_LIMIT,
                            normalized_upper_units: NO_UPPER_LIMIT,
                        });
                audit.records += 1;
            }
        } else {
            self.observe_lazy(values, source);
        }
    }

    #[cfg(test)]
    pub fn observe(&mut self, values: Result<(i64, i64), String>, source: String) {
        self.observe_lazy(values, || source);
    }

    pub fn observe_lazy(
        &mut self,
        values: Result<(i64, i64), String>,
        source: impl FnOnce() -> String,
    ) {
        if self.error.is_some() {
            return;
        }
        let values = match values {
            Ok((high, low))
                if high == NO_UPPER_LIMIT && (low == TICK || low == -NO_UPPER_LIMIT) =>
            {
                (high, low)
            }
            Ok((high, low))
                if high > 0
                    && high < NO_UPPER_LIMIT
                    && low > 0
                    && low <= high
                    && high % TICK == 0
                    && low % TICK == 0 =>
            {
                (high, low)
            }
            Ok(values) => {
                self.error = Some(format!(
                    "invalid SZ daily price limits {values:?} at {}",
                    source()
                ));
                return;
            }
            Err(error) => {
                self.error = Some(format!("{error} at {}", source()));
                return;
            }
        };
        if let Some(previous) = self.values {
            if previous != values {
                self.error = Some(format!(
                    "conflicting SZ daily price limits: {previous:?} at {} vs {values:?} at {}",
                    self.source,
                    source()
                ));
            }
        } else {
            self.values = Some(values);
            self.source = source();
        }
    }

    pub fn unlimited(&self) -> Result<bool, String> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        self.values
            .map(|(high, _)| high == NO_UPPER_LIMIT)
            .ok_or_else(|| "missing SZ daily price-limit metadata for E0 comparison".to_owned())
    }
}

#[derive(Clone, Debug)]
pub(super) struct RangeBase {
    pub price: i64,
    pub time_ms: i64,
    pub raw_sequence: Option<u64>,
}

/// Rule context, present on successful and failed projected comparisons alike.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClosePriceBandAudit {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_limit_normalization: Option<UpperLimitNormalizationAudit>,
    pub rule: String,
    pub base_price_units: i64,
    pub base_quote_time_ms: i64,
    pub base_raw_sequence: u64,
    pub lower_price_units: i64,
    pub upper_price_units: i64,
    pub excluded_bid_quantity: u64,
    pub excluded_ask_quantity: u64,
    pub price_limit_metadata_source: String,
}

pub(super) fn bounds(price: i64) -> Result<(i64, i64), ProductionError> {
    if price <= 0 || price % TICK != 0 {
        return Err(ProductionError::Validation(
            "invalid SZ closing-range base price".to_owned(),
        ));
    }
    let round = |factor: i128| -> Result<i64, ProductionError> {
        i64::try_from((i128::from(price) * factor + 500) / 1000 * 100)
            .map_err(|_| ProductionError::Arithmetic("SZ closing range overflow"))
    };
    let lower = round(9)?.min(price - TICK).max(TICK);
    let upper = round(11)?.max(
        price
            .checked_add(TICK)
            .ok_or(ProductionError::Arithmetic("SZ closing upper range"))?,
    );
    Ok((lower, upper))
}

pub(super) fn project(
    book: &OrderBook,
    base: &RangeBase,
    metadata: &DayLimits,
) -> Result<(SnapshotBookView, ClosePriceBandAudit), ProductionError> {
    let (lower, upper) = bounds(base.price)?;
    let sequence = base.raw_sequence.ok_or_else(|| {
        ProductionError::Validation(
            "missing successful trade sequence for SZ closing range".to_owned(),
        )
    })?;
    // Depth zero avoids first taking ten unfiltered levels. Statistics are kept.
    let mut actual = SnapshotBookView::from_book(book, 0)?;
    let mut excluded = [0_u64; 2];
    for (index, side) in [Side::Buy, Side::Sell].into_iter().enumerate() {
        let mut depth = SnapshotLevels::new();
        let mut total = 0_u64;
        let mut weighted = 0_u128;
        for level in book.levels(side) {
            let price = level.price.units();
            if !(lower..=upper).contains(&price) {
                excluded[index] = excluded[index]
                    .checked_add(level.total_quantity)
                    .ok_or(ProductionError::Arithmetic("SZ excluded range quantity"))?;
                continue;
            }
            total = total
                .checked_add(level.total_quantity)
                .ok_or(ProductionError::Arithmetic("SZ range quantity"))?;
            let value = u128::try_from(price)
                .map_err(|_| ProductionError::Arithmetic("SZ range price"))?
                .checked_mul(u128::from(level.total_quantity))
                .ok_or(ProductionError::Arithmetic("SZ range weighted value"))?;
            weighted = weighted
                .checked_add(value)
                .ok_or(ProductionError::Arithmetic("SZ range weighted sum"))?;
            if depth.len() < 10 {
                depth.push(SnapshotLevel {
                    price_units: price,
                    quantity: level.total_quantity,
                    order_count: u64::try_from(level.order_count)
                        .map_err(|_| ProductionError::Arithmetic("SZ range order count"))?,
                });
            }
        }
        let average = if total == 0 {
            None
        } else {
            Some(
                i64::try_from(round_weighted_to_quantum(
                    weighted,
                    u128::from(total),
                    TICK as u128,
                )?)
                .map_err(|_| ProductionError::Arithmetic("SZ range average"))?,
            )
        };
        if side == Side::Buy {
            actual.bids = depth;
            actual.total_bid_quantity = total;
            actual.weighted_bid_price_units = average;
        } else {
            actual.asks = depth;
            actual.total_ask_quantity = total;
            actual.weighted_ask_price_units = average;
        }
    }
    Ok((
        actual,
        ClosePriceBandAudit {
            upper_limit_normalization: metadata.upper_limit_normalization.clone(),
            rule: "SZ_UNLIMITED_STOCK_E0_PRICE_BAND".to_owned(),
            base_price_units: base.price,
            base_quote_time_ms: base.time_ms,
            base_raw_sequence: sequence,
            lower_price_units: lower,
            upper_price_units: upper,
            excluded_bid_quantity: excluded[0],
            excluded_ask_quantity: excluded[1],
            price_limit_metadata_source: metadata.source.clone(),
        },
    ))
}
