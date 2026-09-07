//! Validation-only E0 projection. Never changes an OrderBook or replay output.
use super::{SnapshotBookView, SnapshotLevel, round_weighted_to_quantum};
use crate::{OrderBook, ProductionError, Side};
use serde::{Deserialize, Serialize};

pub(super) const NO_UPPER_LIMIT: i64 = 9_999_999_999_999;
const TICK: i64 = 100;

#[derive(Clone, Debug, Default)]
pub(super) struct DayLimits {
    pub values: Option<(i64, i64)>,
    pub source: String,
    pub error: Option<String>,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::*;

    fn book() -> OrderBook {
        OrderBook::new(BookConfig::new(
            BookKey {
                market: Market::Szse,
                symbol: Symbol::from("001232"),
                trading_day: TradingDay::from_yyyymmdd(20260806).expect("day"),
            },
            PriceScale::from_decimal_places(4).expect("scale"),
        ))
    }

    fn add(book: &mut OrderBook, seq: u64, side: Side, price: i64, quantity: u64) {
        book.apply(BookEvent::AddOrder(AddOrder {
            meta: EventMeta {
                book_key: book.config().book_key.clone(),
                raw_sequence: RawSequence::new(seq).expect("seq"),
                apply_sequence: ApplySequence::new(seq).expect("seq"),
                local_time: LocalTimestampNs::from_nanos(seq as i64),
                quote_time: QuoteTimestampNs::from_nanos(seq as i64),
            },
            order_key: OrderKey {
                channel_id: ChannelId::new(1).expect("channel"),
                side,
                order_id: OrderId::new(seq).expect("order"),
            },
            pricing: PricingInstruction::Provided(Price::from_units(price).expect("price")),
            crossing: CrossingBehavior::Rest,
            quantity: Quantity::new(quantity).expect("qty"),
        }))
        .expect("apply");
    }

    #[test]
    fn integer_range_rounding_minimum_tick_and_overflow() {
        assert_eq!(bounds(4400).expect("range"), (4000, 4800));
        assert_eq!(bounds(247200).expect("range"), (222500, 271900));
        assert_eq!(bounds(1769900).expect("range"), (1592900, 1946900));
        assert_eq!(bounds(100).expect("range"), (100, 200));
        assert_eq!(bounds(400).expect("range"), (300, 500));
        assert_eq!(bounds(10500).expect("half cent"), (9500, 11600));
        assert!(bounds(0).is_err());
        assert!(bounds(101).is_err());
        assert!(bounds(i64::MAX / 100 * 100).is_err());
    }

    #[test]
    fn metadata_is_explicit_consistent_and_not_inferred_from_depth() {
        let mut metadata = DayLimits::default();
        assert!(metadata.unlimited().is_err());
        metadata.observe(Ok((NO_UPPER_LIMIT, 100)), "first".to_owned());
        assert_eq!(metadata.unlimited(), Ok(true));
        metadata.observe(Ok((NO_UPPER_LIMIT, 100)), "same".to_owned());
        assert_eq!(metadata.source, "first");
        metadata.observe(Ok((120000, 80000)), "conflict".to_owned());
        assert!(metadata.unlimited().is_err());
        for values in [Ok((0, 0)), Ok((120001, 80000)), Err("missing".to_owned())] {
            let mut metadata = DayLimits::default();
            metadata.observe(values, "bad".to_owned());
            assert!(metadata.unlimited().is_err());
        }
    }

    #[test]
    fn filters_before_depth_and_aggregates_all_eligible_levels_without_mutation() {
        let mut book = book();
        // Ten out-of-band orders must not consume the ten eligible depth slots.
        for seq in 1..=10 {
            add(&mut book, seq, Side::Buy, 120000 + seq as i64 * 100, 1000);
        }
        for seq in 11..=22 {
            add(
                &mut book,
                seq,
                Side::Buy,
                90000 + (seq - 11) as i64 * 100,
                10,
            );
        }
        add(&mut book, 23, Side::Sell, 110000, 20);
        add(&mut book, 24, Side::Sell, 110100, 30);
        let before = book.summary();
        let levels = book.levels(Side::Buy);
        let base = RangeBase {
            price: 100000,
            time_ms: 1,
            raw_sequence: Some(99),
        };
        let (actual, audit) = project(&book, &base, &DayLimits::default()).expect("project");
        assert_eq!(actual.bids.len(), 10);
        assert_eq!(actual.bids[0].price_units, 91100);
        assert_eq!(actual.total_bid_quantity, 120);
        assert_eq!(actual.weighted_bid_price_units, Some(90600));
        assert_eq!(actual.asks.len(), 1);
        assert_eq!(actual.total_ask_quantity, 20);
        assert_eq!(audit.excluded_bid_quantity, 10000);
        assert_eq!(audit.excluded_ask_quantity, 30);
        assert_eq!(before, book.summary());
        assert_eq!(levels, book.levels(Side::Buy));
        assert!(book.check_invariants().is_ok());
        let (empty, _) = project(
            &book,
            &RangeBase {
                price: 1000,
                ..base
            },
            &DayLimits::default(),
        )
        .expect("empty");
        assert!(empty.bids.is_empty() && empty.asks.is_empty());
        assert_eq!(empty.total_bid_quantity, 0);
        assert_eq!(empty.weighted_bid_price_units, None);
    }
}

impl DayLimits {
    pub fn observe(&mut self, values: Result<(i64, i64), String>, source: String) {
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
                    "invalid SZ daily price limits {values:?} at {source}"
                ));
                return;
            }
            Err(error) => {
                self.error = Some(format!("{error} at {source}"));
                return;
            }
        };
        if let Some(previous) = self.values {
            if previous != values {
                self.error = Some(format!(
                    "conflicting SZ daily price limits: {previous:?} at {} vs {values:?} at {source}",
                    self.source
                ));
            }
        } else {
            self.values = Some(values);
            self.source = source;
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
        let mut depth = Vec::new();
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
