//! Compact storage for millions of reference frames; public snapshot types stay unchanged.
use super::{SnapshotBookView, SnapshotLevel, SnapshotLevels};
use crate::ProductionError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReferenceLevels {
    prices: [i64; 10],
    quantities: [u64; 10],
    counts: [u32; 10],
    len: u8,
}
impl ReferenceLevels {
    fn from_levels(levels: &SnapshotLevels) -> Result<Self, ProductionError> {
        if levels.len() > 10 {
            return Err(ProductionError::Validation(
                "reference depth exceeds ten".to_owned(),
            ));
        }
        let mut result = Self {
            prices: [0; 10],
            quantities: [0; 10],
            counts: [0; 10],
            len: levels.len() as u8,
        };
        for (i, level) in levels.iter().enumerate() {
            result.prices[i] = level.price_units;
            result.quantities[i] = level.quantity;
            result.counts[i] = u32::try_from(level.order_count)
                .map_err(|_| ProductionError::Arithmetic("reference order count"))?;
        }
        Ok(result)
    }
    fn expand(&self) -> SnapshotLevels {
        (0..usize::from(self.len))
            .map(|i| SnapshotLevel {
                price_units: self.prices[i],
                quantity: self.quantities[i],
                order_count: u64::from(self.counts[i]),
            })
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReferenceBookView {
    bids: ReferenceLevels,
    asks: ReferenceLevels,
    pub total_bid_quantity: u64,
    pub weighted_bid_price_units: Option<i64>,
    pub total_ask_quantity: u64,
    pub weighted_ask_price_units: Option<i64>,
    pub last_price_units: Option<i64>,
    pub high_price_units: Option<i64>,
    pub low_price_units: Option<i64>,
    pub trade_count: u64,
    pub trade_quantity: u64,
    pub turnover_units: u128,
}
impl TryFrom<SnapshotBookView> for ReferenceBookView {
    type Error = ProductionError;
    fn try_from(view: SnapshotBookView) -> Result<Self, Self::Error> {
        Ok(Self {
            bids: ReferenceLevels::from_levels(&view.bids)?,
            asks: ReferenceLevels::from_levels(&view.asks)?,
            total_bid_quantity: view.total_bid_quantity,
            weighted_bid_price_units: view.weighted_bid_price_units,
            total_ask_quantity: view.total_ask_quantity,
            weighted_ask_price_units: view.weighted_ask_price_units,
            last_price_units: view.last_price_units,
            high_price_units: view.high_price_units,
            low_price_units: view.low_price_units,
            trade_count: view.trade_count,
            trade_quantity: view.trade_quantity,
            turnover_units: view.turnover_units,
        })
    }
}
impl ReferenceBookView {
    pub fn expand(&self) -> SnapshotBookView {
        SnapshotBookView {
            bids: self.bids.expand(),
            asks: self.asks.expand(),
            total_bid_quantity: self.total_bid_quantity,
            weighted_bid_price_units: self.weighted_bid_price_units,
            total_ask_quantity: self.total_ask_quantity,
            weighted_ask_price_units: self.weighted_ask_price_units,
            last_price_units: self.last_price_units,
            high_price_units: self.high_price_units,
            low_price_units: self.low_price_units,
            trade_count: self.trade_count,
            trade_quantity: self.trade_quantity,
            turnover_units: self.turnover_units,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;

    #[test]
    fn compact_depth_round_trips_and_checks_bounds() {
        for count in 0..=10 {
            let levels: SnapshotLevels = (0..count)
                .map(|i| SnapshotLevel {
                    price_units: 100_000 + i,
                    quantity: u64::MAX - i as u64,
                    order_count: u64::from(u32::MAX),
                })
                .collect();
            assert_eq!(
                ReferenceLevels::from_levels(&levels)
                    .expect("depth")
                    .expand(),
                levels
            );
        }
        let mut levels: SnapshotLevels = std::iter::repeat_n(
            SnapshotLevel {
                price_units: 10,
                quantity: 100,
                order_count: 1,
            },
            11,
        )
        .collect();
        assert!(ReferenceLevels::from_levels(&levels).is_err());
        levels.truncate(10);
        levels[0].order_count = u64::from(u32::MAX) + 1;
        assert!(ReferenceLevels::from_levels(&levels).is_err());
        assert!(size_of::<ReferenceBookView>() < size_of::<SnapshotBookView>());
    }
}
