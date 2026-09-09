use std::time::Duration;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{LocalTimestampNs, OrderBook, Price, QuoteTimestampNs, Side};

use super::{ProductionError, SnapshotKind, SnapshotSchedule, parse_market_timestamp};
use crate::TradingDay;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotLevel {
    pub price_units: i64,
    pub quantity: u64,
    pub order_count: u64,
}

pub type SnapshotLevels = SmallVec<[SnapshotLevel; 10]>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotBookView {
    pub bids: SnapshotLevels,
    pub asks: SnapshotLevels,
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

impl SnapshotBookView {
    pub fn from_book(book: &OrderBook, depth: usize) -> Result<Self, ProductionError> {
        let (total_bid_quantity, weighted_bid_price_units) = aggregate_side(book, Side::Buy)?;
        let (total_ask_quantity, weighted_ask_price_units) = aggregate_side(book, Side::Sell)?;
        let statistics = book.summary().statistics;
        Ok(Self {
            bids: snapshot_levels(book, Side::Buy, depth)?,
            asks: snapshot_levels(book, Side::Sell, depth)?,
            total_bid_quantity,
            weighted_bid_price_units,
            total_ask_quantity,
            weighted_ask_price_units,
            last_price_units: statistics.last_price.map(Price::units),
            high_price_units: statistics.high_price.map(Price::units),
            low_price_units: statistics.low_price.map(Price::units),
            trade_count: statistics.trade_count,
            trade_quantity: statistics.total_quantity,
            turnover_units: statistics.total_turnover_units,
        })
    }
}

fn snapshot_levels(
    book: &OrderBook,
    side: Side,
    depth: usize,
) -> Result<SnapshotLevels, ProductionError> {
    let mut result = SnapshotLevels::new();
    book.try_visit_levels(side, depth, |level| {
        result.push(SnapshotLevel {
            price_units: level.price.units(),
            quantity: level.total_quantity,
            order_count: u64::try_from(level.order_count)
                .map_err(|_| ProductionError::Arithmetic("level order count"))?,
        });
        Ok(())
    })?;
    Ok(result)
}

fn aggregate_side(book: &OrderBook, side: Side) -> Result<(u64, Option<i64>), ProductionError> {
    let (total, weighted) = book.visible_aggregate(side);
    if total == 0 {
        return Ok((0, None));
    }
    let divisor = u128::from(total);
    let rounded = weighted
        .checked_add(divisor / 2)
        .ok_or(ProductionError::Arithmetic("weighted price rounding"))?
        / divisor;
    let units = i64::try_from(rounded)
        .map_err(|_| ProductionError::Arithmetic("weighted price conversion"))?;
    Ok((total, Some(units)))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub symbol: String,
    pub channel: u32,
    pub kind: SnapshotKind,
    pub boundary_time_ns: i64,
    pub last_local_time_ns: Option<i64>,
    pub last_quote_time_ns: Option<i64>,
    pub book: SnapshotBookView,
}

impl BookSnapshot {
    pub fn capture(
        book: &OrderBook,
        channel: u32,
        kind: SnapshotKind,
        boundary_time_ns: i64,
        depth: usize,
    ) -> Result<Self, ProductionError> {
        let summary = book.summary();
        Ok(Self {
            symbol: summary.book_key.symbol.to_string(),
            channel,
            kind,
            boundary_time_ns,
            last_local_time_ns: summary.last_local_time.map(LocalTimestampNs::as_nanos),
            last_quote_time_ns: summary.last_quote_time.map(QuoteTimestampNs::as_nanos),
            book: SnapshotBookView::from_book(book, depth)?,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SnapshotCursor {
    sessions: [(i64, i64); 2],
    interval_ns: i64,
    session: usize,
    next: Option<i64>,
}

impl SnapshotCursor {
    pub(crate) fn new(
        trading_day: TradingDay,
        schedule: &SnapshotSchedule,
    ) -> Result<Self, ProductionError> {
        let sessions = [
            (
                parse_market_timestamp(trading_day, "09:15:00.000")?,
                parse_market_timestamp(trading_day, "11:30:00.000")?,
            ),
            (
                parse_market_timestamp(trading_day, "13:00:00.000")?,
                parse_market_timestamp(trading_day, "15:00:00.000")?,
            ),
        ];
        let interval_ns = duration_nanos(schedule.interval)?;
        let mut cursor = Self {
            sessions,
            interval_ns,
            session: 0,
            next: None,
        };
        cursor.start_session()?;
        Ok(cursor)
    }

    pub(crate) fn drain_due(&mut self, quote_time_ns: i64) -> Result<Vec<i64>, ProductionError> {
        let mut due = Vec::new();
        while let Some(next) = self.next {
            if next > quote_time_ns {
                break;
            }
            due.push(next);
            self.advance()?;
        }
        Ok(due)
    }

    pub(crate) fn finish(&mut self) -> Result<Vec<i64>, ProductionError> {
        self.drain_due(i64::MAX)
    }

    fn start_session(&mut self) -> Result<(), ProductionError> {
        if self.session >= self.sessions.len() {
            self.next = None;
            return Ok(());
        }
        let (start, end) = self.sessions[self.session];
        let first = start
            .checked_add(self.interval_ns)
            .ok_or(ProductionError::Arithmetic("snapshot boundary"))?;
        self.next = (first <= end).then_some(first);
        if self.next.is_none() {
            self.session += 1;
            self.start_session()?;
        }
        Ok(())
    }

    fn advance(&mut self) -> Result<(), ProductionError> {
        let Some(current) = self.next else {
            return Ok(());
        };
        let candidate = current
            .checked_add(self.interval_ns)
            .ok_or(ProductionError::Arithmetic("snapshot boundary"))?;
        if candidate <= self.sessions[self.session].1 {
            self.next = Some(candidate);
        } else {
            self.session += 1;
            self.start_session()?;
        }
        Ok(())
    }
}

fn duration_nanos(duration: Duration) -> Result<i64, ProductionError> {
    i64::try_from(duration.as_nanos()).map_err(|_| ProductionError::Arithmetic("snapshot interval"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::SnapshotCursor;
    use crate::{SnapshotSchedule, TradingDay};

    fn day() -> TradingDay {
        match TradingDay::from_yyyymmdd(20_260_828) {
            Some(value) => value,
            None => std::process::abort(),
        }
    }

    #[test]
    fn emits_boundaries_before_equal_time_event() {
        let schedule = match SnapshotSchedule::new(Duration::from_secs(30), 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let mut cursor = match SnapshotCursor::new(day(), &schedule) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let boundary = match super::parse_market_timestamp(day(), "09:15:30.000") {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let due = match cursor.drain_due(boundary) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        assert_eq!(due, vec![boundary]);
    }

    #[test]
    fn skips_lunch_grid() {
        let schedule = match SnapshotSchedule::new(Duration::from_secs(60), 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let mut cursor = match SnapshotCursor::new(day(), &schedule) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let due = match cursor.finish() {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let morning_end = match super::parse_market_timestamp(day(), "11:30:00.000") {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let afternoon_first = match super::parse_market_timestamp(day(), "13:01:00.000") {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let morning_index = due.iter().position(|value| *value == morning_end);
        assert!(morning_index.is_some());
        if let Some(index) = morning_index {
            assert_eq!(due.get(index + 1), Some(&afternoon_first));
        }
    }
}
