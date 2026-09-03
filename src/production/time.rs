use std::time::Duration;

use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};

use crate::{QuoteTimestampNs, TradingDay};

use super::ProductionError;

const NANOS_PER_MILLISECOND: i64 = 1_000_000;

pub fn parse_duration(value: &str) -> Result<Duration, ProductionError> {
    let (digits, unit) = value
        .find(|character: char| !character.is_ascii_digit())
        .map_or((value, ""), |index| value.split_at(index));
    let amount = digits
        .parse::<u64>()
        .map_err(|_| ProductionError::InvalidRequest(format!("invalid duration: {value}")))?;
    if amount == 0 {
        return Err(ProductionError::InvalidRequest(
            "duration must be positive".to_owned(),
        ));
    }
    match unit {
        "ms" => Ok(Duration::from_millis(amount)),
        "s" => Ok(Duration::from_secs(amount)),
        "m" => Ok(Duration::from_secs(
            amount
                .checked_mul(60)
                .ok_or(ProductionError::Arithmetic("duration"))?,
        )),
        _ => Err(ProductionError::InvalidRequest(format!(
            "duration must use ms, s, or m: {value}"
        ))),
    }
}

pub fn parse_market_timestamp(
    trading_day: TradingDay,
    value: &str,
) -> Result<i64, ProductionError> {
    if value.len() != 12
        || value.as_bytes().get(2) != Some(&b':')
        || value.as_bytes().get(5) != Some(&b':')
        || value.as_bytes().get(8) != Some(&b'.')
    {
        return Err(ProductionError::InvalidRequest(format!(
            "timestamp must be HH:MM:SS.mmm: {value:?}"
        )));
    }
    let time = NaiveTime::parse_from_str(value, "%H:%M:%S%.3f").map_err(|_| {
        ProductionError::InvalidRequest(format!("invalid market timestamp: {value:?}"))
    })?;
    let date = trading_day.as_yyyymmdd().to_string();
    let date = NaiveDate::parse_from_str(&date, "%Y%m%d").map_err(|_| {
        ProductionError::InvalidRequest(format!(
            "invalid trading day: {}",
            trading_day.as_yyyymmdd()
        ))
    })?;
    let local = NaiveDateTime::new(date, time);
    let offset = FixedOffset::east_opt(8 * 60 * 60)
        .ok_or_else(|| ProductionError::InvalidRequest("invalid Shanghai offset".to_owned()))?;
    let timestamp = offset
        .from_local_datetime(&local)
        .single()
        .ok_or_else(|| ProductionError::InvalidRequest(format!("invalid local time: {value}")))?;
    timestamp
        .timestamp_millis()
        .checked_mul(NANOS_PER_MILLISECOND)
        .ok_or(ProductionError::Arithmetic("timestamp nanoseconds"))
}

pub(crate) fn time_of_day_nanos(timestamp: QuoteTimestampNs) -> i64 {
    const DAY: i64 = 86_400_000_000_000;
    const OFFSET: i64 = 8 * 3_600_000_000_000;
    (timestamp.as_nanos() + OFFSET).rem_euclid(DAY)
}

#[cfg(test)]
mod tests {
    use super::{parse_duration, parse_market_timestamp, time_of_day_nanos};
    use crate::{QuoteTimestampNs, TradingDay};

    fn day() -> TradingDay {
        match TradingDay::from_yyyymmdd(20_260_828) {
            Some(value) => value,
            None => std::process::abort(),
        }
    }

    #[test]
    fn parses_explicit_duration_units() {
        let millis = match parse_duration("100ms") {
            Ok(value) => value.as_millis(),
            Err(_) => std::process::abort(),
        };
        let seconds = match parse_duration("30s") {
            Ok(value) => value.as_secs(),
            Err(_) => std::process::abort(),
        };
        let minute = match parse_duration("1m") {
            Ok(value) => value.as_secs(),
            Err(_) => std::process::abort(),
        };
        assert_eq!(millis, 100);
        assert_eq!(seconds, 30);
        assert_eq!(minute, 60);
        assert!(parse_duration("100").is_err());
    }

    #[test]
    fn parses_shanghai_timestamp() {
        let nanos = match parse_market_timestamp(day(), "09:30:00.010") {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        assert_eq!(
            time_of_day_nanos(QuoteTimestampNs::from_nanos(nanos)),
            34_200_010_000_000
        );
    }

    #[test]
    fn rejects_non_millisecond_text() {
        assert!(parse_market_timestamp(day(), "09:30:00").is_err());
        assert!(parse_market_timestamp(day(), "25:00:00.000").is_err());
    }
}
