use std::time::Duration;

use chrono::NaiveDate;

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
    MarketTimestampParser::new(trading_day)?.parse(value)
}

/// The trading-day conversion is shared by every timestamp in an input stream.
pub(crate) struct MarketTimestampParser {
    midnight_millis: i64,
}

impl MarketTimestampParser {
    pub(crate) fn new(day: TradingDay) -> Result<Self, ProductionError> {
        let encoded = day.as_yyyymmdd();
        let midnight = NaiveDate::from_ymd_opt(
            (encoded / 10_000) as i32,
            encoded / 100 % 100,
            encoded % 100,
        )
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .ok_or_else(|| {
            ProductionError::InvalidRequest(format!("invalid trading day: {encoded}"))
        })?;
        Ok(Self {
            midnight_millis: midnight.and_utc().timestamp_millis() - 28_800_000,
        })
    }

    pub(crate) fn parse(&self, value: &str) -> Result<i64, ProductionError> {
        if value.len() != 12
            || value.as_bytes().get(2) != Some(&b':')
            || value.as_bytes().get(5) != Some(&b':')
            || value.as_bytes().get(8) != Some(&b'.')
        {
            return Err(ProductionError::InvalidRequest(format!(
                "timestamp must be HH:MM:SS.mmm: {value:?}"
            )));
        }
        let bytes = value.as_bytes();
        let digits = |range: std::ops::Range<usize>| -> Option<i64> {
            bytes[range].iter().try_fold(0_i64, |v, b| {
                b.is_ascii_digit().then(|| v * 10 + i64::from(b - b'0'))
            })
        };
        let millis = (|| {
            let hour = digits(0..2)?;
            let minute = digits(3..5)?;
            let second = digits(6..8)?;
            let fraction = digits(9..12)?;
            // Chrono accepts leap seconds at :60; retain that existing contract.
            (hour < 24 && minute < 60 && second <= 60)
                .then_some(((hour * 60 + minute) * 60 + second) * 1000 + fraction)
        })()
        .ok_or_else(|| {
            ProductionError::InvalidRequest(format!("invalid market timestamp: {value:?}"))
        })?;
        self.midnight_millis
            .checked_add(millis)
            .ok_or(ProductionError::Arithmetic("timestamp milliseconds"))?
            .checked_mul(NANOS_PER_MILLISECOND)
            .ok_or(ProductionError::Arithmetic("timestamp nanoseconds"))
    }
}

pub(crate) fn time_of_day_nanos(timestamp: QuoteTimestampNs) -> i64 {
    const DAY: i64 = 86_400_000_000_000;
    const OFFSET: i64 = 8 * 3_600_000_000_000;
    (timestamp.as_nanos() + OFFSET).rem_euclid(DAY)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
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

    #[test]
    fn fixed_parser_matches_chrono_across_day_and_overflow_boundaries() {
        use chrono::{FixedOffset, NaiveDate, NaiveTime, TimeZone};
        for encoded in [16770921, 19691231, 20000229, 20260828, 22620411] {
            let day = TradingDay::from_yyyymmdd(encoded).expect("day");
            let parser = super::MarketTimestampParser::new(day).expect("parser");
            let date = NaiveDate::parse_from_str(&encoded.to_string(), "%Y%m%d").expect("date");
            for hour in 0..24 {
                for minute in 0..60 {
                    for (second, fraction) in [(0, 0), (29, 499), (59, 999), (60, 1)] {
                        let value = format!("{hour:02}:{minute:02}:{second:02}.{fraction:03}");
                        let time = NaiveTime::parse_from_str(&value, "%H:%M:%S%.3f").expect("time");
                        let expected = FixedOffset::east_opt(28_800)
                            .expect("offset")
                            .from_local_datetime(&date.and_time(time))
                            .single()
                            .expect("local")
                            .timestamp_millis()
                            .checked_mul(1_000_000);
                        assert_eq!(parser.parse(&value).ok(), expected, "{encoded} {value}");
                    }
                }
            }
        }
    }

    #[test]
    fn fixed_parser_rejects_invalid_digits_and_fields() {
        let parser = super::MarketTimestampParser::new(day()).expect("parser");
        for value in [
            "",
            "09:30:00",
            "09:30:00.0000",
            "24:00:00.000",
            "09:60:00.000",
            "09:30:61.000",
            "0x:30:00.000",
            "09:30:00.0x0",
            "09-30:00.000",
            "é:30:00.000",
            "09:30:00.-01",
        ] {
            assert!(parser.parse(value).is_err(), "{value}");
        }
    }
}
