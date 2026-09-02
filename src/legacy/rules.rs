use crate::market_data::{CrossingBehavior, QuoteTimestampNs};

/// Historical behavior implemented by the original QTP order book.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LegacyQtpRules;

impl LegacyQtpRules {
    const NANOS_PER_SECOND: i64 = 1_000_000_000;
    const NANOS_PER_DAY: i64 = 86_400 * Self::NANOS_PER_SECOND;
    const SHANGHAI_OFFSET_NANOS: i64 = 8 * 3_600 * Self::NANOS_PER_SECOND;
    const CONTINUOUS_START_NANOS: i64 = (9 * 3_600 + 30 * 60) * Self::NANOS_PER_SECOND;
    const CONTINUOUS_END_NANOS: i64 = (14 * 3_600 + 57 * 60) * Self::NANOS_PER_SECOND;

    /// Returns the crossing behavior corresponding to the old `[09:30, 14:57)`
    /// compatibility window in Asia/Shanghai.
    #[must_use]
    pub fn crossing_behavior(quote_time: QuoteTimestampNs) -> CrossingBehavior {
        let local_time_of_day = (i128::from(quote_time.as_nanos())
            + i128::from(Self::SHANGHAI_OFFSET_NANOS))
        .rem_euclid(i128::from(Self::NANOS_PER_DAY));
        if (i128::from(Self::CONTINUOUS_START_NANOS)..i128::from(Self::CONTINUOUS_END_NANOS))
            .contains(&local_time_of_day)
        {
            CrossingBehavior::HideIfCrossing
        } else {
            CrossingBehavior::Rest
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LegacyQtpRules;
    use crate::market_data::{CrossingBehavior, QuoteTimestampNs};

    const HOUR: i64 = 3_600_000_000_000;
    const MINUTE: i64 = 60_000_000_000;

    fn utc_for_shanghai(hour: i64, minute: i64) -> QuoteTimestampNs {
        QuoteTimestampNs::from_nanos((hour - 8) * HOUR + minute * MINUTE)
    }

    #[test]
    fn uses_the_legacy_continuous_window() {
        assert_eq!(
            LegacyQtpRules::crossing_behavior(utc_for_shanghai(9, 29)),
            CrossingBehavior::Rest
        );
        assert_eq!(
            LegacyQtpRules::crossing_behavior(utc_for_shanghai(9, 30)),
            CrossingBehavior::HideIfCrossing
        );
        assert_eq!(
            LegacyQtpRules::crossing_behavior(utc_for_shanghai(14, 56)),
            CrossingBehavior::HideIfCrossing
        );
        assert_eq!(
            LegacyQtpRules::crossing_behavior(utc_for_shanghai(14, 57)),
            CrossingBehavior::Rest
        );
    }
}
