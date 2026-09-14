//! Narrow reference-only compatibility; never changes replay or cached book values.
use serde::{Deserialize, Serialize};

pub(super) const TURNOVER_PRECISION_TAG: &str = "MISSING_RECEPTION_TURNOVER_12_DIGITS";
pub(super) const UPPER_LIMIT_PRECISION_TAG: &str = "MISSING_RECEPTION_SZ_UPPER_LIMIT_SENTINEL";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TurnoverPrecisionAudit {
    pub actual_units: u128,
    pub reference_units: u128,
    pub quantum_units: u128,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpperLimitNormalizationAudit {
    pub records: u64,
    pub first_source: String,
    pub raw_upper_units: i64,
    pub normalized_upper_units: i64,
}

/// Multiplication by a power of ten does not change significant-digit rounding.
/// Compare the rounded reconstructed value to the *unmodified* reference value.
/// Positive amounts use round-half-up; overflow never qualifies for compatibility.
pub(super) fn turnover_precision(
    missing_reception: bool,
    expected: u128,
    actual: u128,
) -> Option<TurnoverPrecisionAudit> {
    if !missing_reception || expected == actual {
        return None;
    }
    let digits = actual.checked_ilog10()?.checked_add(1)?;
    let discarded = digits.checked_sub(12)?;
    if discarded == 0 {
        return None;
    }
    let quantum = 10_u128.checked_pow(discarded)?;
    let rounded = (actual / quantum)
        .checked_add(u128::from(actual % quantum >= quantum / 2))?
        .checked_mul(quantum)?;
    (rounded == expected).then_some(TurnoverPrecisionAudit {
        actual_units: actual,
        reference_units: expected,
        quantum_units: quantum,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn twelve_digits_are_exact_integer_rounding_not_an_epsilon() {
        for (actual, expected) in [
            (135_011_623_292_600, 135_011_623_293_000),
            (41_026_830_961_490, 41_026_830_961_500),
            (18_414_873_243_130, 18_414_873_243_100),
            (1_234_567_890_125, 1_234_567_890_130),
            (9_999_999_999_995, 10_000_000_000_000),
        ] {
            assert!(turnover_precision(true, expected, actual).is_some());
            assert!(turnover_precision(false, expected, actual).is_none());
            assert!(turnover_precision(true, expected + 1, actual).is_none());
        }
        for (expected, actual) in [
            (0, 0),
            (1, 0),
            (100, 101),
            (999, 999),
            (u128::MAX, u128::MAX - 1),
        ] {
            assert!(turnover_precision(true, expected, actual).is_none());
        }
    }
}
