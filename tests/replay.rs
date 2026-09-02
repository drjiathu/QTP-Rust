mod common;

use common::{legacy_context, raw_order, raw_trade, strict_book};
use qtp_core::{
    LegacyReplay, NormalizeError, RawOrderSide, RawTradeType, ReplayErrorKind, ReplaySource, Side,
};

#[test]
fn merges_order_and_trade_streams_by_raw_sequence() {
    let orders = [
        raw_order(1, RawOrderSide::Buy, 1, 101, 10.0, 100),
        raw_order(3, RawOrderSide::Sell, 1, 202, 10.1, 100),
    ];
    let trades = [raw_trade(4, 10.05, 40, 101, 202)];
    let mut book = strict_book();
    let mut replay = LegacyReplay::new(legacy_context());

    assert!(replay.replay(&mut book, &orders, &trades).is_ok());
    assert_eq!(
        book.summary().last_apply_sequence.map(|seq| seq.get()),
        Some(3)
    );
    assert_eq!(book.summary().statistics.total_quantity, 40);
    assert_eq!(
        book.order(&common::key(Side::Buy, 1, 101))
            .map(|order| order.remaining_quantity),
        Some(60)
    );
    assert_eq!(
        book.order(&common::key(Side::Sell, 1, 202))
            .map(|order| order.remaining_quantity),
        Some(60)
    );
}

#[test]
fn rejects_cross_stream_sequence_ambiguity_without_applying_either_record() {
    let orders = [raw_order(1, RawOrderSide::Buy, 1, 101, 10.0, 100)];
    let trades = [raw_trade(1, 10.0, 10, 0, 0)];
    let mut book = strict_book();
    let mut replay = LegacyReplay::new(legacy_context());

    let error = replay.replay(&mut book, &orders, &trades);
    assert!(matches!(
        error,
        Err(qtp_core::ReplayError {
            kind: ReplayErrorKind::AmbiguousSequence { sequence: 1 },
            source: ReplaySource::Both,
            ..
        })
    ));
    assert!(book.last_applied_meta().is_none());
}

#[test]
fn validates_each_stream_before_mutating_the_book() {
    let orders = [
        raw_order(2, RawOrderSide::Buy, 1, 101, 10.0, 100),
        raw_order(1, RawOrderSide::Buy, 1, 102, 9.9, 100),
    ];
    let mut book = strict_book();
    let mut replay = LegacyReplay::new(legacy_context());

    assert!(matches!(
        replay.replay(&mut book, &orders, &[]),
        Err(qtp_core::ReplayError {
            kind: ReplayErrorKind::NonIncreasingSequence {
                previous: 2,
                current: 1
            },
            ..
        })
    ));
    assert!(book.is_empty());
}

#[test]
fn failed_record_can_be_retried_without_advancing_sequence_or_reference_state() {
    let orders = [raw_order(1, RawOrderSide::Buy, 1, 101, 10.0, 10)];
    let excessive = [raw_trade(2, 10.0, 11, 101, 0)];
    let mut book = strict_book();
    let mut replay = LegacyReplay::new(legacy_context());

    let first = replay.replay(&mut book, &orders, &excessive);
    assert!(first.as_ref().is_err_and(|error| {
        error.order_index == 1
            && error.trade_index == 0
            && matches!(
                &error.kind,
                ReplayErrorKind::Apply(inner)
                    if matches!(inner.as_ref(), qtp_core::BookError::TradeOverfill { .. })
            )
    }));
    assert_eq!(
        book.summary().last_apply_sequence.map(|seq| seq.get()),
        Some(1)
    );

    let corrected = [raw_trade(2, 10.0, 5, 101, 0)];
    assert!(replay.replay(&mut book, &[], &corrected).is_ok());
    assert_eq!(
        book.summary().last_apply_sequence.map(|seq| seq.get()),
        Some(2)
    );
    assert_eq!(
        book.order(&common::key(Side::Buy, 1, 101))
            .map(|order| order.remaining_quantity),
        Some(5)
    );
}

#[test]
fn multiple_channels_make_trade_reference_ambiguous() {
    let orders = [
        raw_order(1, RawOrderSide::Buy, 1, 101, 10.0, 100),
        raw_order(2, RawOrderSide::Buy, 2, 101, 9.9, 100),
    ];
    let trades = [raw_trade(3, 10.0, 10, 101, 0)];
    let mut book = strict_book();
    let mut replay = LegacyReplay::new(legacy_context());

    let result = replay.replay(&mut book, &orders, &trades);
    assert!(result.as_ref().is_err_and(|error| {
        matches!(
            &error.kind,
            ReplayErrorKind::Normalize(inner)
                if matches!(
                    inner.as_ref(),
                    NormalizeError::AmbiguousOrderReference {
                        side: Side::Buy,
                        ..
                    }
                )
        )
    }));
    assert_eq!(book.summary().active_order_count, 2);
}

#[test]
fn transaction_stream_cancellation_removes_all_remaining_quantity() {
    let orders = [raw_order(1, RawOrderSide::Buy, 1, 101, 10.0, 100)];
    let trade = raw_trade(2, 10.0, 40, 101, 0);
    let mut cancellation = raw_trade(3, f64::NAN, 0, 101, 0);
    cancellation.kind = RawTradeType::Cancelled;
    let trades = [trade, cancellation];
    let mut book = strict_book();
    let mut replay = LegacyReplay::new(legacy_context());

    assert!(replay.replay(&mut book, &orders, &trades).is_ok());
    assert!(book.order(&common::key(Side::Buy, 1, 101)).is_none());
    assert!(book.levels(Side::Buy).is_empty());
    assert_eq!(book.summary().statistics.total_quantity, 40);
}
