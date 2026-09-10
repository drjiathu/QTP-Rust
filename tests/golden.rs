mod common;

use common::{add, add_with_crossing, cancel, key, strict_book, trade};
use qtp_core::{BookEvent, OrderBook, OrderKey, OrderReference, Side};

#[test]
fn basic_lifecycle_matches_golden() {
    let bid_101 = key(Side::Buy, 1, 101);
    let bid_102 = key(Side::Buy, 1, 102);
    let ask_201 = key(Side::Sell, 1, 201);
    let regular_keys = [bid_101, bid_102, ask_201];
    let regular_events = [
        add(1, 1, bid_101, 100_000, 100),
        add(2, 2, bid_102, 100_000, 50),
        add(3, 3, ask_201, 101_000, 120),
        trade(
            4,
            4,
            OrderReference::Resolved(bid_101),
            OrderReference::Resolved(ask_201),
            100_500,
            40,
        ),
        cancel(5, 5, bid_102),
        trade(
            6,
            6,
            OrderReference::Resolved(bid_101),
            OrderReference::Resolved(ask_201),
            100_700,
            60,
        ),
        cancel(7, 7, ask_201),
    ];
    let actual = run_scenario("R", regular_events, &regular_keys);
    let expected = include_str!("fixtures/golden/basic_lifecycle.txt");
    assert_eq!(actual, expected);
}

#[test]
fn hidden_reentry_matches_golden() {
    let ask_301 = key(Side::Sell, 1, 301);
    let bid_302 = key(Side::Buy, 1, 302);
    let hidden_keys = [ask_301, bid_302];
    let hidden_events = [
        add(1, 1, ask_301, 100_000, 100),
        add_with_crossing(2, 2, bid_302, 101_000, 150),
        trade(
            3,
            3,
            OrderReference::Resolved(bid_302),
            OrderReference::Resolved(ask_301),
            100_000,
            100,
        ),
    ];
    let actual = run_scenario("H", hidden_events, &hidden_keys);
    let expected = include_str!("fixtures/golden/hidden_reentry.txt");
    assert_eq!(actual, expected);
}

fn run_scenario(
    label_prefix: &str,
    events: impl IntoIterator<Item = BookEvent>,
    tracked_orders: &[OrderKey],
) -> String {
    let mut book = strict_book();
    let mut output = String::new();

    for (index, event) in events.into_iter().enumerate() {
        let checkpoint = format!("{label_prefix}{}", index + 1);
        let result = book.apply(event);

        assert!(
            result.is_ok(),
            "checkpoint {checkpoint}: apply failed: {result:?}"
        );

        output.push_str(&snapshot(&checkpoint, &book, tracked_orders));
    }

    output
}

fn snapshot(label: &str, book: &OrderBook, keys: &[qtp_core::OrderKey]) -> String {
    let summary = book.summary();
    let last = summary
        .statistics
        .last_price
        .map_or_else(|| "-".to_owned(), |price| price.units().to_string());
    let range = match (summary.statistics.low_price, summary.statistics.high_price) {
        (Some(low), Some(high)) => format!("{}/{}", low.units(), high.units()),
        _ => "-/-".to_owned(),
    };
    format!(
        "{label}|last={last}|range={range}|qty={}|turnover={}|trades={}|bids={}|asks={}|active={}\n",
        summary.statistics.total_quantity,
        summary.statistics.total_turnover_units,
        summary.statistics.trade_count,
        levels(book, Side::Buy),
        levels(book, Side::Sell),
        active_orders(book, keys),
    )
}

fn levels(book: &OrderBook, side: Side) -> String {
    let levels = book.levels(side);
    if levels.is_empty() {
        return "-".to_owned();
    }
    levels
        .into_iter()
        .map(|level| {
            let orders = book
                .orders_at(side, level.price)
                .into_iter()
                .map(|order| format!("{}:{}", order.key.order_id.get(), order.remaining_quantity))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "{}:{}[{}]",
                level.price.units(),
                level.total_quantity,
                orders
            )
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn active_orders(book: &OrderBook, keys: &[qtp_core::OrderKey]) -> String {
    let values = keys
        .iter()
        .filter_map(|key| book.order(key))
        .map(|order| {
            let location = match order.location {
                qtp_core::OrderLocation::Resting => "R",
                qtp_core::OrderLocation::Aggressive => "A",
            };
            format!(
                "{}:{}:{location}",
                order.key.order_id.get(),
                order.remaining_quantity
            )
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        "-".to_owned()
    } else {
        values.join(",")
    }
}
