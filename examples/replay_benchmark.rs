//! Core-only apply benchmark; excludes event construction and production file processing.
use std::error::Error;
use std::io;
use std::time::Instant;

use qtp_core::{
    AddOrder, ApplySequence, BookConfig, BookEvent, BookKey, ChannelId, CrossingBehavior,
    EventMeta, LocalTimestampNs, Market, OrderBook, OrderId, OrderKey, Price, PriceScale,
    PricingInstruction, Quantity, QuoteTimestampNs, RawSequence, Side, Symbol, TradingDay,
};

fn required<T>(value: Option<T>) -> Result<T, io::Error> {
    value.ok_or_else(|| io::Error::other("invalid benchmark scalar"))
}

fn main() -> Result<(), Box<dyn Error>> {
    let record_count = std::env::args()
        .nth(1)
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(100_000);
    let price_scale = required(PriceScale::from_decimal_places(4))?;
    let book_key = BookKey {
        market: Market::Sse,
        trading_day: required(TradingDay::from_yyyymmdd(20_250_102))?,
        symbol: Symbol::from("600000"),
    };
    let events = (1..=record_count)
        .map(|sequence| -> Result<BookEvent, Box<dyn Error>> {
            let timestamp = i64::try_from(sequence)?;
            let side = if sequence % 2 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let price_units = match side {
                Side::Buy => 100_000 - timestamp % 100,
                Side::Sell => 101_000 + timestamp % 100,
            };
            Ok(BookEvent::AddOrder(AddOrder {
                meta: EventMeta {
                    book_key: book_key.clone(),
                    raw_sequence: required(RawSequence::new(sequence))?,
                    apply_sequence: required(ApplySequence::new(sequence))?,
                    local_time: LocalTimestampNs::from_nanos(timestamp),
                    quote_time: QuoteTimestampNs::from_nanos(timestamp),
                },
                order_key: OrderKey {
                    channel_id: required(ChannelId::new(1))?,
                    side,
                    order_id: required(OrderId::new(sequence))?,
                },
                pricing: PricingInstruction::Provided(required(Price::from_units(price_units))?),
                crossing: CrossingBehavior::Rest,
                quantity: required(Quantity::new(100))?,
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut book = OrderBook::new(BookConfig::new(book_key, price_scale));

    let started = Instant::now();
    for event in events {
        book.apply(event)?;
    }
    let elapsed = started.elapsed();

    println!(
        "mode=core_apply records={record_count} elapsed={elapsed:?} active_orders={}",
        book.summary().active_order_count
    );
    Ok(())
}
