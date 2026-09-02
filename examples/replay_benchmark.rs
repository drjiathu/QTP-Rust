use std::error::Error;
use std::time::Instant;

use qtp_core::{
    BookConfig, BookKey, LegacyContext, LegacyReplay, LocalTimestampNs, Market, OrderBook,
    OrderRecord, PriceScale, QuoteTimestampNs, RawEventTime, RawOrderSide, RawOrderType, Symbol,
    TradingDay,
};

fn main() -> Result<(), Box<dyn Error>> {
    let record_count = std::env::args()
        .nth(1)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100_000);
    let trading_day = TradingDay::from_yyyymmdd(20_250_102)
        .ok_or_else(|| std::io::Error::other("invalid benchmark trading day"))?;
    let price_scale = PriceScale::from_decimal_places(4)
        .ok_or_else(|| std::io::Error::other("invalid benchmark price scale"))?;
    let book_key = BookKey {
        market: Market::Sse,
        trading_day,
        symbol: Symbol::from("600000.SH"),
    };
    let context = LegacyContext {
        book_key: book_key.clone(),
        price_scale,
    };
    let orders = (1..=record_count)
        .map(|index| OrderRecord {
            event_time: RawEventTime {
                steady_time: None,
                local_time: LocalTimestampNs::from_nanos(index as i64),
                quote_time: QuoteTimestampNs::from_nanos(index as i64),
            },
            symbol: book_key.symbol.clone(),
            kind: RawOrderType::LimitPrice,
            side: if index % 2 == 0 {
                RawOrderSide::Buy
            } else {
                RawOrderSide::Sell
            },
            channel_no: 1,
            sequence: index as i64,
            order_id: index as i64,
            price: 10.0 + (index % 100) as f64 / 10_000.0,
            quantity: 100,
        })
        .collect::<Vec<_>>();
    let mut book = OrderBook::new(BookConfig::new(book_key, price_scale));
    let mut replay = LegacyReplay::new(context);

    let started = Instant::now();
    replay.replay(&mut book, &orders, &[])?;
    let elapsed = started.elapsed();

    println!(
        "records={record_count} elapsed={elapsed:?} active_orders={}",
        book.summary().active_order_count
    );
    Ok(())
}
