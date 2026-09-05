use std::fmt;

/// A monotonic timestamp carried only by legacy QTP input.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LegacySteadyTimestampNs(i64);

impl LegacySteadyTimestampNs {
    #[must_use]
    pub const fn from_nanos(nanoseconds: i64) -> Self {
        Self(nanoseconds)
    }

    #[must_use]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }
}

/// A required local receive or generation timestamp in Unix epoch nanoseconds.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LocalTimestampNs(i64);

impl LocalTimestampNs {
    #[must_use]
    pub const fn from_nanos(nanoseconds: i64) -> Self {
        Self(nanoseconds)
    }

    #[must_use]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }
}

/// A required exchange quote timestamp in Unix epoch nanoseconds.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct QuoteTimestampNs(i64);

impl QuoteTimestampNs {
    #[must_use]
    pub const fn from_nanos(nanoseconds: i64) -> Self {
        Self(nanoseconds)
    }

    #[must_use]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }
}

/// Source timestamps attached to a raw market-data record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawEventTime {
    pub steady_time: Option<LegacySteadyTimestampNs>,
    pub local_time: LocalTimestampNs,
    pub quote_time: QuoteTimestampNs,
}

/// Instrument symbol.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Symbol(Box<str>);

impl Symbol {
    #[must_use]
    pub fn new(value: impl Into<Box<str>>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Symbol {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Symbol {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Supported securities exchange.
///
/// The variants use the exchanges' standard English abbreviations:
/// `Sse` means Shanghai Stock Exchange and `Szse` means Shenzhen Stock
/// Exchange.
/// Rust spells enum variants in UpperCamelCase; external CLI,
/// directory and report market codes remain `SH` and `SZ`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Market {
    Sse,
    Szse,
}

/// Trading day encoded as a validated `YYYYMMDD` value.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TradingDay(u32);

impl TradingDay {
    #[must_use]
    pub const fn from_yyyymmdd(value: u32) -> Option<Self> {
        let year = value / 10_000;
        let month = value / 100 % 100;
        let day = value % 100;
        if year == 0 || month == 0 || month > 12 || day == 0 {
            return None;
        }
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let days = match month {
            2 if leap => 29,
            2 => 28,
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        if day > days {
            return None;
        }
        Some(Self(value))
    }

    #[must_use]
    pub const fn as_yyyymmdd(self) -> u32 {
        self.0
    }
}

/// Identity of one order book.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BookKey {
    pub market: Market,
    pub trading_day: TradingDay,
    pub symbol: Symbol,
}

/// Buy or sell side accepted by the core.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Side {
    Buy,
    Sell,
}

macro_rules! positive_newtype {
    ($name:ident, $inner:ty) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name($inner);

        impl $name {
            #[must_use]
            pub const fn new(value: $inner) -> Option<Self> {
                if value == 0 { None } else { Some(Self(value)) }
            }

            #[must_use]
            pub const fn get(self) -> $inner {
                self.0
            }
        }
    };
}

positive_newtype!(Quantity, u64);
positive_newtype!(OrderId, u64);
positive_newtype!(RawSequence, u64);
positive_newtype!(ApplySequence, u64);
positive_newtype!(ChannelId, u32);

/// Positive integer price units.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Price(i64);

impl Price {
    #[must_use]
    pub const fn from_units(units: i64) -> Option<Self> {
        if units <= 0 { None } else { Some(Self(units)) }
    }

    #[must_use]
    pub const fn units(self) -> i64 {
        self.0
    }
}

/// Decimal scale used to convert display prices into integer units.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PriceScale {
    decimal_places: u8,
    multiplier: u64,
}

impl PriceScale {
    pub const MAX_DECIMAL_PLACES: u8 = 9;

    #[must_use]
    pub const fn from_decimal_places(decimal_places: u8) -> Option<Self> {
        if decimal_places > Self::MAX_DECIMAL_PLACES {
            return None;
        }
        Some(Self {
            decimal_places,
            multiplier: 10_u64.pow(decimal_places as u32),
        })
    }

    #[must_use]
    pub const fn decimal_places(self) -> u8 {
        self.decimal_places
    }

    #[must_use]
    pub const fn multiplier(self) -> u64 {
        self.multiplier
    }
}

/// Full identity of an order within one book.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OrderKey {
    pub channel_id: ChannelId,
    pub side: Side,
    pub order_id: OrderId,
}

/// Metadata shared by normalized events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventMeta {
    pub book_key: BookKey,
    pub raw_sequence: RawSequence,
    pub apply_sequence: ApplySequence,
    pub local_time: LocalTimestampNs,
    pub quote_time: QuoteTimestampNs,
}

/// Legacy QTP order side before normalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawOrderSide {
    Buy,
    Sell,
    Borrow,
    Loan,
}

/// Legacy QTP order type before normalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawOrderType {
    MarketPrice,
    LimitPrice,
    ForwardBestPrice,
    ReverseBestPrice,
    Cancelled,
}

/// Legacy QTP transaction type before normalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawTradeType {
    Trade,
    Cancelled,
}

/// Legacy QTP aggressor flag retained for audit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawTradeSide {
    Unknown,
    Buy,
    Sell,
}

/// Raw legacy order record. Signed identifiers and `f64` prices are preserved.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderRecord {
    pub event_time: RawEventTime,
    pub symbol: Symbol,
    pub kind: RawOrderType,
    pub side: RawOrderSide,
    pub channel_no: i32,
    pub sequence: i64,
    pub order_id: i64,
    pub price: f64,
    pub quantity: u64,
}

/// Raw legacy trade or cancellation record.
#[derive(Clone, Debug, PartialEq)]
pub struct TradeRecord {
    pub event_time: RawEventTime,
    pub symbol: Symbol,
    pub kind: RawTradeType,
    pub side: RawTradeSide,
    pub channel_no: i32,
    pub sequence: i64,
    pub price: f64,
    pub quantity: u64,
    pub bid_order_id: i64,
    pub ask_order_id: i64,
}

/// Raw input accepted by legacy normalization.
#[derive(Clone, Debug, PartialEq)]
pub enum MarketDataRecord {
    Order(OrderRecord),
    Trade(TradeRecord),
}

/// Price resolution performed against the current book at apply time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PricingInstruction {
    Provided(Price),
    SameSideBest,
    OppositeBest,
    /// No effective price is available or needed for an always-hidden order.
    Unpriced,
}

/// Placement behavior after the effective price is resolved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrossingBehavior {
    Rest,
    HideIfCrossing,
    /// Start as an aggressive hidden order, then price any unfilled remainder
    /// at the latest trade price and let it rest once it no longer crosses.
    ///
    /// Shenzhen `OrdType='1'` market orders use this market-to-limit behavior.
    RestAtLastTradePrice,
    /// Keep the order out of visible depth for its entire lifetime.
    AlwaysHide,
}

/// Reference to one side of a trade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderReference {
    Absent,
    Resolved(OrderKey),
    Unresolved { side: Side, order_id: OrderId },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddOrder {
    pub meta: EventMeta,
    pub order_key: OrderKey,
    pub pricing: PricingInstruction,
    pub crossing: CrossingBehavior,
    pub quantity: Quantity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderCancel {
    pub meta: EventMeta,
    pub order_key: OrderKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Trade {
    pub meta: EventMeta,
    pub bid_order: OrderReference,
    pub ask_order: OrderReference,
    pub price: Price,
    pub quantity: Quantity,
}

/// Event accepted by the deterministic order-book state machine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BookEvent {
    AddOrder(AddOrder),
    OrderCancel(OrderCancel),
    Trade(Trade),
}

impl BookEvent {
    #[must_use]
    pub const fn meta(&self) -> &EventMeta {
        match self {
            Self::AddOrder(event) => &event.meta,
            Self::OrderCancel(event) => &event.meta,
            Self::Trade(event) => &event.meta,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PriceScale, TradingDay};

    #[test]
    fn validates_trading_days() {
        assert!(TradingDay::from_yyyymmdd(20_240_229).is_some());
        assert!(TradingDay::from_yyyymmdd(20_230_229).is_none());
        assert!(TradingDay::from_yyyymmdd(20_241_301).is_none());
    }

    #[test]
    fn price_scale_is_bounded() {
        assert_eq!(
            PriceScale::from_decimal_places(4).map(PriceScale::multiplier),
            Some(10_000)
        );
        assert!(PriceScale::from_decimal_places(10).is_none());
    }
}
