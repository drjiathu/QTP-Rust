use thiserror::Error;

use super::references::{OrderReferenceIndex, Resolution};
use super::rules::LegacyQtpRules;
use crate::market_data::{
    AddOrder, ApplySequence, BookEvent, BookKey, ChannelId, EventMeta, MarketDataRecord,
    OrderCancel, OrderId, OrderKey, OrderRecord, OrderReference, Price, PriceScale,
    PricingInstruction, Quantity, RawOrderSide, RawOrderType, RawSequence, RawTradeType, Side,
    Trade, TradeRecord,
};

const PRICE_UNIT_TOLERANCE: f64 = 1.0e-6;

/// Context supplied by the caller rather than inferred from legacy records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacyContext {
    pub book_key: BookKey,
    pub price_scale: PriceScale,
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum NormalizeError {
    #[error("record symbol {actual} does not match book symbol {expected}")]
    SymbolMismatch { expected: String, actual: String },
    #[error("unsupported legacy order side: {0:?}")]
    UnsupportedOrderSide(RawOrderSide),
    #[error("{field} must be positive, got {value}")]
    NonPositiveSigned { field: &'static str, value: i64 },
    #[error("{field} must be positive")]
    ZeroUnsigned { field: &'static str },
    #[error("price must be finite and positive, got {0}")]
    InvalidPrice(f64),
    #[error("price {price} is not aligned to the configured decimal scale")]
    MisalignedPrice { price: f64 },
    #[error("scaled price {price} exceeds the core price range")]
    PriceOverflow { price: f64 },
    #[error("trade cancellation must contain exactly one non-zero order reference")]
    AmbiguousCancellation,
    #[error("order reference is ambiguous for {side:?} order {order_id:?}")]
    AmbiguousOrderReference { side: Side, order_id: OrderId },
    #[error("cannot resolve cancellation target for {side:?} order {order_id:?}")]
    UnresolvedCancellation { side: Side, order_id: OrderId },
}

/// Converts a raw QTP record into one deterministic book event.
pub fn normalize(
    record: &MarketDataRecord,
    apply_sequence: ApplySequence,
    context: &LegacyContext,
    references: &OrderReferenceIndex,
) -> Result<BookEvent, NormalizeError> {
    match record {
        MarketDataRecord::Order(order) => {
            normalize_order(order, apply_sequence, context, references)
        }
        MarketDataRecord::Trade(trade) => {
            normalize_trade(trade, apply_sequence, context, references)
        }
    }
}

fn normalize_order(
    order: &OrderRecord,
    apply_sequence: ApplySequence,
    context: &LegacyContext,
    _references: &OrderReferenceIndex,
) -> Result<BookEvent, NormalizeError> {
    validate_symbol(&order.symbol, context)?;
    let side = normalize_side(order.side)?;
    let order_key = OrderKey {
        channel_id: positive_i32("channel_no", order.channel_no)?,
        side,
        order_id: positive_i64("order_id", order.order_id)?,
    };
    let meta = EventMeta {
        book_key: context.book_key.clone(),
        raw_sequence: positive_i64("sequence", order.sequence)?,
        apply_sequence,
        local_time: order.event_time.local_time,
        quote_time: order.event_time.quote_time,
    };

    let pricing = match order.kind {
        RawOrderType::LimitPrice | RawOrderType::ReverseBestPrice => {
            PricingInstruction::Provided(normalize_price(order.price, context.price_scale)?)
        }
        RawOrderType::MarketPrice => PricingInstruction::OppositeBest,
        RawOrderType::ForwardBestPrice => PricingInstruction::SameSideBest,
        RawOrderType::Cancelled => {
            return Ok(BookEvent::OrderCancel(OrderCancel { meta, order_key }));
        }
    };
    let quantity =
        Quantity::new(order.quantity).ok_or(NormalizeError::ZeroUnsigned { field: "quantity" })?;

    Ok(BookEvent::AddOrder(AddOrder {
        meta,
        order_key,
        pricing,
        crossing: LegacyQtpRules::crossing_behavior(order.event_time.quote_time),
        quantity,
    }))
}

fn normalize_trade(
    trade: &TradeRecord,
    apply_sequence: ApplySequence,
    context: &LegacyContext,
    references: &OrderReferenceIndex,
) -> Result<BookEvent, NormalizeError> {
    validate_symbol(&trade.symbol, context)?;
    let _channel = positive_i32("channel_no", trade.channel_no)?;
    let meta = EventMeta {
        book_key: context.book_key.clone(),
        raw_sequence: positive_i64("sequence", trade.sequence)?,
        apply_sequence,
        local_time: trade.event_time.local_time,
        quote_time: trade.event_time.quote_time,
    };

    if trade.kind == RawTradeType::Cancelled {
        let target = cancellation_target(trade, references)?;
        return Ok(BookEvent::OrderCancel(OrderCancel {
            meta,
            order_key: target,
        }));
    }

    let quantity =
        Quantity::new(trade.quantity).ok_or(NormalizeError::ZeroUnsigned { field: "quantity" })?;
    Ok(BookEvent::Trade(Trade {
        meta,
        bid_order: trade_reference(trade.bid_order_id, Side::Buy, references)?,
        ask_order: trade_reference(trade.ask_order_id, Side::Sell, references)?,
        price: normalize_price(trade.price, context.price_scale)?,
        quantity,
    }))
}

fn validate_symbol(
    actual: &crate::market_data::Symbol,
    context: &LegacyContext,
) -> Result<(), NormalizeError> {
    if actual == &context.book_key.symbol {
        Ok(())
    } else {
        Err(NormalizeError::SymbolMismatch {
            expected: context.book_key.symbol.to_string(),
            actual: actual.to_string(),
        })
    }
}

fn normalize_side(side: RawOrderSide) -> Result<Side, NormalizeError> {
    match side {
        RawOrderSide::Buy => Ok(Side::Buy),
        RawOrderSide::Sell => Ok(Side::Sell),
        RawOrderSide::Borrow | RawOrderSide::Loan => {
            Err(NormalizeError::UnsupportedOrderSide(side))
        }
    }
}

fn positive_i32(field: &'static str, value: i32) -> Result<ChannelId, NormalizeError> {
    let unsigned = u32::try_from(value).map_err(|_| NormalizeError::NonPositiveSigned {
        field,
        value: i64::from(value),
    })?;
    ChannelId::new(unsigned).ok_or(NormalizeError::NonPositiveSigned {
        field,
        value: i64::from(value),
    })
}

fn positive_i64<T>(field: &'static str, value: i64) -> Result<T, NormalizeError>
where
    T: PositiveFromU64,
{
    let unsigned =
        u64::try_from(value).map_err(|_| NormalizeError::NonPositiveSigned { field, value })?;
    T::from_positive(unsigned).ok_or(NormalizeError::NonPositiveSigned { field, value })
}

trait PositiveFromU64: Sized {
    fn from_positive(value: u64) -> Option<Self>;
}

impl PositiveFromU64 for OrderId {
    fn from_positive(value: u64) -> Option<Self> {
        Self::new(value)
    }
}

impl PositiveFromU64 for RawSequence {
    fn from_positive(value: u64) -> Option<Self> {
        Self::new(value)
    }
}

fn normalize_price(raw: f64, scale: PriceScale) -> Result<Price, NormalizeError> {
    if !raw.is_finite() || raw <= 0.0 {
        return Err(NormalizeError::InvalidPrice(raw));
    }
    let scaled = raw * scale.multiplier() as f64;
    if scaled > i64::MAX as f64 {
        return Err(NormalizeError::PriceOverflow { price: raw });
    }
    let rounded = scaled.round();
    if (scaled - rounded).abs() > PRICE_UNIT_TOLERANCE {
        return Err(NormalizeError::MisalignedPrice { price: raw });
    }
    Price::from_units(rounded as i64).ok_or(NormalizeError::InvalidPrice(raw))
}

fn trade_reference(
    raw_order_id: i64,
    side: Side,
    references: &OrderReferenceIndex,
) -> Result<OrderReference, NormalizeError> {
    if raw_order_id == 0 {
        return Ok(OrderReference::Absent);
    }
    let order_id = positive_i64("trade order reference", raw_order_id)?;
    match references.resolve(side, order_id) {
        Resolution::Missing => Ok(OrderReference::Unresolved { side, order_id }),
        Resolution::Unique(key) => Ok(OrderReference::Resolved(key)),
        Resolution::Ambiguous => Err(NormalizeError::AmbiguousOrderReference { side, order_id }),
    }
}

fn cancellation_target(
    trade: &TradeRecord,
    references: &OrderReferenceIndex,
) -> Result<OrderKey, NormalizeError> {
    let (side, raw_order_id) = match (trade.bid_order_id, trade.ask_order_id) {
        (bid, 0) if bid > 0 => (Side::Buy, bid),
        (0, ask) if ask > 0 => (Side::Sell, ask),
        _ => return Err(NormalizeError::AmbiguousCancellation),
    };
    let order_id = positive_i64("cancellation order reference", raw_order_id)?;
    match references.resolve(side, order_id) {
        Resolution::Unique(key) => Ok(key),
        Resolution::Missing => Err(NormalizeError::UnresolvedCancellation { side, order_id }),
        Resolution::Ambiguous => Err(NormalizeError::AmbiguousOrderReference { side, order_id }),
    }
}

#[cfg(test)]
mod tests {
    use super::{LegacyContext, NormalizeError, normalize};
    use crate::legacy::OrderReferenceIndex;
    use crate::market_data::{
        ApplySequence, BookEvent, BookKey, CrossingBehavior, LocalTimestampNs, Market,
        MarketDataRecord, OrderRecord, PriceScale, PricingInstruction, QuoteTimestampNs,
        RawEventTime, RawOrderSide, RawOrderType, Symbol, TradingDay,
    };

    fn context() -> LegacyContext {
        let day = match TradingDay::from_yyyymmdd(20_250_102) {
            Some(day) => day,
            None => std::process::abort(),
        };
        let scale = match PriceScale::from_decimal_places(4) {
            Some(scale) => scale,
            None => std::process::abort(),
        };
        LegacyContext {
            book_key: BookKey {
                market: Market::Sse,
                trading_day: day,
                symbol: Symbol::from("600000.SH"),
            },
            price_scale: scale,
        }
    }

    fn order(kind: RawOrderType) -> MarketDataRecord {
        MarketDataRecord::Order(OrderRecord {
            event_time: RawEventTime {
                steady_time: None,
                local_time: LocalTimestampNs::from_nanos(2),
                quote_time: QuoteTimestampNs::from_nanos(3),
            },
            symbol: Symbol::from("600000.SH"),
            kind,
            side: RawOrderSide::Buy,
            channel_no: 1,
            sequence: 1,
            order_id: 10,
            price: 10.1234,
            quantity: 100,
        })
    }

    fn first_sequence() -> ApplySequence {
        match ApplySequence::new(1) {
            Some(sequence) => sequence,
            None => std::process::abort(),
        }
    }

    #[test]
    fn maps_limit_and_reverse_best_to_provided_price() {
        for kind in [RawOrderType::LimitPrice, RawOrderType::ReverseBestPrice] {
            let event = normalize(
                &order(kind),
                first_sequence(),
                &context(),
                &OrderReferenceIndex::new(),
            );
            assert!(matches!(
                event,
                Ok(BookEvent::AddOrder(crate::market_data::AddOrder {
                    pricing: PricingInstruction::Provided(_),
                    ..
                }))
            ));
        }
    }

    #[test]
    fn ignores_legacy_steady_time() {
        let mut first = order(RawOrderType::LimitPrice);
        let mut second = first.clone();
        if let MarketDataRecord::Order(record) = &mut first {
            record.event_time.steady_time =
                Some(crate::market_data::LegacySteadyTimestampNs::from_nanos(1));
        }
        if let MarketDataRecord::Order(record) = &mut second {
            record.event_time.steady_time =
                Some(crate::market_data::LegacySteadyTimestampNs::from_nanos(999));
        }
        let references = OrderReferenceIndex::new();
        assert_eq!(
            normalize(&first, first_sequence(), &context(), &references),
            normalize(&second, first_sequence(), &context(), &references)
        );
    }

    #[test]
    fn rejects_misaligned_prices() {
        let mut raw = order(RawOrderType::LimitPrice);
        if let MarketDataRecord::Order(record) = &mut raw {
            record.price = 10.12345;
        }
        assert!(matches!(
            normalize(
                &raw,
                first_sequence(),
                &context(),
                &OrderReferenceIndex::new()
            ),
            Err(NormalizeError::MisalignedPrice { .. })
        ));
    }

    #[test]
    fn quote_time_selects_crossing_behavior() {
        let event = normalize(
            &order(RawOrderType::LimitPrice),
            first_sequence(),
            &context(),
            &OrderReferenceIndex::new(),
        );
        assert!(matches!(
            event,
            Ok(BookEvent::AddOrder(crate::market_data::AddOrder {
                crossing: CrossingBehavior::Rest,
                ..
            }))
        ));
    }
}
