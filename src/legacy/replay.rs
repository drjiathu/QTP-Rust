use std::error::Error;
use std::fmt;

use super::{LegacyContext, NormalizeError, OrderReferenceIndex, normalize};
use crate::market_data::{ApplySequence, BookEvent, MarketDataRecord, OrderRecord, TradeRecord};
use crate::order_book::{BookError, OrderBook};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplaySource {
    Order,
    Trade,
    Both,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReplayErrorKind {
    ContextMismatch,
    NonEmptyBook,
    InvalidRawSequence { value: i64 },
    NonIncreasingSequence { previous: i64, current: i64 },
    AmbiguousSequence { sequence: i64 },
    Normalize(Box<NormalizeError>),
    Apply(Box<BookError>),
}

/// Fail-fast replay error. Indices point at the unconsumed record in the
/// slices passed to the failed call.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplayError {
    pub kind: ReplayErrorKind,
    pub source: ReplaySource,
    pub order_index: usize,
    pub trade_index: usize,
    pub apply_sequence: Option<ApplySequence>,
    pub record: Option<Box<MarketDataRecord>>,
}

impl fmt::Display for ReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "replay failed at order index {}, trade index {}: ",
            self.order_index, self.trade_index
        )?;
        match &self.kind {
            ReplayErrorKind::ContextMismatch => {
                formatter.write_str("legacy context does not match the order book")
            }
            ReplayErrorKind::NonEmptyBook => {
                formatter.write_str("a new replay cannot attach to a non-empty order book")
            }
            ReplayErrorKind::InvalidRawSequence { value } => {
                write!(formatter, "raw sequence must be positive, got {value}")
            }
            ReplayErrorKind::NonIncreasingSequence { previous, current } => write!(
                formatter,
                "raw sequence is not strictly increasing: {previous} then {current}"
            ),
            ReplayErrorKind::AmbiguousSequence { sequence } => {
                write!(formatter, "order and trade share raw sequence {sequence}")
            }
            ReplayErrorKind::Normalize(error) => write!(formatter, "normalization error: {error}"),
            ReplayErrorKind::Apply(error) => write!(formatter, "order-book error: {error}"),
        }
    }
}

impl Error for ReplayError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            ReplayErrorKind::Normalize(error) => Some(error.as_ref()),
            ReplayErrorKind::Apply(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

/// Stateful legacy replayer.
///
/// After a failure, call `replay` again on slices beginning at the returned
/// indices. The failed record was not applied and did not update the historical
/// reference index or the next application sequence.
#[derive(Clone, Debug)]
pub struct LegacyReplay {
    context: LegacyContext,
    references: OrderReferenceIndex,
    started: bool,
    last_order_sequence: Option<i64>,
    last_trade_sequence: Option<i64>,
}

impl LegacyReplay {
    #[must_use]
    pub fn new(context: LegacyContext) -> Self {
        Self {
            context,
            references: OrderReferenceIndex::new(),
            started: false,
            last_order_sequence: None,
            last_trade_sequence: None,
        }
    }

    #[must_use]
    pub const fn context(&self) -> &LegacyContext {
        &self.context
    }

    pub fn replay(
        &mut self,
        book: &mut OrderBook,
        orders: &[OrderRecord],
        trades: &[TradeRecord],
    ) -> Result<(), ReplayError> {
        self.validate_context(book)?;
        validate_stream(
            orders,
            self.last_order_sequence,
            ReplaySource::Order,
            |record| record.sequence,
            MarketDataRecord::Order,
        )?;
        validate_stream(
            trades,
            self.last_trade_sequence,
            ReplaySource::Trade,
            |record| record.sequence,
            MarketDataRecord::Trade,
        )?;
        self.started = true;

        let mut order_index = 0;
        let mut trade_index = 0;
        while order_index < orders.len() || trade_index < trades.len() {
            let source = match (orders.get(order_index), trades.get(trade_index)) {
                (Some(order), Some(trade)) if order.sequence < trade.sequence => {
                    ReplaySource::Order
                }
                (Some(order), Some(trade)) if trade.sequence < order.sequence => {
                    ReplaySource::Trade
                }
                (Some(order), Some(_)) => {
                    return Err(ReplayError {
                        kind: ReplayErrorKind::AmbiguousSequence {
                            sequence: order.sequence,
                        },
                        source: ReplaySource::Both,
                        order_index,
                        trade_index,
                        apply_sequence: book.next_apply_sequence().ok(),
                        record: None,
                    });
                }
                (Some(_), None) => ReplaySource::Order,
                (None, Some(_)) => ReplaySource::Trade,
                (None, None) => break,
            };

            let record = match source {
                ReplaySource::Order => MarketDataRecord::Order(orders[order_index].clone()),
                ReplaySource::Trade => MarketDataRecord::Trade(trades[trade_index].clone()),
                ReplaySource::Both => {
                    return Err(ReplayError {
                        kind: ReplayErrorKind::AmbiguousSequence { sequence: 0 },
                        source,
                        order_index,
                        trade_index,
                        apply_sequence: book.next_apply_sequence().ok(),
                        record: None,
                    });
                }
            };
            let apply_sequence = book.next_apply_sequence().map_err(|error| ReplayError {
                kind: ReplayErrorKind::Apply(Box::new(error)),
                source,
                order_index,
                trade_index,
                apply_sequence: None,
                record: Some(Box::new(record.clone())),
            })?;
            let event = normalize(&record, apply_sequence, &self.context, &self.references)
                .map_err(|error| ReplayError {
                    kind: ReplayErrorKind::Normalize(Box::new(error)),
                    source,
                    order_index,
                    trade_index,
                    apply_sequence: Some(apply_sequence),
                    record: Some(Box::new(record.clone())),
                })?;
            let added_key = match &event {
                BookEvent::AddOrder(add) => Some(add.order_key),
                BookEvent::OrderCancel(_) | BookEvent::Trade(_) => None,
            };
            book.apply(event).map_err(|error| ReplayError {
                kind: ReplayErrorKind::Apply(Box::new(error)),
                source,
                order_index,
                trade_index,
                apply_sequence: Some(apply_sequence),
                record: Some(Box::new(record)),
            })?;
            if let Some(key) = added_key {
                self.references.register(key);
            }

            match source {
                ReplaySource::Order => {
                    self.last_order_sequence = Some(orders[order_index].sequence);
                    order_index += 1;
                }
                ReplaySource::Trade => {
                    self.last_trade_sequence = Some(trades[trade_index].sequence);
                    trade_index += 1;
                }
                ReplaySource::Both => {}
            }
        }
        Ok(())
    }

    fn validate_context(&self, book: &OrderBook) -> Result<(), ReplayError> {
        if book.config().book_key != self.context.book_key
            || book.config().price_scale != self.context.price_scale
        {
            return Err(ReplayError {
                kind: ReplayErrorKind::ContextMismatch,
                source: ReplaySource::Both,
                order_index: 0,
                trade_index: 0,
                apply_sequence: None,
                record: None,
            });
        }
        if !self.started && book.last_applied_meta().is_some() {
            return Err(ReplayError {
                kind: ReplayErrorKind::NonEmptyBook,
                source: ReplaySource::Both,
                order_index: 0,
                trade_index: 0,
                apply_sequence: None,
                record: None,
            });
        }
        Ok(())
    }
}

fn validate_stream<T: Clone>(
    records: &[T],
    previous_from_prior_call: Option<i64>,
    source: ReplaySource,
    sequence: impl Fn(&T) -> i64,
    into_record: impl Fn(T) -> MarketDataRecord,
) -> Result<(), ReplayError> {
    let mut previous = previous_from_prior_call;
    for (index, raw_record) in records.iter().enumerate() {
        let current = sequence(raw_record);
        if current <= 0 {
            return Err(ReplayError {
                kind: ReplayErrorKind::InvalidRawSequence { value: current },
                source,
                order_index: if source == ReplaySource::Order {
                    index
                } else {
                    0
                },
                trade_index: if source == ReplaySource::Trade {
                    index
                } else {
                    0
                },
                apply_sequence: None,
                record: Some(Box::new(into_record(raw_record.clone()))),
            });
        }
        if let Some(previous_value) = previous {
            if current <= previous_value {
                return Err(ReplayError {
                    kind: ReplayErrorKind::NonIncreasingSequence {
                        previous: previous_value,
                        current,
                    },
                    source,
                    order_index: if source == ReplaySource::Order {
                        index
                    } else {
                        0
                    },
                    trade_index: if source == ReplaySource::Trade {
                        index
                    } else {
                        0
                    },
                    apply_sequence: None,
                    record: Some(Box::new(into_record(raw_record.clone()))),
                });
            }
        }
        previous = Some(current);
    }
    Ok(())
}
