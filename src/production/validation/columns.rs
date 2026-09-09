//! Bind Arrow columns once per batch. Row decoding performs only indexed reads.
use super::*;
use parquet::arrow::ProjectionMask;

struct DecimalField<'a> {
    name: String,
    array: &'a Decimal128Array,
    scale: i8,
}
impl<'a> DecimalField<'a> {
    fn bind(
        path: &Path,
        batch: &'a RecordBatch,
        name: &str,
        scale: i8,
    ) -> Result<Self, ProductionError> {
        Ok(Self {
            name: name.to_owned(),
            array: decimal128(path, batch, name, scale)?,
            scale,
        })
    }
    fn price(&self, row: usize) -> Result<Option<i64>, ProductionError> {
        let name = &self.name;
        if self.array.is_null(row) || self.array.value(row) == 0 {
            return Ok(None);
        }
        let raw = self.array.value(row);
        let scaled = rescale_nonnegative_decimal(raw, self.scale, 4).ok_or_else(|| {
            ProductionError::Validation(format!("invalid raw reference {name}={raw} at row {row}"))
        })?;
        i64::try_from(scaled).map(Some).map_err(|_| {
            ProductionError::Validation(format!(
                "raw reference {name}={raw} does not fit i64 at row {row}"
            ))
        })
    }
    fn amount(&self, row: usize) -> Result<u128, ProductionError> {
        let name = &self.name;
        if self.array.is_null(row) {
            return Err(ProductionError::Validation(format!(
                "raw reference {name} is null at row {row}"
            )));
        }
        let raw = self.array.value(row);
        let scaled = rescale_nonnegative_decimal(raw, self.scale, 4).ok_or_else(|| {
            ProductionError::Validation(format!("invalid raw reference {name}={raw} at row {row}"))
        })?;
        u128::try_from(scaled).map_err(|_| {
            ProductionError::Validation(format!(
                "raw reference {name}={raw} does not fit u128 at row {row}"
            ))
        })
    }
}
enum QuantityArray<'a> {
    Decimal(&'a Decimal128Array),
    Signed(&'a Int64Array),
    Unsigned(&'a UInt32Array),
}
struct QuantityField<'a> {
    name: String,
    array: QuantityArray<'a>,
}
impl<'a> QuantityField<'a> {
    fn bind(
        path: &Path,
        batch: &'a RecordBatch,
        name: &str,
        market: Market,
        count: bool,
    ) -> Result<Self, ProductionError> {
        let array = if count && market == Market::Sse {
            QuantityArray::Unsigned(uint32(path, batch, name)?)
        } else if market == Market::Sse {
            QuantityArray::Decimal(decimal128(path, batch, name, 3)?)
        } else {
            QuantityArray::Signed(int64(path, batch, name)?)
        };
        Ok(Self {
            name: name.to_owned(),
            array,
        })
    }
    fn optional(&self, row: usize) -> Result<Option<u64>, ProductionError> {
        let name = &self.name;
        match self.array {
            QuantityArray::Unsigned(a) => Ok((!a.is_null(row)).then(|| u64::from(a.value(row)))),
            QuantityArray::Signed(a) => {
                if a.is_null(row) {
                    return Ok(None);
                }
                u64::try_from(a.value(row)).map(Some).map_err(|_| {
                    ProductionError::Validation(format!(
                        "reference {name} is negative at row {row}"
                    ))
                })
            }
            QuantityArray::Decimal(a) => {
                if a.is_null(row) {
                    return Ok(None);
                }
                let raw = a.value(row);
                if raw < 0 || raw % 1000 != 0 {
                    return Err(ProductionError::Validation(format!(
                        "raw reference quantity {name}={raw} is not an integer at row {row}"
                    )));
                }
                u64::try_from(raw / 1000).map(Some).map_err(|_| {
                    ProductionError::Validation(format!(
                        "raw reference quantity {name}={raw} does not fit u64 at row {row}"
                    ))
                })
            }
        }
    }
    fn required(&self, row: usize) -> Result<u64, ProductionError> {
        self.optional(row)?.ok_or_else(|| {
            let prefix = if matches!(self.array, QuantityArray::Signed(_)) {
                "reference"
            } else {
                "raw reference"
            };
            ProductionError::Validation(format!("{prefix} {} is null at row {row}", self.name))
        })
    }
}
struct LevelColumns<'a> {
    price: DecimalField<'a>,
    quantity: QuantityField<'a>,
    count: &'a UInt32Array,
}
pub(super) struct RawSnapshotColumns<'a> {
    asks: Vec<LevelColumns<'a>>,
    bids: Vec<LevelColumns<'a>>,
    total_bid: QuantityField<'a>,
    total_ask: QuantityField<'a>,
    weighted_bid: DecimalField<'a>,
    weighted_ask: DecimalField<'a>,
    last: DecimalField<'a>,
    high: DecimalField<'a>,
    low: DecimalField<'a>,
    trade_count: QuantityField<'a>,
    trade_quantity: QuantityField<'a>,
    turnover: DecimalField<'a>,
    pre_close: Option<DecimalField<'a>>,
    limits: Result<(&'a Decimal128Array, &'a Decimal128Array), String>,
}
impl<'a> RawSnapshotColumns<'a> {
    pub fn bind(
        market: Market,
        path: &Path,
        batch: &'a RecordBatch,
    ) -> Result<Self, ProductionError> {
        let scale = if market == Market::Sse { 3 } else { 6 };
        let levels = |side: &str| -> Result<Vec<LevelColumns<'a>>, ProductionError> {
            (1..=10)
                .map(|level| {
                    Ok(LevelColumns {
                        price: DecimalField::bind(
                            path,
                            batch,
                            &format!("{side}Price{level}"),
                            scale,
                        )?,
                        quantity: QuantityField::bind(
                            path,
                            batch,
                            &format!("{side}Volume{level}"),
                            market,
                            false,
                        )?,
                        count: uint32(
                            path,
                            batch,
                            &format!("NumOrders{}{level}", if side == "Ask" { "S" } else { "B" }),
                        )?,
                    })
                })
                .collect()
        };
        let (bid, ask, wb, wa, count, qty, amount_scale) = match market {
            Market::Sse => (
                "TotalBidVol",
                "TotalAskVol",
                "WAvgBidPri",
                "WAvgAskPri",
                "TradNumber",
                "TradVolume",
                5,
            ),
            Market::Szse => (
                "TotalBidQty",
                "TotalOfferQty",
                "WeightedAvgBidPx",
                "WeightedAvgOfferPx",
                "TurnNum",
                "Volume",
                4,
            ),
        };
        Ok(Self {
            asks: levels("Ask")?,
            bids: levels("Bid")?,
            total_bid: QuantityField::bind(path, batch, bid, market, false)?,
            total_ask: QuantityField::bind(path, batch, ask, market, false)?,
            weighted_bid: DecimalField::bind(path, batch, wb, scale)?,
            weighted_ask: DecimalField::bind(path, batch, wa, scale)?,
            last: DecimalField::bind(path, batch, "LastPrice", scale)?,
            high: DecimalField::bind(path, batch, "HighPrice", scale)?,
            low: DecimalField::bind(path, batch, "LowPrice", scale)?,
            trade_count: QuantityField::bind(path, batch, count, market, true)?,
            trade_quantity: QuantityField::bind(path, batch, qty, market, false)?,
            turnover: DecimalField::bind(path, batch, "Turnover", amount_scale)?,
            pre_close: (market == Market::Szse)
                .then(|| DecimalField::bind(path, batch, "PreCloPrice", 4))
                .transpose()?,
            limits: decimal128(path, batch, "HighLimitPrice", 6)
                .and_then(|high| decimal128(path, batch, "LowLimitPrice", 6).map(|low| (high, low)))
                .map_err(|e| e.to_string()),
        })
    }
    pub fn pre_close(&self, row: usize) -> Result<Option<i64>, ProductionError> {
        self.pre_close
            .as_ref()
            .map(|field| field.price(row))
            .transpose()
            .map(Option::flatten)
    }
    pub fn limits(&self, row: usize) -> Result<(i64, i64), String> {
        let (high, low) = self.limits.as_ref().map_err(Clone::clone)?;
        let read = |a: &Decimal128Array, name: &str| {
            if a.is_null(row) || a.value(row) % 100 != 0 {
                return Err(format!("missing/invalid {name}"));
            }
            i64::try_from(a.value(row) / 100).map_err(|_| format!("overflow in {name}"))
        };
        read(high, "HighLimitPrice").and_then(|h| read(low, "LowLimitPrice").map(|l| (h, l)))
    }
    pub fn view(&self, path: &Path, row: usize) -> Result<SnapshotBookView, ProductionError> {
        let decode = |side: &str,
                      columns: &[LevelColumns<'_>]|
         -> Result<SnapshotLevels, ProductionError> {
            let mut levels = SnapshotLevels::new();
            for (i, column) in columns.iter().enumerate() {
                let price = column.price.price(row)?;
                let quantity = column.quantity.optional(row)?;
                let count =
                    (!column.count.is_null(row)).then(|| u64::from(column.count.value(row)));
                match (price, quantity, count) {
                    (None, None | Some(0), None | Some(0)) => {}
                    (Some(price_units), Some(quantity), Some(order_count))
                        if quantity > 0 && order_count > 0 =>
                    {
                        levels.push(SnapshotLevel {
                            price_units,
                            quantity,
                            order_count,
                        })
                    }
                    values => {
                        return Err(ProductionError::Validation(format!(
                            "inconsistent raw snapshot {side} level {} in {} row {row}: {values:?}",
                            i + 1,
                            path.display()
                        )));
                    }
                }
            }
            Ok(levels)
        };
        // Preserve field/error evaluation order of the original decoder.
        let asks = decode("Ask", &self.asks)?;
        let bids = decode("Bid", &self.bids)?;
        let total_bid_quantity = self.total_bid.required(row)?;
        let weighted_bid_price_units = self.weighted_bid.price(row)?;
        let total_ask_quantity = self.total_ask.required(row)?;
        let weighted_ask_price_units = self.weighted_ask.price(row)?;
        let trade_count = self.trade_count.required(row)?;
        let trade_quantity = self.trade_quantity.required(row)?;
        let turnover_units = self.turnover.amount(row)?;
        Ok(SnapshotBookView {
            bids,
            asks,
            total_bid_quantity,
            weighted_bid_price_units,
            total_ask_quantity,
            weighted_ask_price_units,
            last_price_units: self.last.price(row)?,
            high_price_units: self.high.price(row)?,
            low_price_units: self.low.price(row)?,
            trade_count,
            trade_quantity,
            turnover_units,
        })
    }
}

pub(super) fn projection(
    builder: &ParquetRecordBatchReaderBuilder<File>,
    market: Market,
    path: &Path,
) -> Result<ProjectionMask, ProductionError> {
    let mut names: Vec<String> = [
        "UpdateTime",
        "SecurityID",
        "LastPrice",
        "HighPrice",
        "LowPrice",
        "Turnover",
        "source_row_no",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let extra: &[&str] = match market {
        Market::Sse => &[
            "InstruStatus",
            "TradNumber",
            "TradVolume",
            "TotalBidVol",
            "WAvgBidPri",
            "TotalAskVol",
            "WAvgAskPri",
        ],
        Market::Szse => &[
            "TradingPhaseCode",
            "PreCloPrice",
            "TurnNum",
            "Volume",
            "TotalBidQty",
            "WeightedAvgBidPx",
            "TotalOfferQty",
            "WeightedAvgOfferPx",
        ],
    };
    names.extend(extra.iter().map(|s| (*s).to_owned()));
    for side in ["Ask", "Bid"] {
        for level in 1..=10 {
            names.extend([
                format!("{side}Price{level}"),
                format!("{side}Volume{level}"),
                format!("NumOrders{}{level}", if side == "Ask" { "S" } else { "B" }),
            ]);
        }
    }
    let mut indices = names
        .iter()
        .map(|name| {
            builder
                .schema()
                .index_of(name)
                .map_err(|_| ProductionError::Schema {
                    path: path.to_path_buf(),
                    detail: format!("missing reference field {name}"),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if market == Market::Szse {
        for name in ["HighLimitPrice", "LowLimitPrice"] {
            if let Ok(index) = builder.schema().index_of(name) {
                indices.push(index);
            }
        }
    }
    Ok(ProjectionMask::roots(builder.parquet_schema(), indices))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use arrow::array::ArrayRef;
    use std::sync::Arc;

    #[test]
    fn bound_decimal_preserves_rounding_nulls_and_scale_checks() {
        let array = Decimal128Array::from(vec![None, Some(0), Some(123_456), Some(-1)])
            .with_precision_and_scale(18, 6)
            .expect("decimal");
        let batch =
            RecordBatch::try_from_iter([("price", Arc::new(array) as ArrayRef)]).expect("batch");
        let path = Path::new("fixture.parquet");
        let field = DecimalField::bind(path, &batch, "price", 6).expect("binding");
        assert_eq!(field.price(0).expect("null"), None);
        assert_eq!(field.price(1).expect("zero"), None);
        assert_eq!(field.price(2).expect("rounded"), Some(1235));
        assert!(field.price(3).is_err());
        assert!(field.amount(0).is_err());
        assert_eq!(field.amount(2).expect("amount"), 1235);
        assert!(DecimalField::bind(path, &batch, "price", 4).is_err());
        assert!(DecimalField::bind(path, &batch, "absent", 6).is_err());
    }

    #[test]
    fn bound_quantity_rejects_fractional_negative_and_required_null() {
        let array = Decimal128Array::from(vec![None, Some(1_000), Some(1_001), Some(-1_000)])
            .with_precision_and_scale(18, 3)
            .expect("decimal");
        let batch =
            RecordBatch::try_from_iter([("qty", Arc::new(array) as ArrayRef)]).expect("batch");
        let field = QuantityField::bind(
            Path::new("fixture.parquet"),
            &batch,
            "qty",
            Market::Sse,
            false,
        )
        .expect("binding");
        assert_eq!(field.optional(0).expect("null"), None);
        assert!(field.required(0).is_err());
        assert_eq!(field.required(1).expect("integer"), 1);
        assert!(field.required(2).is_err());
        assert!(field.required(3).is_err());
    }
}
