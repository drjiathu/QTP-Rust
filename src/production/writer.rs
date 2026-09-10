use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Array, StringArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use crate::{Market, TradingDay};

use super::{BookSnapshot, ProductionError, SnapshotKind};

const WRITE_BATCH_SIZE: usize = 4_096;

pub(crate) struct SnapshotWriter {
    path: PathBuf,
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    depth: usize,
    buffer: Vec<BookSnapshot>,
    rows: u64,
}

impl SnapshotWriter {
    pub(crate) fn create(
        output_root: &Path,
        day: TradingDay,
        market: Market,
        channel: u32,
        depth: usize,
    ) -> Result<Self, ProductionError> {
        let directory = output_root
            .join(format!("date={}", day.as_yyyymmdd()))
            .join(format!(
                "market={}",
                match market {
                    Market::Sse => "SH",
                    Market::Szse => "SZ",
                }
            ))
            .join(format!("channel={channel}"));
        fs::create_dir_all(&directory).map_err(|error| ProductionError::io(&directory, error))?;
        let path = directory.join("part-0.parquet");
        let file = File::create(&path).map_err(|error| ProductionError::io(&path, error))?;
        let schema = snapshot_schema(depth);
        let writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
            .map_err(|error| ProductionError::parquet(&path, error))?;
        Ok(Self {
            path,
            writer,
            schema,
            depth,
            buffer: Vec::with_capacity(WRITE_BATCH_SIZE),
            rows: 0,
        })
    }

    pub(crate) fn push(&mut self, snapshot: BookSnapshot) -> Result<(), ProductionError> {
        self.buffer.push(snapshot);
        if self.buffer.len() >= WRITE_BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn close(mut self) -> Result<u64, ProductionError> {
        self.flush()?;
        self.writer
            .close()
            .map_err(|error| ProductionError::parquet(&self.path, error))?;
        Ok(self.rows)
    }

    fn flush(&mut self) -> Result<(), ProductionError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = snapshots_to_batch(&self.buffer, Arc::clone(&self.schema), self.depth)?;
        self.writer
            .write(&batch)
            .map_err(|error| ProductionError::parquet(&self.path, error))?;
        self.rows = self
            .rows
            .checked_add(self.buffer.len() as u64)
            .ok_or(ProductionError::Arithmetic("snapshot row count"))?;
        self.buffer.clear();
        Ok(())
    }
}

fn snapshot_schema(depth: usize) -> Arc<Schema> {
    let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Shanghai".into()));
    let mut fields = vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("channel", DataType::UInt32, false),
        Field::new("snapshot_kind", DataType::Utf8, false),
        Field::new("boundary_time", timestamp.clone(), false),
        Field::new("last_local_time", timestamp.clone(), true),
        Field::new("last_quote_time", timestamp, true),
    ];
    for side in ["ask", "bid"] {
        for level in 1..=depth {
            fields.push(Field::new(
                format!("{side}_price_{level}"),
                DataType::Decimal128(18, 4),
                true,
            ));
            fields.push(Field::new(
                format!("{side}_quantity_{level}"),
                DataType::UInt64,
                true,
            ));
            fields.push(Field::new(
                format!("{side}_order_count_{level}"),
                DataType::UInt64,
                true,
            ));
        }
    }
    fields.extend([
        Field::new("total_bid_quantity", DataType::UInt64, false),
        Field::new("weighted_bid_price", DataType::Decimal128(18, 4), true),
        Field::new("total_ask_quantity", DataType::UInt64, false),
        Field::new("weighted_ask_price", DataType::Decimal128(18, 4), true),
        Field::new("last_price", DataType::Decimal128(18, 4), true),
        Field::new("high_price", DataType::Decimal128(18, 4), true),
        Field::new("low_price", DataType::Decimal128(18, 4), true),
        Field::new("trade_count", DataType::UInt64, false),
        Field::new("trade_quantity", DataType::UInt64, false),
        Field::new("turnover", DataType::Decimal128(38, 4), false),
    ]);
    Arc::new(Schema::new(fields))
}

fn snapshots_to_batch(
    snapshots: &[BookSnapshot],
    schema: Arc<Schema>,
    depth: usize,
) -> Result<RecordBatch, ProductionError> {
    let timezone = Some("Asia/Shanghai");
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            snapshots.iter().map(|snapshot| snapshot.symbol.as_str()),
        )),
        Arc::new(UInt32Array::from_iter_values(
            snapshots.iter().map(|snapshot| snapshot.channel),
        )),
        Arc::new(StringArray::from_iter_values(snapshots.iter().map(
            |snapshot| match snapshot.kind {
                SnapshotKind::Scheduled => "scheduled",
                SnapshotKind::MarketClose => "market_close",
            },
        ))),
        Arc::new(
            TimestampNanosecondArray::from_iter_values(
                snapshots.iter().map(|snapshot| snapshot.boundary_time_ns),
            )
            .with_timezone_opt(timezone),
        ),
        Arc::new(
            TimestampNanosecondArray::from(
                snapshots
                    .iter()
                    .map(|snapshot| snapshot.last_local_time_ns)
                    .collect::<Vec<_>>(),
            )
            .with_timezone_opt(timezone),
        ),
        Arc::new(
            TimestampNanosecondArray::from(
                snapshots
                    .iter()
                    .map(|snapshot| snapshot.last_quote_time_ns)
                    .collect::<Vec<_>>(),
            )
            .with_timezone_opt(timezone),
        ),
    ];
    for asks in [true, false] {
        for level in 0..depth {
            let values = snapshots.iter().map(|snapshot| {
                let levels = if asks {
                    &snapshot.book.asks
                } else {
                    &snapshot.book.bids
                };
                levels.get(level)
            });
            let price = values
                .clone()
                .map(|entry| entry.map(|entry| i128::from(entry.price_units)))
                .collect::<Vec<_>>();
            let quantity = values
                .clone()
                .map(|entry| entry.map(|entry| entry.quantity))
                .collect::<Vec<_>>();
            let count = values
                .map(|entry| entry.map(|entry| entry.order_count))
                .collect::<Vec<_>>();
            columns.push(Arc::new(decimal_array(price, 18, 4)?));
            columns.push(Arc::new(UInt64Array::from(quantity)));
            columns.push(Arc::new(UInt64Array::from(count)));
        }
    }
    columns.push(Arc::new(UInt64Array::from_iter_values(
        snapshots
            .iter()
            .map(|snapshot| snapshot.book.total_bid_quantity),
    )));
    columns.push(Arc::new(decimal_array(
        snapshots
            .iter()
            .map(|snapshot| snapshot.book.weighted_bid_price_units.map(i128::from))
            .collect(),
        18,
        4,
    )?));
    columns.push(Arc::new(UInt64Array::from_iter_values(
        snapshots
            .iter()
            .map(|snapshot| snapshot.book.total_ask_quantity),
    )));
    columns.push(Arc::new(decimal_array(
        snapshots
            .iter()
            .map(|snapshot| snapshot.book.weighted_ask_price_units.map(i128::from))
            .collect(),
        18,
        4,
    )?));
    columns.push(Arc::new(decimal_array(
        snapshots
            .iter()
            .map(|snapshot| snapshot.book.last_price_units.map(i128::from))
            .collect(),
        18,
        4,
    )?));
    columns.push(Arc::new(decimal_array(
        snapshots
            .iter()
            .map(|snapshot| snapshot.book.high_price_units.map(i128::from))
            .collect(),
        18,
        4,
    )?));
    columns.push(Arc::new(decimal_array(
        snapshots
            .iter()
            .map(|snapshot| snapshot.book.low_price_units.map(i128::from))
            .collect(),
        18,
        4,
    )?));
    columns.push(Arc::new(UInt64Array::from_iter_values(
        snapshots.iter().map(|snapshot| snapshot.book.trade_count),
    )));
    columns.push(Arc::new(UInt64Array::from_iter_values(
        snapshots
            .iter()
            .map(|snapshot| snapshot.book.trade_quantity),
    )));
    let turnover = snapshots
        .iter()
        .map(|snapshot| {
            i128::try_from(snapshot.book.turnover_units)
                .map(Some)
                .map_err(|_| ProductionError::Arithmetic("turnover Decimal128 conversion"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    columns.push(Arc::new(decimal_array(turnover, 38, 4)?));
    RecordBatch::try_new(schema, columns).map_err(|source| ProductionError::Arrow {
        context: "snapshot output batch",
        source,
    })
}

fn decimal_array(
    values: Vec<Option<i128>>,
    precision: u8,
    scale: i8,
) -> Result<Decimal128Array, ProductionError> {
    Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|source| ProductionError::Arrow {
            context: "decimal snapshot column",
            source,
        })
}
