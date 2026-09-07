use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, Decimal128Array, Int32Array, Int64Array, LargeStringArray, UInt64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::Market;

use super::spool::{
    Aggressor, FinishedSpool, SpoolSet, SseKind, SseRow, SzExecutionKind, SzExecutionRow,
    SzOrderKind, SzOrderRow, SzSide,
};
use super::types::is_supported_symbol;
use super::{MarketDayRequest, ProductionError, parse_market_timestamp};

#[derive(Clone, Debug, Default)]
pub(crate) struct IngestStats {
    pub input_rows: u64,
    pub selected_rows: u64,
    pub excluded_rows: u64,
    pub excluded_after_cutoff_rows: u64,
    pub excluded_non_stock_rows: u64,
    pub excluded_unselected_stock_rows: u64,
    pub channels: usize,
    pub sz_phase_rows: u64,
    pub sz_phase_source_available: bool,
    pub sz_resumptions: HashMap<String, Vec<i64>>,
    symbol_channels: HashMap<String, u32>,
}

pub(crate) fn spool_inputs(
    request: &MarketDayRequest,
) -> Result<(FinishedSpool, IngestStats), ProductionError> {
    spool_inputs_before(request, None)
}

/// Only phase metadata is projected: reference depth and statistics never seed a book.
/// Tonglian flushes accumulated resumption orders with the resumption timestamp.
fn load_sz_resumptions(
    request: &MarketDayRequest,
    stats: &mut IngestStats,
    quote_time_exclusive: Option<i64>,
) -> Result<(), ProductionError> {
    let path = raw_path(request, "mdl_6_28_0");
    // Retain compatibility with tick-only fixtures and archives. The report makes
    // this limitation explicit; such inputs cannot identify intraday resumptions.
    if !path
        .try_exists()
        .map_err(|error| ProductionError::io(&path, error))?
    {
        return Ok(());
    }
    stats.sz_phase_source_available = true;
    let mut phases: HashMap<String, SzPhaseHistory> = HashMap::new();
    for batch in read_batches(&path, request.batch_size, "SZ", "mdl_6_28_0")? {
        let batch = batch.map_err(|source| ProductionError::Arrow {
            context: "Shenzhen phase batch",
            source,
        })?;
        let symbols = large_string_column(&path, &batch, "SecurityID")?;
        let times = large_string_column(&path, &batch, "UpdateTime")?;
        let codes = large_string_column(&path, &batch, "TradingPhaseCode")?;
        let rows = u64_column(&path, &batch, "source_row_no")?;
        for index in 0..batch.num_rows() {
            stats.sz_phase_rows += 1;
            let row = required_u64(&path, rows, index, "source_row_no", 0)?;
            let symbol = required_str(&path, symbols, index, "SecurityID", row)?;
            if !request.targets.contains(Market::Szse, symbol) {
                continue;
            }
            let time = parse_timestamp_field(request, &path, times, index, row, "UpdateTime")?;
            if quote_time_exclusive.is_some_and(|cutoff| time >= cutoff) {
                continue;
            }
            let code = required_str(&path, codes, index, "TradingPhaseCode", row)?.trim();
            let history = phases.entry(symbol.to_owned()).or_default();
            if history.last_time.is_some_and(|previous| time < previous) {
                return Err(invalid(&path, row, "UpdateTime", "phase time regressed"));
            }
            if history.observe(time, code) {
                stats
                    .sz_resumptions
                    .entry(symbol.to_owned())
                    .or_default()
                    .push(time);
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct SzPhaseHistory {
    last_time: Option<i64>,
    halted: bool,
}

impl SzPhaseHistory {
    fn observe(&mut self, time: i64, code: &str) -> bool {
        self.last_time = Some(time);
        match code {
            "H0" => {
                self.halted = true;
                false
            }
            "T0" => std::mem::take(&mut self.halted),
            _ => false,
        }
    }
}

pub(crate) fn spool_inputs_before(
    request: &MarketDayRequest,
    quote_time_exclusive: Option<i64>,
) -> Result<(FinishedSpool, IngestStats), ProductionError> {
    request
        .validate()
        .map_err(ProductionError::InvalidRequest)?;
    let label = format!(
        "{}-{}",
        request.trading_day.as_yyyymmdd(),
        match request.market {
            Market::Sse => "sh",
            Market::Szse => "sz",
        }
    );
    let mut stats = IngestStats::default();
    if request.market == Market::Szse {
        load_sz_resumptions(request, &mut stats, quote_time_exclusive)?;
    }
    let mut spool = SpoolSet::create(&request.temp_root, &label)?;
    let ingest_result = match request.market {
        Market::Sse => ingest_sse(request, &mut spool, &mut stats, quote_time_exclusive),
        Market::Szse => ingest_sz_orders(request, &mut spool, &mut stats, quote_time_exclusive)
            .and_then(|()| {
                ingest_sz_executions(request, &mut spool, &mut stats, quote_time_exclusive)
            }),
    };
    if let Err(source) = ingest_result {
        return Err(ProductionError::ReplayFailed {
            spool_path: spool.root().to_path_buf(),
            source: Box::new(source),
        });
    }
    let finished = spool.finish()?;
    stats.channels = finished.channels()?.len();
    Ok((finished, stats))
}

fn ingest_sse(
    request: &MarketDayRequest,
    spool: &mut SpoolSet,
    stats: &mut IngestStats,
    quote_time_exclusive: Option<i64>,
) -> Result<(), ProductionError> {
    let path = raw_path(request, "mdl_4_24_0");
    let mut last_sequences = HashMap::new();
    for batch in read_batches(&path, request.batch_size, "SH", "mdl_4_24_0")? {
        let batch = batch.map_err(|source| ProductionError::Arrow {
            context: "raw Parquet batch",
            source,
        })?;
        validate_sse_schema(&path, &batch)?;
        let biz = u64_column(&path, &batch, "BizIndex")?;
        let channels = u64_column(&path, &batch, "Channel")?;
        let symbols = large_string_column(&path, &batch, "SecurityID")?;
        let quote_times = large_string_column(&path, &batch, "TickTime")?;
        let kinds = large_string_column(&path, &batch, "Type")?;
        let bids = u64_column(&path, &batch, "BuyOrderNO")?;
        let asks = u64_column(&path, &batch, "SellOrderNO")?;
        let prices = decimal_column(&path, &batch, "Price")?;
        let quantities = i64_column(&path, &batch, "Qty")?;
        let flags = large_string_column(&path, &batch, "TickBSFlag")?;
        let local_times = large_string_column(&path, &batch, "LocalTime")?;
        let source_rows = u64_column(&path, &batch, "source_row_no")?;
        for index in 0..batch.num_rows() {
            stats.input_rows += 1;
            let source_row = required_u64(&path, source_rows, index, "source_row_no", 0)?;
            let channel_raw = required_u64(&path, channels, index, "Channel", source_row)?;
            let channel = u32::try_from(channel_raw).map_err(|_| {
                invalid(
                    &path,
                    source_row,
                    "Channel",
                    format!("out of range: {channel_raw}"),
                )
            })?;
            if channel == 0 {
                return Err(invalid(&path, source_row, "Channel", "must be positive"));
            }
            let sequence = required_u64(&path, biz, index, "BizIndex", source_row)?;
            validate_sequence(
                &mut last_sequences,
                Market::Sse,
                channel,
                sequence,
                "BizIndex",
            )?;
            let symbol = required_str(&path, symbols, index, "SecurityID", source_row)?;
            if !is_supported_symbol(Market::Sse, symbol) {
                stats.excluded_rows += 1;
                stats.excluded_non_stock_rows += 1;
                continue;
            }
            if !request.targets.contains(Market::Sse, symbol) {
                stats.excluded_rows += 1;
                stats.excluded_unselected_stock_rows += 1;
                continue;
            }
            let quote_time_ns =
                parse_timestamp_field(request, &path, quote_times, index, source_row, "TickTime")?;
            if quote_time_exclusive.is_some_and(|cutoff| quote_time_ns >= cutoff) {
                stats.excluded_after_cutoff_rows += 1;
                continue;
            }
            register_symbol_channel(stats, symbol, channel)?;
            let kind_text = required_str(&path, kinds, index, "Type", source_row)?;
            let kind = match kind_text {
                "A" => SseKind::Add,
                "D" => SseKind::Delete,
                "T" => SseKind::Trade,
                "S" => SseKind::Status,
                other => {
                    return Err(invalid(
                        &path,
                        source_row,
                        "Type",
                        format!("unsupported value {other:?}"),
                    ));
                }
            };
            let flag_text = required_str(&path, flags, index, "TickBSFlag", source_row)?;
            let flag = match flag_text {
                "B" => Aggressor::Buy,
                "S" => Aggressor::Sell,
                "N" => Aggressor::Neutral,
                _ => Aggressor::Other,
            };
            let status = match flag_text {
                "CCALL" => 1,
                "CLOSE" => 2,
                "TRADE" => 3,
                _ => 0,
            };
            let local_time_ns =
                parse_timestamp_field(request, &path, local_times, index, source_row, "LocalTime")?;
            let price_units = decimal_units(&path, prices, index, source_row, "Price", 3)?;
            let quantity = nonnegative_i64(&path, quantities, index, source_row, "Qty")?;
            let row = SseRow {
                source_row,
                sequence,
                channel,
                symbol: symbol.to_owned(),
                quote_time_ns,
                local_time_ns,
                kind,
                buy_order_no: required_u64(&path, bids, index, "BuyOrderNO", source_row)?,
                sell_order_no: required_u64(&path, asks, index, "SellOrderNO", source_row)?,
                price_units,
                quantity,
                flag,
                status,
            };
            spool.write_sse(&row)?;
            stats.selected_rows += 1;
        }
    }
    Ok(())
}

fn ingest_sz_orders(
    request: &MarketDayRequest,
    spool: &mut SpoolSet,
    stats: &mut IngestStats,
    quote_time_exclusive: Option<i64>,
) -> Result<(), ProductionError> {
    let path = raw_path(request, "mdl_6_33_0");
    for batch in read_batches(&path, request.batch_size, "SZ", "mdl_6_33_0")? {
        let batch = batch.map_err(|source| ProductionError::Arrow {
            context: "raw Parquet batch",
            source,
        })?;
        validate_sz_order_schema(&path, &batch)?;
        let channels = i32_column(&path, &batch, "ChannelNo")?;
        let sequences = i64_column(&path, &batch, "ApplSeqNum")?;
        let symbols = large_string_column(&path, &batch, "SecurityID")?;
        let prices = decimal_column(&path, &batch, "Price")?;
        let quantities = i64_column(&path, &batch, "OrderQty")?;
        let sides = i32_column(&path, &batch, "Side")?;
        let quote_times = large_string_column(&path, &batch, "TransactTime")?;
        let kinds = i32_column(&path, &batch, "OrdType")?;
        let local_times = large_string_column(&path, &batch, "LocalTime")?;
        let source_rows = u64_column(&path, &batch, "source_row_no")?;
        for index in 0..batch.num_rows() {
            stats.input_rows += 1;
            let source_row = required_u64(&path, source_rows, index, "source_row_no", 0)?;
            let channel = positive_i32_u32(&path, channels, index, source_row, "ChannelNo")?;
            let sequence = positive_i64_u64(&path, sequences, index, source_row, "ApplSeqNum")?;
            spool.observe_sz_sequence(channel, sequence, source_row, false)?;
            let symbol = required_str(&path, symbols, index, "SecurityID", source_row)?;
            if !is_supported_symbol(Market::Szse, symbol) {
                stats.excluded_rows += 1;
                stats.excluded_non_stock_rows += 1;
                continue;
            }
            if !request.targets.contains(Market::Szse, symbol) {
                stats.excluded_rows += 1;
                stats.excluded_unselected_stock_rows += 1;
                continue;
            }
            let quote_time_ns = parse_timestamp_field(
                request,
                &path,
                quote_times,
                index,
                source_row,
                "TransactTime",
            )?;
            if quote_time_exclusive.is_some_and(|cutoff| quote_time_ns >= cutoff) {
                stats.excluded_after_cutoff_rows += 1;
                continue;
            }
            register_symbol_channel(stats, symbol, channel)?;
            let side = match required_i32(&path, sides, index, "Side", source_row)? {
                49 => SzSide::Buy,
                50 => SzSide::Sell,
                value => {
                    return Err(invalid(
                        &path,
                        source_row,
                        "Side",
                        format!("unsupported stock side: {value}"),
                    ));
                }
            };
            let kind = match required_i32(&path, kinds, index, "OrdType", source_row)? {
                49 => SzOrderKind::Market,
                50 => SzOrderKind::Limit,
                85 => SzOrderKind::SameSideBest,
                value => {
                    return Err(invalid(
                        &path,
                        source_row,
                        "OrdType",
                        format!("unsupported value: {value}"),
                    ));
                }
            };
            spool.write_sz_order(&SzOrderRow {
                source_row,
                sequence,
                channel,
                symbol: symbol.to_owned(),
                quote_time_ns,
                local_time_ns: parse_timestamp_field(
                    request,
                    &path,
                    local_times,
                    index,
                    source_row,
                    "LocalTime",
                )?,
                price_units: decimal_units(&path, prices, index, source_row, "Price", 4)?,
                quantity: positive_i64_u64(&path, quantities, index, source_row, "OrderQty")?,
                side,
                kind,
            })?;
            stats.selected_rows += 1;
        }
    }
    Ok(())
}

fn ingest_sz_executions(
    request: &MarketDayRequest,
    spool: &mut SpoolSet,
    stats: &mut IngestStats,
    quote_time_exclusive: Option<i64>,
) -> Result<(), ProductionError> {
    let path = raw_path(request, "mdl_6_36_0");
    for batch in read_batches(&path, request.batch_size, "SZ", "mdl_6_36_0")? {
        let batch = batch.map_err(|source| ProductionError::Arrow {
            context: "raw Parquet batch",
            source,
        })?;
        validate_sz_execution_schema(&path, &batch)?;
        let channels = i32_column(&path, &batch, "ChannelNo")?;
        let sequences = i64_column(&path, &batch, "ApplSeqNum")?;
        let bids = i64_column(&path, &batch, "BidApplSeqNum")?;
        let asks = i64_column(&path, &batch, "OfferApplSeqNum")?;
        let symbols = large_string_column(&path, &batch, "SecurityID")?;
        let prices = decimal_column(&path, &batch, "LastPx")?;
        let quantities = i64_column(&path, &batch, "LastQty")?;
        let kinds = i32_column(&path, &batch, "ExecType")?;
        let quote_times = large_string_column(&path, &batch, "TransactTime")?;
        let local_times = large_string_column(&path, &batch, "LocalTime")?;
        let source_rows = u64_column(&path, &batch, "source_row_no")?;
        for index in 0..batch.num_rows() {
            stats.input_rows += 1;
            let source_row = required_u64(&path, source_rows, index, "source_row_no", 0)?;
            let channel = positive_i32_u32(&path, channels, index, source_row, "ChannelNo")?;
            let sequence = positive_i64_u64(&path, sequences, index, source_row, "ApplSeqNum")?;
            spool.observe_sz_sequence(channel, sequence, source_row, true)?;
            let symbol = required_str(&path, symbols, index, "SecurityID", source_row)?;
            if !is_supported_symbol(Market::Szse, symbol) {
                stats.excluded_rows += 1;
                stats.excluded_non_stock_rows += 1;
                continue;
            }
            if !request.targets.contains(Market::Szse, symbol) {
                stats.excluded_rows += 1;
                stats.excluded_unselected_stock_rows += 1;
                continue;
            }
            let quote_time_ns = parse_timestamp_field(
                request,
                &path,
                quote_times,
                index,
                source_row,
                "TransactTime",
            )?;
            if quote_time_exclusive.is_some_and(|cutoff| quote_time_ns >= cutoff) {
                stats.excluded_after_cutoff_rows += 1;
                continue;
            }
            register_symbol_channel(stats, symbol, channel)?;
            let kind = match required_i32(&path, kinds, index, "ExecType", source_row)? {
                70 => SzExecutionKind::Trade,
                52 => SzExecutionKind::Cancel,
                value => {
                    return Err(invalid(
                        &path,
                        source_row,
                        "ExecType",
                        format!("unsupported value: {value}"),
                    ));
                }
            };
            spool.write_sz_execution(&SzExecutionRow {
                source_row,
                sequence,
                channel,
                symbol: symbol.to_owned(),
                quote_time_ns,
                local_time_ns: parse_timestamp_field(
                    request,
                    &path,
                    local_times,
                    index,
                    source_row,
                    "LocalTime",
                )?,
                bid_order_no: nonnegative_i64(&path, bids, index, source_row, "BidApplSeqNum")?,
                ask_order_no: nonnegative_i64(&path, asks, index, source_row, "OfferApplSeqNum")?,
                price_units: decimal_units(&path, prices, index, source_row, "LastPx", 4)?,
                quantity: positive_i64_u64(&path, quantities, index, source_row, "LastQty")?,
                kind,
            })?;
            stats.selected_rows += 1;
        }
    }
    Ok(())
}

type BatchReader = parquet::arrow::arrow_reader::ParquetRecordBatchReader;

fn read_batches(
    path: &Path,
    batch_size: usize,
    market: &str,
    feed: &str,
) -> Result<BatchReader, ProductionError> {
    if !path.is_file() {
        return Err(ProductionError::MissingInput(path.to_path_buf()));
    }
    let file = File::open(path).map_err(|error| ProductionError::io(path, error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| ProductionError::parquet(path, error))?;
    validate_footer(path, &builder, market, feed)?;
    let indices = raw_columns(feed)
        .iter()
        .map(|name| {
            builder
                .schema()
                .index_of(name)
                .map_err(|_| ProductionError::Schema {
                    path: path.to_path_buf(),
                    detail: format!("missing field {name}"),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let projection = ProjectionMask::roots(builder.parquet_schema(), indices);
    builder
        .with_batch_size(batch_size)
        .with_projection(projection)
        .build()
        .map_err(|error| ProductionError::parquet(path, error))
}

fn raw_columns(feed: &str) -> &'static [&'static str] {
    match feed {
        "mdl_6_28_0" => &[
            "SecurityID",
            "UpdateTime",
            "TradingPhaseCode",
            "source_row_no",
        ],
        "mdl_4_24_0" => &[
            "BizIndex",
            "Channel",
            "SecurityID",
            "TickTime",
            "Type",
            "BuyOrderNO",
            "SellOrderNO",
            "Price",
            "Qty",
            "TickBSFlag",
            "LocalTime",
            "source_row_no",
        ],
        "mdl_6_33_0" => &[
            "ChannelNo",
            "ApplSeqNum",
            "SecurityID",
            "Price",
            "OrderQty",
            "Side",
            "TransactTime",
            "OrdType",
            "LocalTime",
            "source_row_no",
        ],
        "mdl_6_36_0" => &[
            "ChannelNo",
            "ApplSeqNum",
            "BidApplSeqNum",
            "OfferApplSeqNum",
            "SecurityID",
            "LastPx",
            "LastQty",
            "ExecType",
            "TransactTime",
            "LocalTime",
            "source_row_no",
        ],
        _ => &[],
    }
}

fn validate_footer(
    path: &Path,
    builder: &ParquetRecordBatchReaderBuilder<File>,
    market: &str,
    feed: &str,
) -> Result<(), ProductionError> {
    let metadata = builder.metadata().file_metadata().key_value_metadata();
    let mut values = HashMap::new();
    if let Some(entries) = metadata {
        for entry in entries {
            if let Some(value) = &entry.value {
                values.insert(entry.key.as_str(), value.as_str());
            }
        }
    }
    for (key, expected) in [
        ("clara.raw.market", market),
        ("clara.raw.feed", feed),
        ("clara.raw.document_version", "4.1"),
        ("clara.raw.format_version", "2"),
    ] {
        if values.get(key).copied() != Some(expected) {
            return Err(ProductionError::Schema {
                path: path.to_path_buf(),
                detail: format!("footer {key} must equal {expected:?}"),
            });
        }
    }
    for key in ["clara.raw.schema_hash", "clara.raw.schema_id"] {
        if values.get(key).is_none_or(|value| value.is_empty()) {
            return Err(ProductionError::Schema {
                path: path.to_path_buf(),
                detail: format!("footer {key} is missing"),
            });
        }
    }
    Ok(())
}

fn raw_path(request: &MarketDayRequest, feed: &str) -> PathBuf {
    request
        .raw_root
        .join(format!("date={}", request.trading_day.as_yyyymmdd()))
        .join(feed)
        .join("part-0.parquet")
}

fn validate_sse_schema(path: &Path, batch: &RecordBatch) -> Result<(), ProductionError> {
    validate_types(
        path,
        batch,
        &[
            ("BizIndex", DataType::UInt64),
            ("Channel", DataType::UInt64),
            ("SecurityID", DataType::LargeUtf8),
            ("TickTime", DataType::LargeUtf8),
            ("Type", DataType::LargeUtf8),
            ("BuyOrderNO", DataType::UInt64),
            ("SellOrderNO", DataType::UInt64),
            ("Price", DataType::Decimal128(38, 3)),
            ("Qty", DataType::Int64),
            ("TickBSFlag", DataType::LargeUtf8),
            ("LocalTime", DataType::LargeUtf8),
            ("source_row_no", DataType::UInt64),
        ],
    )
}

fn validate_sz_order_schema(path: &Path, batch: &RecordBatch) -> Result<(), ProductionError> {
    validate_types(
        path,
        batch,
        &[
            ("ChannelNo", DataType::Int32),
            ("ApplSeqNum", DataType::Int64),
            ("SecurityID", DataType::LargeUtf8),
            ("Price", DataType::Decimal128(38, 4)),
            ("OrderQty", DataType::Int64),
            ("Side", DataType::Int32),
            ("TransactTime", DataType::LargeUtf8),
            ("OrdType", DataType::Int32),
            ("LocalTime", DataType::LargeUtf8),
            ("source_row_no", DataType::UInt64),
        ],
    )
}

fn validate_sz_execution_schema(path: &Path, batch: &RecordBatch) -> Result<(), ProductionError> {
    validate_types(
        path,
        batch,
        &[
            ("ChannelNo", DataType::Int32),
            ("ApplSeqNum", DataType::Int64),
            ("BidApplSeqNum", DataType::Int64),
            ("OfferApplSeqNum", DataType::Int64),
            ("SecurityID", DataType::LargeUtf8),
            ("LastPx", DataType::Decimal128(38, 4)),
            ("LastQty", DataType::Int64),
            ("ExecType", DataType::Int32),
            ("TransactTime", DataType::LargeUtf8),
            ("LocalTime", DataType::LargeUtf8),
            ("source_row_no", DataType::UInt64),
        ],
    )
}

fn validate_types(
    path: &Path,
    batch: &RecordBatch,
    fields: &[(&str, DataType)],
) -> Result<(), ProductionError> {
    for (name, expected) in fields {
        let schema = batch.schema();
        let field = schema
            .field_with_name(name)
            .map_err(|_| ProductionError::Schema {
                path: path.to_path_buf(),
                detail: format!("missing field {name}"),
            })?;
        if field.data_type() != expected {
            return Err(ProductionError::Schema {
                path: path.to_path_buf(),
                detail: format!(
                    "field {name} has type {:?}, expected {expected:?}",
                    field.data_type()
                ),
            });
        }
    }
    Ok(())
}

fn column_index(path: &Path, batch: &RecordBatch, name: &str) -> Result<usize, ProductionError> {
    batch
        .schema()
        .index_of(name)
        .map_err(|_| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("missing field {name}"),
        })
}

macro_rules! column {
    ($function:ident, $type:ty) => {
        fn $function<'a>(
            path: &Path,
            batch: &'a RecordBatch,
            name: &str,
        ) -> Result<&'a $type, ProductionError> {
            let index = column_index(path, batch, name)?;
            batch
                .column(index)
                .as_any()
                .downcast_ref::<$type>()
                .ok_or_else(|| ProductionError::Schema {
                    path: path.to_path_buf(),
                    detail: format!("field {name} has an unexpected Arrow array type"),
                })
        }
    };
}

column!(u64_column, UInt64Array);
column!(i64_column, Int64Array);
column!(i32_column, Int32Array);
column!(decimal_column, Decimal128Array);
column!(large_string_column, LargeStringArray);

fn required_str<'a>(
    path: &Path,
    array: &'a LargeStringArray,
    index: usize,
    field: &'static str,
    source_row: u64,
) -> Result<&'a str, ProductionError> {
    if array.is_null(index) || array.value(index).is_empty() {
        return Err(invalid(path, source_row, field, "is null or empty"));
    }
    Ok(array.value(index).trim())
}

fn required_u64(
    path: &Path,
    array: &UInt64Array,
    index: usize,
    field: &'static str,
    source_row: u64,
) -> Result<u64, ProductionError> {
    if array.is_null(index) {
        return Err(invalid(path, source_row, field, "is null"));
    }
    Ok(array.value(index))
}

fn required_i32(
    path: &Path,
    array: &Int32Array,
    index: usize,
    field: &'static str,
    source_row: u64,
) -> Result<i32, ProductionError> {
    if array.is_null(index) {
        return Err(invalid(path, source_row, field, "is null"));
    }
    Ok(array.value(index))
}

fn nonnegative_i64(
    path: &Path,
    array: &Int64Array,
    index: usize,
    source_row: u64,
    field: &'static str,
) -> Result<u64, ProductionError> {
    if array.is_null(index) {
        return Err(invalid(path, source_row, field, "is null"));
    }
    u64::try_from(array.value(index)).map_err(|_| {
        invalid(
            path,
            source_row,
            field,
            format!("must be nonnegative: {}", array.value(index)),
        )
    })
}

fn positive_i64_u64(
    path: &Path,
    array: &Int64Array,
    index: usize,
    source_row: u64,
    field: &'static str,
) -> Result<u64, ProductionError> {
    let value = nonnegative_i64(path, array, index, source_row, field)?;
    if value == 0 {
        return Err(invalid(path, source_row, field, "must be positive"));
    }
    Ok(value)
}

fn positive_i32_u32(
    path: &Path,
    array: &Int32Array,
    index: usize,
    source_row: u64,
    field: &'static str,
) -> Result<u32, ProductionError> {
    let value = required_i32(path, array, index, field, source_row)?;
    u32::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            invalid(
                path,
                source_row,
                field,
                format!("must be positive: {value}"),
            )
        })
}

fn decimal_units(
    path: &Path,
    array: &Decimal128Array,
    index: usize,
    source_row: u64,
    field: &'static str,
    source_scale: i8,
) -> Result<i64, ProductionError> {
    if array.is_null(index) {
        return Err(invalid(path, source_row, field, "is null"));
    }
    let scale = match array.data_type() {
        DataType::Decimal128(_, scale) => *scale,
        _ => return Err(invalid(path, source_row, field, "is not Decimal128")),
    };
    if scale != source_scale {
        return Err(invalid(
            path,
            source_row,
            field,
            format!("scale {scale} does not match {source_scale}"),
        ));
    }
    let value = array.value(index);
    let scaled = match source_scale {
        3 => value.checked_mul(10),
        4 => Some(value),
        _ => None,
    }
    .ok_or(ProductionError::Arithmetic("price scale conversion"))?;
    i64::try_from(scaled).map_err(|_| invalid(path, source_row, field, "price does not fit i64"))
}

fn parse_timestamp_field(
    request: &MarketDayRequest,
    path: &Path,
    array: &LargeStringArray,
    index: usize,
    source_row: u64,
    field: &'static str,
) -> Result<i64, ProductionError> {
    let value = required_str(path, array, index, field, source_row)?;
    parse_market_timestamp(request.trading_day, value)
        .map_err(|error| invalid(path, source_row, field, error.to_string()))
}

fn validate_sequence(
    sequences: &mut HashMap<u32, u64>,
    market: Market,
    channel: u32,
    current: u64,
    sequence_name: &'static str,
) -> Result<(), ProductionError> {
    if current == 0 {
        return Err(ProductionError::NonIncreasingSequence {
            market,
            channel,
            sequence_name,
            previous: 0,
            current,
        });
    }
    if let Some(previous) = sequences.insert(channel, current) {
        if current <= previous {
            return Err(ProductionError::NonIncreasingSequence {
                market,
                channel,
                sequence_name,
                previous,
                current,
            });
        }
    }
    Ok(())
}

fn register_symbol_channel(
    stats: &mut IngestStats,
    symbol: &str,
    channel: u32,
) -> Result<(), ProductionError> {
    if let Some(first_channel) = stats.symbol_channels.insert(symbol.to_owned(), channel) {
        if first_channel != channel {
            return Err(ProductionError::SymbolChannelConflict {
                symbol: crate::Symbol::from(symbol),
                first_channel,
                second_channel: channel,
            });
        }
    }
    Ok(())
}

fn invalid(
    path: &Path,
    source_row: u64,
    field: &'static str,
    detail: impl Into<String>,
) -> ProductionError {
    ProductionError::InvalidField {
        path: path.to_path_buf(),
        source_row,
        field,
        detail: detail.into(),
    }
}

#[cfg(test)]
mod phase_tests {
    use super::SzPhaseHistory;

    #[test]
    fn normal_open_and_suspended_frames_do_not_create_resumptions() {
        let mut state = SzPhaseHistory::default();
        for (index, phase) in ["O0", "B0", "T0", "T0", "C0", "E0", "B1", "T1", "E1"]
            .into_iter()
            .enumerate()
        {
            assert!(!state.observe(index as i64, phase));
        }
    }

    #[test]
    fn only_first_normal_trading_frame_after_each_halt_creates_checkpoint() {
        let mut state = SzPhaseHistory::default();
        for (index, (phase, resumed)) in [
            ("T0", false),
            ("H0", false),
            ("H0", false),
            ("B0", false),
            ("T0", true),
            ("T0", false),
            ("H0", false),
            ("T0", true),
            ("T0", false),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(state.observe(index as i64, phase), resumed);
        }
    }
}
