#![allow(clippy::expect_used)]

use std::fs::{self, File};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, Decimal128Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use qtp_core::{
    Market, MarketDayRequest, ProductionError, SnapshotSchedule, Symbol, TargetUniverse,
    TradingDay, parse_market_timestamp, replay_market_day,
};
use tempfile::TempDir;

fn day() -> TradingDay {
    TradingDay::from_yyyymmdd(20_260_828).expect("valid test day")
}

fn write_sse_fixture(root: &TempDir, local_times: Vec<Option<&str>>) {
    let directory = root.path().join("raw/date=20260828/mdl_4_24_0");
    fs::create_dir_all(&directory).expect("fixture directory");
    let path = directory.join("part-0.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("BizIndex", DataType::UInt64, false),
        Field::new("Channel", DataType::UInt64, false),
        Field::new("SecurityID", DataType::LargeUtf8, false),
        Field::new("TickTime", DataType::LargeUtf8, false),
        Field::new("Type", DataType::LargeUtf8, false),
        Field::new("BuyOrderNO", DataType::UInt64, false),
        Field::new("SellOrderNO", DataType::UInt64, false),
        Field::new("Price", DataType::Decimal128(38, 3), false),
        Field::new("Qty", DataType::Int64, false),
        Field::new("TickBSFlag", DataType::LargeUtf8, false),
        Field::new("LocalTime", DataType::LargeUtf8, true),
        Field::new("source_row_no", DataType::UInt64, false),
    ]));
    let prices = Decimal128Array::from_iter_values([10_000, 0, 11_000, 10_500, 0, 0])
        .with_precision_and_scale(38, 3)
        .expect("decimal metadata");
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values([1, 2, 3, 4, 5, 6])),
        Arc::new(UInt64Array::from_iter_values([1, 1, 1, 1, 1, 1])),
        Arc::new(LargeStringArray::from_iter_values([
            "600000", "510300", "600000", "600000", "600000", "600000",
        ])),
        Arc::new(LargeStringArray::from_iter_values([
            "09:20:00.000",
            "09:30:00.000",
            "10:15:00.000",
            "14:00:00.000",
            "14:30:00.000",
            "15:00:01.000",
        ])),
        Arc::new(LargeStringArray::from_iter_values([
            "A", "A", "A", "T", "D", "S",
        ])),
        Arc::new(UInt64Array::from_iter_values([1, 2, 0, 1, 1, 0])),
        Arc::new(UInt64Array::from_iter_values([0, 0, 3, 3, 0, 0])),
        Arc::new(prices),
        Arc::new(Int64Array::from_iter_values([100, 1, 200, 40, 60, 0])),
        Arc::new(LargeStringArray::from_iter_values([
            "B", "B", "S", "N", "B", "CLOSE",
        ])),
        Arc::new(LargeStringArray::from(local_times)),
        Arc::new(UInt64Array::from_iter_values([1, 2, 3, 4, 5, 6])),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("fixture batch");
    let metadata = [
        ("clara.raw.market", "SH"),
        ("clara.raw.feed", "mdl_4_24_0"),
        ("clara.raw.document_version", "4.1"),
        ("clara.raw.format_version", "2"),
        ("clara.raw.schema_hash", "test-hash"),
        ("clara.raw.schema_id", "test-schema"),
    ]
    .into_iter()
    .map(|(key, value)| KeyValue::new(key.to_owned(), value.to_owned()))
    .collect();
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(metadata))
        .build();
    let file = File::create(path).expect("fixture file");
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties)).expect("fixture writer");
    writer.write(&batch).expect("write fixture");
    writer.close().expect("close fixture");
}

fn request(root: &TempDir, snapshots: Option<SnapshotSchedule>) -> MarketDayRequest {
    MarketDayRequest {
        raw_root: root.path().join("raw"),
        output_root: root.path().join("output"),
        temp_root: root.path().join("temp"),
        trading_day: day(),
        market: Market::Sse,
        targets: TargetUniverse::Symbols(vec![Symbol::from("600000")]),
        snapshots,
        batch_size: 2,
    }
}

fn write_batch_with_metadata(
    path: &std::path::Path,
    schema: Arc<Schema>,
    columns: Vec<ArrayRef>,
    feed: &str,
) {
    fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture directory");
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("fixture batch");
    let metadata = [
        ("clara.raw.market", "SZ"),
        ("clara.raw.feed", feed),
        ("clara.raw.document_version", "4.1"),
        ("clara.raw.format_version", "2"),
        ("clara.raw.schema_hash", "test-hash"),
        ("clara.raw.schema_id", "test-schema"),
    ]
    .into_iter()
    .map(|(key, value)| KeyValue::new(key.to_owned(), value.to_owned()))
    .collect();
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(metadata))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(path).expect("fixture file"),
        schema,
        Some(properties),
    )
    .expect("fixture writer");
    writer.write(&batch).expect("write fixture");
    writer.close().expect("close fixture");
}

fn write_sz_fixture(root: &TempDir, execution_sequences: [i64; 2], cancel_quantity: i64) {
    let base = root.path().join("raw/date=20260828");
    let order_schema = Arc::new(Schema::new(vec![
        Field::new("ChannelNo", DataType::Int32, false),
        Field::new("ApplSeqNum", DataType::Int64, false),
        Field::new("SecurityID", DataType::LargeUtf8, false),
        Field::new("Price", DataType::Decimal128(38, 4), false),
        Field::new("OrderQty", DataType::Int64, false),
        Field::new("Side", DataType::Int32, false),
        Field::new("TransactTime", DataType::LargeUtf8, false),
        Field::new("OrdType", DataType::Int32, false),
        Field::new("LocalTime", DataType::LargeUtf8, false),
        Field::new("source_row_no", DataType::UInt64, false),
    ]));
    let order_prices = Decimal128Array::from_iter_values([100_000, 110_000])
        .with_precision_and_scale(38, 4)
        .expect("order prices");
    write_batch_with_metadata(
        &base.join("mdl_6_33_0/part-0.parquet"),
        order_schema,
        vec![
            Arc::new(Int32Array::from_iter_values([1, 1])),
            Arc::new(Int64Array::from_iter_values([1, 2])),
            Arc::new(LargeStringArray::from_iter_values(["000001", "000001"])),
            Arc::new(order_prices),
            Arc::new(Int64Array::from_iter_values([100, 200])),
            Arc::new(Int32Array::from_iter_values([49, 50])),
            Arc::new(LargeStringArray::from_iter_values([
                "09:20:00.000",
                "09:21:00.000",
            ])),
            Arc::new(Int32Array::from_iter_values([50, 50])),
            Arc::new(LargeStringArray::from_iter_values([
                "09:20:00.010",
                "09:21:00.010",
            ])),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
        ],
        "mdl_6_33_0",
    );

    let execution_schema = Arc::new(Schema::new(vec![
        Field::new("ChannelNo", DataType::Int32, false),
        Field::new("ApplSeqNum", DataType::Int64, false),
        Field::new("BidApplSeqNum", DataType::Int64, false),
        Field::new("OfferApplSeqNum", DataType::Int64, false),
        Field::new("SecurityID", DataType::LargeUtf8, false),
        Field::new("LastPx", DataType::Decimal128(38, 4), false),
        Field::new("LastQty", DataType::Int64, false),
        Field::new("ExecType", DataType::Int32, false),
        Field::new("TransactTime", DataType::LargeUtf8, false),
        Field::new("LocalTime", DataType::LargeUtf8, false),
        Field::new("source_row_no", DataType::UInt64, false),
    ]));
    let execution_prices = Decimal128Array::from_iter_values([105_000, 0])
        .with_precision_and_scale(38, 4)
        .expect("execution prices");
    write_batch_with_metadata(
        &base.join("mdl_6_36_0/part-0.parquet"),
        execution_schema,
        vec![
            Arc::new(Int32Array::from_iter_values([1, 1])),
            Arc::new(Int64Array::from_iter_values(execution_sequences)),
            Arc::new(Int64Array::from_iter_values([1, 1])),
            Arc::new(Int64Array::from_iter_values([2, 0])),
            Arc::new(LargeStringArray::from_iter_values(["000001", "000001"])),
            Arc::new(execution_prices),
            Arc::new(Int64Array::from_iter_values([40, cancel_quantity])),
            Arc::new(Int32Array::from_iter_values([70, 52])),
            Arc::new(LargeStringArray::from_iter_values([
                "09:30:00.000",
                "09:31:00.000",
            ])),
            Arc::new(LargeStringArray::from_iter_values([
                "09:30:00.010",
                "09:31:00.010",
            ])),
            Arc::new(UInt64Array::from_iter_values([1, 2])),
        ],
        "mdl_6_36_0",
    );
}

#[test]
fn streams_sse_parquet_and_keeps_equal_time_event_out_of_snapshot() {
    let root = TempDir::new().expect("temp directory");
    fs::create_dir_all(root.path().join("temp")).expect("temp root");
    write_sse_fixture(
        &root,
        vec![
            Some("09:20:00.010"),
            Some("09:30:00.010"),
            Some("10:15:00.010"),
            Some("14:00:00.010"),
            Some("14:30:00.010"),
            Some("15:00:01.010"),
        ],
    );
    let schedule =
        SnapshotSchedule::new(Duration::from_secs(3_600), 10).expect("snapshot schedule");
    let report = replay_market_day(&request(&root, Some(schedule))).expect("replay succeeds");
    assert_eq!(report.input_rows, 6);
    assert_eq!(report.selected_rows, 5);
    assert_eq!(report.excluded_rows, 1);
    assert_eq!(report.excluded_non_stock_rows, 1);
    assert_eq!(report.excluded_unselected_stock_rows, 0);
    assert_eq!(report.applied_events, 4);
    assert_eq!(report.status_events, 1);
    assert_eq!(report.scheduled_snapshots, 4);
    assert_eq!(report.market_close_snapshots, 1);

    let output = root
        .path()
        .join("output/date=20260828/market=SH/channel=1/part-0.parquet");
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(output).expect("open snapshot output"))
            .expect("snapshot reader")
            .build()
            .expect("snapshot batches");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("read snapshots");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
    let first = &batches[0];
    let kinds = first
        .column(
            first
                .schema()
                .index_of("snapshot_kind")
                .expect("kind index"),
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("kind strings");
    let boundaries = first
        .column(
            first
                .schema()
                .index_of("boundary_time")
                .expect("time index"),
        )
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("boundary timestamps");
    let asks = first
        .column(first.schema().index_of("ask_price_1").expect("ask index"))
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("ask prices");
    assert_eq!(kinds.value(0), "scheduled");
    assert_eq!(
        boundaries.value(0),
        parse_market_timestamp(day(), "10:15:00.000").expect("boundary")
    );
    assert!(asks.is_null(0), "event at T must not enter Snapshot(T)");
    assert_eq!(asks.value(1), 110_000);
    assert_eq!(kinds.value(3), "scheduled");
    assert_eq!(
        boundaries.value(3),
        parse_market_timestamp(day(), "15:00:00.000").expect("scheduled close boundary")
    );
    assert_eq!(kinds.value(4), "market_close");
    assert_eq!(
        boundaries.value(4),
        parse_market_timestamp(day(), "15:00:01.000").expect("market close checkpoint")
    );
}

#[test]
fn rejects_missing_required_local_time_before_replay() {
    let root = TempDir::new().expect("temp directory");
    fs::create_dir_all(root.path().join("temp")).expect("temp root");
    write_sse_fixture(
        &root,
        vec![
            None,
            Some("09:30:00.010"),
            Some("10:15:00.010"),
            Some("14:00:00.010"),
            Some("14:30:00.010"),
            Some("15:00:01.010"),
        ],
    );
    let error = replay_market_day(&request(&root, None)).expect_err("missing LocalTime must fail");
    assert!(error.to_string().contains("LocalTime"));
    assert!(matches!(
        error,
        ProductionError::ReplayFailed { spool_path, .. } if spool_path.is_dir()
    ));
}

#[test]
fn merges_shenzhen_order_and_execution_streams_by_appl_seq_num() {
    let root = TempDir::new().expect("temp directory");
    fs::create_dir_all(root.path().join("temp")).expect("temp root");
    write_sz_fixture(&root, [3, 4], 60);
    let request = MarketDayRequest {
        raw_root: root.path().join("raw"),
        output_root: root.path().join("output"),
        temp_root: root.path().join("temp"),
        trading_day: day(),
        market: Market::Szse,
        targets: TargetUniverse::Symbols(vec![Symbol::from("000001")]),
        snapshots: Some(
            SnapshotSchedule::new(Duration::from_secs(3_600), 10).expect("snapshot schedule"),
        ),
        batch_size: 1,
    };
    let report = replay_market_day(&request).expect("Shenzhen replay succeeds");
    assert_eq!(report.input_rows, 4);
    assert_eq!(report.applied_events, 4);
    assert_eq!(report.market_close_snapshots, 1);

    let output = root
        .path()
        .join("output/date=20260828/market=SZ/channel=1/part-0.parquet");
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(output).expect("open snapshot output"))
            .expect("snapshot reader")
            .build()
            .expect("snapshot batches");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("read snapshots");
    let batch = &batches[0];
    let kinds = batch
        .column(
            batch
                .schema()
                .index_of("snapshot_kind")
                .expect("kind index"),
        )
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("kind strings");
    let close_row = (0..batch.num_rows())
        .find(|row| kinds.value(*row) == "market_close")
        .expect("market close row");
    let bid_quantity = batch
        .column(
            batch
                .schema()
                .index_of("bid_quantity_1")
                .expect("bid quantity index"),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("bid quantities");
    let ask_quantity = batch
        .column(
            batch
                .schema()
                .index_of("ask_quantity_1")
                .expect("ask quantity index"),
        )
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("ask quantities");
    assert!(bid_quantity.is_null(close_row));
    assert_eq!(ask_quantity.value(close_row), 160);
}

fn sz_request(root: &TempDir) -> MarketDayRequest {
    MarketDayRequest {
        raw_root: root.path().join("raw"),
        output_root: root.path().join("output"),
        temp_root: root.path().join("temp"),
        trading_day: day(),
        market: Market::Szse,
        targets: TargetUniverse::Symbols(vec![Symbol::from("000001")]),
        snapshots: None,
        batch_size: 1,
    }
}

#[test]
fn rejects_cross_stream_shenzhen_sequence_ambiguity() {
    let root = TempDir::new().expect("temp directory");
    write_sz_fixture(&root, [2, 4], 60);
    let error = replay_market_day(&sz_request(&root)).expect_err("equal sequence must fail");
    assert!(
        error
            .to_string()
            .contains("ambiguous Shenzhen ApplSeqNum 2")
    );
}

#[test]
fn rejects_descending_shenzhen_source_sequence() {
    let root = TempDir::new().expect("temp directory");
    write_sz_fixture(&root, [4, 3], 60);
    let error = replay_market_day(&sz_request(&root)).expect_err("descending sequence must fail");
    assert!(error.to_string().contains("non-increasing ApplSeqNum"));
}

#[test]
fn rejects_shenzhen_cancellation_quantity_mismatch() {
    let root = TempDir::new().expect("temp directory");
    write_sz_fixture(&root, [3, 4], 59);
    let error = replay_market_day(&sz_request(&root)).expect_err("bad cancellation must fail");
    assert!(error.to_string().contains("does not equal remaining 60"));
}
