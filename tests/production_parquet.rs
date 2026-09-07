#![allow(clippy::expect_used)]

use std::fs::{self, File};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, Decimal128Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use qtp_core::{
    Market, MarketDayRequest, ProductionError, SnapshotSchedule, Symbol, TargetUniverse,
    TradingDay, ValidationAnchor, ValidationConfig, ValidationOutcome, parse_market_timestamp,
    replay_market_day, validate_market_day, validate_pre_open_market_day,
};
use tempfile::TempDir;

fn day() -> TradingDay {
    TradingDay::from_yyyymmdd(20_260_828).expect("valid test day")
}

fn write_sse_fixture(root: &TempDir, local_times: Vec<Option<&str>>) {
    write_sse_symbol_fixture(root, local_times, "600000", "510300");
}

fn write_sse_symbol_fixture(
    root: &TempDir,
    local_times: Vec<Option<&str>>,
    symbol: &str,
    excluded_symbol: &str,
) {
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
            symbol,
            excluded_symbol,
            symbol,
            symbol,
            symbol,
            symbol,
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
        sz_market_order_policy: qtp_core::SzMarketOrderPolicy::RequireEvidence,
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
        (
            "clara.raw.market",
            if feed == "MarketData" { "SH" } else { "SZ" },
        ),
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
    assert_eq!(report.excluded_non_stock_rows, 0);
    assert_eq!(report.excluded_unselected_stock_rows, 1);
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
        sz_market_order_policy: qtp_core::SzMarketOrderPolicy::RequireEvidence,
    };
    let report = replay_market_day(&request).expect("Shenzhen replay succeeds");
    assert_eq!(report.input_rows, 4);
    assert!(!report.sz_phase_source_available);
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
        sz_market_order_policy: qtp_core::SzMarketOrderPolicy::RequireEvidence,
    }
}

#[test]
fn e0_range_metadata_and_successful_trade_sequence_survive_source_loading() {
    for case in ["unlimited", "conflict", "null"] {
        let root = TempDir::new().expect("temporary directory");
        write_sz_fixture(&root, [3, 4], 60);
        write_sz_e0_fixture(&root);
        let path = root
            .path()
            .join("raw/date=20260828/mdl_6_28_0/part-0.parquet");
        let batch = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).expect("file"))
            .expect("builder")
            .build()
            .expect("reader")
            .next()
            .expect("batch")
            .expect("batch");
        let mut columns = batch.columns().to_vec();
        let high = batch.schema().index_of("HighLimitPrice").expect("high");
        let low = batch.schema().index_of("LowLimitPrice").expect("low");
        columns[high] = Arc::new(
            Decimal128Array::from(vec![
                Some(999_999_999_999_900_i128),
                match case {
                    "conflict" => Some(12_000_000),
                    "null" => None,
                    _ => Some(999_999_999_999_900),
                },
            ])
            .with_precision_and_scale(38, 6)
            .expect("price"),
        );
        columns[low] = Arc::new(
            Decimal128Array::from(vec![10_000_i128; 2])
                .with_precision_and_scale(38, 6)
                .expect("price"),
        );
        let mut fields = batch.schema().fields().to_vec();
        fields[high] = Arc::new(Field::new(
            "HighLimitPrice",
            DataType::Decimal128(38, 6),
            true,
        ));
        write_batch_with_metadata(&path, Arc::new(Schema::new(fields)), columns, "mdl_6_28_0");
        let config = ValidationConfig {
            request: sz_request(&root),
            continuous_lookback: None,
            continuous_lookahead: None,
            retain_matched_records: true,
            max_detail_records: None,
        };
        let report = validate_market_day(&config).expect("validation");
        let close = report
            .records
            .iter()
            .find(|r| r.anchor == ValidationAnchor::MarketClose)
            .expect("close");
        if case == "unlimited" {
            assert_eq!(close.outcome, ValidationOutcome::Matched);
            let band = close.close_price_band.as_ref().expect("projection");
            assert_eq!(
                band.base_raw_sequence, 3,
                "last event 4 is a cancellation, not the range base"
            );
            assert_eq!(band.base_price_units, 105_000);
            #[cfg(feature = "profiling")]
            {
                let (profiled, _) =
                    qtp_core::profile_validate_market_day(&config).expect("profiling");
                assert_eq!(
                    serde_json::to_value(&report).expect("json"),
                    serde_json::to_value(profiled).expect("json")
                );
            }
        } else {
            assert_eq!(close.outcome, ValidationOutcome::DataError);
            assert!(close.close_price_band.is_none());
        }
        assert_eq!(report.replay.applied_events, 4);
    }
}

#[cfg(feature = "profiling")]
#[test]
fn profiling_preserves_reports_and_partitions_elapsed_time() {
    let root = TempDir::new().expect("temporary directory");
    write_sz_fixture(&root, [3, 4], 60);
    write_sz_e0_fixture(&root);
    let config = ValidationConfig {
        request: sz_request(&root),
        continuous_lookback: None,
        continuous_lookahead: None,
        retain_matched_records: true,
        max_detail_records: None,
    };
    let original = validate_market_day(&config).expect("plain validation");
    let (profiled, timings) =
        qtp_core::profile_validate_market_day(&config).expect("profiled validation");
    assert_eq!(
        serde_json::to_value(original).expect("serialize"),
        serde_json::to_value(profiled).expect("serialize")
    );
    assert!(timings.observation_calls > 0);
    assert!(timings.restore_total_seconds >= 0.0);
    assert!(timings.validation_total_seconds >= 0.0);
    assert!(
        (timings.profiled_total_seconds
            - timings.restore_total_seconds
            - timings.validation_total_seconds
            - timings.unattributed_seconds)
            .abs()
            < 1e-9
    );
    // An invalid cancellation must remain a replay failure, not a successful
    // timing result or a silently shortened day.
    write_sz_fixture(&root, [3, 4], 61);
    assert!(qtp_core::profile_validate_market_day(&config).is_err());
    assert!(validate_market_day(&config).is_err());
}

fn rewrite_sz_columns(root: &TempDir, feed: &str, replacements: Vec<(&str, ArrayRef)>) {
    let path = root
        .path()
        .join(format!("raw/date=20260828/{feed}/part-0.parquet"));
    let batch = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).expect("fixture"))
        .expect("reader")
        .build()
        .expect("batches")
        .next()
        .expect("batch")
        .expect("valid batch");
    let mut columns = batch.columns().to_vec();
    for (name, column) in replacements {
        columns[batch.schema().index_of(name).expect("field")] = column;
    }
    write_batch_with_metadata(&path, batch.schema(), columns, feed);
}

// Ask #1, new bid #2, then bid #6. Responses to #2 are not split by batches.
fn write_sz_pending_fixture(
    root: &TempDir,
    kind: i32,
    ask_side: i32,
    responses: &[(i64, i64, i32, i64, &str)],
) {
    write_sz_fixture(root, [3, 4], 60);
    let base = root.path().join("raw/date=20260828");
    let path = base.join("mdl_6_33_0/part-0.parquet");
    let schema =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&path).expect("valid test fixture"))
            .expect("valid test fixture")
            .schema()
            .clone();
    write_batch_with_metadata(
        &path,
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1; 3])),
            Arc::new(Int64Array::from(vec![1, 2, 6])),
            Arc::new(LargeStringArray::from(vec!["000001"; 3])),
            Arc::new(
                Decimal128Array::from(vec![100_000, 123_456, 100_000])
                    .with_precision_and_scale(38, 4)
                    .expect("valid test fixture"),
            ),
            Arc::new(Int64Array::from(vec![100, 300, 50])),
            Arc::new(Int32Array::from(vec![ask_side, 49, 49])),
            Arc::new(LargeStringArray::from(vec![
                "09:30:00.000",
                "10:00:00.000",
                "10:00:00.001",
            ])),
            Arc::new(Int32Array::from(vec![50, kind, 50])),
            Arc::new(LargeStringArray::from(vec!["10:00:01.999"; 3])),
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
        ],
        "mdl_6_33_0",
    );
    let path = base.join("mdl_6_36_0/part-0.parquet");
    let schema =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&path).expect("valid test fixture"))
            .expect("valid test fixture")
            .schema()
            .clone();
    let n = responses.len();
    write_batch_with_metadata(
        &path,
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1; n])),
            Arc::new(Int64Array::from_iter_values(responses.iter().map(|r| r.0))),
            Arc::new(Int64Array::from(vec![2; n])),
            Arc::new(Int64Array::from_iter_values(
                responses.iter().map(|r| if r.2 == 70 { 1 } else { 0 }),
            )),
            Arc::new(LargeStringArray::from(vec!["000001"; n])),
            Arc::new(
                Decimal128Array::from_iter_values(responses.iter().map(|r| i128::from(r.3)))
                    .with_precision_and_scale(38, 4)
                    .expect("valid test fixture"),
            ),
            Arc::new(Int64Array::from_iter_values(responses.iter().map(|r| r.1))),
            Arc::new(Int32Array::from_iter_values(responses.iter().map(|r| r.2))),
            Arc::new(LargeStringArray::from_iter_values(
                responses.iter().map(|r| r.4),
            )),
            Arc::new(LargeStringArray::from(vec!["10:00:02.999"; n])),
            Arc::new(UInt64Array::from_iter_values((1..=n).map(|i| i as u64))),
        ],
        "mdl_6_36_0",
    );
}

fn pending_output(root: &TempDir) -> RecordBatch {
    ParquetRecordBatchReaderBuilder::try_new(
        File::open(
            root.path()
                .join("output/date=20260828/market=SZ/channel=1/part-0.parquet"),
        )
        .expect("valid test fixture"),
    )
    .expect("valid test fixture")
    .build()
    .expect("valid test fixture")
    .next()
    .expect("valid test fixture")
    .expect("valid test fixture")
}

#[test]
fn pending_ioc_cancels_without_publishing_remainder_and_preserves_left_limit() {
    let root = TempDir::new().expect("valid test fixture");
    write_sz_pending_fixture(
        &root,
        49,
        50,
        &[
            (3, 100, 70, 100_000, "10:00:00.000"),
            (4, 200, 52, 0, "10:00:00.000"),
        ],
    );
    let mut req = sz_request(&root);
    req.snapshots =
        Some(SnapshotSchedule::new(Duration::from_secs(2700), 10).expect("valid test fixture"));
    let report = replay_market_day(&req).expect("valid test fixture");
    assert_eq!(report.sz_pending_groups, 1);
    assert_eq!(report.sz_inferred_market_remainders, 0);
    assert_eq!(report.applied_events, 5);
    let batch = pending_output(&root);
    let bids = batch
        .column_by_name("bid_quantity_1")
        .expect("valid test fixture")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("valid test fixture");
    let asks = batch
        .column_by_name("ask_quantity_1")
        .expect("valid test fixture")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("valid test fixture");
    assert!(bids.is_null(0)); // scheduled 10:00 excludes every group event
    assert_eq!(asks.value(0), 100);
    assert_eq!(bids.value(batch.num_rows() - 1), 50); // only order #6 remains
}

#[test]
fn pending_single_price_does_not_silently_enable_rest_in_strict_mode() {
    let root = TempDir::new().expect("valid test fixture");
    write_sz_pending_fixture(&root, 49, 50, &[(3, 100, 70, 100_000, "10:00:00.000")]);
    let error = replay_market_day(&sz_request(&root))
        .expect_err("must reject invalid fixture")
        .to_string();
    assert!(error.contains("lacks execution qualifier"), "{error}");
}

#[test]
fn practical_market_replay_accepts_gaps_and_cross_millisecond_responses() {
    for raw_price in [0, 123_456] {
        for cancel in [false, true] {
            let root = TempDir::new().expect("fixture");
            let mut responses = vec![(4, 100, 70, 100_000, "10:00:00.001")];
            if cancel {
                responses.push((5, 200, 52, 0, "10:00:00.001"));
            }
            write_sz_pending_fixture(&root, 49, 50, &responses);
            rewrite_sz_columns(
                &root,
                "mdl_6_33_0",
                vec![(
                    "Price",
                    Arc::new(
                        Decimal128Array::from(vec![100_000, raw_price, 100_000])
                            .with_precision_and_scale(38, 4)
                            .expect("decimal"),
                    ),
                )],
            );
            let mut req = sz_request(&root);
            req.sz_market_order_policy = qtp_core::SzMarketOrderPolicy::RestAtLastTradePrice;
            req.snapshots =
                Some(SnapshotSchedule::new(Duration::from_secs(2700), 10).expect("schedule"));
            let report = replay_market_day(&req).expect("practical replay");
            assert_eq!(report.sz_pending_groups, 0);
            assert_eq!(report.applied_events, if cancel { 5 } else { 4 });
            assert_eq!(report.sz_market_order_policy, req.sz_market_order_policy);
            let batch = pending_output(&root);
            let bids = batch
                .column_by_name("bid_quantity_1")
                .expect("quantity")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("u64");
            assert!(bids.is_null(0)); // 10:00 left limit precedes market order.
            assert_eq!(
                bids.value(batch.num_rows() - 1),
                if cancel { 50 } else { 250 }
            );
        }
    }
}

#[test]
fn practical_market_policy_does_not_relax_empty_same_side_reconciliation() {
    let root = TempDir::new().expect("fixture");
    write_sz_pending_fixture(&root, 85, 50, &[]);
    let mut req = sz_request(&root);
    req.sz_market_order_policy = qtp_core::SzMarketOrderPolicy::RestAtLastTradePrice;
    assert!(
        replay_market_day(&req)
            .expect_err("empty own side")
            .to_string()
            .contains("same-side best")
    );
}

#[test]
fn diagnostic_remainder_uses_initial_best_not_protection_price() {
    let root = TempDir::new().expect("valid test fixture");
    write_sz_pending_fixture(&root, 49, 50, &[(3, 100, 70, 100_000, "10:00:00.000")]);
    // An adjacent next order is required; a filtered/raw gap is not a boundary.
    rewrite_sz_columns(
        &root,
        "mdl_6_33_0",
        vec![("ApplSeqNum", Arc::new(Int64Array::from(vec![1, 2, 4])))],
    );
    let mut req = sz_request(&root);
    req.snapshots =
        Some(SnapshotSchedule::new(Duration::from_secs(2700), 10).expect("valid test fixture"));
    req.sz_market_order_policy = qtp_core::SzMarketOrderPolicy::AssumeContiguous;
    let report = replay_market_day(&req).expect("valid test fixture");
    assert_eq!(report.sz_inferred_market_remainders, 1);
    let batch = pending_output(&root);
    let last = batch.num_rows() - 1;
    let bids = batch
        .column_by_name("bid_quantity_1")
        .expect("valid test fixture")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("valid test fixture");
    let prices = batch
        .column_by_name("bid_price_1")
        .expect("valid test fixture")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("valid test fixture");
    assert_eq!(bids.value(last), 250);
    assert_eq!(prices.value(last), 100_000);
}

#[test]
fn empty_same_side_requires_exact_source_cancel_and_never_lingers() {
    for responses in [vec![], vec![(3, 300, 52, 0, "10:00:00.000")]] {
        let root = TempDir::new().expect("valid test fixture");
        write_sz_pending_fixture(&root, 85, 50, &responses);
        let result = replay_market_day(&sz_request(&root));
        if responses.is_empty() {
            assert!(
                result
                    .expect_err("must reject invalid fixture")
                    .to_string()
                    .contains("source cancellation reconciliation")
            );
        } else {
            assert_eq!(
                result
                    .expect("valid test fixture")
                    .sz_empty_same_side_cancellations,
                1
            );
        }
    }
}

#[test]
fn same_side_best_is_fixed_at_entry_and_not_raw_price() {
    let root = TempDir::new().expect("valid test fixture");
    write_sz_pending_fixture(&root, 85, 49, &[]);
    let mut req = sz_request(&root);
    req.snapshots =
        Some(SnapshotSchedule::new(Duration::from_secs(2700), 10).expect("valid test fixture"));
    let report = replay_market_day(&req).expect("valid test fixture");
    assert_eq!(report.sz_pending_groups, 0);
    let batch = pending_output(&root);
    let bids = batch
        .column_by_name("bid_quantity_1")
        .expect("valid test fixture")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("valid test fixture");
    assert_eq!(bids.value(batch.num_rows() - 1), 450);
}

#[test]
fn pending_cross_timestamp_and_sequence_gap_are_explicit_failures() {
    for (sequence, time, reason) in [
        (3, "10:00:00.001", "quote-time boundary"),
        (4, "10:00:00.000", "non-adjacent"),
    ] {
        let root = TempDir::new().expect("valid test fixture");
        write_sz_pending_fixture(&root, 49, 50, &[(sequence, 300, 52, 0, time)]);
        let error = replay_market_day(&sz_request(&root))
            .expect_err("must reject invalid fixture")
            .to_string();
        assert!(error.contains(reason), "{error}");
    }
}

#[test]
fn shenzhen_close_waits_for_both_streams_and_preserves_scheduled_left_limit() {
    // Exercise execution-stream tails, order-stream tails and multiple events
    // at exactly 15:00. LocalTime intentionally remains unrelated to the cutoff.
    for (order_tail, late) in [(false, false), (false, true), (true, true)] {
        let root = TempDir::new().expect("temp directory");
        write_sz_fixture(&root, [3, 4], 60);
        let times = if late {
            ["15:00:00.001", "15:00:00.002"]
        } else {
            ["15:00:00.000", "15:00:00.000"]
        };
        rewrite_sz_columns(
            &root,
            "mdl_6_36_0",
            vec![(
                "TransactTime",
                Arc::new(LargeStringArray::from_iter_values(times)),
            )],
        );
        if order_tail {
            rewrite_sz_columns(
                &root,
                "mdl_6_33_0",
                vec![
                    ("ApplSeqNum", Arc::new(Int64Array::from_iter_values([1, 5]))),
                    (
                        "TransactTime",
                        Arc::new(LargeStringArray::from_iter_values([
                            "09:20:00.000",
                            "15:00:00.003",
                        ])),
                    ),
                ],
            );
            rewrite_sz_columns(
                &root,
                "mdl_6_36_0",
                vec![(
                    "OfferApplSeqNum",
                    Arc::new(Int64Array::from_iter_values([0, 0])),
                )],
            );
        }
        let mut req = sz_request(&root);
        req.snapshots =
            Some(SnapshotSchedule::new(Duration::from_secs(3_600), 10).expect("schedule"));
        let report = replay_market_day(&req).expect("complete replay");
        assert_eq!(report.applied_events, 4);
        assert_eq!(report.market_close_snapshots, 1);
        let late_count = if order_tail {
            3
        } else if late {
            2
        } else {
            0
        };
        assert_eq!(report.sz_after_close_events, late_count);
        assert_eq!(
            report
                .sz_after_close_events_by_symbol
                .get("000001")
                .copied()
                .unwrap_or(0),
            late_count
        );
        let path = req
            .output_root
            .join("date=20260828/market=SZ/channel=1/part-0.parquet");
        let batch = ParquetRecordBatchReaderBuilder::try_new(File::open(path).expect("output"))
            .expect("reader")
            .build()
            .expect("batches")
            .next()
            .expect("batch")
            .expect("valid batch");
        let kinds = batch
            .column_by_name("snapshot_kind")
            .expect("kind")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("strings");
        let bids = batch
            .column_by_name("bid_quantity_1")
            .expect("bids")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("quantities");
        let asks = batch
            .column_by_name("ask_quantity_1")
            .expect("asks")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("quantities");
        let boundaries = batch
            .column_by_name("boundary_time")
            .expect("boundary")
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("timestamps");
        let mut closes = 0;
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "market_close" {
                closes += 1;
                assert!(bids.is_null(row), "the tail cancellation must be applied");
                assert_eq!(asks.value(row), if order_tail { 200 } else { 160 });
                let end = if order_tail { "15:00:00.003" } else { times[1] };
                assert_eq!(
                    boundaries.value(row),
                    parse_market_timestamp(day(), end).expect("end")
                );
            } else if boundaries.value(row)
                == parse_market_timestamp(day(), "15:00:00.000").expect("15:00")
            {
                assert_eq!(
                    bids.value(row),
                    100,
                    "scheduled 15:00 excludes the entire close batch"
                );
            }
        }
        assert_eq!(closes, 1);
    }
}

fn write_sz_e0_fixture(root: &TempDir) {
    // Two identical E0 frames: final state after a 40-share trade and a full
    // cancellation of the buy remainder. This fixture deliberately has no
    // opening/continuous references, which must be reported as missing source.
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (name, values) in [
        ("SecurityID", ["000001", "000001"]),
        ("UpdateTime", ["15:00:00.000", "15:00:03.000"]),
        ("TradingPhaseCode", ["E0", "E0"]),
    ] {
        fields.push(Field::new(name, DataType::LargeUtf8, false));
        columns.push(Arc::new(LargeStringArray::from_iter_values(values)));
    }
    fields.push(Field::new("source_row_no", DataType::UInt64, false));
    columns.push(Arc::new(UInt64Array::from_iter_values([1, 2])));
    for (name, value) in [
        ("TurnNum", 1),
        ("Volume", 40),
        ("TotalBidQty", 0),
        ("TotalOfferQty", 160),
    ] {
        fields.push(Field::new(name, DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from_iter_values([value; 2])));
    }
    let mut prices = vec![
        ("LastPrice".to_owned(), 10_500_000, 6),
        ("HighPrice".to_owned(), 10_500_000, 6),
        ("LowPrice".to_owned(), 10_500_000, 6),
        ("Turnover".to_owned(), 4_200_000, 4),
        ("PreCloPrice".to_owned(), 100_000, 4),
        ("HighLimitPrice".to_owned(), 12_000_000, 6),
        ("LowLimitPrice".to_owned(), 8_000_000, 6),
        ("WeightedAvgBidPx".to_owned(), 0, 6),
        ("WeightedAvgOfferPx".to_owned(), 11_000_000, 6),
    ];
    for side in ["Ask", "Bid"] {
        for level in 1..=10 {
            let populated = side == "Ask" && level == 1;
            prices.push((
                format!("{side}Price{level}"),
                if populated { 11_000_000 } else { 0 },
                6,
            ));
            fields.push(Field::new(
                format!("{side}Volume{level}"),
                DataType::Int64,
                false,
            ));
            columns.push(Arc::new(Int64Array::from_iter_values(
                [if populated { 160 } else { 0 }; 2],
            )));
            fields.push(Field::new(
                format!("NumOrders{}{level}", if side == "Ask" { "S" } else { "B" }),
                DataType::UInt32,
                false,
            ));
            columns.push(Arc::new(UInt32Array::from_iter_values(
                [u32::from(populated); 2],
            )));
        }
    }
    for (name, value, scale) in prices {
        fields.push(Field::new(name, DataType::Decimal128(38, scale), false));
        columns.push(Arc::new(
            Decimal128Array::from_iter_values([value; 2])
                .with_precision_and_scale(38, scale)
                .expect("decimal"),
        ));
    }
    write_batch_with_metadata(
        &root
            .path()
            .join("raw/date=20260828/mdl_6_28_0/part-0.parquet"),
        Arc::new(Schema::new(fields)),
        columns,
        "mdl_6_28_0",
    );
}

#[test]
fn shanghai_repeated_close_checks_normalized_fields_and_keeps_first_frame() {
    for (symbol, trading_day, predecessor) in [
        ("600000", 20_260_828, "CCALL"),
        ("510300", 20_260_703, "TRADE"),
        ("510300", 20_260_706, "CCALL"),
    ] {
        check_shanghai_repeated_close(
            symbol,
            trading_day,
            [predecessor, "CLOSE", "CLOSE"],
            ValidationOutcome::Matched,
        );
    }
}

#[test]
fn shanghai_suspended_close_is_excluded_but_missing_normal_close_is_not() {
    for (symbol, trading_day) in [
        ("600000", 20_260_828),
        ("510300", 20_260_703),
        ("510300", 20_260_706),
    ] {
        for statuses in [["SUSP", "SUSP", "SUSP"], ["SUSP", "CLOSE", "CLOSE"]] {
            check_shanghai_repeated_close(
                symbol,
                trading_day,
                statuses,
                ValidationOutcome::ExcludedByStatus,
            );
        }
        check_shanghai_repeated_close(
            symbol,
            trading_day,
            ["SUSP", "TRADE", "CCALL"],
            ValidationOutcome::MissingSource,
        );
    }
}

fn check_shanghai_repeated_close(
    symbol: &str,
    trading_day: u32,
    statuses: [&str; 3],
    expected: ValidationOutcome,
) {
    for conflict in [false, true] {
        let root = TempDir::new().expect("temporary directory");
        write_sse_symbol_fixture(&root, vec![Some("15:00:10.000"); 6], symbol, "600001");
        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for (name, values) in [
            ("SecurityID", [symbol; 3]),
            (
                "UpdateTime",
                ["14:57:00.000", "15:00:01.000", "15:00:02.000"],
            ),
            ("InstruStatus", statuses),
        ] {
            fields.push(Field::new(name, DataType::LargeUtf8, false));
            columns.push(Arc::new(LargeStringArray::from_iter_values(values)));
        }
        fields.push(Field::new("source_row_no", DataType::UInt64, false));
        columns.push(Arc::new(UInt64Array::from_iter_values([1, 2, 3])));
        fields.push(Field::new("TradNumber", DataType::UInt32, false));
        columns.push(Arc::new(UInt32Array::from_iter_values([1; 3])));
        let mut decimals = vec![
            ("LastPrice".to_owned(), 10_500, 3),
            ("HighPrice".to_owned(), 10_500, 3),
            ("LowPrice".to_owned(), 10_500, 3),
            ("Turnover".to_owned(), 42_000_000, 5),
            ("TradVolume".to_owned(), 40_000, 3),
            ("TotalBidVol".to_owned(), 0, 3),
            ("TotalAskVol".to_owned(), 160_000, 3),
            ("WAvgBidPri".to_owned(), 0, 3),
            ("WAvgAskPri".to_owned(), 11_000, 3),
        ];
        for side in ["Ask", "Bid"] {
            for level in 1..=10 {
                let populated = side == "Ask" && level == 1;
                decimals.push((
                    format!("{side}Price{level}"),
                    if populated { 11_000 } else { 0 },
                    3,
                ));
                decimals.push((
                    format!("{side}Volume{level}"),
                    if populated { 160_000 } else { 0 },
                    3,
                ));
                fields.push(Field::new(
                    format!("NumOrders{}{level}", if side == "Ask" { "S" } else { "B" }),
                    DataType::UInt32,
                    false,
                ));
                columns.push(Arc::new(UInt32Array::from_iter_values(
                    [u32::from(populated); 3],
                )));
            }
        }
        for (name, value, scale) in decimals {
            // Halt-only frames must not be parsed as normal book references:
            // this intentionally has a zero ask price with positive quantity.
            let value = if expected == ValidationOutcome::ExcludedByStatus && name == "AskPrice1" {
                0
            } else {
                value
            };
            let last = if conflict && name == "WAvgAskPri" {
                value + 1
            } else {
                value
            };
            fields.push(Field::new(name, DataType::Decimal128(38, scale), false));
            columns.push(Arc::new(
                Decimal128Array::from_iter_values([value, value, last])
                    .with_precision_and_scale(38, scale)
                    .expect("decimal"),
            ));
        }
        write_batch_with_metadata(
            &root
                .path()
                .join("raw/date=20260828/MarketData/part-0.parquet"),
            Arc::new(Schema::new(fields)),
            columns,
            "MarketData",
        );
        let mut req = request(&root, None);
        req.trading_day = TradingDay::from_yyyymmdd(trading_day).expect("valid test day");
        req.targets = TargetUniverse::Symbols(vec![Symbol::from(symbol)]);
        if trading_day != day().as_yyyymmdd() {
            fs::rename(
                root.path().join("raw/date=20260828"),
                root.path().join(format!("raw/date={trading_day}")),
            )
            .expect("fixture trading day");
        }
        let config = ValidationConfig {
            request: req.clone(),
            continuous_lookback: None,
            continuous_lookahead: None,
            retain_matched_records: true,
            max_detail_records: None,
        };
        let report = validate_market_day(&config).expect("report");
        #[cfg(feature = "profiling")]
        {
            let (profiled, _) = qtp_core::profile_validate_market_day(&config)
                .expect("profiled Shanghai validation");
            assert_eq!(
                serde_json::to_value(&report).expect("serialize"),
                serde_json::to_value(profiled).expect("serialize")
            );
        }
        let close = report
            .records
            .iter()
            .find(|r| r.anchor == ValidationAnchor::MarketClose)
            .expect("close");
        assert_eq!(
            close.outcome,
            if conflict && expected == ValidationOutcome::Matched {
                ValidationOutcome::DataError
            } else {
                expected.clone()
            }
        );
        if expected != ValidationOutcome::Matched {
            assert!(close.reference_time_ms.is_none());
            assert!(close.matched_candidate_raw_sequence.is_none());
            assert!(close.differences.is_empty());
            continue;
        }
        assert_eq!(
            close.reference_time_ms,
            Some(
                parse_market_timestamp(req.trading_day, "15:00:01.000").expect("time") / 1_000_000
            )
        );
        if conflict {
            assert!(
                close
                    .reason
                    .as_deref()
                    .expect("reason")
                    .contains("repeated")
            );
        } else {
            // The S/CLOSE sequence is 6; the last book-changing event is 5.
            assert_eq!(close.matched_candidate_raw_sequence, Some(5));
        }
    }
}

#[test]
fn repeated_close_ignores_pre_close_price_outside_comparison_fields() {
    let root = TempDir::new().expect("temporary directory");
    write_sz_fixture(&root, [3, 4], 60);
    write_sz_e0_fixture(&root);
    rewrite_sz_columns(
        &root,
        "mdl_6_28_0",
        vec![(
            "PreCloPrice",
            Arc::new(
                Decimal128Array::from_iter_values([100_000, 100_001])
                    .with_precision_and_scale(38, 4)
                    .expect("decimal"),
            ),
        )],
    );
    let report = validate_market_day(&ValidationConfig {
        request: sz_request(&root),
        continuous_lookback: None,
        continuous_lookahead: None,
        retain_matched_records: true,
        max_detail_records: None,
    })
    .expect("report");
    let close = report
        .records
        .iter()
        .find(|r| r.anchor == ValidationAnchor::MarketClose)
        .expect("close");
    assert_eq!(close.outcome, ValidationOutcome::Matched);
}

#[test]
fn repeated_sz_static_reference_conflict_is_a_data_error() {
    for opening in [false, true] {
        let root = TempDir::new().expect("temporary directory");
        write_sz_fixture(&root, [3, 4], 60);
        write_sz_e0_fixture(&root);
        let feed = "mdl_6_28_0";
        // Expand the two-row source with a preceding O0 for an opening test.
        if opening {
            let path = root
                .path()
                .join("raw/date=20260828/mdl_6_28_0/part-0.parquet");
            let mut reader =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&path).expect("file"))
                    .expect("builder")
                    .build()
                    .expect("reader");
            let batch = reader.next().expect("batch").expect("batch read");
            let indices = UInt32Array::from(vec![0, 0, 1]);
            let columns = batch
                .columns()
                .iter()
                .map(|column| arrow::compute::take(column, &indices, None).expect("take"))
                .collect();
            write_batch_with_metadata(&path, batch.schema(), columns, feed);
            rewrite_sz_columns(
                &root,
                feed,
                vec![
                    (
                        "TradingPhaseCode",
                        Arc::new(LargeStringArray::from_iter_values(["O0", "B0", "B0"])),
                    ),
                    (
                        "UpdateTime",
                        Arc::new(LargeStringArray::from_iter_values([
                            "09:24:00.000",
                            "09:25:00.000",
                            "09:25:03.000",
                        ])),
                    ),
                    (
                        "source_row_no",
                        Arc::new(UInt64Array::from_iter_values([1, 2, 3])),
                    ),
                    (
                        "Volume",
                        Arc::new(Int64Array::from_iter_values([40, 40, 41])),
                    ),
                ],
            );
        } else {
            rewrite_sz_columns(
                &root,
                feed,
                vec![("Volume", Arc::new(Int64Array::from_iter_values([40, 41])))],
            );
        }
        let report = validate_market_day(&ValidationConfig {
            request: sz_request(&root),
            continuous_lookback: None,
            continuous_lookahead: None,
            retain_matched_records: true,
            max_detail_records: None,
        })
        .expect("structured report");
        let anchor = if opening {
            ValidationAnchor::PreOpen
        } else {
            ValidationAnchor::MarketClose
        };
        let record = report
            .records
            .iter()
            .find(|r| r.anchor == anchor)
            .expect("record");
        assert_eq!(record.outcome, ValidationOutcome::DataError);
        assert!(
            record
                .reason
                .as_deref()
                .expect("reason")
                .contains("repeated")
        );
        assert!(record.matched_candidate_raw_sequence.is_none());
        assert!(!report.is_success());
    }
}

#[test]
fn shenzhen_e0_validates_eof_but_blocks_unclassified_late_events() {
    for late in [false, true] {
        let root = TempDir::new().expect("temp directory");
        write_sz_fixture(&root, [3, 4], 60);
        write_sz_e0_fixture(&root);
        rewrite_sz_columns(
            &root,
            "mdl_6_36_0",
            vec![(
                "TransactTime",
                Arc::new(LargeStringArray::from_iter_values([
                    "15:00:00.000",
                    if late { "15:00:00.001" } else { "15:00:00.000" },
                ])),
            )],
        );
        let config = ValidationConfig {
            request: sz_request(&root),
            continuous_lookback: None,
            continuous_lookahead: None,
            retain_matched_records: true,
            max_detail_records: None,
        };
        let report = validate_market_day(&config).expect("validation report");
        assert_eq!(report.replay.applied_events, 4);
        assert_eq!(report.replay.market_close_snapshots, 1);
        assert_eq!(report.replay.sz_after_close_events, u64::from(late));
        assert!(
            !report.is_success(),
            "required opening/continuous frames are missing"
        );
        assert_eq!(report.missing_source, 2);
        let close = report
            .records
            .iter()
            .find(|r| r.anchor == ValidationAnchor::MarketClose)
            .expect("E0 record");
        assert_eq!(
            close.reference_time_ms,
            Some(parse_market_timestamp(day(), "15:00:00.000").expect("time") / 1_000_000)
        );
        assert!(
            close.differences.is_empty(),
            "final fields equal even in the blocked case"
        );
        if late {
            assert_eq!(close.outcome, ValidationOutcome::DataError);
            assert!(
                close
                    .reason
                    .as_deref()
                    .expect("reason")
                    .contains("phase review required")
            );
            assert!(close.matched_candidate_time_ms.is_none());
            assert!(close.match_tag.is_none());
        } else {
            assert_eq!(close.outcome, ValidationOutcome::Matched);
        }
        let partial = validate_pre_open_market_day(&config).expect("opening-only replay");
        assert_eq!(partial.replay.market_close_snapshots, 0);
        assert_eq!(partial.replay.sz_after_close_events, 0);
        if late {
            let status = std::process::Command::new(env!("CARGO_BIN_EXE_qtp-replay"))
                .args([
                    "validate",
                    "--date",
                    "20260828",
                    "--market",
                    "SZ",
                    "--symbols",
                    "000001",
                ])
                .arg("--raw-root")
                .arg(&config.request.raw_root)
                .arg("--temp-root")
                .arg(&config.request.temp_root)
                .arg("--report")
                .arg(root.path().join("validation.json"))
                .output()
                .expect("CLI");
            assert!(
                !status.status.success(),
                "unclassified late events must fail CLI acceptance"
            );
            let json: serde_json::Value = serde_json::from_slice(
                &fs::read(root.path().join("validation.json")).expect("JSON report"),
            )
            .expect("parse report");
            assert_eq!(json["replay"]["sz_after_close_events"], 1);
        }
    }
}

// The buy at 10.50 never trades in the reopening auction. It must survive
// regardless of phase feed availability/timing. A later crossing limit ask
// also remains visible until an actual source execution/cancellation arrives.
#[test]
fn reopening_auction_keeps_untraded_limit_orders_without_phase_dependency() {
    for (phase, resume_time, reference_time) in [
        ("H0", "10:30:00.000", "10:30:00.000"),
        ("V0", "11:00:00.000", "10:59:59.000"),
        ("B0", "10:30:00.000", "10:30:00.000"),
        ("missing", "11:00:00.000", "11:00:00.000"),
    ] {
        let root = TempDir::new().expect("temp directory");
        write_sz_fixture(&root, [4, 6], 60);
        let base = root.path().join("raw/date=20260828");
        let order_path = base.join("mdl_6_33_0/part-0.parquet");
        let schema =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&order_path).expect("orders"))
                .expect("builder")
                .schema()
                .clone();
        let later = resume_time.replace(".000", ".001");
        write_batch_with_metadata(
            &order_path,
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1; 4])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 5])),
                Arc::new(LargeStringArray::from(vec!["000001"; 4])),
                Arc::new(
                    Decimal128Array::from(vec![100_000, 110_000, 105_000, 100_000])
                        .with_precision_and_scale(38, 4)
                        .expect("prices"),
                ),
                Arc::new(Int64Array::from(vec![100; 4])),
                Arc::new(Int32Array::from(vec![50, 49, 49, 50])),
                Arc::new(LargeStringArray::from(vec![
                    resume_time,
                    resume_time,
                    resume_time,
                    later.as_str(),
                ])),
                Arc::new(Int32Array::from(vec![50; 4])),
                Arc::new(LargeStringArray::from(vec![later.as_str(); 4])),
                Arc::new(UInt64Array::from(vec![1, 2, 3, 4])),
            ],
            "mdl_6_33_0",
        );
        let execution_path = base.join("mdl_6_36_0/part-0.parquet");
        let schema =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&execution_path).expect("trades"))
                .expect("builder")
                .schema()
                .clone();
        write_batch_with_metadata(
            &execution_path,
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![4])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(LargeStringArray::from(vec!["000001"])),
                Arc::new(
                    Decimal128Array::from(vec![110_000])
                        .with_precision_and_scale(38, 4)
                        .expect("price"),
                ),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Int32Array::from(vec![70])),
                Arc::new(LargeStringArray::from(vec![resume_time])),
                Arc::new(LargeStringArray::from(vec![later.as_str()])),
                Arc::new(UInt64Array::from(vec![1])),
            ],
            "mdl_6_36_0",
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("SecurityID", DataType::LargeUtf8, false),
            Field::new("UpdateTime", DataType::LargeUtf8, false),
            Field::new("TradingPhaseCode", DataType::LargeUtf8, false),
            Field::new("source_row_no", DataType::UInt64, false),
        ]));
        // Even an offset V0 -> T0 reference cannot affect order visibility.
        if phase != "missing" {
            write_batch_with_metadata(
                &base.join("mdl_6_28_0/part-0.parquet"),
                schema,
                vec![
                    Arc::new(LargeStringArray::from(vec!["000001"; 3])),
                    Arc::new(LargeStringArray::from(vec![
                        "09:25:00.000",
                        reference_time,
                        later.as_str(),
                    ])),
                    Arc::new(LargeStringArray::from(vec![phase, "T0", "T0"])),
                    Arc::new(UInt64Array::from(vec![1, 2, 3])),
                ],
                "mdl_6_28_0",
            );
        }
        let mut req = sz_request(&root);
        req.snapshots =
            Some(SnapshotSchedule::new(Duration::from_secs(900), 10).expect("schedule"));
        let report = replay_market_day(&req).expect("reopening replay");
        assert!(!report.sz_phase_source_available);
        assert_eq!(report.sz_phase_rows, 0);
        assert_eq!(report.sz_resumption_checkpoints, 0);
        assert_eq!(report.sz_resumption_rest_orders, 0);
        assert_eq!(report.sz_direct_rest_limit_orders, 4);
        let output = root
            .path()
            .join("output/date=20260828/market=SZ/channel=1/part-0.parquet");
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(output).expect("output"))
            .expect("builder")
            .build()
            .expect("reader");
        let batches = reader.collect::<Result<Vec<_>, _>>().expect("batches");
        let batch = batches.last().expect("batch");
        let index = batch.num_rows() - 1;
        let quantity = |name: &str| {
            let values = batch
                .column_by_name(name)
                .expect("column")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("quantities");
            if values.is_null(index) {
                0
            } else {
                values.value(index)
            }
        };
        assert_eq!(quantity("bid_quantity_1"), 100);
        assert_eq!(quantity("ask_quantity_1"), 100);
        let boundaries = batch
            .column_by_name("boundary_time")
            .expect("times")
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("timestamps");
        let resume_ns = parse_market_timestamp(day(), resume_time).expect("resume timestamp");
        let before = (0..batch.num_rows())
            .find(|i| boundaries.value(*i) == resume_ns)
            .expect("left-limit frame");
        for name in ["bid_quantity_1", "ask_quantity_1"] {
            assert!(
                batch.column_by_name(name).expect("depth").is_null(before),
                "reopening events at T must stay outside Snapshot(T)"
            );
        }
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
fn tick_replay_does_not_consume_even_invalid_reference_phase_files() {
    for case in 0..5 {
        let root = TempDir::new().expect("temp directory");
        write_sz_fixture(&root, [3, 4], 60);
        let schema = Arc::new(Schema::new(vec![
            Field::new("SecurityID", DataType::LargeUtf8, false),
            Field::new(
                if case == 3 { "WrongTime" } else { "UpdateTime" },
                DataType::LargeUtf8,
                false,
            ),
            Field::new("TradingPhaseCode", DataType::LargeUtf8, true),
            Field::new("source_row_no", DataType::UInt64, false),
        ]));
        let times = match case {
            0 => vec!["not-a-time", "10:30:00.000"],
            2 => vec!["11:00:00.000", "10:30:00.000"],
            _ => vec!["09:25:00.000", "10:30:00.000"],
        };
        write_batch_with_metadata(
            &root
                .path()
                .join("raw/date=20260828/mdl_6_28_0/part-0.parquet"),
            schema,
            vec![
                Arc::new(LargeStringArray::from(vec!["000001"; 2])),
                Arc::new(LargeStringArray::from(times)),
                Arc::new(LargeStringArray::from(vec![
                    Some("H0"),
                    if case == 1 { None } else { Some("T0") },
                ])),
                Arc::new(UInt64Array::from(vec![1, 2])),
            ],
            if case == 4 {
                "mdl_6_33_0"
            } else {
                "mdl_6_28_0"
            },
        );
        let report = replay_market_day(&sz_request(&root)).expect("tick-only replay");
        assert_eq!(report.applied_events, 4);
        assert_eq!(report.sz_direct_rest_limit_orders, 2);
        assert!(!report.sz_phase_source_available);
        assert_eq!(report.sz_phase_rows, 0);
    }
}

#[test]
fn repairs_descending_shenzhen_streams_before_two_way_merge() {
    for feed in ["mdl_6_33_0", "mdl_6_36_0"] {
        let root = TempDir::new().expect("temp directory");
        write_sz_fixture(&root, [3, 4], 60);
        let mut request = sz_request(&root);
        request.snapshots =
            Some(SnapshotSchedule::new(Duration::from_secs(3600), 10).expect("schedule"));
        let baseline = replay_market_day(&request).expect("ordered baseline");
        let output = root
            .path()
            .join("output/date=20260828/market=SZ/channel=1/part-0.parquet");
        let read_output = || {
            ParquetRecordBatchReaderBuilder::try_new(File::open(&output).expect("output"))
                .expect("reader")
                .build()
                .expect("reader")
                .collect::<Result<Vec<_>, _>>()
                .expect("batches")
        };
        let expected = read_output();
        assert!(baseline.sz_sequence_repairs.is_empty());
        let path = root
            .path()
            .join(format!("raw/date=20260828/{feed}/part-0.parquet"));
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).expect("file"))
            .expect("reader")
            .build()
            .expect("reader");
        let batch = reader.next().expect("batch").expect("batch");
        drop(reader);
        let indices = UInt32Array::from(vec![1, 0]);
        let columns = batch
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &indices, None).expect("reorder"))
            .collect();
        write_batch_with_metadata(&path, batch.schema(), columns, feed);
        request.batch_size = 1; // inversion spans Arrow batch boundaries
        let repaired = replay_market_day(&request).expect("repaired replay");
        assert_eq!(repaired.applied_events, baseline.applied_events);
        assert_eq!(repaired.selected_rows, baseline.selected_rows);
        assert_eq!(repaired.sz_sequence_regressions.len(), 1);
        assert_eq!(repaired.sz_sequence_repairs.len(), 1);
        assert_eq!(repaired.sz_sequence_repairs[0].natural_runs, 2);
        assert_eq!(repaired.sz_sequence_repairs[0].rows, 2);
        assert_eq!(read_output(), expected);
    }
}

#[test]
fn channel_repair_orders_multiple_symbols_before_cross_stream_execution() {
    let root = TempDir::new().expect("temp");
    write_sz_fixture(&root, [3, 4], 60);
    let path = root
        .path()
        .join("raw/date=20260828/mdl_6_33_0/part-0.parquet");
    let batch = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).expect("file"))
        .expect("reader")
        .build()
        .expect("reader")
        .next()
        .expect("batch")
        .expect("batch");
    let indices = UInt32Array::from(vec![0, 0, 1]);
    let mut columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &indices, None).expect("take"))
        .collect();
    for (name, array) in [
        (
            "ApplSeqNum",
            Arc::new(Int64Array::from(vec![1, 5, 2])) as ArrayRef,
        ),
        (
            "SecurityID",
            Arc::new(LargeStringArray::from(vec!["000001", "000002", "000001"])),
        ),
        ("source_row_no", Arc::new(UInt64Array::from(vec![1, 2, 3]))),
    ] {
        columns[batch.schema().index_of(name).expect("column")] = array;
    }
    write_batch_with_metadata(&path, batch.schema(), columns, "mdl_6_33_0");
    let mut request = sz_request(&root);
    request.targets = TargetUniverse::AllStocksAndEtfs;
    let report =
        replay_market_day(&request).expect("order 2 must precede trade 3 across securities");
    assert_eq!(report.symbols, 2);
    assert_eq!(report.applied_events, 5);
    assert_eq!(report.sz_sequence_regressions[0].previous_sequence, 5);
    assert_eq!(report.sz_sequence_regressions[0].sequence, 2);
    assert_eq!(report.sz_sequence_repairs[0].rows, 3);
}

#[test]
fn repaired_stream_still_rejects_cross_stream_equal_sequence() {
    let root = TempDir::new().expect("temp");
    write_sz_fixture(&root, [3, 2], 60);
    let error = replay_market_day(&sz_request(&root)).expect_err("equal sequence after repair");
    assert!(
        error
            .to_string()
            .contains("ambiguous Shenzhen ApplSeqNum 2")
    );
}

#[test]
fn native_sequence_repair_does_not_mask_invalid_order_lifecycle() {
    let root = TempDir::new().expect("temp directory");
    write_sz_fixture(&root, [4, 3], 60);
    // Merely swapping sequence values makes cancellation precede the trade.
    let error = replay_market_day(&sz_request(&root)).expect_err("invalid lifecycle must fail");
    assert!(error.to_string().contains("does not equal remaining 100"));
}

#[test]
fn rejects_shenzhen_cancellation_quantity_mismatch() {
    let root = TempDir::new().expect("temp directory");
    write_sz_fixture(&root, [3, 4], 59);
    let error = replay_market_day(&sz_request(&root)).expect_err("bad cancellation must fail");
    assert!(error.to_string().contains("does not equal remaining 60"));
}
