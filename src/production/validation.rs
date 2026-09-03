use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, Float64Array, Int64Array, LargeStringArray, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

use crate::{Market, OrderBook};

use super::replay::{ObservationPoint, ReplayReport, StateObserver, run_market_day};
use super::types::is_stock_symbol;
use super::{
    MarketDayRequest, PRODUCTION_PRICE_MULTIPLIER, ProductionError, SnapshotBookView,
    SnapshotLevel, ValidationAnchor, ValidationConfig,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FieldDifference {
    pub field: String,
    pub expected: String,
    pub actual: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationOutcome {
    Matched,
    Mismatched,
    NotComparable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ValidationRecord {
    pub market: String,
    pub symbol: String,
    pub anchor: ValidationAnchor,
    pub outcome: ValidationOutcome,
    pub reference_time_ns: Option<i64>,
    pub reason: Option<String>,
    pub differences: Vec<FieldDifference>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationReport {
    pub replay: ReplayReport,
    pub total_anchors: u64,
    pub comparable_anchors: u64,
    pub matched: u64,
    pub mismatched: u64,
    pub not_comparable: u64,
    pub match_rate: Option<f64>,
    pub not_comparable_rate: Option<f64>,
    pub mismatch_fields: BTreeMap<String, u64>,
    pub mismatch_reasons: BTreeMap<String, u64>,
    pub not_comparable_reasons: BTreeMap<String, u64>,
    pub records: Vec<ValidationRecord>,
}

impl ValidationReport {
    #[must_use]
    pub const fn is_success(&self) -> bool {
        self.mismatched == 0
    }
}

#[derive(Clone, Debug)]
struct ReferenceSnapshot {
    time_ns: i64,
    view: Option<SnapshotBookView>,
    load_error: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct SymbolReferences {
    pre_open: Option<ReferenceSnapshot>,
    continuous_end: Option<ReferenceSnapshot>,
    market_close: Option<ReferenceSnapshot>,
}

impl SymbolReferences {
    fn get(&self, anchor: ValidationAnchor) -> Option<&ReferenceSnapshot> {
        match anchor {
            ValidationAnchor::PreOpen => self.pre_open.as_ref(),
            ValidationAnchor::ContinuousEnd => self.continuous_end.as_ref(),
            ValidationAnchor::MarketClose => self.market_close.as_ref(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct AnchorState {
    matched: bool,
    finalized: bool,
    best_differences: Option<Vec<FieldDifference>>,
}

struct ValidationObserver {
    market: Market,
    references: HashMap<String, SymbolReferences>,
    states: HashMap<(String, ValidationAnchor), AnchorState>,
    seen_symbols: HashSet<String>,
}

impl ValidationObserver {
    fn new(market: Market, references: HashMap<String, SymbolReferences>) -> Self {
        Self {
            market,
            references,
            states: HashMap::new(),
            seen_symbols: HashSet::new(),
        }
    }

    fn compare_candidate(
        &mut self,
        symbol: &str,
        book: &OrderBook,
        anchor: ValidationAnchor,
    ) -> Result<(), ProductionError> {
        let Some(reference) = self
            .references
            .get(symbol)
            .and_then(|references| references.get(anchor))
        else {
            return Ok(());
        };
        let state = self.states.entry((symbol.to_owned(), anchor)).or_default();
        if state.matched || state.finalized {
            return Ok(());
        }
        let Some(expected) = reference.view.as_ref() else {
            return Ok(());
        };
        let actual = SnapshotBookView::from_book(book, 10)?;
        let differences = compare_views(self.market, expected, &actual);
        if differences.is_empty() {
            state.matched = true;
            state.finalized = true;
        } else if state
            .best_differences
            .as_ref()
            .is_none_or(|best| differences.len() < best.len())
        {
            state.best_differences = Some(differences);
        }
        Ok(())
    }

    fn observe_timed_anchor(
        &mut self,
        symbol: &str,
        book: &OrderBook,
        anchor: ValidationAnchor,
        point: ObservationPoint,
    ) -> Result<(), ProductionError> {
        let Some(reference_time) = self
            .references
            .get(symbol)
            .and_then(|references| references.get(anchor))
            .map(|reference| reference.time_ns)
        else {
            return Ok(());
        };
        let point_time = match point {
            ObservationPoint::BeforeEvent(value) | ObservationPoint::AfterEvent(value) => value,
            ObservationPoint::MarketClose(_) | ObservationPoint::ChannelFinished => return Ok(()),
        };
        if point_time >= reference_time {
            self.compare_candidate(symbol, book, anchor)?;
        }
        if point_time > reference_time {
            self.states
                .entry((symbol.to_owned(), anchor))
                .or_default()
                .finalized = true;
        }
        Ok(())
    }

    fn into_report(mut self, replay: ReplayReport) -> ValidationReport {
        let market = market_text(self.market).to_owned();
        let mut symbols = self
            .references
            .keys()
            .chain(&self.seen_symbols)
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        symbols.sort_unstable();
        let mut records = Vec::new();
        for symbol in symbols {
            for anchor in [
                ValidationAnchor::PreOpen,
                ValidationAnchor::ContinuousEnd,
                ValidationAnchor::MarketClose,
            ] {
                let reference = self
                    .references
                    .get(&symbol)
                    .and_then(|references| references.get(anchor));
                let state = self.states.remove(&(symbol.clone(), anchor));
                let (outcome, reason, differences) = match (reference, state) {
                    (None, _)
                        if anchor == ValidationAnchor::MarketClose
                            && self.seen_symbols.contains(&symbol) =>
                    {
                        (
                            ValidationOutcome::Mismatched,
                            Some("required market-close reference is missing".to_owned()),
                            Vec::new(),
                        )
                    }
                    (None, _) => (
                        ValidationOutcome::NotComparable,
                        Some("reference anchor is missing".to_owned()),
                        Vec::new(),
                    ),
                    (Some(_), _) if !self.seen_symbols.contains(&symbol) => (
                        ValidationOutcome::NotComparable,
                        Some("no selected raw order/trade events were observed".to_owned()),
                        Vec::new(),
                    ),
                    (Some(reference), _) if reference.load_error.is_some() => (
                        ValidationOutcome::Mismatched,
                        reference.load_error.clone(),
                        Vec::new(),
                    ),
                    (Some(_), Some(state)) if state.matched => {
                        (ValidationOutcome::Matched, None, Vec::new())
                    }
                    (Some(_), Some(state)) => (
                        ValidationOutcome::Mismatched,
                        Some("no reconstructed candidate matched the reference".to_owned()),
                        state.best_differences.unwrap_or_default(),
                    ),
                    (Some(_), None) => (
                        ValidationOutcome::Mismatched,
                        Some("replay did not reach the reference anchor".to_owned()),
                        Vec::new(),
                    ),
                };
                records.push(ValidationRecord {
                    market: market.clone(),
                    symbol: symbol.clone(),
                    anchor,
                    outcome,
                    reference_time_ns: reference.map(|reference| reference.time_ns),
                    reason,
                    differences,
                });
            }
        }
        records.sort_by(|left, right| {
            left.symbol
                .cmp(&right.symbol)
                .then_with(|| anchor_rank(left.anchor).cmp(&anchor_rank(right.anchor)))
        });
        let matched = count_outcome(&records, ValidationOutcome::Matched);
        let mismatched = count_outcome(&records, ValidationOutcome::Mismatched);
        let not_comparable = count_outcome(&records, ValidationOutcome::NotComparable);
        let total_anchors = records.len() as u64;
        let comparable_anchors = matched + mismatched;
        let mismatch_fields = count_mismatch_fields(&records);
        let mismatch_reasons = count_reasons(&records, ValidationOutcome::Mismatched);
        let not_comparable_reasons = count_reasons(&records, ValidationOutcome::NotComparable);
        ValidationReport {
            replay,
            total_anchors,
            comparable_anchors,
            matched,
            mismatched,
            not_comparable,
            match_rate: ratio(matched, comparable_anchors),
            not_comparable_rate: ratio(not_comparable, total_anchors),
            mismatch_fields,
            mismatch_reasons,
            not_comparable_reasons,
            records,
        }
    }
}

impl StateObserver for ValidationObserver {
    fn observe(
        &mut self,
        _channel: u32,
        symbol: &str,
        book: &OrderBook,
        point: ObservationPoint,
    ) -> Result<(), ProductionError> {
        self.seen_symbols.insert(symbol.to_owned());
        self.observe_timed_anchor(symbol, book, ValidationAnchor::PreOpen, point)?;
        self.observe_timed_anchor(symbol, book, ValidationAnchor::ContinuousEnd, point)?;
        if let ObservationPoint::MarketClose(_) = point {
            self.compare_candidate(symbol, book, ValidationAnchor::MarketClose)?;
            self.states
                .entry((symbol.to_owned(), ValidationAnchor::MarketClose))
                .or_default()
                .finalized = true;
        }
        if point == ObservationPoint::ChannelFinished {
            for anchor in [ValidationAnchor::PreOpen, ValidationAnchor::ContinuousEnd] {
                if self
                    .references
                    .get(symbol)
                    .and_then(|references| references.get(anchor))
                    .is_some()
                {
                    self.compare_candidate(symbol, book, anchor)?;
                    self.states
                        .entry((symbol.to_owned(), anchor))
                        .or_default()
                        .finalized = true;
                }
            }
        }
        Ok(())
    }
}

pub fn validate_market_day(config: &ValidationConfig) -> Result<ValidationReport, ProductionError> {
    let references = load_references(&config.request, &config.reference_root)?;
    let mut observer = ValidationObserver::new(config.request.market, references);
    let mut request = config.request.clone();
    request.snapshots = None;
    let replay = run_market_day(&request, &mut observer)?;
    Ok(observer.into_report(replay))
}

fn count_outcome(records: &[ValidationRecord], outcome: ValidationOutcome) -> u64 {
    records
        .iter()
        .filter(|record| record.outcome == outcome)
        .count() as u64
}

fn ratio(numerator: u64, denominator: u64) -> Option<f64> {
    (denominator != 0).then_some(numerator as f64 / denominator as f64)
}

fn count_mismatch_fields(records: &[ValidationRecord]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for difference in records
        .iter()
        .filter(|record| record.outcome == ValidationOutcome::Mismatched)
        .flat_map(|record| &record.differences)
    {
        *counts.entry(difference.field.clone()).or_default() += 1;
    }
    counts
}

fn count_reasons(
    records: &[ValidationRecord],
    outcome: ValidationOutcome,
) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for reason in records
        .iter()
        .filter(|record| record.outcome == outcome)
        .filter_map(|record| record.reason.as_ref())
    {
        *counts.entry(reason.clone()).or_default() += 1;
    }
    counts
}

fn load_references(
    request: &MarketDayRequest,
    root: &Path,
) -> Result<HashMap<String, SymbolReferences>, ProductionError> {
    let path = reference_path(request, root);
    if !path.is_file() {
        return Err(ProductionError::MissingInput(path));
    }
    let file = File::open(&path).map_err(|error| ProductionError::io(&path, error))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| ProductionError::parquet(&path, error))?
        .with_batch_size(request.batch_size)
        .build()
        .map_err(|error| ProductionError::parquet(&path, error))?;
    let pre_open_start = super::parse_market_timestamp(request.trading_day, "09:25:00.000")?;
    let pre_open_end = super::parse_market_timestamp(request.trading_day, "09:30:00.000")?;
    let continuous_start = pre_open_end;
    let continuous_end = super::parse_market_timestamp(request.trading_day, "14:57:00.000")?;
    let mut references: HashMap<String, SymbolReferences> = HashMap::new();
    for batch in reader {
        let batch = batch.map_err(|source| ProductionError::Arrow {
            context: "reference Parquet batch",
            source,
        })?;
        validate_reference_schema(&path, &batch)?;
        let symbols = large_string(&path, &batch, "security")?;
        let times = timestamp_ms(&path, &batch, "ts")?;
        let statuses = large_string(&path, &batch, "instru_status")?;
        for row in 0..batch.num_rows() {
            if symbols.is_null(row) || times.is_null(row) || statuses.is_null(row) {
                return Err(ProductionError::Schema {
                    path: path.clone(),
                    detail: format!("null key field at reference row {row}"),
                });
            }
            let symbol = symbols.value(row).trim();
            if !is_stock_symbol(request.market, symbol)
                || !request.targets.contains(request.market, symbol)
            {
                continue;
            }
            let time_ns = reference_time_millis(times, row)?
                .checked_mul(1_000_000)
                .ok_or(ProductionError::Arithmetic("reference timestamp"))?;
            let status = statuses.value(row).trim();
            let anchor = reference_anchor(
                request.market,
                status,
                time_ns,
                pre_open_start,
                pre_open_end,
                continuous_start,
                continuous_end,
            );
            let Some(anchor) = anchor else {
                continue;
            };
            let entry = references.entry(symbol.to_owned()).or_default();
            let slot = match anchor {
                ValidationAnchor::PreOpen => &mut entry.pre_open,
                ValidationAnchor::ContinuousEnd => &mut entry.continuous_end,
                ValidationAnchor::MarketClose => &mut entry.market_close,
            };
            if let Some(existing) = slot.as_ref() {
                if anchor == ValidationAnchor::MarketClose {
                    return Err(ProductionError::Validation(format!(
                        "duplicate close reference for {symbol}"
                    )));
                }
                if !should_replace_reference(anchor, existing.time_ns, time_ns) {
                    continue;
                }
            }
            let (view, load_error) = match reference_view(&path, &batch, row) {
                Ok(view) => (Some(view), None),
                Err(ProductionError::Validation(detail)) => (None, Some(detail)),
                Err(error) => return Err(error),
            };
            let reference = ReferenceSnapshot {
                time_ns,
                view,
                load_error,
            };
            *slot = Some(reference);
        }
    }
    if let super::TargetUniverse::Symbols(symbols) = &request.targets {
        for symbol in symbols {
            references.entry(symbol.to_string()).or_default();
        }
    }
    Ok(references)
}

fn reference_anchor(
    market: Market,
    status: &str,
    time_ns: i64,
    pre_open_start: i64,
    pre_open_end: i64,
    continuous_start: i64,
    continuous_end: i64,
) -> Option<ValidationAnchor> {
    if (pre_open_start..pre_open_end).contains(&time_ns) {
        Some(ValidationAnchor::PreOpen)
    } else if time_ns >= continuous_start && continuous_trading_status(market, status) {
        Some(ValidationAnchor::ContinuousEnd)
    } else if close_status(market, status, time_ns, continuous_end) {
        Some(ValidationAnchor::MarketClose)
    } else {
        None
    }
}

const fn should_replace_reference(
    anchor: ValidationAnchor,
    existing_time_ns: i64,
    candidate_time_ns: i64,
) -> bool {
    match anchor {
        ValidationAnchor::PreOpen => candidate_time_ns < existing_time_ns,
        ValidationAnchor::ContinuousEnd => candidate_time_ns > existing_time_ns,
        ValidationAnchor::MarketClose => false,
    }
}

fn continuous_trading_status(market: Market, status: &str) -> bool {
    match market {
        Market::Sse => status == "TRADE",
        Market::Szse => status == "T0",
    }
}

fn close_status(market: Market, status: &str, time_ns: i64, continuous_end: i64) -> bool {
    match market {
        Market::Sse => status == "CLOSE",
        Market::Szse => status == "E0" && time_ns >= continuous_end,
    }
}

fn reference_path(request: &MarketDayRequest, root: &Path) -> PathBuf {
    root.join(format!("date={}", request.trading_day.as_yyyymmdd()))
        .join(format!("market={}", market_text(request.market)))
        .join("part-0.parquet")
}

fn market_text(market: Market) -> &'static str {
    match market {
        Market::Sse => "SH",
        Market::Szse => "SZ",
    }
}

fn validate_reference_schema(path: &Path, batch: &RecordBatch) -> Result<(), ProductionError> {
    for field in [
        "security",
        "ts",
        "instru_status",
        "last_price",
        "high_price",
        "low_price",
        "num_trades",
        "volume",
        "turnover",
        "total_bid_quantity",
        "weighted_average_bid_price",
        "total_ask_quantity",
        "weighted_average_ask_price",
    ] {
        batch
            .schema()
            .index_of(field)
            .map_err(|_| ProductionError::Schema {
                path: path.to_path_buf(),
                detail: format!("missing reference field {field}"),
            })?;
    }
    for side in ["ask", "bid"] {
        for level in 1..=10 {
            for field in [
                format!("{side}_price_{level}"),
                format!("{side}_volume_{level}"),
                format!(
                    "num_orders_{}{level}",
                    if side == "ask" { "s" } else { "b" }
                ),
            ] {
                batch
                    .schema()
                    .index_of(&field)
                    .map_err(|_| ProductionError::Schema {
                        path: path.to_path_buf(),
                        detail: format!("missing reference field {field}"),
                    })?;
            }
        }
    }
    Ok(())
}

fn reference_view(
    path: &Path,
    batch: &RecordBatch,
    row: usize,
) -> Result<SnapshotBookView, ProductionError> {
    let mut asks = Vec::new();
    let mut bids = Vec::new();
    for (side, target) in [("ask", &mut asks), ("bid", &mut bids)] {
        for level in 1..=10 {
            let price = optional_price_units(path, batch, &format!("{side}_price_{level}"), row)?;
            let quantity =
                optional_nonnegative(path, batch, &format!("{side}_volume_{level}"), row)?;
            let count = optional_nonnegative(
                path,
                batch,
                &format!(
                    "num_orders_{}{level}",
                    if side == "ask" { "s" } else { "b" }
                ),
                row,
            )?;
            match (price, quantity, count) {
                (None, None | Some(0), None | Some(0)) => {}
                (Some(price_units), Some(quantity), Some(order_count)) if quantity > 0 => {
                    target.push(SnapshotLevel {
                        price_units,
                        quantity,
                        order_count,
                    });
                }
                values => {
                    return Err(ProductionError::Validation(format!(
                        "inconsistent {side} level {level} in {} row {row}: {values:?}",
                        path.display()
                    )));
                }
            }
        }
    }
    Ok(SnapshotBookView {
        bids,
        asks,
        total_bid_quantity: required_nonnegative(path, batch, "total_bid_quantity", row)?,
        weighted_bid_price_units: optional_price_units(
            path,
            batch,
            "weighted_average_bid_price",
            row,
        )?,
        total_ask_quantity: required_nonnegative(path, batch, "total_ask_quantity", row)?,
        weighted_ask_price_units: optional_price_units(
            path,
            batch,
            "weighted_average_ask_price",
            row,
        )?,
        last_price_units: optional_price_units(path, batch, "last_price", row)?,
        high_price_units: optional_price_units(path, batch, "high_price", row)?,
        low_price_units: optional_price_units(path, batch, "low_price", row)?,
        trade_count: required_nonnegative(path, batch, "num_trades", row)?,
        trade_quantity: required_nonnegative(path, batch, "volume", row)?,
        turnover_units: u128::from(required_scaled_float(path, batch, "turnover", row)?),
    })
}

fn compare_views(
    market: Market,
    expected: &SnapshotBookView,
    actual: &SnapshotBookView,
) -> Vec<FieldDifference> {
    let mut differences = Vec::new();
    compare(&mut differences, "bids", &expected.bids, &actual.bids);
    compare(&mut differences, "asks", &expected.asks, &actual.asks);
    compare(
        &mut differences,
        "total_bid_quantity",
        &expected.total_bid_quantity,
        &actual.total_bid_quantity,
    );
    compare(
        &mut differences,
        "weighted_bid_price_units",
        &expected.weighted_bid_price_units,
        &validation_price(market, actual.weighted_bid_price_units),
    );
    compare(
        &mut differences,
        "total_ask_quantity",
        &expected.total_ask_quantity,
        &actual.total_ask_quantity,
    );
    compare(
        &mut differences,
        "weighted_ask_price_units",
        &expected.weighted_ask_price_units,
        &validation_price(market, actual.weighted_ask_price_units),
    );
    for (field, expected_value, actual_value) in [
        (
            "last_price_units",
            expected.last_price_units,
            actual.last_price_units,
        ),
        (
            "high_price_units",
            expected.high_price_units,
            actual.high_price_units,
        ),
        (
            "low_price_units",
            expected.low_price_units,
            actual.low_price_units,
        ),
    ] {
        compare(&mut differences, field, &expected_value, &actual_value);
    }
    compare(
        &mut differences,
        "trade_count",
        &expected.trade_count,
        &actual.trade_count,
    );
    compare(
        &mut differences,
        "trade_quantity",
        &expected.trade_quantity,
        &actual.trade_quantity,
    );
    compare(
        &mut differences,
        "turnover_units",
        &expected.turnover_units,
        &actual.turnover_units,
    );
    differences
}

fn validation_price(market: Market, value: Option<i64>) -> Option<i64> {
    value.map(|units| match market {
        Market::Sse => (units + 5) / 10 * 10,
        Market::Szse => (units + 50) / 100 * 100,
    })
}

fn reference_time_millis(
    array: &TimestampMillisecondArray,
    row: usize,
) -> Result<i64, ProductionError> {
    const SHANGHAI_OFFSET_MILLIS: i64 = 8 * 60 * 60 * 1_000;
    let value = array.value(row);
    match array.data_type() {
        arrow::datatypes::DataType::Timestamp(_, None) => value
            .checked_sub(SHANGHAI_OFFSET_MILLIS)
            .ok_or(ProductionError::Arithmetic("reference local timestamp")),
        arrow::datatypes::DataType::Timestamp(_, Some(_)) => Ok(value),
        _ => Err(ProductionError::Validation(
            "reference timestamp has an unsupported Arrow type".to_owned(),
        )),
    }
}

fn compare<T: std::fmt::Debug + PartialEq>(
    differences: &mut Vec<FieldDifference>,
    field: &str,
    expected: &T,
    actual: &T,
) {
    if expected != actual {
        differences.push(FieldDifference {
            field: field.to_owned(),
            expected: format!("{expected:?}"),
            actual: format!("{actual:?}"),
        });
    }
}

fn column_index(path: &Path, batch: &RecordBatch, name: &str) -> Result<usize, ProductionError> {
    batch
        .schema()
        .index_of(name)
        .map_err(|_| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("missing reference field {name}"),
        })
}

fn large_string<'a>(
    path: &Path,
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a LargeStringArray, ProductionError> {
    let index = column_index(path, batch, name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or_else(|| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("reference field {name} is not LargeUtf8"),
        })
}

fn timestamp_ms<'a>(
    path: &Path,
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a TimestampMillisecondArray, ProductionError> {
    let index = column_index(path, batch, name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or_else(|| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("reference field {name} is not timestamp[ms]"),
        })
}

fn float64<'a>(
    path: &Path,
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Float64Array, ProductionError> {
    let index = column_index(path, batch, name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or_else(|| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("reference field {name} is not Float64"),
        })
}

fn int64<'a>(
    path: &Path,
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Int64Array, ProductionError> {
    let index = column_index(path, batch, name)?;
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or_else(|| ProductionError::Schema {
            path: path.to_path_buf(),
            detail: format!("reference field {name} is not Int64"),
        })
}

fn optional_price_units(
    path: &Path,
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<i64>, ProductionError> {
    let array = float64(path, batch, name)?;
    if array.is_null(row) || array.value(row) == 0.0 {
        return Ok(None);
    }
    let value = array.value(row);
    let rounded = scaled_reference_units(value).ok_or_else(|| {
        ProductionError::Validation(format!(
            "invalid reference price {name}={value} at row {row}"
        ))
    })?;
    if rounded == 0.0 || rounded >= i64::MAX as f64 {
        return Err(ProductionError::Validation(format!(
            "reference price {name}={value} is out of range at scale 10,000"
        )));
    }
    Ok(Some(rounded as i64))
}

fn optional_nonnegative(
    path: &Path,
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<u64>, ProductionError> {
    let array = int64(path, batch, name)?;
    if array.is_null(row) {
        return Ok(None);
    }
    u64::try_from(array.value(row)).map(Some).map_err(|_| {
        ProductionError::Validation(format!("reference {name} is negative at row {row}"))
    })
}

fn required_nonnegative(
    path: &Path,
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<u64, ProductionError> {
    optional_nonnegative(path, batch, name, row)?.ok_or_else(|| {
        ProductionError::Validation(format!("reference {name} is null at row {row}"))
    })
}

fn required_scaled_float(
    path: &Path,
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<u64, ProductionError> {
    let array = float64(path, batch, name)?;
    if array.is_null(row) {
        return Err(ProductionError::Validation(format!(
            "reference {name} is null at row {row}"
        )));
    }
    let value = array.value(row);
    let rounded = scaled_reference_units(value).ok_or_else(|| {
        ProductionError::Validation(format!("invalid reference {name}={value} at row {row}"))
    })?;
    if rounded >= u64::MAX as f64 {
        return Err(ProductionError::Validation(format!(
            "reference {name}={value} is out of range at scale 10,000"
        )));
    }
    Ok(rounded as u64)
}

fn scaled_reference_units(value: f64) -> Option<f64> {
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    let scaled = value * PRODUCTION_PRICE_MULTIPLIER as f64;
    scaled.is_finite().then(|| scaled.round())
}

const fn anchor_rank(anchor: ValidationAnchor) -> u8 {
    match anchor {
        ValidationAnchor::PreOpen => 0,
        ValidationAnchor::ContinuousEnd => 1,
        ValidationAnchor::MarketClose => 2,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::TimestampMillisecondArray;

    use super::{
        ObservationPoint, ReferenceSnapshot, ReplayReport, StateObserver, SymbolReferences,
        ValidationAnchor, ValidationObserver, ValidationOutcome,
    };
    use crate::{
        AddOrder, ApplySequence, BookConfig, BookEvent, BookKey, ChannelId, CrossingBehavior,
        EventMeta, LocalTimestampNs, Market, OrderBook, OrderId, OrderKey, Price, PriceScale,
        PricingInstruction, Quantity, QuoteTimestampNs, RawSequence, Side, Symbol, TradingDay,
    };

    fn some_or_abort<T>(value: Option<T>) -> T {
        match value {
            Some(value) => value,
            None => std::process::abort(),
        }
    }

    fn empty_book() -> OrderBook {
        let key = BookKey {
            market: Market::Sse,
            trading_day: some_or_abort(TradingDay::from_yyyymmdd(20_260_828)),
            symbol: Symbol::from("600000"),
        };
        OrderBook::new(BookConfig::new(
            key,
            some_or_abort(PriceScale::from_decimal_places(4)),
        ))
    }

    fn add_order(book: &mut OrderBook) {
        let key = OrderKey {
            channel_id: some_or_abort(ChannelId::new(1)),
            side: Side::Buy,
            order_id: some_or_abort(OrderId::new(1)),
        };
        let event = BookEvent::AddOrder(AddOrder {
            meta: EventMeta {
                book_key: book.config().book_key.clone(),
                raw_sequence: some_or_abort(RawSequence::new(1)),
                apply_sequence: some_or_abort(ApplySequence::new(1)),
                local_time: LocalTimestampNs::from_nanos(11),
                quote_time: QuoteTimestampNs::from_nanos(10),
            },
            order_key: key,
            pricing: PricingInstruction::Provided(some_or_abort(Price::from_units(100_000))),
            crossing: CrossingBehavior::Rest,
            quantity: some_or_abort(Quantity::new(100)),
        });
        if book.apply(event).is_err() {
            std::process::abort();
        }
    }

    #[test]
    fn timed_anchor_accepts_a_state_inside_the_equal_timestamp_bucket() {
        let mut expected_book = empty_book();
        add_order(&mut expected_book);
        let expected = match super::SnapshotBookView::from_book(&expected_book, 10) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let references = HashMap::from([(
            "600000".to_owned(),
            SymbolReferences {
                pre_open: Some(ReferenceSnapshot {
                    time_ns: 10,
                    view: Some(expected),
                    load_error: None,
                }),
                continuous_end: None,
                market_close: None,
            },
        )]);
        let mut observer = ValidationObserver::new(Market::Sse, references);
        let mut actual = empty_book();
        assert!(
            observer
                .observe(1, "600000", &actual, ObservationPoint::BeforeEvent(10))
                .is_ok()
        );
        add_order(&mut actual);
        assert!(
            observer
                .observe(1, "600000", &actual, ObservationPoint::AfterEvent(10))
                .is_ok()
        );
        let report = observer.into_report(ReplayReport::default());
        let pre_open = report
            .records
            .iter()
            .find(|record| record.anchor == ValidationAnchor::PreOpen);
        assert!(matches!(
            pre_open,
            Some(record) if record.outcome == ValidationOutcome::Matched
        ));
        assert_eq!(report.matched, 1);
        assert_eq!(report.mismatched, 1);
        assert_eq!(report.not_comparable, 1);
        assert_eq!(report.match_rate, Some(0.5));
    }

    #[test]
    fn interprets_naive_reference_timestamp_as_shanghai_wall_time() {
        let local_as_utc_millis = 1_787_909_204_000_i64;
        let timestamps = TimestampMillisecondArray::from_iter_values([local_as_utc_millis]);
        let actual = match super::reference_time_millis(&timestamps, 0) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        assert_eq!(actual, local_as_utc_millis - 8 * 60 * 60 * 1_000);
    }

    #[test]
    fn rounds_weighted_prices_to_reference_feed_precision() {
        assert_eq!(
            super::validation_price(Market::Sse, Some(89_086)),
            Some(89_090)
        );
        assert_eq!(
            super::validation_price(Market::Szse, Some(113_222)),
            Some(113_200)
        );
    }

    #[test]
    fn quantizes_float64_reference_values_at_scale_10_000() {
        assert_eq!(
            super::scaled_reference_units(45_789_402.66),
            Some(457_894_026_600.0)
        );
        assert_eq!(
            super::scaled_reference_units(160_930_864.11),
            Some(1_609_308_641_100.0)
        );
        assert_eq!(super::scaled_reference_units(12.345_67), Some(123_457.0));
        assert_eq!(super::scaled_reference_units(f64::NAN), None);
        assert_eq!(super::scaled_reference_units(-0.000_1), None);
    }

    #[test]
    fn production_price_multiplier_matches_four_decimal_places() {
        let scale = some_or_abort(PriceScale::from_decimal_places(
            super::super::PRODUCTION_PRICE_DECIMAL_PLACES,
        ));
        assert_eq!(
            scale.multiplier(),
            super::super::PRODUCTION_PRICE_MULTIPLIER
        );
    }

    #[test]
    fn continuous_end_uses_latest_regular_frame_without_quote_time_cutoff() {
        let pre_open_start = 100;
        let continuous_start = 200;
        let continuous_end = 300;

        assert_eq!(
            super::reference_anchor(
                Market::Sse,
                "TRADE",
                299,
                pre_open_start,
                continuous_start,
                continuous_start,
                continuous_end,
            ),
            Some(ValidationAnchor::ContinuousEnd)
        );
        assert_eq!(
            super::reference_anchor(
                Market::Sse,
                "CCALL",
                300,
                pre_open_start,
                continuous_start,
                continuous_start,
                continuous_end,
            ),
            None
        );
        assert_eq!(
            super::reference_anchor(
                Market::Szse,
                "T0",
                299,
                pre_open_start,
                continuous_start,
                continuous_start,
                continuous_end,
            ),
            Some(ValidationAnchor::ContinuousEnd)
        );
        assert_eq!(
            super::reference_anchor(
                Market::Szse,
                "T0",
                300,
                pre_open_start,
                continuous_start,
                continuous_start,
                continuous_end,
            ),
            Some(ValidationAnchor::ContinuousEnd)
        );
        assert_eq!(
            super::reference_anchor(
                Market::Sse,
                "TRADE",
                301,
                pre_open_start,
                continuous_start,
                continuous_start,
                continuous_end,
            ),
            Some(ValidationAnchor::ContinuousEnd)
        );
        assert_eq!(
            super::reference_anchor(
                Market::Szse,
                "E0",
                300,
                pre_open_start,
                continuous_start,
                continuous_start,
                continuous_end,
            ),
            Some(ValidationAnchor::MarketClose)
        );

        assert!(super::should_replace_reference(
            ValidationAnchor::ContinuousEnd,
            250,
            299,
        ));
        assert!(!super::should_replace_reference(
            ValidationAnchor::ContinuousEnd,
            299,
            250,
        ));
        assert!(super::should_replace_reference(
            ValidationAnchor::PreOpen,
            150,
            120,
        ));
    }
}
