//! Read raw reference frames, select eligible records and retain independent audit.
//! This module never reads or mutates a reconstructed OrderBook.
use super::close_range::DayLimits;
use super::columns::{self, RawSnapshotColumns, large_string, uint64};
use super::phases::{PhaseTracker, SelectionAudit};
use super::reference::ReferenceBookView;
use super::{AnchorState, ReferenceSnapshot, SymbolReferences};
use crate::production::SymbolMap;
use crate::production::types::{is_etf_symbol, is_supported_symbol};
use crate::{Market, MarketDayRequest, ProductionError, ValidationAnchor};
use arrow::array::Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};

pub(super) struct LoadedReferences {
    pub(super) sz_close_limits: HashMap<String, DayLimits>,
    pub(super) books: HashMap<String, SymbolReferences>,
    pub(super) selection_audit: BTreeMap<String, SelectionAudit>,
}

struct ReferenceLoadState {
    references: SymbolReferences,
    tracker: PhaseTracker,
    limits: Option<DayLimits>,
}

pub(super) fn load_references(
    request: &MarketDayRequest,
    pre_open_only: bool,
) -> Result<LoadedReferences, ProductionError> {
    let path = raw_snapshot_reference_path(request);
    let file = File::open(&path).map_err(|error| ProductionError::io(&path, error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| ProductionError::parquet(&path, error))?;
    let projection = columns::projection(&builder, request.market, &path)?;
    let timestamp_parser =
        crate::production::time::MarketTimestampParser::new(request.trading_day)?;
    let reader = builder
        .with_projection(projection)
        .with_batch_size(request.batch_size)
        .build()
        .map_err(|error| ProductionError::parquet(&path, error))?;
    let opening_start =
        crate::production::parse_market_timestamp(request.trading_day, "09:25:00.000")?;
    let continuous_start =
        crate::production::parse_market_timestamp(request.trading_day, "09:30:00.000")?;
    let mut states = SymbolMap::<ReferenceLoadState>::default();
    let status_field = if request.market == Market::Sse {
        "InstruStatus"
    } else {
        "TradingPhaseCode"
    };
    for batch in reader {
        let batch = batch.map_err(|source| ProductionError::Arrow {
            context: "raw snapshot batch",
            source,
        })?;
        let columns = RawSnapshotColumns::bind(request.market, &path, &batch)?;
        let symbols = large_string(&path, &batch, "SecurityID")?;
        let times = large_string(&path, &batch, "UpdateTime")?;
        let statuses = large_string(&path, &batch, status_field)?;
        let rows = uint64(&path, &batch, "source_row_no")?;
        for row in 0..batch.num_rows() {
            if symbols.is_null(row)
                || times.is_null(row)
                || statuses.is_null(row)
                || rows.is_null(row)
            {
                return Err(ProductionError::Schema {
                    path: path.clone(),
                    detail: format!("null snapshot key at batch row {row}"),
                });
            }
            let symbol = symbols.value(row).trim();
            if !is_supported_symbol(request.market, symbol)
                || !request.targets.contains(request.market, symbol)
            {
                continue;
            }
            let time_ns = timestamp_parser.parse(times.value(row))?;
            let missing_reception = columns.missing_reception(row);
            let state = states
                .entry_ref(symbol)
                .or_insert_with(|| ReferenceLoadState {
                    references: SymbolReferences::default(),
                    tracker: PhaseTracker::new(request.market, request.trading_day, symbol)
                        .with_pre_open_only(pre_open_only),
                    limits: None,
                });
            if !pre_open_only
                && request.market == Market::Szse
                && !is_etf_symbol(request.market, symbol)
            {
                let values = columns.limits(row);
                state
                    .limits
                    .get_or_insert_with(DayLimits::default)
                    .observe_reference(values, missing_reception, || {
                        format!("{}#source_row_no={}", path.display(), rows.value(row))
                    });
            }
            let references = &mut state.references;
            let Some(anchor) = state.tracker.observe(
                request.market,
                (time_ns, rows.value(row)),
                statuses.value(row).trim(),
                opening_start,
                continuous_start,
            )?
            else {
                continue;
            };
            let (view, load_error) = load_raw_snapshot_view(&columns, &path, row)?;
            let candidate = ReferenceSnapshot {
                time_ns,
                view,
                load_error,
                source_row_no: rows.value(row),
                missing_reception,
                pre_close_price_units: if request.market == Market::Szse
                    && anchor == ValidationAnchor::MarketClose
                {
                    columns.pre_close(row)?
                } else {
                    None
                },
                state: AnchorState::default(),
            };
            match anchor {
                ValidationAnchor::ContinuousTrading => {
                    references.continuous_trading.push(candidate)
                }
                ValidationAnchor::PreOpen | ValidationAnchor::MarketClose => {
                    let target = if anchor == ValidationAnchor::PreOpen {
                        &mut references.pre_open
                    } else {
                        &mut references.market_close
                    };
                    *target = Some(candidate);
                }
            }
        }
    }
    let mut books = HashMap::with_capacity(states.len());
    let mut sz_close_limits = HashMap::new();
    let mut selection_audit = BTreeMap::new();
    for (symbol, state) in states {
        if let Some(limits) = state.limits {
            sz_close_limits.insert(symbol.clone(), limits);
        }
        selection_audit.insert(symbol.clone(), state.tracker.audit);
        books.insert(symbol, state.references);
    }
    // Duplicate periodic timestamps are source errors, not frames to silently deduplicate.
    for (symbol, references) in &mut books {
        references
            .continuous_trading
            .sort_by_key(|reference| reference.time_ns);
        let duplicates = references
            .continuous_trading
            .windows(2)
            .filter(|w| w[0].time_ns == w[1].time_ns)
            .map(|w| w[0].time_ns)
            .collect::<HashSet<_>>();
        for reference in &mut references.continuous_trading {
            if duplicates.contains(&reference.time_ns) {
                reference.load_error = Some(format!(
                    "duplicate continuous reference timestamp for {symbol}"
                ));
            }
        }
    }
    if let crate::production::TargetUniverse::Symbols(symbols) = &request.targets {
        for symbol in symbols {
            books.entry(symbol.to_string()).or_default();
        }
    }
    Ok(LoadedReferences {
        sz_close_limits,
        books,
        selection_audit,
    })
}

fn load_raw_snapshot_view(
    columns: &RawSnapshotColumns<'_>,
    path: &Path,
    row: usize,
) -> Result<(Option<ReferenceBookView>, Option<String>), ProductionError> {
    match columns.view(path, row) {
        Ok(view) => Ok((Some(view.try_into()?), None)),
        Err(ProductionError::Validation(detail)) => Ok((None, Some(detail))),
        Err(error) => Err(error),
    }
}

fn raw_snapshot_reference_path(request: &MarketDayRequest) -> PathBuf {
    request
        .raw_root
        .join(format!("date={}", request.trading_day.as_yyyymmdd()))
        .join(match request.market {
            Market::Sse => "MarketData",
            Market::Szse => "mdl_6_28_0",
        })
        .join("part-0.parquet")
}
