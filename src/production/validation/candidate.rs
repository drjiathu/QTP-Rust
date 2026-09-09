//! Candidate extraction is cached, never candidate eligibility or match timing.
use super::*;

#[derive(Clone, Debug, Default)]
pub(super) struct CandidateCache {
    // Owned by one symbol's validator for one BookKey lifecycle. Revision is
    // not a global identifier and must not be shared across book instances.
    revision: Option<u64>,
    view: Option<SnapshotBookView>,
    depth_ready: bool,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use crate::{
        AddOrder, ApplySequence, BookConfig, BookEvent, BookKey, ChannelId, CrossingBehavior,
        EventMeta, LocalTimestampNs, OrderId, OrderKey, Price, PriceScale, PricingInstruction,
        Quantity, RawSequence, Symbol, TradingDay,
    };

    fn book(market: Market, symbol: &str) -> OrderBook {
        OrderBook::new(BookConfig::new(
            BookKey {
                market,
                symbol: Symbol::from(symbol),
                trading_day: TradingDay::from_yyyymmdd(20260828).expect("date"),
            },
            PriceScale::from_decimal_places(4).expect("scale"),
        ))
    }

    fn add(book: &mut OrderBook, n: u64, crossing: CrossingBehavior) -> OrderKey {
        let key = OrderKey {
            channel_id: ChannelId::new(1).expect("channel"),
            side: Side::Buy,
            order_id: OrderId::new(n).expect("order"),
        };
        book.apply(BookEvent::AddOrder(AddOrder {
            meta: EventMeta {
                book_key: book.config().book_key.clone(),
                raw_sequence: RawSequence::new(n).expect("raw"),
                apply_sequence: ApplySequence::new(n).expect("apply"),
                local_time: LocalTimestampNs::from_nanos(10),
                quote_time: QuoteTimestampNs::from_nanos(10),
            },
            order_key: key,
            pricing: PricingInstruction::Provided(Price::from_units(100_000).expect("price")),
            crossing,
            quantity: Quantity::new(10).expect("quantity"),
        }))
        .expect("add");
        key
    }

    fn reference(view: SnapshotBookView) -> ReferenceSnapshot {
        ReferenceSnapshot {
            time_ns: 10,
            view: Some(view.try_into().expect("compact")),
            load_error: None,
            not_comparable_reason: None,
            pre_close_price_units: None,
            state: AnchorState::default(),
        }
    }

    fn context<'a>(book: &'a OrderBook, symbol: &'a str) -> CandidateContext<'a> {
        CandidateContext {
            market: book.config().book_key.market,
            symbol,
            book,
            limits: None,
            close_price: None,
        }
    }

    #[test]
    fn pending_reentry_invalidates_cache_without_advancing_event_metadata() {
        let mut book = book(Market::Szse, "000001");
        let key = add(&mut book, 1, CrossingBehavior::AlwaysHide);
        let mut target = book.clone();
        let price = Price::from_units(100_000).expect("price");
        target.rest_pending_order(key, price).expect("target");
        let mut reference = reference(SnapshotBookView::from_book(&target, 10).expect("view"));
        let mut cache = CandidateCache::default();
        let mut counters = CandidateCounters::default();
        compare_candidate(
            context(&book, "000001"),
            &mut reference,
            &mut cache,
            &mut counters,
            ValidationAnchor::ContinuousTrading,
            Some(10),
        )
        .expect("before");
        assert!(!reference.state.matched);
        let metadata = book.last_applied_meta().cloned();
        let revision = book.cache_revision();
        book.rest_pending_order(key, price).expect("reentry");
        assert_eq!(book.last_applied_meta(), metadata.as_ref());
        assert_ne!(book.cache_revision(), revision);
        compare_candidate(
            context(&book, "000001"),
            &mut reference,
            &mut cache,
            &mut counters,
            ValidationAnchor::ContinuousTrading,
            Some(11),
        )
        .expect("after");
        assert!(reference.state.matched);
        assert_eq!(reference.state.matched_candidate_time_ns, Some(11));
        assert_eq!(reference.state.matched_candidate_apply_sequence, Some(1));
        let ready = cache.revision;
        assert!(book.rest_pending_order(key, price).is_err());
        cache
            .prepare(context(&book, "000001"), &mut counters)
            .expect("failed mutation");
        assert_eq!(cache.revision, ready);
        assert!(cache.depth_ready);
    }

    #[test]
    fn cached_views_preserve_per_reference_first_match_and_close_isolation() {
        for (market, symbol) in [
            (Market::Sse, "600000"),
            (Market::Sse, "510300"),
            (Market::Szse, "000001"),
            (Market::Szse, "159915"),
        ] {
            let mut book = book(market, symbol);
            add(&mut book, 1, CrossingBehavior::Rest);
            let expected = SnapshotBookView::from_book(&book, 10).expect("view");
            let mut cache = CandidateCache::default();
            let mut counters = CandidateCounters::default();
            for time in [10, 11, 19] {
                let mut frame = reference(expected.clone());
                compare_candidate(
                    context(&book, symbol),
                    &mut frame,
                    &mut cache,
                    &mut counters,
                    ValidationAnchor::ContinuousTrading,
                    Some(time),
                )
                .expect("match");
                assert_eq!(frame.state.matched_candidate_time_ns, Some(time));
                assert_eq!(frame.state.matched_candidate_raw_sequence, Some(1));
            }
            #[cfg(feature = "profiling")]
            {
                assert_eq!(counters.depth_materializations, 1);
                assert_eq!(counters.candidate_cache_hits, 2);
            }
            let cached = cache.view.clone();
            // Close always gets an independent view, including SZ LastPrice
            // normalization, even if the underlying book revision is unchanged.
            let mut frame = reference(expected);
            let limits = DayLimits {
                values: Some((120_000, 80_000)),
                ..DayLimits::default()
            };
            let close_ctx = CandidateContext {
                limits: Some(&limits),
                ..context(&book, symbol)
            };
            compare_candidate(
                close_ctx,
                &mut frame,
                &mut cache,
                &mut counters,
                ValidationAnchor::MarketClose,
                Some(20),
            )
            .expect("close");
            assert!(frame.state.matched);
            assert_eq!(cache.view, cached);
            add(&mut book, 2, CrossingBehavior::Rest); // Same quote_time, different state.
            let mut frame = reference(SnapshotBookView::from_book(&book, 10).expect("new view"));
            compare_candidate(
                context(&book, symbol),
                &mut frame,
                &mut cache,
                &mut counters,
                ValidationAnchor::ContinuousTrading,
                Some(21),
            )
            .expect("new revision");
            assert!(frame.state.matched);
        }
    }

    #[test]
    fn direct_compact_mask_equals_materialized_comparison_for_every_field() {
        for (market, symbol) in [
            (Market::Sse, "600000"),
            (Market::Sse, "510300"),
            (Market::Szse, "000001"),
            (Market::Szse, "159915"),
        ] {
            let mut book = book(market, symbol);
            add(&mut book, 1, CrossingBehavior::Rest);
            let expected = SnapshotBookView::from_book(&book, 10).expect("view");
            let compact: ReferenceBookView = expected.clone().try_into().expect("compact");
            for field in 0..14 {
                let mut actual = expected.clone();
                match field {
                    0 => actual.bids[0].price_units += 1,
                    1 => actual.bids[0].quantity += 1,
                    2 => actual.bids[0].order_count = u64::from(u32::MAX) + 1,
                    3 => actual.asks.push(SnapshotLevel {
                        price_units: 110_000,
                        quantity: 1,
                        order_count: 1,
                    }),
                    4 => actual.total_bid_quantity += 1,
                    5 => actual.total_ask_quantity += 1,
                    6 => actual.weighted_bid_price_units = Some(999_999),
                    7 => actual.weighted_ask_price_units = Some(999_999),
                    8 => actual.last_price_units = Some(1),
                    9 => actual.high_price_units = Some(2),
                    10 => actual.low_price_units = Some(3),
                    11 => actual.trade_count += 1,
                    12 => actual.trade_quantity += 1,
                    _ => actual.turnover_units += 1,
                }
                let mut mask = compare_reference_scalars(market, symbol, &compact, &actual);
                mask.0 |= compact.depth_differences(&actual).0;
                assert_eq!(mask, compare_view_mask(market, symbol, &expected, &actual));
                assert_eq!(
                    mask.count(),
                    compare_views(market, symbol, &expected, &actual).len()
                );
            }
        }
    }
}

#[derive(Default)]
pub(super) struct CandidateCounters {
    #[cfg(feature = "profiling")]
    pub scalar_rejected_candidates: u64,
    #[cfg(feature = "profiling")]
    pub depth_materializations: u64,
    #[cfg(feature = "profiling")]
    pub candidate_cache_hits: u64,
}

#[derive(Clone, Copy)]
pub(super) struct CandidateContext<'a> {
    pub market: Market,
    pub symbol: &'a str,
    pub book: &'a OrderBook,
    pub limits: Option<&'a DayLimits>,
    pub close_price: Option<&'a SzClosePriceTracker>,
}

fn scalars(ctx: CandidateContext<'_>) -> Result<SnapshotBookView, ProductionError> {
    let stats = ctx.book.statistics();
    Ok(SnapshotBookView {
        bids: SnapshotLevels::new(),
        asks: SnapshotLevels::new(),
        total_bid_quantity: ctx.book.visible_aggregate(Side::Buy).0,
        total_ask_quantity: ctx.book.visible_aggregate(Side::Sell).0,
        weighted_bid_price_units: published_weighted_price(
            ctx.book,
            Side::Buy,
            ctx.market,
            ctx.symbol,
        )?,
        weighted_ask_price_units: published_weighted_price(
            ctx.book,
            Side::Sell,
            ctx.market,
            ctx.symbol,
        )?,
        last_price_units: stats.last_price.map(crate::Price::units),
        high_price_units: stats.high_price.map(crate::Price::units),
        low_price_units: stats.low_price.map(crate::Price::units),
        trade_count: stats.trade_count,
        trade_quantity: stats.total_quantity,
        turnover_units: stats.total_turnover_units,
    })
}

impl CandidateCache {
    fn prepare(
        &mut self,
        ctx: CandidateContext<'_>,
        counters: &mut CandidateCounters,
    ) -> Result<(), ProductionError> {
        let revision = ctx.book.cache_revision();
        if revision.is_some() && self.revision == revision && self.view.is_some() {
            #[cfg(feature = "profiling")]
            {
                counters.candidate_cache_hits += 1;
            }
        } else {
            self.view = Some(scalars(ctx)?);
            self.revision = revision;
            self.depth_ready = false;
        }
        let _ = counters;
        Ok(())
    }
}

pub(super) fn compare_candidate(
    ctx: CandidateContext<'_>,
    reference: &mut ReferenceSnapshot,
    cache: &mut CandidateCache,
    counters: &mut CandidateCounters,
    anchor: ValidationAnchor,
    candidate_time_ns: Option<i64>,
) -> Result<(), ProductionError> {
    if reference.not_comparable_reason.is_some()
        || reference.state.matched
        || reference.state.finalized
    {
        return Ok(());
    }
    let Some(expected) = reference.view.as_ref() else {
        return Ok(());
    };
    // Close projections and closing-price reconciliation have independent
    // audit state. They neither use nor modify the ordinary candidate cache.
    let is_close = anchor == ValidationAnchor::MarketClose;
    let projection =
        if is_close && ctx.market == Market::Szse && !is_etf_symbol(ctx.market, ctx.symbol) {
            let missing = DayLimits::default();
            let limits = ctx.limits.unwrap_or(&missing);
            match limits.unlimited() {
                Ok(false) => Ok(None),
                Ok(true) => ctx
                    .close_price
                    .and_then(|tracker| tracker.range_base.as_ref())
                    .ok_or_else(|| "missing pre-14:57 successful trade for SZ E0 range".to_owned())
                    .and_then(|base| {
                        close_range::project(ctx.book, base, limits)
                            .map(Some)
                            .map_err(|e| e.to_string())
                    }),
                Err(error) => Err(error),
            }
        } else {
            Ok(None)
        };
    let (mut close_view, close_price_band, projected) = match projection {
        Ok(Some((view, audit))) => (Some(view), Some(audit), true),
        Ok(None) if is_close => (Some(scalars(ctx)?), None, false),
        Ok(None) => (None, None, false),
        Err(error) => {
            reference.state.diagnostics_mut().comparison_error = Some(error);
            reference.state.finalized = true;
            return Ok(());
        }
    };
    if !is_close {
        cache.prepare(ctx, counters)?;
    }
    let actual = if let Some(view) = close_view.as_mut() {
        view
    } else {
        cache
            .view
            .as_mut()
            .ok_or(ProductionError::Arithmetic("missing candidate cache"))?
    };
    let mut mask = compare_reference_scalars(ctx.market, ctx.symbol, expected, actual);
    if !is_close
        && !mask.is_empty()
        && reference
            .state
            .best_differences()
            .is_some_and(|best| mask.count() >= best.len())
    {
        #[cfg(feature = "profiling")]
        {
            counters.scalar_rejected_candidates += 1;
        }
        return Ok(());
    }
    if is_close || !cache.depth_ready {
        if !projected {
            actual.fill_depth(ctx.book, 10)?;
        }
        #[cfg(feature = "profiling")]
        {
            counters.depth_materializations += 1;
        }
        if !is_close {
            cache.depth_ready = true;
        }
    }
    mask.0 |= expected.depth_differences(actual).0;
    let match_tag = if is_close && ctx.market == Market::Szse {
        reconcile_sz_market_close_price(
            ctx.symbol,
            reference.pre_close_price_units,
            ctx.close_price,
            &expected.expand(),
            actual,
            mask,
        )?
    } else {
        None
    };
    if match_tag.is_some() {
        mask = compare_reference_scalars(ctx.market, ctx.symbol, expected, actual);
        mask.0 |= expected.depth_differences(actual).0;
    }
    let best = if !mask.is_empty()
        && reference
            .state
            .best_differences()
            .is_none_or(|best| mask.count() < best.len())
    {
        Some(compare_views(
            ctx.market,
            ctx.symbol,
            &expected.expand(),
            actual,
        ))
    } else {
        None
    };
    let state = &mut reference.state;
    if close_price_band.is_some() {
        state.diagnostics_mut().close_price_band = close_price_band;
    }
    if mask.is_empty() {
        state.matched = true;
        state.finalized = true;
        if let Some(d) = state.diagnostics.as_mut() {
            d.best_differences = None;
        }
        if state
            .diagnostics
            .as_ref()
            .is_some_and(|d| d.close_price_band.is_none() && d.comparison_error.is_none())
        {
            state.diagnostics = None;
        }
        state.matched_candidate_time_ns = candidate_time_ns;
        state.matched_candidate_raw_sequence =
            ctx.book.last_applied_meta().map(|m| m.raw_sequence.get());
        state.matched_candidate_apply_sequence =
            ctx.book.last_applied_meta().map(|m| m.apply_sequence.get());
        state.match_tag = match_tag;
    } else if let Some(best) = best {
        state.diagnostics_mut().best_differences = Some(best);
        state.best_candidate_time_ns = candidate_time_ns;
        state.best_candidate_raw_sequence =
            ctx.book.last_applied_meta().map(|m| m.raw_sequence.get());
    }
    Ok(())
}

pub(super) struct WindowConfig {
    pub pre_open_only: bool,
    pub lookback_ns: i64,
    pub lookahead_ns: i64,
}

impl SymbolValidationState {
    pub fn observe_candidates(
        &mut self,
        ctx: CandidateContext<'_>,
        point: ObservationPoint,
        windows: WindowConfig,
        counters: &mut CandidateCounters,
    ) -> Result<(), ProductionError> {
        let ctx = CandidateContext {
            limits: self.limits.as_ref(),
            close_price: self.close_price.as_ref(),
            ..ctx
        };
        let timed = match point {
            ObservationPoint::BeforeEvent(q) => Some((q, true)),
            ObservationPoint::AfterEvent(q) => Some((q, false)),
            _ => None,
        };
        if let Some(reference) = self.references.pre_open.as_mut() {
            let end = reference
                .time_ns
                .checked_add(REFERENCE_SECOND_NS)
                .ok_or(ProductionError::Arithmetic("reference candidate window"))?;
            if let Some((q, before)) = timed {
                if q >= reference.time_ns && (q < end || before) {
                    let time = if q >= end {
                        end - REFERENCE_MILLISECOND_NS
                    } else {
                        q
                    };
                    compare_candidate(
                        ctx,
                        reference,
                        &mut self.cache,
                        counters,
                        ValidationAnchor::PreOpen,
                        Some(time),
                    )?;
                }
                if q >= end {
                    reference.state.finalized = true;
                }
            } else if point == ObservationPoint::ChannelFinished {
                compare_candidate(
                    ctx,
                    reference,
                    &mut self.cache,
                    counters,
                    ValidationAnchor::PreOpen,
                    Some(end - REFERENCE_MILLISECOND_NS),
                )?;
                reference.state.finalized = true;
            }
        }
        if windows.pre_open_only {
            return Ok(());
        }
        if let Some((q, before)) = timed {
            let mut first_pending = None;
            let mut position = self.continuous_position;
            for reference in &mut self.references.continuous_trading[self.continuous_position..] {
                let start = reference.time_ns.checked_sub(windows.lookback_ns).ok_or(
                    ProductionError::Arithmetic("continuous reference window start"),
                )?;
                if q < start {
                    break;
                }
                let end = reference
                    .time_ns
                    .checked_add(windows.lookahead_ns)
                    .ok_or(ProductionError::Arithmetic("continuous reference second"))?;
                if q < end || before {
                    let time = if q >= end {
                        end - REFERENCE_MILLISECOND_NS
                    } else {
                        q
                    };
                    compare_candidate(
                        ctx,
                        reference,
                        &mut self.cache,
                        counters,
                        ValidationAnchor::ContinuousTrading,
                        Some(time),
                    )?;
                }
                if q >= end {
                    reference.state.finalized = true;
                }
                if !reference.state.finalized {
                    first_pending.get_or_insert(position);
                }
                position += 1;
            }
            self.continuous_position = first_pending.unwrap_or(position);
        } else if let ObservationPoint::MarketClose(boundary) = point {
            if let Some(reference) = self.references.market_close.as_mut() {
                compare_candidate(
                    ctx,
                    reference,
                    &mut self.cache,
                    counters,
                    ValidationAnchor::MarketClose,
                    Some(boundary),
                )?;
                reference.state.finalized = true;
            }
        } else if point == ObservationPoint::ChannelFinished {
            for reference in &mut self.references.continuous_trading[self.continuous_position..] {
                let time = reference
                    .time_ns
                    .checked_add(windows.lookahead_ns - REFERENCE_MILLISECOND_NS)
                    .ok_or(ProductionError::Arithmetic(
                        "continuous reference candidate time",
                    ))?;
                compare_candidate(
                    ctx,
                    reference,
                    &mut self.cache,
                    counters,
                    ValidationAnchor::ContinuousTrading,
                    Some(time),
                )?;
                reference.state.finalized = true;
            }
            self.continuous_position = self.references.continuous_trading.len();
        }
        Ok(())
    }
}
