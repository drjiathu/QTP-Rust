use std::collections::{BTreeMap, HashMap, HashSet};

use crate::market_data::{
    AddOrder, ApplySequence, BookEvent, BookKey, CrossingBehavior, EventMeta, OrderCancel,
    OrderKey, OrderReference, Price, PriceScale, PricingInstruction, Side, Trade,
};

use super::error::BookError;
use super::state::{
    OrderHandle, OrderLocation, OrderState, OrderStorage, PriceLevelState, TradeStatistics,
    side_matches,
};
use super::view::{
    ApplyOutcome, BookSummary, DepthView, LevelView, OrderView, TradeStatisticsView,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UnknownTradePolicy {
    #[default]
    Reject,
    UpdateKnownAndStatistics,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookConfig {
    pub book_key: BookKey,
    pub price_scale: PriceScale,
    pub unknown_trade_policy: UnknownTradePolicy,
}

impl BookConfig {
    #[must_use]
    pub const fn new(book_key: BookKey, price_scale: PriceScale) -> Self {
        Self {
            book_key,
            price_scale,
            unknown_trade_policy: UnknownTradePolicy::Reject,
        }
    }

    #[must_use]
    pub const fn with_unknown_trade_policy(mut self, policy: UnknownTradePolicy) -> Self {
        self.unknown_trade_policy = policy;
        self
    }
}

#[derive(Clone, Debug)]
pub struct OrderBook {
    config: BookConfig,
    bids: BTreeMap<Price, PriceLevelState>,
    asks: BTreeMap<Price, PriceLevelState>,
    active_by_key: HashMap<OrderKey, OrderHandle>,
    seen_order_keys: HashSet<OrderKey>,
    orders: OrderStorage,
    bid_aggregate: SideAggregate,
    ask_aggregate: SideAggregate,
    statistics: TradeStatistics,
    last_applied_meta: Option<EventMeta>,
    // A saturated revision disables observer caching instead of wrapping or
    // introducing a new business failure. Includes non-event pending reentry.
    revision: u64,
}

#[derive(Clone, Debug)]
struct KnownTradePlan {
    handle: OrderHandle,
    state: OrderState,
    new_remaining: u64,
    new_effective_price: Option<Price>,
    side_aggregate: Option<SideAggregate>,
    reenter: bool,
}

#[derive(Clone, Debug)]
struct AttachPlan {
    total_quantity: u64,
    order_count: usize,
    previous: Option<OrderHandle>,
    side_aggregate: SideAggregate,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SideAggregate {
    total_quantity: u64,
    weighted_price_quantity: u128,
}

struct TradeReferenceResolution {
    handle: Option<OrderHandle>,
    unknown: Option<(Side, crate::market_data::OrderId)>,
}

impl OrderBook {
    #[must_use]
    pub fn new(config: BookConfig) -> Self {
        Self {
            config,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            active_by_key: HashMap::new(),
            seen_order_keys: HashSet::new(),
            orders: OrderStorage::with_key(),
            bid_aggregate: SideAggregate::default(),
            ask_aggregate: SideAggregate::default(),
            statistics: TradeStatistics::default(),
            last_applied_meta: None,
            revision: 0,
        }
    }

    #[must_use]
    pub const fn config(&self) -> &BookConfig {
        &self.config
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty() && self.active_by_key.is_empty()
    }

    #[must_use]
    pub const fn last_applied_meta(&self) -> Option<&EventMeta> {
        self.last_applied_meta.as_ref()
    }

    pub(crate) fn cache_revision(&self) -> Option<u64> {
        (self.revision != u64::MAX).then_some(self.revision)
    }

    pub fn next_apply_sequence(&self) -> Result<ApplySequence, BookError> {
        let value = match &self.last_applied_meta {
            None => 1,
            Some(meta) => meta
                .apply_sequence
                .get()
                .checked_add(1)
                .ok_or(BookError::ApplySequenceOverflow)?,
        };
        ApplySequence::new(value).ok_or(BookError::ApplySequenceOverflow)
    }

    pub fn apply(&mut self, event: BookEvent) -> Result<ApplyOutcome, BookError> {
        self.validate_meta(event.meta())?;
        let meta = event.meta().clone();
        let outcome = match event {
            BookEvent::AddOrder(event) => self.apply_add(event),
            BookEvent::OrderCancel(event) => self.apply_cancel(event),
            BookEvent::Trade(event) => self.apply_trade(event),
        }?;
        self.last_applied_meta = Some(meta);
        self.revision = self.revision.saturating_add(1);
        debug_assert!(self.check_invariants().is_ok());
        Ok(outcome)
    }

    /// Adapter-only completion of a buffered execution group. No synthetic raw
    /// event or clock update: the group's last real event remains the metadata.
    pub(crate) fn rest_pending_order(
        &mut self,
        key: OrderKey,
        price: Price,
    ) -> Result<(), BookError> {
        let handle = *self
            .active_by_key
            .get(&key)
            .ok_or(BookError::UnknownCancellation(key))?;
        let state = self
            .orders
            .get(handle)
            .ok_or(BookError::InvariantViolation("missing pending order"))?;
        if state.location != OrderLocation::Aggressive || state.allow_reentry {
            return Err(BookError::InvariantViolation(
                "order is not explicitly pending",
            ));
        }
        if self.crosses_opposite(key.side, price) {
            return Err(BookError::InvariantViolation(
                "pending remainder still crosses",
            ));
        }
        let attach = self.validate_attach(key.side, price, state.remaining_quantity)?;
        let state = self
            .orders
            .get_mut(handle)
            .ok_or(BookError::InvariantViolation("missing pending order"))?;
        state.effective_price = Some(price);
        state.location = OrderLocation::Resting;
        state.previous = attach.previous;
        self.commit_attach(key.side, price, handle, attach);
        self.revision = self.revision.saturating_add(1);
        debug_assert!(self.check_invariants().is_ok());
        Ok(())
    }

    fn validate_meta(&self, meta: &EventMeta) -> Result<(), BookError> {
        if meta.book_key != self.config.book_key {
            return Err(BookError::BookKeyMismatch {
                expected: self.config.book_key.clone(),
                actual: meta.book_key.clone(),
            });
        }
        let expected = self.next_apply_sequence()?;
        if meta.apply_sequence != expected {
            return Err(BookError::InvalidApplySequence {
                expected,
                actual: meta.apply_sequence,
            });
        }
        Ok(())
    }

    fn apply_add(&mut self, event: AddOrder) -> Result<ApplyOutcome, BookError> {
        if self.seen_order_keys.contains(&event.order_key) {
            return Err(BookError::DuplicateOrder(event.order_key));
        }
        let price = self.resolve_price(event.order_key.side, event.pricing)?;
        let hidden = matches!(
            event.crossing,
            CrossingBehavior::AlwaysHide | CrossingBehavior::RestAtLastTradePrice
        ) || (event.crossing == CrossingBehavior::HideIfCrossing
            && price.is_some_and(|price| self.crosses_opposite(event.order_key.side, price)));
        let location = if hidden {
            OrderLocation::Aggressive
        } else {
            OrderLocation::Resting
        };
        let attach = if location == OrderLocation::Resting {
            let price = price.ok_or(BookError::UnpricedVisibleOrder)?;
            Some(self.validate_attach(event.order_key.side, price, event.quantity.get())?)
        } else {
            None
        };

        let state = OrderState {
            key: event.order_key,
            effective_price: price,
            original_quantity: event.quantity,
            remaining_quantity: event.quantity.get(),
            previous: attach.as_ref().and_then(|plan| plan.previous),
            next: None,
            location,
            allow_reentry: matches!(
                event.crossing,
                CrossingBehavior::HideIfCrossing | CrossingBehavior::RestAtLastTradePrice
            ),
            reprice_from_trade: event.crossing == CrossingBehavior::RestAtLastTradePrice,
        };
        let handle = self.orders.insert(state);
        if let Some(plan) = attach {
            let price = price.ok_or(BookError::UnpricedVisibleOrder)?;
            self.commit_attach(event.order_key.side, price, handle, plan);
        }
        self.active_by_key.insert(event.order_key, handle);
        self.seen_order_keys.insert(event.order_key);

        Ok(ApplyOutcome::Added {
            key: event.order_key,
            effective_price: price,
            quantity: event.quantity,
            location,
        })
    }

    fn apply_cancel(&mut self, event: OrderCancel) -> Result<ApplyOutcome, BookError> {
        let handle = self
            .active_by_key
            .get(&event.order_key)
            .copied()
            .ok_or(BookError::UnknownCancellation(event.order_key))?;
        let state = self
            .orders
            .get(handle)
            .cloned()
            .ok_or(BookError::InvariantViolation("active handle is missing"))?;
        let side_aggregate = (state.location == OrderLocation::Resting)
            .then(|| self.validate_detach(handle, &state))
            .transpose()?;
        let cancelled_quantity = state.remaining_quantity;
        if let Some(side_aggregate) = side_aggregate {
            self.commit_detach(handle, &state, side_aggregate);
        }
        self.active_by_key.remove(&event.order_key);
        self.orders.remove(handle);
        Ok(ApplyOutcome::Cancelled {
            key: event.order_key,
            cancelled_quantity,
        })
    }

    fn apply_trade(&mut self, event: Trade) -> Result<ApplyOutcome, BookError> {
        let bid_resolution = self.resolve_trade_reference(event.bid_order, Side::Buy)?;
        let ask_resolution = self.resolve_trade_reference(event.ask_order, Side::Sell)?;
        if self.config.unknown_trade_policy == UnknownTradePolicy::Reject {
            if let Some((side, order_id)) = bid_resolution.unknown.or(ask_resolution.unknown) {
                return Err(BookError::UnknownTradeReference { side, order_id });
            }
        }

        let mut bid_plan = match bid_resolution.handle {
            Some(handle) => Some(self.build_trade_plan(handle, event.quantity.get())?),
            None => None,
        };
        let mut ask_plan = match ask_resolution.handle {
            Some(handle) => Some(self.build_trade_plan(handle, event.quantity.get())?),
            None => None,
        };

        for plan in [&mut bid_plan, &mut ask_plan].into_iter().flatten() {
            if plan.state.location == OrderLocation::Aggressive
                && plan.state.reprice_from_trade
                && plan.new_remaining > 0
            {
                plan.new_effective_price = Some(event.price);
            }
        }

        let statistics = self.validate_trade_statistics(event.price, event.quantity.get())?;
        let post_best_ask = self.best_after_trade(Side::Sell, ask_plan.as_ref());
        if let Some(plan) = &mut bid_plan {
            plan.reenter = plan.state.location == OrderLocation::Aggressive
                && plan.state.allow_reentry
                && plan.new_remaining > 0
                && plan
                    .new_effective_price
                    .is_some_and(|price| post_best_ask.is_none_or(|best| price < best));
        }
        let mut post_best_bid = self.best_after_trade(Side::Buy, bid_plan.as_ref());
        if bid_plan.as_ref().is_some_and(|plan| plan.reenter) {
            let bid_price = bid_plan
                .as_ref()
                .and_then(|plan| plan.new_effective_price)
                .ok_or(BookError::InvariantViolation("missing bid reentry plan"))?;
            post_best_bid = Some(post_best_bid.map_or(bid_price, |best| best.max(bid_price)));
        }
        if let Some(plan) = &mut ask_plan {
            plan.reenter = plan.state.location == OrderLocation::Aggressive
                && plan.state.allow_reentry
                && plan.new_remaining > 0
                && plan
                    .new_effective_price
                    .is_some_and(|price| post_best_bid.is_none_or(|best| price > best));
        }

        let bid_attach = self.validate_reentry(bid_plan.as_ref())?;
        let ask_attach = self.validate_reentry(ask_plan.as_ref())?;

        self.statistics = statistics;
        if let Some(plan) = &bid_plan {
            self.commit_trade_plan(plan, event.quantity.get());
        }
        if let Some(plan) = &ask_plan {
            self.commit_trade_plan(plan, event.quantity.get());
        }
        if let (Some(plan), Some(attach)) = (&bid_plan, bid_attach) {
            self.commit_reentry(plan, attach);
        }
        if let (Some(plan), Some(attach)) = (&ask_plan, ask_attach) {
            self.commit_reentry(plan, attach);
        }

        Ok(ApplyOutcome::Traded {
            bid_reduction: bid_plan.as_ref().map_or(0, |_| event.quantity.get()),
            ask_reduction: ask_plan.as_ref().map_or(0, |_| event.quantity.get()),
        })
    }

    fn resolve_price(
        &self,
        side: Side,
        instruction: PricingInstruction,
    ) -> Result<Option<Price>, BookError> {
        match instruction {
            PricingInstruction::Provided(price) => Ok(Some(price)),
            PricingInstruction::SameSideBest => self
                .best_price(side)
                .ok_or(BookError::ReferencePriceUnavailable {
                    side,
                    instruction: "same-side best",
                })
                .map(Some),
            PricingInstruction::OppositeBest => self
                .best_price(opposite(side))
                .ok_or(BookError::ReferencePriceUnavailable {
                    side,
                    instruction: "opposite best",
                })
                .map(Some),
            PricingInstruction::Unpriced => Ok(None),
        }
    }

    fn crosses_opposite(&self, side: Side, price: Price) -> bool {
        match (side, self.best_price(opposite(side))) {
            (Side::Buy, Some(best_ask)) => price >= best_ask,
            (Side::Sell, Some(best_bid)) => price <= best_bid,
            (_, None) => false,
        }
    }

    fn resolve_trade_reference(
        &self,
        reference: OrderReference,
        expected_side: Side,
    ) -> Result<TradeReferenceResolution, BookError> {
        match reference {
            OrderReference::Absent => Ok(TradeReferenceResolution {
                handle: None,
                unknown: None,
            }),
            OrderReference::Unresolved { side, order_id } => {
                validate_reference_side(expected_side, side)?;
                Ok(TradeReferenceResolution {
                    handle: None,
                    unknown: Some((side, order_id)),
                })
            }
            OrderReference::Resolved(key) => {
                validate_reference_side(expected_side, key.side)?;
                match self.active_by_key.get(&key).copied() {
                    Some(handle) => Ok(TradeReferenceResolution {
                        handle: Some(handle),
                        unknown: None,
                    }),
                    None => Ok(TradeReferenceResolution {
                        handle: None,
                        unknown: Some((key.side, key.order_id)),
                    }),
                }
            }
        }
    }

    fn build_trade_plan(
        &self,
        handle: OrderHandle,
        trade_quantity: u64,
    ) -> Result<KnownTradePlan, BookError> {
        let state = self
            .orders
            .get(handle)
            .cloned()
            .ok_or(BookError::InvariantViolation("trade handle is missing"))?;
        let new_remaining = state.remaining_quantity.checked_sub(trade_quantity).ok_or(
            BookError::TradeOverfill {
                key: state.key,
                remaining: state.remaining_quantity,
                trade: trade_quantity,
            },
        )?;
        let side_aggregate = if state.location == OrderLocation::Resting {
            let price = state.effective_price.ok_or(BookError::InvariantViolation(
                "resting order has no effective price",
            ))?;
            if new_remaining == 0 {
                self.validate_detach(handle, &state)?;
            } else {
                let level =
                    self.level(state.key.side, price)
                        .ok_or(BookError::MissingPriceLevel {
                            side: state.key.side,
                            price,
                        })?;
                level
                    .total_quantity
                    .checked_sub(trade_quantity)
                    .ok_or(BookError::ArithmeticUnderflow("price-level quantity"))?;
            }
            Some(self.validate_aggregate_sub(state.key.side, price, trade_quantity)?)
        } else {
            None
        };
        Ok(KnownTradePlan {
            handle,
            new_effective_price: state.effective_price,
            side_aggregate,
            state,
            new_remaining,
            reenter: false,
        })
    }

    fn validate_trade_statistics(
        &self,
        price: Price,
        quantity: u64,
    ) -> Result<TradeStatistics, BookError> {
        let mut statistics = self.statistics.clone();
        statistics.total_quantity = statistics
            .total_quantity
            .checked_add(quantity)
            .ok_or(BookError::ArithmeticOverflow("total trade quantity"))?;
        let turnover = u128::try_from(price.units())
            .map_err(|_| BookError::ArithmeticOverflow("trade turnover"))?
            .checked_mul(u128::from(quantity))
            .ok_or(BookError::ArithmeticOverflow("trade turnover"))?;
        statistics.total_turnover_units = statistics
            .total_turnover_units
            .checked_add(turnover)
            .ok_or(BookError::ArithmeticOverflow("total trade turnover"))?;
        statistics.trade_count = statistics
            .trade_count
            .checked_add(1)
            .ok_or(BookError::ArithmeticOverflow("trade count"))?;
        statistics.last_price = Some(price);
        statistics.high_price = Some(
            statistics
                .high_price
                .map_or(price, |value| value.max(price)),
        );
        statistics.low_price = Some(statistics.low_price.map_or(price, |value| value.min(price)));
        Ok(statistics)
    }

    fn validate_attach(
        &self,
        side: Side,
        price: Price,
        quantity: u64,
    ) -> Result<AttachPlan, BookError> {
        let side_aggregate = self.validate_aggregate_add(side, price, quantity)?;
        match self.level(side, price) {
            None => Ok(AttachPlan {
                total_quantity: quantity,
                order_count: 1,
                previous: None,
                side_aggregate,
            }),
            Some(level) => {
                if let Some(tail) = level.tail {
                    if self.orders.get(tail).is_none() {
                        return Err(BookError::InvariantViolation("level tail is missing"));
                    }
                } else {
                    return Err(BookError::InvariantViolation("non-empty level has no tail"));
                }
                Ok(AttachPlan {
                    total_quantity: level
                        .total_quantity
                        .checked_add(quantity)
                        .ok_or(BookError::ArithmeticOverflow("price-level quantity"))?,
                    order_count: level
                        .order_count
                        .checked_add(1)
                        .ok_or(BookError::ArithmeticOverflow("price-level order count"))?,
                    previous: level.tail,
                    side_aggregate,
                })
            }
        }
    }

    fn commit_attach(&mut self, side: Side, price: Price, handle: OrderHandle, plan: AttachPlan) {
        *self.side_aggregate_mut(side) = plan.side_aggregate;
        if let Some(previous) = plan.previous {
            if let Some(previous_state) = self.orders.get_mut(previous) {
                previous_state.next = Some(handle);
            }
        }
        let level = self.level_map_mut(side).entry(price).or_default();
        if level.head.is_none() {
            level.head = Some(handle);
        }
        level.tail = Some(handle);
        level.total_quantity = plan.total_quantity;
        level.order_count = plan.order_count;
    }

    fn validate_detach(
        &self,
        handle: OrderHandle,
        state: &OrderState,
    ) -> Result<SideAggregate, BookError> {
        let price = state.effective_price.ok_or(BookError::InvariantViolation(
            "resting order has no effective price",
        ))?;
        let level = self
            .level(state.key.side, price)
            .ok_or(BookError::MissingPriceLevel {
                side: state.key.side,
                price,
            })?;
        if level.total_quantity < state.remaining_quantity || level.order_count == 0 {
            return Err(BookError::InvariantViolation("invalid level aggregate"));
        }
        match state.previous {
            Some(previous) => {
                let previous_state = self
                    .orders
                    .get(previous)
                    .ok_or(BookError::InvariantViolation("previous order is missing"))?;
                if previous_state.next != Some(handle) {
                    return Err(BookError::InvariantViolation("broken previous link"));
                }
            }
            None if level.head != Some(handle) => {
                return Err(BookError::InvariantViolation("level head mismatch"));
            }
            None => {}
        }
        match state.next {
            Some(next) => {
                let next_state = self
                    .orders
                    .get(next)
                    .ok_or(BookError::InvariantViolation("next order is missing"))?;
                if next_state.previous != Some(handle) {
                    return Err(BookError::InvariantViolation("broken next link"));
                }
            }
            None if level.tail != Some(handle) => {
                return Err(BookError::InvariantViolation("level tail mismatch"));
            }
            None => {}
        }
        self.validate_aggregate_sub(state.key.side, price, state.remaining_quantity)
    }

    fn commit_detach(
        &mut self,
        _handle: OrderHandle,
        state: &OrderState,
        side_aggregate: SideAggregate,
    ) {
        let Some(price) = state.effective_price else {
            return;
        };
        if let Some(previous) = state.previous {
            if let Some(previous_state) = self.orders.get_mut(previous) {
                previous_state.next = state.next;
            }
        }
        if let Some(next) = state.next {
            if let Some(next_state) = self.orders.get_mut(next) {
                next_state.previous = state.previous;
            }
        }

        let remove_level;
        {
            let Some(level) = self.level_map_mut(state.key.side).get_mut(&price) else {
                return;
            };
            level.total_quantity -= state.remaining_quantity;
            level.order_count -= 1;
            if state.previous.is_none() {
                level.head = state.next;
            }
            if state.next.is_none() {
                level.tail = state.previous;
            }
            remove_level = level.order_count == 0;
        }
        if remove_level {
            self.level_map_mut(state.key.side).remove(&price);
        }
        *self.side_aggregate_mut(state.key.side) = side_aggregate;
    }

    fn best_after_trade(&self, side: Side, plan: Option<&KnownTradePlan>) -> Option<Price> {
        let levels = self.level_map(side);
        let visible_reduction = plan.and_then(|plan| {
            (plan.state.location == OrderLocation::Resting)
                .then_some(plan)
                .and_then(|plan| {
                    plan.state
                        .effective_price
                        .map(|price| (price, plan.state.remaining_quantity - plan.new_remaining))
                })
        });
        match side {
            Side::Buy => levels.iter().rev().find_map(|(price, level)| {
                let reduction = visible_reduction
                    .filter(|(reduction_price, _)| reduction_price == price)
                    .map_or(0, |(_, quantity)| quantity);
                (level.total_quantity > reduction).then_some(*price)
            }),
            Side::Sell => levels.iter().find_map(|(price, level)| {
                let reduction = visible_reduction
                    .filter(|(reduction_price, _)| reduction_price == price)
                    .map_or(0, |(_, quantity)| quantity);
                (level.total_quantity > reduction).then_some(*price)
            }),
        }
    }

    fn validate_reentry(
        &self,
        plan: Option<&KnownTradePlan>,
    ) -> Result<Option<AttachPlan>, BookError> {
        match plan {
            Some(plan) if plan.reenter => {
                let price = plan
                    .new_effective_price
                    .ok_or(BookError::InvariantViolation(
                        "re-entering order has no effective price",
                    ))?;
                self.validate_attach(plan.state.key.side, price, plan.new_remaining)
                    .map(Some)
            }
            _ => Ok(None),
        }
    }

    fn commit_trade_plan(&mut self, plan: &KnownTradePlan, trade_quantity: u64) {
        if plan.state.location == OrderLocation::Resting {
            if plan.new_remaining == 0 {
                if let Some(aggregate) = plan.side_aggregate {
                    self.commit_detach(plan.handle, &plan.state, aggregate);
                }
            } else {
                if let Some(price) = plan.state.effective_price {
                    if let Some(level) = self.level_map_mut(plan.state.key.side).get_mut(&price) {
                        level.total_quantity -= trade_quantity;
                    }
                }
                if let Some(state) = self.orders.get_mut(plan.handle) {
                    state.remaining_quantity = plan.new_remaining;
                }
                if let Some(aggregate) = plan.side_aggregate {
                    *self.side_aggregate_mut(plan.state.key.side) = aggregate;
                }
            }
        } else if let Some(state) = self.orders.get_mut(plan.handle) {
            state.remaining_quantity = plan.new_remaining;
            state.effective_price = plan.new_effective_price;
        }

        if plan.new_remaining == 0 {
            self.active_by_key.remove(&plan.state.key);
            self.orders.remove(plan.handle);
        }
    }

    fn commit_reentry(&mut self, plan: &KnownTradePlan, attach: AttachPlan) {
        let Some(price) = plan.new_effective_price else {
            return;
        };
        if let Some(state) = self.orders.get_mut(plan.handle) {
            state.effective_price = Some(price);
            state.remaining_quantity = plan.new_remaining;
            state.location = OrderLocation::Resting;
            state.previous = attach.previous;
            state.next = None;
        }
        self.commit_attach(plan.state.key.side, price, plan.handle, attach);
    }

    fn best_price(&self, side: Side) -> Option<Price> {
        match side {
            Side::Buy => self.bids.last_key_value().map(|(price, _)| *price),
            Side::Sell => self.asks.first_key_value().map(|(price, _)| *price),
        }
    }

    fn level_map(&self, side: Side) -> &BTreeMap<Price, PriceLevelState> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn level_map_mut(&mut self, side: Side) -> &mut BTreeMap<Price, PriceLevelState> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn side_aggregate(&self, side: Side) -> SideAggregate {
        match side {
            Side::Buy => self.bid_aggregate,
            Side::Sell => self.ask_aggregate,
        }
    }

    fn side_aggregate_mut(&mut self, side: Side) -> &mut SideAggregate {
        match side {
            Side::Buy => &mut self.bid_aggregate,
            Side::Sell => &mut self.ask_aggregate,
        }
    }

    fn validate_aggregate_add(
        &self,
        side: Side,
        price: Price,
        quantity: u64,
    ) -> Result<SideAggregate, BookError> {
        let aggregate = self.side_aggregate(side);
        let weighted = price_quantity(price, quantity)?;
        Ok(SideAggregate {
            total_quantity: aggregate
                .total_quantity
                .checked_add(quantity)
                .ok_or(BookError::ArithmeticOverflow("side quantity"))?,
            weighted_price_quantity: aggregate
                .weighted_price_quantity
                .checked_add(weighted)
                .ok_or(BookError::ArithmeticOverflow("side weighted price"))?,
        })
    }

    fn validate_aggregate_sub(
        &self,
        side: Side,
        price: Price,
        quantity: u64,
    ) -> Result<SideAggregate, BookError> {
        let aggregate = self.side_aggregate(side);
        let weighted = price_quantity(price, quantity)?;
        Ok(SideAggregate {
            total_quantity: aggregate
                .total_quantity
                .checked_sub(quantity)
                .ok_or(BookError::ArithmeticUnderflow("side quantity"))?,
            weighted_price_quantity: aggregate
                .weighted_price_quantity
                .checked_sub(weighted)
                .ok_or(BookError::ArithmeticUnderflow("side weighted price"))?,
        })
    }

    pub(crate) fn visible_aggregate(&self, side: Side) -> (u64, u128) {
        let aggregate = self.side_aggregate(side);
        (aggregate.total_quantity, aggregate.weighted_price_quantity)
    }

    fn level(&self, side: Side, price: Price) -> Option<&PriceLevelState> {
        self.level_map(side).get(&price)
    }

    #[must_use]
    pub fn levels(&self, side: Side) -> Vec<LevelView> {
        match side {
            Side::Buy => self
                .bids
                .iter()
                .rev()
                .map(|(price, level)| level_view(side, price, level))
                .collect(),
            Side::Sell => self
                .asks
                .iter()
                .map(|(price, level)| level_view(side, price, level))
                .collect(),
        }
    }

    #[must_use]
    pub fn best_level(&self, side: Side) -> Option<LevelView> {
        match side {
            Side::Buy => self
                .bids
                .last_key_value()
                .map(|(price, level)| level_view(side, price, level)),
            Side::Sell => self
                .asks
                .first_key_value()
                .map(|(price, level)| level_view(side, price, level)),
        }
    }

    #[must_use]
    pub fn depth(&self, levels: usize) -> DepthView {
        DepthView {
            bids: self
                .bids
                .iter()
                .rev()
                .take(levels)
                .map(|(price, level)| level_view(Side::Buy, price, level))
                .collect(),
            asks: self
                .asks
                .iter()
                .take(levels)
                .map(|(price, level)| level_view(Side::Sell, price, level))
                .collect(),
        }
    }

    pub(crate) fn try_visit_levels<E>(
        &self,
        side: Side,
        levels: usize,
        mut visitor: impl FnMut(LevelView) -> Result<(), E>,
    ) -> Result<(), E> {
        match side {
            Side::Buy => {
                for (price, level) in self.bids.iter().rev().take(levels) {
                    visitor(level_view(side, price, level))?;
                }
            }
            Side::Sell => {
                for (price, level) in self.asks.iter().take(levels) {
                    visitor(level_view(side, price, level))?;
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn orders_at(&self, side: Side, price: Price) -> Vec<OrderView> {
        let mut result = Vec::new();
        let Some(level) = self.level(side, price) else {
            return result;
        };
        let mut current = level.head;
        for _ in 0..level.order_count {
            let Some(handle) = current else {
                break;
            };
            let Some(state) = self.orders.get(handle) else {
                break;
            };
            result.push(order_view(state));
            current = state.next;
        }
        result
    }

    #[must_use]
    pub fn order(&self, key: &OrderKey) -> Option<OrderView> {
        self.active_by_key
            .get(key)
            .and_then(|handle| self.orders.get(*handle))
            .map(order_view)
    }

    #[must_use]
    pub fn statistics(&self) -> TradeStatisticsView {
        TradeStatisticsView {
            last_price: self.statistics.last_price,
            high_price: self.statistics.high_price,
            low_price: self.statistics.low_price,
            total_quantity: self.statistics.total_quantity,
            total_turnover_units: self.statistics.total_turnover_units,
            trade_count: self.statistics.trade_count,
        }
    }

    #[must_use]
    pub fn summary(&self) -> BookSummary {
        let best_bid = self.best_level(Side::Buy);
        let best_ask = self.best_level(Side::Sell);
        let statistics = self.statistics();
        BookSummary {
            book_key: self.config.book_key.clone(),
            last_raw_sequence: self
                .last_applied_meta
                .as_ref()
                .map(|meta| meta.raw_sequence),
            last_apply_sequence: self
                .last_applied_meta
                .as_ref()
                .map(|meta| meta.apply_sequence),
            last_local_time: self.last_applied_meta.as_ref().map(|meta| meta.local_time),
            last_quote_time: self.last_applied_meta.as_ref().map(|meta| meta.quote_time),
            best_bid,
            best_ask,
            active_order_count: self.active_by_key.len(),
            statistics,
        }
    }

    #[doc(hidden)]
    pub fn check_invariants(&self) -> Result<(), BookError> {
        if self.active_by_key.len() != self.orders.len() {
            return Err(BookError::InvariantViolation(
                "active index and storage length differ",
            ));
        }
        for (key, handle) in &self.active_by_key {
            let state = self
                .orders
                .get(*handle)
                .ok_or(BookError::InvariantViolation("active handle is missing"))?;
            if &state.key != key || !self.seen_order_keys.contains(key) {
                return Err(BookError::InvariantViolation("active key mismatch"));
            }
            if state.remaining_quantity == 0
                || state.remaining_quantity > state.original_quantity.get()
            {
                return Err(BookError::InvariantViolation("invalid remaining quantity"));
            }
        }

        let mut visited = HashSet::new();
        for side in [Side::Buy, Side::Sell] {
            let mut side_total = 0_u64;
            let mut side_weighted = 0_u128;
            for (price, level) in self.level_map(side) {
                if level.order_count == 0 || level.head.is_none() || level.tail.is_none() {
                    return Err(BookError::InvariantViolation("empty visible level"));
                }
                let mut total = 0_u64;
                let mut count = 0_usize;
                let mut previous = None;
                let mut current = level.head;
                while let Some(handle) = current {
                    if !visited.insert(handle) {
                        return Err(BookError::InvariantViolation(
                            "order appears twice in levels",
                        ));
                    }
                    let state = self
                        .orders
                        .get(handle)
                        .ok_or(BookError::InvariantViolation("level order is missing"))?;
                    if !side_matches(state, side)
                        || state.effective_price != Some(*price)
                        || state.location != OrderLocation::Resting
                        || state.previous != previous
                    {
                        return Err(BookError::InvariantViolation("invalid FIFO state"));
                    }
                    total = total
                        .checked_add(state.remaining_quantity)
                        .ok_or(BookError::ArithmeticOverflow("invariant total"))?;
                    count += 1;
                    previous = Some(handle);
                    current = state.next;
                    if count > self.orders.len() {
                        return Err(BookError::InvariantViolation("FIFO cycle"));
                    }
                }
                if total != level.total_quantity
                    || count != level.order_count
                    || previous != level.tail
                {
                    return Err(BookError::InvariantViolation("level aggregate mismatch"));
                }
                side_total = side_total
                    .checked_add(level.total_quantity)
                    .ok_or(BookError::ArithmeticOverflow("invariant side total"))?;
                side_weighted = side_weighted
                    .checked_add(price_quantity(*price, level.total_quantity)?)
                    .ok_or(BookError::ArithmeticOverflow("invariant side weighted"))?;
            }
            if self.side_aggregate(side)
                != (SideAggregate {
                    total_quantity: side_total,
                    weighted_price_quantity: side_weighted,
                })
            {
                return Err(BookError::InvariantViolation("side aggregate mismatch"));
            }
        }
        for (handle, state) in &self.orders {
            match state.location {
                OrderLocation::Resting if !visited.contains(&handle) => {
                    return Err(BookError::InvariantViolation(
                        "resting order is absent from levels",
                    ));
                }
                OrderLocation::Aggressive
                    if visited.contains(&handle)
                        || state.previous.is_some()
                        || state.next.is_some() =>
                {
                    return Err(BookError::InvariantViolation(
                        "aggressive order is linked to a level",
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn opposite(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

fn price_quantity(price: Price, quantity: u64) -> Result<u128, BookError> {
    u128::try_from(price.units())
        .map_err(|_| BookError::ArithmeticOverflow("side weighted price"))?
        .checked_mul(u128::from(quantity))
        .ok_or(BookError::ArithmeticOverflow("side weighted price"))
}

fn validate_reference_side(expected: Side, actual: Side) -> Result<(), BookError> {
    if expected == actual {
        Ok(())
    } else {
        Err(BookError::InvalidTradeReferenceSide { expected, actual })
    }
}

fn order_view(state: &OrderState) -> OrderView {
    OrderView {
        key: state.key,
        effective_price: state.effective_price,
        original_quantity: state.original_quantity,
        remaining_quantity: state.remaining_quantity,
        location: state.location,
    }
}

fn level_view(side: Side, price: &Price, level: &PriceLevelState) -> LevelView {
    LevelView {
        side,
        price: *price,
        total_quantity: level.total_quantity,
        order_count: level.order_count,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod pending_tests {
    use super::*;
    use crate::{
        ChannelId, LocalTimestampNs, Market, OrderId, Quantity, QuoteTimestampNs, RawSequence,
        Symbol, TradingDay,
    };

    #[test]
    fn explicit_pending_rest_preserves_metadata_fifo_and_failure_atomicity() {
        let book_key = BookKey {
            market: Market::Szse,
            symbol: Symbol::from("000001"),
            trading_day: TradingDay::from_yyyymmdd(20_260_828).expect("date"),
        };
        let mut book = OrderBook::new(BookConfig::new(
            book_key.clone(),
            PriceScale::from_decimal_places(4).expect("scale"),
        ));
        let key = |side, id| OrderKey {
            side,
            channel_id: ChannelId::new(1).expect("channel"),
            order_id: OrderId::new(id).expect("id"),
        };
        let meta = |n| EventMeta {
            book_key: book_key.clone(),
            raw_sequence: RawSequence::new(n).expect("raw"),
            apply_sequence: ApplySequence::new(n).expect("apply"),
            local_time: LocalTimestampNs::from_nanos(7),
            quote_time: QuoteTimestampNs::from_nanos(6),
        };
        let p = Price::from_units(100_000).expect("price");
        let add = |n, k, crossing| {
            BookEvent::AddOrder(AddOrder {
                meta: meta(n),
                order_key: k,
                pricing: PricingInstruction::Provided(p),
                crossing,
                quantity: Quantity::new(10).expect("quantity"),
            })
        };
        let first = key(Side::Buy, 1);
        let pending = key(Side::Buy, 2);
        let ask = key(Side::Sell, 3);
        book.apply(add(1, first, CrossingBehavior::Rest))
            .expect("first");
        book.apply(add(2, pending, CrossingBehavior::AlwaysHide))
            .expect("pending");
        book.apply(add(3, ask, CrossingBehavior::Rest))
            .expect("ask");
        let before = format!("{book:?}");
        let failed_revision = book.cache_revision();
        assert!(book.rest_pending_order(pending, p).is_err());
        assert_eq!(book.cache_revision(), failed_revision);
        assert_eq!(format!("{book:?}"), before);
        book.apply(BookEvent::OrderCancel(OrderCancel {
            meta: meta(4),
            order_key: ask,
        }))
        .expect("cancel");
        let last_meta = book.last_applied_meta().cloned();
        let revision = book.cache_revision().expect("revision");
        book.rest_pending_order(pending, p).expect("rest");
        assert_eq!(book.cache_revision(), Some(revision + 1));
        assert_eq!(book.last_applied_meta(), last_meta.as_ref());
        let later = key(Side::Buy, 5);
        book.apply(add(5, later, CrossingBehavior::Rest))
            .expect("later");
        assert_eq!(
            book.orders_at(Side::Buy, p)
                .iter()
                .map(|o| o.key)
                .collect::<Vec<_>>(),
            [first, pending, later]
        );
        assert_eq!(book.levels(Side::Buy)[0].total_quantity, 30);
        assert!(book.check_invariants().is_ok());
        book.revision = u64::MAX - 1;
        book.apply(add(6, key(Side::Buy, 6), CrossingBehavior::Rest))
            .expect("saturate");
        assert_eq!(book.cache_revision(), None);
        book.apply(add(7, key(Side::Buy, 7), CrossingBehavior::Rest))
            .expect("no wrap");
        assert_eq!(book.cache_revision(), None);
    }
}
