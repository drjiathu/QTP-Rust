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
    statistics: TradeStatistics,
    last_applied_meta: Option<EventMeta>,
}

#[derive(Clone, Debug)]
struct KnownTradePlan {
    handle: OrderHandle,
    state: OrderState,
    new_remaining: u64,
    new_effective_price: Option<Price>,
    reenter: bool,
}

#[derive(Clone, Debug)]
struct AttachPlan {
    total_quantity: u64,
    order_count: usize,
    previous: Option<OrderHandle>,
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
            statistics: TradeStatistics::default(),
            last_applied_meta: None,
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
        debug_assert!(self.check_invariants().is_ok());
        Ok(outcome)
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
        if state.location == OrderLocation::Resting {
            self.validate_detach(handle, &state)?;
        }
        let cancelled_quantity = state.remaining_quantity;
        if state.location == OrderLocation::Resting {
            self.commit_detach(handle, &state);
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
        if state.location == OrderLocation::Resting {
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
        }
        Ok(KnownTradePlan {
            handle,
            new_effective_price: state.effective_price,
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
        match self.level(side, price) {
            None => Ok(AttachPlan {
                total_quantity: quantity,
                order_count: 1,
                previous: None,
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
                })
            }
        }
    }

    fn commit_attach(&mut self, side: Side, price: Price, handle: OrderHandle, plan: AttachPlan) {
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

    fn validate_detach(&self, handle: OrderHandle, state: &OrderState) -> Result<(), BookError> {
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
        Ok(())
    }

    fn commit_detach(&mut self, _handle: OrderHandle, state: &OrderState) {
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
                self.commit_detach(plan.handle, &plan.state);
            } else {
                if let Some(price) = plan.state.effective_price {
                    if let Some(level) = self.level_map_mut(plan.state.key.side).get_mut(&price) {
                        level.total_quantity -= trade_quantity;
                    }
                }
                if let Some(state) = self.orders.get_mut(plan.handle) {
                    state.remaining_quantity = plan.new_remaining;
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

    fn level(&self, side: Side, price: Price) -> Option<&PriceLevelState> {
        self.level_map(side).get(&price)
    }

    #[must_use]
    pub fn levels(&self, side: Side) -> Vec<LevelView> {
        let make = |(price, level): (&Price, &PriceLevelState)| LevelView {
            side,
            price: *price,
            total_quantity: level.total_quantity,
            order_count: level.order_count,
        };
        match side {
            Side::Buy => self.bids.iter().rev().map(make).collect(),
            Side::Sell => self.asks.iter().map(make).collect(),
        }
    }

    #[must_use]
    pub fn depth(&self, levels: usize) -> DepthView {
        DepthView {
            bids: self.levels(Side::Buy).into_iter().take(levels).collect(),
            asks: self.levels(Side::Sell).into_iter().take(levels).collect(),
        }
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
    pub fn summary(&self) -> BookSummary {
        let best_bid = self.levels(Side::Buy).into_iter().next();
        let best_ask = self.levels(Side::Sell).into_iter().next();
        let statistics = TradeStatisticsView {
            last_price: self.statistics.last_price,
            high_price: self.statistics.high_price,
            low_price: self.statistics.low_price,
            total_quantity: self.statistics.total_quantity,
            total_turnover_units: self.statistics.total_turnover_units,
            trade_count: self.statistics.trade_count,
        };
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
