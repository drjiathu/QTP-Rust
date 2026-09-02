use std::collections::HashMap;

use crate::market_data::{OrderId, OrderKey, Side};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct SourceOrderReference {
    side: Side,
    order_id: OrderId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReferenceEntry {
    Unique(OrderKey),
    Ambiguous,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Resolution {
    Missing,
    Unique(OrderKey),
    Ambiguous,
}

/// Historical index used to map trade-side identifiers to complete order keys.
///
/// Entries are retained after an order completes or is cancelled. Registration
/// must occur only after the corresponding `AddOrder` was successfully applied.
#[derive(Clone, Debug, Default)]
pub struct OrderReferenceIndex {
    entries: HashMap<SourceOrderReference, ReferenceEntry>,
}

impl OrderReferenceIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, key: OrderKey) {
        let source = SourceOrderReference {
            side: key.side,
            order_id: key.order_id,
        };
        self.entries
            .entry(source)
            .and_modify(|entry| {
                if matches!(entry, ReferenceEntry::Unique(existing) if *existing != key) {
                    *entry = ReferenceEntry::Ambiguous;
                }
            })
            .or_insert(ReferenceEntry::Unique(key));
    }

    pub(crate) fn resolve(&self, side: Side, order_id: OrderId) -> Resolution {
        let source = SourceOrderReference { side, order_id };
        match self.entries.get(&source) {
            None => Resolution::Missing,
            Some(ReferenceEntry::Unique(key)) => Resolution::Unique(*key),
            Some(ReferenceEntry::Ambiguous) => Resolution::Ambiguous,
        }
    }
}
