mod common;

use std::collections::BTreeMap;

use common::{add, cancel, key, price, strict_book, trade};
use proptest::prelude::*;
use qtp_core::{OrderReference, Side};

proptest! {
    #[test]
    fn random_legal_sequences_preserve_invariants_and_level_totals(
        operations in prop::collection::vec(
            (any::<bool>(), 90_000_i64..110_000, 1_u64..500, any::<u16>(), any::<bool>()),
            1..40
        )
    ) {
        let mut book = strict_book();
        let mut reference = BTreeMap::<(u8, i64), u64>::new();
        let mut apply_sequence = 1_u64;

        for (index, (buy, price_units, quantity, trade_seed, cancel_remainder)) in
            operations.into_iter().enumerate()
        {
            let side = if buy { Side::Buy } else { Side::Sell };
            let side_code = if buy { 0 } else { 1 };
            let order_key = key(side, 1, index as u64 + 1);
            prop_assert!(
                book.apply(add(
                    apply_sequence,
                    apply_sequence,
                    order_key,
                    price_units,
                    quantity,
                ))
                .is_ok()
            );
            apply_sequence += 1;
            *reference.entry((side_code, price_units)).or_default() += quantity;
            prop_assert!(book.check_invariants().is_ok());

            let traded = u64::from(trade_seed) % (quantity + 1);
            if traded > 0 {
                let (bid, ask) = match side {
                    Side::Buy => (
                        OrderReference::Resolved(order_key),
                        OrderReference::Absent,
                    ),
                    Side::Sell => (
                        OrderReference::Absent,
                        OrderReference::Resolved(order_key),
                    ),
                };
                prop_assert!(
                    book.apply(trade(
                        apply_sequence,
                        apply_sequence,
                        bid,
                        ask,
                        price_units,
                        traded,
                    ))
                    .is_ok()
                );
                apply_sequence += 1;
                reduce_level(&mut reference, (side_code, price_units), traded);
                prop_assert!(book.check_invariants().is_ok());
            }

            if cancel_remainder && traded < quantity {
                prop_assert!(
                    book.apply(cancel(apply_sequence, apply_sequence, order_key))
                        .is_ok()
                );
                apply_sequence += 1;
                reduce_level(
                    &mut reference,
                    (side_code, price_units),
                    quantity - traded,
                );
                prop_assert!(book.check_invariants().is_ok());
            }

            let actual = levels_as_map(&book);
            prop_assert_eq!(actual, reference.clone());
        }
    }
}

fn reduce_level(levels: &mut BTreeMap<(u8, i64), u64>, key: (u8, i64), quantity: u64) {
    let remove = match levels.get_mut(&key) {
        Some(total) => {
            *total -= quantity;
            *total == 0
        }
        None => false,
    };
    if remove {
        levels.remove(&key);
    }
}

fn levels_as_map(book: &qtp_core::OrderBook) -> BTreeMap<(u8, i64), u64> {
    let mut result = BTreeMap::new();
    for level in book.levels(Side::Buy) {
        result.insert((0, level.price.units()), level.total_quantity);
    }
    for level in book.levels(Side::Sell) {
        result.insert((1, level.price.units()), level.total_quantity);
    }
    result
}

#[test]
fn price_helper_remains_positive_for_property_domain() {
    assert_eq!(price(90_000).units(), 90_000);
}
