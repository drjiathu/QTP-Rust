mod common;

use common::{book_key, price_scale};
use qtp_core::{BookConfig, OrderBook};

#[test]
fn public_api_can_create_an_empty_order_book() {
    let book = OrderBook::new(BookConfig::new(book_key(), price_scale()));

    assert_eq!(&book.config().book_key, &book_key());
    assert!(book.is_empty());
    assert!(book.summary().best_bid.is_none());
    assert!(book.summary().best_ask.is_none());
}
