use rust_decimal::Decimal;
use tokenresearch::book::OrderBook;
use tokenresearch::model::{MarketRef, PriceLevel, Venue};

fn dec(value: &str) -> Decimal {
    value.parse().expect("valid decimal")
}

#[test]
fn order_book_applies_snapshot_and_delta_in_sorted_form() {
    let market = MarketRef::new(Venue::Binance, "BTCUSDT");
    let mut book = OrderBook::new(market.clone());

    book.apply_snapshot(
        &[
            PriceLevel::new(dec("100.0"), dec("1.5")),
            PriceLevel::new(dec("99.5"), dec("2.0")),
        ],
        &[
            PriceLevel::new(dec("100.5"), dec("1.1")),
            PriceLevel::new(dec("101.0"), dec("4.0")),
        ],
        Some(100),
        101,
    );

    book.apply_delta(
        &[
            PriceLevel::new(dec("100.0"), dec("1.7")),
            PriceLevel::new(dec("98.0"), Decimal::ZERO),
        ],
        &[PriceLevel::new(dec("100.5"), dec("0.9"))],
        Some(102),
        103,
    );

    let view = book.view(5);
    assert_eq!(view.market, market);
    assert_eq!(view.bids[0].price, dec("100.0"));
    assert_eq!(view.bids[0].quantity, dec("1.7"));
    assert_eq!(view.asks[0].price, dec("100.5"));
    assert_eq!(view.asks[0].quantity, dec("0.9"));
    assert!(
        view.bids
            .windows(2)
            .all(|pair| pair[0].price >= pair[1].price)
    );
    assert!(
        view.asks
            .windows(2)
            .all(|pair| pair[0].price <= pair[1].price)
    );
}

#[test]
fn zero_quantity_removes_level_without_leaking_empty_entries() {
    let market = MarketRef::new(Venue::Hyperliquid, "BTC");
    let mut book = OrderBook::new(market);
    book.apply_snapshot(
        &[PriceLevel::new(dec("100.0"), dec("1.0"))],
        &[PriceLevel::new(dec("100.1"), dec("2.0"))],
        Some(100),
        100,
    );
    book.apply_delta(
        &[PriceLevel::new(dec("100.0"), Decimal::ZERO)],
        &[],
        Some(101),
        101,
    );
    let view = book.view(5);
    assert!(view.bids.is_empty());
    assert_eq!(view.asks.len(), 1);
}
