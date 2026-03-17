use rust_decimal::Decimal;
use serde_json::json;
use tokenresearch::model::{
    EventKind, MarketRef, MarketStatus, MarketType, NormalizedBookEvent, NormalizedMarket,
    PriceLevel, SequenceRange, Venue,
};

#[test]
fn normalized_types_round_trip_without_losing_precision() {
    let market = NormalizedMarket {
        market: MarketRef::new(Venue::Binance, "BTCUSDT"),
        venue_market_id: "BTCUSDT".to_string(),
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        market_type: MarketType::Perpetual,
        status: MarketStatus::Active,
        price_decimals: 1,
        size_decimals: 3,
    };
    let event = NormalizedBookEvent {
        market: market.market.clone(),
        kind: EventKind::Delta,
        exchange_ts_ms: Some(1000),
        received_ts_ms: 1001,
        sequence: Some(SequenceRange {
            start: 10,
            end: 12,
            previous_end: Some(9),
            offset: Some(1),
        }),
        bids: vec![PriceLevel::new(
            Decimal::new(1001, 1),
            Decimal::new(1500, 3),
        )],
        asks: vec![PriceLevel::new(
            Decimal::new(1002, 1),
            Decimal::new(1250, 3),
        )],
        raw_payload: json!({"hello":"world"}),
    };

    let encoded = serde_json::to_string(&(market, event)).expect("serialize");
    let decoded: (NormalizedMarket, NormalizedBookEvent) =
        serde_json::from_str(&encoded).expect("deserialize");

    assert_eq!(decoded.0.price_decimals, 1);
    assert_eq!(decoded.1.bids[0].price.to_string(), "100.1");
    assert_eq!(decoded.1.asks[0].quantity.to_string(), "1.250");
}
