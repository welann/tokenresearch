mod common;

use tokenresearch::model::{MarketStatus, Venue};
use tokenresearch::price_adapters::{
    BinancePriceAdapter, HyperliquidPriceAdapter, LighterPriceAdapter, PriceVenueAdapter,
};
use tokenresearch::price_model::{PriceHistoryRequest, PriceKind};

#[test]
fn binance_price_adapter_parses_markets_ticks_and_history() {
    let adapter = BinancePriceAdapter::default();
    let markets = adapter
        .discover_markets(&common::fixture("price/binance/discovery.json"))
        .expect("markets");
    assert_eq!(markets.len(), 1);
    assert_eq!(markets[0].market.symbol, "BTCUSDT");
    assert_eq!(markets[0].status, MarketStatus::Active);
    assert!(markets[0].supports_reference_history);

    let trade_tick = adapter
        .parse_ws_message(
            &common::fixture("price/binance/ws_trade.json"),
            1_710_000_000_500,
        )
        .expect("trade tick")
        .expect("trade tick should exist");
    assert_eq!(trade_tick.market.symbol, "BTCUSDT");
    assert_eq!(trade_tick.kind, PriceKind::Trade);
    assert_eq!(trade_tick.price.to_string(), "62000.1");

    let reference_tick = adapter
        .parse_ws_message(
            &common::fixture("price/binance/ws_reference.json"),
            1_710_000_001_500,
        )
        .expect("reference tick")
        .expect("reference tick should exist");
    assert_eq!(reference_tick.kind, PriceKind::Reference);
    assert_eq!(reference_tick.price.to_string(), "62001.0");

    let trade_candles = adapter
        .parse_history_candles(
            &markets[0],
            PriceKind::Trade,
            &common::fixture("price/binance/klines_trade.json"),
        )
        .expect("trade candles");
    assert_eq!(trade_candles.len(), 2);
    assert_eq!(trade_candles[0].open.to_string(), "62000.0");

    let request = adapter.history_request(PriceHistoryRequest {
        market: markets[0].clone(),
        kind: PriceKind::Reference,
        start_ms: 1_710_000_000_000,
        end_ms: 1_710_000_119_999,
        limit: 500,
    });
    let request = request.expect("reference history should be supported");
    assert!(request.url.contains("markPriceKlines"));
}

#[test]
fn hyperliquid_price_adapter_parses_trade_reference_and_candles() {
    let adapter = HyperliquidPriceAdapter::default();
    let markets = adapter
        .discover_markets(&common::fixture("price/hyperliquid/discovery.json"))
        .expect("markets");
    assert_eq!(markets.len(), 1);
    assert_eq!(markets[0].market.venue, Venue::Hyperliquid);
    assert!(!markets[0].supports_reference_history);

    let trade_tick = adapter
        .parse_ws_message(
            &common::fixture("price/hyperliquid/ws_trade.json"),
            1_710_000_002_500,
        )
        .expect("trade tick")
        .expect("trade tick should exist");
    assert_eq!(trade_tick.kind, PriceKind::Trade);
    assert_eq!(trade_tick.market.symbol, "BTC");

    let reference_tick = adapter
        .parse_ws_message(
            &common::fixture("price/hyperliquid/ws_reference.json"),
            1_710_000_003_500,
        )
        .expect("reference tick")
        .expect("reference tick should exist");
    assert_eq!(reference_tick.kind, PriceKind::Reference);
    assert_eq!(reference_tick.price.to_string(), "62011.0");

    let candles = adapter
        .parse_history_candles(
            &markets[0],
            PriceKind::Trade,
            &common::fixture("price/hyperliquid/candles_trade.json"),
        )
        .expect("candles");
    assert_eq!(candles.len(), 2);
    assert_eq!(candles[1].close.to_string(), "62035.0");

    assert!(
        adapter
            .history_request(PriceHistoryRequest {
                market: markets[0].clone(),
                kind: PriceKind::Reference,
                start_ms: 1_710_000_000_000,
                end_ms: 1_710_000_119_999,
                limit: 500,
            })
            .is_none()
    );
}

#[test]
fn lighter_price_adapter_parses_trade_reference_and_ignores_control_frames() {
    let adapter = LighterPriceAdapter::default();
    let markets = adapter
        .discover_markets(&common::fixture("price/lighter/discovery.json"))
        .expect("markets");
    assert_eq!(markets.len(), 1);
    assert_eq!(markets[0].market.symbol, "BTC");
    assert!(!markets[0].supports_reference_history);

    assert!(
        adapter
            .parse_ws_message(
                &common::fixture("price/lighter/ws_control.json"),
                1_710_000_004_000
            )
            .expect("control")
            .is_none()
    );

    let trade_tick = adapter
        .parse_ws_message(
            &common::fixture("price/lighter/ws_trade.json"),
            1_710_000_004_500,
        )
        .expect("trade tick")
        .expect("trade tick should exist");
    assert_eq!(trade_tick.kind, PriceKind::Trade);
    assert_eq!(trade_tick.market.symbol, "BTC");

    let reference_tick = adapter
        .parse_ws_message(
            &common::fixture("price/lighter/ws_reference.json"),
            1_710_000_005_500,
        )
        .expect("reference tick")
        .expect("reference tick should exist");
    assert_eq!(reference_tick.kind, PriceKind::Reference);
    assert_eq!(reference_tick.price.to_string(), "62012.5");

    let candles = adapter
        .parse_history_candles(
            &markets[0],
            PriceKind::Trade,
            &common::fixture("price/lighter/candles_trade.json"),
        )
        .expect("candles");
    assert_eq!(candles.len(), 2);
    assert_eq!(candles[0].low.to_string(), "61980.0");
}
