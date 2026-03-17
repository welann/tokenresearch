mod common;

use tokenresearch::adapters::{BinanceAdapter, HyperliquidAdapter, LighterAdapter, VenueAdapter};
use tokenresearch::model::{EventKind, MarketStatus, Venue};

#[test]
fn binance_adapter_parses_markets_and_depth_messages() {
    let adapter = BinanceAdapter;
    let markets = adapter
        .discover_markets(&common::fixture("binance/exchange_info.json"))
        .expect("markets");
    assert_eq!(markets.len(), 2);
    assert_eq!(markets[0].market.venue, Venue::Binance);

    let snapshot = adapter
        .parse_snapshot(
            &markets[0],
            &common::fixture("binance/depth_snapshot.json"),
            2000,
        )
        .expect("snapshot");
    assert_eq!(snapshot.kind, EventKind::Snapshot);
    assert_eq!(snapshot.sequence.expect("sequence").end, 101);

    let delta = adapter
        .parse_ws_message(&common::fixture("binance/depth_update.json"), 2100)
        .expect("parse delta")
        .expect("event");
    assert_eq!(delta.kind, EventKind::Delta);
    assert_eq!(delta.sequence.expect("sequence").previous_end, Some(101));
}

#[test]
fn hyperliquid_adapter_parses_meta_and_l2_book_images() {
    let adapter = HyperliquidAdapter;
    let markets = adapter
        .discover_markets(&common::fixture("hyperliquid/meta.json"))
        .expect("markets");
    assert_eq!(markets.len(), 3);
    assert_eq!(markets[2].status, MarketStatus::Inactive);

    let image = adapter
        .parse_ws_message(&common::fixture("hyperliquid/l2_book.json"), 3000)
        .expect("parse image")
        .expect("event");
    assert_eq!(image.kind, EventKind::Image);
    assert_eq!(image.market.symbol, "BTC");
    assert_eq!(image.bids.len(), 2);
}

#[test]
fn lighter_adapter_parses_discovery_and_delta_messages() {
    let adapter = LighterAdapter::default();
    let markets = adapter
        .discover_markets(&common::fixture("lighter/order_books.json"))
        .expect("markets");
    assert_eq!(markets.len(), 2);
    assert_eq!(markets[0].market.venue, Venue::Lighter);

    let event = adapter
        .parse_ws_message(&common::fixture("lighter/order_book_ws.json"), 4000)
        .expect("parse delta")
        .expect("event");
    assert_eq!(event.kind, EventKind::Delta);
    assert_eq!(event.market.symbol, "PROVE");
    assert_eq!(event.sequence.expect("sequence").end, 10);
}
