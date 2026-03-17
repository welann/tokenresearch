use std::time::Duration;

use tokenresearch::adapters::{BinanceAdapter, HyperliquidAdapter, LighterAdapter, VenueAdapter};
use tokenresearch::model::{EventKind, MarketStatus};
use tokenresearch::runtime::{ReqwestRestClient, TokioWsClient};
use tokenresearch::traits::{RestClient, WsClient};
use tokio::time::timeout;

async fn fetch_markets<A: VenueAdapter>(
    rest: &ReqwestRestClient,
    adapter: &A,
) -> Vec<tokenresearch::NormalizedMarket> {
    let request = adapter.discovery_request();
    let body = match request.method {
        tokenresearch::adapters::HttpMethod::Get => rest
            .get_text(&request.url)
            .await
            .expect("discovery GET should succeed"),
        tokenresearch::adapters::HttpMethod::Post => rest
            .post_json_text(&request.url, request.body.as_ref().expect("body"))
            .await
            .expect("discovery POST should succeed"),
    };

    adapter
        .discover_markets(&body)
        .expect("discovery parsing should succeed")
        .into_iter()
        .filter(|market| market.status == MarketStatus::Active)
        .collect()
}

async fn await_parsed_event<A: VenueAdapter>(
    adapter: &A,
    connection: &mut Box<dyn tokenresearch::traits::WsConnection>,
    max_wait: Duration,
) -> tokenresearch::NormalizedBookEvent {
    timeout(max_wait, async {
        loop {
            let raw = connection
                .next_text()
                .await
                .expect("websocket read should succeed")
                .expect("websocket should yield a message");
            match adapter.parse_ws_message(&raw, 0) {
                Ok(Some(event)) => return event,
                Ok(None) => continue,
                Err(error) => panic!("parse failed: {error}; raw={raw}"),
            }
        }
    })
    .await
    .expect("timed out waiting for event")
}

#[tokio::test]
#[ignore = "requires public network access"]
async fn online_binance_smoke() {
    let rest = ReqwestRestClient::new();
    let ws = TokioWsClient;
    let adapter = BinanceAdapter;
    let markets = fetch_markets(&rest, &adapter).await;
    let market = markets
        .into_iter()
        .find(|market| market.market.symbol == "BTCUSDT")
        .expect("BTCUSDT should exist");

    let snapshot_request = adapter
        .snapshot_request(&market)
        .expect("binance should have snapshot request");
    let snapshot_body = rest
        .get_text(&snapshot_request.url)
        .await
        .expect("snapshot fetch should succeed");
    let snapshot = adapter
        .parse_snapshot(&market, &snapshot_body, 0)
        .expect("snapshot parse should succeed");
    assert_eq!(snapshot.kind, EventKind::Snapshot);
    assert!(!snapshot.bids.is_empty());
    assert!(!snapshot.asks.is_empty());

    let mut connection = ws
        .connect(&adapter.ws_url(std::slice::from_ref(&market)))
        .await
        .expect("binance websocket should connect");
    let event = await_parsed_event(&adapter, &mut connection, Duration::from_secs(20)).await;
    assert_eq!(event.market.symbol, "BTCUSDT");
    assert_eq!(event.kind, EventKind::Delta);
}

#[tokio::test]
#[ignore = "requires public network access"]
async fn online_hyperliquid_smoke() {
    let rest = ReqwestRestClient::new();
    let ws = TokioWsClient;
    let adapter = HyperliquidAdapter;
    let markets = fetch_markets(&rest, &adapter).await;
    let market = markets
        .into_iter()
        .find(|market| market.market.symbol == "BTC")
        .expect("BTC should exist");

    let snapshot_request = adapter
        .snapshot_request(&market)
        .expect("hyperliquid should have snapshot request");
    let snapshot_body = rest
        .post_json_text(
            &snapshot_request.url,
            snapshot_request.body.as_ref().expect("body"),
        )
        .await
        .expect("snapshot fetch should succeed");
    let snapshot = adapter
        .parse_snapshot(&market, &snapshot_body, 0)
        .expect("snapshot parse should succeed");
    assert_eq!(snapshot.kind, EventKind::Image);
    assert!(!snapshot.bids.is_empty());
    assert!(!snapshot.asks.is_empty());

    let mut connection = ws
        .connect(&adapter.ws_url(std::slice::from_ref(&market)))
        .await
        .expect("hyperliquid websocket should connect");
    for message in adapter.subscription_messages(std::slice::from_ref(&market)) {
        connection
            .send_text(message)
            .await
            .expect("subscription should succeed");
    }
    let event = await_parsed_event(&adapter, &mut connection, Duration::from_secs(20)).await;
    assert_eq!(event.market.symbol, "BTC");
    assert_eq!(event.kind, EventKind::Image);
}

#[tokio::test]
#[ignore = "requires public network access"]
async fn online_lighter_smoke() {
    let rest = ReqwestRestClient::new();
    let ws = TokioWsClient;
    let adapter = LighterAdapter::default();
    let markets = fetch_markets(&rest, &adapter).await;
    let market = markets
        .into_iter()
        .find(|market| market.market.symbol == "PROVE")
        .expect("PROVE should exist");

    let mut connection = ws
        .connect(&adapter.ws_url(std::slice::from_ref(&market)))
        .await
        .expect("lighter websocket should connect");
    for message in adapter.subscription_messages(std::slice::from_ref(&market)) {
        connection
            .send_text(message)
            .await
            .expect("subscription should succeed");
    }
    let event = await_parsed_event(&adapter, &mut connection, Duration::from_secs(20)).await;
    assert_eq!(event.market.symbol, "PROVE");
    assert_eq!(event.kind, EventKind::Delta);
    assert!(!event.bids.is_empty() || !event.asks.is_empty());
}
