use std::time::Duration;

use tokenresearch::adapters::HttpMethod;
use tokenresearch::model::{MarketStatus, Venue};
use tokenresearch::price_adapters::{
    BinancePriceAdapter, HyperliquidPriceAdapter, LighterPriceAdapter, PriceVenueAdapter,
};
use tokenresearch::price_model::PriceKind;
use tokenresearch::runtime::{ReqwestRestClient, TokioWsClient};
use tokenresearch::traits::{DynResult, RestClient, WsClient};
use tokio::time::timeout;

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn fetch_markets<A: PriceVenueAdapter>(
    rest: &ReqwestRestClient,
    adapter: &A,
) -> DynResult<Vec<tokenresearch::PriceMarket>> {
    let request = adapter.discovery_request();
    let body = match request.method {
        HttpMethod::Get => rest.get_text(&request.url).await?,
        HttpMethod::Post => {
            rest.post_json_text(&request.url, request.body.as_ref().expect("body"))
                .await?
        }
    };

    Ok(adapter
        .discover_markets(&body)
        .expect("discovery parsing should succeed")
        .into_iter()
        .filter(|market| market.status == MarketStatus::Active)
        .collect())
}

fn is_binance_network_block(error: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(reqwest_error) = error.downcast_ref::<reqwest::Error>() {
        if let Some(status) = reqwest_error.status() {
            return matches!(status.as_u16(), 403 | 418 | 451);
        }
    }
    let message = error.to_string().to_ascii_lowercase();
    message.contains("tls handshake eof") || message.contains("connection reset")
}

async fn await_tick<A: PriceVenueAdapter>(
    adapter: &A,
    connection: &mut Box<dyn tokenresearch::traits::WsConnection>,
    max_wait: Duration,
) -> tokenresearch::NormalizedPriceTick {
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
    .expect("timed out waiting for tick")
}

#[tokio::test]
#[ignore = "requires public network access"]
async fn online_binance_price_smoke() {
    let rest = ReqwestRestClient::new();
    let ws = TokioWsClient;
    let adapter = BinancePriceAdapter::default();
    let markets = match fetch_markets(&rest, &adapter).await {
        Ok(markets) => markets,
        Err(error) if is_binance_network_block(error.as_ref()) => {
            eprintln!("skipping binance price smoke: discovery blocked: {error}");
            return;
        }
        Err(error) => panic!("binance discovery should succeed: {error}"),
    };
    let market = markets
        .into_iter()
        .find(|market| market.market.symbol == "BTCUSDT")
        .expect("BTCUSDT should exist");

    let history_request = adapter
        .history_request(tokenresearch::PriceHistoryRequest {
            market: market.clone(),
            kind: PriceKind::Trade,
            start_ms: now_ms() - 10 * 60_000,
            end_ms: now_ms() - 1_000,
            limit: 5,
        })
        .expect("history request");
    let history_body = match rest.get_text(&history_request.url).await {
        Ok(body) => body,
        Err(error) if is_binance_network_block(error.as_ref()) => {
            eprintln!("skipping binance price smoke: history blocked: {error}");
            return;
        }
        Err(error) => panic!("history should succeed: {error}"),
    };
    let candles = adapter
        .parse_history_candles(&market, PriceKind::Trade, &history_body)
        .expect("candles");
    assert!(!candles.is_empty());

    let mut connection = match ws.connect(&adapter.ws_url()).await {
        Ok(connection) => connection,
        Err(error) if is_binance_network_block(error.as_ref()) => {
            eprintln!("skipping binance price smoke: websocket blocked: {error}");
            return;
        }
        Err(error) => panic!("binance websocket should connect: {error}"),
    };
    for message in adapter.subscription_messages(std::slice::from_ref(&market)) {
        connection.send_text(message).await.expect("subscribe");
    }
    let tick = await_tick(&adapter, &mut connection, Duration::from_secs(20)).await;
    assert_eq!(tick.market.symbol, "BTCUSDT");
    assert!(matches!(tick.kind, PriceKind::Trade | PriceKind::Reference));
}

#[tokio::test]
#[ignore = "requires public network access"]
async fn online_hyperliquid_price_smoke() {
    let rest = ReqwestRestClient::new();
    let ws = TokioWsClient;
    let adapter = HyperliquidPriceAdapter::default();
    let markets = fetch_markets(&rest, &adapter)
        .await
        .expect("hyperliquid discovery should succeed");
    let market = markets
        .into_iter()
        .find(|market| market.market.symbol == "BTC")
        .expect("BTC should exist");

    let history_request = adapter
        .history_request(tokenresearch::PriceHistoryRequest {
            market: market.clone(),
            kind: PriceKind::Trade,
            start_ms: now_ms() - 10 * 60_000,
            end_ms: now_ms() - 1_000,
            limit: 5,
        })
        .expect("history request");
    let history_body = rest
        .post_json_text(
            &history_request.url,
            history_request.body.as_ref().expect("body"),
        )
        .await
        .expect("history should succeed");
    let candles = adapter
        .parse_history_candles(&market, PriceKind::Trade, &history_body)
        .expect("candles");
    assert!(!candles.is_empty());

    let mut connection = ws
        .connect(&adapter.ws_url())
        .await
        .expect("hyperliquid websocket should connect");
    for message in adapter.subscription_messages(std::slice::from_ref(&market)) {
        connection.send_text(message).await.expect("subscribe");
    }
    let tick = await_tick(&adapter, &mut connection, Duration::from_secs(20)).await;
    assert_eq!(tick.market.venue, Venue::Hyperliquid);
}

#[tokio::test]
#[ignore = "requires public network access"]
async fn online_lighter_price_smoke() {
    let rest = ReqwestRestClient::new();
    let ws = TokioWsClient;
    let adapter = LighterPriceAdapter::default();
    let markets = fetch_markets(&rest, &adapter)
        .await
        .expect("lighter discovery should succeed");
    let market = markets
        .into_iter()
        .find(|market| market.status == MarketStatus::Active)
        .expect("active lighter market should exist");

    if let Some(history_request) = adapter.history_request(tokenresearch::PriceHistoryRequest {
        market: market.clone(),
        kind: PriceKind::Trade,
        start_ms: now_ms() - 10 * 60_000,
        end_ms: now_ms() - 1_000,
        limit: 5,
    }) {
        let history_body = rest
            .get_text(&history_request.url)
            .await
            .expect("history should succeed");
        let candles = adapter
            .parse_history_candles(&market, PriceKind::Trade, &history_body)
            .expect("candles");
        assert!(!candles.is_empty());
    }

    let mut connection = ws
        .connect(&adapter.ws_url())
        .await
        .expect("lighter websocket should connect");
    for message in adapter.subscription_messages(std::slice::from_ref(&market)) {
        connection.send_text(message).await.expect("subscribe");
    }
    let tick = await_tick(&adapter, &mut connection, Duration::from_secs(20)).await;
    assert_eq!(tick.market.venue, Venue::Lighter);
}
