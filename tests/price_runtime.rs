mod common;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tempfile::tempdir;
use tokenresearch::model::Venue;
use tokenresearch::price_adapters::{BinancePriceAdapter, PriceVenueAdapter};
use tokenresearch::price_model::{PriceKind, PriceRangeRequest, PriceResolution};
use tokenresearch::price_query::PriceQueryStore;
use tokenresearch::price_runtime::{PriceRuntimeConfig, run_price_runtime_once};
use tokenresearch::price_storage::SqlitePriceStore;
use tokenresearch::traits::{Clock, DynResult, PriceStore, RestClient, WsClient, WsConnection};

#[derive(Clone, Default)]
struct FakeClock {
    now_ms: Arc<Mutex<i64>>,
}

#[async_trait]
impl Clock for FakeClock {
    fn now_ms(&self) -> i64 {
        *self.now_ms.lock().expect("clock")
    }

    async fn sleep(&self, duration: Duration) {
        let mut now_ms = self.now_ms.lock().expect("clock");
        *now_ms += duration.as_millis() as i64;
    }
}

#[derive(Clone, Default)]
struct FakeRest {
    gets: Arc<Mutex<HashMap<String, String>>>,
}

#[async_trait]
impl RestClient for FakeRest {
    async fn get_text(&self, url: &str) -> DynResult<String> {
        self.gets
            .lock()
            .expect("gets")
            .get(url)
            .cloned()
            .ok_or_else(|| format!("missing GET response for {url}").into())
    }

    async fn post_json_text(&self, _url: &str, _body: &Value) -> DynResult<String> {
        Err("unexpected POST".into())
    }
}

struct FakeConnection {
    messages: VecDeque<String>,
}

#[async_trait]
impl WsConnection for FakeConnection {
    async fn send_text(&mut self, _text: String) -> DynResult<()> {
        Ok(())
    }

    async fn next_text(&mut self) -> DynResult<Option<String>> {
        Ok(self.messages.pop_front())
    }
}

#[derive(Clone, Default)]
struct FakeWsClient {
    messages_by_url: Arc<Mutex<HashMap<String, VecDeque<String>>>>,
}

#[async_trait]
impl WsClient for FakeWsClient {
    async fn connect(&self, url: &str) -> DynResult<Box<dyn WsConnection>> {
        let messages = self
            .messages_by_url
            .lock()
            .expect("ws map")
            .remove(url)
            .ok_or_else(|| format!("missing WS response for {url}"))?;
        Ok(Box::new(FakeConnection { messages }))
    }
}

#[tokio::test]
async fn runtime_discovers_backfills_and_persists_live_prices() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("token_prices.sqlite");
    let store = SqlitePriceStore::connect(&db_path).await.expect("connect");
    store.init().await.expect("init");

    let rest = FakeRest::default();
    rest.gets.lock().expect("gets").insert(
        "https://fapi.binance.com/fapi/v1/exchangeInfo".to_string(),
        common::fixture("price/binance/discovery.json"),
    );
    rest.gets.lock().expect("gets").insert(
        "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=500&startTime=1710000000000&endTime=1710000119999".to_string(),
        common::fixture("price/binance/klines_trade.json"),
    );
    rest.gets.lock().expect("gets").insert(
        "https://fapi.binance.com/fapi/v1/markPriceKlines?symbol=BTCUSDT&interval=1m&limit=500&startTime=1710000000000&endTime=1710000119999".to_string(),
        common::fixture("price/binance/klines_reference.json"),
    );

    let ws = FakeWsClient::default();
    ws.messages_by_url.lock().expect("ws").insert(
        "wss://fstream.binance.com/ws".to_string(),
        VecDeque::from(vec![
            common::fixture("price/binance/ws_trade.json"),
            common::fixture("price/binance/ws_reference.json"),
        ]),
    );

    let clock = FakeClock {
        now_ms: Arc::new(Mutex::new(1_710_000_120_500)),
    };

    run_price_runtime_once(
        PriceRuntimeConfig {
            database_path: db_path.display().to_string(),
            sample_retention_days: 30,
            discovery_max_attempts: 1,
            backfill_minutes_on_empty_start: 2,
        },
        store.clone(),
        rest,
        ws,
        clock,
        vec![Arc::new(BinancePriceAdapter::default()) as Arc<dyn PriceVenueAdapter>],
    )
    .await
    .expect("runtime");

    let query = PriceQueryStore::new(store);
    let latest = query
        .latest_price("BTC", PriceKind::Trade, Some(Venue::Binance), None)
        .await
        .expect("latest");
    assert_eq!(latest.len(), 1);

    let history = query
        .price_range(PriceRangeRequest {
            token: Some("BTC".to_string()),
            venue: Some(Venue::Binance),
            market_symbol: Some("BTCUSDT".to_string()),
            kind: PriceKind::Reference,
            start_ms: 1_710_000_000_000,
            end_ms: 1_710_000_119_999,
            resolution: PriceResolution::OneMinute,
        })
        .await
        .expect("history");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].points.len(), 2);
}
