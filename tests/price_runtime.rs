mod common;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tempfile::tempdir;
use tokenresearch::model::Venue;
use tokenresearch::price_adapters::{
    BinancePriceAdapter, HyperliquidPriceAdapter, PriceVenueAdapter,
};
use tokenresearch::price_model::{PriceKind, PriceRangeRequest, PriceResolution};
use tokenresearch::price_query::{PriceQueryStore, TimeRange};
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
async fn runtime_bootstraps_and_persists_live_prices() {
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
        "wss://fstream.binance.com/stream?streams=!ticker@arr/!markPrice@arr@1s".to_string(),
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
            backfill_window_days: 0,
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
}

#[tokio::test]
async fn runtime_processes_multiple_venues_without_blocking_on_first_stream() {
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
        "wss://fstream.binance.com/stream?streams=!ticker@arr/!markPrice@arr@1s".to_string(),
        VecDeque::from(vec![
            common::fixture("price/binance/ws_trade.json"),
            common::fixture("price/binance/ws_reference.json"),
        ]),
    );

    let posts = Arc::new(Mutex::new(HashMap::<(String, String), String>::new()));
    posts.lock().expect("posts").insert(
        (
            "https://api.hyperliquid.xyz/info".to_string(),
            serde_json::json!({ "type": "meta" }).to_string(),
        ),
        common::fixture("price/hyperliquid/discovery.json"),
    );
    posts.lock().expect("posts").insert(
        (
            "https://api.hyperliquid.xyz/info".to_string(),
            serde_json::json!({
                "type": "candleSnapshot",
                "req": {
                    "coin": "BTC",
                    "interval": "1m",
                    "startTime": 1710000000000_i64,
                    "endTime": 1710000119999_i64,
                }
            })
            .to_string(),
        ),
        common::fixture("price/hyperliquid/candles_trade.json"),
    );

    let rest = FakeRestWithPosts {
        gets: rest.gets.clone(),
        posts,
    };

    ws.messages_by_url.lock().expect("ws").insert(
        "wss://api.hyperliquid.xyz/ws".to_string(),
        VecDeque::from(vec![
            common::fixture("price/hyperliquid/ws_trade.json"),
            common::fixture("price/hyperliquid/ws_reference.json"),
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
            backfill_window_days: 0,
        },
        store.clone(),
        rest,
        ws,
        clock,
        vec![
            Arc::new(BinancePriceAdapter::default()) as Arc<dyn PriceVenueAdapter>,
            Arc::new(HyperliquidPriceAdapter::default()) as Arc<dyn PriceVenueAdapter>,
        ],
    )
    .await
    .expect("runtime");

    let query = PriceQueryStore::new(store);
    let binance = query
        .latest_price("BTC", PriceKind::Trade, Some(Venue::Binance), None)
        .await
        .expect("binance latest");
    let hyperliquid = query
        .latest_price("BTC", PriceKind::Trade, Some(Venue::Hyperliquid), None)
        .await
        .expect("hyperliquid latest");

    assert_eq!(binance.len(), 1);
    assert_eq!(hyperliquid.len(), 1);
}

#[tokio::test]
async fn runtime_backfills_large_windows_in_pages() {
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
        "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=500&startTime=1709913720000&endTime=1709943719999".to_string(),
        r#"[[1709913720000,"62000.0","62020.0","61980.0","62010.0","10.0",1709913779999,"620100.0",3]]"#.to_string(),
    );
    rest.gets.lock().expect("gets").insert(
        "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=500&startTime=1709943720000&endTime=1709973719999".to_string(),
        r#"[[1709943720000,"62030.0","62050.0","62020.0","62040.0","11.0",1709943779999,"682440.0",4]]"#.to_string(),
    );
    rest.gets.lock().expect("gets").insert(
        "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=500&startTime=1709973720000&endTime=1710000119999".to_string(),
        r#"[[1709973720000,"62060.0","62080.0","62050.0","62070.0","12.0",1709973779999,"744840.0",5]]"#.to_string(),
    );
    rest.gets.lock().expect("gets").insert(
        "https://fapi.binance.com/fapi/v1/markPriceKlines?symbol=BTCUSDT&interval=1m&limit=500&startTime=1709913720000&endTime=1709943719999".to_string(),
        r#"[[1709913720000,"62001.0","62021.0","61981.0","62011.0","0",1709913779999,"0",0]]"#.to_string(),
    );
    rest.gets.lock().expect("gets").insert(
        "https://fapi.binance.com/fapi/v1/markPriceKlines?symbol=BTCUSDT&interval=1m&limit=500&startTime=1709943720000&endTime=1709973719999".to_string(),
        r#"[[1709943720000,"62031.0","62051.0","62021.0","62041.0","0",1709943779999,"0",0]]"#.to_string(),
    );
    rest.gets.lock().expect("gets").insert(
        "https://fapi.binance.com/fapi/v1/markPriceKlines?symbol=BTCUSDT&interval=1m&limit=500&startTime=1709973720000&endTime=1710000119999".to_string(),
        r#"[[1709973720000,"62061.0","62081.0","62051.0","62071.0","0",1709973779999,"0",0]]"#.to_string(),
    );

    let ws = FakeWsClient::default();
    ws.messages_by_url.lock().expect("ws").insert(
        "wss://fstream.binance.com/stream?streams=!ticker@arr/!markPrice@arr@1s".to_string(),
        VecDeque::from(vec![common::fixture("price/binance/ws_trade.json")]),
    );

    run_price_runtime_once(
        PriceRuntimeConfig {
            database_path: db_path.display().to_string(),
            sample_retention_days: 30,
            discovery_max_attempts: 1,
            backfill_window_days: 1,
        },
        store.clone(),
        rest,
        ws,
        FakeClock {
            now_ms: Arc::new(Mutex::new(1_710_000_120_500)),
        },
        vec![Arc::new(BinancePriceAdapter::default()) as Arc<dyn PriceVenueAdapter>],
    )
    .await
    .expect("runtime");

    let query = PriceQueryStore::new(store);
    let history = query
        .price_range(PriceRangeRequest {
            token: Some("BTC".to_string()),
            venue: Some(Venue::Binance),
            market_symbol: Some("BTCUSDT".to_string()),
            kind: PriceKind::Trade,
            start_ms: 1709913720000,
            end_ms: 1710000119999,
            resolution: PriceResolution::OneMinute,
        })
        .await
        .expect("history");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].points.len(), 3);
}

#[tokio::test]
async fn runtime_continues_live_after_backfill_failure_and_records_gap() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("token_prices.sqlite");
    let store = SqlitePriceStore::connect(&db_path).await.expect("connect");
    store.init().await.expect("init");

    let rest = FakeRest::default();
    rest.gets.lock().expect("gets").insert(
        "https://fapi.binance.com/fapi/v1/exchangeInfo".to_string(),
        common::fixture("price/binance/discovery.json"),
    );

    let ws = FakeWsClient::default();
    ws.messages_by_url.lock().expect("ws").insert(
        "wss://fstream.binance.com/stream?streams=!ticker@arr/!markPrice@arr@1s".to_string(),
        VecDeque::from(vec![
            common::fixture("price/binance/ws_trade.json"),
            common::fixture("price/binance/ws_reference.json"),
        ]),
    );

    run_price_runtime_once(
        PriceRuntimeConfig {
            database_path: db_path.display().to_string(),
            sample_retention_days: 30,
            discovery_max_attempts: 1,
            backfill_window_days: 1,
        },
        store.clone(),
        rest,
        ws,
        FakeClock {
            now_ms: Arc::new(Mutex::new(1_710_000_120_500)),
        },
        vec![Arc::new(BinancePriceAdapter::default()) as Arc<dyn PriceVenueAdapter>],
    )
    .await
    .expect("runtime should continue after backfill failure");

    let query = PriceQueryStore::new(store);
    let latest = query
        .latest_price("BTC", PriceKind::All, Some(Venue::Binance), None)
        .await
        .expect("latest");
    assert_eq!(latest.len(), 2);

    let gaps = query
        .price_gaps("BTC", Some(Venue::Binance), TimeRange::default())
        .await
        .expect("gaps");
    assert!(
        gaps.iter()
            .any(|gap| gap.reason == "backfill_request_failed")
    );
}

#[derive(Clone)]
struct FakeRestWithPosts {
    gets: Arc<Mutex<HashMap<String, String>>>,
    posts: Arc<Mutex<HashMap<(String, String), String>>>,
}

#[async_trait]
impl RestClient for FakeRestWithPosts {
    async fn get_text(&self, url: &str) -> DynResult<String> {
        self.gets
            .lock()
            .expect("gets")
            .get(url)
            .cloned()
            .ok_or_else(|| format!("missing GET response for {url}").into())
    }

    async fn post_json_text(&self, url: &str, body: &Value) -> DynResult<String> {
        let key = (
            url.to_string(),
            serde_json::to_string(body).expect("body json"),
        );
        self.posts
            .lock()
            .expect("posts")
            .get(&key)
            .cloned()
            .ok_or_else(|| format!("missing POST response for {} {}", url, key.1).into())
    }
}
