mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tempfile::tempdir;
use tokenresearch::adapters::{BinanceAdapter, VenueAdapter};
use tokenresearch::model::MarketRef;
use tokenresearch::query::QueryStore;
use tokenresearch::runtime::{CollectorRuntime, RuntimeConfig};
use tokenresearch::storage::SqliteBookStore;
use tokenresearch::sync::BinanceBookSync;
use tokenresearch::traits::{Clock, DynResult, RestClient};

#[derive(Clone, Default)]
struct FakeClock {
    now_ms: i64,
}

#[async_trait]
impl Clock for FakeClock {
    fn now_ms(&self) -> i64 {
        self.now_ms
    }

    async fn sleep(&self, _duration: Duration) {}
}

#[derive(Clone, Default)]
struct FakeRest {
    responses: Arc<HashMap<String, String>>,
}

#[async_trait]
impl RestClient for FakeRest {
    async fn get_text(&self, url: &str) -> DynResult<String> {
        Ok(self.responses.get(url).cloned().expect("response"))
    }

    async fn post_json_text(&self, url: &str, _body: &Value) -> DynResult<String> {
        Ok(self.responses.get(url).cloned().expect("response"))
    }
}

#[tokio::test]
async fn runtime_discovers_markets_and_persists_synced_events() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("collector.sqlite");
    let store = Arc::new(SqliteBookStore::connect(&db_path).await.expect("connect"));
    let clock = Arc::new(FakeClock { now_ms: 10_000 });
    let runtime = CollectorRuntime::new(
        store.clone(),
        clock,
        RuntimeConfig {
            database_path: db_path.display().to_string(),
            snapshot_every_events: 1,
            snapshot_every_ms: 1,
            ..RuntimeConfig::default()
        },
    );
    let run_id = runtime.bootstrap_run().await.expect("run");

    let adapter = BinanceAdapter;
    let rest = FakeRest {
        responses: Arc::new(HashMap::from([(
            "https://fapi.binance.com/fapi/v1/exchangeInfo".to_string(),
            common::fixture("binance/exchange_info.json"),
        )])),
    };
    let markets = runtime
        .discover_markets(&rest, &adapter)
        .await
        .expect("discover");
    assert_eq!(markets.len(), 2);

    let market = markets[0].market.clone();
    let mut session = runtime
        .open_market_session(run_id, &market, 1)
        .await
        .expect("session");
    let mut sync = BinanceBookSync::new(market.clone());
    let snapshot = adapter
        .parse_snapshot(
            &markets[0],
            &common::fixture("binance/depth_snapshot.json"),
            10_100,
        )
        .expect("snapshot");
    let outcome = sync.on_snapshot(snapshot);
    runtime
        .apply_binance_outcome(&mut session, &sync, outcome)
        .await
        .expect("persist snapshot");
    let delta = adapter
        .parse_ws_message(&common::fixture("binance/depth_update.json"), 10_101)
        .expect("delta")
        .expect("event");
    let outcome = sync.on_delta(delta);
    runtime
        .apply_binance_outcome(&mut session, &sync, outcome)
        .await
        .expect("persist delta");

    let query = QueryStore::new((*store).clone());
    let latest = query
        .latest_book(&MarketRef::new(tokenresearch::Venue::Binance, "BTCUSDT"), 5)
        .await
        .expect("latest")
        .expect("book");
    assert_eq!(latest.bids[0].quantity.to_string(), "1.7");
}
