mod common;

use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI64, AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde_json::Value;
use tempfile::tempdir;
use tokenresearch::adapters::{BinanceAdapter, VenueAdapter};
use tokenresearch::model::{
    EventKind, MarketRef, MarketStatus, MarketType, NormalizedBookEvent, NormalizedMarket,
    PriceLevel, SequenceRange, Venue,
};
use tokenresearch::query::QueryStore;
use tokenresearch::runtime::{CollectorRuntime, RuntimeConfig};
use tokenresearch::storage::SqliteBookStore;
use tokenresearch::sync::{BinanceBookSync, GenericBookSync};
use tokenresearch::traits::{BookStore, Clock, CommitBatch, DynResult, RestClient};

#[derive(Clone, Default)]
struct FakeClock {
    now_ms: i64,
    sleeps: Arc<Mutex<Vec<Duration>>>,
}

#[async_trait]
impl Clock for FakeClock {
    fn now_ms(&self) -> i64 {
        self.now_ms
    }

    async fn sleep(&self, duration: Duration) {
        self.sleeps.lock().expect("lock").push(duration);
    }
}

#[derive(Clone, Default)]
struct FakeRest {
    responses: Arc<HashMap<String, String>>,
    transient_failures: Arc<Mutex<HashMap<String, usize>>>,
}

#[derive(Default)]
struct RecordingStore {
    next_epoch_id: AtomicI64,
    active_commits: AtomicUsize,
    max_concurrent_commits: AtomicUsize,
    committed_batches: AtomicUsize,
}

#[async_trait]
impl BookStore for RecordingStore {
    async fn init(&self) -> DynResult<()> {
        Ok(())
    }

    async fn upsert_markets(&self, _markets: &[tokenresearch::NormalizedMarket]) -> DynResult<()> {
        Ok(())
    }

    async fn load_markets(
        &self,
        _venue: Option<Venue>,
    ) -> DynResult<Vec<tokenresearch::NormalizedMarket>> {
        Ok(Vec::new())
    }

    async fn start_run(&self, _started_at_ms: i64) -> DynResult<i64> {
        Ok(1)
    }

    async fn open_epoch(
        &self,
        _run_id: i64,
        _market: &MarketRef,
        _epoch_seq: i64,
        _started_at_ms: i64,
    ) -> DynResult<i64> {
        Ok(self.next_epoch_id.fetch_add(1, Ordering::SeqCst) + 1)
    }

    async fn close_epoch(&self, _epoch_id: i64, _ended_at_ms: i64, _reason: &str) -> DynResult<()> {
        Ok(())
    }

    async fn load_checkpoint(
        &self,
        _market: &MarketRef,
    ) -> DynResult<Option<tokenresearch::CollectorCheckpoint>> {
        Ok(None)
    }

    async fn commit_batch(&self, _batch: CommitBatch) -> DynResult<()> {
        let active = self.active_commits.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let current = self.max_concurrent_commits.load(Ordering::SeqCst);
            if active <= current {
                break;
            }
            if self
                .max_concurrent_commits
                .compare_exchange(current, active, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.committed_batches.fetch_add(1, Ordering::SeqCst);
        self.active_commits.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl RestClient for FakeRest {
    async fn get_text(&self, url: &str) -> DynResult<String> {
        let mut failures = self.transient_failures.lock().expect("lock");
        if let Some(remaining) = failures.get_mut(url) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err("transient get failure".into());
            }
        }
        Ok(self.responses.get(url).cloned().expect("response"))
    }

    async fn post_json_text(&self, url: &str, _body: &Value) -> DynResult<String> {
        let mut failures = self.transient_failures.lock().expect("lock");
        if let Some(remaining) = failures.get_mut(url) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err("transient post failure".into());
            }
        }
        Ok(self.responses.get(url).cloned().expect("response"))
    }
}

#[tokio::test]
async fn runtime_discovers_markets_and_persists_synced_events() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("collector.sqlite");
    let store = Arc::new(SqliteBookStore::connect(&db_path).await.expect("connect"));
    let clock = Arc::new(FakeClock {
        now_ms: 10_000,
        sleeps: Arc::new(Mutex::new(Vec::new())),
    });
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
        transient_failures: Arc::new(Mutex::new(HashMap::new())),
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

#[tokio::test]
async fn discovery_retries_after_transient_rest_failure() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("collector.sqlite");
    let store = Arc::new(SqliteBookStore::connect(&db_path).await.expect("connect"));
    let sleeps = Arc::new(Mutex::new(Vec::new()));
    let clock = Arc::new(FakeClock {
        now_ms: 10_000,
        sleeps: sleeps.clone(),
    });
    let runtime = CollectorRuntime::new(
        store,
        clock,
        RuntimeConfig {
            database_path: db_path.display().to_string(),
            snapshot_every_events: 1,
            snapshot_every_ms: 1,
            discovery_max_attempts: 3,
            ..RuntimeConfig::default()
        },
    );
    runtime.bootstrap_run().await.expect("run");

    let url = "https://fapi.binance.com/fapi/v1/exchangeInfo".to_string();
    let rest = FakeRest {
        responses: Arc::new(HashMap::from([(
            url.clone(),
            common::fixture("binance/exchange_info.json"),
        )])),
        transient_failures: Arc::new(Mutex::new(HashMap::from([(url, 1_usize)]))),
    };

    let markets = runtime
        .discover_markets_with_retry(&rest, &BinanceAdapter)
        .await
        .expect("discovery should recover");
    assert_eq!(markets.len(), 2);
    assert_eq!(sleeps.lock().expect("lock").len(), 1);
}

#[tokio::test]
async fn discovery_error_includes_response_preview() {
    let runtime = CollectorRuntime::new(
        Arc::new(RecordingStore::default()),
        Arc::new(FakeClock::default()),
        RuntimeConfig::default(),
    );
    let url = "https://fapi.binance.com/fapi/v1/exchangeInfo".to_string();
    let rest = FakeRest {
        responses: Arc::new(HashMap::from([(
            url.clone(),
            r#"{"symbols":[{"contractType":"PERPETUAL"}]}"#.to_string(),
        )])),
        transient_failures: Arc::new(Mutex::new(HashMap::new())),
    };

    let error = runtime
        .discover_markets(&rest, &BinanceAdapter)
        .await
        .expect_err("discovery should fail");
    let message = error.to_string();

    assert!(message.contains("market discovery parse failed"));
    assert!(message.contains(&url));
    assert!(message.contains(r#""contractType":"PERPETUAL""#));
}

#[tokio::test]
async fn discovery_uses_cached_markets_before_remote() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("collector.sqlite");
    let store = Arc::new(SqliteBookStore::connect(&db_path).await.expect("connect"));
    store.init().await.expect("init");
    store
        .upsert_markets(&[NormalizedMarket {
            market: MarketRef::new(Venue::Binance, "BTCUSDT"),
            venue_market_id: "BTCUSDT".to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
            market_type: MarketType::Perpetual,
            status: MarketStatus::Active,
            price_decimals: 1,
            size_decimals: 3,
        }])
        .await
        .expect("seed markets");

    let sleeps = Arc::new(Mutex::new(Vec::new()));
    let clock = Arc::new(FakeClock {
        now_ms: 10_000,
        sleeps: sleeps.clone(),
    });
    let runtime = CollectorRuntime::new(
        store,
        clock,
        RuntimeConfig {
            database_path: db_path.display().to_string(),
            discovery_max_attempts: 3,
            ..RuntimeConfig::default()
        },
    );
    runtime.bootstrap_run().await.expect("run");

    let rest = FakeRest {
        responses: Arc::new(HashMap::new()),
        transient_failures: Arc::new(Mutex::new(HashMap::new())),
    };

    let markets = runtime
        .discover_markets_with_retry(&rest, &BinanceAdapter)
        .await
        .expect("discovery should use cache");
    assert_eq!(markets.len(), 1);
    assert_eq!(markets[0].market.symbol, "BTCUSDT");
    assert!(sleeps.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn runtime_serializes_commit_batches_through_single_writer() {
    let store = Arc::new(RecordingStore::default());
    let clock = Arc::new(FakeClock {
        now_ms: 10_000,
        sleeps: Arc::new(Mutex::new(Vec::new())),
    });
    let runtime = CollectorRuntime::new(
        store.clone(),
        clock,
        RuntimeConfig {
            snapshot_every_events: 1000,
            snapshot_every_ms: i64::MAX,
            ..RuntimeConfig::default()
        },
    );
    let run_id = runtime.bootstrap_run().await.expect("run");

    let mut tasks = Vec::new();
    for index in 0..6 {
        let runtime = runtime.clone();
        let market = MarketRef::new(Venue::Hyperliquid, format!("MKT{index}"));
        tasks.push(tokio::spawn(async move {
            let mut session = runtime
                .open_market_session(run_id, &market, 1)
                .await
                .expect("session");
            let mut sync = GenericBookSync::new(market.clone());
            let outcome = sync.apply(NormalizedBookEvent {
                market: market.clone(),
                kind: EventKind::Image,
                exchange_ts_ms: Some(10_000),
                received_ts_ms: 10_000 + index,
                sequence: Some(SequenceRange {
                    start: 1,
                    end: 1,
                    previous_end: None,
                    offset: None,
                }),
                bids: vec![PriceLevel::new(
                    Decimal::new(1000 + index as i64, 1),
                    Decimal::ONE,
                )],
                asks: vec![PriceLevel::new(
                    Decimal::new(1001 + index as i64, 1),
                    Decimal::ONE,
                )],
                raw_payload: serde_json::json!({"index": index}),
            });
            runtime
                .apply_generic_outcome(&mut session, &sync, outcome)
                .await
                .expect("persist");
        }));
    }

    for task in tasks {
        task.await.expect("join");
    }

    assert_eq!(store.committed_batches.load(Ordering::SeqCst), 6);
    assert_eq!(store.max_concurrent_commits.load(Ordering::SeqCst), 1);
}
