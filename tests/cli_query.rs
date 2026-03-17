use std::process::Command;

use rust_decimal::Decimal;
use serde_json::json;
use tempfile::tempdir;
use tokenresearch::model::{
    BookView, CollectorCheckpoint, CollectorHealth, GapWindow, MarketRef, MarketStatus, MarketType,
    NormalizedBookEvent, NormalizedMarket, PriceLevel, SequenceRange, Venue,
};
use tokenresearch::query::SnapshotMeta;
use tokenresearch::storage::SqliteBookStore;
use tokenresearch::traits::{BookStore, CommitBatch, SnapshotRecord};

fn dec(value: &str) -> Decimal {
    value.parse().expect("valid decimal")
}

fn sample_market() -> NormalizedMarket {
    NormalizedMarket {
        market: MarketRef::new(Venue::Binance, "BTCUSDT"),
        venue_market_id: "BTCUSDT".to_string(),
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        market_type: MarketType::Perpetual,
        status: MarketStatus::Active,
        price_decimals: 1,
        size_decimals: 3,
    }
}

fn sample_event(received_ts_ms: i64) -> NormalizedBookEvent {
    NormalizedBookEvent {
        market: sample_market().market,
        kind: tokenresearch::EventKind::Delta,
        exchange_ts_ms: Some(received_ts_ms - 1),
        received_ts_ms,
        sequence: Some(SequenceRange {
            start: 1,
            end: 1,
            previous_end: Some(0),
            offset: None,
        }),
        bids: vec![PriceLevel::new(dec("100.0"), dec("2.0"))],
        asks: vec![PriceLevel::new(dec("100.5"), dec("1.0"))],
        raw_payload: json!({"kind":"delta"}),
    }
}

async fn seed_db() -> (tempfile::TempDir, String) {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("collector.sqlite");
    let store = SqliteBookStore::connect(&db_path).await.expect("connect");
    store.init().await.expect("init");
    store
        .upsert_markets(&[sample_market()])
        .await
        .expect("markets");
    let run_id = store.start_run(1_000).await.expect("run");
    let epoch_id = store
        .open_epoch(run_id, &MarketRef::new(Venue::Binance, "BTCUSDT"), 1, 1_000)
        .await
        .expect("epoch");

    let latest_book = BookView {
        market: MarketRef::new(Venue::Binance, "BTCUSDT"),
        exchange_ts_ms: Some(1_009),
        received_ts_ms: 1_010,
        bids: vec![PriceLevel::new(dec("100.0"), dec("2.0"))],
        asks: vec![PriceLevel::new(dec("100.5"), dec("1.0"))],
        staleness_ms: None,
    };
    let checkpoint = CollectorCheckpoint {
        market: latest_book.market.clone(),
        epoch_id,
        last_sequence_end: Some(1),
        last_exchange_ts_ms: Some(1_009),
        last_snapshot_at_ms: Some(1_010),
        updated_at_ms: 1_010,
        status: "live".to_string(),
    };

    store
        .commit_batch(CommitBatch {
            market: latest_book.market.clone(),
            epoch_id,
            events: vec![sample_event(1_010)],
            latest_book: Some(latest_book.clone()),
            snapshot: Some(SnapshotRecord {
                created_at_ms: 1_010,
                depth: 1,
                book: latest_book,
            }),
            checkpoint: Some(checkpoint),
            gaps: vec![GapWindow {
                market: MarketRef::new(Venue::Binance, "BTCUSDT"),
                epoch_id: Some(epoch_id),
                started_at_ms: 2_000,
                ended_at_ms: 3_000,
                expected_sequence: Some(2),
                observed_sequence: Some(4),
                reason: "network_gap".to_string(),
            }],
        })
        .await
        .expect("commit");

    (dir, db_path.display().to_string())
}

#[test]
fn cli_lists_markets_as_json() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let (_dir, db_path) = runtime.block_on(seed_db());

    let output = Command::new(env!("CARGO_BIN_EXE_query"))
        .args(["--db", &db_path, "--json", "markets"])
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "{output:?}");
    let markets: Vec<NormalizedMarket> =
        serde_json::from_slice(&output.stdout).expect("valid json output");
    assert_eq!(markets.len(), 1);
    assert_eq!(markets[0].market.symbol, "BTCUSDT");
}

#[test]
fn cli_reads_latest_book_as_json() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let (_dir, db_path) = runtime.block_on(seed_db());

    let output = Command::new(env!("CARGO_BIN_EXE_query"))
        .args([
            "--db", &db_path, "--json", "latest", "--venue", "binance", "--symbol", "BTCUSDT",
            "--depth", "5",
        ])
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "{output:?}");
    let book: BookView = serde_json::from_slice(&output.stdout).expect("valid json output");
    assert_eq!(book.market.symbol, "BTCUSDT");
    assert_eq!(book.bids[0].price, dec("100.0"));
}

#[test]
fn cli_reads_health_and_gaps_as_json() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let (_dir, db_path) = runtime.block_on(seed_db());

    let health_output = Command::new(env!("CARGO_BIN_EXE_query"))
        .args([
            "--db", &db_path, "--json", "health", "--venue", "binance", "--symbol", "BTCUSDT",
        ])
        .output()
        .expect("cli should run");
    assert!(health_output.status.success(), "{health_output:?}");
    let health: CollectorHealth =
        serde_json::from_slice(&health_output.stdout).expect("valid json output");
    assert_eq!(health.status, "live");

    let gaps_output = Command::new(env!("CARGO_BIN_EXE_query"))
        .args([
            "--db", &db_path, "--json", "gaps", "--venue", "binance", "--symbol", "BTCUSDT",
        ])
        .output()
        .expect("cli should run");
    assert!(gaps_output.status.success(), "{gaps_output:?}");
    let gaps: Vec<GapWindow> =
        serde_json::from_slice(&gaps_output.stdout).expect("valid json output");
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0].reason, "network_gap");
}

#[test]
fn cli_reads_snapshots_as_json() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let (_dir, db_path) = runtime.block_on(seed_db());

    let output = Command::new(env!("CARGO_BIN_EXE_query"))
        .args([
            "--db",
            &db_path,
            "--json",
            "snapshots",
            "--venue",
            "binance",
            "--symbol",
            "BTCUSDT",
        ])
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "{output:?}");
    let snapshots: Vec<SnapshotMeta> =
        serde_json::from_slice(&output.stdout).expect("valid json output");
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].depth, 1);
}
