use rust_decimal::Decimal;
use serde_json::json;
use tempfile::tempdir;
use tokenresearch::model::{
    BookView, CollectorCheckpoint, EventKind, GapWindow, MarketRef, MarketStatus, MarketType,
    NormalizedBookEvent, NormalizedMarket, PriceLevel, SequenceRange, Venue,
};
use tokenresearch::query::{QueryError, QueryStore, TimeRange};
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
        kind: EventKind::Delta,
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

#[tokio::test]
async fn sqlite_store_commits_events_snapshots_and_queries_latest_book() {
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
                book: latest_book.clone(),
            }),
            checkpoint: Some(checkpoint),
            gaps: Vec::new(),
        })
        .await
        .expect("commit");

    let query = QueryStore::new(store.clone());
    let latest = query
        .latest_book(&latest_book.market, 5)
        .await
        .expect("latest")
        .expect("book");
    assert_eq!(latest.bids[0].price, dec("100.0"));

    let historical = query
        .book_at(&latest_book.market, 1_010, 5)
        .await
        .expect("book_at");
    assert_eq!(historical.asks[0].price, dec("100.5"));

    let events = query
        .events(&latest_book.market, TimeRange::default(), 10)
        .await
        .expect("events");
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn book_at_returns_gap_covered_when_requested_timestamp_is_missing() {
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

    let market = MarketRef::new(Venue::Binance, "BTCUSDT");
    store
        .commit_batch(CommitBatch {
            market: market.clone(),
            epoch_id,
            events: Vec::new(),
            latest_book: None,
            snapshot: None,
            checkpoint: None,
            gaps: vec![GapWindow {
                market: market.clone(),
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

    let query = QueryStore::new(store);
    let error = query.book_at(&market, 2_500, 5).await.expect_err("gap");
    assert!(matches!(error, QueryError::GapCovered));
}
