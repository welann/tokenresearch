use rust_decimal::Decimal;
use tempfile::tempdir;
use tokenresearch::model::{MarketRef, MarketStatus, Venue};
use tokenresearch::price_model::{
    PriceCandle1m, PriceCheckpoint, PriceGapWindow, PriceKind, PriceMarket, PriceResolution,
    PriceSample1s,
};
use tokenresearch::price_query::PriceQueryStore;
use tokenresearch::price_storage::SqlitePriceStore;
use tokenresearch::traits::{PriceCommitBatch, PriceStore};

fn dec(value: &str) -> Decimal {
    value.parse().expect("valid decimal")
}

fn sample_market(venue: Venue, symbol: &str, token: &str) -> PriceMarket {
    PriceMarket {
        market: MarketRef::new(venue, symbol),
        venue_market_id: symbol.to_string(),
        token: token.to_string(),
        quote_asset: "USDT".to_string(),
        status: MarketStatus::Active,
        supports_trade_history: true,
        supports_reference_history: venue == Venue::Binance,
        updated_at_ms: 1_000,
    }
}

#[tokio::test]
async fn sqlite_price_store_upserts_samples_candles_checkpoints_and_prunes_retention() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("prices.sqlite");
    let store = SqlitePriceStore::connect(&db_path).await.expect("connect");
    store.init().await.expect("init");
    store
        .upsert_price_markets(&[sample_market(Venue::Binance, "BTCUSDT", "BTC")])
        .await
        .expect("markets");

    let run_id = store.start_price_run(1_000).await.expect("run");
    let epoch_id = store
        .open_price_epoch(
            run_id,
            &MarketRef::new(Venue::Binance, "BTCUSDT"),
            PriceKind::Trade,
            1,
            1_000,
        )
        .await
        .expect("epoch");

    store
        .commit_price_batch(PriceCommitBatch {
            market: MarketRef::new(Venue::Binance, "BTCUSDT"),
            kind: PriceKind::Trade,
            epoch_id: Some(epoch_id),
            samples_1s: vec![PriceSample1s {
                market: MarketRef::new(Venue::Binance, "BTCUSDT"),
                kind: PriceKind::Trade,
                bucket_ts_ms: 1_710_000_000_000,
                open: dec("62000.0"),
                high: dec("62001.0"),
                low: dec("61999.5"),
                close: dec("62000.5"),
                sample_count: 2,
                first_exchange_ts_ms: Some(1_710_000_000_010),
                last_exchange_ts_ms: Some(1_710_000_000_800),
                updated_at_ms: 1_710_000_001_000,
            }],
            candles_1m: vec![PriceCandle1m {
                market: MarketRef::new(Venue::Binance, "BTCUSDT"),
                kind: PriceKind::Trade,
                open_time_ms: 1_710_000_000_000,
                close_time_ms: 1_710_000_059_999,
                open: dec("62000.0"),
                high: dec("62050.0"),
                low: dec("61980.0"),
                close: dec("62020.0"),
                volume: dec("123.4"),
                trade_count: Some(20),
                source: "live".to_string(),
                updated_at_ms: 1_710_000_060_000,
            }],
            checkpoint: Some(PriceCheckpoint {
                market: MarketRef::new(Venue::Binance, "BTCUSDT"),
                kind: PriceKind::Trade,
                epoch_id,
                last_live_bucket_ms: Some(1_710_000_000_000),
                last_candle_open_ms: Some(1_710_000_000_000),
                last_backfill_open_ms: Some(1_709_999_940_000),
                last_exchange_ts_ms: Some(1_710_000_000_800),
                updated_at_ms: 1_710_000_060_000,
                status: "live".to_string(),
            }),
            gaps: vec![PriceGapWindow {
                market: MarketRef::new(Venue::Binance, "BTCUSDT"),
                kind: PriceKind::Trade,
                resolution: PriceResolution::OneSecond,
                started_at_ms: 1_710_000_010_000,
                ended_at_ms: 1_710_000_012_000,
                reason: "network_gap".to_string(),
            }],
        })
        .await
        .expect("commit");

    store
        .commit_price_batch(PriceCommitBatch {
            market: MarketRef::new(Venue::Binance, "BTCUSDT"),
            kind: PriceKind::Trade,
            epoch_id: Some(epoch_id),
            samples_1s: vec![PriceSample1s {
                market: MarketRef::new(Venue::Binance, "BTCUSDT"),
                kind: PriceKind::Trade,
                bucket_ts_ms: 1_710_000_000_000,
                open: dec("62000.0"),
                high: dec("62002.0"),
                low: dec("61999.5"),
                close: dec("62001.5"),
                sample_count: 3,
                first_exchange_ts_ms: Some(1_710_000_000_010),
                last_exchange_ts_ms: Some(1_710_000_000_900),
                updated_at_ms: 1_710_000_001_200,
            }],
            candles_1m: Vec::new(),
            checkpoint: None,
            gaps: Vec::new(),
        })
        .await
        .expect("upsert sample");

    let query = PriceQueryStore::new(store.clone());
    let latest = query
        .latest_price("BTC", PriceKind::Trade, Some(Venue::Binance), None)
        .await
        .expect("latest");
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].close.to_string(), "62001.5");

    let pruned = store
        .prune_price_samples_older_than(1_710_000_000_001)
        .await
        .expect("prune");
    assert_eq!(pruned, 1);
}
