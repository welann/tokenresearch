use std::process::Command;

use rust_decimal::Decimal;
use tempfile::tempdir;
use tokenresearch::model::{MarketRef, MarketStatus, Venue};
use tokenresearch::price_model::{
    LatestPrice, PriceCandle1m, PriceCheckpoint, PriceCommitBatch, PriceGapWindow, PriceHealth,
    PriceKind, PriceMarket, PriceResolution, PriceSample1s, PriceSeries,
};
use tokenresearch::price_storage::SqlitePriceStore;
use tokenresearch::traits::PriceStore;

fn dec(value: &str) -> Decimal {
    value.parse().expect("valid decimal")
}

async fn seed_db() -> (tempfile::TempDir, String) {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("token_prices.sqlite");
    let store = SqlitePriceStore::connect(&db_path).await.expect("connect");
    store.init().await.expect("init");
    store
        .upsert_price_markets(&[PriceMarket {
            market: MarketRef::new(Venue::Binance, "BTCUSDT"),
            venue_market_id: "BTCUSDT".to_string(),
            token: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
            status: MarketStatus::Active,
            supports_trade_history: true,
            supports_reference_history: true,
            updated_at_ms: 1_000,
        }])
        .await
        .expect("markets");

    store
        .commit_price_batch(PriceCommitBatch {
            market: MarketRef::new(Venue::Binance, "BTCUSDT"),
            kind: PriceKind::Trade,
            epoch_id: Some(1),
            samples_1s: vec![PriceSample1s {
                market: MarketRef::new(Venue::Binance, "BTCUSDT"),
                kind: PriceKind::Trade,
                bucket_ts_ms: 1_768_772_400_000,
                open: dec("62010.0"),
                high: dec("62010.0"),
                low: dec("62010.0"),
                close: dec("62010.0"),
                sample_count: 1,
                first_exchange_ts_ms: Some(1_768_772_400_100),
                last_exchange_ts_ms: Some(1_768_772_400_100),
                updated_at_ms: 1_768_772_401_000,
            }],
            candles_1m: vec![PriceCandle1m {
                market: MarketRef::new(Venue::Binance, "BTCUSDT"),
                kind: PriceKind::Trade,
                open_time_ms: 1_767_225_600_000,
                close_time_ms: 1_767_225_659_999,
                open: dec("61500.0"),
                high: dec("61500.0"),
                low: dec("61500.0"),
                close: dec("61500.0"),
                volume: dec("10"),
                trade_count: Some(3),
                source: "backfill".to_string(),
                updated_at_ms: 1_767_225_660_000,
            }],
            checkpoint: Some(PriceCheckpoint {
                market: MarketRef::new(Venue::Binance, "BTCUSDT"),
                kind: PriceKind::Trade,
                epoch_id: 1,
                last_live_bucket_ms: Some(1_768_772_400_000),
                last_candle_open_ms: Some(1_768_772_400_000),
                last_backfill_open_ms: Some(1_767_225_600_000),
                last_exchange_ts_ms: Some(1_768_772_400_100),
                updated_at_ms: 1_768_772_401_000,
                status: "live".to_string(),
            }),
            gaps: vec![PriceGapWindow {
                market: MarketRef::new(Venue::Binance, "BTCUSDT"),
                kind: PriceKind::Trade,
                resolution: PriceResolution::OneSecond,
                started_at_ms: 1_768_772_500_000,
                ended_at_ms: 1_768_772_503_000,
                reason: "network_gap".to_string(),
            }],
        })
        .await
        .expect("commit");

    (dir, db_path.display().to_string())
}

#[test]
fn price_cli_reads_markets_latest_range_gap_and_health() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let (_dir, db_path) = runtime.block_on(seed_db());

    let markets = Command::new(env!("CARGO_BIN_EXE_query"))
        .args(["--price-db", &db_path, "--json", "price-markets"])
        .output()
        .expect("price-markets");
    assert!(markets.status.success(), "{markets:?}");
    let markets: Vec<PriceMarket> = serde_json::from_slice(&markets.stdout).expect("markets json");
    assert_eq!(markets.len(), 1);

    let latest = Command::new(env!("CARGO_BIN_EXE_query"))
        .args([
            "--price-db",
            &db_path,
            "--json",
            "price-latest",
            "--token",
            "BTC",
            "--kind",
            "trade",
        ])
        .output()
        .expect("price-latest");
    assert!(latest.status.success(), "{latest:?}");
    let latest: Vec<LatestPrice> = serde_json::from_slice(&latest.stdout).expect("latest json");
    assert_eq!(latest[0].close.to_string(), "62010.0");

    let range = Command::new(env!("CARGO_BIN_EXE_query"))
        .args([
            "--price-db",
            &db_path,
            "--json",
            "price-range",
            "--token",
            "BTC",
            "--kind",
            "trade",
            "--start-ms",
            "1767225600000",
            "--end-ms",
            "1767225659999",
            "--resolution",
            "1m",
        ])
        .output()
        .expect("price-range");
    assert!(range.status.success(), "{range:?}");
    let range: Vec<PriceSeries> = serde_json::from_slice(&range.stdout).expect("range json");
    assert_eq!(range.len(), 1);

    let gaps = Command::new(env!("CARGO_BIN_EXE_query"))
        .args([
            "--price-db",
            &db_path,
            "--json",
            "price-gaps",
            "--token",
            "BTC",
        ])
        .output()
        .expect("price-gaps");
    assert!(gaps.status.success(), "{gaps:?}");
    let gaps: Vec<PriceGapWindow> = serde_json::from_slice(&gaps.stdout).expect("gaps json");
    assert_eq!(gaps.len(), 1);

    let health = Command::new(env!("CARGO_BIN_EXE_query"))
        .args([
            "--price-db",
            &db_path,
            "--json",
            "price-health",
            "--venue",
            "binance",
            "--symbol",
            "BTCUSDT",
            "--kind",
            "trade",
        ])
        .output()
        .expect("price-health");
    assert!(health.status.success(), "{health:?}");
    let health: PriceHealth = serde_json::from_slice(&health.stdout).expect("health json");
    assert_eq!(health.status, "live");
}
