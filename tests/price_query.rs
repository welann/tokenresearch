use rust_decimal::Decimal;
use tempfile::tempdir;
use tokenresearch::model::{MarketRef, MarketStatus, Venue};
use tokenresearch::price_model::{
    PriceCandle1m, PriceCheckpoint, PriceCommitBatch, PriceGapWindow, PriceKind, PriceMarket,
    PriceRangeRequest, PriceResolution, PriceSample1s,
};
use tokenresearch::price_query::{PriceQueryError, PriceQueryStore};
use tokenresearch::price_storage::SqlitePriceStore;
use tokenresearch::traits::PriceStore;

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

async fn seed_price_db() -> (tempfile::TempDir, SqlitePriceStore) {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("prices.sqlite");
    let store = SqlitePriceStore::connect(&db_path).await.expect("connect");
    store.init().await.expect("init");
    store
        .upsert_price_markets(&[
            sample_market(Venue::Binance, "BTCUSDT", "BTC"),
            sample_market(Venue::Hyperliquid, "BTC", "BTC"),
        ])
        .await
        .expect("markets");

    for (market, close_recent, close_old) in [
        (
            MarketRef::new(Venue::Binance, "BTCUSDT"),
            "62010.0",
            "61500.0",
        ),
        (
            MarketRef::new(Venue::Hyperliquid, "BTC"),
            "62011.0",
            "61510.0",
        ),
    ] {
        store
            .commit_price_batch(PriceCommitBatch {
                market: market.clone(),
                kind: PriceKind::Trade,
                epoch_id: Some(1),
                samples_1s: vec![PriceSample1s {
                    market: market.clone(),
                    kind: PriceKind::Trade,
                    bucket_ts_ms: 1_768_772_400_000,
                    open: dec(close_recent),
                    high: dec(close_recent),
                    low: dec(close_recent),
                    close: dec(close_recent),
                    sample_count: 1,
                    first_exchange_ts_ms: Some(1_768_772_400_100),
                    last_exchange_ts_ms: Some(1_768_772_400_100),
                    updated_at_ms: 1_768_772_401_000,
                }],
                candles_1m: vec![PriceCandle1m {
                    market: market.clone(),
                    kind: PriceKind::Trade,
                    open_time_ms: 1_767_225_600_000,
                    close_time_ms: 1_767_225_659_999,
                    open: dec(close_old),
                    high: dec(close_old),
                    low: dec(close_old),
                    close: dec(close_old),
                    volume: dec("10"),
                    trade_count: Some(3),
                    source: "backfill".to_string(),
                    updated_at_ms: 1_767_225_660_000,
                }],
                checkpoint: Some(PriceCheckpoint {
                    market: market.clone(),
                    kind: PriceKind::Trade,
                    epoch_id: 1,
                    last_live_bucket_ms: Some(1_768_772_400_000),
                    last_candle_open_ms: Some(1_768_772_400_000),
                    last_backfill_open_ms: Some(1_767_225_600_000),
                    last_exchange_ts_ms: Some(1_768_772_400_100),
                    updated_at_ms: 1_768_772_401_000,
                    status: "live".to_string(),
                }),
                gaps: Vec::new(),
            })
            .await
            .expect("commit");
    }

    store
        .commit_price_batch(PriceCommitBatch {
            market: MarketRef::new(Venue::Binance, "BTCUSDT"),
            kind: PriceKind::Trade,
            epoch_id: Some(1),
            samples_1s: Vec::new(),
            candles_1m: Vec::new(),
            checkpoint: None,
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
        .expect("gap");

    (dir, store)
}

#[tokio::test]
async fn price_query_returns_multi_venue_latest_and_auto_resolution_ranges() {
    let (_dir, store) = seed_price_db().await;
    let query = PriceQueryStore::new(store);

    let latest = query
        .latest_price("BTC", PriceKind::Trade, None, None)
        .await
        .expect("latest");
    assert_eq!(latest.len(), 2);

    let recent = query
        .price_range(PriceRangeRequest {
            token: Some("BTC".to_string()),
            venue: None,
            market_symbol: None,
            kind: PriceKind::Trade,
            start_ms: 1_768_772_400_000,
            end_ms: 1_768_772_401_000,
            resolution: PriceResolution::Auto,
        })
        .await
        .expect("recent");
    assert_eq!(recent[0].resolution, PriceResolution::OneSecond);

    let historical = query
        .price_range(PriceRangeRequest {
            token: Some("BTC".to_string()),
            venue: None,
            market_symbol: None,
            kind: PriceKind::Trade,
            start_ms: 1_767_225_600_000,
            end_ms: 1_767_225_659_999,
            resolution: PriceResolution::Auto,
        })
        .await
        .expect("historical");
    assert_eq!(historical[0].resolution, PriceResolution::OneMinute);
}

#[tokio::test]
async fn price_query_rejects_missing_one_second_coverage() {
    let (_dir, store) = seed_price_db().await;
    let query = PriceQueryStore::new(store);

    let too_old = query
        .price_range(PriceRangeRequest {
            token: Some("BTC".to_string()),
            venue: Some(Venue::Binance),
            market_symbol: Some("BTCUSDT".to_string()),
            kind: PriceKind::Trade,
            start_ms: 1_767_225_600_000,
            end_ms: 1_767_225_659_999,
            resolution: PriceResolution::OneSecond,
        })
        .await
        .expect_err("1s retention");
    assert!(matches!(too_old, PriceQueryError::OneSecondUnavailable(_)));

    let gap = query
        .price_range(PriceRangeRequest {
            token: Some("BTC".to_string()),
            venue: Some(Venue::Binance),
            market_symbol: Some("BTCUSDT".to_string()),
            kind: PriceKind::Trade,
            start_ms: 1_768_772_500_500,
            end_ms: 1_768_772_501_500,
            resolution: PriceResolution::OneSecond,
        })
        .await
        .expect_err("gap");
    assert!(matches!(gap, PriceQueryError::GapCovered));
}
