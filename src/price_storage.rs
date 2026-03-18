use std::path::Path;
use std::str::FromStr;

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use crate::model::{MarketRef, MarketStatus, Venue};
use crate::price_model::{
    PriceCandle1m, PriceCheckpoint, PriceCommitBatch, PriceHealth, PriceKind, PriceMarket,
    PriceResolution, PriceSample1s,
};
use crate::traits::{DynResult, PriceStore};

const SCHEMA_STATEMENTS: &[&str] = &[
    "PRAGMA journal_mode = WAL",
    "CREATE TABLE IF NOT EXISTS price_markets (
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        venue_market_id TEXT NOT NULL,
        token TEXT NOT NULL,
        quote_asset TEXT NOT NULL,
        status TEXT NOT NULL,
        supports_trade_history INTEGER NOT NULL,
        supports_reference_history INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (venue, symbol)
    )",
    "CREATE TABLE IF NOT EXISTS price_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at_ms INTEGER NOT NULL,
        stopped_at_ms INTEGER,
        status TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS price_stream_epochs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        price_kind TEXT NOT NULL,
        epoch_seq INTEGER NOT NULL,
        started_at_ms INTEGER NOT NULL,
        ended_at_ms INTEGER,
        reason TEXT
    )",
    "CREATE TABLE IF NOT EXISTS price_samples_1s (
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        price_kind TEXT NOT NULL,
        bucket_ts_ms INTEGER NOT NULL,
        open_price TEXT NOT NULL,
        high_price TEXT NOT NULL,
        low_price TEXT NOT NULL,
        close_price TEXT NOT NULL,
        sample_count INTEGER NOT NULL,
        first_exchange_ts_ms INTEGER,
        last_exchange_ts_ms INTEGER,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (venue, symbol, price_kind, bucket_ts_ms)
    )",
    "CREATE INDEX IF NOT EXISTS idx_price_samples_1s_lookup
        ON price_samples_1s (venue, symbol, price_kind, bucket_ts_ms)",
    "CREATE TABLE IF NOT EXISTS price_candles_1m (
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        price_kind TEXT NOT NULL,
        open_time_ms INTEGER NOT NULL,
        close_time_ms INTEGER NOT NULL,
        open_price TEXT NOT NULL,
        high_price TEXT NOT NULL,
        low_price TEXT NOT NULL,
        close_price TEXT NOT NULL,
        volume TEXT NOT NULL,
        trade_count INTEGER,
        source TEXT NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (venue, symbol, price_kind, open_time_ms)
    )",
    "CREATE INDEX IF NOT EXISTS idx_price_candles_1m_lookup
        ON price_candles_1m (venue, symbol, price_kind, open_time_ms)",
    "CREATE TABLE IF NOT EXISTS price_checkpoints (
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        price_kind TEXT NOT NULL,
        epoch_id INTEGER NOT NULL,
        last_live_bucket_ms INTEGER,
        last_candle_open_ms INTEGER,
        last_backfill_open_ms INTEGER,
        last_exchange_ts_ms INTEGER,
        updated_at_ms INTEGER NOT NULL,
        status TEXT NOT NULL,
        PRIMARY KEY (venue, symbol, price_kind)
    )",
    "CREATE TABLE IF NOT EXISTS price_gap_windows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        price_kind TEXT NOT NULL,
        resolution TEXT NOT NULL,
        started_at_ms INTEGER NOT NULL,
        ended_at_ms INTEGER NOT NULL,
        reason TEXT NOT NULL
    )",
    "CREATE VIEW IF NOT EXISTS v_price_latest AS
        SELECT
            samples.venue AS venue,
            samples.symbol AS symbol,
            samples.price_kind AS price_kind,
            samples.bucket_ts_ms AS ts_ms,
            samples.open_price AS open_price,
            samples.high_price AS high_price,
            samples.low_price AS low_price,
            samples.close_price AS close_price,
            samples.updated_at_ms AS updated_at_ms
        FROM price_samples_1s samples
        JOIN (
            SELECT venue, symbol, price_kind, MAX(bucket_ts_ms) AS max_bucket_ts_ms
            FROM price_samples_1s
            GROUP BY venue, symbol, price_kind
        ) latest
            ON latest.venue = samples.venue
           AND latest.symbol = samples.symbol
           AND latest.price_kind = samples.price_kind
           AND latest.max_bucket_ts_ms = samples.bucket_ts_ms",
    "CREATE VIEW IF NOT EXISTS v_price_health AS
        SELECT
            checkpoints.venue AS venue,
            checkpoints.symbol AS symbol,
            checkpoints.price_kind AS price_kind,
            checkpoints.status AS status,
            checkpoints.updated_at_ms AS updated_at_ms,
            checkpoints.last_live_bucket_ms AS last_live_bucket_ms,
            checkpoints.last_candle_open_ms AS last_candle_open_ms,
            checkpoints.last_backfill_open_ms AS last_backfill_open_ms,
            (
                SELECT MAX(gaps.ended_at_ms)
                FROM price_gap_windows gaps
                WHERE gaps.venue = checkpoints.venue
                  AND gaps.symbol = checkpoints.symbol
                  AND gaps.price_kind = checkpoints.price_kind
            ) AS last_gap_at_ms
        FROM price_checkpoints checkpoints",
    "CREATE VIEW IF NOT EXISTS v_price_gap_summary AS
        SELECT
            venue,
            symbol,
            price_kind,
            COUNT(*) AS gap_count,
            MAX(ended_at_ms) AS last_gap_at_ms
        FROM price_gap_windows
        GROUP BY venue, symbol, price_kind",
];

#[derive(Clone, Debug)]
pub struct SqlitePriceStore {
    pool: SqlitePool,
}

impl SqlitePriceStore {
    pub async fn connect(path: impl AsRef<Path>) -> DynResult<Self> {
        let options =
            SqliteConnectOptions::from_str(&format!("sqlite://{}", path.as_ref().display()))?
                .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .connect_with(options)
            .await?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[async_trait]
impl PriceStore for SqlitePriceStore {
    async fn init(&self) -> DynResult<()> {
        for statement in SCHEMA_STATEMENTS {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn upsert_price_markets(&self, markets: &[PriceMarket]) -> DynResult<()> {
        for market in markets {
            sqlx::query(
                "INSERT INTO price_markets (
                    venue, symbol, venue_market_id, token, quote_asset, status,
                    supports_trade_history, supports_reference_history, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(venue, symbol) DO UPDATE SET
                    venue_market_id = excluded.venue_market_id,
                    token = excluded.token,
                    quote_asset = excluded.quote_asset,
                    status = excluded.status,
                    supports_trade_history = excluded.supports_trade_history,
                    supports_reference_history = excluded.supports_reference_history,
                    updated_at_ms = excluded.updated_at_ms",
            )
            .bind(market.market.venue.as_str())
            .bind(&market.market.symbol)
            .bind(&market.venue_market_id)
            .bind(&market.token)
            .bind(&market.quote_asset)
            .bind(match market.status {
                MarketStatus::Active => "active",
                MarketStatus::Inactive => "inactive",
            })
            .bind(if market.supports_trade_history {
                1_i64
            } else {
                0_i64
            })
            .bind(if market.supports_reference_history {
                1_i64
            } else {
                0_i64
            })
            .bind(market.updated_at_ms)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn load_price_markets(&self, venue: Option<Venue>) -> DynResult<Vec<PriceMarket>> {
        let rows = if let Some(venue) = venue {
            sqlx::query(
                "SELECT venue, symbol, venue_market_id, token, quote_asset, status,
                        supports_trade_history, supports_reference_history, updated_at_ms
                 FROM price_markets WHERE venue = ? ORDER BY symbol",
            )
            .bind(venue.as_str())
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT venue, symbol, venue_market_id, token, quote_asset, status,
                        supports_trade_history, supports_reference_history, updated_at_ms
                 FROM price_markets ORDER BY venue, symbol",
            )
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows
            .into_iter()
            .map(parse_price_market_row)
            .collect::<DynResult<Vec<_>>>()?)
    }

    async fn start_price_run(&self, started_at_ms: i64) -> DynResult<i64> {
        let result =
            sqlx::query("INSERT INTO price_runs (started_at_ms, status) VALUES (?, 'running')")
                .bind(started_at_ms)
                .execute(&self.pool)
                .await?;
        Ok(result.last_insert_rowid())
    }

    async fn open_price_epoch(
        &self,
        run_id: i64,
        market: &MarketRef,
        kind: PriceKind,
        epoch_seq: i64,
        started_at_ms: i64,
    ) -> DynResult<i64> {
        let result = sqlx::query(
            "INSERT INTO price_stream_epochs (
                run_id, venue, symbol, price_kind, epoch_seq, started_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(run_id)
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(kind.as_str())
        .bind(epoch_seq)
        .bind(started_at_ms)
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    async fn close_price_epoch(
        &self,
        epoch_id: i64,
        ended_at_ms: i64,
        reason: &str,
    ) -> DynResult<()> {
        sqlx::query("UPDATE price_stream_epochs SET ended_at_ms = ?, reason = ? WHERE id = ?")
            .bind(ended_at_ms)
            .bind(reason)
            .bind(epoch_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn load_price_checkpoint(
        &self,
        market: &MarketRef,
        kind: PriceKind,
    ) -> DynResult<Option<PriceCheckpoint>> {
        let row = sqlx::query(
            "SELECT epoch_id, last_live_bucket_ms, last_candle_open_ms, last_backfill_open_ms,
                    last_exchange_ts_ms, updated_at_ms, status
             FROM price_checkpoints WHERE venue = ? AND symbol = ? AND price_kind = ?",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(kind.as_str())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| PriceCheckpoint {
            market: market.clone(),
            kind,
            epoch_id: row.get("epoch_id"),
            last_live_bucket_ms: row.try_get("last_live_bucket_ms").ok().flatten(),
            last_candle_open_ms: row.try_get("last_candle_open_ms").ok().flatten(),
            last_backfill_open_ms: row.try_get("last_backfill_open_ms").ok().flatten(),
            last_exchange_ts_ms: row.try_get("last_exchange_ts_ms").ok().flatten(),
            updated_at_ms: row.get("updated_at_ms"),
            status: row.get("status"),
        }))
    }

    async fn commit_price_batch(&self, batch: PriceCommitBatch) -> DynResult<()> {
        let mut tx = self.pool.begin().await?;

        for sample in &batch.samples_1s {
            sqlx::query(
                "INSERT INTO price_samples_1s (
                    venue, symbol, price_kind, bucket_ts_ms, open_price, high_price, low_price,
                    close_price, sample_count, first_exchange_ts_ms, last_exchange_ts_ms,
                    updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(venue, symbol, price_kind, bucket_ts_ms) DO UPDATE SET
                    open_price = excluded.open_price,
                    high_price = excluded.high_price,
                    low_price = excluded.low_price,
                    close_price = excluded.close_price,
                    sample_count = excluded.sample_count,
                    first_exchange_ts_ms = excluded.first_exchange_ts_ms,
                    last_exchange_ts_ms = excluded.last_exchange_ts_ms,
                    updated_at_ms = excluded.updated_at_ms",
            )
            .bind(sample.market.venue.as_str())
            .bind(&sample.market.symbol)
            .bind(sample.kind.as_str())
            .bind(sample.bucket_ts_ms)
            .bind(sample.open.to_string())
            .bind(sample.high.to_string())
            .bind(sample.low.to_string())
            .bind(sample.close.to_string())
            .bind(sample.sample_count)
            .bind(sample.first_exchange_ts_ms)
            .bind(sample.last_exchange_ts_ms)
            .bind(sample.updated_at_ms)
            .execute(&mut *tx)
            .await?;
        }

        for candle in &batch.candles_1m {
            sqlx::query(
                "INSERT INTO price_candles_1m (
                    venue, symbol, price_kind, open_time_ms, close_time_ms, open_price,
                    high_price, low_price, close_price, volume, trade_count, source, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(venue, symbol, price_kind, open_time_ms) DO UPDATE SET
                    close_time_ms = excluded.close_time_ms,
                    open_price = excluded.open_price,
                    high_price = excluded.high_price,
                    low_price = excluded.low_price,
                    close_price = excluded.close_price,
                    volume = excluded.volume,
                    trade_count = excluded.trade_count,
                    source = excluded.source,
                    updated_at_ms = excluded.updated_at_ms",
            )
            .bind(candle.market.venue.as_str())
            .bind(&candle.market.symbol)
            .bind(candle.kind.as_str())
            .bind(candle.open_time_ms)
            .bind(candle.close_time_ms)
            .bind(candle.open.to_string())
            .bind(candle.high.to_string())
            .bind(candle.low.to_string())
            .bind(candle.close.to_string())
            .bind(candle.volume.to_string())
            .bind(candle.trade_count)
            .bind(&candle.source)
            .bind(candle.updated_at_ms)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(checkpoint) = &batch.checkpoint {
            sqlx::query(
                "INSERT INTO price_checkpoints (
                    venue, symbol, price_kind, epoch_id, last_live_bucket_ms, last_candle_open_ms,
                    last_backfill_open_ms, last_exchange_ts_ms, updated_at_ms, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(venue, symbol, price_kind) DO UPDATE SET
                    epoch_id = excluded.epoch_id,
                    last_live_bucket_ms = excluded.last_live_bucket_ms,
                    last_candle_open_ms = excluded.last_candle_open_ms,
                    last_backfill_open_ms = excluded.last_backfill_open_ms,
                    last_exchange_ts_ms = excluded.last_exchange_ts_ms,
                    updated_at_ms = excluded.updated_at_ms,
                    status = excluded.status",
            )
            .bind(checkpoint.market.venue.as_str())
            .bind(&checkpoint.market.symbol)
            .bind(checkpoint.kind.as_str())
            .bind(checkpoint.epoch_id)
            .bind(checkpoint.last_live_bucket_ms)
            .bind(checkpoint.last_candle_open_ms)
            .bind(checkpoint.last_backfill_open_ms)
            .bind(checkpoint.last_exchange_ts_ms)
            .bind(checkpoint.updated_at_ms)
            .bind(&checkpoint.status)
            .execute(&mut *tx)
            .await?;
        }

        for gap in &batch.gaps {
            sqlx::query(
                "INSERT INTO price_gap_windows (
                    venue, symbol, price_kind, resolution, started_at_ms, ended_at_ms, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(gap.market.venue.as_str())
            .bind(&gap.market.symbol)
            .bind(gap.kind.as_str())
            .bind(gap.resolution.as_str())
            .bind(gap.started_at_ms)
            .bind(gap.ended_at_ms)
            .bind(&gap.reason)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn prune_price_samples_older_than(&self, min_bucket_ts_ms: i64) -> DynResult<u64> {
        let result = sqlx::query("DELETE FROM price_samples_1s WHERE bucket_ts_ms < ?")
            .bind(min_bucket_ts_ms)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

pub fn parse_price_market_row(row: sqlx::sqlite::SqliteRow) -> DynResult<PriceMarket> {
    let venue: String = row.get("venue");
    let symbol: String = row.get("symbol");
    Ok(PriceMarket {
        market: parse_market_ref(&venue, &symbol),
        venue_market_id: row.get("venue_market_id"),
        token: row.get("token"),
        quote_asset: row.get("quote_asset"),
        status: match row.get::<String, _>("status").as_str() {
            "active" => MarketStatus::Active,
            _ => MarketStatus::Inactive,
        },
        supports_trade_history: row.get::<i64, _>("supports_trade_history") != 0,
        supports_reference_history: row.get::<i64, _>("supports_reference_history") != 0,
        updated_at_ms: row.get("updated_at_ms"),
    })
}

pub fn parse_market_ref(venue: &str, symbol: &str) -> MarketRef {
    let venue = match venue {
        "binance" => Venue::Binance,
        "hyperliquid" => Venue::Hyperliquid,
        "lighter" => Venue::Lighter,
        _ => Venue::Binance,
    };
    MarketRef::new(venue, symbol)
}

pub fn parse_price_kind(value: &str) -> DynResult<PriceKind> {
    value.parse().map_err(|error: String| error.into())
}

pub fn parse_price_resolution(value: &str) -> DynResult<PriceResolution> {
    value.parse().map_err(|error: String| error.into())
}

pub fn parse_price_sample_row(row: &sqlx::sqlite::SqliteRow) -> DynResult<PriceSample1s> {
    let venue: String = row.get("venue");
    let symbol: String = row.get("symbol");
    let kind: String = row.get("price_kind");
    Ok(PriceSample1s {
        market: parse_market_ref(&venue, &symbol),
        kind: parse_price_kind(&kind)?,
        bucket_ts_ms: row.get("bucket_ts_ms"),
        open: row.get::<String, _>("open_price").parse()?,
        high: row.get::<String, _>("high_price").parse()?,
        low: row.get::<String, _>("low_price").parse()?,
        close: row.get::<String, _>("close_price").parse()?,
        sample_count: row.get("sample_count"),
        first_exchange_ts_ms: row.try_get("first_exchange_ts_ms").ok().flatten(),
        last_exchange_ts_ms: row.try_get("last_exchange_ts_ms").ok().flatten(),
        updated_at_ms: row.get("updated_at_ms"),
    })
}

pub fn parse_price_candle_row(row: &sqlx::sqlite::SqliteRow) -> DynResult<PriceCandle1m> {
    let venue: String = row.get("venue");
    let symbol: String = row.get("symbol");
    let kind: String = row.get("price_kind");
    Ok(PriceCandle1m {
        market: parse_market_ref(&venue, &symbol),
        kind: parse_price_kind(&kind)?,
        open_time_ms: row.get("open_time_ms"),
        close_time_ms: row.get("close_time_ms"),
        open: row.get::<String, _>("open_price").parse()?,
        high: row.get::<String, _>("high_price").parse()?,
        low: row.get::<String, _>("low_price").parse()?,
        close: row.get::<String, _>("close_price").parse()?,
        volume: row.get::<String, _>("volume").parse()?,
        trade_count: row.try_get("trade_count").ok().flatten(),
        source: row.get("source"),
        updated_at_ms: row.get("updated_at_ms"),
    })
}

pub fn parse_price_health_row(row: sqlx::sqlite::SqliteRow) -> DynResult<PriceHealth> {
    let venue: String = row.get("venue");
    let symbol: String = row.get("symbol");
    let kind: String = row.get("price_kind");
    Ok(PriceHealth {
        market: parse_market_ref(&venue, &symbol),
        kind: parse_price_kind(&kind)?,
        status: row.get("status"),
        updated_at_ms: row.get("updated_at_ms"),
        last_live_bucket_ms: row.try_get("last_live_bucket_ms").ok().flatten(),
        last_candle_open_ms: row.try_get("last_candle_open_ms").ok().flatten(),
        last_backfill_open_ms: row.try_get("last_backfill_open_ms").ok().flatten(),
        last_gap_at_ms: row.try_get("last_gap_at_ms").ok().flatten(),
    })
}
