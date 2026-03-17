use std::path::Path;
use std::str::FromStr;

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use crate::model::{
    BookView, CollectorCheckpoint, MarketRef, MarketStatus, MarketType, NormalizedBookEvent,
    NormalizedMarket, PriceLevel, SequenceRange, Venue,
};
use crate::traits::{BookStore, CommitBatch, DynResult};

const SCHEMA_STATEMENTS: &[&str] = &[
    "PRAGMA journal_mode = WAL",
    "CREATE TABLE IF NOT EXISTS markets (
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        venue_market_id TEXT NOT NULL,
        base_asset TEXT NOT NULL,
        quote_asset TEXT NOT NULL,
        market_type TEXT NOT NULL,
        status TEXT NOT NULL,
        price_decimals INTEGER NOT NULL,
        size_decimals INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (venue, symbol)
    )",
    "CREATE TABLE IF NOT EXISTS collector_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at_ms INTEGER NOT NULL,
        stopped_at_ms INTEGER,
        status TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS stream_epochs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        epoch_seq INTEGER NOT NULL,
        started_at_ms INTEGER NOT NULL,
        ended_at_ms INTEGER,
        reason TEXT
    )",
    "CREATE TABLE IF NOT EXISTS book_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        epoch_id INTEGER NOT NULL,
        event_kind TEXT NOT NULL,
        exchange_ts_ms INTEGER,
        received_ts_ms INTEGER NOT NULL,
        sequence_start INTEGER,
        sequence_end INTEGER,
        previous_sequence INTEGER,
        offset INTEGER,
        bids_json TEXT NOT NULL,
        asks_json TEXT NOT NULL,
        raw_payload_json TEXT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS idx_book_events_market_time
        ON book_events (venue, symbol, received_ts_ms, id)",
    "CREATE TABLE IF NOT EXISTS book_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        epoch_id INTEGER NOT NULL,
        source_event_id INTEGER,
        created_at_ms INTEGER NOT NULL,
        depth INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS book_snapshot_levels (
        snapshot_id INTEGER NOT NULL,
        side TEXT NOT NULL,
        price TEXT NOT NULL,
        quantity TEXT NOT NULL,
        level_rank INTEGER NOT NULL,
        PRIMARY KEY (snapshot_id, side, level_rank)
    )",
    "CREATE TABLE IF NOT EXISTS latest_levels (
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        price TEXT NOT NULL,
        quantity TEXT NOT NULL,
        level_rank INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (venue, symbol, side, level_rank)
    )",
    "CREATE TABLE IF NOT EXISTS collector_checkpoints (
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        epoch_id INTEGER NOT NULL,
        last_event_id INTEGER,
        last_sequence_end INTEGER,
        last_exchange_ts_ms INTEGER,
        last_snapshot_id INTEGER,
        last_status TEXT NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (venue, symbol)
    )",
    "CREATE TABLE IF NOT EXISTS gap_windows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        venue TEXT NOT NULL,
        symbol TEXT NOT NULL,
        epoch_id INTEGER,
        started_at_ms INTEGER NOT NULL,
        ended_at_ms INTEGER NOT NULL,
        expected_sequence INTEGER,
        observed_sequence INTEGER,
        reason TEXT NOT NULL
    )",
    "CREATE VIEW IF NOT EXISTS v_latest_best_quote AS
        SELECT
            bids.venue AS venue,
            bids.symbol AS symbol,
            bids.price AS best_bid,
            asks.price AS best_ask,
            bids.updated_at_ms AS updated_at_ms
        FROM latest_levels bids
        LEFT JOIN latest_levels asks
            ON asks.venue = bids.venue
            AND asks.symbol = bids.symbol
            AND asks.side = 'ask'
            AND asks.level_rank = 0
        WHERE bids.side = 'bid' AND bids.level_rank = 0",
    "CREATE VIEW IF NOT EXISTS v_market_health AS
        SELECT
            checkpoints.venue AS venue,
            checkpoints.symbol AS symbol,
            checkpoints.last_status AS status,
            checkpoints.updated_at_ms AS updated_at_ms,
            checkpoints.last_sequence_end AS last_sequence_end,
            (
                SELECT MAX(gaps.ended_at_ms)
                FROM gap_windows gaps
                WHERE gaps.venue = checkpoints.venue
                  AND gaps.symbol = checkpoints.symbol
            ) AS last_gap_at_ms
        FROM collector_checkpoints checkpoints",
    "CREATE VIEW IF NOT EXISTS v_gap_summary AS
        SELECT venue, symbol, COUNT(*) AS gap_count, MAX(ended_at_ms) AS last_gap_at_ms
        FROM gap_windows
        GROUP BY venue, symbol",
];

#[derive(Clone, Debug)]
pub struct SqliteBookStore {
    pool: SqlitePool,
}

impl SqliteBookStore {
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
impl BookStore for SqliteBookStore {
    async fn init(&self) -> DynResult<()> {
        for statement in SCHEMA_STATEMENTS {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn upsert_markets(&self, markets: &[NormalizedMarket]) -> DynResult<()> {
        for market in markets {
            sqlx::query(
                "INSERT INTO markets (
                    venue, symbol, venue_market_id, base_asset, quote_asset,
                    market_type, status, price_decimals, size_decimals, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(venue, symbol) DO UPDATE SET
                    venue_market_id = excluded.venue_market_id,
                    base_asset = excluded.base_asset,
                    quote_asset = excluded.quote_asset,
                    market_type = excluded.market_type,
                    status = excluded.status,
                    price_decimals = excluded.price_decimals,
                    size_decimals = excluded.size_decimals,
                    updated_at_ms = excluded.updated_at_ms",
            )
            .bind(market.market.venue.as_str())
            .bind(&market.market.symbol)
            .bind(&market.venue_market_id)
            .bind(&market.base_asset)
            .bind(&market.quote_asset)
            .bind(match market.market_type {
                MarketType::Perpetual => "perpetual",
            })
            .bind(match market.status {
                MarketStatus::Active => "active",
                MarketStatus::Inactive => "inactive",
            })
            .bind(market.price_decimals as i64)
            .bind(market.size_decimals as i64)
            .bind(0_i64)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn start_run(&self, started_at_ms: i64) -> DynResult<i64> {
        let result =
            sqlx::query("INSERT INTO collector_runs (started_at_ms, status) VALUES (?, 'running')")
                .bind(started_at_ms)
                .execute(&self.pool)
                .await?;
        Ok(result.last_insert_rowid())
    }

    async fn open_epoch(
        &self,
        run_id: i64,
        market: &MarketRef,
        epoch_seq: i64,
        started_at_ms: i64,
    ) -> DynResult<i64> {
        let result = sqlx::query(
            "INSERT INTO stream_epochs (run_id, venue, symbol, epoch_seq, started_at_ms)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(run_id)
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(epoch_seq)
        .bind(started_at_ms)
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    async fn close_epoch(&self, epoch_id: i64, ended_at_ms: i64, reason: &str) -> DynResult<()> {
        sqlx::query("UPDATE stream_epochs SET ended_at_ms = ?, reason = ? WHERE id = ?")
            .bind(ended_at_ms)
            .bind(reason)
            .bind(epoch_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn load_checkpoint(&self, market: &MarketRef) -> DynResult<Option<CollectorCheckpoint>> {
        let row = sqlx::query(
            "SELECT epoch_id, last_sequence_end, last_exchange_ts_ms, updated_at_ms, last_status
             FROM collector_checkpoints
             WHERE venue = ? AND symbol = ?",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| CollectorCheckpoint {
            market: market.clone(),
            epoch_id: row.get("epoch_id"),
            last_sequence_end: row
                .try_get::<Option<i64>, _>("last_sequence_end")
                .ok()
                .flatten()
                .map(|value| value as u64),
            last_exchange_ts_ms: row.try_get("last_exchange_ts_ms").ok().flatten(),
            last_snapshot_at_ms: None,
            updated_at_ms: row.get("updated_at_ms"),
            status: row.get("last_status"),
        }))
    }

    async fn commit_batch(&self, batch: CommitBatch) -> DynResult<()> {
        let mut tx = self.pool.begin().await?;
        let mut last_event_id = None;

        for event in &batch.events {
            let sequence = event.sequence.as_ref();
            let result = sqlx::query(
                "INSERT INTO book_events (
                    venue, symbol, epoch_id, event_kind, exchange_ts_ms, received_ts_ms,
                    sequence_start, sequence_end, previous_sequence, offset,
                    bids_json, asks_json, raw_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(event.market.venue.as_str())
            .bind(&event.market.symbol)
            .bind(batch.epoch_id)
            .bind(match event.kind {
                crate::model::EventKind::Snapshot => "snapshot",
                crate::model::EventKind::Delta => "delta",
                crate::model::EventKind::Image => "image",
                crate::model::EventKind::Gap => "gap",
                crate::model::EventKind::Heartbeat => "heartbeat",
            })
            .bind(event.exchange_ts_ms)
            .bind(event.received_ts_ms)
            .bind(sequence.map(|value| value.start as i64))
            .bind(sequence.map(|value| value.end as i64))
            .bind(sequence.and_then(|value| value.previous_end.map(|inner| inner as i64)))
            .bind(sequence.and_then(|value| value.offset.map(|inner| inner as i64)))
            .bind(serde_json::to_string(&event.bids)?)
            .bind(serde_json::to_string(&event.asks)?)
            .bind(event.raw_payload.to_string())
            .execute(&mut *tx)
            .await?;
            last_event_id = Some(result.last_insert_rowid());
        }

        let mut snapshot_id = None;
        if let Some(snapshot) = &batch.snapshot {
            let result = sqlx::query(
                "INSERT INTO book_snapshots (
                    venue, symbol, epoch_id, source_event_id, created_at_ms, depth
                ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(snapshot.book.market.venue.as_str())
            .bind(&snapshot.book.market.symbol)
            .bind(batch.epoch_id)
            .bind(last_event_id)
            .bind(snapshot.created_at_ms)
            .bind(snapshot.depth as i64)
            .execute(&mut *tx)
            .await?;
            snapshot_id = Some(result.last_insert_rowid());
            persist_levels(
                &mut tx,
                "book_snapshot_levels",
                snapshot_id.unwrap(),
                &snapshot.book,
            )
            .await?;
        }

        if let Some(book) = &batch.latest_book {
            sqlx::query("DELETE FROM latest_levels WHERE venue = ? AND symbol = ?")
                .bind(book.market.venue.as_str())
                .bind(&book.market.symbol)
                .execute(&mut *tx)
                .await?;
            persist_latest_levels(&mut tx, book).await?;
        }

        if let Some(checkpoint) = &batch.checkpoint {
            sqlx::query(
                "INSERT INTO collector_checkpoints (
                    venue, symbol, epoch_id, last_event_id, last_sequence_end,
                    last_exchange_ts_ms, last_snapshot_id, last_status, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(venue, symbol) DO UPDATE SET
                    epoch_id = excluded.epoch_id,
                    last_event_id = excluded.last_event_id,
                    last_sequence_end = excluded.last_sequence_end,
                    last_exchange_ts_ms = excluded.last_exchange_ts_ms,
                    last_snapshot_id = excluded.last_snapshot_id,
                    last_status = excluded.last_status,
                    updated_at_ms = excluded.updated_at_ms",
            )
            .bind(checkpoint.market.venue.as_str())
            .bind(&checkpoint.market.symbol)
            .bind(checkpoint.epoch_id)
            .bind(last_event_id)
            .bind(checkpoint.last_sequence_end.map(|value| value as i64))
            .bind(checkpoint.last_exchange_ts_ms)
            .bind(snapshot_id)
            .bind(&checkpoint.status)
            .bind(checkpoint.updated_at_ms)
            .execute(&mut *tx)
            .await?;
        }

        for gap in &batch.gaps {
            sqlx::query(
                "INSERT INTO gap_windows (
                    venue, symbol, epoch_id, started_at_ms, ended_at_ms,
                    expected_sequence, observed_sequence, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(gap.market.venue.as_str())
            .bind(&gap.market.symbol)
            .bind(gap.epoch_id)
            .bind(gap.started_at_ms)
            .bind(gap.ended_at_ms)
            .bind(gap.expected_sequence.map(|value| value as i64))
            .bind(gap.observed_sequence.map(|value| value as i64))
            .bind(&gap.reason)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

async fn persist_levels(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    snapshot_id: i64,
    book: &BookView,
) -> DynResult<()> {
    for (side, levels) in [("bid", &book.bids), ("ask", &book.asks)] {
        for (index, level) in levels.iter().enumerate() {
            let sql = format!(
                "INSERT INTO {table} (snapshot_id, side, price, quantity, level_rank)
                 VALUES (?, ?, ?, ?, ?)"
            );
            sqlx::query(&sql)
                .bind(snapshot_id)
                .bind(side)
                .bind(level.price.to_string())
                .bind(level.quantity.to_string())
                .bind(index as i64)
                .execute(&mut **tx)
                .await?;
        }
    }
    Ok(())
}

async fn persist_latest_levels(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    book: &BookView,
) -> DynResult<()> {
    for (side, levels) in [("bid", &book.bids), ("ask", &book.asks)] {
        for (index, level) in levels.iter().enumerate() {
            sqlx::query(
                "INSERT INTO latest_levels (
                    venue, symbol, side, price, quantity, level_rank, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(book.market.venue.as_str())
            .bind(&book.market.symbol)
            .bind(side)
            .bind(level.price.to_string())
            .bind(level.quantity.to_string())
            .bind(index as i64)
            .bind(book.received_ts_ms)
            .execute(&mut **tx)
            .await?;
        }
    }
    Ok(())
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

pub fn parse_event_row(row: &sqlx::sqlite::SqliteRow) -> DynResult<NormalizedBookEvent> {
    let venue: String = row.get("venue");
    let symbol: String = row.get("symbol");
    let bids_json: String = row.get("bids_json");
    let asks_json: String = row.get("asks_json");
    let raw_payload_json: String = row.get("raw_payload_json");
    let kind = match row.get::<String, _>("event_kind").as_str() {
        "snapshot" => crate::model::EventKind::Snapshot,
        "delta" => crate::model::EventKind::Delta,
        "image" => crate::model::EventKind::Image,
        "gap" => crate::model::EventKind::Gap,
        "heartbeat" => crate::model::EventKind::Heartbeat,
        other => {
            return Err(format!("unknown event_kind {other}").into());
        }
    };

    Ok(NormalizedBookEvent {
        market: parse_market_ref(&venue, &symbol),
        kind,
        exchange_ts_ms: row.try_get("exchange_ts_ms").ok().flatten(),
        received_ts_ms: row.get("received_ts_ms"),
        sequence: match row.try_get::<Option<i64>, _>("sequence_end").ok().flatten() {
            Some(end) => Some(SequenceRange {
                start: row
                    .try_get::<Option<i64>, _>("sequence_start")
                    .ok()
                    .flatten()
                    .unwrap_or(end) as u64,
                end: end as u64,
                previous_end: row
                    .try_get::<Option<i64>, _>("previous_sequence")
                    .ok()
                    .flatten()
                    .map(|value| value as u64),
                offset: row
                    .try_get::<Option<i64>, _>("offset")
                    .ok()
                    .flatten()
                    .map(|value| value as u64),
            }),
            None => None,
        },
        bids: serde_json::from_str(&bids_json)?,
        asks: serde_json::from_str(&asks_json)?,
        raw_payload: serde_json::from_str(&raw_payload_json)?,
    })
}

pub fn parse_levels(
    rows: Vec<sqlx::sqlite::SqliteRow>,
    market: MarketRef,
    received_ts_ms: i64,
) -> BookView {
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for row in rows {
        let level = PriceLevel::new(
            row.get::<String, _>("price").parse().unwrap_or_default(),
            row.get::<String, _>("quantity").parse().unwrap_or_default(),
        );
        match row.get::<String, _>("side").as_str() {
            "bid" => bids.push(level),
            "ask" => asks.push(level),
            _ => {}
        }
    }
    BookView {
        market,
        exchange_ts_ms: None,
        received_ts_ms,
        bids,
        asks,
        staleness_ms: None,
    }
}
