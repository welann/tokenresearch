use sqlx::Row;
use thiserror::Error;

use crate::book::OrderBook;
use crate::model::{
    BookView, CollectorHealth, GapWindow, MarketRef, MarketStatus, MarketType, NormalizedBookEvent,
    NormalizedMarket, Venue,
};
use crate::storage::{SqliteBookStore, parse_event_row, parse_levels, parse_market_ref};

#[derive(Clone, Debug, Default)]
pub struct TimeRange {
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub id: i64,
    pub market: MarketRef,
    pub created_at_ms: i64,
    pub depth: usize,
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("sql error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("gap covers requested time")]
    GapCovered,
    #[error("book not found")]
    NotFound,
    #[error("other error: {0}")]
    Other(String),
}

#[derive(Clone, Debug)]
pub struct QueryStore {
    store: SqliteBookStore,
}

impl QueryStore {
    pub fn new(store: SqliteBookStore) -> Self {
        Self { store }
    }

    pub async fn list_markets(
        &self,
        venue: Option<Venue>,
    ) -> Result<Vec<NormalizedMarket>, QueryError> {
        let rows = if let Some(venue) = venue {
            sqlx::query(
                "SELECT venue, symbol, venue_market_id, base_asset, quote_asset,
                    market_type, status, price_decimals, size_decimals
                 FROM markets WHERE venue = ? ORDER BY symbol",
            )
            .bind(venue.as_str())
            .fetch_all(self.store.pool())
            .await?
        } else {
            sqlx::query(
                "SELECT venue, symbol, venue_market_id, base_asset, quote_asset,
                    market_type, status, price_decimals, size_decimals
                 FROM markets ORDER BY venue, symbol",
            )
            .fetch_all(self.store.pool())
            .await?
        };

        Ok(rows
            .into_iter()
            .map(|row| {
                let venue = row.get::<String, _>("venue");
                let symbol = row.get::<String, _>("symbol");
                NormalizedMarket {
                    market: parse_market_ref(&venue, &symbol),
                    venue_market_id: row.get("venue_market_id"),
                    base_asset: row.get("base_asset"),
                    quote_asset: row.get("quote_asset"),
                    market_type: match row.get::<String, _>("market_type").as_str() {
                        "perpetual" => MarketType::Perpetual,
                        _ => MarketType::Perpetual,
                    },
                    status: match row.get::<String, _>("status").as_str() {
                        "active" => MarketStatus::Active,
                        _ => MarketStatus::Inactive,
                    },
                    price_decimals: row.get::<i64, _>("price_decimals") as u32,
                    size_decimals: row.get::<i64, _>("size_decimals") as u32,
                }
            })
            .collect())
    }

    pub async fn latest_book(
        &self,
        market: &MarketRef,
        depth: usize,
    ) -> Result<Option<BookView>, QueryError> {
        let rows = sqlx::query(
            "SELECT side, price, quantity, updated_at_ms
             FROM latest_levels
             WHERE venue = ? AND symbol = ? AND level_rank < ?
             ORDER BY side, level_rank",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(depth as i64)
        .fetch_all(self.store.pool())
        .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let received_ts_ms = rows[0].get::<i64, _>("updated_at_ms");
        let mut view = parse_levels(rows, market.clone(), received_ts_ms);
        view.staleness_ms = Some(0);
        Ok(Some(view))
    }

    pub async fn events(
        &self,
        market: &MarketRef,
        range: TimeRange,
        limit: usize,
    ) -> Result<Vec<NormalizedBookEvent>, QueryError> {
        let start_ms = range.start_ms.unwrap_or(i64::MIN);
        let end_ms = range.end_ms.unwrap_or(i64::MAX);
        let rows = sqlx::query(
            "SELECT venue, symbol, event_kind, exchange_ts_ms, received_ts_ms,
                    sequence_start, sequence_end, previous_sequence, offset,
                    bids_json, asks_json, raw_payload_json
             FROM book_events
             WHERE venue = ? AND symbol = ? AND received_ts_ms >= ? AND received_ts_ms <= ?
             ORDER BY id ASC LIMIT ?",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(start_ms)
        .bind(end_ms)
        .bind(limit as i64)
        .fetch_all(self.store.pool())
        .await?;

        rows.iter()
            .map(parse_event_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| QueryError::Other(error.to_string()))
    }

    pub async fn snapshots(
        &self,
        market: &MarketRef,
        range: TimeRange,
        limit: usize,
    ) -> Result<Vec<SnapshotMeta>, QueryError> {
        let start_ms = range.start_ms.unwrap_or(i64::MIN);
        let end_ms = range.end_ms.unwrap_or(i64::MAX);
        let rows = sqlx::query(
            "SELECT id, created_at_ms, depth
             FROM book_snapshots
             WHERE venue = ? AND symbol = ? AND created_at_ms >= ? AND created_at_ms <= ?
             ORDER BY id DESC LIMIT ?",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(start_ms)
        .bind(end_ms)
        .bind(limit as i64)
        .fetch_all(self.store.pool())
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| SnapshotMeta {
                id: row.get("id"),
                market: market.clone(),
                created_at_ms: row.get("created_at_ms"),
                depth: row.get::<i64, _>("depth") as usize,
            })
            .collect())
    }

    pub async fn gaps(
        &self,
        market: &MarketRef,
        range: TimeRange,
    ) -> Result<Vec<GapWindow>, QueryError> {
        let start_ms = range.start_ms.unwrap_or(i64::MIN);
        let end_ms = range.end_ms.unwrap_or(i64::MAX);
        let rows = sqlx::query(
            "SELECT started_at_ms, ended_at_ms, epoch_id, expected_sequence, observed_sequence, reason
             FROM gap_windows
             WHERE venue = ? AND symbol = ? AND ended_at_ms >= ? AND started_at_ms <= ?
             ORDER BY started_at_ms ASC",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(start_ms)
        .bind(end_ms)
        .fetch_all(self.store.pool())
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| GapWindow {
                market: market.clone(),
                epoch_id: row.try_get("epoch_id").ok().flatten(),
                started_at_ms: row.get("started_at_ms"),
                ended_at_ms: row.get("ended_at_ms"),
                expected_sequence: row
                    .try_get::<Option<i64>, _>("expected_sequence")
                    .ok()
                    .flatten()
                    .map(|value| value as u64),
                observed_sequence: row
                    .try_get::<Option<i64>, _>("observed_sequence")
                    .ok()
                    .flatten()
                    .map(|value| value as u64),
                reason: row.get("reason"),
            })
            .collect())
    }

    pub async fn collector_state(
        &self,
        market: &MarketRef,
    ) -> Result<Option<CollectorHealth>, QueryError> {
        let row = sqlx::query(
            "SELECT status, updated_at_ms, last_sequence_end, last_gap_at_ms
             FROM v_market_health WHERE venue = ? AND symbol = ?",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .fetch_optional(self.store.pool())
        .await?;

        Ok(row.map(|row| CollectorHealth {
            market: market.clone(),
            status: row.get("status"),
            updated_at_ms: row.get("updated_at_ms"),
            last_sequence_end: row
                .try_get::<Option<i64>, _>("last_sequence_end")
                .ok()
                .flatten()
                .map(|value| value as u64),
            last_gap_at_ms: row.try_get("last_gap_at_ms").ok().flatten(),
        }))
    }

    pub async fn book_at(
        &self,
        market: &MarketRef,
        ts_ms: i64,
        depth: usize,
    ) -> Result<BookView, QueryError> {
        let gap = sqlx::query(
            "SELECT 1 FROM gap_windows
             WHERE venue = ? AND symbol = ? AND started_at_ms <= ? AND ended_at_ms >= ?
             LIMIT 1",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(ts_ms)
        .bind(ts_ms)
        .fetch_optional(self.store.pool())
        .await?;
        if gap.is_some() {
            return Err(QueryError::GapCovered);
        }

        let snapshot_row = sqlx::query(
            "SELECT id, created_at_ms
             FROM book_snapshots
             WHERE venue = ? AND symbol = ? AND created_at_ms <= ?
             ORDER BY created_at_ms DESC LIMIT 1",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(ts_ms)
        .fetch_optional(self.store.pool())
        .await?;

        let mut book = OrderBook::new(market.clone());
        let anchor_ts = if let Some(snapshot_row) = snapshot_row {
            let snapshot_id: i64 = snapshot_row.get("id");
            let rows = sqlx::query(
                "SELECT side, price, quantity
                 FROM book_snapshot_levels
                 WHERE snapshot_id = ?
                 ORDER BY side, level_rank",
            )
            .bind(snapshot_id)
            .fetch_all(self.store.pool())
            .await?;
            let snapshot_book =
                parse_levels(rows, market.clone(), snapshot_row.get("created_at_ms"));
            book.apply_snapshot(
                &snapshot_book.bids,
                &snapshot_book.asks,
                snapshot_book.exchange_ts_ms,
                snapshot_book.received_ts_ms,
            );
            snapshot_book.received_ts_ms
        } else {
            let row = sqlx::query(
                "SELECT venue, symbol, event_kind, exchange_ts_ms, received_ts_ms,
                        sequence_start, sequence_end, previous_sequence, offset,
                        bids_json, asks_json, raw_payload_json
                 FROM book_events
                 WHERE venue = ? AND symbol = ? AND event_kind IN ('image', 'snapshot')
                   AND received_ts_ms <= ?
                 ORDER BY id DESC LIMIT 1",
            )
            .bind(market.venue.as_str())
            .bind(&market.symbol)
            .bind(ts_ms)
            .fetch_optional(self.store.pool())
            .await?;

            let row = row.ok_or(QueryError::NotFound)?;
            let event =
                parse_event_row(&row).map_err(|error| QueryError::Other(error.to_string()))?;
            book.apply_event(&event);
            event.received_ts_ms
        };

        let rows = sqlx::query(
            "SELECT venue, symbol, event_kind, exchange_ts_ms, received_ts_ms,
                    sequence_start, sequence_end, previous_sequence, offset,
                    bids_json, asks_json, raw_payload_json
             FROM book_events
             WHERE venue = ? AND symbol = ? AND received_ts_ms > ? AND received_ts_ms <= ?
             ORDER BY id ASC",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(anchor_ts)
        .bind(ts_ms)
        .fetch_all(self.store.pool())
        .await?;

        for row in rows {
            let event =
                parse_event_row(&row).map_err(|error| QueryError::Other(error.to_string()))?;
            book.apply_event(&event);
        }

        Ok(book.view(depth))
    }
}
