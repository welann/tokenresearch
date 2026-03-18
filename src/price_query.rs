use serde::{Deserialize, Serialize};
use sqlx::Row;
use thiserror::Error;

use crate::model::{MarketRef, Venue};
use crate::price_model::{
    LatestPrice, PriceGapWindow, PriceHealth, PriceKind, PriceMarket, PricePoint,
    PriceRangeRequest, PriceResolution, PriceSeries,
};
use crate::price_storage::{
    SqlitePriceStore, parse_market_ref, parse_price_candle_row, parse_price_health_row,
    parse_price_kind, parse_price_market_row, parse_price_resolution, parse_price_sample_row,
};

const ONE_SECOND_RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1_000;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
}

#[derive(Debug, Error)]
pub enum PriceQueryError {
    #[error("sql error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("decimal parse error: {0}")]
    Decimal(#[from] rust_decimal::Error),
    #[error("gap covers requested time")]
    GapCovered,
    #[error("one second data unavailable: {0}")]
    OneSecondUnavailable(String),
    #[error("price not found")]
    NotFound,
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("other error: {0}")]
    Other(String),
}

#[derive(Clone, Debug)]
pub struct PriceQueryStore {
    store: SqlitePriceStore,
}

impl PriceQueryStore {
    pub fn new(store: SqlitePriceStore) -> Self {
        Self { store }
    }

    pub async fn list_price_markets(
        &self,
        venue: Option<Venue>,
    ) -> Result<Vec<PriceMarket>, PriceQueryError> {
        let rows = if let Some(venue) = venue {
            sqlx::query(
                "SELECT venue, symbol, venue_market_id, token, quote_asset, status,
                        supports_trade_history, supports_reference_history, updated_at_ms
                 FROM price_markets WHERE venue = ? ORDER BY symbol",
            )
            .bind(venue.as_str())
            .fetch_all(self.store.pool())
            .await?
        } else {
            sqlx::query(
                "SELECT venue, symbol, venue_market_id, token, quote_asset, status,
                        supports_trade_history, supports_reference_history, updated_at_ms
                 FROM price_markets ORDER BY venue, symbol",
            )
            .fetch_all(self.store.pool())
            .await?
        };

        rows.into_iter()
            .map(parse_price_market_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| PriceQueryError::Other(error.to_string()))
    }

    pub async fn latest_price(
        &self,
        token: &str,
        kind: PriceKind,
        venue: Option<Venue>,
        market_symbol: Option<&str>,
    ) -> Result<Vec<LatestPrice>, PriceQueryError> {
        let markets = self
            .matching_markets(Some(token), venue, market_symbol)
            .await?;
        let mut results = Vec::new();
        for market in markets {
            for kind in kind.storage_variants() {
                if let Some(row) = sqlx::query(
                    "SELECT venue, symbol, price_kind, bucket_ts_ms AS ts_ms,
                            open_price, high_price, low_price, close_price
                     FROM price_samples_1s
                     WHERE venue = ? AND symbol = ? AND price_kind = ?
                     ORDER BY bucket_ts_ms DESC LIMIT 1",
                )
                .bind(market.market.venue.as_str())
                .bind(&market.market.symbol)
                .bind(kind.as_str())
                .fetch_optional(self.store.pool())
                .await?
                {
                    results.push(LatestPrice {
                        venue: market.market.venue,
                        market_symbol: market.market.symbol.clone(),
                        token: market.token.clone(),
                        kind: *kind,
                        resolution: PriceResolution::OneSecond,
                        ts_ms: row.get("ts_ms"),
                        open: row.get::<String, _>("open_price").parse()?,
                        high: row.get::<String, _>("high_price").parse()?,
                        low: row.get::<String, _>("low_price").parse()?,
                        close: row.get::<String, _>("close_price").parse()?,
                    });
                    continue;
                }

                if let Some(row) = sqlx::query(
                    "SELECT venue, symbol, price_kind, open_time_ms AS ts_ms,
                            open_price, high_price, low_price, close_price
                     FROM price_candles_1m
                     WHERE venue = ? AND symbol = ? AND price_kind = ?
                     ORDER BY open_time_ms DESC LIMIT 1",
                )
                .bind(market.market.venue.as_str())
                .bind(&market.market.symbol)
                .bind(kind.as_str())
                .fetch_optional(self.store.pool())
                .await?
                {
                    results.push(LatestPrice {
                        venue: market.market.venue,
                        market_symbol: market.market.symbol.clone(),
                        token: market.token.clone(),
                        kind: *kind,
                        resolution: PriceResolution::OneMinute,
                        ts_ms: row.get("ts_ms"),
                        open: row.get::<String, _>("open_price").parse()?,
                        high: row.get::<String, _>("high_price").parse()?,
                        low: row.get::<String, _>("low_price").parse()?,
                        close: row.get::<String, _>("close_price").parse()?,
                    });
                }
            }
        }

        if results.is_empty() {
            return Err(PriceQueryError::NotFound);
        }

        Ok(results)
    }

    pub async fn price_range(
        &self,
        request: PriceRangeRequest,
    ) -> Result<Vec<PriceSeries>, PriceQueryError> {
        if request.end_ms < request.start_ms {
            return Err(PriceQueryError::InvalidRequest(
                "end_ms must be >= start_ms".to_string(),
            ));
        }

        let markets = self
            .matching_markets(
                request.token.as_deref(),
                request.venue,
                request.market_symbol.as_deref(),
            )
            .await?;
        if markets.is_empty() {
            return Err(PriceQueryError::NotFound);
        }

        let resolution = self.resolve_resolution(&request, &markets).await?;
        let mut series = Vec::new();
        for market in markets {
            for kind in request.kind.storage_variants() {
                let points = match resolution {
                    PriceResolution::OneSecond => {
                        self.load_sample_points(
                            &market.market,
                            *kind,
                            request.start_ms,
                            request.end_ms,
                        )
                        .await?
                    }
                    PriceResolution::OneMinute => {
                        self.load_candle_points(
                            &market.market,
                            *kind,
                            request.start_ms,
                            request.end_ms,
                        )
                        .await?
                    }
                    PriceResolution::Auto => unreachable!("auto should be resolved"),
                };
                if points.is_empty() {
                    continue;
                }
                series.push(PriceSeries {
                    venue: market.market.venue,
                    market_symbol: market.market.symbol.clone(),
                    token: market.token.clone(),
                    kind: *kind,
                    resolution,
                    points,
                });
            }
        }

        if series.is_empty() {
            return Err(PriceQueryError::NotFound);
        }

        Ok(series)
    }

    pub async fn price_gaps(
        &self,
        token: &str,
        venue: Option<Venue>,
        range: TimeRange,
    ) -> Result<Vec<PriceGapWindow>, PriceQueryError> {
        let markets = self.matching_markets(Some(token), venue, None).await?;
        if markets.is_empty() {
            return Ok(Vec::new());
        }
        let start_ms = range.start_ms.unwrap_or(i64::MIN);
        let end_ms = range.end_ms.unwrap_or(i64::MAX);
        let mut gaps = Vec::new();
        for market in markets {
            let rows = sqlx::query(
                "SELECT venue, symbol, price_kind, resolution, started_at_ms, ended_at_ms, reason
                 FROM price_gap_windows
                 WHERE venue = ? AND symbol = ? AND ended_at_ms >= ? AND started_at_ms <= ?
                 ORDER BY started_at_ms ASC",
            )
            .bind(market.market.venue.as_str())
            .bind(&market.market.symbol)
            .bind(start_ms)
            .bind(end_ms)
            .fetch_all(self.store.pool())
            .await?;
            for row in rows {
                let venue: String = row.get("venue");
                let symbol: String = row.get("symbol");
                let kind: String = row.get("price_kind");
                let resolution: String = row.get("resolution");
                gaps.push(PriceGapWindow {
                    market: parse_market_ref(&venue, &symbol),
                    kind: parse_price_kind(&kind)
                        .map_err(|error| PriceQueryError::Other(error.to_string()))?,
                    resolution: parse_price_resolution(&resolution)
                        .map_err(|error| PriceQueryError::Other(error.to_string()))?,
                    started_at_ms: row.get("started_at_ms"),
                    ended_at_ms: row.get("ended_at_ms"),
                    reason: row.get("reason"),
                });
            }
        }
        Ok(gaps)
    }

    pub async fn price_health(
        &self,
        venue: Venue,
        market_symbol: &str,
        kind: PriceKind,
    ) -> Result<Option<PriceHealth>, PriceQueryError> {
        let row = sqlx::query(
            "SELECT venue, symbol, price_kind, status, updated_at_ms,
                    last_live_bucket_ms, last_candle_open_ms, last_backfill_open_ms, last_gap_at_ms
             FROM v_price_health WHERE venue = ? AND symbol = ? AND price_kind = ?",
        )
        .bind(venue.as_str())
        .bind(market_symbol)
        .bind(kind.as_str())
        .fetch_optional(self.store.pool())
        .await?;

        row.map(parse_price_health_row)
            .transpose()
            .map_err(|error| PriceQueryError::Other(error.to_string()))
    }

    async fn matching_markets(
        &self,
        token: Option<&str>,
        venue: Option<Venue>,
        market_symbol: Option<&str>,
    ) -> Result<Vec<PriceMarket>, PriceQueryError> {
        let mut markets = self.list_price_markets(venue).await?;
        if let Some(token) = token {
            markets.retain(|market| market.token.eq_ignore_ascii_case(token));
        }
        if let Some(symbol) = market_symbol {
            markets.retain(|market| market.market.symbol.eq_ignore_ascii_case(symbol));
        }
        Ok(markets)
    }

    async fn resolve_resolution(
        &self,
        request: &PriceRangeRequest,
        markets: &[PriceMarket],
    ) -> Result<PriceResolution, PriceQueryError> {
        match request.resolution {
            PriceResolution::OneMinute => Ok(PriceResolution::OneMinute),
            PriceResolution::OneSecond => {
                self.ensure_one_second_coverage(markets, request).await?;
                Ok(PriceResolution::OneSecond)
            }
            PriceResolution::Auto => {
                if self
                    .ensure_one_second_coverage(markets, request)
                    .await
                    .is_ok()
                {
                    Ok(PriceResolution::OneSecond)
                } else {
                    Ok(PriceResolution::OneMinute)
                }
            }
        }
    }

    async fn ensure_one_second_coverage(
        &self,
        markets: &[PriceMarket],
        request: &PriceRangeRequest,
    ) -> Result<(), PriceQueryError> {
        let newest_bucket =
            sqlx::query("SELECT MAX(bucket_ts_ms) AS max_bucket_ts_ms FROM price_samples_1s")
                .fetch_one(self.store.pool())
                .await?
                .try_get::<Option<i64>, _>("max_bucket_ts_ms")
                .ok()
                .flatten()
                .ok_or_else(|| {
                    PriceQueryError::OneSecondUnavailable(
                        "no one second samples available".to_string(),
                    )
                })?;

        if request.start_ms < newest_bucket - ONE_SECOND_RETENTION_MS {
            return Err(PriceQueryError::OneSecondUnavailable(
                "requested range is outside 30d retention".to_string(),
            ));
        }

        for market in markets {
            for kind in request.kind.storage_variants() {
                let gap_count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM price_gap_windows
                     WHERE venue = ? AND symbol = ? AND price_kind = ? AND resolution = '1s'
                       AND ended_at_ms >= ? AND started_at_ms <= ?",
                )
                .bind(market.market.venue.as_str())
                .bind(&market.market.symbol)
                .bind(kind.as_str())
                .bind(request.start_ms)
                .bind(request.end_ms)
                .fetch_one(self.store.pool())
                .await?;
                if gap_count > 0 {
                    return Err(PriceQueryError::GapCovered);
                }

                let sample_count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM price_samples_1s
                     WHERE venue = ? AND symbol = ? AND price_kind = ?
                       AND bucket_ts_ms >= ? AND bucket_ts_ms <= ?",
                )
                .bind(market.market.venue.as_str())
                .bind(&market.market.symbol)
                .bind(kind.as_str())
                .bind(request.start_ms)
                .bind(request.end_ms)
                .fetch_one(self.store.pool())
                .await?;
                if sample_count == 0 {
                    return Err(PriceQueryError::OneSecondUnavailable(format!(
                        "no one second samples for {} {} {}",
                        market.market.venue, market.market.symbol, kind
                    )));
                }
            }
        }

        Ok(())
    }

    async fn load_sample_points(
        &self,
        market: &MarketRef,
        kind: PriceKind,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<PricePoint>, PriceQueryError> {
        let rows = sqlx::query(
            "SELECT venue, symbol, price_kind, bucket_ts_ms, open_price, high_price,
                    low_price, close_price, sample_count, first_exchange_ts_ms,
                    last_exchange_ts_ms, updated_at_ms
             FROM price_samples_1s
             WHERE venue = ? AND symbol = ? AND price_kind = ?
               AND bucket_ts_ms >= ? AND bucket_ts_ms <= ?
             ORDER BY bucket_ts_ms ASC",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(kind.as_str())
        .bind(start_ms)
        .bind(end_ms)
        .fetch_all(self.store.pool())
        .await?;

        rows.iter()
            .map(|row| {
                let sample = parse_price_sample_row(row)
                    .map_err(|error| PriceQueryError::Other(error.to_string()))?;
                Ok(PricePoint {
                    ts_ms: sample.bucket_ts_ms,
                    open: sample.open,
                    high: sample.high,
                    low: sample.low,
                    close: sample.close,
                    volume: None,
                    trade_count: Some(sample.sample_count),
                })
            })
            .collect()
    }

    async fn load_candle_points(
        &self,
        market: &MarketRef,
        kind: PriceKind,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<PricePoint>, PriceQueryError> {
        let rows = sqlx::query(
            "SELECT venue, symbol, price_kind, open_time_ms, close_time_ms, open_price,
                    high_price, low_price, close_price, volume, trade_count, source, updated_at_ms
             FROM price_candles_1m
             WHERE venue = ? AND symbol = ? AND price_kind = ?
               AND open_time_ms >= ? AND open_time_ms <= ?
             ORDER BY open_time_ms ASC",
        )
        .bind(market.venue.as_str())
        .bind(&market.symbol)
        .bind(kind.as_str())
        .bind(start_ms)
        .bind(end_ms)
        .fetch_all(self.store.pool())
        .await?;

        rows.iter()
            .map(|row| {
                let candle = parse_price_candle_row(row)
                    .map_err(|error| PriceQueryError::Other(error.to_string()))?;
                Ok(PricePoint {
                    ts_ms: candle.open_time_ms,
                    open: candle.open,
                    high: candle.high,
                    low: candle.low,
                    close: candle.close,
                    volume: Some(candle.volume),
                    trade_count: candle.trade_count,
                })
            })
            .collect()
    }
}
