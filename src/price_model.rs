use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::model::{MarketRef, MarketStatus, Venue};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceKind {
    Trade,
    Reference,
    All,
}

impl PriceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trade => "trade",
            Self::Reference => "reference",
            Self::All => "all",
        }
    }

    pub fn storage_variants(self) -> &'static [Self] {
        match self {
            Self::All => &[Self::Trade, Self::Reference],
            Self::Trade => &[Self::Trade],
            Self::Reference => &[Self::Reference],
        }
    }

    pub fn is_all(self) -> bool {
        matches!(self, Self::All)
    }
}

impl std::fmt::Display for PriceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for PriceKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "trade" => Ok(Self::Trade),
            "reference" => Ok(Self::Reference),
            "all" => Ok(Self::All),
            other => Err(format!("unsupported price kind: {other}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceResolution {
    OneSecond,
    OneMinute,
    Auto,
}

impl PriceResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OneSecond => "1s",
            Self::OneMinute => "1m",
            Self::Auto => "auto",
        }
    }
}

impl std::fmt::Display for PriceResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for PriceResolution {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "1s" | "one_second" | "one-second" => Ok(Self::OneSecond),
            "1m" | "one_minute" | "one-minute" => Ok(Self::OneMinute),
            "auto" => Ok(Self::Auto),
            other => Err(format!("unsupported price resolution: {other}")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceMarket {
    pub market: MarketRef,
    pub venue_market_id: String,
    pub token: String,
    pub quote_asset: String,
    pub status: MarketStatus,
    pub supports_trade_history: bool,
    pub supports_reference_history: bool,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedPriceTick {
    pub market: MarketRef,
    pub kind: PriceKind,
    pub exchange_ts_ms: Option<i64>,
    pub received_ts_ms: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub quantity: Option<Decimal>,
    pub raw_payload: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceSample1s {
    pub market: MarketRef,
    pub kind: PriceKind,
    pub bucket_ts_ms: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub open: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub high: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub low: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub close: Decimal,
    pub sample_count: i64,
    pub first_exchange_ts_ms: Option<i64>,
    pub last_exchange_ts_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceCandle1m {
    pub market: MarketRef,
    pub kind: PriceKind,
    pub open_time_ms: i64,
    pub close_time_ms: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub open: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub high: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub low: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub close: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub volume: Decimal,
    pub trade_count: Option<i64>,
    pub source: String,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceCheckpoint {
    pub market: MarketRef,
    pub kind: PriceKind,
    pub epoch_id: i64,
    pub last_live_bucket_ms: Option<i64>,
    pub last_candle_open_ms: Option<i64>,
    pub last_backfill_open_ms: Option<i64>,
    pub last_exchange_ts_ms: Option<i64>,
    pub updated_at_ms: i64,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceGapWindow {
    pub market: MarketRef,
    pub kind: PriceKind,
    pub resolution: PriceResolution,
    pub started_at_ms: i64,
    pub ended_at_ms: i64,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PricePoint {
    pub ts_ms: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub open: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub high: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub low: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub close: Decimal,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub volume: Option<Decimal>,
    pub trade_count: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceSeries {
    pub venue: Venue,
    pub market_symbol: String,
    pub token: String,
    pub kind: PriceKind,
    pub resolution: PriceResolution,
    pub points: Vec<PricePoint>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatestPrice {
    pub venue: Venue,
    pub market_symbol: String,
    pub token: String,
    pub kind: PriceKind,
    pub resolution: PriceResolution,
    pub ts_ms: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub open: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub high: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub low: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub close: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceHealth {
    pub market: MarketRef,
    pub kind: PriceKind,
    pub status: String,
    pub updated_at_ms: i64,
    pub last_live_bucket_ms: Option<i64>,
    pub last_candle_open_ms: Option<i64>,
    pub last_backfill_open_ms: Option<i64>,
    pub last_gap_at_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceRangeRequest {
    pub token: Option<String>,
    pub venue: Option<Venue>,
    pub market_symbol: Option<String>,
    pub kind: PriceKind,
    pub start_ms: i64,
    pub end_ms: i64,
    pub resolution: PriceResolution,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceHistoryRequest {
    pub market: PriceMarket,
    pub kind: PriceKind,
    pub start_ms: i64,
    pub end_ms: i64,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceCommitBatch {
    pub market: MarketRef,
    pub kind: PriceKind,
    pub epoch_id: Option<i64>,
    pub samples_1s: Vec<PriceSample1s>,
    pub candles_1m: Vec<PriceCandle1m>,
    pub checkpoint: Option<PriceCheckpoint>,
    pub gaps: Vec<PriceGapWindow>,
}
