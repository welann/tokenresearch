use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Binance,
    Hyperliquid,
    Lighter,
}

impl Venue {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Hyperliquid => "hyperliquid",
            Self::Lighter => "lighter",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketType {
    Perpetual,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketStatus {
    Active,
    Inactive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketRef {
    pub venue: Venue,
    pub symbol: String,
}

impl MarketRef {
    pub fn new(venue: Venue, symbol: impl Into<String>) -> Self {
        Self {
            venue,
            symbol: symbol.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedMarket {
    pub market: MarketRef,
    pub venue_market_id: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub market_type: MarketType,
    pub status: MarketStatus,
    pub price_decimals: u32,
    pub size_decimals: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceLevel {
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
}

impl PriceLevel {
    pub fn new(price: Decimal, quantity: Decimal) -> Self {
        Self { price, quantity }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceRange {
    pub start: u64,
    pub end: u64,
    pub previous_end: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Snapshot,
    Delta,
    Image,
    Gap,
    Heartbeat,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedBookEvent {
    pub market: MarketRef,
    pub kind: EventKind,
    pub exchange_ts_ms: Option<i64>,
    pub received_ts_ms: i64,
    pub sequence: Option<SequenceRange>,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub raw_payload: Value,
}

impl NormalizedBookEvent {
    pub fn heartbeat(market: MarketRef, received_ts_ms: i64, raw_payload: Value) -> Self {
        Self {
            market,
            kind: EventKind::Heartbeat,
            exchange_ts_ms: None,
            received_ts_ms,
            sequence: None,
            bids: Vec::new(),
            asks: Vec::new(),
            raw_payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapWindow {
    pub market: MarketRef,
    pub epoch_id: Option<i64>,
    pub started_at_ms: i64,
    pub ended_at_ms: i64,
    pub expected_sequence: Option<u64>,
    pub observed_sequence: Option<u64>,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorCheckpoint {
    pub market: MarketRef,
    pub epoch_id: i64,
    pub last_sequence_end: Option<u64>,
    pub last_exchange_ts_ms: Option<i64>,
    pub last_snapshot_at_ms: Option<i64>,
    pub updated_at_ms: i64,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorHealth {
    pub market: MarketRef,
    pub status: String,
    pub updated_at_ms: i64,
    pub last_sequence_end: Option<u64>,
    pub last_gap_at_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookView {
    pub market: MarketRef,
    pub exchange_ts_ms: Option<i64>,
    pub received_ts_ms: i64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub staleness_ms: Option<i64>,
}
