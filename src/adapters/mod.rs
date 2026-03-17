mod binance;
mod hyperliquid;
mod lighter;

pub use binance::BinanceAdapter;
pub use hyperliquid::HyperliquidAdapter;
pub use lighter::LighterAdapter;

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

use crate::model::{NormalizedBookEvent, NormalizedMarket, Venue};

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("missing field: {0}")]
    MissingField(&'static str),
    #[error("invalid field {field}: {message}")]
    InvalidField {
        field: &'static str,
        message: String,
    },
    #[error("unsupported payload: {0}")]
    Unsupported(String),
}

#[derive(Clone, Debug)]
pub struct DiscoveryRequest {
    pub method: HttpMethod,
    pub url: String,
    pub body: Option<Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
}

#[async_trait]
pub trait VenueAdapter: Send + Sync {
    fn venue(&self) -> Venue;
    fn discovery_request(&self) -> DiscoveryRequest;
    fn discover_markets(&self, body: &str) -> Result<Vec<NormalizedMarket>, AdapterError>;
    fn ws_url(&self, markets: &[NormalizedMarket]) -> String;
    fn subscription_messages(&self, markets: &[NormalizedMarket]) -> Vec<String>;
    fn parse_ws_message(
        &self,
        raw: &str,
        received_ts_ms: i64,
    ) -> Result<Option<NormalizedBookEvent>, AdapterError>;
    fn snapshot_request(&self, market: &NormalizedMarket) -> Option<DiscoveryRequest>;
    fn parse_snapshot(
        &self,
        market: &NormalizedMarket,
        body: &str,
        received_ts_ms: i64,
    ) -> Result<NormalizedBookEvent, AdapterError>;
}

fn decimal_from_value(
    value: &Value,
    field: &'static str,
) -> Result<rust_decimal::Decimal, AdapterError> {
    let raw = if let Some(text) = value.as_str() {
        text.to_string()
    } else if value.is_number() {
        value.to_string()
    } else {
        return Err(AdapterError::InvalidField {
            field,
            message: "expected string or number".to_string(),
        });
    };

    raw.parse().map_err(|error| AdapterError::InvalidField {
        field,
        message: format!("decimal parse failed: {error}"),
    })
}

fn decimals_from_step(step: &str) -> u32 {
    step.split('.')
        .nth(1)
        .map(|fraction| fraction.trim_end_matches('0').len() as u32)
        .unwrap_or(0)
}
