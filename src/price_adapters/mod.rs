mod binance;
mod hyperliquid;
mod lighter;

pub use binance::BinancePriceAdapter;
pub use hyperliquid::HyperliquidPriceAdapter;
pub use lighter::LighterPriceAdapter;

use async_trait::async_trait;
use serde_json::Value;

use crate::adapters::{AdapterError, DiscoveryRequest};
use crate::model::Venue;
use crate::price_model::{NormalizedPriceTick, PriceCandle1m, PriceHistoryRequest, PriceMarket};

#[async_trait]
pub trait PriceVenueAdapter: Send + Sync {
    fn venue(&self) -> Venue;
    fn discovery_request(&self) -> DiscoveryRequest;
    fn discover_markets(&self, body: &str) -> Result<Vec<PriceMarket>, AdapterError>;
    fn ws_url(&self) -> String;
    fn subscription_messages(&self, markets: &[PriceMarket]) -> Vec<String>;
    fn parse_ws_message(
        &self,
        raw: &str,
        received_ts_ms: i64,
    ) -> Result<Option<NormalizedPriceTick>, AdapterError>;
    fn parse_ws_message_ticks(
        &self,
        raw: &str,
        received_ts_ms: i64,
    ) -> Result<Vec<NormalizedPriceTick>, AdapterError> {
        Ok(self
            .parse_ws_message(raw, received_ts_ms)?
            .into_iter()
            .collect())
    }
    fn history_request(&self, request: PriceHistoryRequest) -> Option<DiscoveryRequest>;
    fn parse_history_candles(
        &self,
        market: &PriceMarket,
        kind: crate::price_model::PriceKind,
        body: &str,
    ) -> Result<Vec<PriceCandle1m>, AdapterError>;
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

    raw.parse()
        .or_else(|_| rust_decimal::Decimal::from_scientific(&raw))
        .map_err(|error| AdapterError::InvalidField {
            field,
            message: format!("decimal parse failed: {error}"),
        })
}
