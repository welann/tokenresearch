use std::collections::HashMap;
use std::sync::RwLock;

use serde_json::{Value, json};

use crate::adapters::{
    AdapterError, DiscoveryRequest, HttpMethod, VenueAdapter, decimal_from_value,
};
use crate::model::{
    EventKind, MarketRef, MarketStatus, MarketType, NormalizedBookEvent, NormalizedMarket,
    PriceLevel, SequenceRange, Venue,
};

#[derive(Debug, Default)]
pub struct LighterAdapter {
    symbol_by_market_id: RwLock<HashMap<String, String>>,
}

impl Clone for LighterAdapter {
    fn clone(&self) -> Self {
        let snapshot = self
            .symbol_by_market_id
            .read()
            .expect("rwlock poisoned")
            .clone();
        Self {
            symbol_by_market_id: RwLock::new(snapshot),
        }
    }
}

impl LighterAdapter {
    fn parse_level(entry: &Value) -> Result<PriceLevel, AdapterError> {
        let price = entry
            .get("price")
            .or_else(|| entry.get("px"))
            .ok_or(AdapterError::MissingField("price"))?;
        let size = entry
            .get("size")
            .or_else(|| entry.get("quantity"))
            .or_else(|| entry.get("qty"))
            .ok_or(AdapterError::MissingField("size"))?;

        Ok(PriceLevel::new(
            decimal_from_value(price, "price")?,
            decimal_from_value(size, "size")?,
        ))
    }

    fn parse_side(value: &Value, field: &'static str) -> Result<Vec<PriceLevel>, AdapterError> {
        value
            .as_array()
            .ok_or(AdapterError::MissingField(field))?
            .iter()
            .map(Self::parse_level)
            .collect()
    }
}

impl VenueAdapter for LighterAdapter {
    fn venue(&self) -> Venue {
        Venue::Lighter
    }

    fn discovery_request(&self) -> DiscoveryRequest {
        DiscoveryRequest {
            method: HttpMethod::Get,
            url: "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks".to_string(),
            body: None,
        }
    }

    fn discover_markets(&self, body: &str) -> Result<Vec<NormalizedMarket>, AdapterError> {
        let value: Value = serde_json::from_str(body)?;
        let markets = value
            .get("order_books")
            .and_then(Value::as_array)
            .ok_or(AdapterError::MissingField("order_books"))?;

        let mut id_map = self.symbol_by_market_id.write().expect("rwlock poisoned");
        id_map.clear();

        let mut normalized = Vec::new();
        for market in markets {
            let market_type = market
                .get("market_type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if market_type != "perp" {
                continue;
            }
            let symbol = market
                .get("symbol")
                .and_then(Value::as_str)
                .ok_or(AdapterError::MissingField("symbol"))?;
            let market_id = market
                .get("market_id")
                .and_then(Value::as_i64)
                .ok_or(AdapterError::MissingField("market_id"))?;
            let status = market
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("inactive");

            id_map.insert(market_id.to_string(), symbol.to_string());
            normalized.push(NormalizedMarket {
                market: MarketRef::new(Venue::Lighter, symbol),
                venue_market_id: market_id.to_string(),
                base_asset: symbol.to_string(),
                quote_asset: "USDC".to_string(),
                market_type: MarketType::Perpetual,
                status: if status == "active" {
                    MarketStatus::Active
                } else {
                    MarketStatus::Inactive
                },
                price_decimals: market
                    .get("supported_price_decimals")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as u32,
                size_decimals: market
                    .get("supported_size_decimals")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as u32,
            });
        }

        Ok(normalized)
    }

    fn ws_url(&self, _markets: &[NormalizedMarket]) -> String {
        "wss://mainnet.zklighter.elliot.ai/stream?readonly=true".to_string()
    }

    fn subscription_messages(&self, markets: &[NormalizedMarket]) -> Vec<String> {
        markets
            .iter()
            .map(|market| {
                json!({
                    "type": "subscribe",
                    "channel": format!("order_book/{}", market.venue_market_id),
                })
                .to_string()
            })
            .collect()
    }

    fn parse_ws_message(
        &self,
        raw: &str,
        received_ts_ms: i64,
    ) -> Result<Option<NormalizedBookEvent>, AdapterError> {
        let parsed: Value = serde_json::from_str(raw)?;
        if parsed.get("type").and_then(Value::as_str) == Some("subscribed") {
            return Ok(None);
        }

        let payload = parsed
            .get("data")
            .or_else(|| parsed.get("order_book"))
            .unwrap_or(&parsed);
        let market_id = payload
            .get("market_id")
            .and_then(Value::as_i64)
            .map(|value| value.to_string())
            .or_else(|| {
                parsed
                    .get("channel")
                    .and_then(Value::as_str)
                    .and_then(|channel| channel.rsplit('/').next().map(str::to_string))
            })
            .ok_or(AdapterError::MissingField("market_id"))?;

        let symbol = self
            .symbol_by_market_id
            .read()
            .expect("rwlock poisoned")
            .get(&market_id)
            .cloned()
            .unwrap_or_else(|| market_id.clone());

        Ok(Some(NormalizedBookEvent {
            market: MarketRef::new(Venue::Lighter, symbol),
            kind: EventKind::Delta,
            exchange_ts_ms: payload
                .get("timestamp")
                .and_then(Value::as_i64)
                .or_else(|| parsed.get("timestamp").and_then(Value::as_i64)),
            received_ts_ms,
            sequence: Some(SequenceRange {
                start: payload
                    .get("begin_nonce")
                    .and_then(Value::as_u64)
                    .map(|value| value + 1)
                    .unwrap_or(0),
                end: payload
                    .get("nonce")
                    .and_then(Value::as_u64)
                    .ok_or(AdapterError::MissingField("nonce"))?,
                previous_end: payload.get("begin_nonce").and_then(Value::as_u64),
                offset: payload.get("offset").and_then(Value::as_u64),
            }),
            bids: Self::parse_side(
                payload
                    .get("bids")
                    .ok_or(AdapterError::MissingField("bids"))?,
                "bids",
            )?,
            asks: Self::parse_side(
                payload
                    .get("asks")
                    .ok_or(AdapterError::MissingField("asks"))?,
                "asks",
            )?,
            raw_payload: parsed,
        }))
    }

    fn snapshot_request(&self, _market: &NormalizedMarket) -> Option<DiscoveryRequest> {
        None
    }

    fn parse_snapshot(
        &self,
        _market: &NormalizedMarket,
        _body: &str,
        _received_ts_ms: i64,
    ) -> Result<NormalizedBookEvent, AdapterError> {
        Err(AdapterError::Unsupported(
            "lighter uses websocket bootstrap images".to_string(),
        ))
    }
}
