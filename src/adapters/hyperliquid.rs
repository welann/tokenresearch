use serde_json::{Value, json};

use crate::adapters::{
    AdapterError, DiscoveryRequest, HttpMethod, VenueAdapter, decimal_from_value,
};
use crate::model::{
    EventKind, MarketRef, MarketStatus, MarketType, NormalizedBookEvent, NormalizedMarket,
    PriceLevel, Venue,
};

#[derive(Clone, Debug, Default)]
pub struct HyperliquidAdapter;

impl HyperliquidAdapter {
    fn parse_levels(
        value: &Value,
        field: &'static str,
    ) -> Result<(Vec<PriceLevel>, Vec<PriceLevel>), AdapterError> {
        let sides = value.as_array().ok_or(AdapterError::MissingField(field))?;
        if sides.len() != 2 {
            return Err(AdapterError::InvalidField {
                field,
                message: "expected [bids, asks]".to_string(),
            });
        }

        fn parse_side(side: &Value, field: &'static str) -> Result<Vec<PriceLevel>, AdapterError> {
            side.as_array()
                .ok_or(AdapterError::MissingField(field))?
                .iter()
                .map(|entry| {
                    Ok(PriceLevel::new(
                        decimal_from_value(
                            entry.get("px").ok_or(AdapterError::MissingField("px"))?,
                            "px",
                        )?,
                        decimal_from_value(
                            entry.get("sz").ok_or(AdapterError::MissingField("sz"))?,
                            "sz",
                        )?,
                    ))
                })
                .collect()
        }

        Ok((
            parse_side(&sides[0], "levels[0]")?,
            parse_side(&sides[1], "levels[1]")?,
        ))
    }
}

impl VenueAdapter for HyperliquidAdapter {
    fn venue(&self) -> Venue {
        Venue::Hyperliquid
    }

    fn discovery_request(&self) -> DiscoveryRequest {
        DiscoveryRequest {
            method: HttpMethod::Post,
            url: "https://api.hyperliquid.xyz/info".to_string(),
            body: Some(json!({ "type": "meta" })),
        }
    }

    fn discover_markets(&self, body: &str) -> Result<Vec<NormalizedMarket>, AdapterError> {
        let value: Value = serde_json::from_str(body)?;
        let universe = value
            .get("universe")
            .and_then(Value::as_array)
            .ok_or(AdapterError::MissingField("universe"))?;

        universe
            .iter()
            .map(|entry| {
                let symbol = entry
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or(AdapterError::MissingField("name"))?;
                let size_decimals = entry
                    .get("szDecimals")
                    .and_then(Value::as_u64)
                    .ok_or(AdapterError::MissingField("szDecimals"))?
                    as u32;
                let is_delisted = entry
                    .get("isDelisted")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);

                Ok(NormalizedMarket {
                    market: MarketRef::new(Venue::Hyperliquid, symbol),
                    venue_market_id: symbol.to_string(),
                    base_asset: symbol.to_string(),
                    quote_asset: "USDC".to_string(),
                    market_type: MarketType::Perpetual,
                    status: if is_delisted {
                        MarketStatus::Inactive
                    } else {
                        MarketStatus::Active
                    },
                    price_decimals: 6u32.saturating_sub(size_decimals),
                    size_decimals,
                })
            })
            .collect()
    }

    fn ws_url(&self, _markets: &[NormalizedMarket]) -> String {
        "wss://api.hyperliquid.xyz/ws".to_string()
    }

    fn subscription_messages(&self, markets: &[NormalizedMarket]) -> Vec<String> {
        markets
            .iter()
            .map(|market| {
                json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "l2Book",
                        "coin": market.market.symbol,
                    }
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
        if parsed.get("channel").and_then(Value::as_str) == Some("subscriptionResponse") {
            return Ok(None);
        }
        if parsed.get("channel").and_then(Value::as_str) == Some("pong") {
            return Ok(None);
        }

        let data = parsed.get("data").unwrap_or(&parsed);
        let coin = data
            .get("coin")
            .and_then(Value::as_str)
            .ok_or(AdapterError::MissingField("coin"))?;
        let (bids, asks) = Self::parse_levels(
            data.get("levels")
                .ok_or(AdapterError::MissingField("levels"))?,
            "levels",
        )?;

        Ok(Some(NormalizedBookEvent {
            market: MarketRef::new(Venue::Hyperliquid, coin),
            kind: EventKind::Image,
            exchange_ts_ms: data.get("time").and_then(Value::as_i64),
            received_ts_ms,
            sequence: None,
            bids,
            asks,
            raw_payload: parsed,
        }))
    }

    fn snapshot_request(&self, market: &NormalizedMarket) -> Option<DiscoveryRequest> {
        Some(DiscoveryRequest {
            method: HttpMethod::Post,
            url: "https://api.hyperliquid.xyz/info".to_string(),
            body: Some(json!({
                "type": "l2Book",
                "coin": market.market.symbol,
            })),
        })
    }

    fn parse_snapshot(
        &self,
        market: &NormalizedMarket,
        body: &str,
        received_ts_ms: i64,
    ) -> Result<NormalizedBookEvent, AdapterError> {
        let parsed: Value = serde_json::from_str(body)?;
        let (bids, asks) = Self::parse_levels(
            parsed
                .get("levels")
                .ok_or(AdapterError::MissingField("levels"))?,
            "levels",
        )?;
        Ok(NormalizedBookEvent {
            market: market.market.clone(),
            kind: EventKind::Image,
            exchange_ts_ms: parsed.get("time").and_then(Value::as_i64),
            received_ts_ms,
            sequence: None,
            bids,
            asks,
            raw_payload: json!({
                "source": "rest_snapshot",
                "payload": parsed,
            }),
        })
    }
}
