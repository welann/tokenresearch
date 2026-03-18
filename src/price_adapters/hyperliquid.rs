use serde_json::{Value, json};

use crate::adapters::{AdapterError, DiscoveryRequest, HttpMethod};
use crate::model::{MarketRef, MarketStatus, Venue};
use crate::price_adapters::PriceVenueAdapter;
use crate::price_adapters::decimal_from_value;
use crate::price_model::{
    NormalizedPriceTick, PriceCandle1m, PriceHistoryRequest, PriceKind, PriceMarket,
};

#[derive(Clone, Debug, Default)]
pub struct HyperliquidPriceAdapter;

impl PriceVenueAdapter for HyperliquidPriceAdapter {
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

    fn discover_markets(&self, body: &str) -> Result<Vec<PriceMarket>, AdapterError> {
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
                Ok(PriceMarket {
                    market: MarketRef::new(Venue::Hyperliquid, symbol),
                    venue_market_id: symbol.to_string(),
                    token: symbol.to_string(),
                    quote_asset: "USDC".to_string(),
                    status: if entry
                        .get("isDelisted")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                    {
                        MarketStatus::Inactive
                    } else {
                        MarketStatus::Active
                    },
                    supports_trade_history: true,
                    supports_reference_history: false,
                    updated_at_ms: 0,
                })
            })
            .collect()
    }

    fn ws_url(&self) -> String {
        "wss://api.hyperliquid.xyz/ws".to_string()
    }

    fn subscription_messages(&self, markets: &[PriceMarket]) -> Vec<String> {
        let mut messages = vec![
            json!({
                "method": "subscribe",
                "subscription": { "type": "allMids" }
            })
            .to_string(),
        ];
        messages.extend(markets.iter().map(|market| {
            json!({
                "method": "subscribe",
                "subscription": {
                    "type": "trades",
                    "coin": market.market.symbol,
                }
            })
            .to_string()
        }));
        messages
    }

    fn parse_ws_message(
        &self,
        raw: &str,
        received_ts_ms: i64,
    ) -> Result<Option<NormalizedPriceTick>, AdapterError> {
        Ok(self
            .parse_ws_message_ticks(raw, received_ts_ms)?
            .into_iter()
            .next())
    }

    fn parse_ws_message_ticks(
        &self,
        raw: &str,
        received_ts_ms: i64,
    ) -> Result<Vec<NormalizedPriceTick>, AdapterError> {
        let parsed: Value = serde_json::from_str(raw)?;
        match parsed.get("channel").and_then(Value::as_str) {
            Some("subscriptionResponse" | "pong") => Ok(Vec::new()),
            Some("trades") => {
                let trades = parsed
                    .get("data")
                    .and_then(Value::as_array)
                    .ok_or(AdapterError::MissingField("data"))?;
                trades
                    .iter()
                    .map(|trade| {
                        Ok(NormalizedPriceTick {
                            market: MarketRef::new(
                                Venue::Hyperliquid,
                                trade
                                    .get("coin")
                                    .and_then(Value::as_str)
                                    .ok_or(AdapterError::MissingField("coin"))?,
                            ),
                            kind: PriceKind::Trade,
                            exchange_ts_ms: trade.get("time").and_then(Value::as_i64),
                            received_ts_ms,
                            price: decimal_from_value(
                                trade.get("px").ok_or(AdapterError::MissingField("px"))?,
                                "px",
                            )?,
                            quantity: Some(decimal_from_value(
                                trade.get("sz").ok_or(AdapterError::MissingField("sz"))?,
                                "sz",
                            )?),
                            raw_payload: parsed.clone(),
                        })
                    })
                    .collect()
            }
            Some("allMids") => {
                let data = parsed
                    .get("data")
                    .ok_or(AdapterError::MissingField("data"))?;
                let mids = data
                    .get("mids")
                    .and_then(Value::as_object)
                    .ok_or(AdapterError::MissingField("mids"))?;
                mids.iter()
                    .map(|(coin, price)| {
                        Ok(NormalizedPriceTick {
                            market: MarketRef::new(Venue::Hyperliquid, coin),
                            kind: PriceKind::Reference,
                            exchange_ts_ms: data.get("time").and_then(Value::as_i64),
                            received_ts_ms,
                            price: decimal_from_value(price, "mid")?,
                            quantity: None,
                            raw_payload: parsed.clone(),
                        })
                    })
                    .collect()
            }
            _ => Ok(Vec::new()),
        }
    }

    fn history_request(&self, request: PriceHistoryRequest) -> Option<DiscoveryRequest> {
        match request.kind {
            PriceKind::Trade => Some(DiscoveryRequest {
                method: HttpMethod::Post,
                url: "https://api.hyperliquid.xyz/info".to_string(),
                body: Some(json!({
                    "type": "candleSnapshot",
                    "req": {
                        "coin": request.market.market.symbol,
                        "interval": "1m",
                        "startTime": request.start_ms,
                        "endTime": request.end_ms,
                    }
                })),
            }),
            PriceKind::Reference | PriceKind::All => None,
        }
    }

    fn parse_history_candles(
        &self,
        market: &PriceMarket,
        kind: PriceKind,
        body: &str,
    ) -> Result<Vec<PriceCandle1m>, AdapterError> {
        let value: Value = serde_json::from_str(body)?;
        let rows = value.as_array().ok_or(AdapterError::InvalidField {
            field: "body",
            message: "expected candle array".to_string(),
        })?;
        rows.iter()
            .map(|row| {
                Ok(PriceCandle1m {
                    market: market.market.clone(),
                    kind,
                    open_time_ms: row
                        .get("t")
                        .and_then(Value::as_i64)
                        .ok_or(AdapterError::MissingField("t"))?,
                    close_time_ms: row
                        .get("T")
                        .and_then(Value::as_i64)
                        .ok_or(AdapterError::MissingField("T"))?,
                    open: decimal_from_value(
                        row.get("o").ok_or(AdapterError::MissingField("o"))?,
                        "o",
                    )?,
                    high: decimal_from_value(
                        row.get("h").ok_or(AdapterError::MissingField("h"))?,
                        "h",
                    )?,
                    low: decimal_from_value(
                        row.get("l").ok_or(AdapterError::MissingField("l"))?,
                        "l",
                    )?,
                    close: decimal_from_value(
                        row.get("c").ok_or(AdapterError::MissingField("c"))?,
                        "c",
                    )?,
                    volume: decimal_from_value(
                        row.get("v").ok_or(AdapterError::MissingField("v"))?,
                        "v",
                    )?,
                    trade_count: row.get("n").and_then(Value::as_i64),
                    source: "backfill".to_string(),
                    updated_at_ms: row
                        .get("T")
                        .and_then(Value::as_i64)
                        .ok_or(AdapterError::MissingField("T"))?,
                })
            })
            .collect()
    }
}
