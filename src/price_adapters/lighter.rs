use std::collections::HashMap;
use std::sync::RwLock;

use serde_json::{Value, json};

use crate::adapters::{AdapterError, DiscoveryRequest, HttpMethod};
use crate::model::{MarketRef, MarketStatus, Venue};
use crate::price_adapters::PriceVenueAdapter;
use crate::price_adapters::decimal_from_value;
use crate::price_model::{
    NormalizedPriceTick, PriceCandle1m, PriceHistoryRequest, PriceKind, PriceMarket,
};

#[derive(Debug, Default)]
pub struct LighterPriceAdapter {
    symbol_by_market_id: RwLock<HashMap<String, String>>,
}

impl Clone for LighterPriceAdapter {
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

impl LighterPriceAdapter {
    fn parse_market_id(value: Option<&Value>) -> Option<String> {
        value
            .and_then(Value::as_i64)
            .map(|value| value.to_string())
            .or_else(|| value.and_then(Value::as_str).map(ToString::to_string))
    }

    fn channel_market_id(value: Option<&Value>) -> Option<String> {
        value.and_then(Value::as_str).and_then(|channel| {
            channel
                .rsplit_once('/')
                .map(|(_, suffix)| suffix.to_string())
                .or_else(|| {
                    channel
                        .rsplit_once(':')
                        .map(|(_, suffix)| suffix.to_string())
                })
        })
    }
}

impl PriceVenueAdapter for LighterPriceAdapter {
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

    fn discover_markets(&self, body: &str) -> Result<Vec<PriceMarket>, AdapterError> {
        let value: Value = serde_json::from_str(body)?;
        let markets = value
            .get("order_books")
            .and_then(Value::as_array)
            .ok_or(AdapterError::MissingField("order_books"))?;
        let mut id_map = self.symbol_by_market_id.write().expect("rwlock poisoned");
        id_map.clear();

        let mut normalized = Vec::new();
        for market in markets {
            if market
                .get("market_type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                != "perp"
            {
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
            id_map.insert(market_id.to_string(), symbol.to_string());
            normalized.push(PriceMarket {
                market: MarketRef::new(Venue::Lighter, symbol),
                venue_market_id: market_id.to_string(),
                token: symbol.to_string(),
                quote_asset: "USDC".to_string(),
                status: if market.get("status").and_then(Value::as_str) == Some("active") {
                    MarketStatus::Active
                } else {
                    MarketStatus::Inactive
                },
                supports_trade_history: true,
                supports_reference_history: false,
                updated_at_ms: 0,
            });
        }
        Ok(normalized)
    }

    fn ws_url(&self) -> String {
        "wss://mainnet.zklighter.elliot.ai/stream?readonly=true".to_string()
    }

    fn subscription_messages(&self, _markets: &[PriceMarket]) -> Vec<String> {
        vec![json!({"type":"subscribe","channel":"market_stats/all"}).to_string()]
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
        if let Some(error) = parsed.get("error") {
            let code = error
                .get("code")
                .and_then(Value::as_i64)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            return Err(AdapterError::Unsupported(format!(
                "server error code={} message={}",
                code, message
            )));
        }

        match parsed.get("type").and_then(Value::as_str) {
            Some("connected" | "subscribed" | "pong" | "ping") => return Ok(Vec::new()),
            Some("trade" | "market_stats" | "update/market_stats") => {}
            _ => {}
        }

        let trade_payload = parsed
            .get("trades")
            .and_then(|value| {
                if value.is_array() {
                    value.as_array().and_then(|rows| rows.first())
                } else {
                    Some(value)
                }
            })
            .or_else(|| parsed.get("trade"))
            .or_else(|| parsed.get("data"))
            .unwrap_or(&parsed);
        let market_stats_payload = parsed
            .get("market_stats")
            .or_else(|| parsed.get("data"))
            .unwrap_or(&parsed);

        let market_id = Self::parse_market_id(trade_payload.get("market_id"))
            .or_else(|| Self::parse_market_id(market_stats_payload.get("market_id")))
            .or_else(|| Self::parse_market_id(parsed.get("market_id")))
            .or_else(|| Self::channel_market_id(parsed.get("channel")))
            .ok_or(AdapterError::MissingField("market_id"))?;
        let symbol = market_stats_payload
            .get("symbol")
            .and_then(Value::as_str)
            .or_else(|| trade_payload.get("symbol").and_then(Value::as_str))
            .map(ToString::to_string)
            .or_else(|| {
                self.symbol_by_market_id
                    .read()
                    .expect("rwlock poisoned")
                    .get(&market_id)
                    .cloned()
            })
            .unwrap_or_else(|| market_id.clone());
        let exchange_ts_ms = parsed
            .get("timestamp")
            .and_then(Value::as_i64)
            .or_else(|| {
                market_stats_payload
                    .get("timestamp")
                    .and_then(Value::as_i64)
            })
            .or_else(|| {
                trade_payload
                    .get("timestamp")
                    .and_then(Value::as_i64)
                    .map(|value| value * 1_000)
            });

        let mut ticks = Vec::new();
        if let Some(price) = market_stats_payload
            .get("last_trade_price")
            .or_else(|| trade_payload.get("price"))
        {
            ticks.push(NormalizedPriceTick {
                market: MarketRef::new(Venue::Lighter, symbol.clone()),
                kind: PriceKind::Trade,
                exchange_ts_ms,
                received_ts_ms,
                price: decimal_from_value(price, "last_trade_price")?,
                quantity: trade_payload
                    .get("size")
                    .map(|value| decimal_from_value(value, "size"))
                    .transpose()?,
                raw_payload: parsed.clone(),
            });
        }

        if let Some(price) = market_stats_payload
            .get("mark_price")
            .or_else(|| market_stats_payload.get("mid_price"))
            .or_else(|| market_stats_payload.get("index_price"))
        {
            ticks.push(NormalizedPriceTick {
                market: MarketRef::new(Venue::Lighter, symbol),
                kind: PriceKind::Reference,
                exchange_ts_ms,
                received_ts_ms,
                price: decimal_from_value(price, "mark_price")?,
                quantity: None,
                raw_payload: parsed,
            });
        }

        Ok(ticks)
    }

    fn history_request(&self, request: PriceHistoryRequest) -> Option<DiscoveryRequest> {
        match request.kind {
            PriceKind::Trade => Some(DiscoveryRequest {
                method: HttpMethod::Get,
                url: format!(
                    "https://mainnet.zklighter.elliot.ai/api/v1/candles?market_id={}&resolution=1m&start_timestamp={}&end_timestamp={}&count_back={}",
                    request.market.venue_market_id,
                    request.start_ms,
                    request.end_ms,
                    (((request.end_ms - request.start_ms) / 60_000) + 1)
                        .max(1)
                        .min(request.limit as i64)
                ),
                body: None,
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
        let candles = value
            .get("candles")
            .and_then(Value::as_array)
            .or_else(|| value.get("c").and_then(Value::as_array))
            .ok_or(AdapterError::MissingField("candles"))?;
        candles
            .iter()
            .map(|row| {
                let open_time_ms = row
                    .get("open_time")
                    .and_then(Value::as_i64)
                    .or_else(|| row.get("t").and_then(Value::as_i64))
                    .ok_or(AdapterError::MissingField("open_time"))?;
                let close_time_ms = row
                    .get("close_time")
                    .and_then(Value::as_i64)
                    .unwrap_or(open_time_ms + 59_999);
                Ok(PriceCandle1m {
                    market: market.market.clone(),
                    kind,
                    open_time_ms,
                    close_time_ms,
                    open: decimal_from_value(
                        row.get("open")
                            .or_else(|| row.get("o"))
                            .ok_or(AdapterError::MissingField("open"))?,
                        "open",
                    )?,
                    high: decimal_from_value(
                        row.get("high")
                            .or_else(|| row.get("h"))
                            .ok_or(AdapterError::MissingField("high"))?,
                        "high",
                    )?,
                    low: decimal_from_value(
                        row.get("low")
                            .or_else(|| row.get("l"))
                            .ok_or(AdapterError::MissingField("low"))?,
                        "low",
                    )?,
                    close: decimal_from_value(
                        row.get("close")
                            .or_else(|| row.get("c"))
                            .ok_or(AdapterError::MissingField("close"))?,
                        "close",
                    )?,
                    volume: decimal_from_value(
                        row.get("volume")
                            .or_else(|| row.get("v"))
                            .ok_or(AdapterError::MissingField("volume"))?,
                        "volume",
                    )?,
                    trade_count: row
                        .get("trade_count")
                        .and_then(Value::as_i64)
                        .or_else(|| row.get("i").and_then(Value::as_i64)),
                    source: "backfill".to_string(),
                    updated_at_ms: close_time_ms,
                })
            })
            .collect()
    }
}
