use serde_json::Value;

use crate::adapters::{AdapterError, DiscoveryRequest, HttpMethod};
use crate::model::{MarketRef, MarketStatus, Venue};
use crate::price_adapters::{PriceVenueAdapter, decimal_from_value};
use crate::price_model::{
    NormalizedPriceTick, PriceCandle1m, PriceHistoryRequest, PriceKind, PriceMarket,
};

#[derive(Clone, Debug, Default)]
pub struct BinancePriceAdapter;

impl BinancePriceAdapter {
    fn parse_trade_tick(
        payload: &Value,
        raw_payload: Value,
        received_ts_ms: i64,
    ) -> Result<NormalizedPriceTick, AdapterError> {
        let quantity_field = if payload.get("q").is_some() { "q" } else { "Q" };
        let quantity = payload
            .get(quantity_field)
            .map(|value| decimal_from_value(value, quantity_field))
            .transpose()?;

        Ok(NormalizedPriceTick {
            market: MarketRef::new(
                Venue::Binance,
                payload
                    .get("s")
                    .and_then(Value::as_str)
                    .ok_or(AdapterError::MissingField("s"))?,
            ),
            kind: PriceKind::Trade,
            exchange_ts_ms: payload
                .get("T")
                .and_then(Value::as_i64)
                .or_else(|| payload.get("E").and_then(Value::as_i64)),
            received_ts_ms,
            price: decimal_from_value(
                payload
                    .get("p")
                    .or_else(|| payload.get("c"))
                    .ok_or(AdapterError::MissingField("p"))?,
                "p",
            )?,
            quantity,
            raw_payload,
        })
    }

    fn parse_reference_tick(
        payload: &Value,
        raw_payload: Value,
        received_ts_ms: i64,
    ) -> Result<NormalizedPriceTick, AdapterError> {
        Ok(NormalizedPriceTick {
            market: MarketRef::new(
                Venue::Binance,
                payload
                    .get("s")
                    .and_then(Value::as_str)
                    .ok_or(AdapterError::MissingField("s"))?,
            ),
            kind: PriceKind::Reference,
            exchange_ts_ms: payload.get("E").and_then(Value::as_i64),
            received_ts_ms,
            price: decimal_from_value(
                payload
                    .get("p")
                    .ok_or(AdapterError::MissingField("p"))?,
                "p",
            )?,
            quantity: None,
            raw_payload,
        })
    }

    fn parse_event(
        payload: &Value,
        raw_payload: Value,
        received_ts_ms: i64,
    ) -> Result<Option<NormalizedPriceTick>, AdapterError> {
        match payload.get("e").and_then(Value::as_str) {
            Some("aggTrade") | Some("24hrTicker") | Some("24hrMiniTicker") => Ok(Some(
                Self::parse_trade_tick(payload, raw_payload, received_ts_ms)?,
            )),
            Some("markPriceUpdate") => Ok(Some(Self::parse_reference_tick(
                payload,
                raw_payload,
                received_ts_ms,
            )?)),
            _ => Ok(None),
        }
    }

    fn parse_market(symbol: &Value) -> Result<Option<PriceMarket>, AdapterError> {
        let symbol_name = symbol
            .get("symbol")
            .and_then(Value::as_str)
            .ok_or(AdapterError::MissingField("symbol"))?;
        let contract_type = symbol
            .get("contractType")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let quote_asset = symbol
            .get("quoteAsset")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if contract_type != "PERPETUAL" || quote_asset != "USDT" {
            return Ok(None);
        }

        let filters = symbol
            .get("filters")
            .and_then(Value::as_array)
            .ok_or(AdapterError::MissingField("filters"))?;
        let _tick_size = filters
            .iter()
            .find(|filter| filter.get("filterType").and_then(Value::as_str) == Some("PRICE_FILTER"))
            .and_then(|filter| filter.get("tickSize"))
            .and_then(Value::as_str)
            .ok_or(AdapterError::MissingField("tickSize"))?;
        let _step_size = filters
            .iter()
            .find(|filter| filter.get("filterType").and_then(Value::as_str) == Some("LOT_SIZE"))
            .and_then(|filter| filter.get("stepSize"))
            .and_then(Value::as_str)
            .ok_or(AdapterError::MissingField("stepSize"))?;

        Ok(Some(PriceMarket {
            market: MarketRef::new(Venue::Binance, symbol_name),
            venue_market_id: symbol_name.to_string(),
            token: symbol
                .get("baseAsset")
                .and_then(Value::as_str)
                .unwrap_or(symbol_name)
                .to_string(),
            quote_asset: quote_asset.to_string(),
            status: if symbol.get("status").and_then(Value::as_str) == Some("TRADING") {
                MarketStatus::Active
            } else {
                MarketStatus::Inactive
            },
            supports_trade_history: true,
            supports_reference_history: true,
            updated_at_ms: 0,
        }))
    }

    fn parse_klines(
        market: &PriceMarket,
        kind: PriceKind,
        body: &str,
        source: &str,
    ) -> Result<Vec<PriceCandle1m>, AdapterError> {
        let value: Value = serde_json::from_str(body)?;
        let rows = value.as_array().ok_or(AdapterError::InvalidField {
            field: "body",
            message: "expected kline array".to_string(),
        })?;
        rows.iter()
            .map(|row| {
                let row = row.as_array().ok_or(AdapterError::InvalidField {
                    field: "kline",
                    message: "expected array row".to_string(),
                })?;
                if row.len() < 9 {
                    return Err(AdapterError::InvalidField {
                        field: "kline",
                        message: "expected at least 9 fields".to_string(),
                    });
                }
                Ok(PriceCandle1m {
                    market: market.market.clone(),
                    kind,
                    open_time_ms: row[0]
                        .as_i64()
                        .ok_or(AdapterError::MissingField("open_time"))?,
                    close_time_ms: row[6]
                        .as_i64()
                        .ok_or(AdapterError::MissingField("close_time"))?,
                    open: decimal_from_value(&row[1], "open")?,
                    high: decimal_from_value(&row[2], "high")?,
                    low: decimal_from_value(&row[3], "low")?,
                    close: decimal_from_value(&row[4], "close")?,
                    volume: decimal_from_value(&row[5], "volume")?,
                    trade_count: row[8].as_i64(),
                    source: source.to_string(),
                    updated_at_ms: row[6]
                        .as_i64()
                        .ok_or(AdapterError::MissingField("close_time"))?,
                })
            })
            .collect()
    }
}

impl PriceVenueAdapter for BinancePriceAdapter {
    fn venue(&self) -> Venue {
        Venue::Binance
    }

    fn discovery_request(&self) -> DiscoveryRequest {
        DiscoveryRequest {
            method: HttpMethod::Get,
            url: "https://fapi.binance.com/fapi/v1/exchangeInfo".to_string(),
            body: None,
        }
    }

    fn discover_markets(&self, body: &str) -> Result<Vec<PriceMarket>, AdapterError> {
        let value: Value = serde_json::from_str(body)?;
        let symbols = value
            .get("symbols")
            .and_then(Value::as_array)
            .ok_or(AdapterError::MissingField("symbols"))?;
        symbols
            .iter()
            .filter_map(|symbol| match Self::parse_market(symbol) {
                Ok(Some(market)) => Some(Ok(market)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    fn ws_url(&self) -> String {
        "wss://fstream.binance.com/stream?streams=!ticker@arr/!markPrice@arr@1s".to_string()
    }

    fn subscription_messages(&self, _markets: &[PriceMarket]) -> Vec<String> {
        Vec::new()
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
        if parsed.get("result").is_some() {
            return Ok(Vec::new());
        }

        let data = parsed.get("data").cloned().unwrap_or_else(|| parsed.clone());
        if let Some(events) = data.as_array() {
            return events
                .iter()
                .filter_map(|event| {
                    match Self::parse_event(event, event.clone(), received_ts_ms) {
                        Ok(Some(tick)) => Some(Ok(tick)),
                        Ok(None) => None,
                        Err(error) => Some(Err(error)),
                    }
                })
                .collect();
        }

        Self::parse_event(&data, parsed, received_ts_ms).map(|tick| tick.into_iter().collect())
    }

    fn history_request(&self, request: PriceHistoryRequest) -> Option<DiscoveryRequest> {
        let endpoint = match request.kind {
            PriceKind::Trade => "klines",
            PriceKind::Reference => "markPriceKlines",
            PriceKind::All => return None,
        };
        Some(DiscoveryRequest {
            method: HttpMethod::Get,
            url: format!(
                "https://fapi.binance.com/fapi/v1/{endpoint}?symbol={}&interval=1m&limit={}&startTime={}&endTime={}",
                request.market.market.symbol, request.limit, request.start_ms, request.end_ms
            ),
            body: None,
        })
    }

    fn parse_history_candles(
        &self,
        market: &PriceMarket,
        kind: PriceKind,
        body: &str,
    ) -> Result<Vec<PriceCandle1m>, AdapterError> {
        Self::parse_klines(market, kind, body, "backfill")
    }
}
