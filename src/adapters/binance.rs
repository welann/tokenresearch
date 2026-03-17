use serde_json::{Value, json};

use crate::adapters::{
    AdapterError, DiscoveryRequest, HttpMethod, VenueAdapter, decimal_from_value,
    decimals_from_step,
};
use crate::model::{
    EventKind, MarketRef, MarketStatus, MarketType, NormalizedBookEvent, NormalizedMarket,
    PriceLevel, SequenceRange, Venue,
};

#[derive(Clone, Debug, Default)]
pub struct BinanceAdapter;

impl BinanceAdapter {
    fn parse_levels(value: &Value, field: &'static str) -> Result<Vec<PriceLevel>, AdapterError> {
        let levels = value.as_array().ok_or(AdapterError::MissingField(field))?;
        levels
            .iter()
            .map(|entry| {
                let pair = entry.as_array().ok_or(AdapterError::InvalidField {
                    field,
                    message: "expected [price, qty] pair".to_string(),
                })?;
                if pair.len() < 2 {
                    return Err(AdapterError::InvalidField {
                        field,
                        message: "expected [price, qty] pair".to_string(),
                    });
                }
                Ok(PriceLevel::new(
                    decimal_from_value(&pair[0], field)?,
                    decimal_from_value(&pair[1], field)?,
                ))
            })
            .collect()
    }

    fn parse_market(symbol: &Value) -> Result<Option<NormalizedMarket>, AdapterError> {
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
        let status = symbol
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if contract_type != "PERPETUAL" || quote_asset != "USDT" {
            return Ok(None);
        }

        let filters = symbol
            .get("filters")
            .and_then(Value::as_array)
            .ok_or(AdapterError::MissingField("filters"))?;
        let tick_size = filters
            .iter()
            .find(|filter| filter.get("filterType").and_then(Value::as_str) == Some("PRICE_FILTER"))
            .and_then(|filter| filter.get("tickSize"))
            .and_then(Value::as_str)
            .ok_or(AdapterError::MissingField("tickSize"))?;
        let step_size = filters
            .iter()
            .find(|filter| filter.get("filterType").and_then(Value::as_str) == Some("LOT_SIZE"))
            .and_then(|filter| filter.get("stepSize"))
            .and_then(Value::as_str)
            .ok_or(AdapterError::MissingField("stepSize"))?;

        Ok(Some(NormalizedMarket {
            market: MarketRef::new(Venue::Binance, symbol_name),
            venue_market_id: symbol_name.to_string(),
            base_asset: symbol
                .get("baseAsset")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            quote_asset: quote_asset.to_string(),
            market_type: MarketType::Perpetual,
            status: if status == "TRADING" {
                MarketStatus::Active
            } else {
                MarketStatus::Inactive
            },
            price_decimals: decimals_from_step(tick_size),
            size_decimals: decimals_from_step(step_size),
        }))
    }
}

impl VenueAdapter for BinanceAdapter {
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

    fn discover_markets(&self, body: &str) -> Result<Vec<NormalizedMarket>, AdapterError> {
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

    fn ws_url(&self, markets: &[NormalizedMarket]) -> String {
        let streams = markets
            .iter()
            .map(|market| format!("{}@depth@100ms", market.market.symbol.to_lowercase()))
            .collect::<Vec<_>>()
            .join("/");
        format!("wss://fstream.binance.com/stream?streams={streams}")
    }

    fn subscription_messages(&self, _markets: &[NormalizedMarket]) -> Vec<String> {
        Vec::new()
    }

    fn parse_ws_message(
        &self,
        raw: &str,
        received_ts_ms: i64,
    ) -> Result<Option<NormalizedBookEvent>, AdapterError> {
        let parsed: Value = serde_json::from_str(raw)?;
        let data = parsed.get("data").unwrap_or(&parsed);
        if data.get("e").and_then(Value::as_str) != Some("depthUpdate") {
            return Ok(None);
        }

        let symbol = data
            .get("s")
            .and_then(Value::as_str)
            .ok_or(AdapterError::MissingField("s"))?;
        Ok(Some(NormalizedBookEvent {
            market: MarketRef::new(Venue::Binance, symbol),
            kind: EventKind::Delta,
            exchange_ts_ms: data.get("E").and_then(Value::as_i64),
            received_ts_ms,
            sequence: Some(SequenceRange {
                start: data
                    .get("U")
                    .and_then(Value::as_u64)
                    .ok_or(AdapterError::MissingField("U"))?,
                end: data
                    .get("u")
                    .and_then(Value::as_u64)
                    .ok_or(AdapterError::MissingField("u"))?,
                previous_end: data.get("pu").and_then(Value::as_u64),
                offset: None,
            }),
            bids: Self::parse_levels(data.get("b").ok_or(AdapterError::MissingField("b"))?, "b")?,
            asks: Self::parse_levels(data.get("a").ok_or(AdapterError::MissingField("a"))?, "a")?,
            raw_payload: parsed,
        }))
    }

    fn snapshot_request(&self, market: &NormalizedMarket) -> Option<DiscoveryRequest> {
        Some(DiscoveryRequest {
            method: HttpMethod::Get,
            url: format!(
                "https://fapi.binance.com/fapi/v1/depth?symbol={}&limit=1000",
                market.market.symbol
            ),
            body: None,
        })
    }

    fn parse_snapshot(
        &self,
        market: &NormalizedMarket,
        body: &str,
        received_ts_ms: i64,
    ) -> Result<NormalizedBookEvent, AdapterError> {
        let parsed: Value = serde_json::from_str(body)?;
        Ok(NormalizedBookEvent {
            market: market.market.clone(),
            kind: EventKind::Snapshot,
            exchange_ts_ms: None,
            received_ts_ms,
            sequence: Some(SequenceRange {
                start: parsed
                    .get("lastUpdateId")
                    .and_then(Value::as_u64)
                    .ok_or(AdapterError::MissingField("lastUpdateId"))?,
                end: parsed
                    .get("lastUpdateId")
                    .and_then(Value::as_u64)
                    .ok_or(AdapterError::MissingField("lastUpdateId"))?,
                previous_end: None,
                offset: None,
            }),
            bids: Self::parse_levels(
                parsed
                    .get("bids")
                    .ok_or(AdapterError::MissingField("bids"))?,
                "bids",
            )?,
            asks: Self::parse_levels(
                parsed
                    .get("asks")
                    .ok_or(AdapterError::MissingField("asks"))?,
                "asks",
            )?,
            raw_payload: json!({
                "source": "rest_snapshot",
                "payload": parsed,
            }),
        })
    }
}
