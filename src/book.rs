use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::model::{BookView, EventKind, MarketRef, NormalizedBookEvent, PriceLevel};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderBook {
    market: MarketRef,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    exchange_ts_ms: Option<i64>,
    received_ts_ms: i64,
}

impl OrderBook {
    pub fn new(market: MarketRef) -> Self {
        Self {
            market,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            exchange_ts_ms: None,
            received_ts_ms: 0,
        }
    }

    pub fn market(&self) -> &MarketRef {
        &self.market
    }

    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    pub fn apply_snapshot(
        &mut self,
        bids: &[PriceLevel],
        asks: &[PriceLevel],
        exchange_ts_ms: Option<i64>,
        received_ts_ms: i64,
    ) {
        self.bids.clear();
        self.asks.clear();
        self.apply_side(true, bids);
        self.apply_side(false, asks);
        self.exchange_ts_ms = exchange_ts_ms;
        self.received_ts_ms = received_ts_ms;
    }

    pub fn apply_delta(
        &mut self,
        bids: &[PriceLevel],
        asks: &[PriceLevel],
        exchange_ts_ms: Option<i64>,
        received_ts_ms: i64,
    ) {
        self.apply_side(true, bids);
        self.apply_side(false, asks);
        self.exchange_ts_ms = exchange_ts_ms.or(self.exchange_ts_ms);
        self.received_ts_ms = received_ts_ms;
    }

    pub fn apply_event(&mut self, event: &NormalizedBookEvent) {
        match event.kind {
            EventKind::Snapshot | EventKind::Image => {
                self.apply_snapshot(
                    &event.bids,
                    &event.asks,
                    event.exchange_ts_ms,
                    event.received_ts_ms,
                );
            }
            EventKind::Delta => {
                self.apply_delta(
                    &event.bids,
                    &event.asks,
                    event.exchange_ts_ms,
                    event.received_ts_ms,
                );
            }
            EventKind::Gap | EventKind::Heartbeat => {}
        }
    }

    pub fn view(&self, depth: usize) -> BookView {
        BookView {
            market: self.market.clone(),
            exchange_ts_ms: self.exchange_ts_ms,
            received_ts_ms: self.received_ts_ms,
            bids: self
                .bids
                .iter()
                .rev()
                .take(depth)
                .map(|(price, quantity)| PriceLevel::new(*price, *quantity))
                .collect(),
            asks: self
                .asks
                .iter()
                .take(depth)
                .map(|(price, quantity)| PriceLevel::new(*price, *quantity))
                .collect(),
            staleness_ms: None,
        }
    }

    fn apply_side(&mut self, is_bid: bool, levels: &[PriceLevel]) {
        let side = if is_bid {
            &mut self.bids
        } else {
            &mut self.asks
        };

        for level in levels {
            if level.quantity <= Decimal::ZERO {
                side.remove(&level.price);
            } else {
                side.insert(level.price, level.quantity);
            }
        }
    }
}
