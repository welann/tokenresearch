use rust_decimal::Decimal;
use serde_json::json;
use tokenresearch::model::{
    EventKind, MarketRef, NormalizedBookEvent, PriceLevel, SequenceRange, Venue,
};
use tokenresearch::sync::{BinanceBookSync, GenericBookSync};

fn delta_event(
    market: MarketRef,
    received_ts_ms: i64,
    start: u64,
    end: u64,
    previous_end: Option<u64>,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
) -> NormalizedBookEvent {
    NormalizedBookEvent {
        market,
        kind: EventKind::Delta,
        exchange_ts_ms: Some(received_ts_ms - 1),
        received_ts_ms,
        sequence: Some(SequenceRange {
            start,
            end,
            previous_end,
            offset: None,
        }),
        bids,
        asks,
        raw_payload: json!({}),
    }
}

#[test]
fn binance_sync_replays_buffered_deltas_after_snapshot() {
    let market = MarketRef::new(Venue::Binance, "BTCUSDT");
    let mut sync = BinanceBookSync::new(market.clone());
    let buffered = delta_event(
        market.clone(),
        101,
        102,
        104,
        Some(101),
        vec![PriceLevel::new(
            "100.0".parse().unwrap(),
            "2.0".parse().unwrap(),
        )],
        vec![],
    );
    let buffered_outcome = sync.on_delta(buffered);
    assert!(buffered_outcome.needs_resync);

    let snapshot = NormalizedBookEvent {
        market: market.clone(),
        kind: EventKind::Snapshot,
        exchange_ts_ms: None,
        received_ts_ms: 100,
        sequence: Some(SequenceRange {
            start: 101,
            end: 101,
            previous_end: None,
            offset: None,
        }),
        bids: vec![PriceLevel::new(
            "100.0".parse().unwrap(),
            "1.0".parse().unwrap(),
        )],
        asks: vec![PriceLevel::new(
            "100.5".parse().unwrap(),
            "1.0".parse().unwrap(),
        )],
        raw_payload: json!({}),
    };
    let outcome = sync.on_snapshot(snapshot);
    assert_eq!(outcome.accepted_events.len(), 2);
    let view = sync.book().view(5);
    assert_eq!(view.bids[0].quantity.to_string(), "2.0");
}

#[test]
fn generic_sync_marks_gap_on_sequence_break() {
    let market = MarketRef::new(Venue::Lighter, "PROVE");
    let mut sync = GenericBookSync::new(market.clone());
    let first = delta_event(
        market.clone(),
        100,
        1,
        1,
        Some(0),
        vec![PriceLevel::new(
            "1.0".parse().unwrap(),
            "5".parse().unwrap(),
        )],
        vec![PriceLevel::new(
            "1.1".parse().unwrap(),
            "5".parse().unwrap(),
        )],
    );
    let accepted = sync.apply(first);
    assert!(!accepted.needs_resync);

    let broken = delta_event(
        market,
        101,
        3,
        3,
        Some(1),
        vec![PriceLevel::new(Decimal::ONE, Decimal::ONE)],
        vec![],
    );
    let outcome = sync.apply(broken);
    assert!(outcome.needs_resync);
    assert!(outcome.gap.is_some());
}
