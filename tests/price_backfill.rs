use tokenresearch::model::{MarketRef, Venue};
use tokenresearch::price_model::{PriceCheckpoint, PriceKind, PriceResolution};
use tokenresearch::price_runtime::{BackfillDecision, plan_backfill};

#[test]
fn backfill_planner_extends_from_last_checkpoint_to_last_closed_minute() {
    let checkpoint = PriceCheckpoint {
        market: MarketRef::new(Venue::Binance, "BTCUSDT"),
        kind: PriceKind::Trade,
        epoch_id: 1,
        last_live_bucket_ms: Some(1_710_000_030_000),
        last_candle_open_ms: Some(1_710_000_000_000),
        last_backfill_open_ms: Some(1_710_000_000_000),
        last_exchange_ts_ms: Some(1_710_000_030_100),
        updated_at_ms: 1_710_000_031_000,
        status: "live".to_string(),
    };

    let plan = plan_backfill(Some(&checkpoint), PriceKind::Trade, true, 1_710_000_190_000);
    assert_eq!(
        plan,
        BackfillDecision::Fetch {
            start_open_ms: 1_710_000_060_000,
            end_open_ms: 1_710_000_120_000,
        }
    );
}

#[test]
fn backfill_planner_marks_reference_history_as_unsupported() {
    let plan = plan_backfill(None, PriceKind::Reference, false, 1_710_000_190_000);
    assert_eq!(
        plan,
        BackfillDecision::Gap {
            resolution: PriceResolution::OneMinute,
            reason: "unsupported_history".to_string(),
        }
    );
}
