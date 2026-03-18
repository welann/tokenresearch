use tokenresearch::model::{MarketRef, Venue};
use tokenresearch::price_model::{PriceCheckpoint, PriceKind, PriceResolution};
use tokenresearch::price_runtime::{BackfillDecision, plan_backfill};

#[test]
fn backfill_planner_uses_trailing_window_when_checkpoint_missing() {
    let plan = plan_backfill(None, PriceKind::Trade, true, 1_710_000_190_000, 90);
    assert_eq!(
        plan,
        BackfillDecision::Fetch {
            start_open_ms: 1_702_224_180_000,
            end_open_ms: 1_710_000_120_000,
        }
    );
}

#[test]
fn backfill_planner_clamps_old_checkpoint_to_window_start() {
    let checkpoint = PriceCheckpoint {
        market: MarketRef::new(Venue::Binance, "BTCUSDT"),
        kind: PriceKind::Trade,
        epoch_id: 1,
        last_live_bucket_ms: Some(1_710_000_030_000),
        last_candle_open_ms: Some(1_700_000_000_000),
        last_backfill_open_ms: Some(1_700_000_000_000),
        last_exchange_ts_ms: Some(1_710_000_030_100),
        updated_at_ms: 1_710_000_031_000,
        status: "live".to_string(),
    };

    let plan = plan_backfill(
        Some(&checkpoint),
        PriceKind::Trade,
        true,
        1_710_000_190_000,
        90,
    );
    assert_eq!(
        plan,
        BackfillDecision::Fetch {
            start_open_ms: 1_702_224_180_000,
            end_open_ms: 1_710_000_120_000,
        }
    );
}

#[test]
fn backfill_planner_resumes_inside_window_from_checkpoint() {
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

    let plan = plan_backfill(
        Some(&checkpoint),
        PriceKind::Trade,
        true,
        1_710_000_190_000,
        90,
    );
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
    let plan = plan_backfill(None, PriceKind::Reference, false, 1_710_000_190_000, 90);
    assert_eq!(
        plan,
        BackfillDecision::Gap {
            resolution: PriceResolution::OneMinute,
            reason: "unsupported_history".to_string(),
        }
    );
}

#[test]
fn backfill_planner_can_disable_window() {
    let plan = plan_backfill(None, PriceKind::Trade, true, 1_710_000_190_000, 0);
    assert_eq!(plan, BackfillDecision::Skip);
}
