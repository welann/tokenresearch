use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{info, warn};

use crate::adapters::{DiscoveryRequest, HttpMethod};
use crate::diagnostics::{http_method_name, preview_optional_json, preview_text};
use crate::model::MarketRef;
use crate::price_adapters::PriceVenueAdapter;
use crate::price_model::{
    NormalizedPriceTick, PriceCandle1m, PriceCheckpoint, PriceCommitBatch, PriceGapWindow,
    PriceKind, PriceMarket, PriceResolution, PriceSample1s,
};
use crate::traits::{Clock, DynResult, PriceStore, RestClient, WsClient};

const ONE_MINUTE_MS: i64 = 60_000;
const ONE_SECOND_MS: i64 = 1_000;
const HISTORY_PAGE_LIMIT: usize = 500;

#[derive(Clone, Debug, Deserialize)]
pub struct PriceRuntimeConfig {
    #[serde(default = "default_price_db")]
    pub database_path: String,
    #[serde(default = "default_sample_retention_days")]
    pub sample_retention_days: i64,
    #[serde(default = "default_discovery_max_attempts")]
    pub discovery_max_attempts: usize,
    #[serde(default = "default_backfill_window_days")]
    pub backfill_window_days: i64,
    #[serde(default = "default_http_min_interval_ms")]
    pub http_min_interval_ms: u64,
}

fn default_price_db() -> String {
    "token_prices.sqlite".to_string()
}

fn default_sample_retention_days() -> i64 {
    30
}

fn default_discovery_max_attempts() -> usize {
    5
}

fn default_backfill_window_days() -> i64 {
    90
}

fn default_http_min_interval_ms() -> u64 {
    1_000
}

impl Default for PriceRuntimeConfig {
    fn default() -> Self {
        Self {
            database_path: default_price_db(),
            sample_retention_days: default_sample_retention_days(),
            discovery_max_attempts: default_discovery_max_attempts(),
            backfill_window_days: default_backfill_window_days(),
            http_min_interval_ms: default_http_min_interval_ms(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackfillDecision {
    Fetch {
        start_open_ms: i64,
        end_open_ms: i64,
    },
    Gap {
        resolution: PriceResolution,
        reason: String,
    },
    Skip,
}

#[derive(Clone, Debug)]
struct VenueHttpLimiter {
    min_interval_ms: u64,
    next_allowed_ms: Option<i64>,
}

impl VenueHttpLimiter {
    fn new(min_interval_ms: u64) -> Self {
        Self {
            min_interval_ms,
            next_allowed_ms: None,
        }
    }

    async fn wait_turn<C>(&mut self, clock: &C)
    where
        C: Clock + Send + Sync,
    {
        if self.min_interval_ms == 0 {
            return;
        }
        let now_ms = clock.now_ms();
        if let Some(next_allowed_ms) = self.next_allowed_ms {
            if now_ms < next_allowed_ms {
                clock
                    .sleep(Duration::from_millis((next_allowed_ms - now_ms) as u64))
                    .await;
            }
        }
        self.next_allowed_ms = Some(clock.now_ms() + self.min_interval_ms as i64);
    }
}

pub fn plan_backfill(
    checkpoint: Option<&PriceCheckpoint>,
    kind: PriceKind,
    supports_history: bool,
    now_ms: i64,
    backfill_window_days: i64,
) -> BackfillDecision {
    if !supports_history {
        return BackfillDecision::Gap {
            resolution: PriceResolution::OneMinute,
            reason: if kind == PriceKind::Reference {
                "unsupported_history".to_string()
            } else {
                "history_unavailable".to_string()
            },
        };
    }

    let last_closed_open_ms = ((now_ms / ONE_MINUTE_MS) * ONE_MINUTE_MS) - ONE_MINUTE_MS;
    if last_closed_open_ms < 0 {
        return BackfillDecision::Skip;
    }

    let backfill_window_minutes = backfill_window_days.saturating_mul(24 * 60);
    if backfill_window_minutes <= 0 {
        return BackfillDecision::Skip;
    }

    let window_start_open_ms =
        last_closed_open_ms - (backfill_window_minutes - 1).saturating_mul(ONE_MINUTE_MS);

    let from_open_ms = checkpoint
        .and_then(|checkpoint| {
            checkpoint
                .last_backfill_open_ms
                .or(checkpoint.last_candle_open_ms)
                .map(|value| value + ONE_MINUTE_MS)
        })
        .map(|value| value.max(window_start_open_ms))
        .unwrap_or(window_start_open_ms);

    if from_open_ms > last_closed_open_ms {
        BackfillDecision::Skip
    } else {
        BackfillDecision::Fetch {
            start_open_ms: from_open_ms,
            end_open_ms: last_closed_open_ms,
        }
    }
}

fn backfill_chunk_end(start_open_ms: i64, end_open_ms: i64) -> i64 {
    let page_span_ms = (HISTORY_PAGE_LIMIT as i64 - 1) * ONE_MINUTE_MS;
    (start_open_ms + page_span_ms).min(end_open_ms)
}

#[derive(Clone, Debug)]
struct SampleAccumulator {
    market: MarketRef,
    kind: PriceKind,
    bucket_ts_ms: i64,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    sample_count: i64,
    first_exchange_ts_ms: Option<i64>,
    last_exchange_ts_ms: Option<i64>,
    updated_at_ms: i64,
}

impl SampleAccumulator {
    fn new(tick: &NormalizedPriceTick) -> Self {
        let bucket_ts_ms = (tick.received_ts_ms / ONE_SECOND_MS) * ONE_SECOND_MS;
        Self {
            market: tick.market.clone(),
            kind: tick.kind,
            bucket_ts_ms,
            open: tick.price,
            high: tick.price,
            low: tick.price,
            close: tick.price,
            sample_count: 1,
            first_exchange_ts_ms: tick.exchange_ts_ms,
            last_exchange_ts_ms: tick.exchange_ts_ms,
            updated_at_ms: tick.received_ts_ms,
        }
    }

    fn update(&mut self, tick: &NormalizedPriceTick) {
        self.high = self.high.max(tick.price);
        self.low = self.low.min(tick.price);
        self.close = tick.price;
        self.sample_count += 1;
        self.last_exchange_ts_ms = tick.exchange_ts_ms.or(self.last_exchange_ts_ms);
        self.updated_at_ms = tick.received_ts_ms;
    }

    fn into_record(self) -> PriceSample1s {
        PriceSample1s {
            market: self.market,
            kind: self.kind,
            bucket_ts_ms: self.bucket_ts_ms,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            sample_count: self.sample_count,
            first_exchange_ts_ms: self.first_exchange_ts_ms,
            last_exchange_ts_ms: self.last_exchange_ts_ms,
            updated_at_ms: self.updated_at_ms,
        }
    }
}

#[derive(Clone, Debug)]
struct CandleAccumulator {
    market: MarketRef,
    kind: PriceKind,
    open_time_ms: i64,
    close_time_ms: i64,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    trade_count: Option<i64>,
    updated_at_ms: i64,
}

impl CandleAccumulator {
    fn new(tick: &NormalizedPriceTick) -> Self {
        let open_time_ms = (tick.received_ts_ms / ONE_MINUTE_MS) * ONE_MINUTE_MS;
        Self {
            market: tick.market.clone(),
            kind: tick.kind,
            open_time_ms,
            close_time_ms: open_time_ms + ONE_MINUTE_MS - 1,
            open: tick.price,
            high: tick.price,
            low: tick.price,
            close: tick.price,
            volume: tick.quantity.unwrap_or_default(),
            trade_count: Some(1),
            updated_at_ms: tick.received_ts_ms,
        }
    }

    fn update(&mut self, tick: &NormalizedPriceTick) {
        self.high = self.high.max(tick.price);
        self.low = self.low.min(tick.price);
        self.close = tick.price;
        self.volume += tick.quantity.unwrap_or_default();
        self.trade_count = self.trade_count.map(|value| value + 1);
        self.updated_at_ms = tick.received_ts_ms;
    }

    fn into_record(self, source: &str) -> PriceCandle1m {
        PriceCandle1m {
            market: self.market,
            kind: self.kind,
            open_time_ms: self.open_time_ms,
            close_time_ms: self.close_time_ms,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            trade_count: self.trade_count,
            source: source.to_string(),
            updated_at_ms: self.updated_at_ms,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct PriceAggregator {
    sample: Option<SampleAccumulator>,
    candle: Option<CandleAccumulator>,
}

impl PriceAggregator {
    fn apply_tick(
        &mut self,
        tick: &NormalizedPriceTick,
    ) -> (Vec<PriceSample1s>, Vec<PriceCandle1m>) {
        let mut samples = Vec::new();
        let mut candles = Vec::new();

        let sample_bucket = (tick.received_ts_ms / ONE_SECOND_MS) * ONE_SECOND_MS;
        match &mut self.sample {
            Some(current) if current.bucket_ts_ms == sample_bucket => current.update(tick),
            Some(current) => {
                samples.push(current.clone().into_record());
                self.sample = Some(SampleAccumulator::new(tick));
            }
            None => {
                self.sample = Some(SampleAccumulator::new(tick));
            }
        }

        let candle_open_ms = (tick.received_ts_ms / ONE_MINUTE_MS) * ONE_MINUTE_MS;
        match &mut self.candle {
            Some(current) if current.open_time_ms == candle_open_ms => current.update(tick),
            Some(current) => {
                candles.push(current.clone().into_record("live"));
                self.candle = Some(CandleAccumulator::new(tick));
            }
            None => {
                self.candle = Some(CandleAccumulator::new(tick));
            }
        }

        (samples, candles)
    }

    fn flush(self) -> (Vec<PriceSample1s>, Vec<PriceCandle1m>) {
        let mut samples = Vec::new();
        let mut candles = Vec::new();
        if let Some(sample) = self.sample {
            samples.push(sample.into_record());
        }
        if let Some(candle) = self.candle {
            candles.push(candle.into_record("live"));
        }
        (samples, candles)
    }
}

pub async fn run_price_runtime_once<S, R, W, C>(
    config: PriceRuntimeConfig,
    store: S,
    rest: R,
    ws: W,
    clock: C,
    adapters: Vec<Arc<dyn PriceVenueAdapter>>,
) -> DynResult<()>
where
    S: PriceStore + Clone + Send + Sync + 'static,
    R: RestClient + Clone + Send + Sync + 'static,
    W: WsClient + Clone + Send + Sync + 'static,
    C: Clock + Clone + Send + Sync + 'static,
{
    store.init().await?;
    let run_id = store.start_price_run(clock.now_ms()).await?;
    let mut tasks = tokio::task::JoinSet::new();

    for adapter in adapters {
        let store = store.clone();
        let rest = rest.clone();
        let ws = ws.clone();
        let clock = clock.clone();
        let config = config.clone();
        tasks.spawn(async move {
            let mut http_limiter = VenueHttpLimiter::new(config.http_min_interval_ms);
            let markets = discover_markets_with_retry(
                &store,
                &rest,
                &clock,
                &mut http_limiter,
                &*adapter,
                &config,
            )
            .await?;
            if markets.is_empty() {
                return Ok(());
            }

            let (live_ready_tx, live_ready_rx) = tokio::sync::oneshot::channel();
            let live_store = store.clone();
            let live_ws = ws.clone();
            let live_clock = clock.clone();
            let live_adapter = adapter.clone();
            let live_markets = markets.clone();
            let live_task = tokio::spawn(async move {
                live_once(
                    &live_store,
                    &live_ws,
                    &live_clock,
                    &*live_adapter,
                    &live_markets,
                    run_id,
                    Some(live_ready_tx),
                )
                .await
            });

            match live_ready_rx.await {
                Ok(()) => {}
                Err(_) => {
                    return match live_task.await {
                        Ok(result) => result,
                        Err(error) => Err(error.into()),
                    };
                }
            }

            let backfill_store = store.clone();
            let backfill_rest = rest.clone();
            let backfill_clock = clock.clone();
            let backfill_adapter = adapter.clone();
            let backfill_market_list = markets.clone();
            let backfill_config = config.clone();
            let backfill_task = tokio::spawn(async move {
                if let Err(error) = backfill_markets(
                    &backfill_store,
                    &backfill_rest,
                    &backfill_clock,
                    &mut http_limiter,
                    &*backfill_adapter,
                    &backfill_market_list,
                    &backfill_config,
                    run_id,
                )
                .await
                {
                    warn!(
                        venue = %backfill_adapter.venue(),
                        error = %error,
                        "price backfill worker failed"
                    );
                }
            });

            match live_task.await {
                Ok(Ok(())) => {
                    let _ = backfill_task.await;
                    Ok(())
                }
                Ok(Err(error)) => {
                    if !backfill_task.is_finished() {
                        backfill_task.abort();
                    }
                    let _ = backfill_task.await;
                    Err(error)
                }
                Err(error) => {
                    if !backfill_task.is_finished() {
                        backfill_task.abort();
                    }
                    let _ = backfill_task.await;
                    Err(error.into())
                }
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(error.into()),
        }
    }

    let retention_ms = config.sample_retention_days * 24 * 60 * 60 * 1_000;
    store
        .prune_price_samples_older_than(clock.now_ms() - retention_ms)
        .await?;
    Ok(())
}

async fn discover_markets_with_retry<S, R, C>(
    store: &S,
    rest: &R,
    clock: &C,
    http_limiter: &mut VenueHttpLimiter,
    adapter: &dyn PriceVenueAdapter,
    config: &PriceRuntimeConfig,
) -> DynResult<Vec<PriceMarket>>
where
    S: PriceStore + Send + Sync,
    R: RestClient + Send + Sync,
    C: Clock + Send + Sync,
{
    let cached = store.load_price_markets(Some(adapter.venue())).await?;
    if !cached.is_empty() {
        return Ok(cached);
    }

    let mut backoff = Duration::from_millis(500);
    for attempt in 1..=config.discovery_max_attempts {
        let request = adapter.discovery_request();
        let request_method = http_method_name(request.method);
        let request_body = preview_optional_json(request.body.as_ref());
        let request_url = request.url.clone();
        http_limiter.wait_turn(clock).await;
        let body = execute_request(rest, &request).await;
        match body {
            Ok(body) => {
                let markets = adapter.discover_markets(&body).map_err(|error| {
                    io::Error::other(format!(
                        "price market discovery parse failed venue={} method={} url={} request_body={} response_preview={} error={}",
                        adapter.venue(),
                        request_method,
                        request_url,
                        request_body,
                        preview_text(&body),
                        error,
                    ))
                })?;
                store.upsert_price_markets(&markets).await?;
                info!(
                    venue = %adapter.venue(),
                    market_count = markets.len(),
                    "price market discovery succeeded"
                );
                return Ok(markets);
            }
            Err(error) => {
                if attempt == config.discovery_max_attempts {
                    return Err(error);
                }
                warn!(
                    venue = %adapter.venue(),
                    attempt,
                    max_attempts = config.discovery_max_attempts,
                    backoff_ms = backoff.as_millis() as i64,
                    error = %error,
                    "price market discovery failed, retrying"
                );
                clock.sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(8));
            }
        }
    }

    Ok(Vec::new())
}

async fn backfill_markets<S, R, C>(
    store: &S,
    rest: &R,
    clock: &C,
    http_limiter: &mut VenueHttpLimiter,
    adapter: &dyn PriceVenueAdapter,
    markets: &[PriceMarket],
    config: &PriceRuntimeConfig,
    run_id: i64,
) -> DynResult<()>
where
    S: PriceStore + Send + Sync,
    R: RestClient + Send + Sync,
    C: Clock + Send + Sync,
{
    for market in markets {
        for kind in [PriceKind::Trade, PriceKind::Reference] {
            let epoch_id = store
                .open_price_epoch(run_id, &market.market, kind, 1, clock.now_ms())
                .await?;
            let checkpoint = store.load_price_checkpoint(&market.market, kind).await?;
            let supports_history = match kind {
                PriceKind::Trade => market.supports_trade_history,
                PriceKind::Reference => market.supports_reference_history,
                PriceKind::All => false,
            };

            let decision = plan_backfill(
                checkpoint.as_ref(),
                kind,
                supports_history,
                clock.now_ms(),
                config.backfill_window_days,
            );

            match decision {
                BackfillDecision::Fetch {
                    start_open_ms,
                    end_open_ms,
                } => {
                    let mut chunk_start_open_ms = start_open_ms;
                    while chunk_start_open_ms <= end_open_ms {
                        let chunk_end_open_ms =
                            backfill_chunk_end(chunk_start_open_ms, end_open_ms);
                        let Some(request) =
                            adapter.history_request(crate::price_model::PriceHistoryRequest {
                                market: market.clone(),
                                kind,
                                start_ms: chunk_start_open_ms,
                                end_ms: chunk_end_open_ms + ONE_MINUTE_MS - 1,
                                limit: HISTORY_PAGE_LIMIT,
                            })
                        else {
                            warn!(
                                venue = %adapter.venue(),
                                market = %market.market.symbol,
                                kind = %kind,
                                start_open_ms = chunk_start_open_ms,
                                end_open_ms = chunk_end_open_ms,
                                "price history request unavailable for backfill chunk"
                            );
                            store
                                .commit_price_batch(PriceCommitBatch {
                                    market: market.market.clone(),
                                    kind,
                                    epoch_id: Some(epoch_id),
                                    samples_1s: Vec::new(),
                                    candles_1m: Vec::new(),
                                    checkpoint: None,
                                    gaps: vec![PriceGapWindow {
                                        market: market.market.clone(),
                                        kind,
                                        resolution: PriceResolution::OneMinute,
                                        started_at_ms: chunk_start_open_ms,
                                        ended_at_ms: end_open_ms + ONE_MINUTE_MS - 1,
                                        reason: "history_request_unavailable".to_string(),
                                    }],
                                })
                                .await?;
                            break;
                        };
                        let request_method = http_method_name(request.method);
                        let request_body = preview_optional_json(request.body.as_ref());
                        let request_url = request.url.clone();
                        http_limiter.wait_turn(clock).await;
                        let body = match execute_request(rest, &request).await {
                            Ok(body) => body,
                            Err(error) => {
                                warn!(
                                    venue = %adapter.venue(),
                                    market = %market.market.symbol,
                                    kind = %kind,
                                    method = request_method,
                                    url = %request_url,
                                    request_body = %request_body,
                                    start_open_ms = chunk_start_open_ms,
                                    end_open_ms = chunk_end_open_ms,
                                    error = %error,
                                    "price history request failed"
                                );
                                store
                                    .commit_price_batch(PriceCommitBatch {
                                        market: market.market.clone(),
                                        kind,
                                        epoch_id: Some(epoch_id),
                                        samples_1s: Vec::new(),
                                        candles_1m: Vec::new(),
                                        checkpoint: None,
                                        gaps: vec![PriceGapWindow {
                                            market: market.market.clone(),
                                            kind,
                                            resolution: PriceResolution::OneMinute,
                                            started_at_ms: chunk_start_open_ms,
                                            ended_at_ms: end_open_ms + ONE_MINUTE_MS - 1,
                                            reason: "backfill_request_failed".to_string(),
                                        }],
                                    })
                                    .await?;
                                break;
                            }
                        };
                        let candles =
                            match adapter.parse_history_candles(market, kind, &body).map_err(
                                |error| {
                                    io::Error::other(format!(
                                        "price history parse failed venue={} market={} kind={} method={} url={} request_body={} response_preview={} error={}",
                                        adapter.venue(),
                                        market.market.symbol,
                                        kind.as_str(),
                                        request_method,
                                        request_url,
                                        request_body,
                                        preview_text(&body),
                                        error,
                                    ))
                                },
                            ) {
                                Ok(candles) => candles,
                                Err(error) => {
                                    warn!(
                                        venue = %adapter.venue(),
                                        market = %market.market.symbol,
                                        kind = %kind,
                                        method = request_method,
                                        url = %request_url,
                                        request_body = %request_body,
                                        response_preview = %preview_text(&body),
                                        start_open_ms = chunk_start_open_ms,
                                        end_open_ms = chunk_end_open_ms,
                                        error = %error,
                                        "price history parse failed"
                                    );
                                    store
                                        .commit_price_batch(PriceCommitBatch {
                                            market: market.market.clone(),
                                            kind,
                                            epoch_id: Some(epoch_id),
                                            samples_1s: Vec::new(),
                                            candles_1m: Vec::new(),
                                            checkpoint: None,
                                            gaps: vec![PriceGapWindow {
                                                market: market.market.clone(),
                                                kind,
                                                resolution: PriceResolution::OneMinute,
                                                started_at_ms: chunk_start_open_ms,
                                                ended_at_ms: end_open_ms + ONE_MINUTE_MS - 1,
                                                reason: "backfill_parse_failed".to_string(),
                                            }],
                                        })
                                        .await?;
                                    break;
                                }
                            };
                        let checkpoint = candles.last().map(|last| PriceCheckpoint {
                            market: market.market.clone(),
                            kind,
                            epoch_id,
                            last_live_bucket_ms: checkpoint
                                .as_ref()
                                .and_then(|value| value.last_live_bucket_ms),
                            last_candle_open_ms: Some(last.open_time_ms),
                            last_backfill_open_ms: Some(last.open_time_ms),
                            last_exchange_ts_ms: checkpoint
                                .as_ref()
                                .and_then(|value| value.last_exchange_ts_ms),
                            updated_at_ms: clock.now_ms(),
                            status: "backfilled".to_string(),
                        });
                        store
                            .commit_price_batch(PriceCommitBatch {
                                market: market.market.clone(),
                                kind,
                                epoch_id: Some(epoch_id),
                                samples_1s: Vec::new(),
                                candles_1m: candles,
                                checkpoint,
                                gaps: Vec::new(),
                            })
                            .await?;
                        chunk_start_open_ms = chunk_end_open_ms + ONE_MINUTE_MS;
                    }
                }
                BackfillDecision::Gap { resolution, reason } => {
                    store
                        .commit_price_batch(PriceCommitBatch {
                            market: market.market.clone(),
                            kind,
                            epoch_id: Some(epoch_id),
                            samples_1s: Vec::new(),
                            candles_1m: Vec::new(),
                            checkpoint: None,
                            gaps: vec![PriceGapWindow {
                                market: market.market.clone(),
                                kind,
                                resolution,
                                started_at_ms: clock.now_ms(),
                                ended_at_ms: clock.now_ms(),
                                reason,
                            }],
                        })
                        .await?;
                }
                BackfillDecision::Skip => {}
            }
        }
    }
    Ok(())
}

async fn live_once<S, W, C>(
    store: &S,
    ws: &W,
    clock: &C,
    adapter: &dyn PriceVenueAdapter,
    markets: &[PriceMarket],
    run_id: i64,
    ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
) -> DynResult<()>
where
    S: PriceStore + Send + Sync,
    W: WsClient + Send + Sync,
    C: Clock + Send + Sync,
{
    let ws_url = adapter.ws_url();
    info!(
        venue = %adapter.venue(),
        market_count = markets.len(),
        url = %ws_url,
        "price websocket connecting"
    );
    let mut connection = ws.connect(&ws_url).await.map_err(|error| {
        io::Error::other(format!(
            "price websocket connect failed venue={} url={} error={}",
            adapter.venue(),
            ws_url,
            error,
        ))
    })?;
    info!(
        venue = %adapter.venue(),
        market_count = markets.len(),
        url = %ws_url,
        "price websocket connected"
    );
    for message in adapter.subscription_messages(markets) {
        let preview = preview_text(&message);
        connection.send_text(message).await.map_err(|error| {
            io::Error::other(format!(
                "price websocket subscription failed venue={} url={} message_preview={} error={}",
                adapter.venue(),
                ws_url,
                preview,
                error,
            ))
        })?;
    }
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(());
    }

    let mut epoch_by_key: HashMap<(String, PriceKind), i64> = HashMap::new();
    let mut aggregator_by_key: HashMap<(String, PriceKind), PriceAggregator> = HashMap::new();

    while let Some(raw) = connection.next_text().await.map_err(|error| {
        io::Error::other(format!(
            "price websocket receive failed venue={} url={} error={}",
            adapter.venue(),
            ws_url,
            error,
        ))
    })? {
        match adapter.parse_ws_message_ticks(&raw, clock.now_ms()) {
            Ok(ticks) => {
                for tick in ticks {
                    let epoch_id = match epoch_by_key.get(&(tick.market.symbol.clone(), tick.kind))
                    {
                        Some(epoch_id) => *epoch_id,
                        None => {
                            let epoch_id = store
                                .open_price_epoch(
                                    run_id,
                                    &tick.market,
                                    tick.kind,
                                    1,
                                    clock.now_ms(),
                                )
                                .await?;
                            epoch_by_key.insert((tick.market.symbol.clone(), tick.kind), epoch_id);
                            epoch_id
                        }
                    };

                    let aggregator = aggregator_by_key
                        .entry((tick.market.symbol.clone(), tick.kind))
                        .or_default();
                    let (samples, candles) = aggregator.apply_tick(&tick);
                    let checkpoint = PriceCheckpoint {
                        market: tick.market.clone(),
                        kind: tick.kind,
                        epoch_id,
                        last_live_bucket_ms: Some(
                            (tick.received_ts_ms / ONE_SECOND_MS) * ONE_SECOND_MS,
                        ),
                        last_candle_open_ms: Some(
                            (tick.received_ts_ms / ONE_MINUTE_MS) * ONE_MINUTE_MS,
                        ),
                        last_backfill_open_ms: store
                            .load_price_checkpoint(&tick.market, tick.kind)
                            .await?
                            .and_then(|value| value.last_backfill_open_ms),
                        last_exchange_ts_ms: tick.exchange_ts_ms,
                        updated_at_ms: clock.now_ms(),
                        status: "live".to_string(),
                    };
                    store
                        .commit_price_batch(PriceCommitBatch {
                            market: tick.market.clone(),
                            kind: tick.kind,
                            epoch_id: Some(epoch_id),
                            samples_1s: samples,
                            candles_1m: candles,
                            checkpoint: Some(checkpoint),
                            gaps: Vec::new(),
                        })
                        .await?;
                }
            }
            Err(error) => {
                warn!(
                    venue = %adapter.venue(),
                    url = %ws_url,
                    error = %error,
                    raw_payload = %preview_text(&raw),
                    "price websocket parse failed"
                );
            }
        };
    }

    for ((symbol, kind), aggregator) in aggregator_by_key {
        let Some(epoch_id) = epoch_by_key.get(&(symbol.clone(), kind)).copied() else {
            continue;
        };
        let (samples, candles) = aggregator.flush();
        if samples.is_empty() && candles.is_empty() {
            continue;
        }
        let market = samples
            .first()
            .map(|sample| sample.market.clone())
            .or_else(|| candles.first().map(|candle| candle.market.clone()))
            .expect("flush always has data");
        let last_candle_open_ms = candles
            .last()
            .map(|candle| candle.open_time_ms)
            .or_else(|| {
                samples
                    .last()
                    .map(|sample| (sample.bucket_ts_ms / ONE_MINUTE_MS) * ONE_MINUTE_MS)
            });
        let last_live_bucket_ms = samples.last().map(|sample| sample.bucket_ts_ms);
        store
            .commit_price_batch(PriceCommitBatch {
                market: market.clone(),
                kind,
                epoch_id: Some(epoch_id),
                samples_1s: samples,
                candles_1m: candles,
                checkpoint: Some(PriceCheckpoint {
                    market,
                    kind,
                    epoch_id,
                    last_live_bucket_ms,
                    last_candle_open_ms,
                    last_backfill_open_ms: store
                        .load_price_checkpoint(&MarketRef::new(adapter.venue(), symbol), kind)
                        .await?
                        .and_then(|value| value.last_backfill_open_ms),
                    last_exchange_ts_ms: None,
                    updated_at_ms: clock.now_ms(),
                    status: "live".to_string(),
                }),
                gaps: Vec::new(),
            })
            .await?;
    }

    Ok(())
}

async fn execute_request<R>(rest: &R, request: &DiscoveryRequest) -> DynResult<String>
where
    R: RestClient + Send + Sync,
{
    let request_method = http_method_name(request.method);
    let request_body = preview_optional_json(request.body.as_ref());
    match request.method {
        HttpMethod::Get => rest.get_text(&request.url).await.map_err(|error| {
            io::Error::other(format!(
                "http request failed method={} url={} request_body={} error={}",
                request_method, request.url, request_body, error,
            ))
            .into()
        }),
        HttpMethod::Post => {
            let body = request.body.clone().unwrap_or_default();
            rest.post_json_text(&request.url, &body)
                .await
                .map_err(|error| {
                    io::Error::other(format!(
                        "http request failed method={} url={} request_body={} error={}",
                        request_method, request.url, request_body, error,
                    ))
                    .into()
                })
        }
    }
}
