use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::adapters::{
    BinanceAdapter, DiscoveryRequest, HttpMethod, HyperliquidAdapter, LighterAdapter, VenueAdapter,
};
use crate::diagnostics::{http_method_name, preview_optional_json, preview_text};
use crate::model::{BookView, GapWindow, MarketRef, NormalizedBookEvent, NormalizedMarket};
use crate::sync::{BinanceBookSync, GenericBookSync, SyncOutcome};
use crate::traits::{
    BookStore, Clock, CommitBatch, DynResult, RestClient, SnapshotRecord, WsClient, WsConnection,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub database_path: String,
    pub snapshot_every_events: usize,
    pub snapshot_every_ms: i64,
    pub max_markets_per_connection: usize,
    pub discovery_max_attempts: usize,
    pub reconnect_backoff_ms: u64,
    pub reconnect_backoff_cap_ms: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            database_path: "tokenresearch.sqlite".to_string(),
            snapshot_every_events: 100,
            snapshot_every_ms: 60_000,
            max_markets_per_connection: 25,
            discovery_max_attempts: 5,
            reconnect_backoff_ms: 1_000,
            reconnect_backoff_cap_ms: 30_000,
        }
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("runtime error: {0}")]
    Other(String),
}

#[derive(Clone, Debug)]
pub struct MarketSession {
    pub run_id: i64,
    pub epoch_id: i64,
    pub epoch_seq: i64,
    pub events_since_snapshot: usize,
    pub last_snapshot_at_ms: Option<i64>,
}

pub struct CollectorRuntime<S, C> {
    store: Arc<S>,
    writer: Arc<Mutex<Option<StoreWriterHandle>>>,
    clock: Arc<C>,
    config: RuntimeConfig,
}

impl<S, C> Clone for CollectorRuntime<S, C> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            writer: self.writer.clone(),
            clock: self.clock.clone(),
            config: self.config.clone(),
        }
    }
}

impl<S, C> CollectorRuntime<S, C>
where
    S: BookStore + 'static,
    C: Clock + 'static,
{
    pub fn new(store: Arc<S>, clock: Arc<C>, config: RuntimeConfig) -> Self {
        Self {
            store,
            writer: Arc::new(Mutex::new(None)),
            clock,
            config,
        }
    }

    fn owned_clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            writer: self.writer.clone(),
            clock: self.clock.clone(),
            config: self.config.clone(),
        }
    }

    pub async fn bootstrap_run(&self) -> DynResult<i64> {
        self.store.init().await?;
        let run_id = self.store.start_run(self.clock.now_ms()).await?;
        self.ensure_writer().await;
        Ok(run_id)
    }

    pub async fn discover_markets<A: VenueAdapter>(
        &self,
        rest: &dyn RestClient,
        adapter: &A,
    ) -> DynResult<Vec<NormalizedMarket>> {
        let request = adapter.discovery_request();
        let request_method = http_method_name(request.method);
        let request_body = preview_optional_json(request.body.as_ref());
        let request_url = request.url.clone();
        let body = execute_request(rest, &request).await?;
        let markets = adapter
            .discover_markets(&body)
            .map_err(|error| {
                RuntimeError::Other(format!(
                    "market discovery parse failed venue={} method={} url={} request_body={} response_preview={} error={}",
                    adapter.venue().as_str(),
                    request_method,
                    request_url,
                    request_body,
                    preview_text(&body),
                    error,
                ))
            })?;
        self.store.upsert_markets(&markets).await?;
        Ok(markets)
    }

    pub async fn discover_markets_with_retry<A: VenueAdapter>(
        &self,
        rest: &dyn RestClient,
        adapter: &A,
    ) -> DynResult<Vec<NormalizedMarket>> {
        let cached_markets = self
            .store
            .load_markets(Some(adapter.venue()))
            .await?
            .into_iter()
            .filter(|market| market.status == crate::model::MarketStatus::Active)
            .collect::<Vec<_>>();
        if !cached_markets.is_empty() {
            tracing::info!(
                venue = %adapter.venue().as_str(),
                count = cached_markets.len(),
                "using cached market metadata"
            );
            return Ok(cached_markets);
        }

        let max_attempts = self.config.discovery_max_attempts.max(1);
        let mut attempt = 0usize;
        let mut backoff_ms = self.config.reconnect_backoff_ms.max(1);

        loop {
            attempt += 1;
            match self.discover_markets(rest, adapter).await {
                Ok(markets) => return Ok(markets),
                Err(error) => {
                    if attempt >= max_attempts {
                        return Err(error);
                    }
                    tracing::warn!(
                        venue = %adapter.venue().as_str(),
                        attempt,
                        max_attempts,
                        backoff_ms,
                        error = %error,
                        "market discovery failed, retrying"
                    );
                    self.clock.sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(self.config.reconnect_backoff_cap_ms);
                }
            }
        }
    }

    async fn ensure_writer(&self) {
        let mut guard = self.writer.lock().await;
        if guard.is_none() {
            *guard = Some(StoreWriterHandle::spawn(self.store.clone()));
        }
    }

    async fn writer(&self) -> DynResult<StoreWriterHandle> {
        self.ensure_writer().await;
        self.writer
            .lock()
            .await
            .clone()
            .ok_or_else(|| RuntimeError::Other("store writer not initialized".to_string()).into())
    }

    pub async fn open_market_session(
        &self,
        run_id: i64,
        market: &MarketRef,
        epoch_seq: i64,
    ) -> DynResult<MarketSession> {
        let now_ms = self.clock.now_ms();
        let epoch_id = self
            .writer()
            .await?
            .open_epoch(run_id, market, epoch_seq, now_ms)
            .await?;
        Ok(MarketSession {
            run_id,
            epoch_id,
            epoch_seq,
            events_since_snapshot: 0,
            last_snapshot_at_ms: None,
        })
    }

    pub async fn apply_binance_outcome(
        &self,
        session: &mut MarketSession,
        sync: &BinanceBookSync,
        outcome: SyncOutcome,
    ) -> DynResult<()> {
        self.persist_outcome(session, sync.book().view(50), outcome)
            .await
    }

    pub async fn apply_generic_outcome(
        &self,
        session: &mut MarketSession,
        sync: &GenericBookSync,
        outcome: SyncOutcome,
    ) -> DynResult<()> {
        self.persist_outcome(session, sync.book().view(50), outcome)
            .await
    }

    async fn persist_outcome(
        &self,
        session: &mut MarketSession,
        current_book: BookView,
        outcome: SyncOutcome,
    ) -> DynResult<()> {
        if outcome.needs_resync && outcome.epoch_seq != session.epoch_seq {
            self.writer()
                .await?
                .close_epoch(session.epoch_id, self.clock.now_ms(), "resync")
                .await?;
            session.epoch_seq = outcome.epoch_seq;
            session.epoch_id = self
                .writer()
                .await?
                .open_epoch(
                    session.run_id,
                    &current_book.market,
                    session.epoch_seq,
                    self.clock.now_ms(),
                )
                .await?;
            session.events_since_snapshot = 0;
            session.last_snapshot_at_ms = None;
        }

        if outcome.accepted_events.is_empty() && outcome.gap.is_none() {
            return Ok(());
        }

        let now_ms = self.clock.now_ms();
        session.events_since_snapshot += outcome.accepted_events.len();

        let should_snapshot = !current_book.bids.is_empty()
            && !current_book.asks.is_empty()
            && (session.last_snapshot_at_ms.is_none()
                || session.events_since_snapshot >= self.config.snapshot_every_events
                || now_ms - session.last_snapshot_at_ms.unwrap_or(0)
                    >= self.config.snapshot_every_ms);

        let snapshot = should_snapshot.then(|| SnapshotRecord {
            created_at_ms: now_ms,
            depth: current_book.bids.len().max(current_book.asks.len()),
            book: current_book.clone(),
        });

        self.writer()
            .await?
            .commit_batch(CommitBatch {
                market: current_book.market.clone(),
                epoch_id: session.epoch_id,
                events: outcome.accepted_events,
                latest_book: outcome.latest_book.or_else(|| Some(current_book.clone())),
                snapshot: snapshot.clone(),
                checkpoint: outcome.checkpoint,
                gaps: outcome.gap.into_iter().collect(),
            })
            .await?;

        if snapshot.is_some() {
            session.events_since_snapshot = 0;
            session.last_snapshot_at_ms = Some(now_ms);
        }

        Ok(())
    }

    pub async fn run_live<R, W>(&self, rest: R, ws: W) -> DynResult<()>
    where
        R: RestClient + Clone + Send + Sync + 'static,
        W: WsClient + Clone + Send + Sync + 'static,
    {
        let run_id = self.bootstrap_run().await?;
        let mut tasks = Vec::new();

        match self
            .discover_markets_with_retry(&rest, &BinanceAdapter)
            .await
        {
            Ok(binance) if !binance.is_empty() => {
                for batch in split_batches(binance, self.config.max_markets_per_connection) {
                    let runtime = self.owned_clone();
                    let rest = rest.clone();
                    let ws = ws.clone();
                    tasks.push(tokio::spawn(async move {
                        runtime.run_binance_batch(run_id, rest, ws, batch).await
                    }));
                }
            }
            Ok(_) => {
                tracing::warn!("binance discovery returned no active markets");
            }
            Err(error) => {
                tracing::error!(error = %error, "binance discovery failed after retries");
            }
        }

        match self
            .discover_markets_with_retry(&rest, &HyperliquidAdapter)
            .await
        {
            Ok(hyperliquid) if !hyperliquid.is_empty() => {
                for batch in split_batches(hyperliquid, self.config.max_markets_per_connection) {
                    let runtime = self.owned_clone();
                    let ws = ws.clone();
                    tasks.push(tokio::spawn(async move {
                        runtime
                            .run_generic_batch(run_id, ws, HyperliquidAdapter, batch)
                            .await
                    }));
                }
            }
            Ok(_) => {
                tracing::warn!("hyperliquid discovery returned no active markets");
            }
            Err(error) => {
                tracing::error!(error = %error, "hyperliquid discovery failed after retries");
            }
        }

        let lighter_adapter = LighterAdapter::default();
        match self
            .discover_markets_with_retry(&rest, &lighter_adapter)
            .await
        {
            Ok(lighter) if !lighter.is_empty() => {
                for batch in split_batches(lighter, self.config.max_markets_per_connection) {
                    let runtime = self.owned_clone();
                    let ws = ws.clone();
                    let adapter = lighter_adapter.clone();
                    tasks.push(tokio::spawn(async move {
                        runtime.run_generic_batch(run_id, ws, adapter, batch).await
                    }));
                }
            }
            Ok(_) => {
                tracing::warn!("lighter discovery returned no active markets");
            }
            Err(error) => {
                tracing::error!(error = %error, "lighter discovery failed after retries");
            }
        }

        if tasks.is_empty() {
            return Err(
                RuntimeError::Other("no venue bootstrapped successfully".to_string()).into(),
            );
        }

        tokio::signal::ctrl_c().await?;
        for task in tasks {
            task.abort();
        }
        Ok(())
    }

    async fn run_binance_batch<R, W>(
        &self,
        run_id: i64,
        rest: R,
        ws: W,
        markets: Vec<NormalizedMarket>,
    ) -> DynResult<()>
    where
        R: RestClient + Clone + Send + Sync + 'static,
        W: WsClient + Clone + Send + Sync + 'static,
    {
        let adapter = BinanceAdapter;
        let mut backoff_ms = self.config.reconnect_backoff_ms;
        loop {
            let ws_url = adapter.ws_url(&markets);
            let mut connection = match ws.connect(&ws_url).await {
                Ok(connection) => connection,
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        url = %ws_url,
                        market_count = markets.len(),
                        "binance websocket connect failed"
                    );
                    self.clock.sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(self.config.reconnect_backoff_cap_ms);
                    continue;
                }
            };
            backoff_ms = self.config.reconnect_backoff_ms;

            let market_by_symbol = markets
                .iter()
                .cloned()
                .map(|market| (market.market.symbol.clone(), market))
                .collect::<HashMap<_, _>>();
            let mut states = HashMap::new();
            for market in &markets {
                let session = self.open_market_session(run_id, &market.market, 1).await?;
                states.insert(
                    market.market.symbol.clone(),
                    (session, BinanceBookSync::new(market.market.clone())),
                );
            }

            let (snapshot_request_tx, mut snapshot_request_rx) =
                mpsc::unbounded_channel::<(NormalizedMarket, i64)>();
            let (snapshot_result_tx, mut snapshot_result_rx) =
                mpsc::unbounded_channel::<(String, DynResult<NormalizedBookEvent>)>();
            let rest_for_snapshots = rest.clone();
            tokio::spawn(async move {
                while let Some((market, received_ts_ms)) = snapshot_request_rx.recv().await {
                    let result =
                        fetch_binance_snapshot(&rest_for_snapshots, &market, received_ts_ms).await;
                    let _ = snapshot_result_tx.send((market.market.symbol.clone(), result));
                }
            });
            let mut snapshot_in_flight = HashSet::new();
            let mut snapshot_retry_after_ms = HashMap::<String, i64>::new();
            let mut snapshot_backoff_ms = HashMap::<String, u64>::new();

            loop {
                tokio::select! {
                    maybe_snapshot = snapshot_result_rx.recv() => {
                        if let Some((symbol, result)) = maybe_snapshot {
                            snapshot_in_flight.remove(&symbol);
                            match result {
                                Ok(snapshot) => {
                                    snapshot_retry_after_ms.remove(&symbol);
                                    snapshot_backoff_ms.remove(&symbol);
                                    if let Some((session, sync)) = states.get_mut(&symbol) {
                                        let outcome = sync.on_snapshot(snapshot);
                                        self.apply_binance_outcome(session, sync, outcome).await?;
                                    }
                                }
                                Err(error) => {
                                    let next_backoff = snapshot_backoff_ms
                                        .get(&symbol)
                                        .copied()
                                        .unwrap_or_else(|| self.config.reconnect_backoff_ms.max(1));
                                    snapshot_retry_after_ms.insert(
                                        symbol.clone(),
                                        self.clock.now_ms() + next_backoff as i64,
                                    );
                                    snapshot_backoff_ms.insert(
                                        symbol.clone(),
                                        (next_backoff * 2).min(self.config.reconnect_backoff_cap_ms),
                                    );
                                    tracing::warn!(
                                        error = %error,
                                        market = %symbol,
                                        retry_after_ms = next_backoff,
                                        "binance snapshot fetch failed"
                                    );
                                }
                            }
                        }
                    }
                    message = connection.next_text() => {
                        match message {
                            Ok(Some(raw)) => {
                                match adapter.parse_ws_message(&raw, self.clock.now_ms()) {
                                    Ok(Some(event)) => {
                                        let symbol = event.market.symbol.clone();
                                        let received_ts_ms = event.received_ts_ms;
                                        if let Some((session, sync)) = states.get_mut(&symbol) {
                                            let outcome = sync.on_delta(event);
                                            let should_request_snapshot = outcome.needs_resync
                                                && outcome.accepted_events.is_empty();
                                            self.apply_binance_outcome(session, sync, outcome).await?;
                                            let snapshot_retry_ready = snapshot_retry_after_ms
                                                .get(&symbol)
                                                .is_none_or(|retry_at_ms| self.clock.now_ms() >= *retry_at_ms);
                                            if should_request_snapshot
                                                && snapshot_retry_ready
                                                && snapshot_in_flight.insert(symbol.clone())
                                            {
                                                if let Some(market) = market_by_symbol.get(&symbol) {
                                                    if snapshot_request_tx.send((market.clone(), received_ts_ms)).is_err() {
                                                        snapshot_in_flight.remove(&symbol);
                                                        tracing::warn!(market = %symbol, "binance snapshot worker unavailable");
                                                    }
                                                } else {
                                                    snapshot_in_flight.remove(&symbol);
                                                }
                                            }
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        tracing::warn!(
                                            error = %error,
                                            url = %ws_url,
                                            raw_payload = %preview_text(&raw),
                                            "binance websocket parse failed"
                                        );
                                    }
                                }
                            }
                            Ok(None) => {
                                tracing::warn!(url = %ws_url, "binance websocket closed");
                                self.record_disconnect_gaps(&mut states, "binance_disconnect").await?;
                                break;
                            }
                            Err(error) => {
                                tracing::warn!(
                                    error = %error,
                                    url = %ws_url,
                                    "binance websocket receive failed"
                                );
                                self.record_disconnect_gaps(&mut states, "binance_disconnect").await?;
                                break;
                            }
                        }
                    }
                }
            }

            self.clock.sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(self.config.reconnect_backoff_cap_ms);
        }
    }

    async fn run_generic_batch<W, A>(
        &self,
        run_id: i64,
        ws: W,
        adapter: A,
        markets: Vec<NormalizedMarket>,
    ) -> DynResult<()>
    where
        W: WsClient + Clone + Send + Sync + 'static,
        A: VenueAdapter + Clone + Send + Sync + 'static,
    {
        let mut backoff_ms = self.config.reconnect_backoff_ms;
        loop {
            let ws_url = adapter.ws_url(&markets);
            let mut connection = match ws.connect(&ws_url).await {
                Ok(connection) => connection,
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        venue = %adapter.venue().as_str(),
                        url = %ws_url,
                        market_count = markets.len(),
                        "generic websocket connect failed"
                    );
                    self.clock.sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(self.config.reconnect_backoff_cap_ms);
                    continue;
                }
            };
            backoff_ms = self.config.reconnect_backoff_ms;

            for message in adapter.subscription_messages(&markets) {
                connection.send_text(message).await?;
            }

            let mut states = HashMap::new();
            for market in &markets {
                let session = self.open_market_session(run_id, &market.market, 1).await?;
                states.insert(
                    market.market.symbol.clone(),
                    (session, GenericBookSync::new(market.market.clone())),
                );
            }

            loop {
                match connection.next_text().await {
                    Ok(Some(raw)) => match adapter.parse_ws_message(&raw, self.clock.now_ms()) {
                        Ok(Some(event)) => {
                            if let Some((session, sync)) = states.get_mut(&event.market.symbol) {
                                let outcome = sync.apply(event);
                                self.apply_generic_outcome(session, sync, outcome).await?;
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::warn!(
                                error = %error,
                                venue = %adapter.venue().as_str(),
                                url = %ws_url,
                                raw_payload = %preview_text(&raw),
                                "generic websocket parse failed"
                            );
                        }
                    },
                    Ok(None) => {
                        tracing::warn!(
                            venue = %adapter.venue().as_str(),
                            url = %ws_url,
                            "generic websocket closed"
                        );
                        self.record_disconnect_gaps(&mut states, "ws_disconnect")
                            .await?;
                        break;
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            venue = %adapter.venue().as_str(),
                            url = %ws_url,
                            "generic websocket receive failed"
                        );
                        self.record_disconnect_gaps(&mut states, "ws_disconnect")
                            .await?;
                        break;
                    }
                }
            }

            self.clock.sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(self.config.reconnect_backoff_cap_ms);
        }
    }

    async fn record_disconnect_gaps<T>(
        &self,
        states: &mut HashMap<String, (MarketSession, T)>,
        reason: &str,
    ) -> DynResult<()>
    where
        T: DisconnectMarket,
    {
        let now_ms = self.clock.now_ms();
        for (session, state) in states.values_mut() {
            let market = state.market().clone();
            self.writer()
                .await?
                .commit_batch(CommitBatch {
                    market: market.clone(),
                    epoch_id: session.epoch_id,
                    events: Vec::new(),
                    latest_book: None,
                    snapshot: None,
                    checkpoint: None,
                    gaps: vec![GapWindow {
                        market: market.clone(),
                        epoch_id: Some(session.epoch_id),
                        started_at_ms: now_ms,
                        ended_at_ms: now_ms,
                        expected_sequence: None,
                        observed_sequence: None,
                        reason: reason.to_string(),
                    }],
                })
                .await?;
            self.writer()
                .await?
                .close_epoch(session.epoch_id, now_ms, reason)
                .await?;
            session.epoch_seq += 1;
            session.epoch_id = self
                .writer()
                .await?
                .open_epoch(session.run_id, &market, session.epoch_seq, now_ms)
                .await?;
            state.reset();
        }
        Ok(())
    }
}

trait DisconnectMarket {
    fn market(&self) -> &MarketRef;
    fn reset(&mut self);
}

async fn fetch_binance_snapshot<R: RestClient>(
    rest: &R,
    market: &NormalizedMarket,
    received_ts_ms: i64,
) -> DynResult<NormalizedBookEvent> {
    let request = BinanceAdapter
        .snapshot_request(market)
        .ok_or_else(|| RuntimeError::Other("binance snapshot request missing".to_string()))?;
    let request_method = http_method_name(request.method);
    let request_body = preview_optional_json(request.body.as_ref());
    let request_url = request.url.clone();
    let body = execute_request(rest, &request).await?;
    BinanceAdapter
        .parse_snapshot(market, &body, received_ts_ms)
        .map_err(|error| {
            RuntimeError::Other(format!(
                "binance snapshot parse failed market={} method={} url={} request_body={} response_preview={} error={}",
                market.market.symbol,
                request_method,
                request_url,
                request_body,
                preview_text(&body),
                error,
            ))
            .into()
        })
}

async fn execute_request(rest: &dyn RestClient, request: &DiscoveryRequest) -> DynResult<String> {
    let request_method = http_method_name(request.method);
    let request_body = preview_optional_json(request.body.as_ref());
    match request.method {
        HttpMethod::Get => rest.get_text(&request.url).await.map_err(|error| {
            RuntimeError::Other(format!(
                "http request failed method={} url={} request_body={} error={}",
                request_method, request.url, request_body, error,
            ))
            .into()
        }),
        HttpMethod::Post => {
            let body = request.body.clone().unwrap_or(Value::Null);
            rest.post_json_text(&request.url, &body)
                .await
                .map_err(|error| {
                    RuntimeError::Other(format!(
                        "http request failed method={} url={} request_body={} error={}",
                        request_method, request.url, request_body, error,
                    ))
                    .into()
                })
        }
    }
}

impl DisconnectMarket for BinanceBookSync {
    fn market(&self) -> &MarketRef {
        self.book().market()
    }

    fn reset(&mut self) {
        *self = BinanceBookSync::new(self.book().market().clone());
    }
}

impl DisconnectMarket for GenericBookSync {
    fn market(&self) -> &MarketRef {
        self.book().market()
    }

    fn reset(&mut self) {
        *self = GenericBookSync::new(self.book().market().clone());
    }
}

fn split_batches<T>(items: Vec<T>, batch_size: usize) -> Vec<Vec<T>> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    for item in items {
        current.push(item);
        if current.len() >= batch_size.max(1) {
            batches.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

#[derive(Clone, Debug)]
struct StoreWriterHandle {
    tx: mpsc::Sender<StoreCommand>,
}

impl StoreWriterHandle {
    fn spawn<S>(store: Arc<S>) -> Self
    where
        S: BookStore + 'static,
    {
        let (tx, mut rx) = mpsc::channel::<StoreCommand>(1024);
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    StoreCommand::OpenEpoch {
                        run_id,
                        market,
                        epoch_seq,
                        started_at_ms,
                        reply,
                    } => {
                        let _ = reply.send(
                            store
                                .open_epoch(run_id, &market, epoch_seq, started_at_ms)
                                .await,
                        );
                    }
                    StoreCommand::CloseEpoch {
                        epoch_id,
                        ended_at_ms,
                        reason,
                        reply,
                    } => {
                        let _ = reply.send(store.close_epoch(epoch_id, ended_at_ms, &reason).await);
                    }
                    StoreCommand::CommitBatch { batch, reply } => {
                        let _ = reply.send(store.commit_batch(batch).await);
                    }
                }
            }
        });
        Self { tx }
    }

    async fn open_epoch(
        &self,
        run_id: i64,
        market: &MarketRef,
        epoch_seq: i64,
        started_at_ms: i64,
    ) -> DynResult<i64> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(StoreCommand::OpenEpoch {
                run_id,
                market: market.clone(),
                epoch_seq,
                started_at_ms,
                reply: reply_tx,
            })
            .await
            .map_err(|_| RuntimeError::Other("store writer task stopped".to_string()))?;
        reply_rx
            .await
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(RuntimeError::Other(
                    "store writer dropped open_epoch reply".to_string(),
                ))
            })?
    }

    async fn close_epoch(&self, epoch_id: i64, ended_at_ms: i64, reason: &str) -> DynResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(StoreCommand::CloseEpoch {
                epoch_id,
                ended_at_ms,
                reason: reason.to_string(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| RuntimeError::Other("store writer task stopped".to_string()))?;
        reply_rx
            .await
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(RuntimeError::Other(
                    "store writer dropped close_epoch reply".to_string(),
                ))
            })?
    }

    async fn commit_batch(&self, batch: CommitBatch) -> DynResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(StoreCommand::CommitBatch {
                batch,
                reply: reply_tx,
            })
            .await
            .map_err(|_| RuntimeError::Other("store writer task stopped".to_string()))?;
        reply_rx
            .await
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(RuntimeError::Other(
                    "store writer dropped commit reply".to_string(),
                ))
            })?
    }
}

enum StoreCommand {
    OpenEpoch {
        run_id: i64,
        market: MarketRef,
        epoch_seq: i64,
        started_at_ms: i64,
        reply: oneshot::Sender<DynResult<i64>>,
    },
    CloseEpoch {
        epoch_id: i64,
        ended_at_ms: i64,
        reason: String,
        reply: oneshot::Sender<DynResult<()>>,
    },
    CommitBatch {
        batch: CommitBatch,
        reply: oneshot::Sender<DynResult<()>>,
    },
}

#[derive(Clone, Debug, Default)]
pub struct TokioClock;

#[async_trait]
impl Clock for TokioClock {
    fn now_ms(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        duration.as_millis() as i64
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

#[derive(Clone, Debug, Default)]
pub struct ReqwestRestClient {
    client: Client,
}

impl ReqwestRestClient {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .http1_only()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(20))
                .pool_idle_timeout(Duration::from_secs(30))
                .tcp_keepalive(Duration::from_secs(30))
                .user_agent("tokenresearch/0.1")
                .build()
                .expect("reqwest client should build"),
        }
    }
}

#[async_trait]
impl RestClient for ReqwestRestClient {
    async fn get_text(&self, url: &str) -> DynResult<String> {
        Ok(self
            .client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?)
    }

    async fn post_json_text(&self, url: &str, body: &Value) -> DynResult<String> {
        Ok(self
            .client
            .post(url)
            .json(body)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?)
    }
}

#[derive(Clone, Debug, Default)]
pub struct TokioWsClient;

pub struct TokioWsConnection {
    sink: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tokio_tungstenite::tungstenite::Message,
    >,
    stream: futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
}

#[async_trait]
impl WsClient for TokioWsClient {
    async fn connect(&self, url: &str) -> DynResult<Box<dyn WsConnection>> {
        let (stream, _) = tokio_tungstenite::connect_async(url).await?;
        let (sink, stream) = futures_util::StreamExt::split(stream);
        Ok(Box::new(TokioWsConnection { sink, stream }))
    }
}

#[async_trait]
impl WsConnection for TokioWsConnection {
    async fn send_text(&mut self, text: String) -> DynResult<()> {
        use futures_util::SinkExt;
        self.sink
            .send(tokio_tungstenite::tungstenite::Message::Text(text.into()))
            .await?;
        Ok(())
    }

    async fn next_text(&mut self) -> DynResult<Option<String>> {
        use futures_util::StreamExt;
        while let Some(message) = self.stream.next().await {
            let message = message?;
            if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                return Ok(Some(text.to_string()));
            }
        }
        Ok(None)
    }
}
