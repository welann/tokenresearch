use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::model::{
    BookView, CollectorCheckpoint, GapWindow, MarketRef, NormalizedBookEvent, NormalizedMarket,
    Venue,
};
pub use crate::price_model::PriceCommitBatch;
use crate::price_model::{PriceCheckpoint, PriceKind, PriceMarket};

pub type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type DynResult<T> = Result<T, DynError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub created_at_ms: i64,
    pub depth: usize,
    pub book: BookView,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitBatch {
    pub market: MarketRef,
    pub epoch_id: i64,
    pub events: Vec<NormalizedBookEvent>,
    pub latest_book: Option<BookView>,
    pub snapshot: Option<SnapshotRecord>,
    pub checkpoint: Option<CollectorCheckpoint>,
    pub gaps: Vec<GapWindow>,
}

#[async_trait]
pub trait WsConnection: Send + Sync {
    async fn send_text(&mut self, text: String) -> DynResult<()>;
    async fn next_text(&mut self) -> DynResult<Option<String>>;
}

#[async_trait]
pub trait WsClient: Send + Sync {
    async fn connect(&self, url: &str) -> DynResult<Box<dyn WsConnection>>;
}

#[async_trait]
pub trait RestClient: Send + Sync {
    async fn get_text(&self, url: &str) -> DynResult<String>;
    async fn post_json_text(&self, url: &str, body: &Value) -> DynResult<String>;
}

#[async_trait]
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
    async fn sleep(&self, duration: Duration);
}

#[async_trait]
pub trait BookStore: Send + Sync {
    async fn init(&self) -> DynResult<()>;
    async fn upsert_markets(&self, markets: &[NormalizedMarket]) -> DynResult<()>;
    async fn load_markets(&self, venue: Option<Venue>) -> DynResult<Vec<NormalizedMarket>>;
    async fn start_run(&self, started_at_ms: i64) -> DynResult<i64>;
    async fn open_epoch(
        &self,
        run_id: i64,
        market: &MarketRef,
        epoch_seq: i64,
        started_at_ms: i64,
    ) -> DynResult<i64>;
    async fn close_epoch(&self, epoch_id: i64, ended_at_ms: i64, reason: &str) -> DynResult<()>;
    async fn load_checkpoint(&self, market: &MarketRef) -> DynResult<Option<CollectorCheckpoint>>;
    async fn commit_batch(&self, batch: CommitBatch) -> DynResult<()>;
}

#[async_trait]
pub trait PriceStore: Send + Sync {
    async fn init(&self) -> DynResult<()>;
    async fn upsert_price_markets(&self, markets: &[PriceMarket]) -> DynResult<()>;
    async fn load_price_markets(&self, venue: Option<Venue>) -> DynResult<Vec<PriceMarket>>;
    async fn start_price_run(&self, started_at_ms: i64) -> DynResult<i64>;
    async fn open_price_epoch(
        &self,
        run_id: i64,
        market: &MarketRef,
        kind: PriceKind,
        epoch_seq: i64,
        started_at_ms: i64,
    ) -> DynResult<i64>;
    async fn close_price_epoch(
        &self,
        epoch_id: i64,
        ended_at_ms: i64,
        reason: &str,
    ) -> DynResult<()>;
    async fn load_price_checkpoint(
        &self,
        market: &MarketRef,
        kind: PriceKind,
    ) -> DynResult<Option<PriceCheckpoint>>;
    async fn commit_price_batch(&self, batch: PriceCommitBatch) -> DynResult<()>;
    async fn prune_price_samples_older_than(&self, min_bucket_ts_ms: i64) -> DynResult<u64>;
}
