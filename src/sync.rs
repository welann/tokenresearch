use crate::book::OrderBook;
use crate::model::{
    BookView, CollectorCheckpoint, EventKind, GapWindow, MarketRef, NormalizedBookEvent,
};

#[derive(Clone, Debug)]
pub struct SyncOutcome {
    pub accepted_events: Vec<NormalizedBookEvent>,
    pub latest_book: Option<BookView>,
    pub checkpoint: Option<CollectorCheckpoint>,
    pub gap: Option<GapWindow>,
    pub needs_resync: bool,
    pub epoch_seq: i64,
}

impl SyncOutcome {
    fn empty(epoch_seq: i64) -> Self {
        Self {
            accepted_events: Vec::new(),
            latest_book: None,
            checkpoint: None,
            gap: None,
            needs_resync: false,
            epoch_seq,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BinanceBookSync {
    market: MarketRef,
    book: OrderBook,
    buffered_deltas: Vec<NormalizedBookEvent>,
    snapshot_loaded: bool,
    last_sequence_end: Option<u64>,
    epoch_seq: i64,
}

impl BinanceBookSync {
    pub fn new(market: MarketRef) -> Self {
        Self {
            book: OrderBook::new(market.clone()),
            market,
            buffered_deltas: Vec::new(),
            snapshot_loaded: false,
            last_sequence_end: None,
            epoch_seq: 0,
        }
    }

    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    pub fn on_delta(&mut self, event: NormalizedBookEvent) -> SyncOutcome {
        let mut outcome = SyncOutcome::empty(self.epoch_seq);
        if !self.snapshot_loaded {
            self.buffered_deltas.push(event);
            outcome.needs_resync = true;
            return outcome;
        }

        let Some(sequence) = &event.sequence else {
            return self.force_gap(event.received_ts_ms, "missing sequence fields");
        };

        if let Some(previous_end) = sequence.previous_end {
            if Some(previous_end) != self.last_sequence_end {
                return self.force_gap(event.received_ts_ms, "binance sequence discontinuity");
            }
        } else if let Some(last) = self.last_sequence_end {
            if sequence.start > last + 1 {
                return self.force_gap(event.received_ts_ms, "binance sequence gap");
            }
        }

        self.book.apply_event(&event);
        self.last_sequence_end = Some(sequence.end);
        outcome.accepted_events.push(event.clone());
        outcome.latest_book = Some(self.book.view(50));
        outcome.checkpoint = Some(CollectorCheckpoint {
            market: self.market.clone(),
            epoch_id: self.epoch_seq,
            last_sequence_end: self.last_sequence_end,
            last_exchange_ts_ms: event.exchange_ts_ms,
            last_snapshot_at_ms: None,
            updated_at_ms: event.received_ts_ms,
            status: "live".to_string(),
        });
        outcome
    }

    pub fn on_snapshot(&mut self, snapshot: NormalizedBookEvent) -> SyncOutcome {
        let mut outcome = SyncOutcome::empty(self.epoch_seq);
        let Some(snapshot_sequence) = snapshot.sequence.as_ref() else {
            return self.force_gap(snapshot.received_ts_ms, "missing snapshot sequence");
        };

        self.snapshot_loaded = true;
        self.epoch_seq += 1;
        self.book.apply_event(&snapshot);
        self.last_sequence_end = Some(snapshot_sequence.end);
        outcome.epoch_seq = self.epoch_seq;
        outcome.accepted_events.push(snapshot.clone());

        let mut replayable = Vec::new();
        let buffered_deltas = std::mem::take(&mut self.buffered_deltas);
        for event in buffered_deltas {
            if let Some(sequence) = &event.sequence {
                if sequence.end < snapshot_sequence.end {
                    continue;
                }

                if sequence.start <= snapshot_sequence.end + 1
                    && snapshot_sequence.end + 1 <= sequence.end
                {
                    self.book.apply_event(&event);
                    self.last_sequence_end = Some(sequence.end);
                    replayable.push(event);
                } else if sequence.start > snapshot_sequence.end + 1 {
                    return self
                        .force_gap(event.received_ts_ms, "snapshot cannot bridge binance delta");
                }
            }
        }

        outcome.accepted_events.extend(replayable);
        outcome.latest_book = Some(self.book.view(50));
        outcome.checkpoint = Some(CollectorCheckpoint {
            market: self.market.clone(),
            epoch_id: self.epoch_seq,
            last_sequence_end: self.last_sequence_end,
            last_exchange_ts_ms: snapshot.exchange_ts_ms,
            last_snapshot_at_ms: Some(snapshot.received_ts_ms),
            updated_at_ms: snapshot.received_ts_ms,
            status: "live".to_string(),
        });
        outcome.needs_resync = false;
        outcome
    }

    fn force_gap(&mut self, now_ms: i64, reason: &str) -> SyncOutcome {
        self.snapshot_loaded = false;
        self.buffered_deltas.clear();
        let gap = GapWindow {
            market: self.market.clone(),
            epoch_id: Some(self.epoch_seq),
            started_at_ms: now_ms,
            ended_at_ms: now_ms,
            expected_sequence: self.last_sequence_end.map(|sequence| sequence + 1),
            observed_sequence: None,
            reason: reason.to_string(),
        };
        self.book = OrderBook::new(self.market.clone());
        self.last_sequence_end = None;
        self.epoch_seq += 1;

        SyncOutcome {
            accepted_events: Vec::new(),
            latest_book: None,
            checkpoint: Some(CollectorCheckpoint {
                market: self.market.clone(),
                epoch_id: self.epoch_seq,
                last_sequence_end: None,
                last_exchange_ts_ms: None,
                last_snapshot_at_ms: None,
                updated_at_ms: now_ms,
                status: "resync_required".to_string(),
            }),
            gap: Some(gap),
            needs_resync: true,
            epoch_seq: self.epoch_seq,
        }
    }
}

#[derive(Clone, Debug)]
pub struct GenericBookSync {
    market: MarketRef,
    book: OrderBook,
    last_sequence_end: Option<u64>,
    initialized: bool,
    epoch_seq: i64,
}

impl GenericBookSync {
    pub fn new(market: MarketRef) -> Self {
        Self {
            book: OrderBook::new(market.clone()),
            market,
            last_sequence_end: None,
            initialized: false,
            epoch_seq: 1,
        }
    }

    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    pub fn apply(&mut self, event: NormalizedBookEvent) -> SyncOutcome {
        let mut outcome = SyncOutcome::empty(self.epoch_seq);
        match event.kind {
            EventKind::Heartbeat => {
                outcome.accepted_events.push(event);
                return outcome;
            }
            EventKind::Image | EventKind::Snapshot => {
                self.book.apply_event(&event);
                self.last_sequence_end = event.sequence.as_ref().map(|sequence| sequence.end);
                self.initialized = true;
            }
            EventKind::Delta => {
                if let Some(sequence) = &event.sequence {
                    if let Some(last) = self.last_sequence_end {
                        if let Some(previous_end) = sequence.previous_end {
                            if previous_end != last {
                                return self.gap(
                                    sequence.start,
                                    event.received_ts_ms,
                                    "delta discontinuity",
                                );
                            }
                            if sequence.start > last + 1 {
                                return self.gap(sequence.start, event.received_ts_ms, "delta gap");
                            }
                        } else if sequence.start > last + 1 {
                            return self.gap(sequence.start, event.received_ts_ms, "delta gap");
                        }
                    }
                    self.last_sequence_end = Some(sequence.end);
                }
                self.book.apply_event(&event);
                self.initialized = true;
            }
            EventKind::Gap => {
                return self.gap(None, event.received_ts_ms, "explicit gap event");
            }
        }

        outcome.latest_book = if self.initialized {
            Some(self.book.view(50))
        } else {
            None
        };
        outcome.checkpoint = Some(CollectorCheckpoint {
            market: self.market.clone(),
            epoch_id: self.epoch_seq,
            last_sequence_end: self.last_sequence_end,
            last_exchange_ts_ms: event.exchange_ts_ms,
            last_snapshot_at_ms: matches!(event.kind, EventKind::Image | EventKind::Snapshot)
                .then_some(event.received_ts_ms),
            updated_at_ms: event.received_ts_ms,
            status: "live".to_string(),
        });
        outcome.accepted_events.push(event);
        outcome
    }

    fn gap(
        &mut self,
        observed_sequence: impl Into<Option<u64>>,
        now_ms: i64,
        reason: &str,
    ) -> SyncOutcome {
        let gap = GapWindow {
            market: self.market.clone(),
            epoch_id: Some(self.epoch_seq),
            started_at_ms: now_ms,
            ended_at_ms: now_ms,
            expected_sequence: self.last_sequence_end.map(|sequence| sequence + 1),
            observed_sequence: observed_sequence.into(),
            reason: reason.to_string(),
        };

        self.epoch_seq += 1;
        self.last_sequence_end = None;
        self.initialized = false;
        self.book = OrderBook::new(self.market.clone());

        SyncOutcome {
            accepted_events: Vec::new(),
            latest_book: None,
            checkpoint: Some(CollectorCheckpoint {
                market: self.market.clone(),
                epoch_id: self.epoch_seq,
                last_sequence_end: None,
                last_exchange_ts_ms: None,
                last_snapshot_at_ms: None,
                updated_at_ms: now_ms,
                status: "resync_required".to_string(),
            }),
            gap: Some(gap),
            needs_resync: true,
            epoch_seq: self.epoch_seq,
        }
    }
}
