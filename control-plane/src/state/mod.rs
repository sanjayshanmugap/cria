use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestRecord {
    pub request_id: String,
    pub status: RequestStatusInternal,
    pub emitted_tokens: u32,
    pub worker_id: Option<String>,
    pub error_message: Option<String>,
    pub cancellation_reason: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl RequestRecord {
    pub fn queued(request_id: String) -> Self {
        let now = now_ms();
        Self {
            request_id,
            status: RequestStatusInternal::Queued,
            emitted_tokens: 0,
            worker_id: None,
            error_message: None,
            cancellation_reason: None,
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            RequestStatusInternal::Completed
                | RequestStatusInternal::Failed
                | RequestStatusInternal::Cancelled
        )
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RequestStatusInternal {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Default)]
pub struct RequestState {
    inner: Arc<DashMap<String, RequestRecord>>,
}

impl RequestState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, record: RequestRecord) {
        self.inner.insert(record.request_id.clone(), record);
    }

    pub fn get(&self, request_id: &str) -> Option<RequestRecord> {
        self.inner.get(request_id).map(|entry| entry.clone())
    }

    pub fn active_count(&self) -> usize {
        self.inner
            .iter()
            .filter(|entry| !entry.is_terminal())
            .count()
    }

    pub fn mark_running(&self, request_id: &str, worker_id: String) {
        self.update(request_id, |record| {
            record.status = RequestStatusInternal::Running;
            record.worker_id = Some(worker_id);
        });
    }

    pub fn note_token(&self, request_id: &str, sequence_number: u32, worker_id: String) {
        self.update(request_id, |record| {
            record.status = RequestStatusInternal::Running;
            record.emitted_tokens = record.emitted_tokens.max(sequence_number);
            record.worker_id = Some(worker_id);
        });
    }

    pub fn cancel(&self, request_id: &str, reason: String) -> bool {
        let Some(mut entry) = self.inner.get_mut(request_id) else {
            return false;
        };
        if entry.is_terminal() {
            return false;
        }
        entry.status = RequestStatusInternal::Cancelled;
        entry.cancellation_reason = Some(reason);
        entry.updated_at_ms = now_ms();
        true
    }

    pub fn finish_completed(&self, request_id: &str, sequence_number: u32, worker_id: String) {
        self.update(request_id, |record| {
            record.status = RequestStatusInternal::Completed;
            record.emitted_tokens = record.emitted_tokens.max(sequence_number);
            record.worker_id = Some(worker_id);
        });
    }

    pub fn finish_failed(&self, request_id: &str, error: &str) {
        self.update(request_id, |record| {
            record.status = RequestStatusInternal::Failed;
            record.error_message = Some(error.to_string());
        });
    }

    pub fn finish_cancelled(&self, request_id: &str, reason: String) {
        self.update(request_id, |record| {
            record.status = RequestStatusInternal::Cancelled;
            record.error_message = Some(reason);
        });
    }

    fn update(&self, request_id: &str, update: impl FnOnce(&mut RequestRecord)) {
        if let Some(mut entry) = self.inner.get_mut(request_id) {
            update(&mut entry);
            entry.updated_at_ms = now_ms();
        }
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}
