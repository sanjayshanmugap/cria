use crate::pb::TokenEvent;
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
    events: Arc<DashMap<String, Vec<TokenEvent>>>,
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

    pub fn record_event(&self, event: TokenEvent) {
        self.events
            .entry(event.request_id.clone())
            .or_default()
            .push(event);
    }

    pub fn events(&self, request_id: &str) -> Vec<TokenEvent> {
        self.events
            .get(request_id)
            .map(|entry| entry.clone())
            .unwrap_or_default()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_request_lifecycle() {
        let state = RequestState::new();
        state.insert(RequestRecord::queued("req-1".to_string()));

        assert_eq!(state.active_count(), 1);
        assert_eq!(
            state.get("req-1").expect("record exists").status,
            RequestStatusInternal::Queued
        );

        state.mark_running("req-1", "worker-a".to_string());
        state.note_token("req-1", 2, "worker-a".to_string());
        state.note_token("req-1", 1, "worker-a".to_string());
        let running = state.get("req-1").expect("record exists");
        assert_eq!(running.status, RequestStatusInternal::Running);
        assert_eq!(running.emitted_tokens, 2);
        assert_eq!(running.worker_id.as_deref(), Some("worker-a"));

        state.finish_completed("req-1", 3, "worker-a".to_string());
        let completed = state.get("req-1").expect("record exists");
        assert_eq!(completed.status, RequestStatusInternal::Completed);
        assert_eq!(completed.emitted_tokens, 3);
        assert_eq!(state.active_count(), 0);
    }

    #[test]
    fn cancel_only_non_terminal_requests() {
        let state = RequestState::new();
        state.insert(RequestRecord::queued("req-1".to_string()));

        assert!(state.cancel("req-1", "user abort".to_string()));
        let cancelled = state.get("req-1").expect("record exists");
        assert_eq!(cancelled.status, RequestStatusInternal::Cancelled);
        assert_eq!(cancelled.cancellation_reason.as_deref(), Some("user abort"));
        assert!(!state.cancel("req-1", "again".to_string()));
        assert!(!state.cancel("missing", "no-op".to_string()));
    }
}
