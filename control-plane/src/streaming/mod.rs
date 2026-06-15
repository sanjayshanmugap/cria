use crate::pb::TokenEvent;
use dashmap::DashMap;
use std::{fmt, sync::Arc};
use tokio::sync::mpsc;
use tonic::Status;

#[derive(Debug)]
pub struct StreamAlreadyRegistered;

impl fmt::Display for StreamAlreadyRegistered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stream already registered")
    }
}

#[derive(Clone, Default)]
pub struct StreamRegistry {
    inner: Arc<DashMap<String, mpsc::Sender<Result<TokenEvent, Status>>>>,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &self,
        request_id: String,
    ) -> Result<mpsc::Receiver<Result<TokenEvent, Status>>, StreamAlreadyRegistered> {
        if self.inner.contains_key(&request_id) {
            return Err(StreamAlreadyRegistered);
        }
        let (sender, receiver) = mpsc::channel(256);
        self.inner.insert(request_id, sender);
        Ok(receiver)
    }

    pub async fn send(
        &self,
        request_id: &str,
        event: Result<TokenEvent, Status>,
    ) -> Result<(), String> {
        let Some(sender) = self.inner.get(request_id).map(|entry| entry.clone()) else {
            return Err("stream not registered".to_string());
        };
        sender.send(event).await.map_err(|err| err.to_string())
    }

    pub fn unregister(&self, request_id: &str) {
        self.inner.remove(request_id);
    }
}
