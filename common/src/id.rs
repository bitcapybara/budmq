use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use chrono::Local;

pub fn next_id() -> u64 {
    Local::now().timestamp_millis() as u64
}

#[derive(Clone)]
pub struct SerialId(Arc<AtomicU64>);

impl Default for SerialId {
    fn default() -> Self {
        SerialId(Arc::new(AtomicU64::new(0)))
    }
}

impl SerialId {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}
