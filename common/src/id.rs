use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use chrono::Local;

pub fn next_id() -> u64 {
    Local::now().timestamp_millis() as u64
}

#[derive(Clone)]
pub struct SerialId(Arc<AtomicUsize>);

impl Default for SerialId {
    fn default() -> Self {
        SerialId(Arc::new(AtomicUsize::new(0)))
    }
}

impl SerialId {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) as u64
    }
}
