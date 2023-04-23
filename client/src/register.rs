use std::sync::Arc;

use tokio::sync::RwLock;

use crate::consumer::SubscribeMessage;

#[derive(Clone)]
pub struct Registers {
    producers: Arc<RwLock<Vec<(u64, String)>>>,
    consumers: Arc<RwLock<Vec<(u64, SubscribeMessage)>>>,
}

impl Registers {
    pub fn new() -> Self {
        Self {
            producers: Arc::new(RwLock::new(Vec::new())),
            consumers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn add_producer(&self, id: u64, topic: &str) {
        let mut registers = self.producers.write().await;
        registers.push((id, topic.to_string()));
    }

    pub async fn add_consumer(&self, id: u64, sub: SubscribeMessage) {
        let mut registers = self.consumers.write().await;
        registers.push((id, sub));
    }

    pub async fn producers(&self) -> Vec<(u64, String)> {
        let registers = self.producers.read().await;
        let mut producers = Vec::with_capacity(registers.len());
        for re in registers.iter() {
            producers.push((re.0, re.1.clone()));
        }
        producers
    }

    pub async fn consumers(&self) -> Vec<(u64, SubscribeMessage)> {
        let registers = self.consumers.read().await;
        let mut consumers = Vec::with_capacity(registers.len());
        for re in registers.iter() {
            consumers.push((re.0, re.1.clone()));
        }
        consumers
    }
}
