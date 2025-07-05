use crate::airlock::types::NetworkMode;
use log::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{interval, Duration};

#[derive(Debug)]
pub struct ChainMonitor {
    pub validator_slot: Arc<AtomicU64>,
    pub network_slot: Arc<AtomicU64>,
    network_mode: NetworkMode,
}

impl ChainMonitor {
    pub fn new(network_mode: NetworkMode) -> Self {
        Self {
            validator_slot: Arc::new(AtomicU64::new(0)),
            network_slot: Arc::new(AtomicU64::new(0)),
            network_mode,
        }
    }

    pub fn update_validator_slot(&self, slot: u64) {
        self.validator_slot.store(slot, Ordering::Relaxed);
    }

    pub async fn start_network_monitoring(self: Arc<Self>) {
        let rpc_url = self.network_mode.rpc_endpoint();

        info!(
            "Starting network monitoring for {:?} at {}",
            self.network_mode, rpc_url
        );

        let mut interval = interval(Duration::from_secs(5));
        let client = reqwest::Client::new();

        loop {
            interval.tick().await;

            match self.fetch_network_slot(&client, rpc_url).await {
                Ok(slot) => {
                    self.network_slot.store(slot, Ordering::Relaxed);
                    debug!("Network slot updated: {}", slot);
                }
                Err(e) => {
                    warn!("Failed to fetch network slot: {}", e);
                }
            }
        }
    }

    async fn fetch_network_slot(
        &self,
        client: &reqwest::Client,
        rpc_url: &str,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": []
        });

        let response = client
            .post(rpc_url)
            .json(&request)
            .timeout(Duration::from_secs(3))
            .send()
            .await?;

        let json: serde_json::Value = response.json().await?;

        let slot = json["result"].as_u64().ok_or("Invalid slot response")?;

        Ok(slot)
    }
}
