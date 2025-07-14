use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::interval;
use serde::{Deserialize, Serialize};

/// RPC poller for network slot updates
pub struct RpcPoller {
    rpc_url: String,
    client: reqwest::Client,
    shutdown: Arc<AtomicBool>,
}

#[derive(Serialize)]
struct RpcRequest {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    jsonrpc: String,
    result: Option<T>,
    error: Option<RpcError>,
    id: u64,
}

#[derive(Deserialize)]
struct RpcError {
    code: i32,
    message: String,
}

impl RpcPoller {
    pub fn new(rpc_url: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to create HTTP client");
            
        Self {
            rpc_url: rpc_url.to_string(),
            client,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
    
    /// Start polling for network slot updates
    pub async fn start_polling(&self) {
        let mut ticker = interval(Duration::from_secs(1)); // Poll every second
        
        log::info!("Starting RPC poller for network slot updates from: {}", self.rpc_url);
        
        while !self.shutdown.load(Ordering::Relaxed) {
            ticker.tick().await;
            
            match self.fetch_network_slot().await {
                Ok(slot) => {
                    crate::metrics::update_network_slot(slot);
                }
                Err(e) => {
                    log::warn!("Failed to fetch network slot: {}", e);
                }
            }
        }
        
        log::info!("RPC poller stopped");
    }
    
    /// Fetch current network slot from RPC
    async fn fetch_network_slot(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let request = RpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "getSlot",
            params: vec![serde_json::json!({
                "commitment": "processed"
            })],
        };
        
        let response = self.client
            .post(&self.rpc_url)
            .json(&request)
            .send()
            .await?;
            
        let rpc_response: RpcResponse<u64> = response.json().await?;
        
        if let Some(error) = rpc_response.error {
            return Err(format!("RPC error {}: {}", error.code, error.message).into());
        }
        
        rpc_response.result
            .ok_or_else(|| "No result in RPC response".into())
    }
    
    /// Stop the poller
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}