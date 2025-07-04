use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::airlock::AirlockStats;
use crate::chain_monitor::ChainMonitor;
use std::sync::atomic::Ordering;

#[derive(Clone)]
struct MetricsState {
    stats: Arc<AirlockStats>,
    chain_monitor: Arc<ChainMonitor>,
}

pub struct MetricsServer {
    stats: Arc<AirlockStats>,
    chain_monitor: Arc<ChainMonitor>,
    port: u16,
}

impl MetricsServer {
    pub fn new(stats: Arc<AirlockStats>, chain_monitor: Arc<ChainMonitor>, port: u16) -> Self {
        Self { stats, chain_monitor, port }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let state = MetricsState {
            stats: self.stats,
            chain_monitor: self.chain_monitor,
        };
        
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .with_state(state);

        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.port)).await?;
        log::info!("Metrics server listening on port {}", self.port);

        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

async fn metrics_handler(State(state): State<MetricsState>) -> impl IntoResponse {
    let snapshot = state.stats.snapshot();
    let validator_slot = state.chain_monitor.validator_slot.load(Ordering::Relaxed);
    let network_slot = state.chain_monitor.network_slot.load(Ordering::Relaxed);
    
    // Get memory statistics
    let rusage = get_memory_usage();
    
    let metrics = format!(
        r#"# HELP twine_geyser_total_updates Total number of account updates processed
# TYPE twine_geyser_total_updates counter
twine_geyser_total_updates {}

# HELP twine_geyser_sealed_slots Total number of sealed slots
# TYPE twine_geyser_sealed_slots counter
twine_geyser_sealed_slots {}

# HELP twine_geyser_active_slots Number of currently active slots
# TYPE twine_geyser_active_slots gauge
twine_geyser_active_slots {}

# HELP twine_geyser_monitored_account_changes Total monitored account changes
# TYPE twine_geyser_monitored_account_changes counter
twine_geyser_monitored_account_changes {}

# HELP twine_geyser_proof_requests_generated Total proof requests generated
# TYPE twine_geyser_proof_requests_generated counter
twine_geyser_proof_requests_generated {}

# HELP twine_geyser_db_writes Total database writes
# TYPE twine_geyser_db_writes counter
twine_geyser_db_writes {}

# HELP twine_geyser_queue_depth Current queue depth
# TYPE twine_geyser_queue_depth gauge
twine_geyser_queue_depth {}

# HELP twine_geyser_queue_capacity Maximum queue capacity
# TYPE twine_geyser_queue_capacity gauge
twine_geyser_queue_capacity {}

# HELP twine_geyser_worker_pool_size Number of worker threads
# TYPE twine_geyser_worker_pool_size gauge
twine_geyser_worker_pool_size {}

# HELP twine_geyser_validator_slot Current slot being processed by validator
# TYPE twine_geyser_validator_slot gauge
twine_geyser_validator_slot {}

# HELP twine_geyser_network_slot Current slot on the network
# TYPE twine_geyser_network_slot gauge
twine_geyser_network_slot {}

# HELP twine_geyser_memory_rss_bytes Resident set size in bytes
# TYPE twine_geyser_memory_rss_bytes gauge
twine_geyser_memory_rss_bytes {}

# HELP twine_geyser_memory_virtual_bytes Virtual memory size in bytes
# TYPE twine_geyser_memory_virtual_bytes gauge
twine_geyser_memory_virtual_bytes {}
"#,
        snapshot.total_updates,
        snapshot.sealed_slots,
        snapshot.active_slots,
        snapshot.monitored_account_changes,
        snapshot.proof_requests_generated,
        snapshot.db_writes,
        snapshot.queue_depth,
        snapshot.queue_capacity,
        snapshot.worker_pool_size,
        validator_slot,
        network_slot,
        rusage.rss_bytes,
        rusage.vsz_bytes,
    );

    (StatusCode::OK, metrics)
}

#[derive(Default)]
struct MemoryUsage {
    rss_bytes: u64,
    vsz_bytes: u64,
}

fn get_memory_usage() -> MemoryUsage {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            let mut usage = MemoryUsage::default();
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<u64>() {
                            usage.rss_bytes = kb * 1024;
                        }
                    }
                } else if line.starts_with("VmSize:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<u64>() {
                            usage.vsz_bytes = kb * 1024;
                        }
                    }
                }
            }
            return usage;
        }
    }
    
    // Fallback for other platforms or if reading fails
    MemoryUsage::default()
}