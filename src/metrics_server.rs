use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use std::sync::Arc;
use tokio::net::TcpListener;

use sysinfo::{Pid, System};

use crate::airlock::AirlockStats;
use crate::chain_monitor::ChainMonitor;
use std::sync::atomic::Ordering;

#[derive(Clone)]
struct MetricsState {
    stats: Arc<AirlockStats>,
    chain_monitor: Arc<ChainMonitor>,
    system: Arc<parking_lot::Mutex<System>>,
    process_start_time: std::time::Instant,
}

pub struct MetricsServer {
    stats: Arc<AirlockStats>,
    chain_monitor: Arc<ChainMonitor>,
    port: u16,
}

impl MetricsServer {
    pub fn new(stats: Arc<AirlockStats>, chain_monitor: Arc<ChainMonitor>, port: u16) -> Self {
        Self {
            stats,
            chain_monitor,
            port,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let state = MetricsState {
            stats: self.stats,
            chain_monitor: self.chain_monitor,
            system: Arc::new(parking_lot::Mutex::new(System::new_all())),
            process_start_time: std::time::Instant::now(),
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

    // Calculate slot deltas
    let validator_network_delta = if network_slot > 0 && validator_slot > 0 {
        network_slot as i64 - validator_slot as i64
    } else {
        0
    };

    let db_validator_delta = if validator_slot > 0 && snapshot.last_db_batch_slot > 0 {
        validator_slot as i64 - snapshot.last_db_batch_slot as i64
    } else {
        0
    };

    // Get memory statistics
    let rusage = get_memory_usage(&state);

    // Calculate rates
    let uptime_seconds = state.process_start_time.elapsed().as_secs_f64();
    let updates_per_second = if uptime_seconds > 0.0 {
        snapshot.total_updates as f64 / uptime_seconds
    } else {
        0.0
    };

    let slots_per_second = if uptime_seconds > 0.0 {
        snapshot.sealed_slots as f64 / uptime_seconds
    } else {
        0.0
    };

    // Calculate queue utilization
    let queue_utilization = if snapshot.queue_capacity > 0 {
        (snapshot.queue_depth as f64 / snapshot.queue_capacity as f64) * 100.0
    } else {
        0.0
    };

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

# HELP twine_geyser_queue_utilization_percent Queue utilization percentage
# TYPE twine_geyser_queue_utilization_percent gauge
twine_geyser_queue_utilization_percent {:.2}

# HELP twine_geyser_worker_pool_size Number of worker threads
# TYPE twine_geyser_worker_pool_size gauge
twine_geyser_worker_pool_size {}

# HELP twine_geyser_validator_slot Current slot being processed by validator
# TYPE twine_geyser_validator_slot gauge
twine_geyser_validator_slot {}

# HELP twine_geyser_network_slot Current slot on the network (RPC tip)
# TYPE twine_geyser_network_slot gauge
twine_geyser_network_slot {}

# HELP twine_geyser_validator_network_slot_delta Delta between network and validator slots
# TYPE twine_geyser_validator_network_slot_delta gauge
twine_geyser_validator_network_slot_delta {}

# HELP twine_geyser_db_validator_slot_delta Delta between validator and last DB slot
# TYPE twine_geyser_db_validator_slot_delta gauge
twine_geyser_db_validator_slot_delta {}

# HELP twine_geyser_memory_rss_bytes Resident set size in bytes (Geyser plugin only)
# TYPE twine_geyser_memory_rss_bytes gauge
twine_geyser_memory_rss_bytes {}

# HELP twine_geyser_memory_virtual_bytes Virtual memory size in bytes (Geyser plugin only)
# TYPE twine_geyser_memory_virtual_bytes gauge
twine_geyser_memory_virtual_bytes {}

# HELP twine_geyser_db_batch_success_count Total successful database batches
# TYPE twine_geyser_db_batch_success_count counter
twine_geyser_db_batch_success_count {}

# HELP twine_geyser_db_batch_error_count Total failed database batches
# TYPE twine_geyser_db_batch_error_count counter
twine_geyser_db_batch_error_count {}

# HELP twine_geyser_last_db_batch_slot Last slot successfully written to the database
# TYPE twine_geyser_last_db_batch_slot gauge
twine_geyser_last_db_batch_slot {}

# HELP twine_geyser_last_db_batch_timestamp Timestamp of the last successful database write
# TYPE twine_geyser_last_db_batch_timestamp gauge
twine_geyser_last_db_batch_timestamp {}

# HELP twine_geyser_uptime_seconds Plugin uptime in seconds
# TYPE twine_geyser_uptime_seconds gauge
twine_geyser_uptime_seconds {:.2}

# HELP twine_geyser_updates_per_second Average updates per second
# TYPE twine_geyser_updates_per_second gauge
twine_geyser_updates_per_second {:.2}

# HELP twine_geyser_slots_per_second Average slots per second
# TYPE twine_geyser_slots_per_second gauge
twine_geyser_slots_per_second {:.2}

# HELP twine_geyser_airlock_pending_slots Number of slots in airlock
# TYPE twine_geyser_airlock_pending_slots gauge
twine_geyser_airlock_pending_slots {}

# HELP twine_geyser_airlock_missing_data_slots Number of slots waiting for complete data
# TYPE twine_geyser_airlock_missing_data_slots gauge
twine_geyser_airlock_missing_data_slots {}

# HELP twine_geyser_slot_status_updates Total slot status updates sent
# TYPE twine_geyser_slot_status_updates counter
twine_geyser_slot_status_updates {}

# HELP twine_geyser_block_metadata_received Total block metadata notifications received
# TYPE twine_geyser_block_metadata_received counter
twine_geyser_block_metadata_received {}
"#,
        snapshot.total_updates,
        snapshot.sealed_slots,
        snapshot.active_slots,
        snapshot.monitored_account_changes,
        snapshot.proof_requests_generated,
        snapshot.db_writes,
        snapshot.queue_depth,
        snapshot.queue_capacity,
        queue_utilization,
        snapshot.worker_pool_size,
        validator_slot,
        network_slot,
        validator_network_delta,
        db_validator_delta,
        rusage.rss_bytes,
        rusage.vsz_bytes,
        snapshot.db_batch_success_count,
        snapshot.db_batch_error_count,
        snapshot.last_db_batch_slot,
        snapshot.last_db_batch_timestamp,
        uptime_seconds,
        updates_per_second,
        slots_per_second,
        snapshot.airlock_pending_slots,
        snapshot.airlock_missing_data_slots,
        snapshot.slot_status_updates,
        snapshot.block_metadata_received,
    );

    (StatusCode::OK, metrics)
}

#[derive(Default)]
struct MemoryUsage {
    rss_bytes: u64,
    vsz_bytes: u64,
}

fn get_memory_usage(state: &MetricsState) -> MemoryUsage {
    let mut sys = state.system.lock();
    // In sysinfo 0.35, refresh_processes takes ProcessesToUpdate and a bool
    use sysinfo::ProcessesToUpdate;
    sys.refresh_processes(ProcessesToUpdate::All, true);

    if let Some(process) = sys.process(Pid::from(std::process::id() as usize)) {
        MemoryUsage {
            rss_bytes: process.memory(),
            vsz_bytes: process.virtual_memory(),
        }
    } else {
        MemoryUsage::default()
    }
}
