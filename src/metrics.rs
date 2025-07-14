use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::{
    routing::get,
    Router,
    extract::State,
    http::StatusCode,
};
use prometheus::{
    Encoder, TextEncoder,
    register_int_counter_vec, register_int_gauge_vec, register_histogram_vec,
    IntCounterVec, IntGaugeVec, HistogramVec,
};
use tokio::time::interval;

use crate::airlock::AirLock;

/// Global metrics state
pub struct GlobalMetrics {
    pub network_slot: AtomicU64,
    pub validator_slot: AtomicU64,
    pub plugin_start_time: u64,
}

impl GlobalMetrics {
    fn new() -> Self {
        Self {
            network_slot: AtomicU64::new(0),
            validator_slot: AtomicU64::new(0),
            plugin_start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

lazy_static::lazy_static! {
    pub static ref GLOBAL_METRICS: GlobalMetrics = GlobalMetrics::new();
}

/// Prometheus metrics collector
pub struct MetricsCollector {
    airlock: Arc<AirLock>,
    aggregator_threads: usize,
    db_writer_threads: usize,
    
    // Counters
    slots_created: IntCounterVec,
    slots_completed: IntCounterVec,
    slots_dropped: IntCounterVec,
    slots_rooted: IntCounterVec,
    account_changes: IntCounterVec,
    account_changes_written: IntCounterVec,
    vote_transactions: IntCounterVec,
    vote_transactions_written: IntCounterVec,
    cache_hits: IntCounterVec,
    cache_misses: IntCounterVec,
    buffer_allocations: IntCounterVec,
    buffer_evictions: IntCounterVec,
    write_errors: IntCounterVec,
    slot_timeouts: IntCounterVec,
    
    // Gauges
    network_slot: IntGaugeVec,
    validator_slot: IntGaugeVec,
    slot_lag: IntGaugeVec,
    plugin_start_time: IntGaugeVec,
    active_slots: IntGaugeVec,
    ready_slots: IntGaugeVec,
    ready_queue_len: IntGaugeVec,
    buffered_account_changes: IntGaugeVec,
    buffered_vote_transactions: IntGaugeVec,
    active_threads: IntGaugeVec,
    thread_pool_size: IntGaugeVec,
    thread_pool_queued: IntGaugeVec,
    memory_usage: IntGaugeVec,
    memory_airlock_slots: IntGaugeVec,
    memory_airlock_changes: IntGaugeVec,
    memory_airlock_votes: IntGaugeVec,
    memory_thread_local: IntGaugeVec,
    memory_ready_queue: IntGaugeVec,
    buffer_size: IntGaugeVec,
    buffer_capacity: IntGaugeVec,
    buffer_hit_rate: IntGaugeVec,
    oldest_slot_age_ms: IntGaugeVec,
    avg_slot_age_ms: IntGaugeVec,
    last_successful_write: IntGaugeVec,
    
    // Histograms
    write_latency: HistogramVec,
    aggregate_latency: HistogramVec,
    write_batch_size: HistogramVec,
    write_batch_duration: HistogramVec,
}

impl MetricsCollector {
    pub fn new(airlock: Arc<AirLock>, aggregator_threads: usize, db_writer_threads: usize) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            airlock,
            aggregator_threads,
            db_writer_threads,
            
            // Counters
            slots_created: register_int_counter_vec!(
                "geyser_slots_created_total",
                "Total number of slots created",
                &["status"]
            )?,
            
            slots_completed: register_int_counter_vec!(
                "geyser_slots_completed_total",
                "Total number of slots completed and written to DB",
                &["status"]
            )?,
            
            slots_dropped: register_int_counter_vec!(
                "geyser_slots_dropped_total",
                "Total number of slots dropped due to age or cleanup",
                &["reason"]
            )?,
            
            slots_rooted: register_int_counter_vec!(
                "twine_geyser_slots_rooted",
                "Total number of slots that reached rooted status",
                &["type"]
            )?,
            
            account_changes: register_int_counter_vec!(
                "geyser_account_changes_total",
                "Total number of account changes buffered",
                &["type"]
            )?,
            
            account_changes_written: register_int_counter_vec!(
                "twine_geyser_account_changes_written",
                "Total number of account changes written to DB",
                &["status"]
            )?,
            
            vote_transactions: register_int_counter_vec!(
                "geyser_vote_transactions_total",
                "Total number of vote transactions buffered",
                &["type"]
            )?,
            
            vote_transactions_written: register_int_counter_vec!(
                "twine_geyser_vote_transactions_written",
                "Total number of vote transactions written to DB",
                &["status"]
            )?,
            
            cache_hits: register_int_counter_vec!(
                "geyser_cache_hits_total",
                "Total number of hot cache hits",
                &["cache"]
            )?,
            
            cache_misses: register_int_counter_vec!(
                "geyser_cache_misses_total",
                "Total number of hot cache misses",
                &["cache"]
            )?,
            
            buffer_allocations: register_int_counter_vec!(
                "geyser_buffer_allocations_total",
                "Total number of buffer allocations",
                &["type"]
            )?,
            
            buffer_evictions: register_int_counter_vec!(
                "twine_geyser_buffer_evictions_total",
                "Total number of buffer evictions",
                &["reason"]
            )?,
            
            write_errors: register_int_counter_vec!(
                "twine_geyser_write_errors_total",
                "Total write errors by type",
                &["error_type"]
            )?,
            
            slot_timeouts: register_int_counter_vec!(
                "twine_geyser_slot_timeouts_total",
                "Total slots that timed out",
                &["reason"]
            )?,
            
            // Gauges
            network_slot: register_int_gauge_vec!(
                "twine_geyser_network_slot",
                "Current network slot (RPC tip)",
                &["source"]
            )?,
            
            validator_slot: register_int_gauge_vec!(
                "twine_geyser_validator_slot",
                "Current validator slot being processed",
                &["source"]
            )?,
            
            slot_lag: register_int_gauge_vec!(
                "twine_geyser_slot_lag",
                "Difference between network tip and validator slot",
                &["type"]
            )?,
            
            plugin_start_time: register_int_gauge_vec!(
                "twine_geyser_plugin_start_time",
                "Unix timestamp when plugin started",
                &["instance"]
            )?,
            
            active_slots: register_int_gauge_vec!(
                "geyser_active_slots",
                "Number of currently active slots",
                &["state"]
            )?,
            
            ready_slots: register_int_gauge_vec!(
                "geyser_ready_slots",
                "Number of slots ready for aggregation",
                &["queue"]
            )?,
            
            ready_queue_len: register_int_gauge_vec!(
                "twine_geyser_ready_queue_len",
                "Number of slots in ready queue",
                &["priority"]
            )?,
            
            buffered_account_changes: register_int_gauge_vec!(
                "twine_geyser_buffered_account_changes",
                "Current number of buffered account changes",
                &["state"]
            )?,
            
            buffered_vote_transactions: register_int_gauge_vec!(
                "twine_geyser_buffered_vote_transactions",
                "Current number of buffered vote transactions",
                &["state"]
            )?,
            
            active_threads: register_int_gauge_vec!(
                "geyser_active_threads",
                "Number of active threads with buffers",
                &["pool"]
            )?,
            
            thread_pool_size: register_int_gauge_vec!(
                "twine_geyser_thread_pool_size",
                "Configured thread pool size",
                &["pool"]
            )?,
            
            thread_pool_queued: register_int_gauge_vec!(
                "twine_geyser_thread_pool_queued",
                "Tasks queued in thread pool",
                &["pool"]
            )?,
            
            memory_usage: register_int_gauge_vec!(
                "geyser_memory_usage_bytes",
                "Estimated memory usage in bytes",
                &["component"]
            )?,
            
            memory_airlock_slots: register_int_gauge_vec!(
                "twine_geyser_memory_airlock_slots",
                "Memory used by slot tracking",
                &["type"]
            )?,
            
            memory_airlock_changes: register_int_gauge_vec!(
                "twine_geyser_memory_airlock_changes",
                "Memory used by account changes",
                &["type"]
            )?,
            
            memory_airlock_votes: register_int_gauge_vec!(
                "twine_geyser_memory_airlock_votes",
                "Memory used by vote transactions",
                &["type"]
            )?,
            
            memory_thread_local: register_int_gauge_vec!(
                "twine_geyser_memory_thread_local",
                "Memory used by thread-local buffers",
                &["type"]
            )?,
            
            memory_ready_queue: register_int_gauge_vec!(
                "twine_geyser_memory_ready_queue",
                "Memory used by ready queue",
                &["type"]
            )?,
            
            buffer_size: register_int_gauge_vec!(
                "twine_geyser_buffer_size",
                "Current buffer size in bytes",
                &["buffer"]
            )?,
            
            buffer_capacity: register_int_gauge_vec!(
                "twine_geyser_buffer_capacity",
                "Maximum buffer capacity",
                &["buffer"]
            )?,
            
            buffer_hit_rate: register_int_gauge_vec!(
                "twine_geyser_buffer_hit_rate",
                "Buffer hit rate percentage",
                &["buffer"]
            )?,
            
            oldest_slot_age_ms: register_int_gauge_vec!(
                "twine_geyser_oldest_slot_age_ms",
                "Age of oldest active slot in milliseconds",
                &["type"]
            )?,
            
            avg_slot_age_ms: register_int_gauge_vec!(
                "twine_geyser_avg_slot_age_ms",
                "Average age of active slots in milliseconds",
                &["type"]
            )?,
            
            last_successful_write: register_int_gauge_vec!(
                "twine_geyser_last_successful_write",
                "Timestamp of last successful write",
                &["operation"]
            )?,
            
            // Histograms
            write_latency: register_histogram_vec!(
                "geyser_write_latency_microseconds",
                "Latency of write operations in microseconds",
                &["operation"],
                vec![0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0]
            )?,
            
            aggregate_latency: register_histogram_vec!(
                "geyser_aggregate_latency_microseconds",
                "Latency of aggregation operations in microseconds",
                &["operation"],
                vec![100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0]
            )?,
            
            write_batch_size: register_histogram_vec!(
                "twine_geyser_write_batch_size",
                "Size of write batches",
                &["type"],
                vec![1.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0]
            )?,
            
            write_batch_duration: register_histogram_vec!(
                "twine_geyser_write_batch_duration_ms",
                "Time to write a batch in milliseconds",
                &["type"],
                vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0]
            )?,
        })
    }
    
    /// Update all metrics from airlock state
    pub fn update_metrics(&self) {
        let metrics = self.airlock.get_metrics();
        let stats = self.airlock.get_stats();
        
        // Update counters (using absolute values since Prometheus handles rates)
        self.slots_created
            .with_label_values(&["all"])
            .inc_by(metrics.slots_created.load(Ordering::Relaxed));
            
        self.slots_completed
            .with_label_values(&["rooted"])
            .inc_by(metrics.slots_completed.load(Ordering::Relaxed));
            
        self.slots_dropped
            .with_label_values(&["age"])
            .inc_by(metrics.slots_dropped.load(Ordering::Relaxed));
            
        self.account_changes
            .with_label_values(&["buffered"])
            .inc_by(metrics.account_changes_buffered.load(Ordering::Relaxed));
            
        self.vote_transactions
            .with_label_values(&["buffered"])
            .inc_by(metrics.vote_transactions_buffered.load(Ordering::Relaxed));
            
        self.cache_hits
            .with_label_values(&["hot_slots"])
            .inc_by(metrics.hot_cache_hits.load(Ordering::Relaxed));
            
        self.cache_misses
            .with_label_values(&["hot_slots"])
            .inc_by(metrics.hot_cache_misses.load(Ordering::Relaxed));
            
        self.buffer_allocations
            .with_label_values(&["slot"])
            .inc_by(metrics.buffer_allocations.load(Ordering::Relaxed));
        
        // Update gauges
        
        // Slot tracking metrics
        let network_slot = GLOBAL_METRICS.network_slot.load(Ordering::Relaxed);
        let validator_slot = GLOBAL_METRICS.validator_slot.load(Ordering::Relaxed);
        
        self.network_slot
            .with_label_values(&["rpc"])
            .set(network_slot as i64);
            
        self.validator_slot
            .with_label_values(&["local"])
            .set(validator_slot as i64);
            
        self.slot_lag
            .with_label_values(&["current"])
            .set((network_slot.saturating_sub(validator_slot)) as i64);
            
        self.plugin_start_time
            .with_label_values(&["geyser"])
            .set(GLOBAL_METRICS.plugin_start_time as i64);
        
        self.active_slots
            .with_label_values(&["tracking"])
            .set(stats.active_slots as i64);
            
        self.ready_slots
            .with_label_values(&["pending"])
            .set(stats.ready_slots as i64);
            
        self.ready_queue_len
            .with_label_values(&["normal"])
            .set(stats.ready_slots as i64);
            
        self.buffered_account_changes
            .with_label_values(&["pending"])
            .set(stats.total_account_changes as i64);
            
        self.buffered_vote_transactions
            .with_label_values(&["pending"])
            .set(stats.total_vote_transactions as i64);
            
        self.active_threads
            .with_label_values(&["geyser"])
            .set(metrics.active_threads.load(Ordering::Relaxed) as i64);
            
        // Thread pool metrics
        self.thread_pool_size
            .with_label_values(&["aggregators"])
            .set(self.aggregator_threads as i64);
            
        self.thread_pool_size
            .with_label_values(&["db_writers"])
            .set(self.db_writer_threads as i64);
            
        self.thread_pool_queued
            .with_label_values(&["aggregators"])
            .set(0); // TODO: Get from thread pool if we implement queue tracking
        
        // Memory breakdown
        let slot_memory = stats.active_slots * 4096; // ~4KB per slot
        let changes_memory = (stats.total_account_changes as usize) * 1024; // ~1KB per change
        let votes_memory = (stats.total_vote_transactions as usize) * 512; // ~512B per vote
        let thread_memory = metrics.active_threads.load(Ordering::Relaxed) as usize * 65536; // ~64KB per thread
        let queue_memory = stats.ready_slots * 128; // ~128B per ready slot
        
        self.memory_airlock_slots
            .with_label_values(&["active"])
            .set(slot_memory as i64);
            
        self.memory_airlock_changes
            .with_label_values(&["buffered"])
            .set(changes_memory as i64);
            
        self.memory_airlock_votes
            .with_label_values(&["buffered"])
            .set(votes_memory as i64);
            
        self.memory_thread_local
            .with_label_values(&["buffers"])
            .set(thread_memory as i64);
            
        self.memory_ready_queue
            .with_label_values(&["slots"])
            .set(queue_memory as i64);
        
        let total_memory = slot_memory + changes_memory + votes_memory + thread_memory + queue_memory;
        self.memory_usage
            .with_label_values(&["total"])
            .set(total_memory as i64);
            
        // Buffer statistics
        let hits = metrics.hot_cache_hits.load(Ordering::Relaxed) as f64;
        let misses = metrics.hot_cache_misses.load(Ordering::Relaxed) as f64;
        let total_lookups = hits + misses;
        let hit_rate = if total_lookups > 0.0 { (hits / total_lookups * 100.0) as i64 } else { 0 };
        
        self.buffer_hit_rate
            .with_label_values(&["hot_cache"])
            .set(hit_rate);
            
        self.buffer_size
            .with_label_values(&["hot_cache"])
            .set(total_memory as i64);
            
        self.buffer_capacity
            .with_label_values(&["hot_cache"])
            .set((1024 * 1024 * 256) as i64); // 256MB capacity example
            
        // Slot age metrics
        let (oldest_age_ms, avg_age_ms) = self.calculate_slot_ages();
        self.oldest_slot_age_ms
            .with_label_values(&["active"])
            .set(oldest_age_ms as i64);
            
        self.avg_slot_age_ms
            .with_label_values(&["active"])
            .set(avg_age_ms as i64);
            
        // Last write timestamp
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_successful_write
            .with_label_values(&["slot"])
            .set(now as i64);
        
        // Update histograms
        let write_time = metrics.avg_write_time_us.load(Ordering::Relaxed) as f64;
        if write_time > 0.0 {
            self.write_latency
                .with_label_values(&["account_change"])
                .observe(write_time);
        }
        
        let aggregate_time = metrics.avg_aggregate_time_us.load(Ordering::Relaxed) as f64;
        if aggregate_time > 0.0 {
            self.aggregate_latency
                .with_label_values(&["slot"])
                .observe(aggregate_time);
        }
    }
    
    /// Calculate slot ages
    fn calculate_slot_ages(&self) -> (u64, u64) {
        let slot_ages = self.airlock.get_slot_ages_ms();
        
        if slot_ages.is_empty() {
            return (0, 0);
        }
        
        let oldest = *slot_ages.iter().max().unwrap_or(&0);
        let avg = if !slot_ages.is_empty() {
            slot_ages.iter().sum::<u64>() / slot_ages.len() as u64
        } else {
            0
        };
        
        (oldest, avg)
    }
    
    /// Generate Prometheus metrics string
    pub fn render_metrics(&self) -> Result<String, Box<dyn std::error::Error>> {
        // Update metrics before rendering
        self.update_metrics();
        
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        
        Ok(String::from_utf8(buffer)?)
    }
}

/// Spawn metrics server
pub async fn spawn_metrics_server(
    airlock: Arc<AirLock>,
    bind_addr: SocketAddr,
    aggregator_threads: usize,
    db_writer_threads: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let collector = Arc::new(MetricsCollector::new(airlock, aggregator_threads, db_writer_threads)?);
    
    // Spawn background metric updater
    let collector_clone = collector.clone();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(5));
        loop {
            ticker.tick().await;
            collector_clone.update_metrics();
        }
    });
    
    // Create HTTP server
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(collector);
    
    log::info!("Starting metrics server on {}", bind_addr);
    
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
    
    Ok(())
}

async fn metrics_handler(
    State(collector): State<Arc<MetricsCollector>>,
) -> Result<String, StatusCode> {
    collector.render_metrics()
        .map_err(|e| {
            log::error!("Failed to render metrics: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn health_handler() -> &'static str {
    "OK"
}

/// Additional custom metrics that can be exposed
pub struct DetailedMetrics {
    pub cache_hit_rate: f64,
    pub avg_slot_lifetime_ms: u64,
    pub writes_per_second: f64,
    pub slots_per_second: f64,
    pub memory_efficiency: f64,
}

impl DetailedMetrics {
    pub fn calculate(airlock: &AirLock) -> Self {
        let metrics = airlock.get_metrics();
        let _stats = airlock.get_stats();
        
        let hits = metrics.hot_cache_hits.load(Ordering::Relaxed) as f64;
        let misses = metrics.hot_cache_misses.load(Ordering::Relaxed) as f64;
        let total_lookups = hits + misses;
        
        Self {
            cache_hit_rate: if total_lookups > 0.0 { hits / total_lookups } else { 0.0 },
            avg_slot_lifetime_ms: 0, // Would need to track this
            writes_per_second: 0.0, // Would need time tracking
            slots_per_second: 0.0, // Would need time tracking
            memory_efficiency: 0.0, // Would need to calculate
        }
    }
}

/// Helper functions to update global metrics from the plugin
pub fn update_network_slot(slot: u64) {
    GLOBAL_METRICS.network_slot.store(slot, Ordering::Relaxed);
}

pub fn update_validator_slot(slot: u64) {
    GLOBAL_METRICS.validator_slot.store(slot, Ordering::Relaxed);
}

// Lazy static for metric collectors that need to be accessed globally
lazy_static::lazy_static! {
    static ref WRITE_ERRORS: IntCounterVec = register_int_counter_vec!(
        "twine_geyser_write_errors_total",
        "Total write errors by type",
        &["operation", "error_type"]
    ).unwrap();
    
    static ref WRITE_SUCCESS: IntCounterVec = register_int_counter_vec!(
        "twine_geyser_write_success_total",
        "Total successful writes by type",
        &["operation"]
    ).unwrap();
    
    static ref ACCOUNT_CHANGES_WRITTEN: IntCounterVec = register_int_counter_vec!(
        "twine_geyser_account_changes_written",
        "Total account changes written to DB",
        &["status"]
    ).unwrap();
    
    static ref VOTE_TRANSACTIONS_WRITTEN: IntCounterVec = register_int_counter_vec!(
        "twine_geyser_vote_transactions_written",
        "Total vote transactions written to DB",
        &["status"]
    ).unwrap();
    
    static ref SLOTS_ROOTED: IntCounterVec = register_int_counter_vec!(
        "twine_geyser_slots_rooted",
        "Total number of slots that reached rooted status",
        &["type"]
    ).unwrap();
}

/// Increment write error counter
pub fn increment_write_error(operation: &str, error_type: &str) {
    WRITE_ERRORS.with_label_values(&[operation, error_type]).inc();
}

/// Increment write success counter
pub fn increment_write_success(operation: &str) {
    WRITE_SUCCESS.with_label_values(&[operation]).inc();
}

/// Increment account changes written counter
pub fn increment_account_changes_written(count: u64) {
    ACCOUNT_CHANGES_WRITTEN.with_label_values(&["success"]).inc_by(count);
}

/// Increment vote transactions written counter
pub fn increment_vote_transactions_written(count: u64) {
    VOTE_TRANSACTIONS_WRITTEN.with_label_values(&["success"]).inc_by(count);
}

/// Increment slots rooted counter
pub fn increment_slots_rooted() {
    SLOTS_ROOTED.with_label_values(&["finalized"]).inc();
}

// Lazy static for buffer eviction tracking
lazy_static::lazy_static! {
    static ref BUFFER_EVICTIONS: IntCounterVec = register_int_counter_vec!(
        "twine_geyser_buffer_evictions_total",
        "Total number of buffer evictions",
        &["reason"]
    ).unwrap();
}

/// Increment buffer evictions counter
pub fn increment_buffer_evictions(reason: &str, count: u64) {
    BUFFER_EVICTIONS.with_label_values(&[reason]).inc_by(count);
}