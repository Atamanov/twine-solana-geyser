use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;

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

/// Prometheus metrics collector
pub struct MetricsCollector {
    airlock: Arc<AirLock>,
    
    // Counters
    slots_created: IntCounterVec,
    slots_completed: IntCounterVec,
    slots_dropped: IntCounterVec,
    account_changes: IntCounterVec,
    vote_transactions: IntCounterVec,
    cache_hits: IntCounterVec,
    cache_misses: IntCounterVec,
    buffer_allocations: IntCounterVec,
    
    // Gauges
    active_slots: IntGaugeVec,
    ready_slots: IntGaugeVec,
    active_threads: IntGaugeVec,
    memory_usage: IntGaugeVec,
    
    // Histograms
    write_latency: HistogramVec,
    aggregate_latency: HistogramVec,
}

impl MetricsCollector {
    pub fn new(airlock: Arc<AirLock>) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            airlock,
            
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
            
            account_changes: register_int_counter_vec!(
                "geyser_account_changes_total",
                "Total number of account changes buffered",
                &["type"]
            )?,
            
            vote_transactions: register_int_counter_vec!(
                "geyser_vote_transactions_total",
                "Total number of vote transactions buffered",
                &["type"]
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
            
            // Gauges
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
            
            active_threads: register_int_gauge_vec!(
                "geyser_active_threads",
                "Number of active threads with buffers",
                &["pool"]
            )?,
            
            memory_usage: register_int_gauge_vec!(
                "geyser_memory_usage_bytes",
                "Estimated memory usage in bytes",
                &["component"]
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
        self.active_slots
            .with_label_values(&["tracking"])
            .set(stats.active_slots as i64);
            
        self.ready_slots
            .with_label_values(&["pending"])
            .set(stats.ready_slots as i64);
            
        self.active_threads
            .with_label_values(&["geyser"])
            .set(metrics.active_threads.load(Ordering::Relaxed) as i64);
        
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
        
        // Estimate memory usage
        let estimated_memory = stats.active_slots * 1024 // Rough estimate per slot
            + (stats.total_account_changes as usize) * 512; // Rough estimate per change
        self.memory_usage
            .with_label_values(&["buffers"])
            .set(estimated_memory as i64);
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
) -> Result<(), Box<dyn std::error::Error>> {
    let collector = Arc::new(MetricsCollector::new(airlock)?);
    
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