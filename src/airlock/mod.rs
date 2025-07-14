pub mod pool;
pub mod types;
pub mod optimized_types;

use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct AirlockStats {
    // Account and slot tracking
    pub total_updates: AtomicUsize,
    pub sealed_slots: AtomicUsize,
    pub active_slots: AtomicUsize,
    pub monitored_account_changes: AtomicUsize,
    pub slots_with_monitored_accounts: AtomicUsize,
    pub proof_requests_generated: AtomicUsize,
    pub db_writes: AtomicUsize,
    pub queue_depth: AtomicUsize,
    pub queue_capacity: AtomicUsize,
    pub worker_pool_size: AtomicUsize,
    pub queue_throughput: AtomicUsize,
    // DB metrics
    pub db_batch_success_count: AtomicUsize,
    pub db_batch_error_count: AtomicUsize,
    pub last_db_batch_slot: AtomicUsize,
    pub last_db_batch_timestamp: AtomicUsize,
    // Geyser plugin internal metrics
    pub airlock_pending_slots: AtomicUsize,
    pub airlock_missing_data_slots: AtomicUsize,
    pub slot_status_updates: AtomicUsize,
    pub block_metadata_received: AtomicUsize,
    // Memory tracking
    pub total_memory_usage: AtomicUsize,
    pub peak_memory_usage: AtomicUsize,
    pub slots_in_memory: AtomicUsize,
    // Vote tracking
    pub vote_transactions_total: AtomicUsize,
    pub tower_sync_count: AtomicUsize,
    pub compact_vote_count: AtomicUsize,
    pub vote_switch_count: AtomicUsize,
    pub other_vote_count: AtomicUsize,
    // Memory management
    pub slots_deleted_total: AtomicUsize,
    pub stakes_saved_total: AtomicUsize,
    pub vote_buffer_memory_bytes: AtomicUsize,
    // New fields for optimized implementation
    pub slots_created: AtomicUsize,
    pub slots_flushed: AtomicUsize,
    pub slots_cleaned: AtomicUsize,
    pub db_writes_queued: AtomicUsize,
    pub db_writes_dropped: AtomicUsize,
    pub memory_pressure_events: AtomicUsize,
}

impl Default for AirlockStats {
    fn default() -> Self {
        Self {
            total_updates: AtomicUsize::new(0),
            sealed_slots: AtomicUsize::new(0),
            active_slots: AtomicUsize::new(0),
            monitored_account_changes: AtomicUsize::new(0),
            slots_with_monitored_accounts: AtomicUsize::new(0),
            proof_requests_generated: AtomicUsize::new(0),
            db_writes: AtomicUsize::new(0),
            queue_depth: AtomicUsize::new(0),
            queue_capacity: AtomicUsize::new(0),
            worker_pool_size: AtomicUsize::new(0),
            queue_throughput: AtomicUsize::new(0),
            db_batch_success_count: AtomicUsize::new(0),
            db_batch_error_count: AtomicUsize::new(0),
            last_db_batch_slot: AtomicUsize::new(0),
            last_db_batch_timestamp: AtomicUsize::new(0),
            airlock_pending_slots: AtomicUsize::new(0),
            airlock_missing_data_slots: AtomicUsize::new(0),
            slot_status_updates: AtomicUsize::new(0),
            block_metadata_received: AtomicUsize::new(0),
            total_memory_usage: AtomicUsize::new(0),
            peak_memory_usage: AtomicUsize::new(0),
            slots_in_memory: AtomicUsize::new(0),
            vote_transactions_total: AtomicUsize::new(0),
            tower_sync_count: AtomicUsize::new(0),
            compact_vote_count: AtomicUsize::new(0),
            vote_switch_count: AtomicUsize::new(0),
            other_vote_count: AtomicUsize::new(0),
            slots_deleted_total: AtomicUsize::new(0),
            stakes_saved_total: AtomicUsize::new(0),
            vote_buffer_memory_bytes: AtomicUsize::new(0),
            slots_created: AtomicUsize::new(0),
            slots_flushed: AtomicUsize::new(0),
            slots_cleaned: AtomicUsize::new(0),
            db_writes_queued: AtomicUsize::new(0),
            db_writes_dropped: AtomicUsize::new(0),
            memory_pressure_events: AtomicUsize::new(0),
        }
    }
}

impl AirlockStats {
    pub fn snapshot(&self) -> AirlockStatsSnapshot {
        AirlockStatsSnapshot {
            total_updates: self.total_updates.load(Ordering::Relaxed),
            sealed_slots: self.sealed_slots.load(Ordering::Relaxed),
            active_slots: self.active_slots.load(Ordering::Relaxed),
            monitored_accounts: 0, // Will be set by caller
            monitored_account_changes: self.monitored_account_changes.load(Ordering::Relaxed),
            slots_with_monitored_accounts: self
                .slots_with_monitored_accounts
                .load(Ordering::Relaxed),
            proof_requests_generated: self.proof_requests_generated.load(Ordering::Relaxed),
            db_writes: self.db_writes.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_capacity: self.queue_capacity.load(Ordering::Relaxed),
            worker_pool_size: self.worker_pool_size.load(Ordering::Relaxed),
            queue_throughput: self.queue_throughput.load(Ordering::Relaxed),
            db_batch_success_count: self.db_batch_success_count.load(Ordering::Relaxed),
            db_batch_error_count: self.db_batch_error_count.load(Ordering::Relaxed),
            last_db_batch_slot: self.last_db_batch_slot.load(Ordering::Relaxed),
            last_db_batch_timestamp: self.last_db_batch_timestamp.load(Ordering::Relaxed),
            airlock_pending_slots: self.airlock_pending_slots.load(Ordering::Relaxed),
            airlock_missing_data_slots: self.airlock_missing_data_slots.load(Ordering::Relaxed),
            slot_status_updates: self.slot_status_updates.load(Ordering::Relaxed),
            block_metadata_received: self.block_metadata_received.load(Ordering::Relaxed),
            total_memory_usage: self.total_memory_usage.load(Ordering::Relaxed),
            peak_memory_usage: self.peak_memory_usage.load(Ordering::Relaxed),
            slots_in_memory: self.slots_in_memory.load(Ordering::Relaxed),
            vote_transactions_total: self.vote_transactions_total.load(Ordering::Relaxed),
            tower_sync_count: self.tower_sync_count.load(Ordering::Relaxed),
            compact_vote_count: self.compact_vote_count.load(Ordering::Relaxed),
            vote_switch_count: self.vote_switch_count.load(Ordering::Relaxed),
            other_vote_count: self.other_vote_count.load(Ordering::Relaxed),
            slots_deleted_total: self.slots_deleted_total.load(Ordering::Relaxed),
            stakes_saved_total: self.stakes_saved_total.load(Ordering::Relaxed),
            vote_buffer_memory_bytes: self.vote_buffer_memory_bytes.load(Ordering::Relaxed),
            slots_created: self.slots_created.load(Ordering::Relaxed),
            slots_flushed: self.slots_flushed.load(Ordering::Relaxed),
            slots_cleaned: self.slots_cleaned.load(Ordering::Relaxed),
            db_writes_queued: self.db_writes_queued.load(Ordering::Relaxed),
            db_writes_dropped: self.db_writes_dropped.load(Ordering::Relaxed),
            memory_pressure_events: self.memory_pressure_events.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AirlockStatsSnapshot {
    pub total_updates: usize,
    pub sealed_slots: usize,
    pub active_slots: usize,
    pub monitored_accounts: usize,
    pub monitored_account_changes: usize,
    pub slots_with_monitored_accounts: usize,
    pub proof_requests_generated: usize,
    pub db_writes: usize,
    pub queue_depth: usize,
    pub queue_capacity: usize,
    pub worker_pool_size: usize,
    pub queue_throughput: usize,
    pub db_batch_success_count: usize,
    pub db_batch_error_count: usize,
    pub last_db_batch_slot: usize,
    pub last_db_batch_timestamp: usize,
    pub airlock_pending_slots: usize,
    pub airlock_missing_data_slots: usize,
    pub slot_status_updates: usize,
    pub block_metadata_received: usize,
    pub total_memory_usage: usize,
    pub peak_memory_usage: usize,
    pub slots_in_memory: usize,
    pub vote_transactions_total: usize,
    pub tower_sync_count: usize,
    pub compact_vote_count: usize,
    pub vote_switch_count: usize,
    pub other_vote_count: usize,
    pub slots_deleted_total: usize,
    pub stakes_saved_total: usize,
    pub vote_buffer_memory_bytes: usize,
    // New fields
    pub slots_created: usize,
    pub slots_flushed: usize,
    pub slots_cleaned: usize,
    pub db_writes_queued: usize,
    pub db_writes_dropped: usize,
    pub memory_pressure_events: usize,
}