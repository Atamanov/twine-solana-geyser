use crossbeam::queue::SegQueue;
use crossbeam::utils::CachePadded;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use solana_lattice_hash::lt_hash::LtHash;
use solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey};

// Import OwnedAccountChange from geyser interface
#[cfg(feature = "geyser-plugin-enhanced-notifications")]
pub use agave_geyser_plugin_interface::geyser_plugin_interface::OwnedAccountChange;

// Fallback definition when feature is not enabled
#[cfg(not(feature = "geyser-plugin-enhanced-notifications"))]
#[derive(Debug, Clone)]
pub struct OwnedAccountChange {
    pub pubkey: Pubkey,
    pub slot: Slot,
    pub old_account: AccountSharedData,
    pub new_account: AccountSharedData,
    pub old_lthash: LtHash,
    pub new_lthash: LtHash,
}

// Constants for performance tuning
const HOT_SLOTS: usize = 8; // Power of 2 for fast modulo
const SLOT_BUFFER_CAPACITY: usize = 4096;
const VOTE_BUFFER_CAPACITY: usize = 256;
const MAX_SLOT_AGE: Duration = Duration::from_secs(300); // 5 minutes
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// High-performance AirLock optimized for burst writes
pub struct AirLock {
    /// Hot slots - most recently accessed slots
    /// Using CachePadded to prevent false sharing
    hot_slots: Box<[CachePadded<AtomicU64>]>,

    /// Slot data storage
    slots: DashMap<Slot, Arc<SlotData>>,

    /// Per-thread write buffers to eliminate contention
    thread_buffers: DashMap<std::thread::ThreadId, ThreadBuffer>,

    /// Ready queue for slots to be written to DB
    ready_queue: SegQueue<Slot>,

    /// Metrics
    metrics: Metrics,

    /// Last cleanup time
    last_cleanup: Mutex<Instant>,
}

/// Slot data with lock-free statistics
pub struct SlotData {
    pub slot: Slot,

    /// Active writer count
    active_writers: CachePadded<AtomicU32>,

    /// Bank hash components
    bank_hash: RwLock<Option<BankHashComponents>>,
    
    /// Block metadata
    block_metadata: RwLock<Option<BlockMetadata>>,

    /// Status
    status: AtomicU32,

    /// Creation time for cleanup
    created_at: Instant,

    /// Total changes buffered
    total_changes: CachePadded<AtomicU64>,

    /// Total votes buffered  
    total_votes: CachePadded<AtomicU64>,
}

/// Per-thread buffer to avoid lock contention
struct ThreadBuffer {
    /// Pre-allocated buffers per slot
    slots: DashMap<Slot, SlotBuffer>,

    /// Last access time for cleanup
    last_access: AtomicI64,
}

/// Buffer for a specific slot in a thread
struct SlotBuffer {
    account_changes: Vec<OwnedAccountChange>,
    vote_transactions: Vec<VoteTransaction>,
}

/// Metrics for monitoring
pub struct Metrics {
    // Slot metrics
    pub slots_created: AtomicU64,
    pub slots_completed: AtomicU64,
    pub slots_dropped: AtomicU64,

    // Write metrics
    pub account_changes_buffered: AtomicU64,
    pub vote_transactions_buffered: AtomicU64,

    // Performance metrics
    pub hot_cache_hits: AtomicU64,
    pub hot_cache_misses: AtomicU64,
    pub buffer_allocations: AtomicU64,

    // Thread metrics
    pub active_threads: AtomicU32,
    pub total_threads_seen: AtomicU64,

    // Timing metrics (microseconds)
    pub avg_write_time_us: AtomicU64,
    pub avg_aggregate_time_us: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct BankHashComponents {
    pub slot: Slot,
    pub bank_hash: Hash,
    pub parent_bank_hash: Hash,
    pub signature_count: u64,
    pub last_blockhash: Hash,
    pub accounts_delta_hash: Option<Hash>,
    pub accounts_lthash_checksum: Option<String>,
    pub epoch_accounts_hash: Option<Hash>,
    pub cumulative_lthash: LtHash,
    pub delta_lthash: LtHash,
}

impl Default for BankHashComponents {
    fn default() -> Self {
        Self {
            slot: 0,
            bank_hash: Hash::default(),
            parent_bank_hash: Hash::default(),
            signature_count: 0,
            last_blockhash: Hash::default(),
            accounts_delta_hash: None,
            accounts_lthash_checksum: None,
            epoch_accounts_hash: None,
            cumulative_lthash: LtHash::identity(),
            delta_lthash: LtHash::identity(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct VoteTransaction {
    pub slot: Slot,
    pub voter_pubkey: Pubkey,
    pub vote_signature: String,
    pub vote_transaction: Vec<u8>,
    pub transaction_meta: Option<serde_json::Value>,
    pub vote_type: String,
    pub vote_slot: Option<Slot>,
    pub vote_hash: Option<String>,
    pub root_slot: Option<Slot>,
    pub lockouts_count: Option<u32>,
    pub timestamp: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct BlockMetadata {
    pub blockhash: String,
    pub parent_slot: Slot,
    pub executed_transaction_count: u64,
    pub entry_count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SlotStatus {
    FirstShredReceived = 0,
    Completed = 1,
    Processed = 2,
    Confirmed = 3,
    Rooted = 4,
}

impl AirLock {
    pub fn new() -> Self {
        let mut hot_slots = Vec::with_capacity(HOT_SLOTS);
        for _ in 0..HOT_SLOTS {
            hot_slots.push(CachePadded::new(AtomicU64::new(0)));
        }

        Self {
            hot_slots: hot_slots.into_boxed_slice(),
            slots: DashMap::with_capacity(1024),
            thread_buffers: DashMap::new(),
            ready_queue: SegQueue::new(),
            metrics: Metrics {
                slots_created: AtomicU64::new(0),
                slots_completed: AtomicU64::new(0),
                slots_dropped: AtomicU64::new(0),
                account_changes_buffered: AtomicU64::new(0),
                vote_transactions_buffered: AtomicU64::new(0),
                hot_cache_hits: AtomicU64::new(0),
                hot_cache_misses: AtomicU64::new(0),
                buffer_allocations: AtomicU64::new(0),
                active_threads: AtomicU32::new(0),
                total_threads_seen: AtomicU64::new(0),
                avg_write_time_us: AtomicU64::new(0),
                avg_aggregate_time_us: AtomicU64::new(0),
            },
            last_cleanup: Mutex::new(Instant::now()),
        }
    }

    /// Get or create slot data with hot cache optimization
    #[inline]
    fn get_or_create_slot(&self, slot: Slot) -> Arc<SlotData> {
        // Check hot slots first (most likely during bursts)
        let hot_index = (slot as usize) & (HOT_SLOTS - 1);
        let hot_slot = self.hot_slots[hot_index].load(Ordering::Relaxed);

        if hot_slot == slot {
            self.metrics.hot_cache_hits.fetch_add(1, Ordering::Relaxed);
            if let Some(data) = self.slots.get(&slot) {
                return data.clone();
            }
        }

        self.metrics
            .hot_cache_misses
            .fetch_add(1, Ordering::Relaxed);

        // Not in hot cache, get or create
        let slot_data = self.slots.entry(slot).or_insert_with(|| {
            self.metrics.slots_created.fetch_add(1, Ordering::Relaxed);

            // Update hot cache
            self.hot_slots[hot_index].store(slot, Ordering::Relaxed);

            Arc::new(SlotData {
                slot,
                active_writers: CachePadded::new(AtomicU32::new(0)),
                bank_hash: RwLock::new(None),
                block_metadata: RwLock::new(None),
                status: AtomicU32::new(SlotStatus::FirstShredReceived as u32),
                created_at: Instant::now(),
                total_changes: CachePadded::new(AtomicU64::new(0)),
                total_votes: CachePadded::new(AtomicU64::new(0)),
            })
        });

        slot_data.clone()
    }

    /// Get or create thread buffer
    #[inline]
    fn get_thread_buffer(&self) -> dashmap::mapref::one::Ref<std::thread::ThreadId, ThreadBuffer> {
        let thread_id = std::thread::current().id();

        self.thread_buffers.entry(thread_id).or_insert_with(|| {
            self.metrics
                .total_threads_seen
                .fetch_add(1, Ordering::Relaxed);
            self.metrics.active_threads.fetch_add(1, Ordering::Relaxed);

            ThreadBuffer {
                slots: DashMap::with_capacity(4),
                last_access: AtomicI64::new(Instant::now().elapsed().as_secs() as i64),
            }
        });
        self.thread_buffers.get(&thread_id).unwrap()
    }

    /// Begin writing to a slot
    pub fn begin_write(&self, slot: Slot) -> SlotWriter {
        let slot_data = self.get_or_create_slot(slot);
        slot_data.active_writers.fetch_add(1, Ordering::AcqRel);

        SlotWriter {
            slot,
            slot_data,
            airlock: self,
            _start_time: Instant::now(),
        }
    }

    /// Buffer an account change
    pub fn buffer_account_change(&self, change: OwnedAccountChange) {
        let start = Instant::now();
        let slot = change.slot;

        let thread_buffer = self.get_thread_buffer();
        thread_buffer
            .last_access
            .store(Instant::now().elapsed().as_secs() as i64, Ordering::Relaxed);

        let mut slot_buffer = thread_buffer.slots.entry(slot).or_insert_with(|| {
            self.metrics
                .buffer_allocations
                .fetch_add(1, Ordering::Relaxed);
            SlotBuffer {
                account_changes: Vec::with_capacity(SLOT_BUFFER_CAPACITY),
                vote_transactions: Vec::with_capacity(VOTE_BUFFER_CAPACITY),
            }
        });

        slot_buffer.account_changes.push(change);

        // Update metrics
        self.metrics
            .account_changes_buffered
            .fetch_add(1, Ordering::Relaxed);
        if let Some(slot_data) = self.slots.get(&slot) {
            slot_data.total_changes.fetch_add(1, Ordering::Relaxed);
        }

        // Update average write time
        let elapsed = start.elapsed().as_micros() as u64;
        self.metrics
            .avg_write_time_us
            .store(elapsed, Ordering::Relaxed);
    }

    /// Buffer a vote transaction
    pub fn buffer_vote_transaction(&self, vote: VoteTransaction) {
        let slot = vote.slot;

        let thread_buffer = self.get_thread_buffer();
        thread_buffer
            .last_access
            .store(Instant::now().elapsed().as_secs() as i64, Ordering::Relaxed);

        let mut slot_buffer = thread_buffer.slots.entry(slot).or_insert_with(|| {
            self.metrics
                .buffer_allocations
                .fetch_add(1, Ordering::Relaxed);
            SlotBuffer {
                account_changes: Vec::with_capacity(SLOT_BUFFER_CAPACITY),
                vote_transactions: Vec::with_capacity(VOTE_BUFFER_CAPACITY),
            }
        });

        slot_buffer.vote_transactions.push(vote);

        // Update metrics
        self.metrics
            .vote_transactions_buffered
            .fetch_add(1, Ordering::Relaxed);
        if let Some(slot_data) = self.slots.get(&slot) {
            slot_data.total_votes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Set bank hash components
    pub fn set_bank_hash_components(&self, slot: Slot, components: BankHashComponents) {
        if let Some(slot_data) = self.slots.get(&slot) {
            *slot_data.bank_hash.write() = Some(components);
        }
    }
    
    /// Set block metadata
    pub fn set_block_metadata(&self, slot: Slot, metadata: BlockMetadata) {
        if let Some(slot_data) = self.slots.get(&slot) {
            *slot_data.block_metadata.write() = Some(metadata);
        }
    }

    /// Update slot status
    pub fn update_slot_status(&self, slot: Slot, status: SlotStatus) {
        if let Some(slot_data) = self.slots.get(&slot) {
            slot_data
                .status
                .store(status as u8 as u32, Ordering::Release);

            // If rooted and no active writers, queue for aggregation
            if status == SlotStatus::Rooted {
                if slot_data.active_writers.load(Ordering::Acquire) == 0 {
                    self.ready_queue.push(slot);
                }
            }
        }
    }

    /// Try to get a ready slot
    pub fn try_get_ready_slot(&self) -> Option<Slot> {
        // Trigger cleanup if needed
        self.maybe_cleanup();

        self.ready_queue.pop()
    }
    
    /// Mark a slot as ready for aggregation (used during shutdown)
    pub fn mark_slot_ready(&self, slot: Slot) {
        if self.slots.contains_key(&slot) {
            self.ready_queue.push(slot);
        }
    }

    /// Aggregate all data for a slot
    pub fn aggregate_slot_data(&self, slot: Slot) -> Option<AggregatedSlotData> {
        let start = Instant::now();

        let slot_data = self.slots.get(&slot)?;

        // Spin-wait for active writers (should be very brief)
        let mut spins = 0;
        while slot_data.active_writers.load(Ordering::Acquire) > 0 {
            spins += 1;
            if spins > 1000 {
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }

        // Get bank hash and block metadata
        let bank_hash_components = slot_data.bank_hash.read().clone()?;
        let block_metadata = slot_data.block_metadata.read().clone();

        // Aggregate from all thread buffers
        let mut all_account_changes = Vec::new();
        let mut all_vote_transactions = Vec::new();

        // Collect and remove from all thread buffers
        for thread_buffer in self.thread_buffers.iter() {
            if let Some((_, mut slot_buffer)) = thread_buffer.slots.remove(&slot) {
                all_account_changes.append(&mut slot_buffer.account_changes);
                all_vote_transactions.append(&mut slot_buffer.vote_transactions);
            }
        }

        // Remove slot from storage
        self.slots.remove(&slot);

        // Update metrics
        self.metrics.slots_completed.fetch_add(1, Ordering::Relaxed);
        let elapsed = start.elapsed().as_micros() as u64;
        self.metrics
            .avg_aggregate_time_us
            .store(elapsed, Ordering::Relaxed);

        Some(AggregatedSlotData {
            slot,
            bank_hash_components,
            block_metadata,
            account_changes: all_account_changes,
            vote_transactions: all_vote_transactions,
            status: SlotStatus::Rooted,
        })
    }

    /// Cleanup old slots and inactive threads
    fn maybe_cleanup(&self) {
        let mut last_cleanup = self.last_cleanup.lock();
        if last_cleanup.elapsed() < CLEANUP_INTERVAL {
            return;
        }

        *last_cleanup = Instant::now();
        drop(last_cleanup);

        // Cleanup old slots
        let now = Instant::now();
        let mut removed = 0;
        self.slots.retain(|_, slot_data| {
            if now.duration_since(slot_data.created_at) > MAX_SLOT_AGE
                && slot_data.active_writers.load(Ordering::Relaxed) == 0
            {
                removed += 1;
                false
            } else {
                true
            }
        });

        if removed > 0 {
            self.metrics
                .slots_dropped
                .fetch_add(removed, Ordering::Relaxed);
        }

        // Cleanup inactive threads
        let current_time = Instant::now().elapsed().as_secs() as i64;
        let mut inactive_threads = 0;
        self.thread_buffers.retain(|_, buffer| {
            let last_access = buffer.last_access.load(Ordering::Relaxed);
            if current_time - last_access > 300 {
                // 5 minutes
                inactive_threads += 1;
                false
            } else {
                true
            }
        });

        if inactive_threads > 0 {
            self.metrics
                .active_threads
                .fetch_sub(inactive_threads, Ordering::Relaxed);
        }
    }

    /// Get current metrics
    pub fn get_metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Get current stats
    pub fn get_stats(&self) -> AirLockStats {
        AirLockStats {
            active_slots: self.slots.len(),
            total_slots: self.metrics.slots_created.load(Ordering::Relaxed),
            total_account_changes: self
                .metrics
                .account_changes_buffered
                .load(Ordering::Relaxed),
            ready_slots: self.ready_queue.len(),
        }
    }
    
    /// Get all active slots (for shutdown)
    pub fn get_active_slots(&self) -> Vec<Slot> {
        self.slots.iter().map(|entry| *entry.key()).collect()
    }
    
    /// Get ready queue length
    pub fn get_ready_queue_len(&self) -> usize {
        self.ready_queue.len()
    }
    
    /// Get total vote transactions
    pub fn get_total_vote_transactions(&self) -> u64 {
        self.metrics.vote_transactions_buffered.load(Ordering::Relaxed)
    }
}

/// RAII wrapper for slot writes
pub struct SlotWriter<'a> {
    slot: Slot,
    slot_data: Arc<SlotData>,
    airlock: &'a AirLock,
    _start_time: Instant,
}

impl<'a> Drop for SlotWriter<'a> {
    fn drop(&mut self) {
        let prev = self.slot_data.active_writers.fetch_sub(1, Ordering::AcqRel);

        // If we were the last writer and slot is rooted, queue for aggregation
        if prev == 1 {
            let status = self.slot_data.status.load(Ordering::Acquire);
            if status == SlotStatus::Rooted as u32 {
                self.airlock.ready_queue.push(self.slot);
            }
        }
    }
}

#[derive(Debug)]
pub struct AggregatedSlotData {
    pub slot: Slot,
    pub bank_hash_components: BankHashComponents,
    pub block_metadata: Option<BlockMetadata>,
    pub account_changes: Vec<OwnedAccountChange>,
    pub vote_transactions: Vec<VoteTransaction>,
    pub status: SlotStatus,
}

#[derive(Debug)]
pub struct AirLockStats {
    pub active_slots: usize,
    pub total_slots: u64,
    pub total_account_changes: u64,
    pub ready_slots: usize,
}
