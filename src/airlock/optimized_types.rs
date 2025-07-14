use crossbeam_queue::SegQueue;
use parking_lot::RwLock;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::account::ReadableAccount;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use crate::airlock::types::{
    BankHashComponentsInfo, VoteTransaction,
};
use agave_geyser_plugin_interface::geyser_plugin_interface::OwnedAccountChange;

// Thread-local buffer for accumulating changes without synchronization
thread_local! {
    static LOCAL_BUFFER: RefCell<LocalBuffer> = RefCell::new(LocalBuffer::new());
}

/// Per-thread local buffer to avoid contention
#[derive(Debug)]
pub struct LocalBuffer {
    /// Slot-indexed buffers for account changes
    slots: HashMap<u64, LocalSlotBuffer>,
    /// Last flush time for periodic cleanup
    last_flush: Instant,
}

#[derive(Debug)]
pub struct LocalSlotBuffer {
    /// Account changes accumulated in this thread - stores the full OwnedAccountChange
    pub account_changes: Vec<OwnedAccountChange>,
    /// Vote transactions seen by this thread
    pub vote_transactions: Vec<VoteTransaction>,
    /// Whether any monitored account was seen
    pub has_monitored: bool,
    /// Approximate memory usage
    pub memory_usage: usize,
}

impl Default for LocalSlotBuffer {
    fn default() -> Self {
        Self {
            // Pre-allocate for high-throughput scenarios
            // 2048 accounts to handle 1024+ with headroom for bursts
            account_changes: Vec::with_capacity(2048),
            // 512 votes to handle 300+ with headroom
            vote_transactions: Vec::with_capacity(512),
            has_monitored: false,
            memory_usage: 0,
        }
    }
}

impl LocalBuffer {
    fn new() -> Self {
        Self {
            slots: HashMap::with_capacity(16), // Typical concurrent slot count
            last_flush: Instant::now(),
        }
    }

    /// Add an account change to the thread-local buffer
    pub fn add_account_change(slot: u64, change: OwnedAccountChange, is_monitored: bool) {
        LOCAL_BUFFER.with(|buffer| {
            let mut buf = buffer.borrow_mut();
            let slot_buf = buf.slots.entry(slot).or_default();
            
            // Track memory usage for backpressure (include account data size)
            let data_size = change.old_account.data().len() + change.new_account.data().len();
            slot_buf.memory_usage += std::mem::size_of::<OwnedAccountChange>() + data_size;
            slot_buf.has_monitored |= is_monitored;
            slot_buf.account_changes.push(change);
            
            // Periodic cleanup of old slots (every 10 seconds)
            if buf.last_flush.elapsed().as_secs() > 10 {
                buf.cleanup_old_slots();
                buf.last_flush = Instant::now();
            }
        });
    }

    /// Add a vote transaction to the thread-local buffer
    pub fn add_vote_transaction(slot: u64, vote: VoteTransaction) {
        LOCAL_BUFFER.with(|buffer| {
            let mut buf = buffer.borrow_mut();
            let slot_buf = buf.slots.entry(slot).or_default();
            slot_buf.memory_usage += std::mem::size_of::<VoteTransaction>() + vote.vote_transaction.len();
            slot_buf.vote_transactions.push(vote);
        });
    }

    /// Flush all data for a specific slot
    pub fn flush_slot(slot: u64) -> Option<LocalSlotBuffer> {
        LOCAL_BUFFER.with(|buffer| {
            buffer.borrow_mut().slots.remove(&slot)
        })
    }

    /// Get current memory usage across all slots
    pub fn get_memory_usage() -> usize {
        LOCAL_BUFFER.with(|buffer| {
            buffer.borrow()
                .slots
                .values()
                .map(|s| s.memory_usage)
                .sum()
        })
    }

    fn cleanup_old_slots(&mut self) {
        // Remove slots older than 100 slots (conservative to avoid data loss)
        if let Some(max_slot) = self.slots.keys().max().copied() {
            let mut removed = 0;
            self.slots.retain(|&slot, buffer| {
                let should_keep = max_slot.saturating_sub(slot) < 100;
                if !should_keep {
                    removed += 1;
                    // Clear vectors to free memory immediately
                    drop(std::mem::take(&mut buffer.account_changes));
                    drop(std::mem::take(&mut buffer.vote_transactions));
                }
                should_keep
            });
            
            if removed > 0 {
                log::debug!("Cleaned up {} old slot buffers", removed);
                // Shrink the HashMap if it's getting too sparse
                if self.slots.len() < self.slots.capacity() / 4 {
                    self.slots.shrink_to_fit();
                }
            }
        }
    }
}

/// Optimized slot data with lock-free structures and better cache locality
#[derive(Debug)]
pub struct OptimizedSlotData {
    /// Slot number for this data
    pub slot: u64,
    
    /// Lock-free vote collection distributed across cores
    pub votes: LockFreeVotes,
    
    /// Optimistic concurrency control for metadata
    pub metadata: OptimisticMetadata,
    
    /// Aggregated account changes (populated during flush)
    pub account_changes: RwLock<Option<Vec<OwnedAccountChange>>>,
    
    /// Lifecycle tracking
    pub lifecycle: SlotLifecycle,
    
    /// Memory pressure tracking
    pub memory_tracker: MemoryTracker,
}

/// Distributed lock-free vote collection
#[derive(Debug)]
pub struct LockFreeVotes {
    /// Per-core queues to reduce contention
    queues: Vec<SegQueue<VoteTransaction>>,
    /// Total vote count for quick checks
    count: AtomicU32,
    /// Number of queues (typically CPU count)
    num_queues: usize,
}

impl LockFreeVotes {
    pub fn new(num_cores: usize) -> Self {
        let queues = (0..num_cores)
            .map(|_| SegQueue::new())
            .collect();
        
        Self {
            queues,
            count: AtomicU32::new(0),
            num_queues: num_cores,
        }
    }

    /// Add vote using CPU core affinity for cache locality
    pub fn push(&self, vote: VoteTransaction) {
        // Use thread ID to distribute across queues (better than random)
        // Use a simple hash of thread ID for distribution
        let thread_id = std::thread::current().id();
        let thread_hash = format!("{:?}", thread_id);
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(&thread_hash, &mut hasher);
        let queue_idx = (std::hash::Hasher::finish(&hasher) as usize) % self.num_queues;
        
        self.queues[queue_idx].push(vote);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Collect all votes efficiently
    pub fn collect_all(&self) -> Vec<VoteTransaction> {
        let expected_count = self.count.load(Ordering::Acquire);
        let mut votes = Vec::with_capacity(expected_count as usize);
        
        // Drain all queues in parallel if worth it
        for queue in &self.queues {
            while let Some(vote) = queue.pop() {
                votes.push(vote);
            }
        }
        
        self.count.store(0, Ordering::Release);
        votes
    }

    pub fn is_empty(&self) -> bool {
        self.count.load(Ordering::Acquire) == 0
    }
}

/// Optimistic concurrency for metadata updates
#[derive(Debug)]
pub struct OptimisticMetadata {
    /// Version counter for optimistic locking
    version: AtomicU64,
    /// Actual metadata snapshot
    data: RwLock<MetadataSnapshot>,
    /// Quick flags for common checks
    flags: MetadataFlags,
}

#[derive(Debug, Default, Clone)]
pub struct MetadataSnapshot {
    pub bank_hash_components: Option<BankHashComponentsInfo>,
    pub delta_lthash: Option<Vec<u8>>,
    pub cumulative_lthash: Option<Vec<u8>>,
    pub blockhash: Option<String>,
    pub parent_slot: Option<u64>,
    pub executed_transaction_count: Option<u64>,
    pub entry_count: Option<u64>,
}

/// Cache-aligned flags for hot path checks
#[repr(align(64))]
#[derive(Debug)]
pub struct MetadataFlags {
    pub has_bank_hash: AtomicBool,
    pub has_lthash: AtomicBool,
    pub has_block_metadata: AtomicBool,
    pub contains_monitored_change: AtomicBool,
    _padding: [u8; 60], // Prevent false sharing
}

impl Default for MetadataFlags {
    fn default() -> Self {
        Self {
            has_bank_hash: AtomicBool::new(false),
            has_lthash: AtomicBool::new(false),
            has_block_metadata: AtomicBool::new(false),
            contains_monitored_change: AtomicBool::new(false),
            _padding: [0; 60],
        }
    }
}

impl OptimisticMetadata {
    pub fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            data: RwLock::new(MetadataSnapshot::default()),
            flags: MetadataFlags::default(),
        }
    }

    /// Update bank hash with optimistic concurrency
    pub fn set_bank_hash(&self, components: BankHashComponentsInfo) {
        let mut data = self.data.write();
        data.bank_hash_components = Some(components);
        self.version.fetch_add(1, Ordering::Release);
        self.flags.has_bank_hash.store(true, Ordering::Release);
    }

    /// Update lthash with optimistic concurrency
    pub fn set_lthash(&self, delta: Vec<u8>, cumulative: Vec<u8>) {
        let mut data = self.data.write();
        data.delta_lthash = Some(delta);
        data.cumulative_lthash = Some(cumulative);
        self.version.fetch_add(1, Ordering::Release);
        self.flags.has_lthash.store(true, Ordering::Release);
    }

    /// Update block metadata
    pub fn set_block_metadata(&self, blockhash: String, parent: u64, tx_count: u64, entries: u64) {
        let mut data = self.data.write();
        data.blockhash = Some(blockhash);
        data.parent_slot = Some(parent);
        data.executed_transaction_count = Some(tx_count);
        data.entry_count = Some(entries);
        self.version.fetch_add(1, Ordering::Release);
        self.flags.has_block_metadata.store(true, Ordering::Release);
    }

    /// Get consistent snapshot for reading
    pub fn snapshot(&self) -> MetadataSnapshot {
        self.data.read().clone()
    }

    /// Quick check if all required data is present
    pub fn is_complete(&self) -> bool {
        self.flags.has_bank_hash.load(Ordering::Acquire) &&
        self.flags.has_lthash.load(Ordering::Acquire)
    }
}

/// Slot lifecycle management with proper state transitions
#[derive(Debug)]
pub struct SlotLifecycle {
    /// Current state (0=created, 1=processed, 2=confirmed, 3=rooted, 4=flushing, 5=flushed)
    state: AtomicU32,
    /// Creation timestamp for age tracking
    created_at: Instant,
    /// When slot was marked for flush
    flush_requested_at: AtomicU64,
    /// Number of active writers (for safe cleanup)
    active_writers: AtomicU32,
}

impl SlotLifecycle {
    pub fn new() -> Self {
        Self {
            state: AtomicU32::new(0),
            created_at: Instant::now(),
            flush_requested_at: AtomicU64::new(0),
            active_writers: AtomicU32::new(0),
        }
    }

    /// Increment writer count (returns previous value)
    pub fn inc_writers(&self) -> u32 {
        self.active_writers.fetch_add(1, Ordering::AcqRel)
    }

    /// Decrement writer count (returns previous value)
    pub fn dec_writers(&self) -> u32 {
        self.active_writers.fetch_sub(1, Ordering::AcqRel)
    }

    /// Check if slot can be safely cleaned up
    pub fn can_cleanup(&self) -> bool {
        self.active_writers.load(Ordering::Acquire) == 0 &&
        self.state.load(Ordering::Acquire) >= 5 // flushed
    }

    /// Transition to new state (returns true if successful)
    pub fn transition_to(&self, new_state: SlotState) -> bool {
        let new_val = new_state as u32;
        let current = self.state.load(Ordering::Acquire);
        
        // Only allow forward transitions
        if new_val > current {
            self.state.compare_exchange(
                current,
                new_val,
                Ordering::Release,
                Ordering::Acquire
            ).is_ok()
        } else {
            false
        }
    }

    pub fn is_rooted(&self) -> bool {
        self.state.load(Ordering::Acquire) >= 3
    }

    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }
}

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Created = 0,
    Processed = 1,
    Confirmed = 2,
    Rooted = 3,
    Flushing = 4,
    Flushed = 5,
}

/// Memory tracking for backpressure
#[derive(Debug)]
pub struct MemoryTracker {
    /// Current memory usage in bytes
    usage: AtomicUsize,
    /// High water mark for monitoring
    high_water_mark: AtomicUsize,
}

impl MemoryTracker {
    pub fn new() -> Self {
        Self {
            usage: AtomicUsize::new(0),
            high_water_mark: AtomicUsize::new(0),
        }
    }

    pub fn add(&self, bytes: usize) {
        let new_usage = self.usage.fetch_add(bytes, Ordering::AcqRel) + bytes;
        self.high_water_mark.fetch_max(new_usage, Ordering::Relaxed);
    }

    pub fn sub(&self, bytes: usize) {
        self.usage.fetch_sub(bytes, Ordering::AcqRel);
    }

    pub fn get(&self) -> usize {
        self.usage.load(Ordering::Acquire)
    }

    pub fn get_high_water_mark(&self) -> usize {
        self.high_water_mark.load(Ordering::Acquire)
    }
}

impl OptimizedSlotData {
    pub fn new(slot: u64, num_cores: usize) -> Self {
        Self {
            slot,
            votes: LockFreeVotes::new(num_cores),
            metadata: OptimisticMetadata::new(),
            account_changes: RwLock::new(None),
            lifecycle: SlotLifecycle::new(),
            memory_tracker: MemoryTracker::new(),
        }
    }

    /// Check if slot has all required data for DB write
    pub fn is_ready_for_write(&self) -> bool {
        self.lifecycle.is_rooted() && self.metadata.is_complete()
    }

    /// Mark that this slot contains monitored changes
    pub fn mark_monitored(&self) {
        self.metadata.flags.contains_monitored_change.store(true, Ordering::Release);
    }

    /// Check if slot contains monitored changes
    pub fn has_monitored_changes(&self) -> bool {
        self.metadata.flags.contains_monitored_change.load(Ordering::Acquire)
    }
}