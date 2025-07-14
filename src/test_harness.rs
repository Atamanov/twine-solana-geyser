#[cfg(test)]
pub mod test_harness {
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};
    use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
    
    use crossbeam::channel::{bounded, Sender, Receiver};
    use dashmap::DashMap;
    
    use solana_sdk::{
        pubkey::Pubkey,
        account::AccountSharedData,
        hash::Hash,
        clock::Slot,
    };
    use solana_lattice_hash::lt_hash::LtHash;
    
    use crate::{
        airlock::{AirLock, BankHashComponents, SlotStatus, OwnedAccountChange, VoteTransaction},
        aggregator::spawn_aggregators,
        db_writer::{DbCommand, SlotData, AccountChangeData},
    };
    
    /// Mock database writer for tests
    pub struct MockDbWriter {
        pub received_commands: Arc<DashMap<String, Vec<DbCommand>>>,
        pub receiver: Receiver<DbCommand>,
    }
    
    impl MockDbWriter {
        pub fn new(receiver: Receiver<DbCommand>) -> Self {
            Self {
                received_commands: Arc::new(DashMap::new()),
                receiver,
            }
        }
        
        pub async fn run(self) {
            while let Ok(cmd) = self.receiver.recv() {
                match &cmd {
                    DbCommand::WriteSlot(slot_data) => {
                        self.received_commands
                            .entry("slots".to_string())
                            .or_insert_with(Vec::new)
                            .push(cmd);
                    }
                    DbCommand::WriteAccountChanges(_) => {
                        self.received_commands
                            .entry("account_changes".to_string())
                            .or_insert_with(Vec::new)
                            .push(cmd);
                    }
                    DbCommand::WriteVoteTransactions(_) => {
                        self.received_commands
                            .entry("vote_transactions".to_string())
                            .or_insert_with(Vec::new)
                            .push(cmd);
                    }
                    DbCommand::Shutdown => break,
                }
            }
        }
    }
    
    /// Test harness for end-to-end testing
    pub struct TestHarness {
        pub airlock: Arc<AirLock>,
        pub db_sender: Sender<DbCommand>,
        pub db_receiver: Receiver<DbCommand>,
        pub aggregator_handles: Vec<thread::JoinHandle<()>>,
        pub db_commands: Arc<DashMap<String, Vec<DbCommand>>>,
    }
    
    impl TestHarness {
        pub fn new(num_aggregators: usize) -> Self {
            let airlock = Arc::new(AirLock::new());
            let (db_sender, db_receiver) = bounded(10000);
            
            let aggregator_handles = spawn_aggregators(
                airlock.clone(),
                db_sender.clone(),
                num_aggregators,
            );
            
            Self {
                airlock,
                db_sender,
                db_receiver,
                aggregator_handles,
                db_commands: Arc::new(DashMap::new()),
            }
        }
        
        pub fn process_slot(&self, slot: Slot, num_changes: usize) {
            let writer = self.airlock.begin_write(slot);
            
            self.airlock.set_bank_hash_components(slot, BankHashComponents {
                slot,
                bank_hash: Hash::new_unique(),
                parent_bank_hash: Hash::new_unique(),
                signature_count: slot * 100,
                last_blockhash: Hash::new_unique(),
                accounts_delta_hash: Some(Hash::new_unique()),
                accounts_lthash_checksum: Some("test".to_string()),
                epoch_accounts_hash: None,
                cumulative_lthash: LtHash::identity(),
                delta_lthash: LtHash::identity(),
            });
            
            for i in 0..num_changes {
                self.airlock.buffer_account_change(OwnedAccountChange {
                    pubkey: Pubkey::new_unique(),
                    slot,
                    old_account: AccountSharedData::new(i as u64, 0, &Pubkey::default()),
                    new_account: AccountSharedData::new((i + 1000) as u64, 32, &Pubkey::default()),
                    old_lthash: LtHash::identity(),
                    new_lthash: LtHash::identity(),
                });
            }
            
            self.airlock.update_slot_status(slot, SlotStatus::Rooted);
            drop(writer);
        }
        
        pub fn collect_db_commands(&self, timeout: Duration) -> DashMap<String, Vec<DbCommand>> {
            let commands = DashMap::new();
            let start = Instant::now();
            
            while start.elapsed() < timeout {
                match self.db_receiver.recv_timeout(Duration::from_millis(10)) {
                    Ok(cmd) => {
                        let key = match &cmd {
                            DbCommand::WriteSlot(_) => "slots",
                            DbCommand::WriteAccountChanges(_) => "account_changes",
                            DbCommand::WriteVoteTransactions(_) => "vote_transactions",
                            DbCommand::Shutdown => "shutdown",
                        };
                        commands.entry(key.to_string()).or_insert_with(Vec::new).push(cmd);
                    }
                    Err(_) => continue,
                }
            }
            
            commands
        }
        
        pub fn shutdown(self) {
            // Send shutdown to aggregators
            for _ in 0..self.aggregator_handles.len() {
                let _ = self.db_sender.send(DbCommand::Shutdown);
            }
            
            // Wait for threads
            for handle in self.aggregator_handles {
                let _ = handle.join();
            }
        }
    }
    
    /// Performance test harness
    pub struct PerfTestHarness {
        pub airlock: Arc<AirLock>,
        pub running: Arc<AtomicBool>,
        pub total_changes: Arc<AtomicU64>,
        pub total_slots: Arc<AtomicU64>,
    }
    
    impl PerfTestHarness {
        pub fn new() -> Self {
            Self {
                airlock: Arc::new(AirLock::new()),
                running: Arc::new(AtomicBool::new(true)),
                total_changes: Arc::new(AtomicU64::new(0)),
                total_slots: Arc::new(AtomicU64::new(0)),
            }
        }
        
        pub fn spawn_writer_threads(&self, num_threads: usize, changes_per_slot: usize) -> Vec<thread::JoinHandle<()>> {
            (0..num_threads)
                .map(|thread_id| {
                    let airlock = self.airlock.clone();
                    let running = self.running.clone();
                    let total_changes = self.total_changes.clone();
                    let total_slots = self.total_slots.clone();
                    
                    thread::spawn(move || {
                        let mut slot = thread_id as u64 * 10000;
                        
                        while running.load(Ordering::Relaxed) {
                            let writer = airlock.begin_write(slot);
                            
                            for i in 0..changes_per_slot {
                                airlock.buffer_account_change(OwnedAccountChange {
                                    pubkey: Pubkey::new_unique(),
                                    slot,
                                    old_account: AccountSharedData::default(),
                                    new_account: AccountSharedData::new(i as u64, 32, &Pubkey::default()),
                                    old_lthash: LtHash::identity(),
                                    new_lthash: LtHash::identity(),
                                });
                                total_changes.fetch_add(1, Ordering::Relaxed);
                            }
                            
                            drop(writer);
                            total_slots.fetch_add(1, Ordering::Relaxed);
                            slot += 1;
                            
                            thread::yield_now();
                        }
                    })
                })
                .collect()
        }
        
        pub fn run_for_duration(&self, duration: Duration) -> PerfTestResults {
            let start = Instant::now();
            thread::sleep(duration);
            self.running.store(false, Ordering::Relaxed);
            
            let elapsed = start.elapsed();
            let total_changes = self.total_changes.load(Ordering::Relaxed);
            let total_slots = self.total_slots.load(Ordering::Relaxed);
            let metrics = self.airlock.get_metrics();
            
            PerfTestResults {
                duration: elapsed,
                total_changes,
                total_slots,
                changes_per_second: total_changes as f64 / elapsed.as_secs_f64(),
                slots_per_second: total_slots as f64 / elapsed.as_secs_f64(),
                cache_hit_rate: calculate_hit_rate(&metrics),
                avg_write_time_us: metrics.avg_write_time_us.load(Ordering::Relaxed),
                buffer_allocations: metrics.buffer_allocations.load(Ordering::Relaxed),
            }
        }
    }
    
    #[derive(Debug)]
    pub struct PerfTestResults {
        pub duration: Duration,
        pub total_changes: u64,
        pub total_slots: u64,
        pub changes_per_second: f64,
        pub slots_per_second: f64,
        pub cache_hit_rate: f64,
        pub avg_write_time_us: u64,
        pub buffer_allocations: u64,
    }
    
    fn calculate_hit_rate(metrics: &crate::airlock::Metrics) -> f64 {
        let hits = metrics.hot_cache_hits.load(Ordering::Relaxed) as f64;
        let misses = metrics.hot_cache_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total > 0.0 { hits / total } else { 0.0 }
    }
    
    /// Create a realistic workload
    pub fn create_mainnet_workload(airlock: Arc<AirLock>, duration_secs: u64) -> Vec<thread::JoinHandle<()>> {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        
        // Stop after duration
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(duration_secs));
            running_clone.store(false, Ordering::Relaxed);
        });
        
        let mut handles = Vec::new();
        
        // Slot producer thread (2 slots/sec like mainnet)
        let slot_counter = Arc::new(AtomicU64::new(100000));
        let slot_counter_clone = slot_counter.clone();
        let running_clone = running.clone();
        handles.push(thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                slot_counter_clone.fetch_add(1, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(500));
            }
        }));
        
        // Writer threads (32 like validator)
        for thread_id in 0..32 {
            let airlock = airlock.clone();
            let running = running.clone();
            let slot_counter = slot_counter.clone();
            
            handles.push(thread::spawn(move || {
                while running.load(Ordering::Relaxed) {
                    let slot = slot_counter.load(Ordering::Relaxed);
                    let writer = airlock.begin_write(slot);
                    
                    // Each thread writes ~150 changes (5000 total / 32 threads)
                    for i in 0..150 {
                        airlock.buffer_account_change(OwnedAccountChange {
                            pubkey: Pubkey::new_unique(),
                            slot,
                            old_account: AccountSharedData::default(),
                            new_account: AccountSharedData::new(i, 32, &Pubkey::default()),
                            old_lthash: LtHash::identity(),
                            new_lthash: LtHash::identity(),
                        });
                    }
                    
                    drop(writer);
                    
                    // Small random delay to simulate real timing
                    thread::sleep(Duration::from_micros(thread_id * 100));
                }
            }));
        }
        
        handles
    }
}