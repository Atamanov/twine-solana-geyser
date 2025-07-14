use crate::airlock::optimized_types::{
    LocalBuffer, OptimizedSlotData, SlotState,
};
use crate::airlock::types::{
    DbWriteCommand, PluginConfig, VoteTransaction,
};
use crate::airlock::{AirlockStats};
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, Result as PluginResult, SlotStatus,
    ReplicaTransactionInfoVersions, MonitoredAccountsContext, OwnedAccountChange,
};
use crossbeam_channel::{bounded, Sender, Receiver, TrySendError};
use dashmap::{DashMap, DashSet};
use log::*;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction::sanitized::SanitizedTransaction;
use solana_transaction_status::TransactionStatusMeta;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use std::time::Duration;

use crate::chain_monitor::ChainMonitor;
use crate::metrics_server::MetricsServer;
use crate::worker_pool::WorkerPool;
use crate::stake::StakeAccountInfo;

#[derive(Debug)]
struct VoteDetails {
    vote_type: String,
    vote_slot: Option<u64>,
    vote_hash: Option<String>,
    root_slot: Option<u64>,
    lockouts_count: Option<u64>,
    timestamp: Option<i64>,
}

/// Optimized Twine Geyser Plugin with lock-free structures
pub struct TwineGeyserPlugin {
    /// Slot data storage with minimal contention
    slots: Arc<DashMap<u64, Arc<OptimizedSlotData>>>,
    
    /// Fast-lookup set of monitored pubkeys
    /// NOTE: This is kept for API compatibility and potential future use,
    /// but we do NOT filter account updates based on this during notify_account_change
    /// for performance reasons - the node sends all updates regardless.
    monitored_accounts: Arc<DashSet<Pubkey>>,
    
    has_monitored_accounts: Arc<AtomicBool>,
    
    /// Queue feeding the database worker pool
    db_writer_queue: Option<Sender<DbWriteCommand>>,
    
    /// Worker pool handle
    worker_pool: Option<WorkerPool>,
    
    /// Configuration
    config: Option<PluginConfig>,
    
    
    /// Runtime for async tasks
    runtime: Option<Runtime>,
    
    /// Statistics for monitoring
    stats: Arc<AirlockStats>,
    
    /// Last slot when stats were logged
    last_stats_log_slot: AtomicU64,
    
    /// Metrics server handle
    metrics_server: Option<tokio::task::JoinHandle<()>>,
    
    /// API server handle
    api_server: Option<std::thread::JoinHandle<()>>,
    
    /// Chain monitor
    chain_monitor: Option<Arc<ChainMonitor>>,
    
    /// Current epoch
    current_epoch: Arc<AtomicU64>,
    
    /// Accumulates stake states during epoch transitions
    epoch_stake_accumulator: Arc<DashMap<Pubkey, StakeAccountInfo>>,
    
    /// Pending slots for database write
    pending_writes: Arc<crossbeam_queue::SegQueue<u64>>,
    
    /// Background flusher for thread-local buffers
    flush_task: Option<tokio::task::JoinHandle<()>>,
    
    /// Cleanup task for old slots
    cleanup_task: Option<tokio::task::JoinHandle<()>>,
    
    /// Global memory pressure (bytes)
    global_memory: Arc<AtomicUsize>,
    
    /// Memory limit for backpressure (default 4GB)
    memory_limit: usize,
    
    /// Number of CPU cores for optimization
    num_cores: usize,
    
    /// Context for dynamically updating monitored accounts
    update_context: Option<Arc<Box<dyn MonitoredAccountsContext + Send + Sync>>>,
    
    /// Channel for receiving account update notifications from API
    account_update_receiver: Option<Receiver<()>>,
    
    /// Task handle for account update listener
    account_update_task: Option<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for TwineGeyserPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwineGeyserPlugin")
            .field("monitored_accounts_count", &self.monitored_accounts.len())
            .field("has_monitored_accounts", &self.has_monitored_accounts)
            .field("config", &self.config)
            .field("last_stats_log_slot", &self.last_stats_log_slot)
            .field("current_epoch", &self.current_epoch)
            .field("update_context_present", &self.update_context.is_some())
            .finish()
    }
}

impl Default for TwineGeyserPlugin {
    fn default() -> Self {
        let num_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        
        Self {
            slots: Arc::new(DashMap::with_capacity(128)),
            monitored_accounts: Arc::new(DashSet::new()),
            has_monitored_accounts: Arc::new(AtomicBool::new(false)),
            db_writer_queue: None,
            worker_pool: None,
            config: None,
            runtime: None,
            stats: Arc::new(AirlockStats::default()),
            last_stats_log_slot: AtomicU64::new(0),
            metrics_server: None,
            api_server: None,
            chain_monitor: None,
            current_epoch: Arc::new(AtomicU64::new(0)),
            epoch_stake_accumulator: Arc::new(DashMap::new()),
            pending_writes: Arc::new(crossbeam_queue::SegQueue::new()),
            flush_task: None,
            cleanup_task: None,
            global_memory: Arc::new(AtomicUsize::new(0)),
            memory_limit: 4 * 1024 * 1024 * 1024, // 4GB default
            num_cores,
            update_context: None,
            account_update_receiver: None,
            account_update_task: None,
        }
    }
}

impl TwineGeyserPlugin {
    /// Get or create slot data with minimal contention
    fn get_or_create_slot(&self, slot: u64) -> Result<Arc<OptimizedSlotData>, GeyserPluginError> {
        // Check memory pressure before creating new slots
        if self.global_memory.load(Ordering::Acquire) > self.memory_limit {
            self.stats.memory_pressure_events.fetch_add(1, Ordering::Relaxed);
            self.trigger_emergency_flush();
        }

        // Fast path: slot exists
        if let Some(slot_data) = self.slots.get(&slot) {
            return Ok(slot_data.clone());
        }

        // Slow path: create new slot
        let slot_data = self.slots
            .entry(slot)
            .or_insert_with(|| {
                self.stats.slots_created.fetch_add(1, Ordering::Relaxed);
                self.stats.slots_in_memory.fetch_add(1, Ordering::Relaxed);
                Arc::new(OptimizedSlotData::new(slot, self.num_cores))
            })
            .clone();

        Ok(slot_data)
    }

    /// Start background tasks for lifecycle management
    fn start_background_tasks(&mut self) {
        let runtime = self.runtime.as_ref().unwrap();
        
        // Thread-local buffer flush task (runs every 100ms)
        let slots = self.slots.clone();
        let pending = self.pending_writes.clone();
        let global_mem = self.global_memory.clone();
        let stats = self.stats.clone();
        
        self.flush_task = Some(runtime.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            
            loop {
                interval.tick().await;
                
                // Collect slots that need flushing
                let mut slots_to_flush = Vec::new();
                for entry in slots.iter() {
                    let slot_data = entry.value();
                    
                    // Flush if rooted or too old (>5 seconds)
                    if slot_data.lifecycle.is_rooted() || slot_data.lifecycle.age() > Duration::from_secs(5) {
                        if slot_data.lifecycle.transition_to(SlotState::Flushing) {
                            slots_to_flush.push(*entry.key());
                        }
                    }
                }
                
                // Flush thread-local buffers for these slots
                for slot in slots_to_flush {
                    if let Some(slot_data) = slots.get(&slot) {
                        Self::flush_thread_local_buffers(&slot_data, &global_mem, &stats);
                        
                        // Mark as ready for DB write if complete
                        if slot_data.is_ready_for_write() {
                            pending.push(slot);
                            slot_data.lifecycle.transition_to(SlotState::Flushed);
                            stats.slots_flushed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }));

        // Cleanup task (runs every second)
        let slots = self.slots.clone();
        let global_mem = self.global_memory.clone();
        let stats = self.stats.clone();
        
        self.cleanup_task = Some(runtime.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            
            loop {
                interval.tick().await;
                
                // Find slots safe to cleanup
                let mut slots_to_remove = Vec::new();
                for entry in slots.iter() {
                    let slot_data = entry.value();
                    
                    // Remove if flushed and no active writers
                    if slot_data.lifecycle.can_cleanup() && slot_data.lifecycle.age() > Duration::from_secs(10) {
                        slots_to_remove.push(*entry.key());
                    }
                }
                
                // Remove old slots
                for slot in slots_to_remove {
                    if let Some((_, slot_data)) = slots.remove(&slot) {
                        let mem_usage = slot_data.memory_tracker.get();
                        
                        // Explicitly drop large data structures to free memory
                        if let Some(changes) = slot_data.account_changes.write().take() {
                            info!("Dropping {} account changes for slot {}", changes.len(), slot);
                            drop(changes);
                        }
                        
                        // Collect and drop all votes
                        let votes = slot_data.votes.collect_all();
                        info!("Dropping {} votes for slot {}", votes.len(), slot);
                        drop(votes);
                        
                        global_mem.fetch_sub(mem_usage, Ordering::AcqRel);
                        stats.slots_cleaned.fetch_add(1, Ordering::Relaxed);
                        stats.slots_in_memory.fetch_sub(1, Ordering::Relaxed);
                        stats.slots_deleted_total.fetch_add(1, Ordering::Relaxed);
                        
                        info!("Cleaned up slot {} (memory: {} bytes)", slot, mem_usage);
                    }
                }
            }
        }));
    }

    /// Flush all thread-local buffers for a slot
    fn flush_thread_local_buffers(
        slot_data: &Arc<OptimizedSlotData>,
        global_mem: &Arc<AtomicUsize>,
        stats: &Arc<AirlockStats>,
    ) {
        // Collect from all thread-local buffers
        let local_data = LocalBuffer::flush_slot(slot_data.slot);
        
        if let Some(local) = local_data {
            // Update memory tracking
            slot_data.memory_tracker.add(local.memory_usage);
            global_mem.fetch_add(local.memory_usage, Ordering::AcqRel);
            stats.total_memory_usage.fetch_add(local.memory_usage, Ordering::AcqRel);
            
            let num_changes = local.account_changes.len();
            
            // Store account changes if monitored
            if local.has_monitored {
                let mut changes = slot_data.account_changes.write();
                if changes.is_none() {
                    // Pre-allocate with extra capacity for multiple thread buffers
                    // Assume ~8 threads contributing, each with up to 2048 accounts
                    *changes = Some(Vec::with_capacity(num_changes.max(4096)));
                }
                if let Some(ref mut vec) = *changes {
                    // Reserve additional capacity if needed to avoid reallocation
                    let needed_capacity = vec.len() + num_changes;
                    if vec.capacity() < needed_capacity {
                        vec.reserve(needed_capacity - vec.len());
                    }
                    vec.extend(local.account_changes);
                }
                slot_data.mark_monitored();
            }
            
            // Add votes to lock-free queue
            for vote in local.vote_transactions {
                slot_data.votes.push(vote);
                stats.vote_transactions_total.fetch_add(1, Ordering::Relaxed);
            }
            
            stats.total_updates.fetch_add(num_changes, Ordering::Relaxed);
        }
    }

    /// Emergency flush when memory pressure is high
    fn trigger_emergency_flush(&self) {
        warn!("Memory pressure detected, triggering emergency flush");
        
        // Force flush all slots
        for entry in self.slots.iter() {
            let slot_data = entry.value();
            if slot_data.lifecycle.transition_to(SlotState::Flushing) {
                Self::flush_thread_local_buffers(&slot_data, &self.global_memory, &self.stats);
                self.pending_writes.push(*entry.key());
            }
        }
    }

    /// Process pending database writes with backpressure
    fn process_pending_writes(&self) -> PluginResult<()> {
        let db_writer = self.db_writer_queue.as_ref()
            .ok_or_else(|| GeyserPluginError::Custom("DB writer not initialized".into()))?;

        // Process up to 100 pending writes
        for _ in 0..100 {
            if let Some(slot) = self.pending_writes.pop() {
                if let Some(slot_data) = self.slots.get(&slot) {
                    if !slot_data.is_ready_for_write() {
                        // Not ready yet, put it back
                        self.pending_writes.push(slot);
                        continue;
                    }

                    // Prepare write command
                    let metadata = slot_data.metadata.snapshot();
                    let votes = if slot_data.has_monitored_changes() {
                        slot_data.votes.collect_all()
                    } else {
                        Vec::new()
                    };

                    let cmd = DbWriteCommand::SlotData {
                        slot,
                        bank_hash: metadata.bank_hash_components.as_ref().map(|c| c.bank_hash.clone()).unwrap_or_default(),
                        parent_bank_hash: metadata.bank_hash_components.as_ref().map(|c| c.parent_bank_hash.clone()).unwrap_or_default(),
                        signature_count: metadata.bank_hash_components.as_ref().map(|c| c.signature_count).unwrap_or(0),
                        last_blockhash: metadata.bank_hash_components.as_ref().map(|c| c.last_blockhash.clone()).unwrap_or_default(),
                        delta_lthash: metadata.delta_lthash.unwrap_or_else(|| vec![0u8; 32]),
                        cumulative_lthash: metadata.cumulative_lthash.unwrap_or_else(|| vec![0u8; 32]),
                        vote_transactions: votes,
                        blockhash: metadata.blockhash,
                        parent_slot: metadata.parent_slot,
                        executed_transaction_count: metadata.executed_transaction_count,
                        entry_count: metadata.entry_count,
                    };

                    // Try send with backpressure handling
                    match db_writer.try_send(cmd) {
                        Ok(_) => {
                            self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
                            self.stats.db_writes_queued.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(TrySendError::Full(_)) => {
                            // Queue is full, put slot back and stop processing
                            self.pending_writes.push(slot);
                            info!("DB writer queue full, applying backpressure");
                            break;
                        }
                        Err(TrySendError::Disconnected(_)) => {
                            self.stats.db_writes_dropped.fetch_add(1, Ordering::Relaxed);
                            error!("DB writer disconnected");
                            return Err(GeyserPluginError::Custom("DB writer disconnected".into()));
                        }
                    }

                    // Send account changes if present
                    if slot_data.has_monitored_changes() {
                        if let Some(changes) = slot_data.account_changes.read().as_ref() {
                            let _ = db_writer.try_send(DbWriteCommand::AccountChanges {
                                slot,
                                changes: changes.clone(),
                            });
                            self.stats.slots_with_monitored_accounts.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            } else {
                // No more pending writes
                break;
            }
        }

        Ok(())
    }

    fn sync_monitored_accounts_from_db(&mut self, config: &PluginConfig) {
        use tokio_postgres::{NoTls, connect};
        
        // Use blocking runtime for sync context
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        
        let connection_string = config.connection_string.clone();
        let monitored_accounts = self.monitored_accounts.clone();
        
        rt.block_on(async move {
            match connect(&connection_string, NoTls).await {
                Ok((client, connection)) => {
                    // Spawn connection handler
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            error!("Database connection error: {}", e);
                        }
                    });
                    
                    match client.query("SELECT address FROM monitored_accounts WHERE active = true", &[]).await {
                        Ok(rows) => {
                            let mut count = 0;
                            for row in rows {
                                if let Ok(address) = row.try_get::<_, String>(0) {
                                    if let Ok(pubkey) = Pubkey::from_str(&address) {
                                        monitored_accounts.insert(pubkey);
                                        count += 1;
                                    }
                                }
                            }
                            info!("Synced {} monitored accounts from database", count);
                        }
                        Err(e) => {
                            warn!("Failed to sync monitored accounts from database: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to connect to database for account sync: {}", e);
                }
            }
        });
    }

    fn parse_vote_instruction(&self, transaction: &SanitizedTransaction) -> VoteDetails {
        #[allow(deprecated)]
        use solana_sdk::vote::instruction::VoteInstruction;
        
        let mut vote_details = VoteDetails {
            vote_type: "unknown".to_string(),
            vote_slot: None,
            vote_hash: None,
            root_slot: None,
            lockouts_count: None,
            timestamp: None,
        };
        
        #[allow(deprecated)]
        let vote_program_id = solana_sdk::vote::program::id();
        
        for (program_id, instruction) in transaction.message().program_instructions_iter() {
            if program_id == &vote_program_id {
                // Simplified vote parsing - just identify the type
                match bincode::deserialize::<VoteInstruction>(&instruction.data) {
                    Ok(VoteInstruction::Vote(_)) => {
                        vote_details.vote_type = "vote".to_string();
                    }
                    Ok(VoteInstruction::VoteSwitch(_, _)) => {
                        vote_details.vote_type = "vote_switch".to_string();
                    }
                    Ok(VoteInstruction::TowerSync(_)) => {
                        vote_details.vote_type = "tower_sync".to_string();
                    }
                    Ok(VoteInstruction::TowerSyncSwitch(_, _)) => {
                        vote_details.vote_type = "tower_sync_switch".to_string();
                    }
                    _ => {}
                }
            }
        }
        
        vote_details
    }

    fn process_transaction_info(
        &self,
        signature: &Signature,
        is_vote: bool,
        transaction: &SanitizedTransaction,
        transaction_status_meta: &TransactionStatusMeta,
        slot: u64,
    ) -> PluginResult<()> {
        if !is_vote {
            return Ok(());
        }

        let vote_pubkey = match transaction.message().account_keys().get(0) {
            Some(pubkey) => pubkey,
            None => return Ok(()),
        };

        let versioned_tx = transaction.to_versioned_transaction();

        let serialized_tx = match bincode::serialize(&versioned_tx) {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to serialize transaction: {}", e);
                return Ok(());
            }
        };

        let vote_details = self.parse_vote_instruction(transaction);
        
        let transaction_meta = serde_json::json!({
            "compute_units_consumed": transaction_status_meta.compute_units_consumed,
            "status": transaction_status_meta.status,
            "fee": transaction_status_meta.fee,
        });

        let vote_tx = VoteTransaction {
            voter_pubkey: vote_pubkey.to_string(),
            vote_signature: signature.to_string(),
            vote_transaction: serialized_tx,
            transaction_meta: Some(transaction_meta),
            vote_type: vote_details.vote_type.clone(),
            vote_slot: vote_details.vote_slot,
            vote_hash: vote_details.vote_hash.clone(),
            root_slot: vote_details.root_slot,
            lockouts_count: vote_details.lockouts_count,
            timestamp: vote_details.timestamp,
        };

        // Add to thread-local buffer (no locking!)
        LocalBuffer::add_vote_transaction(slot, vote_tx);

        // Update stats
        match vote_details.vote_type.as_str() {
            "tower_sync" | "tower_sync_switch" => {
                self.stats.tower_sync_count.fetch_add(1, Ordering::Relaxed);
            }
            "vote_switch" => {
                self.stats.vote_switch_count.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.stats.other_vote_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(())
    }
}

impl GeyserPlugin for TwineGeyserPlugin {
    fn name(&self) -> &'static str {
        "twine-geyser-plugin"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        info!("Twine Geyser Plugin - Starting on_load");
        info!("Loading Twine Geyser Plugin with config: {}", config_file);

        let config_str = std::fs::read_to_string(config_file).map_err(|e| {
            error!("Failed to read config file: {}", e);
            GeyserPluginError::ConfigFileReadError {
                msg: format!("Failed to read config file: {}", e),
            }
        })?;

        let config: PluginConfig = serde_json::from_str(&config_str).map_err(|e| {
            error!("Failed to parse config file: {}", e);
            error!("Config content: {}", config_str);
            GeyserPluginError::ConfigFileReadError {
                msg: format!("Failed to parse config file: {}", e),
            }
        })?;

        info!("Config parsed successfully");

        if let Some(ref log_file) = config.log_file {
            info!(
                "Configuring logging to file: {} with level: {}",
                log_file, config.log_level
            );
            crate::logging::init_configured_logging(Some(log_file), &config.log_level);
            info!("Logging reconfigured based on plugin config");
        }

        for account_str in &config.monitored_accounts {
            if let Ok(pubkey) = Pubkey::from_str(account_str) {
                self.monitored_accounts.insert(pubkey);
                info!("Added monitored account from config: {}", account_str);
            } else {
                warn!("Invalid pubkey in monitored_accounts: {}", account_str);
            }
        }

        self.sync_monitored_accounts_from_db(&config);

        let monitored_count = self.monitored_accounts.len();
        self.has_monitored_accounts.store(monitored_count > 0, Ordering::Relaxed);
        info!(
            "Plugin configured with {} monitored accounts",
            monitored_count
        );

        info!("Creating Tokio runtime");
        self.runtime = Some(Runtime::new().map_err(|e| {
            error!("Failed to create runtime: {}", e);
            GeyserPluginError::Custom(format!("Failed to create runtime: {}", e).into())
        })?);

        self.start_background_tasks();

        let (tx, rx) = bounded(config.max_queue_size);
        self.db_writer_queue = Some(tx);

        let worker_count = config.worker_pool_size;
        let worker_pool = WorkerPool::new(
            worker_count,
            rx,
            &config.connection_string,
            config.batch_size,
            100, // batch_timeout_ms: 100ms default
            self.stats.clone(),
        );
        self.worker_pool = Some(worker_pool);

        if let Some(ref api_config) = config.api_server {
            let (update_tx, update_rx) = bounded(10);
            self.account_update_receiver = Some(update_rx);
            
            let api_server = crate::api_server::ApiServer::new(
                api_config.clone(),
                self.monitored_accounts.clone(),
                update_tx,
            );
            
            // Spawn API server in a separate thread with its own runtime
            self.api_server = Some(std::thread::spawn(move || {
                let rt = Runtime::new().unwrap();
                rt.block_on(async move {
                    if let Err(e) = api_server.run().await {
                        error!("API server error: {}", e);
                    }
                });
            }));
            
            info!("API server started on {}", api_config.bind_address);
        }

        if let Some(ref metrics_config) = config.metrics_server {
            let stats = self.stats.clone();
            let runtime = self.runtime.as_ref().unwrap();
            
            // Parse port from bind address
            let port = if let Some(colon_pos) = metrics_config.bind_address.rfind(':') {
                metrics_config.bind_address[colon_pos + 1..].parse().unwrap_or(9090)
            } else {
                9090
            };
            
            // Create chain monitor if not exists
            if self.chain_monitor.is_none() {
                // Default to mainnet
                self.chain_monitor = Some(Arc::new(ChainMonitor::new(crate::airlock::types::NetworkMode::Mainnet)));
            }
            
            let chain_monitor = self.chain_monitor.as_ref().unwrap().clone();
            let metrics_server = MetricsServer::new(
                stats,
                chain_monitor,
                port,
            );
            
            self.metrics_server = Some(runtime.spawn(async move {
                if let Err(e) = metrics_server.run().await {
                    error!("Metrics server error: {}", e);
                }
            }));
            
            info!("Metrics server started on port {}", port);
        }

        self.stats.queue_capacity.store(config.max_queue_size, Ordering::Relaxed);
        self.stats.worker_pool_size.store(worker_count, Ordering::Relaxed);

        self.config = Some(config);

        info!("Twine Geyser Plugin loaded successfully");
        Ok(())
    }

    fn on_unload(&mut self) {
        info!("Unloading Twine Geyser Plugin");
        
        if let Some(worker_pool) = self.worker_pool.take() {
            worker_pool.shutdown();
        }
        
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(10));
        }
        
        info!("Twine Geyser Plugin unloaded");
    }

    fn notify_account_change(
        &self,
        account_change: OwnedAccountChange,
    ) -> PluginResult<()> {
        let slot = account_change.slot;
        
        // NOTE: We do NOT check if account is monitored here for performance reasons.
        // The node doesn't filter account updates - it sends all of them.
        // Filtering should be done during data processing/storage if needed.
        
        let slot_data = self.get_or_create_slot(slot)?;
        
        slot_data.lifecycle.inc_writers();

        // Pass the OwnedAccountChange directly - we take ownership of Agave's copy
        // Always treat as monitored=true to capture all account changes
        LocalBuffer::add_account_change(slot, account_change, true);
        slot_data.mark_monitored();

        // Update stats for all account changes
        self.stats.monitored_account_changes.fetch_add(1, Ordering::Relaxed);

        slot_data.lifecycle.dec_writers();

        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction_info: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> PluginResult<()> {
        match &transaction_info {
            ReplicaTransactionInfoVersions::V0_0_1(info) => {
                self.process_transaction_info(info.signature, info.is_vote, info.transaction, info.transaction_status_meta, slot)
            }
            ReplicaTransactionInfoVersions::V0_0_2(info) => {
                self.process_transaction_info(info.signature, info.is_vote, info.transaction, info.transaction_status_meta, slot)
            }
        }
    }

    fn update_slot_status(
        &self,
        slot: u64,
        _parent: Option<u64>,
        status: &SlotStatus,
    ) -> PluginResult<()> {
        self.stats.slot_status_updates.fetch_add(1, Ordering::Relaxed);

        if let Some(slot_data) = self.slots.get(&slot) {
            match status {
                SlotStatus::Processed => {
                    slot_data.lifecycle.transition_to(SlotState::Processed);
                    if let Some(monitor) = &self.chain_monitor {
                        monitor.update_validator_slot(slot);
                    }
                }
                SlotStatus::Confirmed => {
                    slot_data.lifecycle.transition_to(SlotState::Confirmed);
                }
                SlotStatus::Rooted => {
                    slot_data.lifecycle.transition_to(SlotState::Rooted);
                    if let Some(monitor) = &self.chain_monitor {
                        monitor.update_validator_slot(slot);
                    }
                }
                _ => {}
            }
        }

        self.process_pending_writes()?;

        Ok(())
    }

    fn notify_bank_hash_components(
        &self,
        components: agave_geyser_plugin_interface::geyser_plugin_interface::BankHashComponentsInfo,
    ) -> PluginResult<()> {
        let slot = components.slot;
        info!("Received bank hash components for slot {}", slot);
        
        let slot_data = self.get_or_create_slot(slot)?;
        
        slot_data.metadata.set_bank_hash(components);
        
        Ok(())
    }

    fn notify_block_metadata(
        &self,
        block_info: agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions,
    ) -> PluginResult<()> {
        match &block_info {
            agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions::V0_0_4(info) => {
                debug!("Block metadata V4 for slot {}", info.slot);
                
                self.stats.block_metadata_received.fetch_add(1, Ordering::Relaxed);
                
                let slot_data = self.get_or_create_slot(info.slot)?;
                
                slot_data.metadata.set_block_metadata(
                    info.blockhash.to_string(),
                    info.parent_slot,
                    info.executed_transaction_count,
                    info.entry_count,
                );
            }
            _ => {}
        }
        
        Ok(())
    }

    fn notify_slot_lthash(
        &self,
        slot: u64,
        delta_lthash: &solana_lattice_hash::lt_hash::LtHash,
        cumulative_lthash: &solana_lattice_hash::lt_hash::LtHash,
    ) -> PluginResult<()> {
        debug!("Received LtHash for slot {}", slot);
        
        let slot_data = self.get_or_create_slot(slot)?;
        
        // Convert u16 array to u8 bytes
        let delta_bytes: Vec<u8> = delta_lthash.0.iter()
            .flat_map(|&x| x.to_le_bytes())
            .collect();
        let cumulative_bytes: Vec<u8> = cumulative_lthash.0.iter()
            .flat_map(|&x| x.to_le_bytes())
            .collect();
        
        slot_data.metadata.set_lthash(delta_bytes, cumulative_bytes);
        
        Ok(())
    }

    fn account_change_notifications_enabled(&self) -> bool {
        true
    }

    fn slot_lthash_notifications_enabled(&self) -> bool {
        true
    }

    fn bank_hash_components_notifications_enabled(&self) -> bool {
        true
    }
    
    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        false
    }
}

impl Drop for TwineGeyserPlugin {
    fn drop(&mut self) {
        self.on_unload();
    }
}