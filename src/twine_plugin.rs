use crate::airlock::types::{
    AirlockSlotData, BankHashComponentsInfo, DbWriteCommand, LtHash, OwnedAccountChange,
    PluginConfig, ProofRequest, ReplicaBlockInfoVersions, VoteTransaction,
    EpochValidator,
};
use crate::airlock::{AirlockStats, AirlockStatsSnapshot};
use crate::stake::{self, StakeAccountInfo, slot_to_epoch, get_epoch_boundaries};
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, Result as PluginResult, SlotStatus,
    ReplicaTransactionInfoVersions,
};
use crossbeam_channel::{bounded, Sender};
use dashmap::{DashMap, DashSet};
use log::*;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::account::ReadableAccount;
use solana_transaction::sanitized::SanitizedTransaction;
use solana_transaction_status::TransactionStatusMeta;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use crate::chain_monitor::ChainMonitor;
use crate::metrics_server::MetricsServer;
use crate::worker_pool::WorkerPool;
use crate::api_server::ApiServerHandle;

#[derive(Debug)]
struct VoteDetails {
    vote_type: String,
    vote_slot: Option<u64>,
    vote_hash: Option<String>,
    root_slot: Option<u64>,
    lockouts_count: Option<u64>,
    timestamp: Option<i64>,
}

/// Main plugin state
#[derive(Debug)]
pub struct TwineGeyserPlugin {
    /// Primary buffer for all in-flight slot data
    airlock: Arc<DashMap<u64, Arc<AirlockSlotData>>>,
    /// Fast-lookup set of monitored pubkeys
    monitored_accounts: Arc<DashSet<Pubkey>>,
    /// Debouncing map for proof scheduling
    pending_proofs_for_scheduling: Arc<DashMap<Pubkey, u64>>,
    /// Cache for whether we have any monitored accounts
    has_monitored_accounts: Arc<AtomicBool>,
    /// Queue feeding the database worker pool
    db_writer_queue: Option<Sender<DbWriteCommand>>,
    /// Worker pool handle
    worker_pool: Option<WorkerPool>,
    /// Configuration
    config: Option<PluginConfig>,
    /// Last processed slot for proof scheduling
    last_proof_scheduling_slot: Arc<AtomicU64>,
    /// Proof scheduling service handle
    proof_scheduler: Option<tokio::task::JoinHandle<()>>,
    /// Runtime for async tasks
    runtime: Option<Runtime>,
    /// Statistics for monitoring
    stats: Arc<AirlockStats>,
    /// Last slot when stats were logged
    last_stats_log_slot: AtomicU64,
    /// Metrics server handle
    metrics_server: Option<tokio::task::JoinHandle<()>>,
    /// API server handle
    api_server: Option<ApiServerHandle>,
    /// Chain monitor
    chain_monitor: Option<Arc<ChainMonitor>>,
    /// Current epoch
    current_epoch: Arc<AtomicU64>,
    /// Accumulates stake states during epoch transitions
    epoch_stake_accumulator: Arc<DashMap<String, StakeAccountInfo>>,
    /// Queue for delayed cleanup of processed slots
    cleanup_queue: Arc<Mutex<VecDeque<(u64, Instant)>>>,
    /// Handle for cleanup task
    cleanup_task: Option<tokio::task::JoinHandle<()>>,
}

impl Default for TwineGeyserPlugin {
    fn default() -> Self {
        Self {
            airlock: Arc::new(DashMap::new()),
            monitored_accounts: Arc::new(DashSet::new()),
            pending_proofs_for_scheduling: Arc::new(DashMap::new()),
            has_monitored_accounts: Arc::new(AtomicBool::new(false)),
            db_writer_queue: None,
            worker_pool: None,
            config: None,
            last_proof_scheduling_slot: Arc::new(AtomicU64::new(0)),
            proof_scheduler: None,
            runtime: None,
            stats: Arc::new(AirlockStats::default()),
            last_stats_log_slot: AtomicU64::new(0),
            metrics_server: None,
            api_server: None,
            chain_monitor: None,
            current_epoch: Arc::new(AtomicU64::new(0)),
            epoch_stake_accumulator: Arc::new(DashMap::new()),
            cleanup_queue: Arc::new(Mutex::new(VecDeque::new())),
            cleanup_task: None,
        }
    }
}

impl GeyserPlugin for TwineGeyserPlugin {
    fn name(&self) -> &'static str {
        "twine-geyser-plugin"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        info!("Twine Geyser Plugin - Starting on_load");
        info!("Loading Twine Geyser Plugin with config: {}", config_file);

        // Load configuration
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

        // Configure logging based on config
        if let Some(ref log_file) = config.log_file {
            info!(
                "Configuring logging to file: {} with level: {}",
                log_file, config.log_level
            );
            crate::logging::init_configured_logging(Some(log_file), &config.log_level);
            info!("Logging reconfigured based on plugin config");
        }

        // Initialize monitored accounts from config
        for account_str in &config.monitored_accounts {
            if let Ok(pubkey) = Pubkey::from_str(account_str) {
                self.monitored_accounts.insert(pubkey);
                info!("Added monitored account from config: {}", account_str);
            } else {
                warn!("Invalid pubkey in monitored_accounts: {}", account_str);
            }
        }

        // Sync monitored accounts from database
        self.sync_monitored_accounts_from_db(&config);

        let monitored_count = self.monitored_accounts.len();
        self.has_monitored_accounts.store(monitored_count > 0, Ordering::Relaxed);
        info!(
            "Plugin configured with {} monitored accounts",
            monitored_count
        );

        // Start runtime
        info!("Creating Tokio runtime");
        self.runtime = Some(Runtime::new().map_err(|e| {
            error!("Failed to create runtime: {}", e);
            GeyserPluginError::Custom(format!("Failed to create runtime: {}", e).into())
        })?);
        info!("Tokio runtime created successfully");

        // Start worker pool
        info!("Starting worker pool");
        let (tx, rx) = bounded(config.max_queue_size);
        self.db_writer_queue = Some(tx.clone());
        self.worker_pool = Some(WorkerPool::start(rx, config.clone(), self.stats.clone()));
        info!("Worker pool started");

        // Set worker pool size and queue capacity in stats
        self.stats
            .worker_pool_size
            .store(config.num_worker_threads, Ordering::Relaxed);
        self.stats
            .queue_capacity
            .store(config.max_queue_size, Ordering::Relaxed);

        // Start queue depth monitoring
        info!("Starting queue depth monitoring");
        let stats = self.stats.clone();
        let queue_tx = tx.clone();
        if let Some(runtime) = self.runtime.as_ref() {
            runtime.spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    stats.queue_depth.store(queue_tx.len(), Ordering::Relaxed);
                }
            });
        }

        // Initialize chain monitor
        info!("Initializing chain monitor");
        let chain_monitor = Arc::new(ChainMonitor::new(config.network_mode.clone()));
        self.chain_monitor = Some(chain_monitor.clone());

        // Start network monitoring
        info!("Starting network monitoring");
        let monitor = chain_monitor.clone();
        if let Some(runtime) = self.runtime.as_ref() {
            runtime.spawn(async move {
                monitor.start_network_monitoring().await;
            });
        }

        // Store config
        self.config = Some(config);

        // Start proof scheduling service
        info!("Starting proof scheduler");
        self.start_proof_scheduler();

        // Start metrics server
        info!("Starting metrics server");
        self.start_metrics_server();
        
        // Start cleanup task
        info!("Starting cleanup task");
        self.start_cleanup_task();
        
        // Start API server if configured
        if self.config.as_ref().and_then(|c| c.api_port).is_some() {
            info!("Starting API server");
            self.start_api_server();
        }

        info!("Twine Geyser Plugin on_load completed successfully");
        Ok(())
    }

    fn on_unload(&mut self) {
        info!("Unloading Twine Geyser Plugin");

        // Send shutdown signal
        if let Some(queue) = &self.db_writer_queue {
            let _ = queue.send(DbWriteCommand::Shutdown);
        }

        // Stop proof scheduler
        if let Some(handle) = self.proof_scheduler.take() {
            handle.abort();
        }

        // Stop metrics server
        if let Some(handle) = self.metrics_server.take() {
            handle.abort();
        }
        
        // Stop cleanup task
        if let Some(handle) = self.cleanup_task.take() {
            handle.abort();
        }
        
        // Stop API server
        if let Some(handle) = self.api_server.take() {
            handle.shutdown();
        }


        // Wait for workers to finish
        if let Some(pool) = self.worker_pool.take() {
            pool.join();
        }

        // Shutdown runtime
        if let Some(runtime) = self.runtime.take() {
            // Give tasks a moment to complete
            runtime.shutdown_timeout(std::time::Duration::from_secs(5));
        }
    }

    fn notify_account_change(&self, account_change: OwnedAccountChange) -> PluginResult<()> {
        debug!("Received account change for slot {}, ", account_change.slot);
        let slot = account_change.slot;
        let current_epoch = slot_to_epoch(slot);
        let stored_epoch = self.current_epoch.load(Ordering::Relaxed);

        // Check if this is a stake account
        let is_stake_account = stake::is_stake_account(account_change.new_account.owner());

        // Handle epoch boundary detection for stake accounts
        if current_epoch > stored_epoch && is_stake_account {
            debug!("First stake account in new epoch {} at slot {}", current_epoch, slot);
            
            // Process validator set for the new epoch
            self.compute_and_store_epoch_validators(stored_epoch, slot);
            self.current_epoch.store(current_epoch, Ordering::Relaxed);
            
            // Clear and rebuild stake accumulator
            self.epoch_stake_accumulator.clear();
            
            // Process this stake account
            if let Ok(stake_state) = stake::deserialize_stake_state(account_change.new_account.data()) {
                let stake_info = StakeAccountInfo::from_state(&account_change.pubkey, &stake_state);
                self.epoch_stake_accumulator.insert(
                    account_change.pubkey.to_string(),
                    stake_info
                );
            }
        } else if is_stake_account && current_epoch == stored_epoch {
            // Continue accumulating stakes for current epoch
            if let Ok(stake_state) = stake::deserialize_stake_state(account_change.new_account.data()) {
                let stake_info = StakeAccountInfo::from_state(&account_change.pubkey, &stake_state);
                self.epoch_stake_accumulator.insert(
                    account_change.pubkey.to_string(),
                    stake_info
                );
            }
        }

        // Try to get existing slot data first to avoid lock contention
        let slot_data = if let Some(existing) = self.airlock.get(&slot) {
            existing.clone()
        } else {
            self.airlock
                .entry(slot)
                .or_insert_with(|| Arc::new(AirlockSlotData::new()))
                .clone()
        };

        // Atomically increment writer count
        slot_data.writer_count.fetch_add(1, Ordering::AcqRel);

        // Quick check: if no monitored accounts at all, skip expensive lookup
        if !self.has_monitored_accounts.load(Ordering::Relaxed) {
            // Still need to buffer the change
            let thread_id = std::thread::current().id();
            slot_data.thread_buffers.entry(thread_id)
                .or_insert_with(Vec::new)
                .push(account_change);
            slot_data.writer_count.fetch_sub(1, Ordering::AcqRel);
            return Ok(());
        }
        
        // Check if this is a monitored account
        let pubkey = account_change.pubkey;
        let is_monitored = self.monitored_accounts.contains(&pubkey);
        
        // Get or create thread-local buffer for this slot
        let thread_id = std::thread::current().id();
        let mut thread_buffer = slot_data.thread_buffers.entry(thread_id)
            .or_insert_with(Vec::new);
        thread_buffer.push(account_change);
        
        // Mark slot if it contains a monitored change
        if is_monitored {
            slot_data
                .contains_monitored_change
                .store(true, Ordering::Release);
            self.pending_proofs_for_scheduling
                .insert(pubkey, slot);
            self.stats
                .monitored_account_changes
                .fetch_add(1, Ordering::Relaxed);
        }

        // Atomically decrement writer count
        slot_data.writer_count.fetch_sub(1, Ordering::AcqRel);

        Ok(())
    }

    fn notify_slot_lthash(
        &self,
        slot: u64,
        delta_lthash: &LtHash,
        cumulative_lthash: &LtHash,
    ) -> PluginResult<()> {
        debug!("Received LtHash for slot {}", slot);

        // Convert LtHash to bytes
        let delta_bytes: Vec<u8> = delta_lthash
            .0
            .iter()
            .flat_map(|&x| x.to_le_bytes())
            .collect();
        let cumulative_bytes: Vec<u8> = cumulative_lthash
            .0
            .iter()
            .flat_map(|&x| x.to_le_bytes())
            .collect();

        // Try to get existing slot data first to avoid lock contention
        let slot_data = if let Some(existing) = self.airlock.get(&slot) {
            existing.clone()
        } else {
            self.airlock
                .entry(slot)
                .or_insert_with(|| Arc::new(AirlockSlotData::new()))
                .clone()
        };

        // Atomically increment writer count while we update
        slot_data.writer_count.fetch_add(1, Ordering::AcqRel);

        // Update LtHash data
        *slot_data.delta_lthash.write() = Some(delta_bytes);
        *slot_data.cumulative_lthash.write() = Some(cumulative_bytes);
        
        debug!("Updated airlock slot {} with LtHash data", slot);

        // Skip immediate write attempt - let rooted handler do it

        // Atomically decrement writer count
        slot_data.writer_count.fetch_sub(1, Ordering::AcqRel);

        // Skip expensive len() calculation in hot path

        Ok(())
    }

    fn notify_bank_hash_components(&self, components: BankHashComponentsInfo) -> PluginResult<()> {
        let slot = components.slot;

        debug!("Received bank hash components for slot {}", slot);

        // Try to get existing slot data first to avoid lock contention
        let slot_data = if let Some(existing) = self.airlock.get(&slot) {
            existing.clone()
        } else {
            self.airlock
                .entry(slot)
                .or_insert_with(|| Arc::new(AirlockSlotData::new()))
                .clone()
        };

        // Atomically increment writer count while we update
        slot_data.writer_count.fetch_add(1, Ordering::AcqRel);

        // Update bank hash components
        *slot_data.bank_hash_components.write() = Some(components);
        
        debug!("Updated airlock slot {} with bank hash components", slot);

        // Skip immediate write attempt - let rooted handler do it

        // Atomically decrement writer count
        slot_data.writer_count.fetch_sub(1, Ordering::AcqRel);

        // Skip expensive len() calculation in hot path

        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        _parent: Option<u64>,
        status: &SlotStatus,
    ) -> PluginResult<()> {
        if let SlotStatus::Rooted = status {
            self.handle_rooted_slot(slot)?;
        } else if let SlotStatus::Processed = status {
            self.handle_processed_slot(slot)?;
        } else if let SlotStatus::Confirmed = status {
            self.handle_confirmed_slot(slot)?;
        } else if let SlotStatus::FirstShredReceived = status {
            self.handle_first_shred_received_slot(slot)?;
        } else if let SlotStatus::Completed = status {
            self.handle_completed_slot(slot)?;
        }

        Ok(())
    }

    fn notify_block_metadata(&self, block_info: ReplicaBlockInfoVersions) -> PluginResult<()> {
        match &block_info {
            ReplicaBlockInfoVersions::V0_0_1(_) => {}
            ReplicaBlockInfoVersions::V0_0_2(_) => {}
            ReplicaBlockInfoVersions::V0_0_3(_) => {}
            ReplicaBlockInfoVersions::V0_0_4(info) => {
                debug!("Block metadata V4 for slot {}", info.slot);

                // Increment block metadata counter
                self.stats
                    .block_metadata_received
                    .fetch_add(1, Ordering::Relaxed);

                // Get or create airlock slot data
                let slot_data = self
                    .airlock
                    .entry(info.slot)
                    .or_insert_with(|| Arc::new(AirlockSlotData::new()))
                    .clone();

                // Atomically increment writer count while we update
                slot_data.writer_count.fetch_add(1, Ordering::AcqRel);

                // Update block metadata
                *slot_data.blockhash.write() = Some(info.blockhash.to_string());
                *slot_data.parent_slot.write() = Some(info.parent_slot);
                *slot_data.executed_transaction_count.write() = Some(info.executed_transaction_count);
                *slot_data.entry_count.write() = Some(info.entry_count);
                
                debug!("Updated airlock slot {} with block metadata", info.slot);

                // Skip immediate write attempt - let rooted handler do it

                // Atomically decrement writer count
                slot_data.writer_count.fetch_sub(1, Ordering::AcqRel);

                // Update stats
                self.stats
                    .active_slots
                    .store(self.airlock.len(), Ordering::Relaxed);
            }
        }
        Ok(())
    }
    
    fn notify_transaction(&self, transaction_info: ReplicaTransactionInfoVersions, slot: u64) -> PluginResult<()> {
        match &transaction_info {
            ReplicaTransactionInfoVersions::V0_0_1(info) => {
                self.process_transaction_info(info.signature, info.is_vote, info.transaction, info.transaction_status_meta, slot)
            }
            ReplicaTransactionInfoVersions::V0_0_2(info) => {
                self.process_transaction_info(info.signature, info.is_vote, info.transaction, info.transaction_status_meta, slot)
            }
        }
    }

    // Enable the enhanced notifications
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
}

impl TwineGeyserPlugin {
    fn process_transaction_info(
        &self,
        signature: &Signature,
        is_vote: bool,
        transaction: &SanitizedTransaction,
        transaction_status_meta: &TransactionStatusMeta,
        slot: u64,
    ) -> PluginResult<()> {
        // Only process vote transactions
        if !is_vote {
            return Ok(());
        }

        // Get the vote account from the transaction
        let vote_pubkey = match transaction.message().account_keys().get(0) {
            Some(pubkey) => pubkey,
            None => return Ok(()),
        };

        // Get the versioned transaction to serialize
        // We need the full transaction data to verify the signature later
        let versioned_tx = transaction.to_versioned_transaction();
        let serialized_tx = match bincode::serialize(&versioned_tx) {
            Ok(data) => data,
            Err(e) => {
                warn!("Failed to serialize vote transaction: {}", e);
                return Ok(());
            }
        };

        // Parse vote instruction details
        let vote_details = self.parse_vote_instruction(transaction);

        // Create transaction metadata
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

        // Try to get existing slot data first to avoid lock contention
        let slot_data = if let Some(existing) = self.airlock.get(&slot) {
            existing.clone()
        } else {
            self.airlock
                .entry(slot)
                .or_insert_with(|| Arc::new(AirlockSlotData::new()))
                .clone()
        };

        // Add vote transaction to slot
        slot_data.vote_transactions.write().push(vote_tx);

        // Update stats
        self.stats.vote_transactions_total.fetch_add(1, Ordering::Relaxed);
        match vote_details.vote_type.as_str() {
            "tower_sync" | "tower_sync_switch" => {
                self.stats.tower_sync_count.fetch_add(1, Ordering::Relaxed);
            }
            "compact_vote_state_update" | "compact_vote_state_update_switch" => {
                self.stats.compact_vote_count.fetch_add(1, Ordering::Relaxed);
            }
            "vote" | "vote_switch" | "vote_state_update" | "vote_state_update_switch" => {
                self.stats.vote_switch_count.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.stats.other_vote_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        debug!(
            "Recorded {} vote transaction for slot {} from validator {} (voting on slot {:?})",
            vote_details.vote_type, slot, vote_pubkey, vote_details.vote_slot
        );

        Ok(())
    }
    
    fn parse_vote_instruction(&self, transaction: &SanitizedTransaction) -> VoteDetails {
        use solana_sdk::vote::{self, instruction::VoteInstruction};
        
        let mut vote_details = VoteDetails {
            vote_type: "unknown".to_string(),
            vote_slot: None,
            vote_hash: None,
            root_slot: None,
            lockouts_count: None,
            timestamp: None,
        };
        
        // Get the vote program ID
        let vote_program_id = vote::program::id();
        
        // Find vote instructions in the transaction
        for (program_id, instruction) in transaction.message().program_instructions_iter() {
            if program_id == &vote_program_id {
                // Try to decode the vote instruction
                match bincode::deserialize::<VoteInstruction>(&instruction.data) {
                    Ok(VoteInstruction::Vote(vote)) => {
                        vote_details.vote_type = "vote".to_string();
                        if let Some(last_vote) = vote.slots.last() {
                            vote_details.vote_slot = Some(*last_vote);
                        }
                        vote_details.vote_hash = Some(vote.hash.to_string());
                        vote_details.lockouts_count = Some(vote.slots.len() as u64);
                        vote_details.timestamp = vote.timestamp;
                    }
                    Ok(VoteInstruction::VoteSwitch(vote_switch, _)) => {
                        vote_details.vote_type = "vote_switch".to_string();
                        if let Some(last_vote) = vote_switch.slots.last() {
                            vote_details.vote_slot = Some(*last_vote);
                        }
                        vote_details.vote_hash = Some(vote_switch.hash.to_string());
                        vote_details.lockouts_count = Some(vote_switch.slots.len() as u64);
                        vote_details.timestamp = vote_switch.timestamp;
                    }
                    Ok(VoteInstruction::UpdateVoteState(vote_state_update)) => {
                        vote_details.vote_type = "update_vote_state".to_string();
                        if let Some(last_vote) = vote_state_update.lockouts.back() {
                            vote_details.vote_slot = Some(last_vote.slot());
                        }
                        vote_details.vote_hash = Some(vote_state_update.hash.to_string());
                        vote_details.root_slot = vote_state_update.root;
                        vote_details.lockouts_count = Some(vote_state_update.lockouts.len() as u64);
                        vote_details.timestamp = vote_state_update.timestamp;
                    }
                    Ok(VoteInstruction::UpdateVoteStateSwitch(vote_state_update, _)) => {
                        vote_details.vote_type = "update_vote_state_switch".to_string();
                        if let Some(last_vote) = vote_state_update.lockouts.back() {
                            vote_details.vote_slot = Some(last_vote.slot());
                        }
                        vote_details.vote_hash = Some(vote_state_update.hash.to_string());
                        vote_details.root_slot = vote_state_update.root;
                        vote_details.lockouts_count = Some(vote_state_update.lockouts.len() as u64);
                        vote_details.timestamp = vote_state_update.timestamp;
                    }
                    Ok(VoteInstruction::CompactUpdateVoteState(compact_vote_state_update)) => {
                        vote_details.vote_type = "compact_update_vote_state".to_string();
                        if let Some(last_vote) = compact_vote_state_update.lockouts.back() {
                            vote_details.vote_slot = Some(last_vote.slot());
                        }
                        vote_details.vote_hash = Some(compact_vote_state_update.hash.to_string());
                        vote_details.root_slot = compact_vote_state_update.root;
                        vote_details.lockouts_count = Some(compact_vote_state_update.lockouts.len() as u64);
                        vote_details.timestamp = compact_vote_state_update.timestamp;
                    }
                    Ok(VoteInstruction::CompactUpdateVoteStateSwitch(compact_vote_state_update, _)) => {
                        vote_details.vote_type = "compact_update_vote_state_switch".to_string();
                        if let Some(last_vote) = compact_vote_state_update.lockouts.back() {
                            vote_details.vote_slot = Some(last_vote.slot());
                        }
                        vote_details.vote_hash = Some(compact_vote_state_update.hash.to_string());
                        vote_details.root_slot = compact_vote_state_update.root;
                        vote_details.lockouts_count = Some(compact_vote_state_update.lockouts.len() as u64);
                        vote_details.timestamp = compact_vote_state_update.timestamp;
                    }
                    Ok(VoteInstruction::TowerSync(tower_sync)) => {
                        vote_details.vote_type = "tower_sync".to_string();
                        if let Some(last_vote) = tower_sync.lockouts.back() {
                            vote_details.vote_slot = Some(last_vote.slot());
                        }
                        vote_details.vote_hash = Some(tower_sync.hash.to_string());
                        vote_details.root_slot = tower_sync.root;
                        vote_details.lockouts_count = Some(tower_sync.lockouts.len() as u64);
                        vote_details.timestamp = tower_sync.timestamp;
                    }
                    Ok(VoteInstruction::TowerSyncSwitch(tower_sync, _)) => {
                        vote_details.vote_type = "tower_sync_switch".to_string();
                        if let Some(last_vote) = tower_sync.lockouts.back() {
                            vote_details.vote_slot = Some(last_vote.slot());
                        }
                        vote_details.vote_hash = Some(tower_sync.hash.to_string());
                        vote_details.root_slot = tower_sync.root;
                        vote_details.lockouts_count = Some(tower_sync.lockouts.len() as u64);
                        vote_details.timestamp = tower_sync.timestamp;
                    }
                    Ok(_) => {
                        // Other vote instructions (Authorize, InitializeAccount, etc.)
                        vote_details.vote_type = "other".to_string();
                    }
                    Err(e) => {
                        debug!("Failed to parse vote instruction: {}", e);
                    }
                }
                
                // We found and processed a vote instruction, so we can break
                break;
            }
        }
        
        vote_details
    }
    
    fn try_write_complete_slot(&self, slot: u64) -> PluginResult<()> {
        // Check if slot exists and has complete data
        if let Some(slot_data_ref) = self.airlock.get(&slot) {
            let slot_data = slot_data_ref.value();
            if slot_data.has_complete_data() && slot_data.is_rooted() {
                // Clone the Arc instead of removing immediately
                let slot_data_clone = slot_data.clone();
                
                // Drop the reference to avoid holding lock
                drop(slot_data_ref);
                
                // Wait for all writers to finish
                while slot_data_clone.writer_count.load(Ordering::Acquire) > 0 {
                    std::hint::spin_loop();
                }
                
                // Write to database without removing from airlock
                self.write_slot_to_database(slot, slot_data_clone)?;
                
                // Add to cleanup queue for delayed removal
                if let Ok(mut queue) = self.cleanup_queue.lock() {
                    queue.push_back((slot, Instant::now()));
                }
            }
            else {
                warn!("Slot {} has incomplete data, skipping write", slot);
            }
        }
        
        Ok(())
    }

    fn write_slot_to_database(&self, slot: u64, slot_data: Arc<AirlockSlotData>) -> PluginResult<()> {
        let queue = self
            .db_writer_queue
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom("Plugin not initialized".into()))?;

        // Extract data from airlock
        let components = slot_data.bank_hash_components.read().clone()
            .unwrap_or_else(|| BankHashComponentsInfo {
                slot,
                bank_hash: String::new(),
                parent_bank_hash: String::new(),
                signature_count: 0,
                last_blockhash: String::new(),
                accounts_delta_hash: None,
                accounts_lthash_checksum: None,
                epoch_accounts_hash: None,
            });

        let delta_lthash = slot_data.delta_lthash.read().clone().unwrap_or_default();
        let cumulative_lthash = slot_data.cumulative_lthash.read().clone().unwrap_or_default();
        let blockhash = slot_data.blockhash.read().clone();
        let parent_slot = slot_data.parent_slot.read().clone();
        let executed_transaction_count = slot_data.executed_transaction_count.read().clone();
        let entry_count = slot_data.entry_count.read().clone();
        let vote_transactions = slot_data.vote_transactions.read().clone();

        debug!(
            "Writing complete slot {} data to DB: bank_hash={}, lthash_len={}, votes={}, status={}",
            slot,
            components.bank_hash,
            delta_lthash.len(),
            vote_transactions.len(),
            slot_data.status.read()
        );

        // Send slot data
        queue
            .send(DbWriteCommand::SlotData {
                slot,
                bank_hash: components.bank_hash,
                parent_bank_hash: components.parent_bank_hash,
                signature_count: components.signature_count,
                last_blockhash: components.last_blockhash,
                delta_lthash,
                cumulative_lthash,
                accounts_delta_hash: components.accounts_delta_hash,
                accounts_lthash_checksum: components.accounts_lthash_checksum,
                epoch_accounts_hash: components.epoch_accounts_hash,
                blockhash,
                parent_slot,
                executed_transaction_count,
                entry_count,
                vote_transactions,
            })
            .map_err(|_| GeyserPluginError::Custom("Failed to send slot data".into()))?;

        self.stats.db_writes.fetch_add(1, Ordering::Relaxed);

        // Handle account changes - save ALL changes if any monitored account changed
        if slot_data.contains_monitored_change.load(Ordering::Acquire) {
            debug!("Monitored account change detected, saving ALL account changes for slot {}", slot);
            
            // Consolidate all thread buffers
            let mut all_changes = Vec::new();
            for entry in slot_data.thread_buffers.iter() {
                all_changes.extend_from_slice(entry.value());
            }
            slot_data.thread_buffers.clear();
            
            if !all_changes.is_empty() {
                let change_count = all_changes.len();
                let monitored_count = all_changes.iter()
                    .filter(|c| self.monitored_accounts.contains(&c.pubkey))
                    .count();
                    
                queue
                    .send(DbWriteCommand::AccountChanges {
                        slot,
                        changes: all_changes,
                    })
                    .map_err(|_| GeyserPluginError::Custom("Failed to send account changes".into()))?;
                
                self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .slots_with_monitored_accounts
                    .fetch_add(1, Ordering::Relaxed);
                debug!("Saved {} account changes for slot {} ({} monitored)", change_count, slot, monitored_count);
            }
        } else {
            // No monitored changes - clear all thread buffers
            let mut discarded_count = 0;
            for mut entry in slot_data.thread_buffers.iter_mut() {
                discarded_count += entry.value().len();
                entry.value_mut().clear();
            }
            slot_data.thread_buffers.clear();
            if discarded_count > 0 {
                debug!("Discarded {} account changes for slot {} (no monitored accounts)", discarded_count, slot);
            }
        }

        // Update stats
        self.stats.sealed_slots.fetch_add(1, Ordering::Relaxed);
        self.stats
            .active_slots
            .store(self.airlock.len(), Ordering::Relaxed);

        Ok(())
    }
    
    fn handle_rooted_slot(&self, slot: u64) -> PluginResult<()> {
        debug!("Rooted slot: {}", slot);
        // Update validator slot in chain monitor
        if let Some(monitor) = &self.chain_monitor {
            monitor.update_validator_slot(slot);
        }

        // Skip cleanup - let the cleanup task handle it asynchronously
        // This was blocking the geyser thread

        let queue = self
            .db_writer_queue
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom("Plugin not initialized".into()))?;

        // Try to get existing slot data first to avoid lock contention
        let slot_data = if let Some(existing) = self.airlock.get(&slot) {
            existing.clone()
        } else {
            self.airlock
                .entry(slot)
                .or_insert_with(|| Arc::new(AirlockSlotData::new()))
                .clone()
        };

        // Update slot status to rooted and record current slot
        *slot_data.status.write() = "rooted".to_string();
        *slot_data.rooted_at_slot.write() = Some(slot);

        // Send status update
        queue
            .send(DbWriteCommand::SlotStatusUpdate {
                slot,
                status: "rooted".to_string(),
            })
            .map_err(|_| GeyserPluginError::Custom("Failed to send slot status update".into()))?;

        self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .slot_status_updates
            .fetch_add(1, Ordering::Relaxed);

        // Check if we have complete data
        if slot_data.has_complete_data() {
            info!(
                "Slot {} rooted with complete data, writing to database",
                slot
            );
            self.try_write_complete_slot(slot)?;
        } else {
            // Log what we're missing with detailed information
            let bank_hash_components = slot_data.bank_hash_components.read();
            let has_bank_hash = bank_hash_components.as_ref()
                .map(|c| !c.bank_hash.is_empty())
                .unwrap_or(false);
            let bank_hash_details = if has_bank_hash {
                bank_hash_components.as_ref()
                    .map(|c| format!("present ({})", c.bank_hash))
                    .unwrap_or_else(|| "present but empty".to_string())
            } else {
                "MISSING".to_string()
            };
            drop(bank_hash_components);
            
            let has_delta = slot_data.delta_lthash.read().is_some();
            let has_cumulative = slot_data.cumulative_lthash.read().is_some();
            let has_blockhash = slot_data.blockhash.read().is_some();
            let has_parent_slot = slot_data.parent_slot.read().is_some();
            let has_executed_transaction_count = slot_data.executed_transaction_count.read().is_some();
            let has_entry_count = slot_data.entry_count.read().is_some();
            
            // Log missing data counter
            self.stats
                .airlock_missing_data_slots
                .fetch_add(1, Ordering::Relaxed);
            
            warn!(
                "Slot {} rooted but INCOMPLETE - bank_hash: {}, delta_lthash: {}, cumulative_lthash: {}, blockhash: {}, parent_slot: {}, executed_transaction_count: {}, entry_count: {}, keeping in airlock",
                slot, 
                bank_hash_details,
                if has_delta { "present" } else { "MISSING" },
                if has_cumulative { "present" } else { "MISSING" },
                if has_blockhash { "present" } else { "MISSING" },
                if has_parent_slot { "present" } else { "MISSING" },
                if has_executed_transaction_count { "present" } else { "MISSING" },
                if has_entry_count { "present" } else { "MISSING" }
            );
        }

        // Skip expensive len() calculation in hot path

        // Log stats every 100 rooted slots
        let last_log = self.last_stats_log_slot.load(Ordering::Relaxed);
        if slot >= last_log + 100 {
            self.log_stats();
            self.last_stats_log_slot.store(slot, Ordering::Relaxed);
        }

        Ok(())
    }

    fn handle_processed_slot(&self, slot: u64) -> PluginResult<()> {
        // Try to get existing slot data first to avoid lock contention
        let slot_data = if let Some(existing) = self.airlock.get(&slot) {
            existing.clone()
        } else {
            self.airlock
                .entry(slot)
                .or_insert_with(|| Arc::new(AirlockSlotData::new()))
                .clone()
        };

        // Update slot status
        *slot_data.status.write() = "processed".to_string();

        let queue = self
            .db_writer_queue
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom("Plugin not initialized".into()))?;

        queue
            .send(DbWriteCommand::SlotStatusUpdate {
                slot,
                status: "processed".to_string(),
            })
            .map_err(|_| GeyserPluginError::Custom("Failed to send slot status update".into()))?;

        self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .slot_status_updates
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn handle_confirmed_slot(&self, slot: u64) -> PluginResult<()> {
        let queue = self
            .db_writer_queue
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom("Plugin not initialized".into()))?;

        queue
            .send(DbWriteCommand::SlotStatusUpdate {
                slot,
                status: "confirmed".to_string(),
            })
            .map_err(|_| GeyserPluginError::Custom("Failed to send slot status update".into()))?;

        self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .slot_status_updates
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn handle_first_shred_received_slot(&self, slot: u64) -> PluginResult<()> {
        let queue = self
            .db_writer_queue
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom("Plugin not initialized".into()))?;

        queue
            .send(DbWriteCommand::SlotStatusUpdate {
                slot,
                status: "first_shred_received".to_string(),
            })
            .map_err(|_| GeyserPluginError::Custom("Failed to send slot status update".into()))?;

        self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .slot_status_updates
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn handle_completed_slot(&self, slot: u64) -> PluginResult<()> {
        let queue = self
            .db_writer_queue
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom("Plugin not initialized".into()))?;

        queue
            .send(DbWriteCommand::SlotStatusUpdate {
                slot,
                status: "completed".to_string(),
            })
            .map_err(|_| GeyserPluginError::Custom("Failed to send slot status update".into()))?;

        self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .slot_status_updates
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn start_proof_scheduler(&mut self) {
        let pending_proofs = self.pending_proofs_for_scheduling.clone();
        let db_queue = match self.db_writer_queue.as_ref() {
            Some(queue) => queue.clone(),
            None => {
                error!("Cannot start proof scheduler: db_writer_queue not initialized");
                return;
            }
        };
        let interval = match self.config.as_ref() {
            Some(config) => config.proof_scheduling_slot_interval,
            None => {
                error!("Cannot start proof scheduler: config not initialized");
                return;
            }
        };
        let _last_slot = self.last_proof_scheduling_slot.clone();
        let stats = self.stats.clone();

        let runtime = match self.runtime.as_ref() {
            Some(rt) => rt,
            None => {
                error!("Cannot start proof scheduler: runtime not initialized");
                return;
            }
        };

        let handle = runtime.spawn(async move {
            let mut interval_timer = tokio::time::interval(
                tokio::time::Duration::from_millis(400 * interval), // ~400ms per slot
            );

            loop {
                interval_timer.tick().await;

                // Drain pending proofs
                let mut requests = Vec::new();
                let current_entries: Vec<_> = pending_proofs
                    .iter()
                    .map(|entry| (*entry.key(), *entry.value()))
                    .collect();

                for (pubkey, slot) in current_entries {
                    pending_proofs.remove(&pubkey);
                    requests.push(ProofRequest {
                        slot,
                        account_pubkey: pubkey.to_string(),
                    });
                }

                if !requests.is_empty() {
                    let count = requests.len();
                    let _ = db_queue.send(DbWriteCommand::ProofRequests { requests });
                    stats
                        .proof_requests_generated
                        .fetch_add(count, Ordering::Relaxed);
                    stats.db_writes.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        self.proof_scheduler = Some(handle);
    }

    fn log_stats(&self) {
        let snapshot = self.get_stats_snapshot();
        info!(
            "Twine Geyser Plugin Stats - Total Updates: {}, Sealed Slots: {}, Active Slots: {}, \
             Monitored Account Changes: {}, Slots with Monitored Accounts: {}, \
             Proof Requests: {}, DB Writes: {}",
            snapshot.total_updates,
            snapshot.sealed_slots,
            snapshot.active_slots,
            snapshot.monitored_account_changes,
            snapshot.slots_with_monitored_accounts,
            snapshot.proof_requests_generated,
            snapshot.db_writes
        );
    }

    fn get_stats_snapshot(&self) -> AirlockStatsSnapshot {
        let mut snapshot = self.stats.snapshot();
        snapshot.monitored_accounts = self.monitored_accounts.len();
        snapshot
    }

    fn start_metrics_server(&mut self) {
        let stats = self.stats.clone();
        let chain_monitor = match self.chain_monitor.as_ref() {
            Some(monitor) => monitor.clone(),
            None => {
                error!("Cannot start metrics server: chain_monitor not initialized");
                return;
            }
        };
        let port = match self.config.as_ref() {
            Some(config) => config.metrics_port,
            None => {
                error!("Cannot start metrics server: config not initialized");
                return;
            }
        };

        let runtime = match self.runtime.as_ref() {
            Some(rt) => rt,
            None => {
                error!("Cannot start metrics server: runtime not initialized");
                return;
            }
        };

        let handle = runtime.spawn(async move {
            let server = MetricsServer::new(stats, chain_monitor, port);
            if let Err(e) = server.run().await {
                error!("Metrics server error: {}", e);
            }
        });

        self.metrics_server = Some(handle);
    }
    
    fn start_cleanup_task(&mut self) {
        let airlock = self.airlock.clone();
        let cleanup_queue = self.cleanup_queue.clone();
        let stats = self.stats.clone();
        let db_writer_queue = self.db_writer_queue.clone();
        
        let runtime = match self.runtime.as_ref() {
            Some(r) => r,
            None => {
                error!("Cannot start cleanup task: runtime not available");
                return;
            }
        };
        
        let handle = runtime.spawn(async move {
            // Run cleanup every 30 seconds
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                
                // Process cleanup queue
                let mut slots_to_remove = Vec::new();
                let mut incomplete_slots_to_write = Vec::new();
                let current_time = Instant::now();
                
                // Get current network slot for grace period calculation
                let current_network_slot = stats.last_db_batch_slot.load(Ordering::Relaxed) as u64;
                
                // Check for incomplete rooted slots that have exceeded grace period (32 slots)
                for entry in airlock.iter() {
                    let slot = *entry.key();
                    let slot_data = entry.value();
                    
                    if let Some(rooted_at_slot) = *slot_data.rooted_at_slot.read() {
                        if !slot_data.has_complete_data() && 
                           current_network_slot > rooted_at_slot + 32 {
                            // This slot has been rooted for over 32 slots without complete data
                            incomplete_slots_to_write.push((slot, slot_data.clone()));
                        }
                    }
                }
                
                // Check which slots are ready for cleanup (older than 5 minutes)
                if let Ok(mut queue) = cleanup_queue.lock() {
                    while let Some(&(slot, insert_time)) = queue.front() {
                        if current_time.duration_since(insert_time).as_secs() > 300 {
                            slots_to_remove.push(slot);
                            queue.pop_front();
                        } else {
                            // Queue is ordered by time, so we can stop here
                            break;
                        }
                    }
                }
                
                // Calculate current memory usage before cleanup
                let mut total_memory = 0;
                for entry in airlock.iter() {
                    let slot_data = entry.value();
                    total_memory += slot_data.memory_usage.load(Ordering::Relaxed);
                }
                stats.total_memory_usage.store(total_memory, Ordering::Relaxed);
                
                // Update peak memory if necessary
                let current_peak = stats.peak_memory_usage.load(Ordering::Relaxed);
                if total_memory > current_peak {
                    stats.peak_memory_usage.store(total_memory, Ordering::Relaxed);
                }
                
                // Remove slots from airlock
                let mut deleted_count = 0;
                for slot in slots_to_remove {
                    if let Some((_, slot_data)) = airlock.remove(&slot) {
                        // Spin wait for writers to finish before dropping
                        while slot_data.writer_count.load(Ordering::Acquire) > 0 {
                            std::hint::spin_loop();
                        }
                        
                        // Subtract memory usage from total
                        let slot_memory = slot_data.memory_usage.load(Ordering::Relaxed);
                        stats.total_memory_usage.fetch_sub(slot_memory, Ordering::Relaxed);
                        
                        deleted_count += 1;
                        debug!("Cleaned up slot {} from memory (freed {} bytes)", slot, slot_memory);
                    }
                }
                
                // Update slots deleted counter
                if deleted_count > 0 {
                    stats.slots_deleted_total.fetch_add(deleted_count, Ordering::Relaxed);
                }
                
                // Write incomplete slots that have exceeded grace period
                for (slot, slot_data) in incomplete_slots_to_write {
                    // Check what components are missing
                    let mut missing = Vec::new();
                    if slot_data.bank_hash_components.read().is_none() {
                        missing.push("bank_hash");
                    }
                    if slot_data.delta_lthash.read().is_none() {
                        missing.push("delta_lthash");
                    }
                    if slot_data.cumulative_lthash.read().is_none() {
                        missing.push("cumulative_lthash");
                    }
                    if slot_data.blockhash.read().is_none() {
                        missing.push("blockhash");
                    }
                    if slot_data.parent_slot.read().is_none() {
                        missing.push("parent_slot");
                    }
                    if slot_data.executed_transaction_count.read().is_none() {
                        missing.push("executed_transaction_count");
                    }
                    if slot_data.entry_count.read().is_none() {
                        missing.push("entry_count");
                    }
                    
                    warn!(
                        "Writing incomplete slot {} to database after 32 slot grace period - missing: [{}]",
                        slot, missing.join(", ")
                    );
                    
                    // Create the command to write the incomplete slot
                    // Extract whatever data we have, use empty/zero values for missing data
                    let components = slot_data.bank_hash_components.read().clone()
                        .unwrap_or_else(|| BankHashComponentsInfo {
                            slot,
                            bank_hash: String::new(),
                            parent_bank_hash: String::new(),
                            signature_count: 0,
                            last_blockhash: String::new(),
                            accounts_delta_hash: None,
                            accounts_lthash_checksum: None,
                            epoch_accounts_hash: None,
                        });
                    
                    let delta_lthash = slot_data.delta_lthash.read().clone()
                        .unwrap_or_else(|| vec![0u8; 32]); // 32 zero bytes for missing lthash
                    let cumulative_lthash = slot_data.cumulative_lthash.read().clone()
                        .unwrap_or_else(|| vec![0u8; 32]); // 32 zero bytes for missing lthash
                    let blockhash = slot_data.blockhash.read().clone();
                    let parent_slot = slot_data.parent_slot.read().clone();
                    let executed_transaction_count = slot_data.executed_transaction_count.read().clone();
                    let entry_count = slot_data.entry_count.read().clone();
                    let vote_transactions = slot_data.vote_transactions.read().clone();
                    
                    // Send to database queue
                    if let Some(queue) = db_writer_queue.as_ref() {
                        let _ = queue.send(DbWriteCommand::SlotData {
                                slot,
                                bank_hash: components.bank_hash,
                                parent_bank_hash: components.parent_bank_hash,
                                signature_count: components.signature_count,
                                last_blockhash: components.last_blockhash,
                                delta_lthash,
                                cumulative_lthash,
                                accounts_delta_hash: components.accounts_delta_hash,
                                accounts_lthash_checksum: components.accounts_lthash_checksum,
                                epoch_accounts_hash: components.epoch_accounts_hash,
                                blockhash,
                                parent_slot,
                                executed_transaction_count,
                                entry_count,
                                vote_transactions,
                            });
                            
                            // Add to cleanup queue for later removal
                            if let Ok(mut cleanup) = cleanup_queue.lock() {
                                cleanup.push_back((slot, Instant::now()));
                            }
                            
                            stats.db_writes.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                // Update slot count stats
                let slot_count = airlock.len();
                stats.active_slots.store(slot_count, Ordering::Relaxed);
                stats.slots_in_memory.store(slot_count, Ordering::Relaxed);
            }
        });
        
        self.cleanup_task = Some(handle);
    }
    
    fn start_api_server(&mut self) {
        let monitored_accounts = self.monitored_accounts.clone();
        let config = match self.config.as_ref() {
            Some(c) => c,
            None => {
                error!("Cannot start API server: config not available");
                return;
            }
        };
        
        let api_port = match config.api_port {
            Some(port) => port,
            None => {
                error!("Cannot start API server: api_port not configured");
                return;
            }
        };
        
        let db_config = format!(
            "host={} port={} user={} password={} dbname={}",
            config.db_host, config.db_port, config.db_user, config.db_password, config.db_name
        );

        // API server starts in its own thread
        let handle = crate::api_server::start_api_server(monitored_accounts, api_port, db_config);
        self.api_server = Some(handle);
    }
    
    fn sync_monitored_accounts_from_db(&self, config: &PluginConfig) {
        use tokio_postgres::NoTls;
        
        let db_config = format!(
            "host={} port={} user={} password={} dbname={}",
            config.db_host, config.db_port, config.db_user, config.db_password, config.db_name
        );
        
        // Use blocking connection for initial sync
        let runtime = Runtime::new().unwrap();
        let result = runtime.block_on(async {
            match tokio_postgres::connect(&db_config, NoTls).await {
                Ok((client, connection)) => {
                    // Spawn connection handler in background
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            error!("Database connection error during sync: {}", e);
                        }
                    });
                    Some(client)
                }
                Err(e) => {
                    warn!("Failed to connect to database for syncing monitored accounts: {}", e);
                    None
                }
            }
        });
        
        if let Some(client) = result {
            // Run the query synchronously
            let sync_result = runtime.block_on(async {
                // First, insert accounts from config that don't exist
                for account_str in &config.monitored_accounts {
                    let _ = client.execute(
                        "INSERT INTO monitored_accounts (account_pubkey) VALUES ($1) ON CONFLICT (account_pubkey) DO NOTHING",
                        &[&account_str],
                    ).await;
                }
                
                // Then fetch all active accounts
                client.query(
                    "SELECT account_pubkey FROM monitored_accounts WHERE active = true",
                    &[],
                ).await
            });
            
            match sync_result {
                Ok(rows) => {
                    let mut count = 0;
                    for row in rows {
                        let account_str: String = row.get(0);
                        if let Ok(pubkey) = Pubkey::from_str(&account_str) {
                            if self.monitored_accounts.insert(pubkey) {
                                count += 1;
                                info!("Added monitored account from DB: {}", account_str);
                            }
                        }
                    }
                    info!("Synced {} additional monitored accounts from database", count);
                }
                Err(e) => {
                    warn!("Failed to query monitored accounts from database: {}", e);
                }
            }
        }
    }

    /// Verify a vote transaction signature
    /// This function can be used to verify that a stored vote transaction has a valid signature
    #[allow(dead_code)]
    fn verify_vote_transaction_signature(
        transaction_bytes: &[u8],
        signature_str: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        use solana_sdk::signature::Signature;
        use solana_sdk::transaction::VersionedTransaction;
        
        // Deserialize the transaction
        let transaction: VersionedTransaction = bincode::deserialize(transaction_bytes)?;
        
        // Parse the signature
        let signature = Signature::from_str(signature_str)?;
        
        // Get the message to verify
        let message_bytes = transaction.message.serialize();
        
        // Get the public key (first account is the signer for vote transactions)
        let pubkey = transaction.message.static_account_keys().get(0)
            .ok_or("No public key found in transaction")?;
        
        // Verify the signature
        let verified = signature.verify(pubkey.as_ref(), &message_bytes);
        
        Ok(verified)
    }
    
    fn compute_and_store_epoch_validators(&self, epoch: u64, computed_at_slot: u64) {
        // Aggregate stakes by validator from accumulated stake accounts
        let mut validator_stakes: HashMap<String, u64> = HashMap::new();
        
        for entry in self.epoch_stake_accumulator.iter() {
            let stake_info = entry.value();
            if let Some(voter_pubkey) = &stake_info.voter_pubkey {
                // Only count active stakes (not deactivating)
                if stake_info.deactivation_epoch.is_none() && stake_info.stake_amount > 0 {
                    *validator_stakes.entry(voter_pubkey.clone()).or_insert(0) += stake_info.stake_amount;
                }
            }
        }
        
        let total_stake: u64 = validator_stakes.values().sum();
        let (epoch_start_slot, epoch_end_slot) = get_epoch_boundaries(epoch);
        
        let validators: Vec<EpochValidator> = validator_stakes.into_iter()
            .map(|(validator_pubkey, total_stake_amount)| {
                let stake_percentage = if total_stake > 0 {
                    (total_stake_amount as f64 / total_stake as f64) * 100.0
                } else {
                    0.0
                };
                
                EpochValidator {
                    validator_pubkey,
                    total_stake: total_stake_amount,
                    stake_percentage,
                    total_epoch_stake: total_stake,
                }
            })
            .collect();
        
        debug!("Computed validator set for epoch {} with {} validators, total stake: {}", 
            epoch, validators.len(), total_stake);
        
        // NOTE: In Solana, stakes are normally only valid for the NEXT epoch after they are activated.
        // The validator set for epoch N+1 is determined at the boundary between epochs N-1 and N.
        // However, for simplicity in this implementation, we are using the stakes collected in the 
        // first change of an epoch for both that epoch and the next one. This means:
        // - Epoch N stakes are computed from the first account change in epoch N
        // - These stakes represent the active set for epoch N (even though technically they were
        //   determined at the N-1/N boundary)
        // This approach works because we're capturing the reward distribution at the start of each
        // epoch, which reflects the stakes that were active for voting in that epoch.
        
        // Send to database
        if let Some(queue) = &self.db_writer_queue {
            let _ = queue.send(DbWriteCommand::EpochValidatorSet {
                epoch,
                epoch_start_slot,
                epoch_end_slot,
                computed_at_slot,
                validators,
            });
        }
    }
}
