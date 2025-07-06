use crate::airlock::types::{
    AirlockSlotData, BankHashComponentsInfo, DbWriteCommand, LtHash, OwnedAccountChange,
    PluginConfig, ProofRequest, ReplicaBlockInfoVersions, VoteTransaction,
    StakeAccountInfo as DbStakeAccountInfo, EpochValidator,
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
use solana_transaction::sanitized::SanitizedTransaction;
use solana_transaction_status::TransactionStatusMeta;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use std::collections::HashMap;

use crate::chain_monitor::ChainMonitor;
use crate::metrics_server::MetricsServer;
use crate::worker_pool::WorkerPool;
use crate::api_server::ApiServerHandle;

/// Main plugin state
#[derive(Debug)]
pub struct TwineGeyserPlugin {
    /// Primary buffer for all in-flight slot data
    airlock: Arc<DashMap<u64, Arc<AirlockSlotData>>>,
    /// Fast-lookup set of monitored pubkeys
    monitored_accounts: Arc<DashSet<Pubkey>>,
    /// Debouncing map for proof scheduling
    pending_proofs_for_scheduling: Arc<DashMap<Pubkey, u64>>,
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
    /// Tracks if we've seen the first account change in a new epoch
    epoch_first_change_seen: Arc<AtomicBool>,
    /// Accumulates stake states during epoch transitions
    epoch_stake_accumulator: Arc<DashMap<String, StakeAccountInfo>>,
}

impl Default for TwineGeyserPlugin {
    fn default() -> Self {
        Self {
            airlock: Arc::new(DashMap::new()),
            monitored_accounts: Arc::new(DashSet::new()),
            pending_proofs_for_scheduling: Arc::new(DashMap::new()),
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
            epoch_first_change_seen: Arc::new(AtomicBool::new(false)),
            epoch_stake_accumulator: Arc::new(DashMap::new()),
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

        info!(
            "Plugin configured with {} monitored accounts",
            self.monitored_accounts.len()
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
        let slot = account_change.slot;
        let current_epoch = slot_to_epoch(slot);
        let stored_epoch = self.current_epoch.load(Ordering::Relaxed);

        // Check if this is a stake account
        let is_stake_account = stake::is_stake_account(account_change.new_account.owner());

        // Handle epoch boundary detection
        if current_epoch > stored_epoch {
            info!("Detected new epoch {} at slot {} (previous epoch: {})", 
                current_epoch, slot, stored_epoch);
            
            // If this is the first change in the new epoch, compute validator set
            if self.epoch_first_change_seen.compare_exchange(
                false, true, Ordering::SeqCst, Ordering::SeqCst
            ).is_ok() {
                self.compute_and_store_epoch_validators(stored_epoch, slot);
                self.current_epoch.store(current_epoch, Ordering::Relaxed);
                // Clear the accumulator for the new epoch
                self.epoch_stake_accumulator.clear();
                // Reset the flag for the next epoch
                self.epoch_first_change_seen.store(false, Ordering::Relaxed);
            }
        }

        // Process stake account if applicable
        if is_stake_account {
            match stake::deserialize_stake_state(account_change.new_account.data()) {
                Ok(stake_state) => {
                    let stake_info = StakeAccountInfo::from_state(&account_change.pubkey, &stake_state);
                    
                    // Store in accumulator for epoch boundary calculations
                    self.epoch_stake_accumulator.insert(
                        account_change.pubkey.to_string(),
                        stake_info.clone()
                    );
                    
                    // Convert to DB type
                    let db_stake_info = DbStakeAccountInfo {
                        stake_pubkey: stake_info.stake_pubkey,
                        voter_pubkey: stake_info.voter_pubkey,
                        stake_amount: stake_info.stake_amount,
                        activation_epoch: stake_info.activation_epoch,
                        deactivation_epoch: stake_info.deactivation_epoch,
                        credits_observed: stake_info.credits_observed,
                        rent_exempt_reserve: stake_info.rent_exempt_reserve,
                        staker: stake_info.staker,
                        withdrawer: stake_info.withdrawer,
                        state_type: stake_info.state_type,
                    };
                    
                    // Send to database
                    if let Some(queue) = &self.db_writer_queue {
                        let _ = queue.send(DbWriteCommand::StakeAccountChange {
                            slot,
                            stake_info: db_stake_info,
                            lthash: account_change.new_lthash.0.to_vec(),
                        });
                    }
                }
                Err(e) => {
                    debug!("Failed to deserialize stake account {}: {}", 
                        account_change.pubkey, e);
                }
            }
        }

        // Get or create airlock slot data
        let slot_data = self
            .airlock
            .entry(slot)
            .or_insert_with(|| Arc::new(AirlockSlotData::new()))
            .clone();

        // Atomically increment writer count
        slot_data.writer_count.fetch_add(1, Ordering::AcqRel);

        // Check if this is a monitored account
        if self.monitored_accounts.contains(&account_change.pubkey) {
            slot_data
                .contains_monitored_change
                .store(true, Ordering::Release);
            self.pending_proofs_for_scheduling
                .insert(account_change.pubkey, slot);
            self.stats
                .monitored_account_changes
                .fetch_add(1, Ordering::Relaxed);
        }

        // Push to buffer - take ownership directly
        slot_data.buffer.push(account_change);

        // Update stats
        self.stats.total_updates.fetch_add(1, Ordering::Relaxed);

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
        info!(
            "Received LtHash for slot {}: delta={:?}, cumulative={:?}",
            slot,
            delta_lthash
                .0
                .iter()
                .take(2)
                .map(|x| format!("{:016x}", x))
                .collect::<Vec<_>>()
                .join(""),
            cumulative_lthash
                .0
                .iter()
                .take(2)
                .map(|x| format!("{:016x}", x))
                .collect::<Vec<_>>()
                .join("")
        );

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

        // Get or create airlock slot data
        let slot_data = self
            .airlock
            .entry(slot)
            .or_insert_with(|| Arc::new(AirlockSlotData::new()))
            .clone();

        // Atomically increment writer count while we update
        slot_data.writer_count.fetch_add(1, Ordering::AcqRel);

        // Update LtHash data
        *slot_data.delta_lthash.write() = Some(delta_bytes);
        *slot_data.cumulative_lthash.write() = Some(cumulative_bytes);
        
        info!("Updated airlock slot {} with LtHash data", slot);

        // Check if we now have complete data and slot is rooted
        if slot_data.is_rooted() && slot_data.has_complete_data() {
            info!("Slot {} has complete data after LtHash update, triggering write", slot);
            self.try_write_complete_slot(slot)?;
        }

        // Atomically decrement writer count
        slot_data.writer_count.fetch_sub(1, Ordering::AcqRel);

        // Update stats
        self.stats
            .active_slots
            .store(self.airlock.len(), Ordering::Relaxed);

        Ok(())
    }

    fn notify_bank_hash_components(&self, components: BankHashComponentsInfo) -> PluginResult<()> {
        let slot = components.slot;

        info!(
            "Received bank hash components for slot {}: bank_hash={}, parent_bank_hash={}, signature_count={}",
            slot, components.bank_hash, components.parent_bank_hash, components.signature_count
        );

        // Get or create airlock slot data
        let slot_data = self
            .airlock
            .entry(slot)
            .or_insert_with(|| Arc::new(AirlockSlotData::new()))
            .clone();

        // Atomically increment writer count while we update
        slot_data.writer_count.fetch_add(1, Ordering::AcqRel);

        // Update bank hash components
        *slot_data.bank_hash_components.write() = Some(components);
        
        info!("Updated airlock slot {} with bank hash components", slot);

        // Check if we now have complete data and slot is rooted
        if slot_data.is_rooted() && slot_data.has_complete_data() {
            info!("Slot {} has complete data after bank hash update, triggering write", slot);
            self.try_write_complete_slot(slot)?;
        }

        // Atomically decrement writer count
        slot_data.writer_count.fetch_sub(1, Ordering::AcqRel);

        // Update stats
        self.stats
            .active_slots
            .store(self.airlock.len(), Ordering::Relaxed);

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
                info!(
                    "Block metadata V4 for slot {}: blockhash={}, parent_blockhash={}, parent_slot={}, executed_transaction_count={}, entry_count={}",
                    info.slot, info.blockhash, info.parent_blockhash, info.parent_slot, info.executed_transaction_count, info.entry_count
                );

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
                
                info!("Updated airlock slot {} with block metadata", info.slot);

                // Check if we now have complete data and slot is rooted
                if slot_data.is_rooted() && slot_data.has_complete_data() {
                    info!("Slot {} has complete data after block metadata update, triggering write", info.slot);
                    self.try_write_complete_slot(info.slot)?;
                }

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

        // Serialize the transaction
        let serialized_tx = match bincode::serialize(transaction) {
            Ok(data) => data,
            Err(e) => {
                warn!("Failed to serialize vote transaction: {}", e);
                return Ok(());
            }
        };

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
        };

        // Get or create airlock slot data
        let slot_data = self
            .airlock
            .entry(slot)
            .or_insert_with(|| Arc::new(AirlockSlotData::new()))
            .clone();

        // Add vote transaction to slot
        slot_data.vote_transactions.write().push(vote_tx);

        info!(
            "Recorded vote transaction for slot {} from validator {}",
            slot, vote_pubkey
        );

        Ok(())
    }

    fn try_write_complete_slot(&self, slot: u64) -> PluginResult<()> {
        // Check if slot exists and has complete data
        if let Some(slot_data_ref) = self.airlock.get(&slot) {
            let slot_data = slot_data_ref.value();
            if slot_data.has_complete_data() && slot_data.is_rooted() {
                // Drop the reference before we try to remove
                drop(slot_data_ref);
                
                // Remove the slot from airlock
                if let Some((_, slot_data)) = self.airlock.remove(&slot) {
                    // Wait for all writers to finish
                    while slot_data.writer_count.load(Ordering::Acquire) > 0 {
                        std::hint::spin_loop();
                    }
                    
                    self.write_slot_to_database(slot, slot_data)?;
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

        info!(
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

        // Send account changes if any
        let mut monitored_changes = Vec::new();
        while let Some(change) = slot_data.buffer.pop() {
            if self.monitored_accounts.contains(&change.pubkey) {
                monitored_changes.push(change);
            }
        }

        if !monitored_changes.is_empty() {
            queue
                .send(DbWriteCommand::AccountChanges {
                    slot,
                    changes: monitored_changes,
                })
                .map_err(|_| GeyserPluginError::Custom("Failed to send account changes".into()))?;

            self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
            self.stats
                .slots_with_monitored_accounts
                .fetch_add(1, Ordering::Relaxed);
        }

        // Update stats
        self.stats.sealed_slots.fetch_add(1, Ordering::Relaxed);
        self.stats
            .active_slots
            .store(self.airlock.len(), Ordering::Relaxed);

        Ok(())
    }
    
    fn handle_rooted_slot(&self, slot: u64) -> PluginResult<()> {
        info!("Rooted slot: {}", slot);
        // Update validator slot in chain monitor
        if let Some(monitor) = &self.chain_monitor {
            monitor.update_validator_slot(slot);
        }

        // Clean up old airlock entries (slots that are more than 100 slots behind)
        let min_slot = slot.saturating_sub(100);
        let mut removed_count = 0;
        self.airlock.retain(|&airlock_slot, _| {
            if airlock_slot < min_slot {
                removed_count += 1;
                false
            } else {
                true
            }
        });
        if removed_count > 0 {
            info!("Cleaned up {} old airlock entries", removed_count);
        }

        let queue = self
            .db_writer_queue
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom("Plugin not initialized".into()))?;

        // Get or create airlock slot data
        let slot_data = self
            .airlock
            .entry(slot)
            .or_insert_with(|| Arc::new(AirlockSlotData::new()))
            .clone();

        // Update slot status to rooted
        *slot_data.status.write() = "rooted".to_string();

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
            // Log what we're missing
            let has_bank_hash = slot_data.bank_hash_components.read().as_ref()
                .map(|c| !c.bank_hash.is_empty())
                .unwrap_or(false);
            let has_delta = slot_data.delta_lthash.read().is_some();
            let has_cumulative = slot_data.cumulative_lthash.read().is_some();
            
            info!(
                "Slot {} rooted but missing data - bank_hash: {}, delta_lthash: {}, cumulative_lthash: {}, keeping in airlock",
                slot, has_bank_hash, has_delta, has_cumulative
            );
        }

        // Update stats
        self.stats
            .active_slots
            .store(self.airlock.len(), Ordering::Relaxed);

        // Log stats every 100 rooted slots
        let last_log = self.last_stats_log_slot.load(Ordering::Relaxed);
        if slot >= last_log + 100 {
            self.log_stats();
            self.last_stats_log_slot.store(slot, Ordering::Relaxed);
        }

        Ok(())
    }

    fn handle_processed_slot(&self, slot: u64) -> PluginResult<()> {
        // Get or create airlock slot data
        let slot_data = self
            .airlock
            .entry(slot)
            .or_insert_with(|| Arc::new(AirlockSlotData::new()))
            .clone();

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
        
        info!("Computed validator set for epoch {} with {} validators, total stake: {}", 
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
