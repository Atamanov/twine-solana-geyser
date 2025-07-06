use crate::airlock::types::{
    AirlockSlotData, BankHashComponentsInfo, DbWriteCommand, LtHash, OwnedAccountChange,
    PluginConfig, ProofRequest, ReplicaBlockInfoVersions,
};
use crate::airlock::{AirlockStats, AirlockStatsSnapshot};
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, Result as PluginResult, SlotStatus,
};
use crossbeam_channel::{bounded, Sender};
use dashmap::{DashMap, DashSet};
use log::*;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::chain_monitor::ChainMonitor;
use crate::metrics_server::MetricsServer;
use crate::worker_pool::WorkerPool;

/// Container for slot data waiting to be persisted
#[derive(Debug)]
struct PendingSlotData {
    components: BankHashComponentsInfo,
    delta_lthash: Vec<u8>,
    cumulative_lthash: Vec<u8>,
    // Block metadata fields
    blockhash: Option<String>,
    parent_slot: Option<u64>,
    executed_transaction_count: Option<u64>,
    entry_count: Option<u64>,
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
    /// Temporary storage for slot data until it's rooted
    pending_slot_data: Arc<DashMap<u64, PendingSlotData>>,
    /// Statistics for monitoring
    stats: Arc<AirlockStats>,
    /// Last slot when stats were logged
    last_stats_log_slot: AtomicU64,
    /// Metrics server handle
    metrics_server: Option<tokio::task::JoinHandle<()>>,
    /// API server handle
    api_server: Option<tokio::task::JoinHandle<()>>,
    /// Chain monitor
    chain_monitor: Option<Arc<ChainMonitor>>,
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
            pending_slot_data: Arc::new(DashMap::new()),
            stats: Arc::new(AirlockStats::default()),
            last_stats_log_slot: AtomicU64::new(0),
            metrics_server: None,
            api_server: None,
            chain_monitor: None,
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
            handle.abort();
        }

        // Wait for workers to finish
        if let Some(pool) = self.worker_pool.take() {
            pool.join();
        }

        // Shutdown runtime
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }

    fn notify_account_change(&self, account_change: OwnedAccountChange) -> PluginResult<()> {
        let slot = account_change.slot;

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
        let delta_bytes = delta_lthash
            .0
            .iter()
            .flat_map(|&x| x.to_le_bytes())
            .collect();
        let cumulative_bytes = cumulative_lthash
            .0
            .iter()
            .flat_map(|&x| x.to_le_bytes())
            .collect();

        // Store or update the pending slot data
        if let Some(mut pending) = self.pending_slot_data.get_mut(&slot) {
            pending.delta_lthash = delta_bytes;
            pending.cumulative_lthash = cumulative_bytes;
            info!("Updated existing pending slot data for slot {}", slot);
        } else {
            // If we get LtHash before bank components, create with placeholder components
            self.pending_slot_data.insert(
                slot,
                PendingSlotData {
                    components: BankHashComponentsInfo {
                        slot,
                        bank_hash: String::new(),
                        parent_bank_hash: String::new(),
                        signature_count: 0,
                        last_blockhash: String::new(),
                        accounts_delta_hash: None,
                        accounts_lthash_checksum: None,
                        epoch_accounts_hash: None,
                    },
                    delta_lthash: delta_bytes,
                    cumulative_lthash: cumulative_bytes,
                    blockhash: None,
                    parent_slot: None,
                    executed_transaction_count: None,
                    entry_count: None,
                },
            );
            info!(
                "Created new pending slot data for slot {} with LtHash",
                slot
            );
        }

        // Update pending slot data count
        self.stats
            .pending_slot_data_count
            .store(self.pending_slot_data.len(), Ordering::Relaxed);

        Ok(())
    }

    fn notify_bank_hash_components(&self, components: BankHashComponentsInfo) -> PluginResult<()> {
        let slot = components.slot;

        info!(
            "Received bank hash components for slot {}: bank_hash={}, parent_bank_hash={}, signature_count={}",
            slot, components.bank_hash, components.parent_bank_hash, components.signature_count
        );

        // Store or update the pending slot data
        if let Some(mut pending) = self.pending_slot_data.get_mut(&slot) {
            pending.components = components;
            info!(
                "Updated existing pending slot data for slot {} with bank components",
                slot
            );
        } else {
            // If we get bank components before LtHash, create with empty LtHash
            self.pending_slot_data.insert(
                slot,
                PendingSlotData {
                    components,
                    delta_lthash: vec![],
                    cumulative_lthash: vec![],
                    blockhash: None,
                    parent_slot: None,
                    executed_transaction_count: None,
                    entry_count: None,
                },
            );
            info!(
                "Created new pending slot data for slot {} with bank components",
                slot
            );
        }

        // Update pending slot data count
        self.stats
            .pending_slot_data_count
            .store(self.pending_slot_data.len(), Ordering::Relaxed);

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

                // Store block metadata in pending slot data
                if let Some(mut pending) = self.pending_slot_data.get_mut(&info.slot) {
                    pending.blockhash = Some(info.blockhash.to_string());
                    pending.parent_slot = Some(info.parent_slot);
                    pending.executed_transaction_count = Some(info.executed_transaction_count);
                    pending.entry_count = Some(info.entry_count);
                } else {
                    // If we get block metadata before other data, create entry
                    self.pending_slot_data.insert(
                        info.slot,
                        PendingSlotData {
                            components: BankHashComponentsInfo {
                                slot: info.slot,
                                bank_hash: String::new(),
                                parent_bank_hash: String::new(),
                                signature_count: 0,
                                last_blockhash: String::new(),
                                accounts_delta_hash: None,
                                accounts_lthash_checksum: None,
                                epoch_accounts_hash: None,
                            },
                            delta_lthash: vec![],
                            cumulative_lthash: vec![],
                            blockhash: Some(info.blockhash.to_string()),
                            parent_slot: Some(info.parent_slot),
                            executed_transaction_count: Some(info.executed_transaction_count),
                            entry_count: Some(info.entry_count),
                        },
                    );
                }

                // Update pending slot data count
                self.stats
                    .pending_slot_data_count
                    .store(self.pending_slot_data.len(), Ordering::Relaxed);
            }
        }
        Ok(())
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
}

impl TwineGeyserPlugin {
    fn handle_rooted_slot(&self, slot: u64) -> PluginResult<()> {
        info!("Rooted slot: {}", slot);
        // Update validator slot in chain monitor
        if let Some(monitor) = &self.chain_monitor {
            monitor.update_validator_slot(slot);
        }

        // Clean up old pending slot data (slots that are more than 100 slots behind)
        let min_slot = slot.saturating_sub(100);
        let mut removed_count = 0;
        self.pending_slot_data.retain(|&pending_slot, _| {
            if pending_slot < min_slot {
                removed_count += 1;
                false
            } else {
                true
            }
        });
        if removed_count > 0 {
            info!("Cleaned up {} old pending slot data entries", removed_count);
        }

        let queue = self
            .db_writer_queue
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom("Plugin not initialized".into()))?;

        // Always push slot data
        if let Some((_, pending_data)) = self.pending_slot_data.remove(&slot) {
            let components = pending_data.components;

            // Check if we have valid data
            if components.bank_hash.is_empty() {
                warn!("Slot {} has empty bank_hash, using placeholder", slot);
            }
            if pending_data.delta_lthash.is_empty() {
                warn!("Slot {} has empty delta_lthash, using zero bytes", slot);
            }
            if pending_data.cumulative_lthash.is_empty() {
                warn!(
                    "Slot {} has empty cumulative_lthash, using zero bytes",
                    slot
                );
            }

            info!(
                "Writing slot {} data to DB: bank_hash={}, has_lthash={}, has_block_metadata={}",
                slot,
                components.bank_hash,
                !pending_data.delta_lthash.is_empty(),
                pending_data.blockhash.is_some()
            );

            // Ensure we have at least empty vectors for LtHash if not provided
            let delta_lthash = if pending_data.delta_lthash.is_empty() {
                vec![0u8; 32] // LtHash is 256 bits = 32 bytes
            } else {
                pending_data.delta_lthash
            };

            let cumulative_lthash = if pending_data.cumulative_lthash.is_empty() {
                vec![0u8; 32] // LtHash is 256 bits = 32 bytes
            } else {
                pending_data.cumulative_lthash
            };

            queue
                .send(DbWriteCommand::SlotData {
                    slot,
                    bank_hash: if components.bank_hash.is_empty() {
                        format!("PLACEHOLDER_{}", slot)
                    } else {
                        components.bank_hash
                    },
                    parent_bank_hash: if components.parent_bank_hash.is_empty() {
                        format!("PLACEHOLDER_{}", slot.saturating_sub(1))
                    } else {
                        components.parent_bank_hash
                    },
                    signature_count: components.signature_count,
                    last_blockhash: if components.last_blockhash.is_empty() {
                        format!("PLACEHOLDER_{}", slot)
                    } else {
                        components.last_blockhash
                    },
                    delta_lthash,
                    cumulative_lthash,
                    accounts_delta_hash: components.accounts_delta_hash,
                    accounts_lthash_checksum: components.accounts_lthash_checksum,
                    epoch_accounts_hash: components.epoch_accounts_hash,
                    // Include block metadata if available
                    blockhash: pending_data.blockhash,
                    parent_slot: pending_data.parent_slot,
                    executed_transaction_count: pending_data.executed_transaction_count,
                    entry_count: pending_data.entry_count,
                })
                .map_err(|_| GeyserPluginError::Custom("Failed to send slot data".into()))?;

            self.stats.db_writes.fetch_add(1, Ordering::Relaxed);

            // Update pending slot data count after removal
            self.stats
                .pending_slot_data_count
                .store(self.pending_slot_data.len(), Ordering::Relaxed);
        } else {
            warn!("No pending slot data found for rooted slot {}", slot);

            // Create minimal slot data to ensure we at least record the rooted slot
            queue
                .send(DbWriteCommand::SlotData {
                    slot,
                    bank_hash: format!("MISSING_{}", slot),
                    parent_bank_hash: format!("MISSING_{}", slot.saturating_sub(1)),
                    signature_count: 0,
                    last_blockhash: format!("MISSING_{}", slot),
                    delta_lthash: vec![0u8; 32],
                    cumulative_lthash: vec![0u8; 32],
                    accounts_delta_hash: None,
                    accounts_lthash_checksum: None,
                    epoch_accounts_hash: None,
                    blockhash: None,
                    parent_slot: None,
                    executed_transaction_count: None,
                    entry_count: None,
                })
                .map_err(|_| GeyserPluginError::Custom("Failed to send slot data".into()))?;

            self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
        }

        // Also send status update to mark as rooted
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

        // Handle account changes if any
        if let Some((_, slot_data)) = self.airlock.remove(&slot) {
            // Wait for all writers to finish
            while slot_data.writer_count.load(Ordering::Acquire) > 0 {
                std::hint::spin_loop();
            }

            // Collect all changes, but only keep full data for monitored accounts
            let mut monitored_changes = Vec::new();

            while let Some(change) = slot_data.buffer.pop() {
                // Only keep changes for monitored accounts
                if self.monitored_accounts.contains(&change.pubkey) {
                    monitored_changes.push(change);
                }
                // For non-monitored accounts, we just discard the data
                // The slot itself is already recorded with LtHash
            }

            // Send monitored account changes to DB
            if !monitored_changes.is_empty() {
                queue
                    .send(DbWriteCommand::AccountChanges {
                        slot,
                        changes: monitored_changes,
                    })
                    .map_err(|_| {
                        GeyserPluginError::Custom("Failed to send account changes".into())
                    })?;

                self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .slots_with_monitored_accounts
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        // Update stats
        self.stats.sealed_slots.fetch_add(1, Ordering::Relaxed);
        self.stats
            .active_slots
            .store(self.airlock.len() as usize, Ordering::Relaxed);

        // Log stats every 100 rooted slots
        let last_log = self.last_stats_log_slot.load(Ordering::Relaxed);
        if slot >= last_log + 100 {
            self.log_stats();
            self.last_stats_log_slot.store(slot, Ordering::Relaxed);
        }

        Ok(())
    }

    fn handle_processed_slot(&self, slot: u64) -> PluginResult<()> {
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
        let api_port = match self.config.as_ref().and_then(|c| c.api_port) {
            Some(port) => port,
            None => {
                error!("Cannot start API server: api_port not configured");
                return;
            }
        };

        let runtime = match self.runtime.as_ref() {
            Some(rt) => rt,
            None => {
                error!("Cannot start API server: runtime not initialized");
                return;
            }
        };

        let handle = runtime.spawn(async move {
            if let Err(e) = crate::api_server::start_api_server(monitored_accounts, api_port).await {
                error!("API server error: {}", e);
            }
        });

        self.api_server = Some(handle);
    }
    
    fn sync_monitored_accounts_from_db(&self, config: &PluginConfig) {
        use tokio_postgres::{NoTls, Client};
        
        let db_config = format!(
            "host={} port={} user={} password={} dbname={}",
            config.db_host, config.db_port, config.db_user, config.db_password, config.db_name
        );
        
        // Use blocking connection for initial sync
        if let Ok((mut client, connection)) = tokio_postgres::connect(&db_config, NoTls).map_err(|e| {
            warn!("Failed to connect to database for syncing monitored accounts: {}", e);
            e
        }).ok().and_then(|result| {
            // Spawn connection handler in background
            std::thread::spawn(move || {
                let runtime = Runtime::new().unwrap();
                runtime.block_on(async {
                    if let Err(e) = connection.await {
                        error!("Database connection error during sync: {}", e);
                    }
                });
            });
            Some(result)
        }) {
            // Run the query synchronously
            let runtime = Runtime::new().unwrap();
            let result = runtime.block_on(async {
                // First, insert accounts from config that don't exist
                for account_str in &config.monitored_accounts {
                    let _ = client.execute(
                        "INSERT INTO monitored_accounts (account_pubkey) VALUES ($1) ON CONFLICT DO NOTHING",
                        &[&account_str],
                    ).await;
                }
                
                // Then fetch all active accounts
                client.query(
                    "SELECT account_pubkey FROM monitored_accounts WHERE active = true",
                    &[],
                ).await
            });
            
            match result {
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
}
