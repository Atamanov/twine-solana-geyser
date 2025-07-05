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

        // Initialize monitored accounts
        for account_str in &config.monitored_accounts {
            if let Ok(pubkey) = Pubkey::from_str(account_str) {
                self.monitored_accounts.insert(pubkey);
                info!("Added monitored account: {}", account_str);
            } else {
                warn!("Invalid pubkey in monitored_accounts: {}", account_str);
            }
        }

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
        self.worker_pool = Some(WorkerPool::start(rx, config.clone()));
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
        // Store the LtHash data temporarily until we get the bank hash components
        if let Some(mut pending) = self.pending_slot_data.get_mut(&slot) {
            pending.delta_lthash = delta_lthash
                .0
                .iter()
                .flat_map(|&x| x.to_le_bytes())
                .collect();
            pending.cumulative_lthash = cumulative_lthash
                .0
                .iter()
                .flat_map(|&x| x.to_le_bytes())
                .collect();
        } else {
            // If we get LtHash before bank components, we'll wait for the components
        }
        Ok(())
    }

    fn notify_bank_hash_components(&self, components: BankHashComponentsInfo) -> PluginResult<()> {
        let slot = components.slot;

        // Store or update the pending slot data
        if let Some(mut pending) = self.pending_slot_data.get_mut(&slot) {
            pending.components = components;
        } else {
            // If we get bank components before LtHash, create with empty LtHash
            self.pending_slot_data.insert(
                slot,
                PendingSlotData {
                    components,
                    delta_lthash: vec![],
                    cumulative_lthash: vec![],
                },
            );
        }
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
        }
        Ok(())
    }

    fn notify_block_metadata(&self, block_info: ReplicaBlockInfoVersions) -> PluginResult<()> {
        match &block_info {
            ReplicaBlockInfoVersions::V0_0_1(info) => {
                debug!(
                    "Block metadata V1 for slot {}: blockhash={}, rewards={:?}",
                    info.slot, info.blockhash, info.rewards
                );
                // V1 doesn't have parent_blockhash
            }
            ReplicaBlockInfoVersions::V0_0_2(info) => {
                debug!(
                    "Block metadata V2 for slot {}: blockhash={}, parent_blockhash={}, parent_slot={}, rewards={:?}",
                    info.slot, info.blockhash, info.parent_blockhash, info.parent_slot, info.rewards
                );
                // Store blockhash info if needed
            }
            ReplicaBlockInfoVersions::V0_0_3(info) => {
                debug!(
                    "Block metadata V3 for slot {}: blockhash={}, parent_blockhash={}, parent_slot={}, executed_transaction_count={}, entry_count={}",
                    info.slot, info.blockhash, info.parent_blockhash, info.parent_slot, info.executed_transaction_count, info.entry_count
                );
                // Store blockhash info if needed
            }
            ReplicaBlockInfoVersions::V0_0_4(info) => {
                debug!(
                    "Block metadata V4 for slot {}: blockhash={}, parent_blockhash={}, parent_slot={}, executed_transaction_count={}, entry_count={}",
                    info.slot, info.blockhash, info.parent_blockhash, info.parent_slot, info.executed_transaction_count, info.entry_count
                );
                // Store blockhash info if needed - V4 has RewardsAndNumPartitions instead of plain rewards
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
        // Update validator slot in chain monitor
        if let Some(monitor) = &self.chain_monitor {
            monitor.update_validator_slot(slot);
        }

        let queue = self
            .db_writer_queue
            .as_ref()
            .ok_or_else(|| GeyserPluginError::Custom("Plugin not initialized".into()))?;

        // Always push slot data
        if let Some((_, pending_data)) = self.pending_slot_data.remove(&slot) {
            let components = pending_data.components;

            queue
                .send(DbWriteCommand::SlotData {
                    slot,
                    bank_hash: components.bank_hash,
                    parent_bank_hash: components.parent_bank_hash,
                    signature_count: components.signature_count,
                    last_blockhash: components.last_blockhash,
                    delta_lthash: pending_data.delta_lthash,
                    cumulative_lthash: pending_data.cumulative_lthash,
                    accounts_delta_hash: components.accounts_delta_hash,
                    accounts_lthash_checksum: components.accounts_lthash_checksum,
                    epoch_accounts_hash: components.epoch_accounts_hash,
                })
                .map_err(|_| GeyserPluginError::Custom("Failed to send slot data".into()))?;

            self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
        }

        // Handle account changes if any
        if let Some((_, slot_data)) = self.airlock.remove(&slot) {
            // Wait for all writers to finish
            while slot_data.writer_count.load(Ordering::Acquire) > 0 {
                std::hint::spin_loop();
            }

            // If contains monitored changes, send all changes to DB
            if slot_data.contains_monitored_change.load(Ordering::Acquire) {
                let mut changes = Vec::new();
                while let Some(change) = slot_data.buffer.pop() {
                    changes.push(change);
                }

                if !changes.is_empty() {
                    queue
                        .send(DbWriteCommand::AccountChanges { slot, changes })
                        .map_err(|_| {
                            GeyserPluginError::Custom("Failed to send account changes".into())
                        })?;

                    self.stats.db_writes.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .slots_with_monitored_accounts
                        .fetch_add(1, Ordering::Relaxed);
                }
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
}
