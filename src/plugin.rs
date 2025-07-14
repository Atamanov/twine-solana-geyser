use std::fs::File;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
};

use crate::airlock::{OwnedAccountChange, VoteTransaction};
use solana_sdk::{clock::Slot, transaction::SanitizedTransaction};

use crate::aggregator::spawn_aggregators;
use crate::airlock::{AirLock, BankHashComponents, BlockMetadata, SlotStatus as AirLockSlotStatus};
use crate::db_writer::{create_pool, spawn_db_writers, DbCommand, DbConfig};
use crate::metrics::spawn_metrics_server;

pub struct TwineGeyserPlugin {
    config: PluginConfig,
    airlock: Arc<AirLock>,
    is_startup_completed: AtomicBool,
    db_writer_threads: Option<Vec<std::thread::JoinHandle<()>>>,
    aggregator_threads: Option<Vec<std::thread::JoinHandle<()>>>,
    db_sender: Option<crossbeam::channel::Sender<DbCommand>>,
    metrics_thread: Option<std::thread::JoinHandle<()>>,
    shutdown_flag: Arc<AtomicBool>,
}

impl std::fmt::Debug for TwineGeyserPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwineGeyserPlugin")
            .field("config", &self.config)
            .field("is_startup_completed", &self.is_startup_completed)
            .finish()
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PluginConfig {
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_password: String,
    pub db_name: String,
    pub db_pool_size: usize,
    pub db_writer_threads: usize,
    pub aggregator_threads: usize,
    pub monitored_accounts: Vec<String>,
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
}

fn default_metrics_port() -> u16 {
    9090
}

impl TwineGeyserPlugin {
    pub fn new() -> Self {
        Self {
            config: PluginConfig {
                db_host: "localhost".to_string(),
                db_port: 5432,
                db_user: "geyser_writer".to_string(),
                db_password: "geyser_writer_password".to_string(),
                db_name: "twine_solana_db".to_string(),
                db_pool_size: 10,
                db_writer_threads: 4,
                aggregator_threads: 2,
                monitored_accounts: Vec::new(),
                metrics_port: 9090,
            },
            airlock: Arc::new(AirLock::new()),
            is_startup_completed: AtomicBool::new(false),
            db_writer_threads: None,
            aggregator_threads: None,
            db_sender: None,
            metrics_thread: None,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl GeyserPlugin for TwineGeyserPlugin {
    fn name(&self) -> &'static str {
        "TwineGeyserPlugin"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        log::info!("Loading Twine Geyser Plugin with config: {}", config_file);

        // Load configuration
        let mut file =
            File::open(config_file).map_err(|e| GeyserPluginError::ConfigFileOpenError(e))?;

        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|e| GeyserPluginError::ConfigFileReadError {
                msg: format!("Failed to read config file: {}", e),
            })?;

        self.config = serde_json::from_str(&contents).map_err(|e| {
            GeyserPluginError::ConfigFileReadError {
                msg: format!("Failed to parse config file: {}", e),
            }
        })?;

        // Create database pool
        let db_config = DbConfig {
            host: self.config.db_host.clone(),
            port: self.config.db_port,
            user: self.config.db_user.clone(),
            password: self.config.db_password.clone(),
            dbname: self.config.db_name.clone(),
            pool_size: self.config.db_pool_size,
        };

        let pool = create_pool(&db_config).map_err(|_| GeyserPluginError::ConfigFileReadError {
            msg: "Failed to create database pool".to_string(),
        })?;

        // Spawn database writer threads
        let (db_writers, db_sender) = spawn_db_writers(pool, self.config.db_writer_threads);
        self.db_writer_threads = Some(db_writers);
        self.db_sender = Some(db_sender.clone());

        // Spawn aggregator threads
        let aggregators = spawn_aggregators(
            self.airlock.clone(),
            db_sender,
            self.config.aggregator_threads,
            self.shutdown_flag.clone(),
        );
        self.aggregator_threads = Some(aggregators);

        // Spawn metrics server
        let airlock_clone = self.airlock.clone();
        let metrics_port = self.config.metrics_port;
        let metrics_thread = std::thread::Builder::new()
            .name("twine-metrics".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime for metrics");

                runtime.block_on(async {
                    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], metrics_port));
                    if let Err(e) = spawn_metrics_server(airlock_clone, addr).await {
                        log::error!("Failed to start metrics server: {}", e);
                    }
                });
            })
            .expect("Failed to spawn metrics thread");
        self.metrics_thread = Some(metrics_thread);

        log::info!("Twine Geyser Plugin loaded successfully");
        Ok(())
    }

    fn on_unload(&mut self) {
        log::info!("Unloading Twine Geyser Plugin - initiating graceful shutdown");

        // Set shutdown flag to stop accepting new data
        self.shutdown_flag.store(true, Ordering::SeqCst);

        // Stop accepting new slot updates
        self.is_startup_completed.store(false, Ordering::SeqCst);

        // Give threads a moment to finish current operations
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Force complete all active slots by marking them ready
        log::info!("Completing all active slots...");
        let active_slots = self.airlock.get_active_slots();

        for slot in active_slots {
            self.airlock.mark_slot_ready(slot);
        }

        // Wait for aggregators to process all ready slots
        let mut retries = 50; // 5 seconds max
        while retries > 0 && self.airlock.get_ready_queue_len() > 0 {
            std::thread::sleep(std::time::Duration::from_millis(100));
            retries -= 1;
        }

        if retries == 0 {
            log::warn!(
                "Timeout waiting for ready queue to drain, {} slots remaining",
                self.airlock.get_ready_queue_len()
            );
        }

        // Send shutdown command to all DB writers
        if let Some(sender) = &self.db_sender {
            log::info!("Sending shutdown signal to DB writers...");
            for _ in 0..self.config.db_writer_threads {
                let _ = sender.send(DbCommand::Shutdown);
            }
        }

        // Wait for DB writer threads to finish
        if let Some(threads) = self.db_writer_threads.take() {
            log::info!(
                "Waiting for {} DB writer threads to finish...",
                threads.len()
            );
            for (i, thread) in threads.into_iter().enumerate() {
                match thread.join() {
                    Ok(_) => log::debug!("DB writer thread {} shut down cleanly", i),
                    Err(_) => log::error!("DB writer thread {} panicked during shutdown", i),
                }
            }
        }

        // Shut down aggregator threads
        if let Some(threads) = self.aggregator_threads.take() {
            log::info!(
                "Waiting for {} aggregator threads to finish...",
                threads.len()
            );
            for (i, thread) in threads.into_iter().enumerate() {
                match thread.join() {
                    Ok(_) => log::debug!("Aggregator thread {} shut down cleanly", i),
                    Err(_) => log::error!("Aggregator thread {} panicked during shutdown", i),
                }
            }
        }

        // Shutdown metrics thread
        if let Some(thread) = self.metrics_thread.take() {
            log::info!("Shutting down metrics server...");
            // The metrics server will exit when the runtime is dropped
            match thread.join() {
                Ok(_) => log::debug!("Metrics thread shut down cleanly"),
                Err(_) => log::error!("Metrics thread panicked during shutdown"),
            }
        }

        // Log final statistics
        let stats = self.airlock.get_stats();
        log::info!("Plugin shutdown complete. Final stats:");
        log::info!("  Active slots: {}", stats.active_slots);
        log::info!("  Ready slots: {}", stats.ready_slots);
        log::info!("  Total account changes: {}", stats.total_account_changes);
        log::info!(
            "  Total vote transactions: {}",
            self.airlock.get_total_vote_transactions()
        );

        let metrics = self.airlock.get_metrics();
        log::info!(
            "  Slots completed: {}",
            metrics.slots_completed.load(Ordering::Relaxed)
        );
        log::info!(
            "  Slots dropped: {}",
            metrics.slots_dropped.load(Ordering::Relaxed)
        );
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        _slot: Slot,
        is_startup: bool,
    ) -> PluginResult<()> {
        // Skip during startup
        if is_startup || !self.is_startup_completed.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Extract account info
        let account_info = match account {
            ReplicaAccountInfoVersions::V0_0_3(info) => info,
            _ => return Ok(()),
        };

        // Check if this is a monitored account
        let pubkey_str = bs58::encode(account_info.pubkey).into_string();
        if !self.config.monitored_accounts.contains(&pubkey_str) {
            return Ok(());
        }

        // We can't directly create OwnedAccountChange here because we don't have
        // the old account data and LT hashes. This would come from the enhanced
        // notifications in the bank.

        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        log::info!("End of startup notification received");
        self.is_startup_completed.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: Slot,
        _parent: Option<Slot>,
        status: &SlotStatus,
    ) -> PluginResult<()> {
        // Convert status
        let airlock_status = match status {
            SlotStatus::Processed => AirLockSlotStatus::Processed,
            SlotStatus::Confirmed => AirLockSlotStatus::Confirmed,
            SlotStatus::Rooted => AirLockSlotStatus::Rooted,
            // Handle other statuses as processed
            _ => return Ok(()),
        };

        // Update slot status
        self.airlock.update_slot_status(slot, airlock_status);

        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: Slot,
    ) -> PluginResult<()> {
        // Skip during startup
        if !self.is_startup_completed.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Extract transaction info
        let transaction_info = match transaction {
            ReplicaTransactionInfoVersions::V0_0_2(info) => info,
            _ => return Ok(()),
        };

        // Check if this is a vote transaction
        let transaction = &transaction_info.transaction;
        if is_vote_transaction(transaction) {
            // Extract vote details
            if let Some(vote_tx) = extract_vote_transaction(transaction, slot, transaction_info) {
                // Buffer the vote transaction
                let _writer = self.airlock.begin_write(slot);
                self.airlock.buffer_vote_transaction(vote_tx);
            }
        }

        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> PluginResult<()> {
        // Skip during startup
        if !self.is_startup_completed.load(Ordering::Relaxed) {
            return Ok(());
        }

        let block_info = match blockinfo {
            ReplicaBlockInfoVersions::V0_0_3(info) => info,
            ReplicaBlockInfoVersions::V0_0_4(info) => {
                // V4 has same fields we need
                let metadata = BlockMetadata {
                    blockhash: info.blockhash.to_string(),
                    parent_slot: info.parent_slot,
                    executed_transaction_count: info.executed_transaction_count,
                    entry_count: info.entry_count,
                };
                self.airlock.set_block_metadata(info.slot, metadata);
                return Ok(());
            }
            _ => return Ok(()),
        };

        // Create block metadata from v3
        let metadata = BlockMetadata {
            blockhash: block_info.blockhash.to_string(),
            parent_slot: block_info.parent_slot,
            executed_transaction_count: block_info.executed_transaction_count,
            entry_count: block_info.entry_count,
        };

        self.airlock.set_block_metadata(block_info.slot, metadata);
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        false
    }
}

// Helper to check if transaction is a simple vote
fn is_vote_transaction(transaction: &SanitizedTransaction) -> bool {
    if transaction.message().instructions().len() != 1 {
        return false;
    }

    let instruction = &transaction.message().instructions()[0];
    let program_id = transaction.message().account_keys()[instruction.program_id_index as usize];

    program_id == solana_sdk::vote::program::id()
}

// Extract vote transaction details (minimal data for ZK proof)
fn extract_vote_transaction(
    transaction: &SanitizedTransaction,
    slot: Slot,
    _tx_info: &agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
) -> Option<VoteTransaction> {
    let instruction = &transaction.message().instructions()[0];
    let account_keys = transaction.message().account_keys();

    // Get voter pubkey (first account in vote instruction)
    let voter_pubkey = account_keys.get(*instruction.accounts.get(0)? as usize)?;

    // Get signature
    let signature = transaction.signature();

    // Serialize full transaction for ZK proof verification
    let vote_transaction = bincode::serialize(&transaction.to_versioned_transaction()).ok()?;

    // Extract vote type for filtering (especially TowerSync)
    let vote_type = match instruction.data.get(0)? {
        0 => "vote",
        1 => "tower_sync",
        2 => "tower_sync_switch",
        3 => "compact_vote_state_update",
        _ => "unknown",
    }
    .to_string();

    Some(VoteTransaction {
        slot,
        voter_pubkey: *voter_pubkey,
        vote_signature: signature.to_string(),
        vote_transaction,
        transaction_meta: None, // Not needed for ZK proof
        vote_type,
        vote_slot: None, // Not needed for basic verification
        vote_hash: None,
        root_slot: None,
        lockouts_count: None,
        timestamp: None,
    })
}

/// Enhanced notification methods that would be called by the bank
impl TwineGeyserPlugin {
    /// Called when bank hash components are available during freeze
    pub fn notify_bank_hash_components(&self, components: BankHashComponents) {
        let slot = components.slot;

        // Use RAII pattern for write tracking
        let _writer = self.airlock.begin_write(slot);

        // Set bank hash components
        self.airlock.set_bank_hash_components(slot, components);

        // Writer automatically dropped, decrementing counter
    }

    /// Called with all account changes for a slot
    pub fn notify_account_changes(&self, slot: Slot, changes: Vec<OwnedAccountChange>) {
        // Use RAII pattern for write tracking
        let _writer = self.airlock.begin_write(slot);

        // Buffer all changes
        for change in changes {
            self.airlock.buffer_account_change(change);
        }

        // Writer automatically dropped, decrementing counter
    }
}
