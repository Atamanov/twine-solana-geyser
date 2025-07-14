use crate::airlock::types::DbWriteCommand;
use crate::airlock::AirlockStats;
use chrono;
use crossbeam_channel::Receiver;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use log::*;
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;
use tokio_postgres::NoTls;

pub struct WorkerPool {
    handles: Vec<thread::JoinHandle<()>>,
}

impl WorkerPool {
    pub fn new(
        worker_count: usize,
        receiver: Receiver<DbWriteCommand>,
        connection_string: &str,
        batch_size: usize,
        batch_timeout_ms: u64,
        stats: Arc<AirlockStats>,
    ) -> Self {
        let mut handles = Vec::new();

        for worker_id in 0..worker_count {
            let rx = receiver.clone();
            let conn_str = connection_string.to_string();
            let stats_clone = stats.clone();

            let handle = thread::spawn(move || {
                let runtime = Runtime::new().unwrap();
                runtime.block_on(async {
                    worker_loop(
                        worker_id,
                        rx,
                        conn_str,
                        batch_size,
                        batch_timeout_ms,
                        stats_clone,
                    )
                    .await;
                });
            });

            handles.push(handle);
        }

        stats.worker_pool_size.store(worker_count, Ordering::Relaxed);

        WorkerPool { handles }
    }

    pub fn shutdown(self) {
        info!("Shutting down worker pool");
        for handle in self.handles {
            let _ = handle.join();
        }
    }
}

async fn worker_loop(
    worker_id: usize,
    receiver: Receiver<DbWriteCommand>,
    connection_string: String,
    batch_size: usize,
    batch_timeout_ms: u64,
    stats: Arc<AirlockStats>,
) {
    info!("Worker {} starting", worker_id);

    // Parse connection string and create pool directly
    let config = match connection_string.parse::<tokio_postgres::Config>() {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Worker {}: Failed to parse connection string: {}", worker_id, e);
            return;
        }
    };

    // Create pool with tokio-postgres config
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let mgr = Manager::from_config(config, NoTls, mgr_config);
    let pool = Pool::builder(mgr).max_size(2).build().unwrap();

    let mut batch_slots = Vec::new();
    let mut batch_changes = Vec::new();
    let mut batch_status_updates = Vec::new();
    let mut batch_epoch_stakes = Vec::new();
    let mut batch_stake_accounts = Vec::new();
    let mut batch_epoch_validators = Vec::new();
    let mut last_batch_time = std::time::Instant::now();

    loop {
        // Try to receive with timeout
        let timeout = std::time::Duration::from_millis(batch_timeout_ms.max(100));
        match receiver.recv_timeout(timeout) {
            Ok(cmd) => {
                match cmd {
                    DbWriteCommand::SlotData { .. } => batch_slots.push(cmd),
                    DbWriteCommand::SlotStatusUpdate { .. } => batch_status_updates.push(cmd),
                    DbWriteCommand::AccountChanges { .. } => batch_changes.push(cmd),
                    DbWriteCommand::ProofRequests { .. } => {
                        // Proof requests are no longer processed by the geyser plugin
                        debug!("Ignoring proof request command");
                    }
                    DbWriteCommand::EpochStakes { .. } => batch_epoch_stakes.push(cmd),
                    DbWriteCommand::StakeAccountChange { .. } => batch_stake_accounts.push(cmd),
                    DbWriteCommand::EpochValidatorSet { .. } => batch_epoch_validators.push(cmd),
                    DbWriteCommand::Shutdown => {
                        info!("Worker {} received shutdown command", worker_id);
                        return;
                    }
                }

                // Check if we should flush
                let should_flush = batch_slots.len() >= batch_size
                    || batch_changes.len() >= batch_size
                    || batch_status_updates.len() >= batch_size
                    || batch_epoch_stakes.len() >= batch_size
                    || batch_stake_accounts.len() >= batch_size
                    || batch_epoch_validators.len() >= batch_size
                    || last_batch_time.elapsed() > timeout;

                if should_flush {
                    flush_batches(
                        &pool,
                        &mut batch_slots,
                        &mut batch_changes,
                        &mut batch_status_updates,
                        &mut batch_epoch_stakes,
                        &mut batch_stake_accounts,
                        &mut batch_epoch_validators,
                        &stats,
                        worker_id,
                    )
                    .await;
                    last_batch_time = std::time::Instant::now();
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                // Flush on timeout
                if !batch_slots.is_empty()
                    || !batch_changes.is_empty()
                    || !batch_status_updates.is_empty()
                    || !batch_epoch_stakes.is_empty()
                    || !batch_stake_accounts.is_empty()
                    || !batch_epoch_validators.is_empty()
                {
                    flush_batches(
                        &pool,
                        &mut batch_slots,
                        &mut batch_changes,
                        &mut batch_status_updates,
                        &mut batch_epoch_stakes,
                        &mut batch_stake_accounts,
                        &mut batch_epoch_validators,
                        &stats,
                        worker_id,
                    )
                    .await;
                    last_batch_time = std::time::Instant::now();
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                info!("Worker {} shutting down - channel disconnected", worker_id);
                break;
            }
        }

        // Update queue depth
        let queue_len = receiver.len();
        stats.queue_depth.store(queue_len, Ordering::Relaxed);
    }
}

async fn flush_batches(
    pool: &Pool,
    batch_slots: &mut Vec<DbWriteCommand>,
    batch_changes: &mut Vec<DbWriteCommand>,
    batch_status_updates: &mut Vec<DbWriteCommand>,
    batch_epoch_stakes: &mut Vec<DbWriteCommand>,
    batch_stake_accounts: &mut Vec<DbWriteCommand>,
    batch_epoch_validators: &mut Vec<DbWriteCommand>,
    stats: &Arc<AirlockStats>,
    worker_id: usize,
) {
    let total_items = batch_slots.len()
        + batch_changes.len()
        + batch_status_updates.len()
        + batch_epoch_stakes.len()
        + batch_stake_accounts.len()
        + batch_epoch_validators.len();

    if total_items == 0 {
        return;
    }

    debug!(
        "Worker {}: Flushing {} items (slots={}, changes={}, status={}, epoch_stakes={}, stake_accounts={}, validators={})",
        worker_id,
        total_items,
        batch_slots.len(),
        batch_changes.len(),
        batch_status_updates.len(),
        batch_epoch_stakes.len(),
        batch_stake_accounts.len(),
        batch_epoch_validators.len()
    );

    let start = std::time::Instant::now();

    match pool.get().await {
        Ok(mut client) => {
            let transaction = match client.transaction().await {
                Ok(tx) => tx,
                Err(e) => {
                    error!("Worker {}: Failed to start transaction: {}", worker_id, e);
                    stats.db_batch_error_count.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            // Process slot data
            for cmd in batch_slots.drain(..) {
                if let DbWriteCommand::SlotData {
                    slot,
                    bank_hash,
                    parent_bank_hash,
                    signature_count,
                    last_blockhash,
                    delta_lthash,
                    cumulative_lthash,
                    blockhash,
                    parent_slot,
                    executed_transaction_count,
                    entry_count,
                    vote_transactions,
                } = cmd
                {
                    if let Err(e) = write_slot_data(
                        &transaction,
                        slot,
                        &bank_hash,
                        &parent_bank_hash,
                        signature_count,
                        &last_blockhash,
                        &delta_lthash,
                        &cumulative_lthash,
                        blockhash.as_deref(),
                        parent_slot,
                        executed_transaction_count,
                        entry_count,
                        &vote_transactions,
                    )
                    .await
                    {
                        error!("Worker {}: Failed to write slot data: {}", worker_id, e);
                        stats.db_batch_error_count.fetch_add(1, Ordering::Relaxed);
                        let _ = transaction.rollback().await;
                        return;
                    }
                }
            }

            // Process account changes
            for cmd in batch_changes.drain(..) {
                if let DbWriteCommand::AccountChanges { slot, changes } = cmd {
                    if let Err(e) = write_account_changes(&transaction, slot, &changes).await {
                        error!("Worker {}: Failed to write account changes: {}", worker_id, e);
                        stats.db_batch_error_count.fetch_add(1, Ordering::Relaxed);
                        let _ = transaction.rollback().await;
                        return;
                    }
                }
            }

            // Process other commands...
            // TODO: Implement other batch processors

            // Commit transaction
            match transaction.commit().await {
                Ok(_) => {
                    let elapsed = start.elapsed();
                    debug!(
                        "Worker {}: Batch of {} items committed in {:?}",
                        worker_id, total_items, elapsed
                    );
                    stats.db_batch_success_count.fetch_add(1, Ordering::Relaxed);
                    stats.queue_throughput.fetch_add(total_items, Ordering::Relaxed);
                }
                Err(e) => {
                    error!("Worker {}: Failed to commit transaction: {}", worker_id, e);
                    stats.db_batch_error_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Err(e) => {
            error!("Worker {}: Failed to get database connection: {}", worker_id, e);
            stats.db_batch_error_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn write_slot_data(
    client: &tokio_postgres::Transaction<'_>,
    slot: u64,
    bank_hash: &str,
    parent_bank_hash: &str,
    signature_count: u64,
    last_blockhash: &str,
    delta_lthash: &[u8],
    cumulative_lthash: &[u8],
    blockhash: Option<&str>,
    parent_slot: Option<u64>,
    executed_transaction_count: Option<u64>,
    entry_count: Option<u64>,
    vote_transactions: &[crate::airlock::types::VoteTransaction],
) -> Result<(), tokio_postgres::Error> {
    // Insert slot data
    client
        .execute(
            "INSERT INTO slot_bank_hash_components (
                slot, bank_hash, parent_bank_hash, signature_count, 
                last_blockhash, delta_lthash, cumulative_lthash,
                blockhash, parent_slot, executed_transaction_count, entry_count,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (slot) DO UPDATE SET
                bank_hash = EXCLUDED.bank_hash,
                parent_bank_hash = EXCLUDED.parent_bank_hash,
                signature_count = EXCLUDED.signature_count,
                last_blockhash = EXCLUDED.last_blockhash,
                delta_lthash = EXCLUDED.delta_lthash,
                cumulative_lthash = EXCLUDED.cumulative_lthash,
                blockhash = COALESCE(EXCLUDED.blockhash, slot_bank_hash_components.blockhash),
                parent_slot = COALESCE(EXCLUDED.parent_slot, slot_bank_hash_components.parent_slot),
                executed_transaction_count = COALESCE(EXCLUDED.executed_transaction_count, slot_bank_hash_components.executed_transaction_count),
                entry_count = COALESCE(EXCLUDED.entry_count, slot_bank_hash_components.entry_count)",
            &[
                &(slot as i64),
                &bank_hash,
                &parent_bank_hash,
                &(signature_count as i64),
                &last_blockhash,
                &delta_lthash,
                &cumulative_lthash,
                &blockhash,
                &parent_slot.map(|s| s as i64),
                &executed_transaction_count.map(|c| c as i64),
                &entry_count.map(|c| c as i64),
                &chrono::Utc::now(),
            ],
        )
        .await?;

    // Insert vote transactions
    for vote in vote_transactions {
        client
            .execute(
                "INSERT INTO vote_transactions (
                    slot, voter_pubkey, vote_signature, vote_transaction,
                    transaction_meta, vote_type, vote_slot, vote_hash,
                    root_slot, lockouts_count, vote_timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
                &[
                    &(slot as i64),
                    &vote.voter_pubkey,
                    &vote.vote_signature,
                    &vote.vote_transaction,
                    &vote.transaction_meta,
                    &vote.vote_type,
                    &vote.vote_slot.map(|s| s as i64),
                    &vote.vote_hash,
                    &vote.root_slot.map(|s| s as i64),
                    &vote.lockouts_count.map(|c| c as i64),
                    &vote.timestamp,
                    &chrono::Utc::now(),
                ],
            )
            .await?;
    }

    Ok(())
}

async fn write_account_changes(
    client: &tokio_postgres::Transaction<'_>,
    slot: u64,
    changes: &[crate::airlock::optimized_types::InternalAccountChange],
) -> Result<(), tokio_postgres::Error> {
    for change in changes {
        client
            .execute(
                "INSERT INTO account_changes (
                    slot, pubkey, is_startup, created_at
                ) VALUES ($1, $2, $3, $4)",
                &[
                    &(slot as i64),
                    &change.pubkey.to_string(),
                    &change.is_startup,
                    &chrono::Utc::now(),
                ],
            )
            .await?;
    }
    Ok(())
}

use std::sync::atomic::Ordering;