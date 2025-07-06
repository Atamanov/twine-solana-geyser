use crate::airlock::types::{DbWriteCommand, PluginConfig};
use crate::airlock::AirlockStats;
use chrono;
use crossbeam_channel::Receiver;
use deadpool_postgres::{Config as DbConfig, Pool, Runtime as DbRuntime};
use log::*;
use solana_sdk::account::ReadableAccount;
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;
use tokio_postgres::NoTls;

#[derive(Debug)]
pub struct WorkerPool {
    handles: Vec<thread::JoinHandle<()>>,
}

impl WorkerPool {
    pub fn start(
        receiver: Receiver<DbWriteCommand>,
        config: PluginConfig,
        stats: Arc<AirlockStats>,
    ) -> Self {
        let mut handles = Vec::new();

        for worker_id in 0..config.num_worker_threads {
            let rx = receiver.clone();
            let cfg = config.clone();
            let stats_clone = stats.clone();

            let handle = thread::spawn(move || {
                let runtime = Runtime::new().unwrap();
                runtime.block_on(async {
                    worker_loop(worker_id, rx, cfg, stats_clone).await;
                });
            });

            handles.push(handle);
        }

        WorkerPool { handles }
    }

    pub fn join(self) {
        for handle in self.handles {
            let _ = handle.join();
        }
    }
}

async fn worker_loop(
    worker_id: usize,
    receiver: Receiver<DbWriteCommand>,
    config: PluginConfig,
    stats: Arc<AirlockStats>,
) {
    info!("Worker {} starting", worker_id);

    // Create database connection pool
    let mut db_config = DbConfig::new();
    db_config.user = Some(config.db_user);
    db_config.password = Some(config.db_password);
    db_config.host = Some(config.db_host);
    db_config.port = Some(config.db_port);
    db_config.dbname = Some(config.db_name);

    let pool = db_config
        .create_pool(Some(DbRuntime::Tokio1), NoTls)
        .unwrap();

    let mut batch_slots = Vec::new();
    let mut batch_changes = Vec::new();
    let mut batch_proofs = Vec::new();
    let mut batch_status_updates = Vec::new();
    let mut last_batch_time = std::time::Instant::now();

    loop {
        // Try to receive with timeout
        match receiver.recv_timeout(std::time::Duration::from_millis(config.batch_timeout_ms)) {
            Ok(cmd) => {
                match cmd {
                    DbWriteCommand::Shutdown => {
                        info!("Worker {} shutting down", worker_id);
                        break;
                    }
                    DbWriteCommand::SlotData { .. } => batch_slots.push(cmd),
                    DbWriteCommand::SlotStatusUpdate { .. } => batch_status_updates.push(cmd),
                    DbWriteCommand::AccountChanges { .. } => batch_changes.push(cmd),
                    DbWriteCommand::ProofRequests { .. } => batch_proofs.push(cmd),
                }

                // Check if we should flush
                let should_flush = batch_slots.len() >= config.batch_size
                    || batch_changes.len() >= config.batch_size
                    || batch_proofs.len() >= config.batch_size
                    || batch_status_updates.len() >= config.batch_size
                    || last_batch_time.elapsed()
                        > std::time::Duration::from_millis(config.batch_timeout_ms);

                if should_flush {
                    flush_batches(
                        &pool,
                        &stats,
                        &mut batch_slots,
                        &mut batch_changes,
                        &mut batch_proofs,
                        &mut batch_status_updates,
                    )
                    .await;
                    last_batch_time = std::time::Instant::now();
                }
            }
            Err(_) => {
                // Timeout - flush any pending data
                flush_batches(
                    &pool,
                    &stats,
                    &mut batch_slots,
                    &mut batch_changes,
                    &mut batch_proofs,
                    &mut batch_status_updates,
                )
                .await;
                last_batch_time = std::time::Instant::now();
            }
        }
    }

    // Final flush before shutdown
    flush_batches(
        &pool,
        &stats,
        &mut batch_slots,
        &mut batch_changes,
        &mut batch_proofs,
        &mut batch_status_updates,
    )
    .await;
    info!("Worker {} stopped", worker_id);
}

async fn flush_batches(
    pool: &Pool,
    stats: &Arc<AirlockStats>,
    slots: &mut Vec<DbWriteCommand>,
    changes: &mut Vec<DbWriteCommand>,
    proofs: &mut Vec<DbWriteCommand>,
    status_updates: &mut Vec<DbWriteCommand>,
) {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to get database connection: {}", e);
            stats
                .db_batch_error_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return;
        }
    };

    let mut success = true;

    // Process slots
    if !slots.is_empty() {
        match process_slot_batch(&client, slots).await {
            Ok(count) => debug!("Inserted {} slots", count),
            Err(e) => {
                error!("Failed to insert slots: {}", e);
                success = false;
            }
        }
    }

    // Process slot status updates
    if !status_updates.is_empty() {
        match process_slot_status_updates(&client, status_updates).await {
            Ok(count) => debug!("Updated {} slot statuses", count),
            Err(e) => {
                error!("Failed to update slot statuses: {}", e);
                success = false;
            }
        }
    }

    // Process account changes
    if !changes.is_empty() {
        match process_account_changes_batch(&client, changes).await {
            Ok(count) => debug!("Inserted {} account changes", count),
            Err(e) => {
                error!("Failed to insert account changes: {}", e);
                success = false;
            }
        }
    }

    // Process proof requests
    if !proofs.is_empty() {
        match process_proof_requests_batch(&client, proofs).await {
            Ok(count) => debug!("Inserted {} proof requests", count),
            Err(e) => {
                error!("Failed to insert proof requests: {}", e);
                success = false;
            }
        }
    }

    if success {
        stats
            .db_batch_success_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Find max slot from all slot-related commands
        let max_slot = slots
            .iter()
            .filter_map(|cmd| match cmd {
                DbWriteCommand::SlotData { slot, .. } => Some(*slot),
                _ => None,
            })
            .chain(status_updates.iter().filter_map(|cmd| match cmd {
                DbWriteCommand::SlotStatusUpdate { slot, .. } => Some(*slot),
                _ => None,
            }))
            .max()
            .unwrap_or(0);

        stats
            .last_db_batch_slot
            .store(max_slot as usize, std::sync::atomic::Ordering::Relaxed);
        stats.last_db_batch_timestamp.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as usize,
            std::sync::atomic::Ordering::Relaxed,
        );
    } else {
        stats
            .db_batch_error_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    slots.clear();
    changes.clear();
    proofs.clear();
    status_updates.clear();
}

async fn process_slot_batch(
    client: &deadpool_postgres::Object,
    slots: &[DbWriteCommand],
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut count = 0;

    for cmd in slots {
        if let DbWriteCommand::SlotData {
            slot,
            bank_hash,
            parent_bank_hash,
            signature_count,
            last_blockhash,
            delta_lthash,
            cumulative_lthash,
            accounts_delta_hash,
            accounts_lthash_checksum,
            epoch_accounts_hash,
            blockhash,
            parent_slot,
            executed_transaction_count,
            entry_count,
        } = cmd
        {
            let query = r#"
                INSERT INTO slots (
                    slot, bank_hash, parent_bank_hash, signature_count, 
                    last_blockhash, cumulative_lthash, delta_lthash,
                    accounts_delta_hash, accounts_lthash_checksum, epoch_accounts_hash,
                    status, rooted_at, blockhash, parent_slot, executed_transaction_count, entry_count,
                    block_metadata_updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'rooted', NOW(), $11, $12, $13, $14, $15)
                ON CONFLICT (slot) DO UPDATE SET
                    bank_hash = EXCLUDED.bank_hash,
                    parent_bank_hash = EXCLUDED.parent_bank_hash,
                    signature_count = EXCLUDED.signature_count,
                    last_blockhash = EXCLUDED.last_blockhash,
                    cumulative_lthash = EXCLUDED.cumulative_lthash,
                    delta_lthash = EXCLUDED.delta_lthash,
                    accounts_delta_hash = EXCLUDED.accounts_delta_hash,
                    accounts_lthash_checksum = EXCLUDED.accounts_lthash_checksum,
                    epoch_accounts_hash = EXCLUDED.epoch_accounts_hash,
                    status = 'rooted',
                    rooted_at = NOW(),
                    blockhash = COALESCE(EXCLUDED.blockhash, slots.blockhash),
                    parent_slot = COALESCE(EXCLUDED.parent_slot, slots.parent_slot),
                    executed_transaction_count = COALESCE(EXCLUDED.executed_transaction_count, slots.executed_transaction_count),
                    entry_count = COALESCE(EXCLUDED.entry_count, slots.entry_count),
                    block_metadata_updated_at = CASE WHEN EXCLUDED.blockhash IS NOT NULL THEN NOW() ELSE slots.block_metadata_updated_at END
            "#;

            client
                .execute(
                    query,
                    &[
                        &(*slot as i64),
                        &bank_hash,
                        &parent_bank_hash,
                        &(*signature_count as i64),
                        &last_blockhash,
                        &delta_lthash,
                        &cumulative_lthash,
                        &accounts_delta_hash,
                        &accounts_lthash_checksum,
                        &epoch_accounts_hash,
                        &blockhash,
                        &parent_slot.map(|s| s as i64),
                        &executed_transaction_count.map(|c| c as i64),
                        &entry_count.map(|c| c as i64),
                        &blockhash.as_ref().map(|_| chrono::Utc::now()),
                    ],
                )
                .await?;

            count += 1;
        }
    }

    Ok(count)
}

async fn process_account_changes_batch(
    client: &deadpool_postgres::Object,
    changes: &[DbWriteCommand],
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut count = 0;

    for cmd in changes {
        if let DbWriteCommand::AccountChanges { slot, changes } = cmd {
            for change in changes {
                let query = r#"
                    INSERT INTO account_changes (
                        &(*slot as i64), account_pubkey, write_version,
                        old_lamports, old_owner, old_executable, old_rent_epoch, old_data, old_lthash,
                        new_lamports, new_owner, new_executable, new_rent_epoch, new_data, new_lthash
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                "#;

                client
                    .execute(
                        query,
                        &[
                            &(*slot as i64),
                            &change.pubkey.to_string(),
                            &0i64, // write_version not available in OwnedAccountChange
                            &(change.old_account.lamports() as i64),
                            &change.old_account.owner().to_string(),
                            &change.old_account.executable(),
                            &(change.old_account.rent_epoch() as i64),
                            &change.old_account.data(),
                            &change
                                .old_lthash
                                .0
                                .iter()
                                .flat_map(|&x| x.to_le_bytes())
                                .collect::<Vec<u8>>(),
                            &(change.new_account.lamports() as i64),
                            &change.new_account.owner().to_string(),
                            &change.new_account.executable(),
                            &(change.new_account.rent_epoch() as i64),
                            &change.new_account.data(),
                            &change
                                .new_lthash
                                .0
                                .iter()
                                .flat_map(|&x| x.to_le_bytes())
                                .collect::<Vec<u8>>(),
                        ],
                    )
                    .await?;

                count += 1;
            }
        }
    }

    Ok(count)
}

async fn process_proof_requests_batch(
    client: &deadpool_postgres::Object,
    proofs: &[DbWriteCommand],
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut count = 0;

    for cmd in proofs {
        if let DbWriteCommand::ProofRequests { requests } = cmd {
            for request in requests {
                let query = r#"
                    INSERT INTO proof_requests (slot, account_pubkey)
                    VALUES ($1, $2)
                "#;

                client
                    .execute(query, &[&(request.slot as i64), &request.account_pubkey])
                    .await?;

                count += 1;
            }
        }
    }

    Ok(count)
}

async fn process_slot_status_updates(
    client: &deadpool_postgres::Object,
    updates: &[DbWriteCommand],
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut count = 0;

    for cmd in updates {
        if let DbWriteCommand::SlotStatusUpdate { slot, status } = cmd {
            let timestamp_field = match status.as_str() {
                "first_shred_received" => "first_shred_received_at",
                "completed" => "completed_at",
                "processed" => "processed_at",
                "confirmed" => "confirmed_at",
                "rooted" => "rooted_at",
                _ => continue,
            };

            // First try to update existing slot record
            let update_query = format!(
                "UPDATE slots SET status = $1, {} = NOW() WHERE slot = $2",
                timestamp_field
            );

            let rows_updated = client
                .execute(&update_query, &[status, &(*slot as i64)])
                .await?;

            // If no rows were updated, insert a new record with minimal data
            if rows_updated == 0 {
                let insert_query = format!(
                    "INSERT INTO slots (slot, bank_hash, parent_bank_hash, signature_count, last_blockhash, cumulative_lthash, delta_lthash, status, {}) 
                     VALUES ($1, '', '', 0, '', '\\x00', '\\x00', $2, NOW()) 
                     ON CONFLICT (slot) DO UPDATE SET status = $2, {} = NOW()",
                    timestamp_field, timestamp_field
                );

                client
                    .execute(&insert_query, &[&(*slot as i64), status])
                    .await?;
            }

            count += 1;
        }
    }

    Ok(count)
}
