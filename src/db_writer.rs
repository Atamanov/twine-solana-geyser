use bytemuck;
use crossbeam::channel::{bounded, Receiver, Sender};
use deadpool_postgres::{Config, Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use solana_sdk::{account::ReadableAccount, clock::Slot};
use std::thread;
use std::time::Duration;
use tokio_postgres::{types::ToSql, NoTls};

use crate::airlock::{OwnedAccountChange, VoteTransaction};

/// Database write commands
#[derive(Debug)]
pub enum DbCommand {
    WriteSlot(SlotData),
    WriteAccountChanges(Vec<OwnedAccountChange>),
    WriteVoteTransactions(Vec<VoteTransaction>),
    Shutdown,
}

#[derive(Debug)]
pub struct SlotData {
    pub slot: Slot,
    pub bank_hash: String,
    pub parent_bank_hash: String,
    pub signature_count: u64,
    pub last_blockhash: String,
    pub cumulative_lthash: Vec<u8>,
    pub delta_lthash: Vec<u8>,
    pub accounts_delta_hash: Option<String>,
    pub accounts_lthash_checksum: Option<String>,
    pub epoch_accounts_hash: Option<String>,
    pub status: String,
    pub vote_count: i64,
    // Block metadata fields
    pub blockhash: Option<String>,
    pub parent_slot: Option<i64>,
    pub executed_transaction_count: Option<i64>,
    pub entry_count: Option<i64>,
}

/// Database writer worker
pub struct DbWriter {
    pool: Pool,
    receiver: Receiver<DbCommand>,
}

impl DbWriter {
    pub fn new(pool: Pool, receiver: Receiver<DbCommand>) -> Self {
        Self { pool, receiver }
    }

    /// Run the database writer loop
    pub async fn run(self) {
        loop {
            // Try to receive with timeout
            match self.receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(DbCommand::Shutdown) => {
                    log::info!("Database writer shutting down");
                    break;
                }
                Ok(cmd) => {
                    self.process_command(cmd).await;
                }
                Err(_) => {
                    // No command received, continue
                }
            }
        }
    }

    async fn process_command(&self, cmd: DbCommand) {
        match cmd {
            DbCommand::WriteSlot(slot_data) => {
                match self.write_slot(slot_data).await {
                    Ok(_) => {
                        // Increment success counter
                        crate::metrics::increment_write_success("slot");
                    }
                    Err(e) => {
                        log::error!("Failed to write slot: {:?}", e);
                        crate::metrics::increment_write_error("slot", "write_error");
                    }
                }
            }
            DbCommand::WriteAccountChanges(changes) => {
                let count = changes.len();
                match self.write_account_changes(changes).await {
                    Ok(_) => {
                        // Increment success counter
                        crate::metrics::increment_account_changes_written(count as u64);
                    }
                    Err(e) => {
                        log::error!("Failed to write account changes: {:?}", e);
                        crate::metrics::increment_write_error("account_changes", "write_error");
                    }
                }
            }
            DbCommand::WriteVoteTransactions(votes) => {
                let count = votes.len();
                match self.write_vote_transactions(votes).await {
                    Ok(_) => {
                        // Increment success counter
                        crate::metrics::increment_vote_transactions_written(count as u64);
                    }
                    Err(e) => {
                        log::error!("Failed to write vote transactions: {:?}", e);
                        crate::metrics::increment_write_error("vote_transactions", "write_error");
                    }
                }
            }
            DbCommand::Shutdown => unreachable!(),
        }
    }

    async fn write_slot(&self, slot: SlotData) -> Result<(), Box<dyn std::error::Error>> {
        let client = self.pool.get().await?;

        let stmt = client
            .prepare_cached(
                "INSERT INTO slots (
                slot, bank_hash, parent_bank_hash, signature_count, last_blockhash,
                cumulative_lthash, delta_lthash, accounts_delta_hash, accounts_lthash_checksum,
                epoch_accounts_hash, status, rooted_at, vote_count,
                blockhash, parent_slot, executed_transaction_count, entry_count, block_metadata_updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), $12, $13, $14, $15, $16, $17)
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
                status = EXCLUDED.status,
                rooted_at = NOW(),
                vote_count = EXCLUDED.vote_count,
                blockhash = COALESCE(EXCLUDED.blockhash, slots.blockhash),
                parent_slot = COALESCE(EXCLUDED.parent_slot, slots.parent_slot),
                executed_transaction_count = COALESCE(EXCLUDED.executed_transaction_count, slots.executed_transaction_count),
                entry_count = COALESCE(EXCLUDED.entry_count, slots.entry_count),
                block_metadata_updated_at = CASE WHEN EXCLUDED.blockhash IS NOT NULL THEN NOW() ELSE slots.block_metadata_updated_at END",
            )
            .await?;

        let block_metadata_updated_at: Option<std::time::SystemTime> = if slot.blockhash.is_some() {
            Some(std::time::SystemTime::now())
        } else {
            None
        };

        client
            .execute(
                &stmt,
                &[
                    &(slot.slot as i64),
                    &slot.bank_hash,
                    &slot.parent_bank_hash,
                    &(slot.signature_count as i64),
                    &slot.last_blockhash,
                    &slot.cumulative_lthash,
                    &slot.delta_lthash,
                    &slot.accounts_delta_hash,
                    &slot.accounts_lthash_checksum,
                    &slot.epoch_accounts_hash,
                    &slot.status,
                    &slot.vote_count,
                    &slot.blockhash,
                    &slot.parent_slot,
                    &slot.executed_transaction_count,
                    &slot.entry_count,
                    &block_metadata_updated_at,
                ],
            )
            .await?;

        Ok(())
    }

    async fn write_account_changes(
        &self,
        changes: Vec<OwnedAccountChange>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if changes.is_empty() {
            return Ok(());
        }

        let client = self.pool.get().await?;

        // Prepare batch insert statement
        let mut query = String::from(
            "INSERT INTO account_changes (
                slot, account_pubkey, write_version,
                old_lamports, old_owner, old_executable, old_rent_epoch, old_data, old_lthash,
                new_lamports, new_owner, new_executable, new_rent_epoch, new_data, new_lthash
            ) VALUES ",
        );

        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        let mut param_idx = 1;

        for (i, change) in changes.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }

            query.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                param_idx,
                param_idx + 1,
                param_idx + 2,
                param_idx + 3,
                param_idx + 4,
                param_idx + 5,
                param_idx + 6,
                param_idx + 7,
                param_idx + 8,
                param_idx + 9,
                param_idx + 10,
                param_idx + 11,
                param_idx + 12,
                param_idx + 13,
                param_idx + 14
            ));

            params.push(Box::new(change.slot as i64));
            params.push(Box::new(change.pubkey.to_string()));
            params.push(Box::new(0i64)); // write_version not available from geyser
            params.push(Box::new(change.old_account.lamports() as i64));
            params.push(Box::new(change.old_account.owner().to_string()));
            params.push(Box::new(change.old_account.executable()));
            params.push(Box::new(change.old_account.rent_epoch() as i64));
            params.push(Box::new(change.old_account.data().to_vec()));
            params.push(Box::new(
                bytemuck::cast_slice::<u16, u8>(&change.old_lthash.0).to_vec(),
            ));
            params.push(Box::new(change.new_account.lamports() as i64));
            params.push(Box::new(change.new_account.owner().to_string()));
            params.push(Box::new(change.new_account.executable()));
            params.push(Box::new(change.new_account.rent_epoch() as i64));
            params.push(Box::new(change.new_account.data().to_vec()));
            params.push(Box::new(
                bytemuck::cast_slice::<u16, u8>(&change.new_lthash.0).to_vec(),
            ));

            param_idx += 15;
        }

        query.push_str(" ON CONFLICT (slot, account_pubkey, write_version) DO NOTHING");

        let params_refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();

        client.execute(&query, &params_refs).await?;
        Ok(())
    }

    async fn write_vote_transactions(
        &self,
        votes: Vec<VoteTransaction>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if votes.is_empty() {
            return Ok(());
        }

        let client = self.pool.get().await?;

        // Prepare statement for bulk insert
        let stmt = client
            .prepare_cached(
                "INSERT INTO vote_transactions (
                slot, voter_pubkey, vote_signature, vote_transaction, transaction_meta,
                vote_type, vote_slot, vote_hash, root_slot, lockouts_count, timestamp
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (slot, voter_pubkey, vote_signature) DO NOTHING",
            )
            .await?;

        // Insert in batches
        for vote in votes {
            client
                .execute(
                    &stmt,
                    &[
                        &(vote.slot as i64),
                        &vote.voter_pubkey.to_string(),
                        &vote.vote_signature,
                        &vote.vote_transaction,
                        &vote.transaction_meta,
                        &vote.vote_type,
                        &vote.vote_slot.map(|s| s as i64),
                        &vote.vote_hash,
                        &vote.root_slot.map(|s| s as i64),
                        &vote.lockouts_count.map(|c| c as i32),
                        &vote.timestamp,
                    ],
                )
                .await?;
        }

        Ok(())
    }
}

/// Database connection configuration
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub pool_size: usize,
}

/// Create database connection pool
pub fn create_pool(config: &DbConfig) -> Result<Pool, Box<dyn std::error::Error>> {
    let mut cfg = Config::new();
    cfg.host = Some(config.host.clone());
    cfg.port = Some(config.port);
    cfg.user = Some(config.user.clone());
    cfg.password = Some(config.password.clone());
    cfg.dbname = Some(config.dbname.clone());

    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    let pg_config: tokio_postgres::Config = cfg.get_pg_config().unwrap();
    let manager = Manager::new(pg_config, NoTls);
    let pool = Pool::builder(manager)
        .max_size(config.pool_size)
        .runtime(Runtime::Tokio1)
        .build()?;

    Ok(pool)
}

/// Spawn database writer threads
pub fn spawn_db_writers(
    pool: Pool,
    num_threads: usize,
) -> (Vec<thread::JoinHandle<()>>, Sender<DbCommand>) {
    let (sender, receiver) = bounded(10000);

    let handles = (0..num_threads)
        .map(|i| {
            let pool = pool.clone();
            let receiver = receiver.clone();

            thread::Builder::new()
                .name(format!("db-writer-{}", i))
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("Failed to create tokio runtime");

                    runtime.block_on(async {
                        let writer = DbWriter::new(pool, receiver);
                        writer.run().await;
                    });
                })
                .expect("Failed to spawn db writer thread")
        })
        .collect();

    (handles, sender)
}
