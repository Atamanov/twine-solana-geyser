use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use crossbeam::channel::Sender;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use crate::airlock::{AggregatedSlotData, AirLock};
use crate::db_writer::{DbCommand, SlotData};

/// Aggregator thread that spins on ready slots and prepares data for DB writes
pub struct Aggregator {
    airlock: Arc<AirLock>,
    db_sender: Sender<DbCommand>,
    shutdown_flag: Arc<AtomicBool>,
}

impl Aggregator {
    pub fn new(airlock: Arc<AirLock>, db_sender: Sender<DbCommand>, shutdown_flag: Arc<AtomicBool>) -> Self {
        Self { airlock, db_sender, shutdown_flag }
    }

    /// Run the aggregator loop
    pub fn run(self) {
        loop {
            // Check for shutdown
            if self.shutdown_flag.load(Ordering::Relaxed) {
                log::info!("Aggregator shutting down");
                break;
            }
            
            // Try to get a ready slot
            if let Some(slot) = self.airlock.try_get_ready_slot() {
                log::info!("Aggregator processing ready slot {}", slot);
                // Aggregate the slot data
                if let Some(aggregated) = self.airlock.aggregate_slot_data(slot) {
                    log::info!("Aggregated slot {} with {} account changes and {} vote transactions", 
                               slot, aggregated.account_changes.len(), aggregated.vote_transactions.len());
                    // Convert and send to DB writer
                    if let Err(e) = self.process_aggregated_slot(aggregated) {
                        log::error!("Failed to process aggregated slot {}: {:?}", slot, e);
                    }
                } else {
                    log::warn!("Failed to aggregate data for slot {}", slot);
                }
            } else {
                // No ready slots, sleep briefly to avoid busy waiting
                thread::sleep(Duration::from_micros(100));
            }
        }
    }

    /// Process aggregated slot data and send to DB writer
    fn process_aggregated_slot(
        &self,
        data: AggregatedSlotData,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Prepare slot data
        let slot_data = SlotData {
            slot: data.slot,
            bank_hash: BASE64.encode(data.bank_hash_components.bank_hash.as_ref()),
            parent_bank_hash: BASE64.encode(data.bank_hash_components.parent_bank_hash.as_ref()),
            signature_count: data.bank_hash_components.signature_count,
            last_blockhash: BASE64.encode(data.bank_hash_components.last_blockhash.as_ref()),
            cumulative_lthash: bytemuck::cast_slice(&data.bank_hash_components.cumulative_lthash.0)
                .to_vec(),
            delta_lthash: bytemuck::cast_slice(&data.bank_hash_components.delta_lthash.0).to_vec(),
            accounts_delta_hash: data
                .bank_hash_components
                .accounts_delta_hash
                .map(|h| BASE64.encode(h.as_ref())),
            accounts_lthash_checksum: data.bank_hash_components.accounts_lthash_checksum,
            epoch_accounts_hash: data
                .bank_hash_components
                .epoch_accounts_hash
                .map(|h| BASE64.encode(h.as_ref())),
            status: "rooted".to_string(),
            vote_count: data.vote_transactions.len() as i64,
            // Block metadata fields - fast pass through
            blockhash: data.block_metadata.as_ref().map(|m| m.blockhash.clone()),
            parent_slot: data.block_metadata.as_ref().map(|m| m.parent_slot as i64),
            executed_transaction_count: data.block_metadata.as_ref().map(|m| m.executed_transaction_count as i64),
            entry_count: data.block_metadata.as_ref().map(|m| m.entry_count as i64),
        };

        // Send slot data
        self.db_sender.send(DbCommand::WriteSlot(slot_data))?;

        // Send account changes in batches
        if !data.account_changes.is_empty() {
            let mut batch = Vec::with_capacity(1000);

            for change in data.account_changes {
                batch.push(change);

                // Send batch when full
                if batch.len() >= 1000 {
                    self.db_sender
                        .send(DbCommand::WriteAccountChanges(std::mem::replace(
                            &mut batch,
                            Vec::with_capacity(1000),
                        )))?;
                }
            }

            // Send remaining
            if !batch.is_empty() {
                self.db_sender.send(DbCommand::WriteAccountChanges(batch))?;
            }
        }

        // Send vote transactions
        if !data.vote_transactions.is_empty() {
            self.db_sender
                .send(DbCommand::WriteVoteTransactions(data.vote_transactions))?;
        }

        Ok(())
    }
}

/// Spawn aggregator threads
pub fn spawn_aggregators(
    airlock: Arc<AirLock>,
    db_sender: Sender<DbCommand>,
    num_threads: usize,
    shutdown_flag: Arc<AtomicBool>,
) -> Vec<thread::JoinHandle<()>> {
    (0..num_threads)
        .map(|i| {
            let aggregator = Aggregator::new(airlock.clone(), db_sender.clone(), shutdown_flag.clone());
            thread::Builder::new()
                .name(format!("aggregator-{}", i))
                .spawn(move || aggregator.run())
                .expect("Failed to spawn aggregator thread")
        })
        .collect()
}
