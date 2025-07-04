pub mod types;
pub mod pool;

use crate::airlock::types::{OwnedReplicaAccountInfo, Slot, SlotAirlock};
use dashmap::DashMap;
use dashmap::DashSet;
use parking_lot::Mutex;
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct AirlockManager {
    slot_airlocks: Arc<DashMap<Slot, Arc<Mutex<SlotAirlock>>>>,
    monitored_accounts: Arc<DashSet<Pubkey>>,
    stats: Arc<AirlockStats>,
}

#[derive(Debug, Default)]
pub struct AirlockStats {
    pub total_updates: AtomicUsize,
    pub sealed_slots: AtomicUsize,
    pub active_slots: AtomicUsize,
    pub monitored_account_changes: AtomicUsize,
    pub slots_with_monitored_accounts: AtomicUsize,
    pub proof_requests_generated: AtomicUsize,
    pub db_writes: AtomicUsize,
}

impl AirlockManager {
    pub fn new(monitored_accounts: Vec<Pubkey>) -> Self {
        let accounts_set = Arc::new(DashSet::new());
        for account in monitored_accounts {
            accounts_set.insert(account);
        }

        Self {
            slot_airlocks: Arc::new(DashMap::new()),
            monitored_accounts: accounts_set,
            stats: Arc::new(AirlockStats::default()),
        }
    }

    pub fn is_account_monitored(&self, pubkey: &Pubkey) -> bool {
        self.monitored_accounts.is_empty() || self.monitored_accounts.contains(pubkey)
    }

    pub fn add_account_update(
        &self,
        slot: Slot,
        account_info: OwnedReplicaAccountInfo,
    ) -> Result<(), String> {
        let airlock_mutex = self
            .slot_airlocks
            .entry(slot)
            .or_insert_with(|| {
                self.stats.active_slots.fetch_add(1, Ordering::Relaxed);
                Arc::new(Mutex::new(SlotAirlock::new()))
            })
            .clone();

        {
            let airlock = airlock_mutex.lock();
            airlock.active_writers.fetch_add(1, Ordering::SeqCst);
        }

        {
            let airlock = airlock_mutex.lock();
            airlock.queue.push(account_info);
        }

        {
            let airlock = airlock_mutex.lock();
            airlock.active_writers.fetch_sub(1, Ordering::SeqCst);
        }

        self.stats.total_updates.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn seal_slot(&self, slot: Slot) -> Option<Vec<OwnedReplicaAccountInfo>> {
        if let Some((_, airlock_mutex)) = self.slot_airlocks.remove(&slot) {
            let mut airlock = airlock_mutex.lock();

            while airlock.active_writers.load(Ordering::SeqCst) != 0 {
                std::thread::yield_now();
            }

            let mut updates = Vec::new();
            while let Some(update) = airlock.queue.pop() {
                updates.push(update);
            }

            airlock.sealed_data = Some(updates.clone());

            self.stats.sealed_slots.fetch_add(1, Ordering::Relaxed);
            self.stats.active_slots.fetch_sub(1, Ordering::Relaxed);

            if !updates.is_empty() {
                log::info!(
                    "Sealed slot {} with {} account updates",
                    slot,
                    updates.len()
                );
            }

            Some(updates)
        } else {
            None
        }
    }

    pub fn get_stats(&self) -> AirlockStatsSnapshot {
        AirlockStatsSnapshot {
            total_updates: self.stats.total_updates.load(Ordering::Relaxed),
            sealed_slots: self.stats.sealed_slots.load(Ordering::Relaxed),
            active_slots: self.stats.active_slots.load(Ordering::Relaxed),
            monitored_accounts: self.monitored_accounts.len(),
            monitored_account_changes: self.stats.monitored_account_changes.load(Ordering::Relaxed),
            slots_with_monitored_accounts: self.stats.slots_with_monitored_accounts.load(Ordering::Relaxed),
            proof_requests_generated: self.stats.proof_requests_generated.load(Ordering::Relaxed),
            db_writes: self.stats.db_writes.load(Ordering::Relaxed),
        }
    }

    pub fn add_monitored_account(&self, pubkey: Pubkey) {
        self.monitored_accounts.insert(pubkey);
    }

    pub fn remove_monitored_account(&self, pubkey: &Pubkey) -> bool {
        self.monitored_accounts.remove(pubkey).is_some()
    }

    pub fn clear_old_slots(&self, current_slot: Slot, keep_slots: u64) {
        let min_slot = current_slot.saturating_sub(keep_slots);
        let mut removed = 0;
        
        self.slot_airlocks.retain(|&slot, _| {
            if slot < min_slot {
                removed += 1;
                false
            } else {
                true
            }
        });

        if removed > 0 {
            log::debug!("Cleared {} old slots before slot {}", removed, min_slot);
            self.stats.active_slots.fetch_sub(removed, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone)]
pub struct AirlockStatsSnapshot {
    pub total_updates: usize,
    pub sealed_slots: usize,
    pub active_slots: usize,
    pub monitored_accounts: usize,
    pub monitored_account_changes: usize,
    pub slots_with_monitored_accounts: usize,
    pub proof_requests_generated: usize,
    pub db_writes: usize,
}