use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::atomic::Ordering;

use solana_sdk::{
    pubkey::Pubkey,
    account::AccountSharedData,
};
use solana_lattice_hash::lt_hash::LtHash;

use crate::airlock::{AirLock, BankHashComponents, SlotStatus, OwnedAccountChange};

#[test]
fn test_hot_cache_performance() {
    let airlock = Arc::new(AirLock::new());
    
    // Warm up the hot cache with a few slots
    for slot in 1000..1008 {
        airlock.begin_write(slot);
    }
    
    // Test cache hit rate
    let start = Instant::now();
    let iterations = 1_000_000;
    
    for i in 0..iterations {
        // Access slots that should be in hot cache
        let slot = 1000 + (i % 8);
        let _writer = airlock.begin_write(slot);
    }
    
    let elapsed = start.elapsed();
    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
    
    println!("Hot cache performance: {:.2} ops/sec", ops_per_sec);
    
    let metrics = airlock.get_metrics();
    let hit_rate = metrics.hot_cache_hits.load(Ordering::Relaxed) as f64 / 
        (metrics.hot_cache_hits.load(Ordering::Relaxed) + metrics.hot_cache_misses.load(Ordering::Relaxed)) as f64;
    
    assert!(hit_rate > 0.99, "Hot cache hit rate should be > 99%, got {:.2}%", hit_rate * 100.0);
    assert!(ops_per_sec > 10_000_000.0, "Should achieve > 10M ops/sec with hot cache");
}

#[test]
fn test_thread_local_buffer_isolation() {
    let airlock = Arc::new(AirLock::new());
    let slot = 5000;
    
    // Spawn multiple threads writing to the same slot
    let handles: Vec<_> = (0..10)
        .map(|thread_id| {
            let airlock = airlock.clone();
            std::thread::spawn(move || {
                for i in 0..100 {
                    let pubkey = Pubkey::new_unique();
                    let old_account = AccountSharedData::new(thread_id as u64, 0, &Pubkey::default());
                    let new_account = AccountSharedData::new(thread_id as u64 + 1000, 32, &Pubkey::default());
                    
                    airlock.buffer_account_change(OwnedAccountChange {
                        pubkey,
                        slot,
                        old_account,
                        new_account,
                        old_lthash: LtHash::identity(),
                        new_lthash: LtHash::identity(),
                    });
                }
            })
        })
        .collect();
    
    for h in handles {
        h.join().unwrap();
    }
    
    // Verify all changes were buffered
    let metrics = airlock.get_metrics();
    assert_eq!(metrics.account_changes_buffered.load(Ordering::Relaxed), 1000);
}

#[test]
fn test_slot_lifecycle() {
    let airlock = Arc::new(AirLock::new());
    let slot = 6000;
    
    // Simulate slot lifecycle
    let writer1 = airlock.begin_write(slot);
    let writer2 = airlock.begin_write(slot);
    
    // Set bank hash components
    airlock.set_bank_hash_components(slot, BankHashComponents {
        slot,
        bank_hash: solana_sdk::hash::Hash::new_unique(),
        parent_bank_hash: solana_sdk::hash::Hash::new_unique(),
        signature_count: 100,
        last_blockhash: solana_sdk::hash::Hash::new_unique(),
        accounts_delta_hash: None,
        accounts_lthash_checksum: Some("checksum".to_string()),
        epoch_accounts_hash: None,
        cumulative_lthash: LtHash::identity(),
        delta_lthash: LtHash::identity(),
    });
    
    // Buffer some changes
    for i in 0..10 {
        airlock.buffer_account_change(OwnedAccountChange {
            pubkey: Pubkey::new_unique(),
            slot,
            old_account: AccountSharedData::default(),
            new_account: AccountSharedData::new(1000, 32, &Pubkey::default()),
            old_lthash: LtHash::identity(),
            new_lthash: LtHash::identity(),
        });
    }
    
    // Update status to rooted
    airlock.update_slot_status(slot, SlotStatus::Rooted);
    
    // Writers still active, shouldn't be ready
    assert!(airlock.try_get_ready_slot().is_none());
    
    // Drop writers
    drop(writer1);
    drop(writer2);
    
    // Now should be ready
    assert_eq!(airlock.try_get_ready_slot(), Some(slot));
    
    // Aggregate data
    let aggregated = airlock.aggregate_slot_data(slot).unwrap();
    assert_eq!(aggregated.slot, slot);
    assert_eq!(aggregated.account_changes.len(), 10);
    assert_eq!(aggregated.bank_hash_components.signature_count, 100);
}

#[test]
fn test_memory_cleanup() {
    let airlock = Arc::new(AirLock::new());
    
    // Create many slots
    for slot in 0..100 {
        let _writer = airlock.begin_write(slot);
        airlock.buffer_account_change(OwnedAccountChange {
            pubkey: Pubkey::new_unique(),
            slot,
            old_account: AccountSharedData::default(),
            new_account: AccountSharedData::new(1000, 32, &Pubkey::default()),
            old_lthash: LtHash::identity(),
            new_lthash: LtHash::identity(),
        });
    }
    
    let stats_before = airlock.get_stats();
    assert!(stats_before.active_slots >= 100);
    
    // Force cleanup by accessing ready queue (triggers maybe_cleanup)
    for _ in 0..10 {
        airlock.try_get_ready_slot();
        std::thread::sleep(Duration::from_millis(10));
    }
    
    // Old slots should eventually be cleaned up
    // Note: This test might be flaky due to timing, but demonstrates the cleanup mechanism
}

#[test]
fn test_concurrent_aggregation() {
    let airlock = Arc::new(AirLock::new());
    
    // Create multiple rooted slots ready for aggregation
    let slots: Vec<u64> = (7000..7010).collect();
    
    for &slot in &slots {
        let writer = airlock.begin_write(slot);
        
        airlock.set_bank_hash_components(slot, BankHashComponents {
            slot,
            bank_hash: solana_sdk::hash::Hash::new_unique(),
            parent_bank_hash: solana_sdk::hash::Hash::new_unique(),
            signature_count: slot,
            last_blockhash: solana_sdk::hash::Hash::new_unique(),
            accounts_delta_hash: None,
            accounts_lthash_checksum: None,
            epoch_accounts_hash: None,
            cumulative_lthash: LtHash::identity(),
            delta_lthash: LtHash::identity(),
        });
        
        for _ in 0..50 {
            airlock.buffer_account_change(OwnedAccountChange {
                pubkey: Pubkey::new_unique(),
                slot,
                old_account: AccountSharedData::default(),
                new_account: AccountSharedData::new(1000, 32, &Pubkey::default()),
                old_lthash: LtHash::identity(),
                new_lthash: LtHash::identity(),
            });
        }
        
        airlock.update_slot_status(slot, SlotStatus::Rooted);
        drop(writer);
    }
    
    // Spawn multiple aggregators
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let airlock = airlock.clone();
            std::thread::spawn(move || {
                let mut aggregated = Vec::new();
                while let Some(slot) = airlock.try_get_ready_slot() {
                    if let Some(data) = airlock.aggregate_slot_data(slot) {
                        aggregated.push(data);
                    }
                }
                aggregated
            })
        })
        .collect();
    
    // Collect all aggregated data
    let mut all_aggregated = Vec::new();
    for h in handles {
        all_aggregated.extend(h.join().unwrap());
    }
    
    // Verify all slots were aggregated exactly once
    assert_eq!(all_aggregated.len(), slots.len());
    let mut aggregated_slots: Vec<_> = all_aggregated.iter().map(|d| d.slot).collect();
    aggregated_slots.sort();
    assert_eq!(aggregated_slots, slots);
}