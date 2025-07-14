use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};

use solana_sdk::{
    pubkey::Pubkey,
    account::AccountSharedData,
    hash::Hash,
};
use solana_lattice_hash::lt_hash::LtHash;

use crate::airlock::{AirLock, BankHashComponents, SlotStatus, OwnedAccountChange};

#[test]
#[ignore] // Run with: cargo test --ignored test_extreme_burst_load
fn test_extreme_burst_load() {
    let airlock = Arc::new(AirLock::new());
    let num_threads = 100;
    let writes_per_thread = 10_000;
    let target_slot = 100_000;
    
    let total_writes = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    // Spawn writer threads
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let airlock = airlock.clone();
            let total_writes = total_writes.clone();
            
            std::thread::spawn(move || {
                // Pin to CPU if possible
                #[cfg(target_os = "linux")]
                {
                    use core_affinity::CoreId;
                    core_affinity::set_for_current(CoreId { id: thread_id % num_cpus::get() });
                }
                
                for i in 0..writes_per_thread {
                    let slot = target_slot + (i % 10); // Concentrate on 10 slots
                    
                    let change = OwnedAccountChange {
                        pubkey: Pubkey::new_unique(),
                        slot,
                        old_account: AccountSharedData::new(i as u64, 0, &Pubkey::default()),
                        new_account: AccountSharedData::new((i + 1000) as u64, 32, &Pubkey::default()),
                        old_lthash: LtHash::identity(),
                        new_lthash: LtHash::identity(),
                    };
                    
                    airlock.buffer_account_change(change);
                    total_writes.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    
    // Wait for completion
    for h in handles {
        h.join().unwrap();
    }
    
    let elapsed = start.elapsed();
    let total = total_writes.load(Ordering::Relaxed);
    let rate = total as f64 / elapsed.as_secs_f64();
    
    println!("Extreme burst test:");
    println!("  Total writes: {}", total);
    println!("  Duration: {:?}", elapsed);
    println!("  Rate: {:.2} writes/sec", rate);
    println!("  Rate per thread: {:.2} writes/sec", rate / num_threads as f64);
    
    let metrics = airlock.get_metrics();
    let hit_rate = metrics.hot_cache_hits.load(Ordering::Relaxed) as f64 / 
        (metrics.hot_cache_hits.load(Ordering::Relaxed) + metrics.hot_cache_misses.load(Ordering::Relaxed)) as f64;
    
    println!("  Cache hit rate: {:.2}%", hit_rate * 100.0);
    
    assert_eq!(total, (num_threads * writes_per_thread) as u64);
    assert!(rate > 1_000_000.0, "Should achieve > 1M writes/sec");
}

#[test]
#[ignore] // Run with: cargo test --ignored test_memory_pressure
fn test_memory_pressure() {
    let airlock = Arc::new(AirLock::new());
    let running = Arc::new(AtomicBool::new(true));
    let mut handles = Vec::new();
    
    // Monitor thread
    let airlock_clone = airlock.clone();
    let running_clone = running.clone();
    let monitor = std::thread::spawn(move || {
        let mut max_slots = 0;
        let mut max_changes = 0;
        
        while running_clone.load(Ordering::Relaxed) {
            let stats = airlock_clone.get_stats();
            max_slots = max_slots.max(stats.active_slots);
            max_changes = max_changes.max(stats.total_account_changes);
            
            if stats.active_slots > 10_000 {
                println!("WARNING: {} active slots", stats.active_slots);
            }
            
            std::thread::sleep(Duration::from_millis(100));
        }
        
        (max_slots, max_changes)
    });
    
    // Spam threads - create slots faster than they can be cleaned up
    for thread_id in 0..20 {
        let airlock = airlock.clone();
        let running = running.clone();
        
        let h = std::thread::spawn(move || {
            let mut slot = thread_id * 100_000;
            
            while running.load(Ordering::Relaxed) {
                // Create new slot every iteration
                let writer = airlock.begin_write(slot);
                
                // Add large account changes
                for _ in 0..100 {
                    let mut data = vec![0u8; 1024 * 10]; // 10KB per account
                    data[0] = thread_id as u8;
                    
                    airlock.buffer_account_change(OwnedAccountChange {
                        pubkey: Pubkey::new_unique(),
                        slot,
                        old_account: AccountSharedData::default(),
                        new_account: AccountSharedData::new_data(1000, &data, &Pubkey::default()).unwrap(),
                        old_lthash: LtHash::identity(),
                        new_lthash: LtHash::identity(),
                    });
                }
                
                drop(writer);
                slot += 1;
                
                // Don't mark as rooted - let them accumulate
                if slot % 100 == 0 {
                    std::thread::yield_now();
                }
            }
        });
        handles.push(h);
    }
    
    // Let it run for a while
    std::thread::sleep(Duration::from_secs(10));
    
    // Trigger cleanup
    for _ in 0..100 {
        airlock.try_get_ready_slot();
        std::thread::sleep(Duration::from_millis(10));
    }
    
    // Stop threads
    running.store(false, Ordering::Relaxed);
    
    for h in handles {
        h.join().unwrap();
    }
    
    let (max_slots, max_changes) = monitor.join().unwrap();
    
    println!("Memory pressure test:");
    println!("  Max active slots: {}", max_slots);
    println!("  Max changes buffered: {}", max_changes);
    
    // Final cleanup should reduce memory
    std::thread::sleep(Duration::from_secs(2));
    let final_stats = airlock.get_stats();
    
    println!("  Final active slots: {}", final_stats.active_slots);
    
    // Should have some cleanup mechanism working
    assert!(final_stats.active_slots < max_slots, "Cleanup should reduce active slots");
}

#[test]
fn test_aggregation_under_load() {
    let airlock = Arc::new(AirLock::new());
    let num_slots = 100;
    let changes_per_slot = 1000;
    
    // Phase 1: Create many slots with data
    let writer_handles: Vec<_> = (0..num_slots)
        .map(|i| {
            let airlock = airlock.clone();
            let slot = 50000 + i;
            
            std::thread::spawn(move || {
                let writer = airlock.begin_write(slot);
                
                // Set bank hash
                airlock.set_bank_hash_components(slot, BankHashComponents {
                    slot,
                    bank_hash: Hash::new_unique(),
                    parent_bank_hash: Hash::new_unique(),
                    signature_count: i,
                    last_blockhash: Hash::new_unique(),
                    accounts_delta_hash: Some(Hash::new_unique()),
                    accounts_lthash_checksum: None,
                    epoch_accounts_hash: None,
                    cumulative_lthash: LtHash::identity(),
                    delta_lthash: LtHash::identity(),
                });
                
                // Add changes
                for j in 0..changes_per_slot {
                    airlock.buffer_account_change(OwnedAccountChange {
                        pubkey: Pubkey::new_unique(),
                        slot,
                        old_account: AccountSharedData::new(j as u64, 0, &Pubkey::default()),
                        new_account: AccountSharedData::new((j + 1000) as u64, 32, &Pubkey::default()),
                        old_lthash: LtHash::identity(),
                        new_lthash: LtHash::identity(),
                    });
                }
                
                // Mark as rooted
                airlock.update_slot_status(slot, SlotStatus::Rooted);
                
                drop(writer);
            })
        })
        .collect();
    
    // Wait for all writers
    for h in writer_handles {
        h.join().unwrap();
    }
    
    // Phase 2: Aggregate concurrently
    let aggregated_count = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    let aggregator_handles: Vec<_> = (0..8)
        .map(|_| {
            let airlock = airlock.clone();
            let aggregated_count = aggregated_count.clone();
            
            std::thread::spawn(move || {
                let mut local_count = 0;
                
                while let Some(slot) = airlock.try_get_ready_slot() {
                    if let Some(data) = airlock.aggregate_slot_data(slot) {
                        assert_eq!(data.account_changes.len(), changes_per_slot);
                        assert_eq!(data.bank_hash_components.signature_count, slot - 50000);
                        local_count += 1;
                    }
                }
                
                aggregated_count.fetch_add(local_count, Ordering::Relaxed);
            })
        })
        .collect();
    
    // Wait for aggregators
    for h in aggregator_handles {
        h.join().unwrap();
    }
    
    let elapsed = start.elapsed();
    let total_aggregated = aggregated_count.load(Ordering::Relaxed);
    
    println!("Aggregation under load:");
    println!("  Slots aggregated: {}", total_aggregated);
    println!("  Duration: {:?}", elapsed);
    println!("  Rate: {:.2} slots/sec", total_aggregated as f64 / elapsed.as_secs_f64());
    
    assert_eq!(total_aggregated, num_slots);
}

#[test]
fn test_pathological_access_pattern() {
    let airlock = Arc::new(AirLock::new());
    let num_threads = 50;
    let iterations = 1000;
    
    // Each thread accesses a different, non-overlapping set of slots
    // This should cause maximum cache misses
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let airlock = airlock.clone();
            
            std::thread::spawn(move || {
                for i in 0..iterations {
                    // Each thread uses slots far apart from each other
                    let slot = thread_id * 1_000_000 + i;
                    let writer = airlock.begin_write(slot);
                    
                    airlock.buffer_account_change(OwnedAccountChange {
                        pubkey: Pubkey::new_unique(),
                        slot,
                        old_account: AccountSharedData::default(),
                        new_account: AccountSharedData::new(1000, 32, &Pubkey::default()),
                        old_lthash: LtHash::identity(),
                        new_lthash: LtHash::identity(),
                    });
                    
                    drop(writer);
                }
            })
        })
        .collect();
    
    let start = Instant::now();
    
    for h in handles {
        h.join().unwrap();
    }
    
    let elapsed = start.elapsed();
    let metrics = airlock.get_metrics();
    
    let total_ops = (num_threads * iterations) as f64;
    let ops_per_sec = total_ops / elapsed.as_secs_f64();
    
    println!("Pathological access pattern:");
    println!("  Duration: {:?}", elapsed);
    println!("  Ops/sec: {:.2}", ops_per_sec);
    println!("  Cache misses: {}", metrics.hot_cache_misses.load(Ordering::Relaxed));
    
    // Even with worst case pattern, should still be fast
    assert!(ops_per_sec > 100_000.0, "Should maintain > 100K ops/sec even with cache misses");
}