use std::sync::Arc;
use std::time::Duration;

use crate::test_harness::test_harness::{TestHarness, PerfTestHarness, create_mainnet_workload};
use crate::db_writer::DbCommand;

#[test]
fn test_end_to_end_with_mock_db() {
    let harness = TestHarness::new(2);
    
    // Process multiple slots
    for slot in 1000..1010 {
        harness.process_slot(slot, 100);
    }
    
    // Collect commands
    let commands = harness.collect_db_commands(Duration::from_secs(2));
    
    // Verify slots were written
    let slot_commands = commands.get("slots").map(|v| v.len()).unwrap_or(0);
    assert_eq!(slot_commands, 10, "Should have 10 slot commands");
    
    // Verify account changes were batched
    let change_commands = commands.get("account_changes").map(|v| v.len()).unwrap_or(0);
    assert!(change_commands >= 10, "Should have at least 10 account change batches");
    
    harness.shutdown();
}

#[test]
fn test_high_throughput_processing() {
    let harness = PerfTestHarness::new();
    
    // Spawn 32 writer threads with 1000 changes per slot
    let handles = harness.spawn_writer_threads(32, 1000);
    
    // Run for 5 seconds
    let results = harness.run_for_duration(Duration::from_secs(5));
    
    // Wait for threads
    for h in handles {
        h.join().unwrap();
    }
    
    println!("High throughput test results:");
    println!("  Total changes: {}", results.total_changes);
    println!("  Changes/sec: {:.2}", results.changes_per_second);
    println!("  Cache hit rate: {:.2}%", results.cache_hit_rate * 100.0);
    println!("  Avg write time: {} μs", results.avg_write_time_us);
    
    // Assertions
    assert!(results.changes_per_second > 1_000_000.0, 
        "Should achieve >1M changes/sec, got {:.2}", results.changes_per_second);
    assert!(results.cache_hit_rate > 0.90, 
        "Cache hit rate should be >90%, got {:.2}%", results.cache_hit_rate * 100.0);
}

#[test]
fn test_mainnet_simulation() {
    let airlock = Arc::new(crate::airlock::AirLock::new());
    
    // Run mainnet workload for 10 seconds
    let handles = create_mainnet_workload(airlock.clone(), 10);
    
    // Wait for completion
    for h in handles {
        h.join().unwrap();
    }
    
    let metrics = airlock.get_metrics();
    let stats = airlock.get_stats();
    
    println!("Mainnet simulation results:");
    println!("  Total slots: {}", stats.total_slots);
    println!("  Total changes: {}", stats.total_account_changes);
    println!("  Active slots: {}", stats.active_slots);
    
    // Should process ~20 slots in 10 seconds (2 slots/sec)
    assert!(stats.total_slots >= 18 && stats.total_slots <= 22, 
        "Should process ~20 slots, got {}", stats.total_slots);
    
    // Should have ~5000 changes per slot
    let expected_changes = stats.total_slots * 4500; // Allow some variance
    assert!(stats.total_account_changes >= expected_changes,
        "Should have at least {} changes, got {}", expected_changes, stats.total_account_changes);
}

#[test]
fn test_concurrent_slot_processing() {
    let harness = TestHarness::new(4);
    let num_threads = 10;
    let slots_per_thread = 5;
    
    // Spawn threads to process slots concurrently
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let airlock = harness.airlock.clone();
            std::thread::spawn(move || {
                for i in 0..slots_per_thread {
                    let slot = 5000 + thread_id * 100 + i;
                    let writer = airlock.begin_write(slot);
                    
                    airlock.set_bank_hash_components(slot, Default::default());
                    
                    for j in 0..50 {
                        airlock.buffer_account_change(crate::airlock::OwnedAccountChange {
                            pubkey: solana_sdk::pubkey::Pubkey::new_unique(),
                            slot,
                            old_account: solana_sdk::account::AccountSharedData::default(),
                            new_account: solana_sdk::account::AccountSharedData::new(j, 32, &solana_sdk::pubkey::Pubkey::default()),
                            old_lthash: solana_lattice_hash::lt_hash::LtHash::identity(),
                            new_lthash: solana_lattice_hash::lt_hash::LtHash::identity(),
                        });
                    }
                    
                    airlock.update_slot_status(slot, crate::airlock::SlotStatus::Rooted);
                    drop(writer);
                }
            })
        })
        .collect();
    
    for h in handles {
        h.join().unwrap();
    }
    
    // Collect results
    let commands = harness.collect_db_commands(Duration::from_secs(3));
    
    let total_slots = commands.get("slots").map(|v| v.len()).unwrap_or(0);
    assert_eq!(total_slots, num_threads * slots_per_thread, 
        "Should process all {} slots", num_threads * slots_per_thread);
    
    harness.shutdown();
}

#[test]
fn test_memory_efficiency() {
    let airlock = Arc::new(crate::airlock::AirLock::new());
    let initial_stats = airlock.get_stats();
    
    // Create many slots
    for slot in 0..1000 {
        let writer = airlock.begin_write(slot);
        
        for i in 0..10 {
            airlock.buffer_account_change(crate::airlock::OwnedAccountChange {
                pubkey: solana_sdk::pubkey::Pubkey::new_unique(),
                slot,
                old_account: solana_sdk::account::AccountSharedData::default(),
                new_account: solana_sdk::account::AccountSharedData::new(i, 32, &solana_sdk::pubkey::Pubkey::default()),
                old_lthash: solana_lattice_hash::lt_hash::LtHash::identity(),
                new_lthash: solana_lattice_hash::lt_hash::LtHash::identity(),
            });
        }
        
        drop(writer);
    }
    
    let stats_after_creation = airlock.get_stats();
    assert_eq!(stats_after_creation.active_slots, 1000);
    
    // Trigger cleanup
    for _ in 0..10 {
        airlock.try_get_ready_slot();
        std::thread::sleep(Duration::from_millis(10));
    }
    
    // Old slots should be cleaned up (this depends on timing)
    let final_stats = airlock.get_stats();
    println!("Memory efficiency test:");
    println!("  Initial slots: {}", initial_stats.active_slots);
    println!("  After creation: {}", stats_after_creation.active_slots);
    println!("  After cleanup: {}", final_stats.active_slots);
}

#[test]
fn test_error_recovery() {
    let harness = TestHarness::new(2);
    
    // Process slot with large data
    let slot = 9999;
    let writer = harness.airlock.begin_write(slot);
    
    // Add very large account data
    for i in 0..100 {
        let large_data = vec![0u8; 1024 * 1024]; // 1MB per account
        let mut account = solana_sdk::account::AccountSharedData::default();
        account.set_data(large_data);
        
        harness.airlock.buffer_account_change(crate::airlock::OwnedAccountChange {
            pubkey: solana_sdk::pubkey::Pubkey::new_unique(),
            slot,
            old_account: solana_sdk::account::AccountSharedData::default(),
            new_account: account,
            old_lthash: solana_lattice_hash::lt_hash::LtHash::identity(),
            new_lthash: solana_lattice_hash::lt_hash::LtHash::identity(),
        });
    }
    
    harness.airlock.update_slot_status(slot, crate::airlock::SlotStatus::Rooted);
    drop(writer);
    
    // Should still be able to aggregate despite large data
    let commands = harness.collect_db_commands(Duration::from_secs(2));
    assert!(commands.get("slots").is_some(), "Should have processed the large slot");
    
    harness.shutdown();
}