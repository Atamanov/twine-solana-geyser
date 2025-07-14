use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use std::sync::Arc;
use std::time::Duration;

use solana_sdk::{
    pubkey::Pubkey,
    account::AccountSharedData,
};
use solana_lattice_hash::lt_hash::LtHash;
use twine_solana_geyser::airlock::{AirLock, OwnedAccountChange};

fn bench_single_thread_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_thread_writes");
    
    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let airlock = Arc::new(AirLock::new());
            let slot = 1000;
            
            b.iter(|| {
                for i in 0..size {
                    let pubkey = Pubkey::new_unique();
                    let change = OwnedAccountChange {
                        pubkey,
                        slot,
                        old_account: AccountSharedData::default(),
                        new_account: AccountSharedData::new(1000, 32, &Pubkey::default()),
                        old_lthash: LtHash::identity(),
                        new_lthash: LtHash::identity(),
                    };
                    airlock.buffer_account_change(black_box(change));
                }
            });
        });
    }
    group.finish();
}

fn bench_multi_thread_burst(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_thread_burst");
    group.measurement_time(Duration::from_secs(10));
    
    for num_threads in [4, 8, 16, 32].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads), 
            num_threads, 
            |b, &num_threads| {
                let airlock = Arc::new(AirLock::new());
                
                b.iter(|| {
                    let slot = 2000;
                    let handles: Vec<_> = (0..num_threads)
                        .map(|_| {
                            let airlock = airlock.clone();
                            std::thread::spawn(move || {
                                for _ in 0..1000 {
                                    let change = OwnedAccountChange {
                                        pubkey: Pubkey::new_unique(),
                                        slot,
                                        old_account: AccountSharedData::default(),
                                        new_account: AccountSharedData::new(1000, 32, &Pubkey::default()),
                                        old_lthash: LtHash::identity(),
                                        new_lthash: LtHash::identity(),
                                    };
                                    airlock.buffer_account_change(change);
                                }
                            })
                        })
                        .collect();
                    
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            }
        );
    }
    group.finish();
}

fn bench_hot_cache_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_cache_access");
    
    group.bench_function("sequential_slots", |b| {
        let airlock = Arc::new(AirLock::new());
        let mut slot = 0;
        
        b.iter(|| {
            let _writer = airlock.begin_write(black_box(slot));
            slot += 1;
        });
    });
    
    group.bench_function("hot_slots_only", |b| {
        let airlock = Arc::new(AirLock::new());
        let mut i = 0;
        
        // Pre-warm the cache
        for slot in 0..8 {
            airlock.begin_write(slot);
        }
        
        b.iter(|| {
            let slot = i % 8;
            let _writer = airlock.begin_write(black_box(slot));
            i += 1;
        });
    });
    
    group.finish();
}

fn bench_aggregation_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregation");
    
    for num_changes in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*num_changes as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_changes), 
            num_changes, 
            |b, &num_changes| {
                b.iter_batched(
                    || {
                        // Setup
                        let airlock = Arc::new(AirLock::new());
                        let slot = 3000;
                        let writer = airlock.begin_write(slot);
                        
                        // Add changes
                        for _ in 0..num_changes {
                            airlock.buffer_account_change(OwnedAccountChange {
                                pubkey: Pubkey::new_unique(),
                                slot,
                                old_account: AccountSharedData::default(),
                                new_account: AccountSharedData::new(1000, 32, &Pubkey::default()),
                                old_lthash: LtHash::identity(),
                                new_lthash: LtHash::identity(),
                            });
                        }
                        
                        airlock.set_bank_hash_components(slot, Default::default());
                        airlock.update_slot_status(slot, twine_solana_geyser::airlock::SlotStatus::Rooted);
                        drop(writer);
                        
                        (airlock, slot)
                    },
                    |(airlock, slot)| {
                        // Benchmark aggregation
                        airlock.aggregate_slot_data(black_box(slot))
                    },
                    criterion::BatchSize::SmallInput
                );
            }
        );
    }
    group.finish();
}

fn bench_realistic_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_workload");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);
    
    group.bench_function("mainnet_simulation", |b| {
        b.iter(|| {
            let airlock = Arc::new(AirLock::new());
            let num_threads = 32;
            let slots_per_second = 2; // Mainnet rate
            let changes_per_slot = 5000; // Realistic for mainnet
            let duration_secs = 5;
            
            let start = std::time::Instant::now();
            let mut handles = Vec::new();
            
            // Writer threads
            for thread_id in 0..num_threads {
                let airlock = airlock.clone();
                let handle = std::thread::spawn(move || {
                    let mut slot = 10000;
                    while start.elapsed().as_secs() < duration_secs {
                        let writer = airlock.begin_write(slot);
                        
                        // Simulate burst writes
                        for _ in 0..(changes_per_slot / num_threads) {
                            airlock.buffer_account_change(OwnedAccountChange {
                                pubkey: Pubkey::new_unique(),
                                slot,
                                old_account: AccountSharedData::default(),
                                new_account: AccountSharedData::new(1000, 32, &Pubkey::default()),
                                old_lthash: LtHash::identity(),
                                new_lthash: LtHash::identity(),
                            });
                        }
                        
                        drop(writer);
                        
                        // Move to next slot based on thread ID to simulate distribution
                        if thread_id == 0 {
                            std::thread::sleep(Duration::from_millis(500 / slots_per_second as u64));
                            slot += 1;
                        }
                    }
                });
                handles.push(handle);
            }
            
            // Aggregator thread
            let airlock_clone = airlock.clone();
            let aggregator = std::thread::spawn(move || {
                let mut aggregated = 0;
                while start.elapsed().as_secs() < duration_secs + 1 {
                    if let Some(slot) = airlock_clone.try_get_ready_slot() {
                        if let Some(_) = airlock_clone.aggregate_slot_data(slot) {
                            aggregated += 1;
                        }
                    } else {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                aggregated
            });
            
            // Wait for all threads
            for h in handles {
                h.join().unwrap();
            }
            
            let total_aggregated = aggregator.join().unwrap();
            let metrics = airlock.get_metrics();
            
            println!(
                "Processed {} changes across {} slots, aggregated {} slots",
                metrics.account_changes_buffered.load(std::sync::atomic::Ordering::Relaxed),
                metrics.slots_created.load(std::sync::atomic::Ordering::Relaxed),
                total_aggregated
            );
        });
    });
    
    group.finish();
}

criterion_group!(
    benches, 
    bench_single_thread_writes,
    bench_multi_thread_burst,
    bench_hot_cache_access,
    bench_aggregation_performance,
    bench_realistic_workload
);
criterion_main!(benches);