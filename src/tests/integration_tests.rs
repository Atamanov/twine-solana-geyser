use std::sync::Arc;
use std::time::Duration;

use crate::test_harness::test_harness::{TestHarness, PerfTestHarness};

async fn setup_test_db() -> (Client, String) {
    // Connect to postgres to create test database
    let (client, connection) = tokio_postgres::connect(
        "host=localhost user=postgres password=postgres",
        NoTls,
    ).await.expect("Failed to connect to postgres");
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Create unique test database
    let db_name = format!("test_geyser_{}", uuid::Uuid::new_v4().to_string().replace("-", ""));
    client.execute(&format!("CREATE DATABASE {}", db_name), &[])
        .await.expect("Failed to create test database");
    
    // Connect to new database and create schema
    let (mut client, connection) = tokio_postgres::connect(
        &format!("host=localhost dbname={} user=postgres password=postgres", db_name),
        NoTls,
    ).await.expect("Failed to connect to test database");
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Create minimal schema for testing
    client.batch_execute(r#"
        CREATE TABLE slots (
            slot BIGINT PRIMARY KEY,
            bank_hash VARCHAR(44) NOT NULL,
            parent_bank_hash VARCHAR(44) NOT NULL,
            signature_count BIGINT NOT NULL,
            last_blockhash VARCHAR(44) NOT NULL,
            cumulative_lthash BYTEA NOT NULL,
            delta_lthash BYTEA NOT NULL,
            accounts_delta_hash VARCHAR(44),
            accounts_lthash_checksum VARCHAR(44),
            epoch_accounts_hash VARCHAR(44),
            status VARCHAR(20) NOT NULL,
            rooted_at TIMESTAMPTZ DEFAULT NOW(),
            vote_count BIGINT DEFAULT 0
        );
        
        CREATE TABLE account_changes (
            slot BIGINT NOT NULL,
            account_pubkey VARCHAR(44) NOT NULL,
            write_version BIGINT NOT NULL,
            old_lamports BIGINT NOT NULL,
            old_owner VARCHAR(44) NOT NULL,
            old_executable BOOLEAN NOT NULL,
            old_rent_epoch BIGINT NOT NULL,
            old_data BYTEA,
            old_lthash BYTEA NOT NULL,
            new_lamports BIGINT NOT NULL,
            new_owner VARCHAR(44) NOT NULL,
            new_executable BOOLEAN NOT NULL,
            new_rent_epoch BIGINT NOT NULL,
            new_data BYTEA,
            new_lthash BYTEA NOT NULL,
            PRIMARY KEY (slot, account_pubkey, write_version)
        );
        
        CREATE TABLE vote_transactions (
            slot BIGINT NOT NULL,
            voter_pubkey VARCHAR(88) NOT NULL,
            vote_signature VARCHAR(88) NOT NULL,
            vote_transaction BYTEA NOT NULL,
            transaction_meta JSONB,
            vote_type VARCHAR(50),
            vote_slot BIGINT,
            vote_hash VARCHAR(88),
            root_slot BIGINT,
            lockouts_count INTEGER,
            timestamp BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (slot, voter_pubkey, vote_signature)
        );
        
        CREATE INDEX idx_slots_rooted_at ON slots (rooted_at DESC);
        CREATE INDEX idx_account_changes_pubkey ON account_changes (account_pubkey, slot DESC);
        CREATE INDEX idx_vote_transactions_voter ON vote_transactions (voter_pubkey, slot DESC);
    "#).await.expect("Failed to create schema");
    
    (client, db_name)
}

async fn cleanup_test_db(db_name: &str) {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost user=postgres password=postgres",
        NoTls,
    ).await.expect("Failed to connect to postgres");
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Terminate connections and drop database
    client.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
        &[&db_name]
    ).await.ok();
    
    client.execute(&format!("DROP DATABASE IF EXISTS {}", db_name), &[])
        .await.expect("Failed to drop test database");
}

#[tokio::test]
#[ignore] // Requires PostgreSQL running
async fn test_end_to_end_flow() {
    let (mut test_client, db_name) = setup_test_db().await;
    
    // Setup components
    let airlock = Arc::new(AirLock::new());
    
    let db_config = DbConfig {
        host: "localhost".to_string(),
        port: 5432,
        user: "postgres".to_string(),
        password: "postgres".to_string(),
        dbname: db_name.clone(),
        pool_size: 10,
    };
    
    let pool = create_pool(&db_config).expect("Failed to create pool");
    
    // Spawn workers
    let (db_writers, db_sender) = spawn_db_writers(pool, 2);
    let aggregators = spawn_aggregators(airlock.clone(), db_sender.clone(), 2);
    
    // Simulate slot processing
    let slots = vec![1000, 1001, 1002];
    
    for &slot in &slots {
        // Begin write
        let writer = airlock.begin_write(slot);
        
        // Set bank hash components
        airlock.set_bank_hash_components(slot, BankHashComponents {
            slot,
            bank_hash: Hash::new_unique(),
            parent_bank_hash: Hash::new_unique(),
            signature_count: 100 + slot,
            last_blockhash: Hash::new_unique(),
            accounts_delta_hash: Some(Hash::new_unique()),
            accounts_lthash_checksum: Some("test_checksum".to_string()),
            epoch_accounts_hash: None,
            cumulative_lthash: LtHash::identity(),
            delta_lthash: LtHash::identity(),
        });
        
        // Add account changes
        for i in 0..10 {
            let pubkey = Pubkey::new_unique();
            airlock.buffer_account_change(OwnedAccountChange {
                pubkey,
                slot,
                old_account: AccountSharedData::new(i * 100, 0, &Pubkey::default()),
                new_account: AccountSharedData::new((i + 1) * 100, 32, &Pubkey::default()),
                old_lthash: LtHash::identity(),
                new_lthash: LtHash::identity(),
            });
        }
        
        // Add vote transactions
        for i in 0..5 {
            airlock.buffer_vote_transaction(VoteTransaction {
                slot,
                voter_pubkey: Pubkey::new_unique(),
                vote_signature: format!("sig_{}", i),
                vote_transaction: vec![1, 2, 3],
                transaction_meta: Some(serde_json::json!({"test": true})),
                vote_type: "vote".to_string(),
                vote_slot: Some(slot - 1),
                vote_hash: Some("vote_hash".to_string()),
                root_slot: Some(slot - 10),
                lockouts_count: Some(8),
                timestamp: Some(1234567890),
            });
        }
        
        // Mark as rooted
        airlock.update_slot_status(slot, SlotStatus::Rooted);
        
        // End write
        drop(writer);
    }
    
    // Wait for processing
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Verify database contents
    let slot_count: i64 = test_client
        .query_one("SELECT COUNT(*) FROM slots", &[])
        .await.unwrap()
        .get(0);
    assert_eq!(slot_count, 3);
    
    let account_changes_count: i64 = test_client
        .query_one("SELECT COUNT(*) FROM account_changes", &[])
        .await.unwrap()
        .get(0);
    assert_eq!(account_changes_count, 30); // 3 slots * 10 changes
    
    let vote_count: i64 = test_client
        .query_one("SELECT COUNT(*) FROM vote_transactions", &[])
        .await.unwrap()
        .get(0);
    assert_eq!(vote_count, 15); // 3 slots * 5 votes
    
    // Verify specific slot data
    let row = test_client
        .query_one("SELECT signature_count, status, vote_count FROM slots WHERE slot = $1", &[&1001i64])
        .await.unwrap();
    
    let sig_count: i64 = row.get(0);
    let status: String = row.get(1);
    let vote_count: i64 = row.get(2);
    
    assert_eq!(sig_count, 1101);
    assert_eq!(status, "rooted");
    assert_eq!(vote_count, 5);
    
    // Shutdown workers
    for _ in 0..2 {
        db_sender.send(crate::db_writer::DbCommand::Shutdown).unwrap();
    }
    
    // Cleanup
    cleanup_test_db(&db_name).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL running
async fn test_high_throughput_scenario() {
    let (mut test_client, db_name) = setup_test_db().await;
    
    let airlock = Arc::new(AirLock::new());
    
    let db_config = DbConfig {
        host: "localhost".to_string(),
        port: 5432,
        user: "postgres".to_string(),
        password: "postgres".to_string(),
        dbname: db_name.clone(),
        pool_size: 20,
    };
    
    let pool = create_pool(&db_config).expect("Failed to create pool");
    let (db_writers, db_sender) = spawn_db_writers(pool, 4);
    let aggregators = spawn_aggregators(airlock.clone(), db_sender.clone(), 4);
    
    // Simulate mainnet-like load
    let num_slots = 50;
    let changes_per_slot = 1000;
    let start_slot = 10000;
    
    // Spawn multiple writer threads
    let handles: Vec<_> = (0..10)
        .map(|thread_id| {
            let airlock = airlock.clone();
            std::thread::spawn(move || {
                for i in 0..num_slots/10 {
                    let slot = start_slot + thread_id * (num_slots/10) + i;
                    let writer = airlock.begin_write(slot);
                    
                    airlock.set_bank_hash_components(slot, BankHashComponents {
                        slot,
                        bank_hash: Hash::new_unique(),
                        parent_bank_hash: Hash::new_unique(),
                        signature_count: slot,
                        last_blockhash: Hash::new_unique(),
                        accounts_delta_hash: None,
                        accounts_lthash_checksum: None,
                        epoch_accounts_hash: None,
                        cumulative_lthash: LtHash::identity(),
                        delta_lthash: LtHash::identity(),
                    });
                    
                    for j in 0..changes_per_slot {
                        airlock.buffer_account_change(OwnedAccountChange {
                            pubkey: Pubkey::new_unique(),
                            slot,
                            old_account: AccountSharedData::default(),
                            new_account: AccountSharedData::new(j as u64, 32, &Pubkey::default()),
                            old_lthash: LtHash::identity(),
                            new_lthash: LtHash::identity(),
                        });
                    }
                    
                    airlock.update_slot_status(slot, SlotStatus::Rooted);
                    drop(writer);
                    
                    // Simulate slot timing
                    std::thread::sleep(Duration::from_millis(50));
                }
            })
        })
        .collect();
    
    // Wait for all writers
    for h in handles {
        h.join().unwrap();
    }
    
    // Wait for database writes to complete
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Verify results
    let slot_count: i64 = test_client
        .query_one("SELECT COUNT(*) FROM slots", &[])
        .await.unwrap()
        .get(0);
    assert_eq!(slot_count, num_slots as i64);
    
    let total_changes: i64 = test_client
        .query_one("SELECT COUNT(*) FROM account_changes", &[])
        .await.unwrap()
        .get(0);
    assert_eq!(total_changes, (num_slots * changes_per_slot) as i64);
    
    // Check performance metrics
    let metrics = airlock.get_metrics();
    println!("High throughput test metrics:");
    println!("  Slots processed: {}", metrics.slots_completed.load(std::sync::atomic::Ordering::Relaxed));
    println!("  Account changes: {}", metrics.account_changes_buffered.load(std::sync::atomic::Ordering::Relaxed));
    println!("  Cache hit rate: {:.2}%", 
        metrics.hot_cache_hits.load(std::sync::atomic::Ordering::Relaxed) as f64 /
        (metrics.hot_cache_hits.load(std::sync::atomic::Ordering::Relaxed) + 
         metrics.hot_cache_misses.load(std::sync::atomic::Ordering::Relaxed)) as f64 * 100.0
    );
    
    // Shutdown
    for _ in 0..4 {
        db_sender.send(crate::db_writer::DbCommand::Shutdown).unwrap();
    }
    
    cleanup_test_db(&db_name).await;
}