#[cfg(test)]
mod tests {
    use twine_solana_geyser::airlock::{AirlockManager, types::OwnedReplicaAccountInfo};
    use solana_sdk::pubkey::Pubkey;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_airlock_basic_operations() {
        let monitored_pubkey = Pubkey::new_unique();
        let manager = Arc::new(AirlockManager::new(vec![monitored_pubkey]));

        let account_info = OwnedReplicaAccountInfo {
            pubkey: monitored_pubkey,
            lamports: 1000,
            owner: Pubkey::default(),
            executable: false,
            rent_epoch: 0,
            data: vec![1, 2, 3, 4],
            write_version: 1,
            slot: 100,
            txn_signature: None,
        };

        manager.add_account_update(100, account_info).unwrap();

        let stats = manager.get_stats();
        assert_eq!(stats.total_updates, 1);
        assert_eq!(stats.active_slots, 1);

        let sealed_data = manager.seal_slot(100).unwrap();
        assert_eq!(sealed_data.len(), 1);
        assert_eq!(sealed_data[0].lamports, 1000);

        let stats = manager.get_stats();
        assert_eq!(stats.sealed_slots, 1);
        assert_eq!(stats.active_slots, 0);
    }

    #[test]
    fn test_concurrent_updates() {
        let monitored_pubkey = Pubkey::new_unique();
        let manager = Arc::new(AirlockManager::new(vec![monitored_pubkey]));
        let slot = 200;
        let num_threads = 10;
        let updates_per_thread = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let manager_clone = manager.clone();
                thread::spawn(move || {
                    for i in 0..updates_per_thread {
                        let account_info = OwnedReplicaAccountInfo {
                            pubkey: monitored_pubkey,
                            lamports: (thread_id * 1000 + i) as u64,
                            owner: Pubkey::default(),
                            executable: false,
                            rent_epoch: 0,
                            data: vec![thread_id as u8, i as u8],
                            write_version: i as u64,
                            slot,
                            txn_signature: None,
                        };
                        manager_clone.add_account_update(slot, account_info).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let sealed_data = manager.seal_slot(slot).unwrap();
        assert_eq!(sealed_data.len(), num_threads * updates_per_thread);

        let stats = manager.get_stats();
        assert_eq!(stats.total_updates, num_threads * updates_per_thread);
        assert_eq!(stats.sealed_slots, 1);
    }

    #[test]
    fn test_monitored_accounts_filtering() {
        let monitored_pubkey = Pubkey::new_unique();
        let unmonitored_pubkey = Pubkey::new_unique();
        let manager = AirlockManager::new(vec![monitored_pubkey]);

        assert!(manager.is_account_monitored(&monitored_pubkey));
        assert!(!manager.is_account_monitored(&unmonitored_pubkey));

        let empty_manager = AirlockManager::new(vec![]);
        assert!(empty_manager.is_account_monitored(&monitored_pubkey));
        assert!(empty_manager.is_account_monitored(&unmonitored_pubkey));
    }

    #[test]
    fn test_old_slot_cleanup() {
        let manager = AirlockManager::new(vec![]);
        
        for slot in 1..=10 {
            let account_info = OwnedReplicaAccountInfo {
                pubkey: Pubkey::new_unique(),
                lamports: slot * 100,
                owner: Pubkey::default(),
                executable: false,
                rent_epoch: 0,
                data: vec![],
                write_version: 1,
                slot,
                txn_signature: None,
            };
            manager.add_account_update(slot, account_info).unwrap();
        }

        assert_eq!(manager.get_stats().active_slots, 10);

        manager.clear_old_slots(10, 5);
        
        assert_eq!(manager.get_stats().active_slots, 5);
    }
}