#[cfg(test)]
pub mod test_helpers {
    use std::sync::Arc;
    use std::time::Duration;
    
    use solana_sdk::{
        pubkey::Pubkey,
        account::AccountSharedData,
        hash::Hash,
    };
    use solana_lattice_hash::lt_hash::LtHash;
    
    use crate::airlock::{AirLock, BankHashComponents, SlotStatus, OwnedAccountChange};
    
    pub fn create_test_account_change(slot: u64, lamports: u64) -> OwnedAccountChange {
        OwnedAccountChange {
            pubkey: Pubkey::new_unique(),
            slot,
            old_account: AccountSharedData::default(),
            new_account: AccountSharedData::new(lamports, 32, &Pubkey::default()),
            old_lthash: LtHash::identity(),
            new_lthash: LtHash::identity(),
        }
    }
    
    pub fn create_test_bank_hash(slot: u64) -> BankHashComponents {
        BankHashComponents {
            slot,
            bank_hash: Hash::new_unique(),
            parent_bank_hash: Hash::new_unique(),
            signature_count: slot * 100,
            last_blockhash: Hash::new_unique(),
            accounts_delta_hash: Some(Hash::new_unique()),
            accounts_lthash_checksum: Some("test".to_string()),
            epoch_accounts_hash: None,
            cumulative_lthash: LtHash::identity(),
            delta_lthash: LtHash::identity(),
        }
    }
    
    pub fn simulate_slot_processing(airlock: &Arc<AirLock>, slot: u64, num_changes: usize) {
        let writer = airlock.begin_write(slot);
        
        airlock.set_bank_hash_components(slot, create_test_bank_hash(slot));
        
        for i in 0..num_changes {
            airlock.buffer_account_change(create_test_account_change(slot, i as u64));
        }
        
        airlock.update_slot_status(slot, SlotStatus::Rooted);
        drop(writer);
    }
}