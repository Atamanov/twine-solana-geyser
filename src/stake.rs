use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use log::*;

// The Stake program ID
pub const STAKE_PROGRAM_ID: &str = "Stake11111111111111111111111111111111111111";

// Epoch schedule constants for mainnet
pub const SLOTS_PER_EPOCH: u64 = 432_000;

// Based on programs/stake/src/stake_state.rs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StakeStateV2 {
    Uninitialized,
    Initialized(Meta),
    Stake(Meta, Stake, StakeFlags),
    RewardsPool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Meta {
    pub rent_exempt_reserve: u64,
    pub authorized: Authorized,
    pub lockup: Lockup,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stake {
    pub delegation: Delegation,
    pub credits_observed: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delegation {
    pub voter_pubkey: Pubkey,
    pub stake: u64,
    pub activation_epoch: u64,
    pub deactivation_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Authorized {
    pub staker: Pubkey,
    pub withdrawer: Pubkey,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Lockup {
    pub unix_timestamp: i64,
    pub epoch: u64,
    pub custodian: Pubkey,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StakeFlags {
    pub bits: u8,
}

impl Default for StakeFlags {
    fn default() -> Self {
        Self { bits: 0 }
    }
}

// Helper to deserialize stake account data
pub fn deserialize_stake_state(data: &[u8]) -> Result<StakeStateV2, Box<dyn std::error::Error>> {
    bincode::deserialize(data).map_err(|e| e.into())
}

// Check if an account is owned by the Stake program
pub fn is_stake_account(owner: &Pubkey) -> bool {
    owner == &Pubkey::from_str(STAKE_PROGRAM_ID).unwrap()
}

// Calculate epoch from slot
pub fn slot_to_epoch(slot: u64) -> u64 {
    slot / SLOTS_PER_EPOCH
}

// Get epoch boundaries
pub fn get_epoch_boundaries(epoch: u64) -> (u64, u64) {
    let start_slot = epoch * SLOTS_PER_EPOCH;
    let end_slot = start_slot + SLOTS_PER_EPOCH - 1;
    (start_slot, end_slot)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeAccountInfo {
    pub stake_pubkey: String,
    pub voter_pubkey: Option<String>,
    pub stake_amount: u64,
    pub activation_epoch: Option<u64>,
    pub deactivation_epoch: Option<u64>,
    pub credits_observed: Option<u64>,
    pub rent_exempt_reserve: u64,
    pub staker: Option<String>,
    pub withdrawer: Option<String>,
    pub state_type: String,
}

impl StakeAccountInfo {
    pub fn from_state(pubkey: &Pubkey, state: &StakeStateV2) -> Self {
        match state {
            StakeStateV2::Uninitialized => StakeAccountInfo {
                stake_pubkey: pubkey.to_string(),
                voter_pubkey: None,
                stake_amount: 0,
                activation_epoch: None,
                deactivation_epoch: None,
                credits_observed: None,
                rent_exempt_reserve: 0,
                staker: None,
                withdrawer: None,
                state_type: "uninitialized".to_string(),
            },
            StakeStateV2::Initialized(meta) => StakeAccountInfo {
                stake_pubkey: pubkey.to_string(),
                voter_pubkey: None,
                stake_amount: 0,
                activation_epoch: None,
                deactivation_epoch: None,
                credits_observed: None,
                rent_exempt_reserve: meta.rent_exempt_reserve,
                staker: Some(meta.authorized.staker.to_string()),
                withdrawer: Some(meta.authorized.withdrawer.to_string()),
                state_type: "initialized".to_string(),
            },
            StakeStateV2::Stake(meta, stake, _flags) => StakeAccountInfo {
                stake_pubkey: pubkey.to_string(),
                voter_pubkey: Some(stake.delegation.voter_pubkey.to_string()),
                stake_amount: stake.delegation.stake,
                activation_epoch: Some(stake.delegation.activation_epoch),
                deactivation_epoch: if stake.delegation.deactivation_epoch == u64::MAX {
                    None
                } else {
                    Some(stake.delegation.deactivation_epoch)
                },
                credits_observed: Some(stake.credits_observed),
                rent_exempt_reserve: meta.rent_exempt_reserve,
                staker: Some(meta.authorized.staker.to_string()),
                withdrawer: Some(meta.authorized.withdrawer.to_string()),
                state_type: "stake".to_string(),
            },
            StakeStateV2::RewardsPool => StakeAccountInfo {
                stake_pubkey: pubkey.to_string(),
                voter_pubkey: None,
                stake_amount: 0,
                activation_epoch: None,
                deactivation_epoch: None,
                credits_observed: None,
                rent_exempt_reserve: 0,
                staker: None,
                withdrawer: None,
                state_type: "rewards_pool".to_string(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValidatorStakeInfo {
    pub validator_pubkey: String,
    pub total_stake: u64,
}

// Aggregate stakes by validator from a list of stake accounts
pub fn aggregate_stakes_by_validator(stakes: &[StakeAccountInfo]) -> Vec<ValidatorStakeInfo> {
    use std::collections::HashMap;
    
    let mut validator_stakes: HashMap<String, u64> = HashMap::new();
    
    for stake in stakes {
        if let Some(voter_pubkey) = &stake.voter_pubkey {
            // Only count active stakes (not deactivating)
            if stake.deactivation_epoch.is_none() && stake.stake_amount > 0 {
                *validator_stakes.entry(voter_pubkey.clone()).or_insert(0) += stake.stake_amount;
            }
        }
    }
    
    validator_stakes.into_iter()
        .map(|(validator_pubkey, total_stake)| ValidatorStakeInfo {
            validator_pubkey,
            total_stake,
        })
        .collect()
}