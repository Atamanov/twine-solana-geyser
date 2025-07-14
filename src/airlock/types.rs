use crossbeam_queue::SegQueue;
use log::error;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::AtomicUsize;

pub type Slot = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteTransaction {
    pub voter_pubkey: String,
    pub vote_signature: String,
    pub vote_transaction: Vec<u8>,  // Serialized transaction
    pub transaction_meta: Option<serde_json::Value>,
    pub vote_type: String,  // Type of vote instruction
    pub vote_slot: Option<u64>,  // The slot being voted for
    pub vote_hash: Option<String>,  // The hash being voted for
    pub root_slot: Option<u64>,  // Root slot for TowerSync
    pub lockouts_count: Option<u64>,  // Number of lockouts
    pub timestamp: Option<i64>,  // Vote timestamp
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnedReplicaAccountInfo {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub write_version: u64,
    pub slot: Slot,
    pub txn_signature: Option<String>,
}

// Re-export types from agave geyser plugin interface
pub use agave_geyser_plugin_interface::geyser_plugin_interface::{
    BankHashComponentsInfo, OwnedAccountChange, ReplicaAccountInfo, ReplicaAccountInfoV2,
    ReplicaAccountInfoV3, ReplicaAccountInfoVersions, ReplicaBlockInfo, ReplicaBlockInfoVersions,
    ReplicaEntryInfo, ReplicaEntryInfoV2, ReplicaEntryInfoVersions, ReplicaTransactionInfo,
    ReplicaTransactionInfoVersions,
};

// Import LtHash from the correct location
pub use solana_lattice_hash::lt_hash::LtHash;

impl OwnedReplicaAccountInfo {
    pub fn from_replica(
        info: &ReplicaAccountInfoVersions,
        slot: Slot,
        pubkey: &Pubkey,
    ) -> Result<Self, String> {
        match info {
            ReplicaAccountInfoVersions::V0_0_1(_) => {
                let error_msg =
                    "Unsupported replica account info version V0_0_1. Only V0_0_3 is supported.";
                error!("{} Account: {}, Slot: {}", error_msg, pubkey, slot);
                Err(error_msg.to_string())
            }
            ReplicaAccountInfoVersions::V0_0_2(_) => {
                let error_msg =
                    "Unsupported replica account info version V0_0_2. Only V0_0_3 is supported.";
                error!("{} Account: {}, Slot: {}", error_msg, pubkey, slot);
                Err(error_msg.to_string())
            }
            ReplicaAccountInfoVersions::V0_0_3(account_info) => Ok(Self {
                pubkey: *pubkey,
                lamports: account_info.lamports,
                owner: Pubkey::try_from(account_info.owner).unwrap_or_default(),
                executable: account_info.executable,
                rent_epoch: account_info.rent_epoch,
                data: account_info.data.to_vec(),
                write_version: account_info.write_version,
                slot,
                txn_signature: None, // V3 doesn't have txn_signature
            }),
        }
    }
}

#[derive(Debug)]
pub struct SlotAirlock {
    pub queue: SegQueue<OwnedReplicaAccountInfo>,
    pub active_writers: AtomicUsize,
    pub sealed_data: Option<Vec<OwnedReplicaAccountInfo>>,
}

// Removed old AirlockSlotData - now using OptimizedSlotData from optimized_types.rs

impl SlotAirlock {
    pub fn new() -> Self {
        Self {
            queue: SegQueue::new(),
            active_writers: AtomicUsize::new(0),
            sealed_data: None,
        }
    }
}

// AirlockSlotData impl removed - now using OptimizedSlotData

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NetworkMode {
    Devnet,
    Mainnet,
}

impl NetworkMode {
    pub fn rpc_endpoint(&self) -> &'static str {
        match self {
            NetworkMode::Devnet => "https://api.devnet.solana.com",
            NetworkMode::Mainnet => "https://api.mainnet-beta.solana.com",
        }
    }
}

// Custom deserializer to handle both "mainnet" and "mainnet-beta"
impl std::str::FromStr for NetworkMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "devnet" => Ok(NetworkMode::Devnet),
            "mainnet" | "mainnet-beta" => Ok(NetworkMode::Mainnet),
            _ => Err(format!("Unknown network mode: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiServerConfig {
    pub bind_address: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsServerConfig {
    pub bind_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    pub monitored_accounts: Vec<String>,
    
    // Database configuration
    pub connection_string: String,
    
    // Worker pool configuration
    pub worker_pool_size: usize,
    pub max_queue_size: usize,
    pub batch_size: usize,

    // API server configuration
    pub api_server: Option<ApiServerConfig>,

    // Metrics server configuration  
    pub metrics_server: Option<MetricsServerConfig>,

    // Logging configuration
    #[serde(default = "default_log_file")]
    pub log_file: Option<String>,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_log_file() -> Option<String> {
    Some("twine-geyser-plugin.log".to_string())
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            monitored_accounts: Vec::new(),
            connection_string: "postgresql://geyser_writer:geyser_writer_password@localhost:5432/twine_solana_db".to_string(),
            worker_pool_size: 4,
            max_queue_size: 10000,
            batch_size: 1000,
            api_server: None,
            metrics_server: None,
            log_file: default_log_file(),
            log_level: default_log_level(),
        }
    }
}

/// Commands sent to the database writer pool
#[derive(Debug)]
pub enum DbWriteCommand {
    SlotData {
        slot: u64,
        bank_hash: String,
        parent_bank_hash: String,
        signature_count: u64,
        last_blockhash: String,
        delta_lthash: Vec<u8>,
        cumulative_lthash: Vec<u8>,
        // Block metadata fields
        blockhash: Option<String>,
        parent_slot: Option<u64>,
        executed_transaction_count: Option<u64>,
        entry_count: Option<u64>,
        // Vote transactions
        vote_transactions: Vec<VoteTransaction>,
    },
    SlotStatusUpdate {
        slot: u64,
        status: String,
    },
    AccountChanges {
        slot: u64,
        changes: Vec<OwnedAccountChange>,
    },
    ProofRequests {
        // NOTE: Proof requests are no longer processed by the geyser plugin
        // This variant is kept for backwards compatibility only
        requests: Vec<ProofRequest>,
    },
    EpochStakes {
        epoch: u64,
        stakes: Vec<ValidatorStake>,
    },
    StakeAccountChange {
        slot: u64,
        stake_info: StakeAccountInfo,
        lthash: Vec<u8>,
    },
    EpochValidatorSet {
        epoch: u64,
        epoch_start_slot: u64,
        epoch_end_slot: u64,
        computed_at_slot: u64,
        validators: Vec<EpochValidator>,
    },
    Shutdown,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochValidator {
    pub validator_pubkey: String,
    pub total_stake: u64,
    pub stake_percentage: f64,
    pub total_epoch_stake: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorStake {
    pub validator_pubkey: String,
    pub stake_amount: u64,
    pub stake_percentage: f64,
    pub total_epoch_stake: u64,
}

/// DEPRECATED: Proof requests are no longer processed by the geyser plugin
#[derive(Debug)]
pub struct ProofRequest {
    pub slot: u64,
    pub account_pubkey: String,
}
