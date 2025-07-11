use crossbeam_queue::SegQueue;
use dashmap::DashMap;
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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;

#[derive(Debug)]
pub struct AirlockSlotData {
    /// Tracks how many validator threads are currently writing to this slot's buffer
    pub writer_count: AtomicU32,
    /// Per-thread buffers to eliminate contention - indexed by thread ID
    /// Using a fixed-size array would be better but ThreadId is opaque
    pub thread_buffers: DashMap<std::thread::ThreadId, Vec<OwnedAccountChange>>,
    /// Tracks if a monitored account was seen in this slot
    pub contains_monitored_change: AtomicBool,
    /// Bank hash components (may arrive at any time)
    pub bank_hash_components: parking_lot::RwLock<Option<BankHashComponentsInfo>>,
    /// LtHash data (may arrive at any time)
    pub delta_lthash: parking_lot::RwLock<Option<Vec<u8>>>,
    pub cumulative_lthash: parking_lot::RwLock<Option<Vec<u8>>>,
    /// Block metadata (may arrive at any time)
    pub blockhash: parking_lot::RwLock<Option<String>>,
    pub parent_slot: parking_lot::RwLock<Option<u64>>,
    pub executed_transaction_count: parking_lot::RwLock<Option<u64>>,
    pub entry_count: parking_lot::RwLock<Option<u64>>,
    /// Slot status (processed, confirmed, rooted, etc)
    pub status: parking_lot::RwLock<String>,
    /// Timestamp when slot was created
    pub created_at: std::time::Instant,
    /// Vote transactions in this slot
    pub vote_transactions: parking_lot::RwLock<Vec<VoteTransaction>>,
    /// Estimated memory usage in bytes
    pub memory_usage: AtomicUsize,
    /// Slot number when this slot was marked as rooted (for grace period handling)
    pub rooted_at_slot: parking_lot::RwLock<Option<u64>>,
}

impl SlotAirlock {
    pub fn new() -> Self {
        Self {
            queue: SegQueue::new(),
            active_writers: AtomicUsize::new(0),
            sealed_data: None,
        }
    }
}

impl AirlockSlotData {
    pub fn new() -> Self {
        Self {
            writer_count: AtomicU32::new(0),
            thread_buffers: DashMap::with_capacity(32), // Pre-size for typical thread count
            contains_monitored_change: AtomicBool::new(false),
            bank_hash_components: parking_lot::RwLock::new(None),
            delta_lthash: parking_lot::RwLock::new(None),
            cumulative_lthash: parking_lot::RwLock::new(None),
            blockhash: parking_lot::RwLock::new(None),
            parent_slot: parking_lot::RwLock::new(None),
            executed_transaction_count: parking_lot::RwLock::new(None),
            entry_count: parking_lot::RwLock::new(None),
            status: parking_lot::RwLock::new("created".to_string()),
            created_at: std::time::Instant::now(),
            vote_transactions: parking_lot::RwLock::new(Vec::with_capacity(300)),
            memory_usage: AtomicUsize::new(0),
            rooted_at_slot: parking_lot::RwLock::new(None),
        }
    }

    /// Check if we have all required data for database write
    pub fn has_complete_data(&self) -> bool {
        // Quick check using atomics to avoid locks
        // We only do the expensive lock-based check if likely to succeed
        if !self.is_rooted() {
            return false;
        }
        
        // Do all checks at once to minimize lock acquisition
        let has_bank_hash = self.bank_hash_components.read().is_some();
        if !has_bank_hash {
            return false;
        }
        
        let has_lthash = self.delta_lthash.read().is_some();
        if !has_lthash {
            return false;
        }
        
        let has_blockhash = self.blockhash.read().is_some();
        has_blockhash && 
        self.cumulative_lthash.read().is_some() &&
        self.parent_slot.read().is_some() &&
        self.executed_transaction_count.read().is_some() &&
        self.entry_count.read().is_some()
    }

    /// Check if slot is rooted
    pub fn is_rooted(&self) -> bool {
        *self.status.read() == "rooted"
    }
}

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
pub struct PluginConfig {
    pub monitored_accounts: Vec<String>,
    pub max_slots_tracked: usize,
    pub enable_lthash_notifications: bool,

    // Database configuration
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_password: String,
    pub db_name: String,

    // Worker pool configuration
    pub num_worker_threads: usize,
    pub db_connections_per_worker: usize,
    pub max_queue_size: usize,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,

    // API server configuration
    pub api_port: Option<u16>,

    // Proof scheduling
    pub proof_scheduling_slot_interval: u64,

    // Metrics
    pub metrics_port: u16,

    // Network mode
    pub network_mode: NetworkMode,

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
            max_slots_tracked: 100,
            enable_lthash_notifications: true,
            db_host: "localhost".to_string(),
            db_port: 5432,
            db_user: "geyser_writer".to_string(),
            db_password: "geyser_writer_password".to_string(),
            db_name: "twine_solana_db".to_string(),
            num_worker_threads: 4,
            db_connections_per_worker: 2,
            max_queue_size: 10000,
            batch_size: 1000,
            batch_timeout_ms: 100,
            api_port: None,
            proof_scheduling_slot_interval: 10,
            metrics_port: 9091,
            network_mode: NetworkMode::Mainnet,
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
        accounts_delta_hash: Option<String>,
        accounts_lthash_checksum: Option<String>,
        epoch_accounts_hash: Option<String>,
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

#[derive(Debug)]
pub struct ProofRequest {
    pub slot: u64,
    pub account_pubkey: String,
}
