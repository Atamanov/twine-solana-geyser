use crossbeam_queue::SegQueue;
use log::error;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::AtomicUsize;

pub type Slot = u64;

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
    /// Lock-free queue for all account changes in this slot
    pub buffer: SegQueue<OwnedAccountChange>,
    /// Tracks if a monitored account was seen in this slot
    pub contains_monitored_change: AtomicBool,
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
            buffer: SegQueue::new(),
            contains_monitored_change: AtomicBool::new(false),
        }
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
            db_user: "geyser".to_string(),
            db_password: "password".to_string(),
            db_name: "geyser_db".to_string(),
            num_worker_threads: 4,
            db_connections_per_worker: 2,
            max_queue_size: 10000,
            batch_size: 1000,
            batch_timeout_ms: 100,
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
    Shutdown,
}

#[derive(Debug)]
pub struct ProofRequest {
    pub slot: u64,
    pub account_pubkey: String,
}
