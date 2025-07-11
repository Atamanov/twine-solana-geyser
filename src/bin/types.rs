use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ProofPackage {
    pub start_slot: u64,
    pub end_slot: u64,
    pub account_pubkey: String,
    pub slot_chain: Vec<SlotData>,
    pub account_changes: Vec<AccountChange>,
    pub vote_transactions: Vec<VoteTransaction>,
    pub validation_results: ValidationResults,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SlotData {
    pub slot: u64,
    pub bank_hash: String,
    pub parent_bank_hash: String,
    pub signature_count: u64,
    pub last_blockhash: String,
    pub cumulative_lthash: Vec<u8>,
    pub delta_lthash: Vec<u8>,
    pub accounts_delta_hash: Option<String>,
    pub accounts_lthash_checksum: Option<String>,
    pub epoch_accounts_hash: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountChange {
    pub slot: u64,
    pub account_pubkey: String,
    pub write_version: i64,
    pub old_lamports: i64,
    pub old_owner: String,
    pub old_executable: bool,
    pub old_rent_epoch: i64,
    pub old_data: Vec<u8>,
    pub old_lthash: Vec<u8>,
    pub new_lamports: i64,
    pub new_owner: String,
    pub new_executable: bool,
    pub new_rent_epoch: i64,
    pub new_data: Vec<u8>,
    pub new_lthash: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationResults {
    pub chain_continuity: bool,
    pub bank_hash_validation: Vec<BankHashValidation>,
    pub lthash_validation: LtHashValidation,
    pub checksum_validation: ChecksumValidation,
    pub vote_signature_validation: Vec<VoteSignatureValidation>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BankHashValidation {
    pub slot: u64,
    pub expected_hash: String,
    pub calculated_hash: String,
    pub valid: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LtHashValidation {
    pub previous_slot: u64,
    pub last_slot: u64,
    pub previous_cumulative_lthash: String,
    pub calculated_new_lthash: String,
    pub stored_new_lthash: String,
    pub valid: bool,
    pub details: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChecksumValidation {
    pub slot: u64,
    pub calculated_checksum: String,
    pub stored_checksum: Option<String>,
    pub valid: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VoteTransaction {
    pub slot: u64,
    pub voter_pubkey: String,
    pub vote_signature: String,
    pub vote_transaction: Vec<u8>,
    pub transaction_meta: Option<serde_json::Value>,
    pub vote_type: String,
    pub vote_slot: Option<u64>,
    pub vote_hash: Option<String>,
    pub root_slot: Option<u64>,
    pub lockouts_count: Option<u64>,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VoteSignatureValidation {
    pub slot: u64,
    pub voter_pubkey: String,
    pub vote_signature: String,
    pub expected_signature: String,
    pub valid: bool,
    pub error: Option<String>,
}