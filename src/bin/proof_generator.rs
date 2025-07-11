use clap::Parser;
use deadpool_postgres::{Config as DbConfig, Pool, Runtime as DbRuntime};
use log::*;
use serde::{Deserialize, Serialize};
use solana_sdk::{
    hash::{Hash, Hasher},
    pubkey::Pubkey,
    transaction::Transaction,
};
use std::str::FromStr;
use tokio_postgres::NoTls;

// Use types from Solana/Agave
use solana_lattice_hash::lt_hash::LtHash;

/// CLI arguments for the proof generator
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Starting slot number
    #[arg(short, long)]
    start_slot: u64,

    /// Account public key to track
    #[arg(short, long)]
    pubkey: String,

    /// Database connection string
    #[arg(short, long, default_value = "host=localhost port=5432 user=geyser_writer password=geyser_writer_password dbname=twine_solana_db")]
    db_url: String,

    /// Output file for the proof package
    #[arg(short, long, default_value = "proof_package.json")]
    output: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProofPackage {
    start_slot: u64,
    end_slot: u64,
    account_pubkey: String,
    slot_chain: Vec<SlotData>,
    account_changes: Vec<AccountChange>,
    vote_transactions: Vec<VoteTransaction>,
    validation_results: ValidationResults,
}

#[derive(Debug, Serialize, Deserialize)]
struct SlotData {
    slot: u64,
    bank_hash: String,
    parent_bank_hash: String,
    signature_count: u64,
    last_blockhash: String,
    cumulative_lthash: Vec<u8>,
    delta_lthash: Vec<u8>,
    accounts_delta_hash: Option<String>,
    accounts_lthash_checksum: Option<String>,
    epoch_accounts_hash: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AccountChange {
    slot: u64,
    account_pubkey: String,
    write_version: i64,
    old_lamports: i64,
    old_owner: String,
    old_executable: bool,
    old_rent_epoch: i64,
    old_data: Vec<u8>,
    old_lthash: Vec<u8>,
    new_lamports: i64,
    new_owner: String,
    new_executable: bool,
    new_rent_epoch: i64,
    new_data: Vec<u8>,
    new_lthash: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ValidationResults {
    chain_continuity: bool,
    bank_hash_validation: Vec<BankHashValidation>,
    lthash_validation: LtHashValidation,
    checksum_validation: ChecksumValidation,
    vote_signature_validation: Vec<VoteSignatureValidation>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BankHashValidation {
    slot: u64,
    expected_hash: String,
    calculated_hash: String,
    valid: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct LtHashValidation {
    previous_slot: u64,
    last_slot: u64,
    previous_cumulative_lthash: String,
    calculated_new_lthash: String,
    stored_new_lthash: String,
    valid: bool,
    details: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChecksumValidation {
    slot: u64,
    calculated_checksum: String,
    stored_checksum: Option<String>,
    valid: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct VoteTransaction {
    slot: u64,
    voter_pubkey: String,
    vote_signature: String,
    vote_transaction: Vec<u8>,
    transaction_meta: Option<serde_json::Value>,
    vote_type: String,
    vote_slot: Option<u64>,
    vote_hash: Option<String>,
    root_slot: Option<u64>,
    lockouts_count: Option<u64>,
    timestamp: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct VoteSignatureValidation {
    slot: u64,
    voter_pubkey: String,
    vote_signature: String,
    expected_signature: String,
    valid: bool,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();

    // Parse the account pubkey
    let account_pubkey = Pubkey::from_str(&args.pubkey)?;

    // Create database pool
    let mut db_config = DbConfig::new();
    db_config.url = Some(args.db_url);
    db_config.dbname = Some("twine_solana_db".to_string());
    let pool = db_config.create_pool(Some(DbRuntime::Tokio1), NoTls)?;

    // Generate the proof package
    let proof_package = generate_proof_package(&pool, args.start_slot, account_pubkey).await?;

    // Save to file
    let json = serde_json::to_string_pretty(&proof_package)?;
    std::fs::write(&args.output, json)?;

    // Print summary
    println!("Proof package generated successfully!");
    println!("Start slot: {}", proof_package.start_slot);
    println!("End slot: {}", proof_package.end_slot);
    println!("Account: {}", proof_package.account_pubkey);
    println!("Slot chain length: {}", proof_package.slot_chain.len());
    println!("Account changes: {}", proof_package.account_changes.len());
    println!("Vote transactions: {}", proof_package.vote_transactions.len());
    println!("\nValidation Results:");
    println!("  Chain continuity: {}", proof_package.validation_results.chain_continuity);
    println!("  Bank hash validations: {}/{} valid", 
        proof_package.validation_results.bank_hash_validation.iter().filter(|v| v.valid).count(),
        proof_package.validation_results.bank_hash_validation.len()
    );
    println!("  LtHash validation: {}", proof_package.validation_results.lthash_validation.valid);
    println!("  Checksum validation: {}", proof_package.validation_results.checksum_validation.valid);
    println!("  Vote signature validations: {}/{} valid",
        proof_package.validation_results.vote_signature_validation.iter().filter(|v| v.valid).count(),
        proof_package.validation_results.vote_signature_validation.len()
    );
    println!("\nProof package saved to: {}", args.output);

    Ok(())
}

async fn generate_proof_package(
    pool: &Pool,
    start_slot: u64,
    account_pubkey: Pubkey,
) -> Result<ProofPackage, Box<dyn std::error::Error>> {
    let client = pool.get().await?;

    // 1. Find the last slot where the account changed
    let last_slot_query = r#"
        SELECT MAX(slot) as last_slot
        FROM account_changes
        WHERE account_pubkey = $1 AND slot >= $2
    "#;
    
    let row = client
        .query_one(last_slot_query, &[&account_pubkey.to_string(), &(start_slot as i64)])
        .await?;
    
    let end_slot = row.get::<_, Option<i64>>("last_slot")
        .ok_or("No account changes found after start slot")?;
    let end_slot = end_slot as u64;

    info!("Found last account change at slot {}", end_slot);

    // 2. Fetch complete slot chain from start to end
    let slots_query = r#"
        SELECT 
            slot, bank_hash, parent_bank_hash, signature_count, last_blockhash,
            cumulative_lthash, delta_lthash, accounts_delta_hash, 
            accounts_lthash_checksum, epoch_accounts_hash
        FROM slots
        WHERE slot >= $1 AND slot <= $2
        ORDER BY slot ASC
    "#;

    let rows = client
        .query(slots_query, &[&(start_slot as i64), &(end_slot as i64)])
        .await?;

    let mut slot_chain = Vec::new();
    for row in rows {
        slot_chain.push(SlotData {
            slot: row.get::<_, i64>("slot") as u64,
            bank_hash: row.get("bank_hash"),
            parent_bank_hash: row.get("parent_bank_hash"),
            signature_count: row.get::<_, i64>("signature_count") as u64,
            last_blockhash: row.get("last_blockhash"),
            cumulative_lthash: row.get("cumulative_lthash"),
            delta_lthash: row.get("delta_lthash"),
            accounts_delta_hash: row.get("accounts_delta_hash"),
            accounts_lthash_checksum: row.get("accounts_lthash_checksum"),
            epoch_accounts_hash: row.get("epoch_accounts_hash"),
        });
    }

    if slot_chain.is_empty() {
        return Err("No slots found in the specified range".into());
    }

    // 3. Fetch all account changes for the last slot
    let changes_query = r#"
        SELECT 
            slot, account_pubkey, write_version,
            old_lamports, old_owner, old_executable, old_rent_epoch, old_data, old_lthash,
            new_lamports, new_owner, new_executable, new_rent_epoch, new_data, new_lthash
        FROM account_changes
        WHERE slot = $1
        ORDER BY account_pubkey, write_version
    "#;

    let rows = client.query(changes_query, &[&(end_slot as i64)]).await?;

    let mut account_changes = Vec::new();
    for row in rows {
        account_changes.push(AccountChange {
            slot: row.get::<_, i64>("slot") as u64,
            account_pubkey: row.get("account_pubkey"),
            write_version: row.get("write_version"),
            old_lamports: row.get("old_lamports"),
            old_owner: row.get("old_owner"),
            old_executable: row.get("old_executable"),
            old_rent_epoch: row.get("old_rent_epoch"),
            old_data: row.get("old_data"),
            old_lthash: row.get("old_lthash"),
            new_lamports: row.get("new_lamports"),
            new_owner: row.get("new_owner"),
            new_executable: row.get("new_executable"),
            new_rent_epoch: row.get("new_rent_epoch"),
            new_data: row.get("new_data"),
            new_lthash: row.get("new_lthash"),
        });
    }

    // 4. Fetch vote transactions for the last slot
    let votes_query = r#"
        SELECT 
            slot, voter_pubkey, vote_signature, vote_transaction, transaction_meta,
            vote_type, vote_slot, vote_hash, root_slot, lockouts_count, timestamp
        FROM vote_transactions
        WHERE slot = $1
        ORDER BY voter_pubkey, vote_signature
    "#;

    let rows = client.query(votes_query, &[&(end_slot as i64)]).await?;

    let mut vote_transactions = Vec::new();
    for row in rows {
        vote_transactions.push(VoteTransaction {
            slot: row.get::<_, i64>("slot") as u64,
            voter_pubkey: row.get("voter_pubkey"),
            vote_signature: row.get("vote_signature"),
            vote_transaction: row.get("vote_transaction"),
            transaction_meta: row.get("transaction_meta"),
            vote_type: row.get("vote_type"),
            vote_slot: row.get::<_, Option<i64>>("vote_slot").map(|v| v as u64),
            vote_hash: row.get("vote_hash"),
            root_slot: row.get::<_, Option<i64>>("root_slot").map(|v| v as u64),
            lockouts_count: row.get::<_, Option<i32>>("lockouts_count").map(|v| v as u64),
            timestamp: row.get("timestamp"),
        });
    }

    info!("Found {} vote transactions for slot {}", vote_transactions.len(), end_slot);

    // 5. Validate the proof package
    let validation_results = validate_proof_package(&slot_chain, &account_changes, &vote_transactions, &account_pubkey.to_string());

    Ok(ProofPackage {
        start_slot,
        end_slot,
        account_pubkey: account_pubkey.to_string(),
        slot_chain,
        account_changes,
        vote_transactions,
        validation_results,
    })
}

fn validate_proof_package(
    slot_chain: &[SlotData],
    account_changes: &[AccountChange],
    vote_transactions: &[VoteTransaction],
    target_pubkey: &str,
) -> ValidationResults {
    // 1. Check chain continuity
    let mut chain_continuity = true;
    for i in 1..slot_chain.len() {
        let prev_slot = &slot_chain[i - 1];
        let curr_slot = &slot_chain[i];
        
        // Check if current slot's parent_bank_hash matches previous slot's bank_hash
        if curr_slot.parent_bank_hash != prev_slot.bank_hash {
            warn!(
                "Chain break at slot {}: parent_bank_hash {} doesn't match previous bank_hash {}",
                curr_slot.slot, curr_slot.parent_bank_hash, prev_slot.bank_hash
            );
            chain_continuity = false;
        }
    }

    // 2. Validate bank hashes
    let bank_hash_validation = validate_bank_hashes(slot_chain);

    // 3. Validate LtHash transformation
    let lthash_validation = if slot_chain.len() >= 2 && !account_changes.is_empty() {
        validate_lthash_transformation(
            &slot_chain[slot_chain.len() - 2],
            &slot_chain[slot_chain.len() - 1],
            account_changes,
            target_pubkey,
        )
    } else {
        LtHashValidation {
            previous_slot: 0,
            last_slot: 0,
            previous_cumulative_lthash: String::new(),
            calculated_new_lthash: String::new(),
            stored_new_lthash: String::new(),
            valid: false,
            details: "Insufficient data for LtHash validation".to_string(),
        }
    };

    // 4. Validate checksum
    let checksum_validation = if let Some(last_slot) = slot_chain.last() {
        validate_checksum(last_slot)
    } else {
        ChecksumValidation {
            slot: 0,
            calculated_checksum: String::new(),
            stored_checksum: None,
            valid: false,
        }
    };

    // 5. Validate vote signatures
    let vote_signature_validation = validate_vote_signatures(vote_transactions);

    ValidationResults {
        chain_continuity,
        bank_hash_validation,
        lthash_validation,
        checksum_validation,
        vote_signature_validation,
    }
}

fn validate_bank_hashes(slot_chain: &[SlotData]) -> Vec<BankHashValidation> {
    let mut validations = Vec::new();

    for slot in slot_chain {
        // Calculate bank hash from components
        let mut hasher = Hasher::default();
        
        // Add parent bank hash
        if let Ok(parent_hash) = Hash::from_str(&slot.parent_bank_hash) {
            hasher.hash(parent_hash.as_ref());
        }
        
        // Add accounts lthash checksum
        if let Some(checksum) = &slot.accounts_lthash_checksum {
            if let Ok(hash) = Hash::from_str(checksum) {
                hasher.hash(hash.as_ref());
            }
        }
        
        // Add signature count
        hasher.hash(&slot.signature_count.to_le_bytes());
        
        // Add last blockhash
        if let Ok(blockhash) = Hash::from_str(&slot.last_blockhash) {
            hasher.hash(blockhash.as_ref());
        }
        
        let calculated_hash = hasher.result();
        let valid = calculated_hash.to_string() == slot.bank_hash;
        
        if !valid {
            warn!(
                "Bank hash mismatch at slot {}: expected {}, calculated {}",
                slot.slot, slot.bank_hash, calculated_hash
            );
            warn!(
                "  Components: parent_bank_hash={}, accounts_delta_hash={:?}, accounts_lthash_checksum={:?}, signature_count={}, last_blockhash={}",
                slot.parent_bank_hash, slot.accounts_delta_hash, slot.accounts_lthash_checksum, slot.signature_count, slot.last_blockhash
            );
        }

        validations.push(BankHashValidation {
            slot: slot.slot,
            expected_hash: slot.bank_hash.clone(),
            calculated_hash: calculated_hash.to_string(),
            valid,
        });
    }

    validations
}

fn validate_lthash_transformation(
    prev_slot: &SlotData,
    last_slot: &SlotData,
    account_changes: &[AccountChange],
    target_pubkey: &str,
) -> LtHashValidation {
    let mut details = String::new();
    
    // Find changes for the target account
    let target_changes: Vec<_> = account_changes
        .iter()
        .filter(|c| c.account_pubkey == target_pubkey)
        .collect();
    
    let other_changes: Vec<_> = account_changes
        .iter()
        .filter(|c| c.account_pubkey != target_pubkey)
        .collect();
    
    details.push_str(&format!(
        "Found {} changes for target account, {} other changes\n",
        target_changes.len(),
        other_changes.len()
    ));
    
    // Parse LtHashes
    let prev_cumulative = match parse_lthash(&prev_slot.cumulative_lthash) {
        Ok(h) => h,
        Err(e) => {
            details.push_str(&format!("Failed to parse previous cumulative LtHash: {}\n", e));
            return LtHashValidation {
                previous_slot: prev_slot.slot,
                last_slot: last_slot.slot,
                previous_cumulative_lthash: hex::encode(&prev_slot.cumulative_lthash),
                calculated_new_lthash: "Error".to_string(),
                stored_new_lthash: hex::encode(&last_slot.cumulative_lthash),
                valid: false,
                details,
            };
        }
    };
    
    let stored_cumulative = match parse_lthash(&last_slot.cumulative_lthash) {
        Ok(h) => h,
        Err(e) => {
            details.push_str(&format!("Failed to parse stored cumulative LtHash: {}\n", e));
            return LtHashValidation {
                previous_slot: prev_slot.slot,
                last_slot: last_slot.slot,
                previous_cumulative_lthash: hex::encode(&prev_slot.cumulative_lthash),
                calculated_new_lthash: "Error".to_string(),
                stored_new_lthash: hex::encode(&last_slot.cumulative_lthash),
                valid: false,
                details,
            };
        }
    };
    
    // Start with previous cumulative
    let mut calculated = prev_cumulative;
    
    // Process all account changes
    for change in account_changes {
        // Parse old and new LtHashes
        match (parse_lthash(&change.old_lthash), parse_lthash(&change.new_lthash)) {
            (Ok(old_lt), Ok(new_lt)) => {
                // Subtract old, add new
                calculated.mix_out(&old_lt);
                calculated.mix_in(&new_lt);
                details.push_str(&format!(
                    "Account {}: subtracted old LtHash, added new LtHash\n",
                    &change.account_pubkey[..8]
                ));
            }
            (Err(e1), _) => {
                details.push_str(&format!(
                    "Failed to parse old LtHash for account {}: {}\n",
                    change.account_pubkey, e1
                ));
            }
            (_, Err(e2)) => {
                details.push_str(&format!(
                    "Failed to parse new LtHash for account {}: {}\n",
                    change.account_pubkey, e2
                ));
            }
        }
    }
    
    // Compare calculated with stored
    let valid = calculated == stored_cumulative;
    
    if !valid {
        details.push_str(&format!(
            "LtHash mismatch!\nCalculated: {:?}\nStored: {:?}\n",
            format_lthash(&calculated),
            format_lthash(&stored_cumulative)
        ));
    } else {
        details.push_str("LtHash validation successful!\n");
    }
    
    LtHashValidation {
        previous_slot: prev_slot.slot,
        last_slot: last_slot.slot,
        previous_cumulative_lthash: hex::encode(&prev_slot.cumulative_lthash),
        calculated_new_lthash: format_lthash(&calculated),
        stored_new_lthash: format_lthash(&stored_cumulative),
        valid,
        details,
    }
}

fn parse_lthash(bytes: &[u8]) -> Result<LtHash, String> {
    if bytes.len() != 2048 {
        return Err(format!("Invalid LtHash length: expected 2048, got {}", bytes.len()));
    }
    
    let mut arr = [0u16; LtHash::NUM_ELEMENTS];
    for i in 0..LtHash::NUM_ELEMENTS {
        arr[i] = u16::from_le_bytes(
            bytes[i * 2..(i + 1) * 2]
                .try_into()
                .map_err(|_| "Failed to convert bytes")?
        );
    }
    
    Ok(LtHash(arr))
}

fn format_lthash(lthash: &LtHash) -> String {
    // Convert LtHash to bytes and encode as hex
    let mut bytes = Vec::with_capacity(2048);
    for &val in &lthash.0 {
        bytes.extend_from_slice(&val.to_le_bytes());
    }
    hex::encode(&bytes)
}

fn validate_checksum(slot: &SlotData) -> ChecksumValidation {
    // Parse the LtHash first
    match parse_lthash(&slot.cumulative_lthash) {
        Ok(lthash) => {
            // Calculate checksum from LtHash
            let checksum = lthash.checksum();
            let calculated_checksum = checksum.to_string();
            
            let valid = if let Some(stored) = &slot.accounts_lthash_checksum {
                &calculated_checksum == stored
            } else {
                // No checksum stored (pre-LtHash fork)
                true
            };
            
            ChecksumValidation {
                slot: slot.slot,
                calculated_checksum,
                stored_checksum: slot.accounts_lthash_checksum.clone(),
                valid,
            }
        }
        Err(e) => {
            warn!("Failed to parse LtHash for checksum validation: {}", e);
            ChecksumValidation {
                slot: slot.slot,
                calculated_checksum: "Error".to_string(),
                stored_checksum: slot.accounts_lthash_checksum.clone(),
                valid: false,
            }
        }
    }
}

fn validate_vote_signatures(vote_transactions: &[VoteTransaction]) -> Vec<VoteSignatureValidation> {
    let mut validations = Vec::new();

    for vote_tx in vote_transactions {
        let mut valid = false;
        let mut error = None;
        let mut expected_signature = String::new();

        // Deserialize the transaction
        match bincode::deserialize::<Transaction>(&vote_tx.vote_transaction) {
            Ok(transaction) => {
                // Get the first signature (the payer/voter signature)
                if let Some(sig) = transaction.signatures.first() {
                    expected_signature = sig.to_string();
                    
                    // Compare with stored signature
                    if expected_signature == vote_tx.vote_signature {
                        // Verify the signature against the transaction message
                        match transaction.verify() {
                            Ok(_) => {
                                valid = true;
                            }
                            Err(e) => {
                                error = Some(format!("Signature verification failed: {:?}", e));
                            }
                        }
                    } else {
                        error = Some(format!(
                            "Signature mismatch: expected {}, got {}",
                            expected_signature, vote_tx.vote_signature
                        ));
                    }
                } else {
                    error = Some("No signatures found in transaction".to_string());
                }
            }
            Err(e) => {
                error = Some(format!("Failed to deserialize transaction: {}", e));
            }
        }

        validations.push(VoteSignatureValidation {
            slot: vote_tx.slot,
            voter_pubkey: vote_tx.voter_pubkey.clone(),
            vote_signature: vote_tx.vote_signature.clone(),
            expected_signature,
            valid,
            error,
        });
    }

    validations
}