mod types;
mod utils;

use clap::Parser;
use deadpool_postgres::{Config as DbConfig, Pool, Runtime as DbRuntime};
use log::*;
use solana_sdk::{
    pubkey::Pubkey,
    transaction::Transaction,
};
use std::str::FromStr;
use tokio_postgres::NoTls;
use chrono;

use types::*;
use utils::*;

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
    
    /// Output file for validation details (optional)
    #[arg(long, default_value = "validation_details.log")]
    validation_log: String,
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
    
    // Save validation details to separate file if validation failed
    if !proof_package.validation_results.lthash_validation.valid || 
       !proof_package.validation_results.checksum_validation.valid ||
       !proof_package.validation_results.chain_continuity ||
       proof_package.validation_results.vote_signature_validation.iter().any(|v| !v.valid) {
        save_validation_details(&proof_package, &args.validation_log)?;
        println!("\nValidation details saved to: {}", args.validation_log);
    }
    
    println!("\nProof package saved to: {}", args.output);

    Ok(())
}

/// Save validation details to a separate log file
fn save_validation_details(proof_package: &ProofPackage, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;
    
    let mut file = std::fs::File::create(file_path)?;
    
    writeln!(file, "=== Validation Details Report ===")?;
    writeln!(file, "Generated at: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"))?;
    writeln!(file, "Start slot: {}", proof_package.start_slot)?;
    writeln!(file, "End slot: {}", proof_package.end_slot)?;
    writeln!(file, "Account: {}", proof_package.account_pubkey)?;
    writeln!(file)?;
    
    writeln!(file, "=== Summary ===")?;
    writeln!(file, "Chain continuity: {}", proof_package.validation_results.chain_continuity)?;
    writeln!(file, "Bank hash validations: {}/{} valid", 
        proof_package.validation_results.bank_hash_validation.iter().filter(|v| v.valid).count(),
        proof_package.validation_results.bank_hash_validation.len()
    )?;
    writeln!(file, "LtHash validation: {}", proof_package.validation_results.lthash_validation.valid)?;
    writeln!(file, "Checksum validation: {}", proof_package.validation_results.checksum_validation.valid)?;
    writeln!(file, "Vote signature validations: {}/{} valid",
        proof_package.validation_results.vote_signature_validation.iter().filter(|v| v.valid).count(),
        proof_package.validation_results.vote_signature_validation.len()
    )?;
    writeln!(file)?;
    
    // Bank hash failures
    let failed_bank_hashes: Vec<_> = proof_package.validation_results.bank_hash_validation
        .iter()
        .filter(|v| !v.valid)
        .collect();
    if !failed_bank_hashes.is_empty() {
        writeln!(file, "=== Failed Bank Hash Validations ===")?;
        for validation in failed_bank_hashes {
            writeln!(file, "Slot {}: expected={}, calculated={}", 
                validation.slot, validation.expected_hash, validation.calculated_hash)?;
        }
        writeln!(file)?;
    }
    
    // LtHash validation details
    if !proof_package.validation_results.lthash_validation.valid {
        writeln!(file, "=== LtHash Validation Details ===")?;
        writeln!(file, "{}", proof_package.validation_results.lthash_validation.details)?;
        writeln!(file)?;
    }
    
    // Vote signature failures
    let failed_votes: Vec<_> = proof_package.validation_results.vote_signature_validation
        .iter()
        .filter(|v| !v.valid)
        .collect();
    if !failed_votes.is_empty() {
        writeln!(file, "=== Failed Vote Signatures ===")?;
        for vote in failed_votes {
            writeln!(file, "Slot {}, Voter {}: {}", 
                vote.slot, 
                vote.voter_pubkey,
                vote.error.as_ref().unwrap_or(&"Unknown error".to_string())
            )?;
        }
    }
    
    Ok(())
}

async fn generate_proof_package(
    pool: &Pool,
    start_slot: u64,
    account_pubkey: Pubkey,
) -> Result<ProofPackage, Box<dyn std::error::Error>> {
    let client = pool.get().await?;

    // Find the last slot where the account changed
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

    // Fetch complete slot chain from start to end
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

    // Fetch all account changes for the last slot
    let changes_query = r#"
        SELECT 
            slot, account_pubkey, write_version,
            old_lamports, old_owner, old_executable, old_rent_epoch, old_data, old_lthash,
            new_lamports, new_owner, new_executable, new_rent_epoch, new_data, new_lthash
        FROM account_changes
        WHERE slot = $1
        ORDER BY write_version
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

    // Fetch vote transactions for the last slot
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

    // Validate the proof package
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
    // Check chain continuity
    let chain_continuity = validate_chain_continuity(slot_chain);

    // Validate bank hashes
    let bank_hash_validation = validate_bank_hashes(slot_chain);

    // Validate LtHash transformation
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

    // Validate checksum
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

    // Validate vote signatures
    let vote_signature_validation = validate_vote_signatures(vote_transactions);

    ValidationResults {
        chain_continuity,
        bank_hash_validation,
        lthash_validation,
        checksum_validation,
        vote_signature_validation,
    }
}

/// Validate chain continuity
fn validate_chain_continuity(slot_chain: &[SlotData]) -> bool {
    for i in 1..slot_chain.len() {
        let prev_slot = &slot_chain[i - 1];
        let curr_slot = &slot_chain[i];
        
        if curr_slot.parent_bank_hash != prev_slot.bank_hash {
            warn!(
                "Chain break at slot {}: parent_bank_hash {} doesn't match previous bank_hash {}",
                curr_slot.slot, curr_slot.parent_bank_hash, prev_slot.bank_hash
            );
            return false;
        }
    }
    true
}

/// Validate bank hashes for the slot chain
fn validate_bank_hashes(slot_chain: &[SlotData]) -> Vec<BankHashValidation> {
    slot_chain.iter().map(|slot| {
        let calculated_hash = match calculate_bank_hash(
            &slot.parent_bank_hash,
            &slot.last_blockhash,
            slot.signature_count,
            &slot.cumulative_lthash,
        ) {
            Ok(hash) => hash.to_string(),
            Err(e) => {
                warn!("Failed to calculate bank hash for slot {}: {}", slot.slot, e);
                "Error".to_string()
            }
        };
        
        let valid = calculated_hash == slot.bank_hash;
        
        if !valid {
            warn!(
                "Bank hash mismatch at slot {}: expected {}, calculated {}",
                slot.slot, slot.bank_hash, calculated_hash
            );
        }

        BankHashValidation {
            slot: slot.slot,
            expected_hash: slot.bank_hash.clone(),
            calculated_hash,
            valid,
        }
    }).collect()
}

/// Validate LtHash transformation between two slots
fn validate_lthash_transformation(
    prev_slot: &SlotData,
    last_slot: &SlotData,
    account_changes: &[AccountChange],
    target_pubkey: &str,
) -> LtHashValidation {
    let mut details = String::new();
    
    // Count monitored vs other accounts
    let monitored_count = account_changes.iter()
        .filter(|c| c.account_pubkey == target_pubkey)
        .count();
    let other_count = account_changes.len() - monitored_count;
    
    details.push_str(&format!(
        "Found {} changes for monitored account, {} other changes\n",
        monitored_count, other_count
    ));
    
    // Parse LtHashes
    let (prev_cumulative, stored_cumulative, stored_delta) = match (
        parse_lthash(&prev_slot.cumulative_lthash),
        parse_lthash(&last_slot.cumulative_lthash),
        parse_lthash(&last_slot.delta_lthash),
    ) {
        (Ok(prev), Ok(curr), Ok(delta)) => (prev, curr, delta),
        (Err(e), _, _) => {
            details.push_str(&format!("Failed to parse previous cumulative LtHash: {}\n", e));
            return create_error_validation(prev_slot.slot, last_slot.slot, details);
        }
        (_, Err(e), _) => {
            details.push_str(&format!("Failed to parse current cumulative LtHash: {}\n", e));
            return create_error_validation(prev_slot.slot, last_slot.slot, details);
        }
        (_, _, Err(e)) => {
            details.push_str(&format!("Failed to parse delta LtHash: {}\n", e));
            return create_error_validation(prev_slot.slot, last_slot.slot, details);
        }
    };
    
    // Verify stored data consistency
    let mut test_cumulative = prev_cumulative.clone();
    test_cumulative.mix_in(&stored_delta);
    if test_cumulative != stored_cumulative {
        warn!("Stored LtHash data inconsistent: prev + delta != cumulative");
        details.push_str("ERROR: Stored data inconsistency detected\n");
    }
    
    // Calculate cumulative from account changes
    let mut calculated_cumulative = prev_cumulative.clone();
    let mut mismatch_count = 0;
    
    for change in account_changes {
        let pubkey = match Pubkey::from_str(&change.account_pubkey) {
            Ok(pk) => pk,
            Err(e) => {
                details.push_str(&format!("Failed to parse pubkey {}: {}\n", change.account_pubkey, e));
                continue;
            }
        };
        
        // Handle negative lamports
        let old_lamports = if change.old_lamports < 0 { 0 } else { change.old_lamports as u64 };
        let new_lamports = if change.new_lamports < 0 { 0 } else { change.new_lamports as u64 };
        
        // Calculate and verify account LtHashes
        let calculated_old = calculate_account_lthash(
            old_lamports,
            change.old_rent_epoch as u64,
            &change.old_data,
            change.old_executable,
            &change.old_owner,
            &pubkey,
        );
        
        let calculated_new = calculate_account_lthash(
            new_lamports,
            change.new_rent_epoch as u64,
            &change.new_data,
            change.new_executable,
            &change.new_owner,
            &pubkey,
        );
        
        match (parse_lthash(&change.old_lthash), parse_lthash(&change.new_lthash)) {
            (Ok(stored_old), Ok(stored_new)) => {
                // Check if our calculations match stored values
                if calculated_old != stored_old || calculated_new != stored_new {
                    mismatch_count += 1;
                    if mismatch_count <= 5 { // Only log first 5 mismatches
                        details.push_str(&format!(
                            "LtHash mismatch for account {}: old={}, new={}\n",
                            &change.account_pubkey[..8],
                            calculated_old == stored_old,
                            calculated_new == stored_new
                        ));
                    }
                }
                
                // Apply the transformation
                calculated_cumulative.mix_out(&stored_old);
                calculated_cumulative.mix_in(&stored_new);
            }
            _ => {
                details.push_str(&format!(
                    "Failed to parse LtHash for account {}\n",
                    change.account_pubkey
                ));
            }
        }
    }
    
    if mismatch_count > 5 {
        details.push_str(&format!("... and {} more mismatches\n", mismatch_count - 5));
    }
    
    // Verify final cumulative
    let valid = calculated_cumulative == stored_cumulative;
    
    // Calculate checksum
    let calculated_checksum = calculated_cumulative.checksum();
    let checksum_valid = if let Some(stored_checksum_str) = &last_slot.accounts_lthash_checksum {
        calculated_checksum.to_string() == *stored_checksum_str
    } else {
        true // No checksum to verify
    };
    
    details.push_str(&format!(
        "\nValidation result: cumulative={}, checksum={}\n",
        if valid { "PASS" } else { "FAIL" },
        if checksum_valid { "PASS" } else { "FAIL" }
    ));
    
    LtHashValidation {
        previous_slot: prev_slot.slot,
        last_slot: last_slot.slot,
        previous_cumulative_lthash: hex::encode(&prev_slot.cumulative_lthash),
        calculated_new_lthash: format_lthash(&calculated_cumulative),
        stored_new_lthash: format_lthash(&stored_cumulative),
        valid: valid && checksum_valid,
        details,
    }
}

/// Create an error validation result
fn create_error_validation(prev_slot: u64, last_slot: u64, details: String) -> LtHashValidation {
    LtHashValidation {
        previous_slot: prev_slot,
        last_slot,
        previous_cumulative_lthash: String::new(),
        calculated_new_lthash: "Error".to_string(),
        stored_new_lthash: String::new(),
        valid: false,
        details,
    }
}

/// Validate checksum for a slot
fn validate_checksum(slot: &SlotData) -> ChecksumValidation {
    match parse_lthash(&slot.cumulative_lthash) {
        Ok(lthash) => {
            let checksum = lthash.checksum();
            let calculated_checksum = checksum.to_string();
            
            let valid = if let Some(stored) = &slot.accounts_lthash_checksum {
                &calculated_checksum == stored
            } else {
                true // No checksum stored
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

/// Validate vote signatures
fn validate_vote_signatures(vote_transactions: &[VoteTransaction]) -> Vec<VoteSignatureValidation> {
    vote_transactions.iter().map(|vote_tx| {
        match bincode::deserialize::<Transaction>(&vote_tx.vote_transaction) {
            Ok(transaction) => {
                if let Some(sig) = transaction.signatures.first() {
                    let expected_signature = sig.to_string();
                    
                    if expected_signature == vote_tx.vote_signature {
                        match transaction.verify() {
                            Ok(_) => VoteSignatureValidation {
                                slot: vote_tx.slot,
                                voter_pubkey: vote_tx.voter_pubkey.clone(),
                                vote_signature: vote_tx.vote_signature.clone(),
                                expected_signature,
                                valid: true,
                                error: None,
                            },
                            Err(e) => VoteSignatureValidation {
                                slot: vote_tx.slot,
                                voter_pubkey: vote_tx.voter_pubkey.clone(),
                                vote_signature: vote_tx.vote_signature.clone(),
                                expected_signature,
                                valid: false,
                                error: Some(format!("Signature verification failed: {:?}", e)),
                            }
                        }
                    } else {
                        VoteSignatureValidation {
                            slot: vote_tx.slot,
                            voter_pubkey: vote_tx.voter_pubkey.clone(),
                            vote_signature: vote_tx.vote_signature.clone(),
                            expected_signature,
                            valid: false,
                            error: Some("Signature mismatch".to_string()),
                        }
                    }
                } else {
                    VoteSignatureValidation {
                        slot: vote_tx.slot,
                        voter_pubkey: vote_tx.voter_pubkey.clone(),
                        vote_signature: vote_tx.vote_signature.clone(),
                        expected_signature: String::new(),
                        valid: false,
                        error: Some("No signatures in transaction".to_string()),
                    }
                }
            }
            Err(e) => VoteSignatureValidation {
                slot: vote_tx.slot,
                voter_pubkey: vote_tx.voter_pubkey.clone(),
                vote_signature: vote_tx.vote_signature.clone(),
                expected_signature: String::new(),
                valid: false,
                error: Some(format!("Failed to deserialize transaction: {}", e)),
            }
        }
    }).collect()
}