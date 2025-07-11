use log::*;
use solana_lattice_hash::lt_hash::LtHash;
use solana_sdk::{
    hash::Hash,
    pubkey::Pubkey,
};
use std::str::FromStr;

/// Parse LtHash from bytes
pub fn parse_lthash(bytes: &[u8]) -> Result<LtHash, String> {
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

/// Format LtHash as hex string
pub fn format_lthash(lthash: &LtHash) -> String {
    let mut bytes = Vec::with_capacity(2048);
    for &val in &lthash.0 {
        bytes.extend_from_slice(&val.to_le_bytes());
    }
    hex::encode(&bytes)
}

/// Calculate account LtHash using Solana's algorithm
pub fn calculate_account_lthash(
    lamports: u64,
    _rent_epoch: u64,
    data: &[u8],
    executable: bool,
    owner: &str,
    pubkey: &Pubkey,
) -> LtHash {
    // Zero-lamport accounts have identity LtHash
    if lamports == 0 {
        return LtHash::identity();
    }
    
    // Parse owner pubkey
    let owner_pubkey = match Pubkey::from_str(owner) {
        Ok(pk) => pk,
        Err(_) => {
            warn!("Failed to parse owner pubkey: {}", owner);
            return LtHash::identity();
        }
    };
    
    // Hash account components in Solana's order
    let mut hasher = blake3::Hasher::new();
    
    hasher.update(&lamports.to_le_bytes());
    // rent_epoch is excluded for LtHash
    hasher.update(data);
    hasher.update(&[if executable { 1u8 } else { 0u8 }]);
    hasher.update(owner_pubkey.as_ref());
    hasher.update(pubkey.as_ref());
    
    LtHash::with(&hasher)
}

/// Calculate bank hash for a slot (post-LtHash era)
pub fn calculate_bank_hash(
    parent_bank_hash: &str,
    last_blockhash: &str,
    signature_count: u64,
    cumulative_lthash: &[u8],
) -> Result<Hash, String> {
    // Parse parent bank hash
    let parent_hash = Hash::from_str(parent_bank_hash)
        .map_err(|e| format!("Invalid parent_bank_hash: {}", e))?;
    
    // Parse last blockhash
    let last_blockhash = Hash::from_str(last_blockhash)
        .map_err(|e| format!("Invalid last_blockhash: {}", e))?;
    
    let signature_count_bytes = signature_count.to_le_bytes();
    
    // Step 1: Base hash (parent + signature count + last blockhash)
    let mut calculated_hash = solana_sdk::hash::hashv(&[
        parent_hash.as_ref(),
        &signature_count_bytes,
        last_blockhash.as_ref(),
    ]);

    // Step 2: Add cumulative LtHash bytes
    if cumulative_lthash.is_empty() {
        return Err("Missing cumulative_lthash".to_string());
    }
    
    calculated_hash = solana_sdk::hash::hashv(&[
        calculated_hash.as_ref(),
        cumulative_lthash,
    ]);
    
    Ok(calculated_hash)
}