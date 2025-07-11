use clap::Parser;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use std::str::FromStr;
use tokio_postgres::{NoTls, Row};

#[derive(Parser, Debug)]
#[clap(author, version, about = "Verify vote transaction signatures from database")]
struct Args {
    /// Database connection string
    #[clap(short, long, default_value = "host=localhost user=geyser_writer dbname=twine_solana_db")]
    database_url: String,
    
    /// Slot to verify (if not specified, verifies recent slots)
    #[clap(short, long)]
    slot: Option<i64>,
    
    /// Number of transactions to verify
    #[clap(short, long, default_value = "100")]
    limit: i64,
    
    /// Voter pubkey to filter by
    #[clap(short, long)]
    voter: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();
    
    // Connect to database
    let (client, connection) = tokio_postgres::connect(&args.database_url, NoTls).await?;
    
    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Build query
    let query = if let Some(slot) = args.slot {
        if let Some(voter) = args.voter {
            format!(
                "SELECT slot, voter_pubkey, vote_signature, vote_transaction, vote_type 
                 FROM vote_transactions 
                 WHERE slot = {} AND voter_pubkey = '{}' 
                 LIMIT {}",
                slot, voter, args.limit
            )
        } else {
            format!(
                "SELECT slot, voter_pubkey, vote_signature, vote_transaction, vote_type 
                 FROM vote_transactions 
                 WHERE slot = {} 
                 LIMIT {}",
                slot, args.limit
            )
        }
    } else if let Some(voter) = args.voter {
        format!(
            "SELECT slot, voter_pubkey, vote_signature, vote_transaction, vote_type 
             FROM vote_transactions 
             WHERE voter_pubkey = '{}' 
             ORDER BY slot DESC 
             LIMIT {}",
            voter, args.limit
        )
    } else {
        format!(
            "SELECT slot, voter_pubkey, vote_signature, vote_transaction, vote_type 
             FROM vote_transactions 
             ORDER BY slot DESC 
             LIMIT {}",
            args.limit
        )
    };
    
    // Execute query
    let rows = client.query(&query, &[]).await?;
    
    println!("Verifying {} vote transactions...\n", rows.len());
    
    let mut verified_count = 0;
    let mut failed_count = 0;
    
    for row in rows {
        let slot: i64 = row.get("slot");
        let voter_pubkey: String = row.get("voter_pubkey");
        let vote_signature: String = row.get("vote_signature");
        let vote_transaction: Vec<u8> = row.get("vote_transaction");
        let vote_type: String = row.get("vote_type");
        
        match verify_vote_signature(&vote_transaction, &vote_signature) {
            Ok(true) => {
                verified_count += 1;
                println!("✓ Slot {} - {} - {} - VERIFIED", slot, voter_pubkey, vote_type);
            }
            Ok(false) => {
                failed_count += 1;
                println!("✗ Slot {} - {} - {} - INVALID SIGNATURE", slot, voter_pubkey, vote_type);
            }
            Err(e) => {
                failed_count += 1;
                println!("✗ Slot {} - {} - {} - ERROR: {}", slot, voter_pubkey, vote_type, e);
            }
        }
    }
    
    println!("\n=== Summary ===");
    println!("Total transactions: {}", verified_count + failed_count);
    println!("Verified: {}", verified_count);
    println!("Failed: {}", failed_count);
    println!("Success rate: {:.2}%", (verified_count as f64 / (verified_count + failed_count) as f64) * 100.0);
    
    Ok(())
}

fn verify_vote_signature(
    transaction_bytes: &[u8],
    signature_str: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    // Deserialize the transaction
    let transaction: VersionedTransaction = bincode::deserialize(transaction_bytes)?;
    
    // Parse the signature
    let signature = Signature::from_str(signature_str)?;
    
    // Get the message bytes
    let message_bytes = transaction.message.serialize();
    
    // Get the public key (first account is the signer for vote transactions)
    let pubkey = transaction.message.static_account_keys()
        .get(0)
        .ok_or("No public key found in transaction")?;
    
    // Verify the signature
    let verified = signature.verify(pubkey.as_ref(), &message_bytes);
    
    Ok(verified)
}