use actix_web::{web, App, HttpResponse, HttpServer, Result};
use dashmap::DashSet;
use log::*;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::sync::mpsc;

#[derive(Debug, Serialize, Deserialize)]
struct MonitoredAccount {
    pubkey: String,
    added_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MonitoredAccountsResponse {
    accounts: Vec<MonitoredAccount>,
    count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct AddAccountRequest {
    pubkey: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RemoveAccountRequest {
    pubkey: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ApiResponse {
    success: bool,
    message: String,
}

pub struct ApiState {
    pub monitored_accounts: Arc<DashSet<Pubkey>>,
    pub account_metadata: Arc<RwLock<std::collections::HashMap<Pubkey, chrono::DateTime<chrono::Utc>>>>,
    pub db_config: String,
}

#[derive(Debug)]
pub struct ApiServerHandle {
    shutdown_tx: mpsc::Sender<()>,
    thread_handle: Option<std::thread::JoinHandle<()>>,
}

pub fn start_api_server(
    monitored_accounts: Arc<DashSet<Pubkey>>,
    api_port: u16,
    db_config: String,
) -> ApiServerHandle {
    let account_metadata = Arc::new(RwLock::new(std::collections::HashMap::new()));
    
    let state = web::Data::new(ApiState {
        monitored_accounts,
        account_metadata,
        db_config,
    });

    info!("Starting API server on port {}", api_port);

    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    
    // Run actix-web in a separate thread with its own runtime
    let thread_handle = std::thread::spawn(move || {
        // Create a new tokio runtime for the API server
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");
        
        rt.block_on(async {
            let server = HttpServer::new(move || {
                App::new()
                    .app_data(state.clone())
                    .route("/api/monitored-accounts", web::get().to(get_monitored_accounts))
                    .route("/api/monitored-accounts", web::post().to(add_monitored_account))
                    .route("/api/monitored-accounts", web::delete().to(remove_monitored_account))
                    .route("/api/health", web::get().to(health_check))
            })
            .bind(("0.0.0.0", api_port))
            .expect("Failed to bind API server")
            .disable_signals()
            .run();
            
            let server_handle = server.handle();
            
            // Spawn a task to listen for shutdown signal
            let shutdown_handle = server_handle.clone();
            tokio::spawn(async move {
                // Wait for shutdown signal in a blocking manner
                let (tx, rx) = tokio::sync::oneshot::channel();
                std::thread::spawn(move || {
                    let _ = shutdown_rx.recv();
                    let _ = tx.send(());
                });
                let _ = rx.await;
                info!("API server received shutdown signal");
                shutdown_handle.stop(true).await;
            });
            
            if let Err(e) = server.await {
                error!("API server error: {}", e);
            }
        });
        
        info!("API server thread exiting");
    });
    
    ApiServerHandle {
        shutdown_tx,
        thread_handle: Some(thread_handle),
    }
}

impl ApiServerHandle {
    pub fn shutdown(mut self) {
        info!("Shutting down API server");
        let _ = self.shutdown_tx.send(());
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
        info!("API server shutdown complete");
    }
}

async fn health_check() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok().json(&ApiResponse {
        success: true,
        message: "Twine Geyser Plugin API is running".to_string(),
    }))
}

async fn get_monitored_accounts(data: web::Data<ApiState>) -> Result<HttpResponse> {
    let metadata = data.account_metadata.read().await;
    
    let accounts: Vec<MonitoredAccount> = data
        .monitored_accounts
        .iter()
        .map(|entry| {
            let pubkey = *entry.key();
            let added_at = metadata
                .get(&pubkey)
                .cloned()
                .unwrap_or_else(chrono::Utc::now);
            
            MonitoredAccount {
                pubkey: pubkey.to_string(),
                added_at,
            }
        })
        .collect();

    let response = MonitoredAccountsResponse {
        count: accounts.len(),
        accounts,
    };

    Ok(HttpResponse::Ok().json(&response))
}

async fn add_monitored_account(
    data: web::Data<ApiState>,
    req: web::Json<AddAccountRequest>,
) -> Result<HttpResponse> {
    match Pubkey::from_str(&req.pubkey) {
        Ok(pubkey) => {
            let is_new = data.monitored_accounts.insert(pubkey);
            
            if is_new {
                let mut metadata = data.account_metadata.write().await;
                metadata.insert(pubkey, chrono::Utc::now());
                
                // Persist to database
                match persist_account_to_db(&data.db_config, &req.pubkey).await {
                    Ok(_) => {
                        info!("Added monitored account to DB: {}", req.pubkey);
                    }
                    Err(e) => {
                        error!("Failed to persist account to DB: {} - {}", req.pubkey, e);
                        // Continue anyway - account is in memory
                    }
                }
                
                info!("Added monitored account: {}", req.pubkey);
                Ok(HttpResponse::Ok().json(&ApiResponse {
                    success: true,
                    message: format!("Account {} added to monitoring", req.pubkey),
                }))
            } else {
                Ok(HttpResponse::Ok().json(&ApiResponse {
                    success: false,
                    message: format!("Account {} is already being monitored", req.pubkey),
                }))
            }
        }
        Err(e) => {
            warn!("Invalid pubkey in add request: {} - {}", req.pubkey, e);
            Ok(HttpResponse::BadRequest().json(&ApiResponse {
                success: false,
                message: format!("Invalid pubkey: {}", e),
            }))
        }
    }
}

async fn persist_account_to_db(db_config: &str, pubkey: &str) -> Result<(), Box<dyn std::error::Error>> {
    use tokio_postgres::{NoTls, connect};
    
    let (client, connection) = connect(db_config, NoTls).await?;
    
    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Database connection error: {}", e);
        }
    });
    
    client.execute(
        "INSERT INTO monitored_accounts (account_pubkey) VALUES ($1) ON CONFLICT (account_pubkey) DO NOTHING",
        &[&pubkey],
    ).await?;
    
    Ok(())
}

async fn remove_monitored_account(
    data: web::Data<ApiState>,
    req: web::Json<RemoveAccountRequest>,
) -> Result<HttpResponse> {
    match Pubkey::from_str(&req.pubkey) {
        Ok(pubkey) => {
            let removed = data.monitored_accounts.remove(&pubkey).is_some();
            
            if removed {
                let mut metadata = data.account_metadata.write().await;
                metadata.remove(&pubkey);
                
                // Update database - set active to false
                match deactivate_account_in_db(&data.db_config, &req.pubkey).await {
                    Ok(_) => {
                        info!("Deactivated monitored account in DB: {}", req.pubkey);
                    }
                    Err(e) => {
                        error!("Failed to deactivate account in DB: {} - {}", req.pubkey, e);
                    }
                }
                
                info!("Removed monitored account: {}", req.pubkey);
                Ok(HttpResponse::Ok().json(&ApiResponse {
                    success: true,
                    message: format!("Account {} removed from monitoring", req.pubkey),
                }))
            } else {
                Ok(HttpResponse::Ok().json(&ApiResponse {
                    success: false,
                    message: format!("Account {} was not being monitored", req.pubkey),
                }))
            }
        }
        Err(e) => {
            warn!("Invalid pubkey in remove request: {} - {}", req.pubkey, e);
            Ok(HttpResponse::BadRequest().json(&ApiResponse {
                success: false,
                message: format!("Invalid pubkey: {}", e),
            }))
        }
    }
}

async fn deactivate_account_in_db(db_config: &str, pubkey: &str) -> Result<(), Box<dyn std::error::Error>> {
    use tokio_postgres::{NoTls, connect};
    
    let (client, connection) = connect(db_config, NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Database connection error: {}", e);
        }
    });
    
    client.execute(
        "UPDATE monitored_accounts SET active = false WHERE account_pubkey = $1",
        &[&pubkey],
    ).await?;
    
    Ok(())
}