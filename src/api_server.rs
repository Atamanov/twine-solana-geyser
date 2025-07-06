use actix_web::{web, App, HttpResponse, HttpServer, Result};
use dashmap::DashSet;
use log::*;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

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
}

pub async fn start_api_server(
    monitored_accounts: Arc<DashSet<Pubkey>>,
    api_port: u16,
) -> std::io::Result<()> {
    let account_metadata = Arc::new(RwLock::new(std::collections::HashMap::new()));
    
    let state = web::Data::new(ApiState {
        monitored_accounts,
        account_metadata,
    });

    info!("Starting API server on port {}", api_port);

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/api/monitored-accounts", web::get().to(get_monitored_accounts))
            .route("/api/monitored-accounts", web::post().to(add_monitored_account))
            .route("/api/monitored-accounts", web::delete().to(remove_monitored_account))
            .route("/api/health", web::get().to(health_check))
    })
    .bind(("0.0.0.0", api_port))?
    .run()
    .await
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