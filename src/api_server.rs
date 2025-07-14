use crate::airlock::types::ApiServerConfig;
use actix_web::{web, App, HttpResponse, HttpServer, Result};
use dashmap::DashSet;
use log::*;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use crossbeam_channel::Sender;

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
    pub update_notifier: Sender<()>,
}

/// API server handle for async task management
pub type ApiServerHandle = tokio::task::JoinHandle<()>;

pub struct ApiServer {
    config: ApiServerConfig,
    monitored_accounts: Arc<DashSet<Pubkey>>,
    update_notifier: Sender<()>,
}

impl ApiServer {
    pub fn new(
        config: ApiServerConfig,
        monitored_accounts: Arc<DashSet<Pubkey>>,
        update_notifier: Sender<()>,
    ) -> Self {
        Self {
            config,
            monitored_accounts,
            update_notifier,
        }
    }

    pub async fn run(self) -> std::io::Result<()> {
        let state = web::Data::new(ApiState {
            monitored_accounts: self.monitored_accounts.clone(),
            update_notifier: self.update_notifier,
        });

        info!("Starting API server on {}:{}", self.config.bind_address, self.config.port);

        HttpServer::new(move || {
            App::new()
                .app_data(state.clone())
                .service(
                    web::scope("/api")
                        .route("/monitored_accounts", web::get().to(get_monitored_accounts))
                        .route("/monitored_accounts", web::post().to(add_monitored_account))
                        .route("/monitored_accounts", web::delete().to(remove_monitored_account))
                )
                .route("/health", web::get().to(health_check))
        })
        .bind(format!("{}:{}", self.config.bind_address, self.config.port))?
        .run()
        .await
    }
}

async fn health_check() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok().json(ApiResponse {
        success: true,
        message: "Healthy".to_string(),
    }))
}

async fn get_monitored_accounts(data: web::Data<ApiState>) -> Result<HttpResponse> {
    let accounts: Vec<MonitoredAccount> = data.monitored_accounts
        .iter()
        .map(|pubkey| MonitoredAccount {
            pubkey: pubkey.to_string(),
            added_at: chrono::Utc::now(), // Note: actual timestamp not tracked
        })
        .collect();

    let response = MonitoredAccountsResponse {
        count: accounts.len(),
        accounts,
    };

    Ok(HttpResponse::Ok().json(response))
}

async fn add_monitored_account(
    req: web::Json<AddAccountRequest>,
    data: web::Data<ApiState>,
) -> Result<HttpResponse> {
    let pubkey = match Pubkey::from_str(&req.pubkey) {
        Ok(pk) => pk,
        Err(_) => {
            return Ok(HttpResponse::BadRequest().json(ApiResponse {
                success: false,
                message: "Invalid pubkey".to_string(),
            }));
        }
    };

    if data.monitored_accounts.contains(&pubkey) {
        return Ok(HttpResponse::Ok().json(ApiResponse {
            success: false,
            message: "Account already monitored".to_string(),
        }));
    }

    data.monitored_accounts.insert(pubkey);
    
    // Notify plugin of update
    let _ = data.update_notifier.send(());

    // Persist to database
    if let Err(e) = persist_monitored_account(&pubkey, true) {
        error!("Failed to persist monitored account: {}", e);
    }

    Ok(HttpResponse::Ok().json(ApiResponse {
        success: true,
        message: format!("Account {} added to monitoring", pubkey),
    }))
}

async fn remove_monitored_account(
    req: web::Json<RemoveAccountRequest>,
    data: web::Data<ApiState>,
) -> Result<HttpResponse> {
    let pubkey = match Pubkey::from_str(&req.pubkey) {
        Ok(pk) => pk,
        Err(_) => {
            return Ok(HttpResponse::BadRequest().json(ApiResponse {
                success: false,
                message: "Invalid pubkey".to_string(),
            }));
        }
    };

    if !data.monitored_accounts.contains(&pubkey) {
        return Ok(HttpResponse::Ok().json(ApiResponse {
            success: false,
            message: "Account not monitored".to_string(),
        }));
    }

    data.monitored_accounts.remove(&pubkey);
    
    // Notify plugin of update
    let _ = data.update_notifier.send(());

    // Update database
    if let Err(e) = persist_monitored_account(&pubkey, false) {
        error!("Failed to update monitored account: {}", e);
    }

    Ok(HttpResponse::Ok().json(ApiResponse {
        success: true,
        message: format!("Account {} removed from monitoring", pubkey),
    }))
}

fn persist_monitored_account(_pubkey: &Pubkey, _active: bool) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Implement database persistence
    // This would use the connection_string from config
    Ok(())
}