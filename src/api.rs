use std::sync::Arc;
use std::net::SocketAddr;

use axum::{
    routing::{get, post, delete},
    Router,
    extract::{State, Path},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

#[derive(Clone)]
struct ApiState {
    monitored_accounts: Arc<parking_lot::RwLock<Vec<Pubkey>>>,
    update_context: Arc<parking_lot::RwLock<Option<Box<dyn agave_geyser_plugin_interface::geyser_plugin_interface::MonitoredAccountsContext + Send + Sync>>>>,
}

#[derive(Serialize)]
struct AccountListResponse {
    accounts: Vec<String>,
}

#[derive(Deserialize)]
struct AccountUpdateRequest {
    accounts: Vec<String>,
}

#[derive(Serialize)]
struct ApiResponse {
    success: bool,
    message: String,
}

/// Spawn the API server for dynamic account management
pub async fn spawn_api_server(
    monitored_accounts: Arc<parking_lot::RwLock<Vec<Pubkey>>>,
    update_context: Arc<parking_lot::RwLock<Option<Box<dyn agave_geyser_plugin_interface::geyser_plugin_interface::MonitoredAccountsContext + Send + Sync>>>>,
    bind_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = ApiState {
        monitored_accounts,
        update_context,
    };
    
    let app = Router::new()
        .route("/accounts", get(list_accounts))
        .route("/accounts", post(update_accounts))
        .route("/accounts/:pubkey", post(add_account))
        .route("/accounts/:pubkey", delete(remove_account))
        .route("/health", get(health_handler))
        .with_state(state);
    
    log::info!("Starting API server on {}", bind_addr);
    
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
    
    Ok(())
}

async fn list_accounts(
    State(state): State<ApiState>,
) -> Json<AccountListResponse> {
    let accounts = state.monitored_accounts.read();
    Json(AccountListResponse {
        accounts: accounts.iter().map(|p| p.to_string()).collect(),
    })
}

async fn update_accounts(
    State(state): State<ApiState>,
    Json(req): Json<AccountUpdateRequest>,
) -> Result<Json<ApiResponse>, StatusCode> {
    // Parse new accounts
    let mut new_accounts = Vec::new();
    let account_count = req.accounts.len();
    
    for account_str in req.accounts {
        match account_str.parse::<Pubkey>() {
            Ok(pubkey) => new_accounts.push(pubkey),
            Err(e) => {
                return Ok(Json(ApiResponse {
                    success: false,
                    message: format!("Invalid pubkey '{}': {}", account_str, e),
                }));
            }
        }
    }
    
    // Update the list
    *state.monitored_accounts.write() = new_accounts.clone();
    
    // Update geyser if available
    if let Some(context) = state.update_context.read().as_ref() {
        if let Err(e) = context.set_monitored_accounts(new_accounts) {
            return Ok(Json(ApiResponse {
                success: false,
                message: format!("Failed to update geyser: {:?}", e),
            }));
        }
    }
    
    Ok(Json(ApiResponse {
        success: true,
        message: format!("Updated monitored accounts list with {} accounts", account_count),
    }))
}

async fn add_account(
    State(state): State<ApiState>,
    Path(pubkey_str): Path<String>,
) -> Result<Json<ApiResponse>, StatusCode> {
    // Parse pubkey
    let pubkey = match pubkey_str.parse::<Pubkey>() {
        Ok(p) => p,
        Err(e) => {
            return Ok(Json(ApiResponse {
                success: false,
                message: format!("Invalid pubkey: {}", e),
            }));
        }
    };
    
    // Add to list if not already present
    let mut accounts = state.monitored_accounts.write();
    if !accounts.contains(&pubkey) {
        accounts.push(pubkey);
        
        // Update geyser if available
        if let Some(context) = state.update_context.read().as_ref() {
            if let Err(e) = context.set_monitored_accounts(accounts.clone()) {
                // Rollback
                accounts.pop();
                return Ok(Json(ApiResponse {
                    success: false,
                    message: format!("Failed to update geyser: {:?}", e),
                }));
            }
        }
        
        Ok(Json(ApiResponse {
            success: true,
            message: format!("Added account {}", pubkey_str),
        }))
    } else {
        Ok(Json(ApiResponse {
            success: false,
            message: "Account already monitored".to_string(),
        }))
    }
}

async fn remove_account(
    State(state): State<ApiState>,
    Path(pubkey_str): Path<String>,
) -> Result<Json<ApiResponse>, StatusCode> {
    // Parse pubkey
    let pubkey = match pubkey_str.parse::<Pubkey>() {
        Ok(p) => p,
        Err(e) => {
            return Ok(Json(ApiResponse {
                success: false,
                message: format!("Invalid pubkey: {}", e),
            }));
        }
    };
    
    // Remove from list
    let mut accounts = state.monitored_accounts.write();
    if let Some(pos) = accounts.iter().position(|p| p == &pubkey) {
        accounts.remove(pos);
        
        // Update geyser if available
        if let Some(context) = state.update_context.read().as_ref() {
            if let Err(e) = context.set_monitored_accounts(accounts.clone()) {
                // Rollback
                accounts.insert(pos, pubkey);
                return Ok(Json(ApiResponse {
                    success: false,
                    message: format!("Failed to update geyser: {:?}", e),
                }));
            }
        }
        
        Ok(Json(ApiResponse {
            success: true,
            message: format!("Removed account {}", pubkey_str),
        }))
    } else {
        Ok(Json(ApiResponse {
            success: false,
            message: "Account not found".to_string(),
        }))
    }
}

async fn health_handler() -> &'static str {
    "OK"
}