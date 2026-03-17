use std::env;
use std::fs;
use std::sync::Arc;

use tokenresearch::runtime::{
    CollectorRuntime, ReqwestRestClient, RuntimeConfig, TokioClock, TokioWsClient,
};
use tokenresearch::storage::SqliteBookStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = load_config()?;
    let store = Arc::new(SqliteBookStore::connect(&config.database_path).await?);
    let clock = Arc::new(TokioClock);
    let runtime = CollectorRuntime::new(store.clone(), clock, config.clone());
    let rest = ReqwestRestClient::new();
    let ws = TokioWsClient;

    runtime.run_live(rest, ws).await
}

fn load_config() -> Result<RuntimeConfig, Box<dyn std::error::Error + Send + Sync>> {
    let path = env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    if let Ok(raw) = fs::read_to_string(&path) {
        Ok(toml::from_str(&raw)?)
    } else {
        Ok(RuntimeConfig::default())
    }
}
