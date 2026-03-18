use std::env;
use std::fs;
use std::sync::Arc;

use serde::Deserialize;
use tokenresearch::model::Venue;
use tokenresearch::price_adapters::{
    BinancePriceAdapter, HyperliquidPriceAdapter, LighterPriceAdapter, PriceVenueAdapter,
};
use tokenresearch::price_runtime::{PriceRuntimeConfig, run_price_runtime_once};
use tokenresearch::price_storage::SqlitePriceStore;
use tokenresearch::runtime::{ReqwestRestClient, TokioClock, TokioWsClient};
use tokenresearch::traits::Clock;

#[derive(Clone, Debug, Deserialize)]
struct PriceCollectorConfig {
    #[serde(flatten)]
    runtime: PriceRuntimeConfig,
    #[serde(default = "default_venues")]
    venues: Vec<Venue>,
    #[serde(default = "default_restart_delay_ms")]
    restart_delay_ms: u64,
}

fn default_venues() -> Vec<Venue> {
    vec![Venue::Binance, Venue::Hyperliquid, Venue::Lighter]
}

fn default_restart_delay_ms() -> u64 {
    1_000
}

impl Default for PriceCollectorConfig {
    fn default() -> Self {
        Self {
            runtime: PriceRuntimeConfig::default(),
            venues: default_venues(),
            restart_delay_ms: default_restart_delay_ms(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = load_config()?;
    let store = SqlitePriceStore::connect(&config.runtime.database_path).await?;
    let rest = ReqwestRestClient::new();
    let ws = TokioWsClient;
    let clock = TokioClock;
    let adapters = build_adapters(&config.venues);

    loop {
        let run = run_price_runtime_once(
            config.runtime.clone(),
            store.clone(),
            rest.clone(),
            ws.clone(),
            clock.clone(),
            adapters.clone(),
        );

        tokio::select! {
            result = run => {
                if let Err(error) = result {
                    tracing::warn!(error = %error, "pricecollector cycle failed");
                }
                clock
                    .sleep(std::time::Duration::from_millis(config.restart_delay_ms))
                    .await;
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                tracing::info!("pricecollector received ctrl-c, shutting down");
                break;
            }
        }
    }

    Ok(())
}

fn build_adapters(venues: &[Venue]) -> Vec<Arc<dyn PriceVenueAdapter>> {
    venues
        .iter()
        .map(|venue| match venue {
            Venue::Binance => {
                Arc::new(BinancePriceAdapter::default()) as Arc<dyn PriceVenueAdapter>
            }
            Venue::Hyperliquid => {
                Arc::new(HyperliquidPriceAdapter::default()) as Arc<dyn PriceVenueAdapter>
            }
            Venue::Lighter => {
                Arc::new(LighterPriceAdapter::default()) as Arc<dyn PriceVenueAdapter>
            }
        })
        .collect()
}

fn load_config() -> Result<PriceCollectorConfig, Box<dyn std::error::Error + Send + Sync>> {
    let path = env::args()
        .nth(1)
        .unwrap_or_else(|| "price_config.toml".to_string());
    if let Ok(raw) = fs::read_to_string(&path) {
        Ok(toml::from_str(&raw)?)
    } else {
        Ok(PriceCollectorConfig::default())
    }
}
