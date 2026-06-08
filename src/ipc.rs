use crate::config::Config;
use crate::AppResult;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFrame {
    pub ts_ms: u64,
    pub market: String,
    #[serde(default)]
    pub condition_id: String,
    #[serde(default)]
    pub up_token_id: String,
    #[serde(default)]
    pub down_token_id: String,
    pub up_ask: f64,
    pub down_ask: f64,
    pub btc_price: f64,
    pub price_to_beat: f64,
    /// Seconds remaining until this window settles. Drives time-aware pricing.
    #[serde(default)]
    pub tau_seconds: f64,
    /// Adaptive BTC dollar volatility per sqrt-second (sigma_$), from collector.
    #[serde(default)]
    pub vol_per_sqrt_sec: f64,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteIntent {
    #[serde(default)]
    pub quote_id: String,
    pub ts_ms: u64,
    pub market: String,
    #[serde(default)]
    pub condition_id: String,
    #[serde(default)]
    pub token_id: String,
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub fair: f64,
    pub inventory_up: f64,
    pub inventory_down: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderAccepted {
    pub accepted_ts_ms: u64,
    pub expires_ts_ms: u64,
    pub quote: QuoteIntent,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCancelled {
    pub ts_ms: u64,
    pub quote_id: String,
    pub market: String,
    pub side: String,
    pub size: f64,
    pub reason: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillEvent {
    #[serde(default)]
    pub quote_id: String,
    pub ts_ms: u64,
    pub market: String,
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub inventory_up: f64,
    pub inventory_down: f64,
    pub pnl_if_up: f64,
    pub pnl_if_down: f64,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Inventory {
    pub ts_ms: u64,
    pub market: String,
    pub up_shares: f64,
    pub down_shares: f64,
    pub up_cost: f64,
    pub down_cost: f64,
    #[serde(default)]
    pub pending_up: f64,
    #[serde(default)]
    pub pending_down: f64,
}

impl Default for Inventory {
    fn default() -> Self {
        Self {
            ts_ms: now_ms(),
            market: String::new(),
            up_shares: 0.0,
            down_shares: 0.0,
            up_cost: 0.0,
            down_cost: 0.0,
            pending_up: 0.0,
            pending_down: 0.0,
        }
    }
}

impl Inventory {
    pub fn add_fill(&mut self, market: &str, side: &str, price: f64, size: f64) {
        self.ts_ms = now_ms();
        self.market = market.to_string();
        match side {
            "Up" => {
                self.up_shares += size;
                self.up_cost += price * size;
            }
            "Down" => {
                self.down_shares += size;
                self.down_cost += price * size;
            }
            _ => {}
        }
    }

    pub fn pnl_if_up(&self) -> f64 {
        (self.up_shares - self.up_cost) - self.down_cost
    }

    pub fn pnl_if_down(&self) -> f64 {
        (self.down_shares - self.down_cost) - self.up_cost
    }

    pub fn effective_up(&self) -> f64 {
        self.up_shares + self.pending_up
    }

    pub fn effective_down(&self) -> f64 {
        self.down_shares + self.pending_down
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub ts_ms: u64,
    pub role: String,
    pub pid: u32,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum WireMessage {
    MarketFrame(MarketFrame),
    QuoteIntent(QuoteIntent),
    OrderAccepted(OrderAccepted),
    OrderCancelled(OrderCancelled),
    FillEvent(FillEvent),
    Inventory(Inventory),
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> AppResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
    }
    std::fs::rename(tmp, path)?;
    Ok(())
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> AppResult<Option<T>> {
    let Ok(file) = File::open(path) else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_reader(file)?))
}

pub fn heartbeat(cfg: &Config, role: &str, status: impl Into<String>) -> AppResult<()> {
    let hb = Heartbeat {
        ts_ms: now_ms(),
        role: role.to_string(),
        pid: std::process::id(),
        status: status.into(),
    };
    write_json(&cfg.heartbeat_dir().join(format!("{role}.json")), &hb)
}

pub fn should_stop(cfg: &Config) -> bool {
    cfg.stop_file().exists()
}
