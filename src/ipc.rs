use crate::config::Config;
use crate::AppResult;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static JSON_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

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
    #[serde(default)]
    pub up_bid: f64,
    pub up_ask: f64,
    #[serde(default)]
    pub down_bid: f64,
    pub down_ask: f64,
    pub btc_price: f64,
    pub price_to_beat: f64,
    /// Seconds remaining until this window settles. Drives time-aware pricing.
    #[serde(default)]
    pub tau_seconds: f64,
    /// Adaptive BTC dollar volatility per sqrt-second (sigma_$), from collector.
    #[serde(default)]
    pub vol_per_sqrt_sec: f64,
    /// BTC price change over the last 1 second, in dollars.
    #[serde(default)]
    pub mom_1s: f64,
    /// BTC price change over the last 3 seconds, in dollars.
    #[serde(default)]
    pub mom_3s: f64,
    /// BTC price change over the last 10 seconds, in dollars.
    #[serde(default)]
    pub mom_10s: f64,
    /// Change in 1-second momentum versus the previous BTC tick.
    #[serde(default)]
    pub accel: f64,
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
    #[serde(default)]
    pub model_up: f64,
    #[serde(default)]
    pub market_up: f64,
    #[serde(default)]
    pub final_up_shadow: f64,
    #[serde(default)]
    pub momentum_up_shadow: f64,
    #[serde(default)]
    pub momentum_delta: f64,
    #[serde(default)]
    pub momentum_score: f64,
    #[serde(default)]
    pub mom_1s: f64,
    #[serde(default)]
    pub mom_3s: f64,
    #[serde(default)]
    pub mom_10s: f64,
    #[serde(default)]
    pub accel: f64,
    #[serde(default)]
    pub market_anchor_weight: f64,
    #[serde(default)]
    pub fair_source: String,
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
        if self.market != market {
            *self = Self {
                market: market.to_string(),
                ..Self::default()
            };
        }
        self.ts_ms = now_ms();
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

#[cfg(test)]
mod tests {
    use super::{now_ms, read_json, write_json, Inventory};
    use serde_json::{json, Value};
    use std::thread;

    #[test]
    fn add_fill_clears_old_market_inventory() {
        let mut inv = Inventory {
            market: "old-market".to_string(),
            up_shares: 40.0,
            up_cost: 28.8,
            ..Inventory::default()
        };

        inv.add_fill("new-market", "Down", 0.25, 10.0);

        assert_eq!(inv.market, "new-market");
        assert_eq!(inv.up_shares, 0.0);
        assert_eq!(inv.up_cost, 0.0);
        assert_eq!(inv.down_shares, 10.0);
        assert_eq!(inv.down_cost, 2.5);
    }

    #[test]
    fn concurrent_write_json_uses_unique_temp_files() {
        let path = std::env::temp_dir().join(format!(
            "polymaker-write-json-{}-{}.json",
            std::process::id(),
            now_ms()
        ));
        let mut handles = Vec::new();
        for worker in 0..24 {
            let path = path.clone();
            handles.push(thread::spawn(move || {
                for seq in 0..25 {
                    write_json(&path, &json!({ "worker": worker, "seq": seq })).unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let value = read_json::<Value>(&path).unwrap().expect("final json");
        assert!(value.get("worker").is_some());
        assert!(value.get("seq").is_some());
        let _ = std::fs::remove_file(path);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub ts_ms: u64,
    pub role: String,
    pub pid: u32,
    pub status: String,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> AppResult<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("json");
    let mut last_collision = None;
    for _ in 0..16 {
        let nonce = JSON_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce));
        let mut file = match OpenOptions::new().write(true).create_new(true).open(&tmp) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                last_collision = Some(err);
                continue;
            }
            Err(err) => return Err(err.into()),
        };
        let result = (|| -> AppResult<()> {
            serde_json::to_writer_pretty(&mut file, value)?;
            file.write_all(b"\n")?;
            drop(file);
            std::fs::rename(&tmp, path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        return result;
    }
    Err(format!(
        "failed to create unique temp file for {} after collisions: {:?}",
        path.display(),
        last_collision
    )
    .into())
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
