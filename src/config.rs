use crate::AppResult;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub run_dir: PathBuf,
    pub market_slug: String,
    pub tick_size: f64,
    pub quote_size: f64,
    pub quote_spread: f64,
    pub inventory_skew: f64,
    pub inventory_mult: f64,
    pub min_bid: f64,
    pub max_bid: f64,
    pub dry_run: bool,
    pub sim_fill_chance: f64,
    pub market_interval_ms: u64,
    pub stale_after_ms: u64,
    pub btc_sigma_usd: f64,
}

impl Config {
    pub fn from_env() -> AppResult<Self> {
        let file_env = load_dotenv_like(".env")?;
        let cfg = Self {
            run_dir: PathBuf::from(get(&file_env, "BOT_RUN_DIR", "run")),
            market_slug: get(&file_env, "MARKET_SLUG", "btc-updown-5m"),
            tick_size: get_f64(&file_env, "TICK_SIZE", 0.01),
            quote_size: get_f64(&file_env, "QUOTE_SIZE", 5.0),
            quote_spread: get_f64(&file_env, "QUOTE_SPREAD", 0.04),
            inventory_skew: get_f64(&file_env, "INVENTORY_SKEW", 0.03),
            inventory_mult: get_f64(&file_env, "INVENTORY_MULT", 2.0),
            min_bid: get_f64(&file_env, "MIN_BID", 0.05),
            max_bid: get_f64(&file_env, "MAX_BID", 0.62),
            dry_run: get_bool(&file_env, "DRY_RUN", true),
            sim_fill_chance: get_f64(&file_env, "SIM_FILL_CHANCE", 0.18),
            market_interval_ms: get_u64(&file_env, "MARKET_INTERVAL_MS", 600),
            stale_after_ms: get_u64(&file_env, "STALE_AFTER_MS", 5000),
            btc_sigma_usd: get_f64(&file_env, "BTC_SIGMA_USD", 35.0),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> AppResult<()> {
        if self.quote_size <= 0.0 || !self.quote_size.is_finite() {
            return Err("QUOTE_SIZE must be positive".into());
        }
        if self.inventory_mult < 0.0 || !self.inventory_mult.is_finite() {
            return Err("INVENTORY_MULT must be finite and non-negative".into());
        }
        if self.tick_size <= 0.0 || self.tick_size > 0.1 {
            return Err("TICK_SIZE must be in (0, 0.1]".into());
        }
        if self.min_bid < 0.0 || self.max_bid > 1.0 || self.min_bid > self.max_bid {
            return Err("MIN_BID/MAX_BID are invalid".into());
        }
        if !self.dry_run {
            return Err("live trading is intentionally not implemented yet; keep DRY_RUN=1".into());
        }
        Ok(())
    }

    pub fn ensure_dirs(&self) -> AppResult<()> {
        std::fs::create_dir_all(&self.run_dir)?;
        std::fs::create_dir_all(self.heartbeat_dir())?;
        std::fs::create_dir_all(self.socket_dir())?;
        Ok(())
    }

    pub fn stop_file(&self) -> PathBuf {
        self.run_dir.join("STOP")
    }

    pub fn heartbeat_dir(&self) -> PathBuf {
        self.run_dir.join("heartbeats")
    }

    pub fn socket_dir(&self) -> PathBuf {
        self.run_dir.join("sockets")
    }

    pub fn engine_socket(&self) -> PathBuf {
        self.socket_dir().join("quote-engine.sock")
    }

    pub fn gateway_socket(&self) -> PathBuf {
        self.socket_dir().join("order-gateway.sock")
    }

    pub fn risk_socket(&self) -> PathBuf {
        self.socket_dir().join("risk-ledger.sock")
    }

    pub fn book_path(&self) -> PathBuf {
        self.run_dir.join("book.jsonl")
    }

    pub fn quotes_path(&self) -> PathBuf {
        self.run_dir.join("quotes.jsonl")
    }

    pub fn fills_path(&self) -> PathBuf {
        self.run_dir.join("fills.jsonl")
    }

    pub fn inventory_path(&self) -> PathBuf {
        self.run_dir.join("inventory.json")
    }

    pub fn max_side_inventory(&self) -> f64 {
        (self.quote_size * self.inventory_mult).round().max(0.0)
    }
}

fn load_dotenv_like(path: &str) -> AppResult<HashMap<String, String>> {
    let mut values = HashMap::new();
    let Ok(raw) = std::fs::read_to_string(path) else {
        return Ok(values);
    };
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        let value = value
            .split('#')
            .next()
            .unwrap_or("")
            .trim()
            .trim_matches('"')
            .to_string();
        values.insert(key.to_string(), value);
    }
    Ok(values)
}

fn get(file_env: &HashMap<String, String>, key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .or_else(|| file_env.get(key).cloned())
        .unwrap_or_else(|| default.to_string())
}

fn get_f64(file_env: &HashMap<String, String>, key: &str, default: f64) -> f64 {
    get(file_env, key, &default.to_string())
        .parse()
        .unwrap_or(default)
}

fn get_u64(file_env: &HashMap<String, String>, key: &str, default: u64) -> u64 {
    get(file_env, key, &default.to_string())
        .parse()
        .unwrap_or(default)
}

fn get_bool(file_env: &HashMap<String, String>, key: &str, default: bool) -> bool {
    match get(file_env, key, if default { "1" } else { "0" })
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}
