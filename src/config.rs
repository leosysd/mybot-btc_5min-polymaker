use crate::AppResult;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub base_dir: PathBuf,
    pub run_dir: PathBuf,
    pub market_slug: String,
    pub data_mode: String,
    pub polymarket_ws_url: String,
    pub polymarket_up_token_id: String,
    pub polymarket_down_token_id: String,
    pub binance_ws_url: String,
    pub price_to_beat: f64,
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
    pub ws_stale_after_ms: u64,
    pub btc_sigma_usd: f64,
    pub quote_ttl_ms: u64,
    pub requote_threshold_ticks: f64,
    pub max_loss: f64,
    pub max_total_inventory: f64,
    pub live_order_notional_cap: f64,
}

impl Config {
    pub fn from_env() -> AppResult<Self> {
        let base_dir = detect_base_dir()?;
        let file_env = load_dotenv_like(&base_dir.join(".env"))?;
        let run_dir_raw = PathBuf::from(get(&file_env, "BOT_RUN_DIR", "run"));
        let run_dir = if run_dir_raw.is_absolute() {
            run_dir_raw
        } else {
            base_dir.join(run_dir_raw)
        };
        let cfg = Self {
            base_dir,
            run_dir,
            market_slug: get(&file_env, "MARKET_SLUG", "btc-updown-5m"),
            data_mode: get(&file_env, "DATA_MODE", "sim"),
            polymarket_ws_url: get(
                &file_env,
                "POLYMARKET_WS_URL",
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            ),
            polymarket_up_token_id: get(&file_env, "POLYMARKET_UP_TOKEN_ID", ""),
            polymarket_down_token_id: get(&file_env, "POLYMARKET_DOWN_TOKEN_ID", ""),
            binance_ws_url: get(
                &file_env,
                "BINANCE_WS_URL",
                "wss://stream.binance.com:9443/ws/btcusdt@trade",
            ),
            price_to_beat: get_f64(&file_env, "PRICE_TO_BEAT", 68_000.0),
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
            ws_stale_after_ms: get_u64(&file_env, "WS_STALE_AFTER_MS", 10_000),
            btc_sigma_usd: get_f64(&file_env, "BTC_SIGMA_USD", 35.0),
            quote_ttl_ms: get_u64(&file_env, "QUOTE_TTL_MS", 1500),
            requote_threshold_ticks: get_f64(&file_env, "REQUOTE_THRESHOLD_TICKS", 1.0),
            max_loss: get_f64(&file_env, "MAX_LOSS", 25.0),
            max_total_inventory: get_f64(&file_env, "MAX_TOTAL_INVENTORY", 50.0),
            live_order_notional_cap: get_f64(&file_env, "LIVE_ORDER_NOTIONAL_CAP", 5.0),
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
        if !matches!(self.data_mode.as_str(), "sim" | "live") {
            return Err("DATA_MODE must be sim or live".into());
        }
        if self.price_to_beat <= 0.0 || !self.price_to_beat.is_finite() {
            return Err("PRICE_TO_BEAT must be positive".into());
        }
        if self.ws_stale_after_ms < 1_000 {
            return Err("WS_STALE_AFTER_MS must be at least 1000".into());
        }
        if self.requote_threshold_ticks < 0.0 || !self.requote_threshold_ticks.is_finite() {
            return Err("REQUOTE_THRESHOLD_TICKS must be finite and non-negative".into());
        }
        if self.max_loss < 0.0 || !self.max_loss.is_finite() {
            return Err("MAX_LOSS must be finite and non-negative".into());
        }
        if self.max_total_inventory < 0.0 || !self.max_total_inventory.is_finite() {
            return Err("MAX_TOTAL_INVENTORY must be finite and non-negative".into());
        }
        Ok(())
    }

    pub fn ensure_live_market_config(&self) -> AppResult<()> {
        if self.data_mode != "live" {
            return Ok(());
        }
        if self.polymarket_up_token_id.trim().is_empty()
            || self.polymarket_down_token_id.trim().is_empty()
        {
            return Err(
                "DATA_MODE=live 需要配置 POLYMARKET_UP_TOKEN_ID 和 POLYMARKET_DOWN_TOKEN_ID".into(),
            );
        }
        Ok(())
    }

    pub fn ensure_trading_supported(&self) -> AppResult<()> {
        if !self.dry_run {
            return Err("真实下单接口尚未接入；当前必须保持 DRY_RUN=1".into());
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

    pub fn pid_file(&self) -> PathBuf {
        self.run_dir.join("supervisor.pid")
    }

    pub fn env_file(&self) -> PathBuf {
        self.base_dir.join(".env")
    }

    pub fn env_example_file(&self) -> PathBuf {
        self.base_dir.join(".env.example")
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

fn detect_base_dir() -> AppResult<PathBuf> {
    if let Ok(home) = std::env::var("POLYMAKER_HOME") {
        if !home.trim().is_empty() {
            return Ok(PathBuf::from(home));
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            if parent.join(".env").exists() || parent.join(".env.example").exists() {
                return Ok(parent.to_path_buf());
            }
        }
    }

    let cwd = std::env::current_dir()?;
    if cwd.join(".env").exists() || cwd.join("Cargo.toml").exists() {
        return Ok(cwd);
    }

    Ok(cwd)
}

fn load_dotenv_like(path: &std::path::Path) -> AppResult<HashMap<String, String>> {
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
