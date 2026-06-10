use crate::AppResult;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub base_dir: PathBuf,
    pub run_dir: PathBuf,
    pub market_slug: String,
    pub data_mode: String,
    pub auto_discover_market: bool,
    pub gamma_api_url: String,
    pub market_window_secs: u64,
    pub market_discovery_ms: u64,
    pub market_switch_grace_ms: u64,
    pub polymarket_ws_url: String,
    pub polymarket_user_ws_url: String,
    pub polymarket_up_token_id: String,
    pub polymarket_down_token_id: String,
    pub binance_ws_url: String,
    pub binance_rest_url: String,
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
    // ── strategy brain v3 ──────────────────────────────────────────────
    pub vol_seed_per_sqrt_sec: f64,
    pub vol_halflife_sec: f64,
    pub vol_min_per_sqrt_sec: f64,
    pub vol_max_per_sqrt_sec: f64,
    pub width_floor_usd: f64,
    pub base_half_spread: f64,
    pub min_lock_edge: f64,
    pub latency_sec: f64,
    pub k_adverse: f64,
    pub min_half_spread: f64,
    pub max_half_spread: f64,
    pub endgame_pull_secs: f64,
    pub inventory_skew_time_boost: f64,
    pub tox_horizon_ms: u64,
    pub tox_decay: f64,
    pub tox_k_widen: f64,
    pub tox_max_widen: f64,
    pub enable_delta_hedge: bool,
    pub hedge_venue: String,
    pub reconcile_interval_ms: u64,
    pub enable_prewarm: bool,
    pub prewarm_interval_ms: u64,
    pub post_only_margin_ticks: f64,
    pub reject_backoff_ms: u64,
    pub quote_warmup_secs: f64,
    pub value_min_edge: f64,
    pub value_aggression_ticks: f64,
    pub value_min_fair: f64,
    pub enable_real_orders: String,
    pub polymarket_clob_host: String,
    pub poly_private_key: String,
    pub poly_api_key: String,
    pub poly_secret: String,
    pub poly_passphrase: String,
    pub poly_signature_type: String,
    pub poly_funder_address: String,
    pub poly_chain_id: u64,
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
            auto_discover_market: get_bool(&file_env, "AUTO_DISCOVER_MARKET", true),
            gamma_api_url: get(
                &file_env,
                "POLYMARKET_GAMMA_API_URL",
                "https://gamma-api.polymarket.com",
            ),
            market_window_secs: get_u64(&file_env, "MARKET_WINDOW_SECS", 300),
            market_discovery_ms: get_u64(&file_env, "MARKET_DISCOVERY_MS", 15_000),
            market_switch_grace_ms: get_u64(&file_env, "MARKET_SWITCH_GRACE_MS", 90_000),
            polymarket_ws_url: get(
                &file_env,
                "POLYMARKET_WS_URL",
                "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            ),
            polymarket_user_ws_url: get(
                &file_env,
                "POLYMARKET_USER_WS_URL",
                "wss://ws-subscriptions-clob.polymarket.com/ws/user",
            ),
            polymarket_up_token_id: get(&file_env, "POLYMARKET_UP_TOKEN_ID", ""),
            polymarket_down_token_id: get(&file_env, "POLYMARKET_DOWN_TOKEN_ID", ""),
            binance_ws_url: get(
                &file_env,
                "BINANCE_WS_URL",
                "wss://stream.binance.com:9443/ws/btcusdt@trade",
            ),
            binance_rest_url: get(&file_env, "BINANCE_REST_URL", "https://api.binance.com"),
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
            vol_seed_per_sqrt_sec: get_f64(&file_env, "VOL_SEED_PER_SQRT_SEC", 6.0),
            vol_halflife_sec: get_f64(&file_env, "VOL_HALFLIFE_SEC", 20.0),
            vol_min_per_sqrt_sec: get_f64(&file_env, "VOL_MIN_PER_SQRT_SEC", 1.5),
            vol_max_per_sqrt_sec: get_f64(&file_env, "VOL_MAX_PER_SQRT_SEC", 60.0),
            width_floor_usd: get_f64(&file_env, "WIDTH_FLOOR_USD", 3.0),
            base_half_spread: get_f64(&file_env, "BASE_HALF_SPREAD", 0.012),
            min_lock_edge: get_f64(&file_env, "MIN_LOCK_EDGE", 0.02),
            latency_sec: get_f64(&file_env, "LATENCY_SEC", 0.4),
            k_adverse: get_f64(&file_env, "K_ADVERSE", 1.0),
            min_half_spread: get_f64(&file_env, "MIN_HALF_SPREAD", 0.005),
            max_half_spread: get_f64(&file_env, "MAX_HALF_SPREAD", 0.25),
            endgame_pull_secs: get_f64(&file_env, "ENDGAME_PULL_SECS", 12.0),
            inventory_skew_time_boost: get_f64(&file_env, "INVENTORY_SKEW_TIME_BOOST", 2.0),
            tox_horizon_ms: get_u64(&file_env, "TOX_HORIZON_MS", 2500),
            tox_decay: get_f64(&file_env, "TOX_DECAY", 0.5),
            tox_k_widen: get_f64(&file_env, "TOX_K_WIDEN", 1.5),
            tox_max_widen: get_f64(&file_env, "TOX_MAX_WIDEN", 0.08),
            enable_delta_hedge: get_bool(&file_env, "ENABLE_DELTA_HEDGE", false),
            hedge_venue: get(&file_env, "HEDGE_VENUE", "binance_perp"),
            reconcile_interval_ms: get_u64(&file_env, "RECONCILE_INTERVAL_MS", 30_000),
            enable_prewarm: get_bool(&file_env, "ENABLE_PREWARM", true),
            prewarm_interval_ms: get_u64(&file_env, "PREWARM_INTERVAL_MS", 60_000),
            post_only_margin_ticks: get_f64(&file_env, "POST_ONLY_MARGIN_TICKS", 2.0),
            reject_backoff_ms: get_u64(&file_env, "REJECT_BACKOFF_MS", 500),
            quote_warmup_secs: get_f64(&file_env, "QUOTE_WARMUP_SECS", 25.0),
            value_min_edge: get_f64(&file_env, "VALUE_MIN_EDGE", 0.03),
            value_aggression_ticks: get_f64(&file_env, "VALUE_AGGRESSION_TICKS", 1.0),
            value_min_fair: get_f64(&file_env, "VALUE_MIN_FAIR", 0.05),
            enable_real_orders: get(&file_env, "ENABLE_REAL_ORDERS", ""),
            polymarket_clob_host: get(
                &file_env,
                "POLYMARKET_CLOB_HOST",
                "https://clob.polymarket.com",
            ),
            poly_private_key: get(&file_env, "POLY_PRIVATE_KEY", ""),
            poly_api_key: get(&file_env, "POLY_API_KEY", ""),
            poly_secret: get(&file_env, "POLY_SECRET", ""),
            poly_passphrase: get(&file_env, "POLY_PASSPHRASE", ""),
            poly_signature_type: get(&file_env, "POLY_SIGNATURE_TYPE", "proxy"),
            poly_funder_address: get(&file_env, "POLY_FUNDER_ADDRESS", ""),
            poly_chain_id: get_u64(&file_env, "POLY_CHAIN_ID", 137),
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
        if self.market_window_secs < 60 {
            return Err("MARKET_WINDOW_SECS must be at least 60".into());
        }
        if self.market_discovery_ms < 1_000 {
            return Err("MARKET_DISCOVERY_MS must be at least 1000".into());
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
        if self.live_order_notional_cap < 0.0 || !self.live_order_notional_cap.is_finite() {
            return Err("LIVE_ORDER_NOTIONAL_CAP must be finite and non-negative".into());
        }
        // ── strategy brain v3 sanity ───────────────────────────────────
        if !(self.vol_seed_per_sqrt_sec.is_finite() && self.vol_seed_per_sqrt_sec > 0.0) {
            return Err("VOL_SEED_PER_SQRT_SEC must be positive".into());
        }
        if self.vol_halflife_sec < 1.0 || !self.vol_halflife_sec.is_finite() {
            return Err("VOL_HALFLIFE_SEC must be at least 1".into());
        }
        if self.vol_min_per_sqrt_sec <= 0.0 || self.vol_max_per_sqrt_sec < self.vol_min_per_sqrt_sec
        {
            return Err("VOL_MIN/MAX_PER_SQRT_SEC are invalid".into());
        }
        if self.width_floor_usd <= 0.0 || !self.width_floor_usd.is_finite() {
            return Err("WIDTH_FLOOR_USD must be positive".into());
        }
        if !(0.0..=0.5).contains(&self.base_half_spread) {
            return Err("BASE_HALF_SPREAD must be in [0, 0.5]".into());
        }
        if !(0.0..0.95).contains(&self.min_lock_edge) {
            return Err("MIN_LOCK_EDGE must be in [0, 0.95)".into());
        }
        if self.min_half_spread < 0.0 || self.max_half_spread < self.min_half_spread {
            return Err("MIN/MAX_HALF_SPREAD are invalid".into());
        }
        if self.latency_sec < 0.0 || !self.latency_sec.is_finite() {
            return Err("LATENCY_SEC must be finite and non-negative".into());
        }
        if self.endgame_pull_secs < 0.0 || !self.endgame_pull_secs.is_finite() {
            return Err("ENDGAME_PULL_SECS must be finite and non-negative".into());
        }
        if self.value_min_edge < 0.0
            || self.value_min_edge >= 1.0
            || !self.value_min_edge.is_finite()
        {
            return Err("VALUE_MIN_EDGE must be finite and in [0, 1)".into());
        }
        if self.value_aggression_ticks < 0.0 || !self.value_aggression_ticks.is_finite() {
            return Err("VALUE_AGGRESSION_TICKS must be finite and non-negative".into());
        }
        if !(0.0..=1.0).contains(&self.value_min_fair) || !self.value_min_fair.is_finite() {
            return Err("VALUE_MIN_FAIR must be a finite probability in [0, 1]".into());
        }
        Ok(())
    }

    pub fn ensure_live_market_config(&self) -> AppResult<()> {
        if self.data_mode != "live" {
            return Ok(());
        }
        if self.auto_discover_market {
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
        if self.dry_run {
            return Ok(());
        }
        self.ensure_real_order_config()
    }

    pub fn real_orders_enabled(&self) -> bool {
        !self.dry_run && self.enable_real_orders == "I_UNDERSTAND_REAL_MONEY"
    }

    pub fn ensure_real_order_config(&self) -> AppResult<()> {
        if self.enable_real_orders != "I_UNDERSTAND_REAL_MONEY" {
            return Err(
                "实单需要同时设置 DRY_RUN=0 和 ENABLE_REAL_ORDERS=I_UNDERSTAND_REAL_MONEY".into(),
            );
        }
        if self.data_mode != "live" {
            return Err("实单必须使用 DATA_MODE=live 真实行情".into());
        }
        if !self.auto_discover_market {
            self.ensure_live_market_config()?;
        }
        if self.poly_private_key.trim().is_empty() {
            return Err("实单需要 POLY_PRIVATE_KEY，且只能放在 VPS 的 .env 里".into());
        }
        if self.live_order_notional_cap <= 0.0 || self.live_order_notional_cap > 25.0 {
            return Err("实单 LIVE_ORDER_NOTIONAL_CAP 必须在 (0, 25]，先小额验证".into());
        }
        if self.poly_chain_id != 137 {
            return Err("当前只支持 Polygon 主网 POLY_CHAIN_ID=137".into());
        }
        if self.enable_delta_hedge {
            return Err(
                "ENABLE_DELTA_HEDGE 当前只是占位骨架，未接真实对冲交易所，禁止在实单模式开启。\
                 先用模拟验证策略大脑，等对冲模块真正实现后再启用。"
                    .into(),
            );
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

    pub fn active_orders_path(&self) -> PathBuf {
        self.run_dir.join("active_orders.json")
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
