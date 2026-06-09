//! Market-data collector thread: produces `MarketFrame`s (simulated or live
//! Polymarket/Binance WS + auto discovery) and sends them over the channel to
//! the quote engine. Logs every frame to book.jsonl.

use crate::config::Config;
use crate::ipc::{heartbeat, now_ms, MarketFrame};
use crate::pricing::{digital_p_up, uncertainty_width, VolEstimator};
use crate::AppResult;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tungstenite::{connect, Message};

use super::state::{
    is_ws_timeout, log_jsonl, request_stop, sleep_ms, spawn_jsonl_writer, stopping, tune_ws_socket,
    AsyncJsonlWriter, MarketTx, StopFlag,
};

/// Entry point for the collector thread. Picks sim vs live mode, retrying the
/// live path until stopped (mirrors the old per-process retry loop).
pub fn run(cfg: Config, stop: StopFlag, tx: MarketTx) -> AppResult<()> {
    if cfg.data_mode == "live" {
        cfg.ensure_live_market_config()?;
        while !stopping(&stop, &cfg) {
            match run_live_market_data(&cfg, &stop, &tx) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    let _ = heartbeat(&cfg, "collector", format!("live collector retry: {err}"));
                    sleep_ms(1_000);
                }
            }
        }
        return Ok(());
    }
    run_sim_market_data(&cfg, &stop, &tx)
}

fn run_sim_market_data(cfg: &Config, stop: &StopFlag, tx: &MarketTx) -> AppResult<()> {
    let logger = spawn_jsonl_writer();
    let mut step = 0u64;
    let center = cfg.price_to_beat;
    let window = cfg.market_window_secs.max(1);
    let dt_sec = (cfg.market_interval_ms.max(1) as f64) / 1000.0;
    let mut vol = VolEstimator::new(
        cfg.vol_seed_per_sqrt_sec,
        cfg.vol_halflife_sec,
        cfg.vol_min_per_sqrt_sec,
        cfg.vol_max_per_sqrt_sec,
    );
    // Each window mimics a real 5-min market: price_to_beat is locked at the
    // BTC price when the window opens, then tau counts down to settlement.
    let mut window_start = (now_ms() / 1000) / window * window;
    let mut price_to_beat = center;
    let mut window_initialized = false;

    while !stopping(stop, cfg) {
        let phase = step as f64 / 9.0;
        let btc_price = center + 42.0 * phase.sin() + 15.0 * (phase * 0.37).cos();
        let vol_now = vol.update(btc_price, dt_sec);

        let now_s = now_ms() / 1000;
        let this_window = now_s / window * window;
        if !window_initialized || this_window != window_start {
            window_start = this_window;
            price_to_beat = btc_price; // lock the beat at the open
            window_initialized = true;
        }
        let window_end_ms = (window_start + window) * 1000;
        let tau_seconds = ((window_end_ms.saturating_sub(now_ms())) as f64 / 1000.0).max(0.0);

        // Bracket the time-aware fair value so the simulated book is sensible.
        let w = uncertainty_width(vol_now, tau_seconds, cfg.width_floor_usd);
        let fair_up = digital_p_up(btc_price, price_to_beat, w);
        let micro_noise = 0.012 * (phase * 1.7).sin();
        let frame = MarketFrame {
            ts_ms: now_ms(),
            market: cfg.market_slug.clone(),
            condition_id: String::new(),
            up_token_id: cfg.polymarket_up_token_id.clone(),
            down_token_id: cfg.polymarket_down_token_id.clone(),
            up_ask: (fair_up + 0.035 + micro_noise).clamp(0.03, 0.97),
            down_ask: (1.0 - fair_up + 0.035 - micro_noise).clamp(0.03, 0.97),
            btc_price,
            price_to_beat,
            tau_seconds,
            vol_per_sqrt_sec: vol_now,
            source: "simulated_collector".to_string(),
        };
        match tx.send(frame.clone()) {
            Ok(()) => {
                log_jsonl(&logger, &cfg.book_path(), &frame)?;
                heartbeat(cfg, "collector", format!("sent step={step}"))?;
            }
            Err(_) => {
                // Receiver (quote engine) gone — the bot is shutting down.
                break;
            }
        }
        step = step.wrapping_add(1);
        sleep_ms(cfg.market_interval_ms);
    }
    Ok(())
}

// ── Live market data ──────────────────────────────────────────────────────────

fn run_live_market_data(cfg: &Config, stop: &StopFlag, tx: &MarketTx) -> AppResult<()> {
    let state = Arc::new(Mutex::new(LiveMarketState::new(cfg)));
    let sink = LiveFrameSink {
        tx: tx.clone(),
        book_path: cfg.book_path().to_path_buf(),
        logger: spawn_jsonl_writer(),
    };

    {
        let cfg = cfg.clone();
        let stop = Arc::clone(stop);
        let state = Arc::clone(&state);
        let sink = sink.clone();
        thread::spawn(move || live_polymarket_loop(cfg, stop, state, sink));
    }
    if cfg.auto_discover_market {
        let cfg = cfg.clone();
        let stop = Arc::clone(stop);
        let state = Arc::clone(&state);
        thread::spawn(move || market_discovery_loop(cfg, stop, state));
    }
    {
        let cfg = cfg.clone();
        let stop = Arc::clone(stop);
        let state = Arc::clone(&state);
        let sink = sink.clone();
        thread::spawn(move || live_btc_loop(cfg, stop, state, sink));
    }

    while !stopping(stop, cfg) {
        if state.lock().unwrap().frame(cfg).is_none() {
            heartbeat(cfg, "collector", "waiting live ws")?;
        }

        let now = now_ms();
        let stale = {
            let state = state.lock().unwrap();
            state.started
                && state.market.is_some()
                && (now.saturating_sub(state.last_polymarket_ts) > cfg.ws_stale_after_ms
                    || now.saturating_sub(state.last_btc_ts) > cfg.ws_stale_after_ms)
        };
        if stale {
            heartbeat(cfg, "collector", "kill switch: live ws stale")?;
            request_stop(stop, cfg)?;
            break;
        }

        sleep_ms(250);
    }
    Ok(())
}

/// Sends frames into the quote-engine channel and logs them to book.jsonl.
#[derive(Clone)]
struct LiveFrameSink {
    tx: MarketTx,
    book_path: std::path::PathBuf,
    logger: AsyncJsonlWriter,
}

fn push_live_frame(
    cfg: &Config,
    state: &Arc<Mutex<LiveMarketState>>,
    sink: &LiveFrameSink,
    status: &str,
) -> AppResult<bool> {
    let frame = {
        let state = state.lock().unwrap();
        state.frame(cfg)
    };
    let Some(frame) = frame else {
        return Ok(false);
    };

    if sink.tx.send(frame.clone()).is_err() {
        // Quote engine gone; nothing more to do.
        return Ok(false);
    }
    log_jsonl(&sink.logger, &sink.book_path, &frame)?;
    heartbeat(cfg, "collector", status)?;
    Ok(true)
}

#[derive(Clone)]
struct LiveMarketIdentity {
    slug: String,
    condition_id: String,
    up_token_id: String,
    down_token_id: String,
    price_to_beat: f64,
    window_end_s: u64,
}

#[derive(Clone)]
struct LiveMarketState {
    started: bool,
    market: Option<LiveMarketIdentity>,
    btc_price: f64,
    price_to_beat: f64,
    up_ask: f64,
    down_ask: f64,
    last_btc_ts: u64,
    last_polymarket_ts: u64,
    vol: VolEstimator,
}

impl LiveMarketState {
    /// Record a fresh BTC price and update the volatility estimate using the
    /// real elapsed time since the previous tick.
    fn record_btc(&mut self, price: f64) {
        let now = now_ms();
        let dt_sec = if self.last_btc_ts == 0 {
            1.0
        } else {
            (now.saturating_sub(self.last_btc_ts) as f64 / 1000.0).clamp(0.05, 30.0)
        };
        self.vol.update(price, dt_sec);
        self.btc_price = price;
        self.last_btc_ts = now;
        self.started = true;
    }

    fn new(cfg: &Config) -> Self {
        let market = (!cfg.polymarket_up_token_id.trim().is_empty()
            && !cfg.polymarket_down_token_id.trim().is_empty())
        .then(|| LiveMarketIdentity {
            slug: cfg.market_slug.clone(),
            condition_id: String::new(),
            up_token_id: cfg.polymarket_up_token_id.clone(),
            down_token_id: cfg.polymarket_down_token_id.clone(),
            price_to_beat: cfg.price_to_beat,
            window_end_s: (now_ms() / 1000) + cfg.market_window_secs,
        });
        let last_polymarket_ts = market.as_ref().map(|_| now_ms()).unwrap_or(0);
        Self {
            started: false,
            market,
            btc_price: cfg.price_to_beat,
            price_to_beat: cfg.price_to_beat,
            up_ask: 0.0,
            down_ask: 0.0,
            last_btc_ts: 0,
            last_polymarket_ts,
            vol: VolEstimator::new(
                cfg.vol_seed_per_sqrt_sec,
                cfg.vol_halflife_sec,
                cfg.vol_min_per_sqrt_sec,
                cfg.vol_max_per_sqrt_sec,
            ),
        }
    }

    fn frame(&self, cfg: &Config) -> Option<MarketFrame> {
        let market = self.market.as_ref()?;
        let now = now_ms();
        if now / 1000 >= market.window_end_s {
            return None;
        }
        if self.last_btc_ts == 0
            || self.up_ask <= 0.0
            || self.down_ask <= 0.0
            || self.btc_price <= 0.0
        {
            return None;
        }
        let window_end_ms = market.window_end_s.saturating_mul(1000);
        let tau_seconds = (window_end_ms.saturating_sub(now) as f64 / 1000.0).max(0.0);
        Some(MarketFrame {
            ts_ms: now,
            market: market.slug.clone(),
            condition_id: market.condition_id.clone(),
            up_token_id: market.up_token_id.clone(),
            down_token_id: market.down_token_id.clone(),
            up_ask: self.up_ask,
            down_ask: self.down_ask,
            btc_price: self.btc_price,
            price_to_beat: market.price_to_beat,
            tau_seconds,
            vol_per_sqrt_sec: self.vol.current(),
            source: if cfg.auto_discover_market {
                "auto_gamma_polymarket_binance_ws".to_string()
            } else {
                "live_polymarket_binance_ws".to_string()
            },
        })
    }

    fn set_market(&mut self, next: LiveMarketIdentity) -> bool {
        let changed = self.market.as_ref().is_none_or(|old| old.slug != next.slug);
        if changed {
            self.up_ask = 0.0;
            self.down_ask = 0.0;
            self.last_polymarket_ts = now_ms();
        }
        self.price_to_beat = next.price_to_beat;
        self.market = Some(next);
        changed
    }
}

fn live_polymarket_loop(
    cfg: Config,
    stop: StopFlag,
    state: Arc<Mutex<LiveMarketState>>,
    sink: LiveFrameSink,
) {
    while !stopping(&stop, &cfg) {
        if let Err(err) = live_polymarket_once(&cfg, &stop, &state, &sink) {
            let _ = heartbeat(&cfg, "collector", format!("polymarket ws reconnect: {err}"));
            sleep_ms(1_000);
        }
    }
}

fn market_discovery_loop(cfg: Config, stop: StopFlag, state: Arc<Mutex<LiveMarketState>>) {
    while !stopping(&stop, &cfg) {
        match discover_current_market(&cfg, &state) {
            Ok(Some(identity)) => {
                let changed = {
                    let mut state = state.lock().unwrap();
                    state.set_market(identity.clone())
                };
                if changed {
                    let _ = heartbeat(
                        &cfg,
                        "collector",
                        format!(
                            "auto market {} beat {:.2}",
                            identity.slug, identity.price_to_beat
                        ),
                    );
                }
            }
            Ok(None) => {
                let _ = heartbeat(&cfg, "collector", "auto market not found");
            }
            Err(err) => {
                let _ = heartbeat(&cfg, "collector", format!("auto market error: {err}"));
            }
        }
        sleep_ms(cfg.market_discovery_ms);
    }
}

fn discover_current_market(
    cfg: &Config,
    state: &Arc<Mutex<LiveMarketState>>,
) -> AppResult<Option<LiveMarketIdentity>> {
    let now_s = now_ms() / 1000;
    let start_s = (now_s / cfg.market_window_secs) * cfg.market_window_secs;
    let slug = format!("{}-{start_s}", cfg.market_slug.trim_end_matches('-'));
    let Some(mut identity) = fetch_gamma_market(cfg, &slug, start_s)? else {
        return Ok(None);
    };

    let price_to_beat = fetch_binance_start_price(cfg, start_s).or_else(|| {
        let state = state.lock().unwrap();
        (state.btc_price > 0.0).then_some(state.btc_price)
    });
    if let Some(price) = price_to_beat {
        identity.price_to_beat = price;
    }
    Ok(Some(identity))
}

fn fetch_gamma_market(
    cfg: &Config,
    slug: &str,
    window_start_s: u64,
) -> AppResult<Option<LiveMarketIdentity>> {
    let base = cfg.gamma_api_url.trim_end_matches('/');
    let url = format!("{base}/events/slug/{slug}");
    let value: Value = match ureq::get(&url).set("User-Agent", "polymaker/0.1").call() {
        Ok(resp) => resp.into_json()?,
        Err(ureq::Error::Status(404, _)) => return Ok(None),
        Err(err) => return Err(format!("Gamma query failed for {slug}: {err}").into()),
    };

    let market = value
        .get("markets")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .ok_or_else(|| format!("Gamma event {slug} has no markets"))?;
    if market
        .get("closed")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(None);
    }
    if !market
        .get("enableOrderBook")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        return Ok(None);
    }

    let outcomes = parse_json_string_array(market.get("outcomes"))
        .ok_or_else(|| format!("Gamma market {slug} missing outcomes"))?;
    let token_ids = parse_json_string_array(market.get("clobTokenIds"))
        .ok_or_else(|| format!("Gamma market {slug} missing clobTokenIds"))?;
    let (up_token_id, down_token_id) = map_up_down_tokens(&outcomes, &token_ids)
        .ok_or_else(|| format!("Gamma market {slug} does not expose Up/Down tokens"))?;

    Ok(Some(LiveMarketIdentity {
        slug: market
            .get("slug")
            .and_then(Value::as_str)
            .unwrap_or(slug)
            .to_string(),
        condition_id: market
            .get("conditionId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        up_token_id,
        down_token_id,
        price_to_beat: cfg.price_to_beat,
        window_end_s: window_start_s + cfg.market_window_secs,
    }))
}

fn fetch_binance_start_price(cfg: &Config, window_start_s: u64) -> Option<f64> {
    let start_ms = window_start_s * 1000;
    let end_ms = start_ms + cfg.market_switch_grace_ms.max(1_000).min(30_000);
    for base in [
        cfg.binance_rest_url.trim_end_matches('/'),
        "https://api.binance.us",
    ] {
        let url = format!(
            "{base}/api/v3/aggTrades?symbol=BTCUSDT&startTime={start_ms}&endTime={end_ms}&limit=1"
        );
        let Some(value) = http_json(&url) else {
            continue;
        };
        if let Some(price) = value
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| parse_f64_value(row.get("p")))
        {
            return Some(price);
        }
    }
    fetch_btc_rest_price(cfg)
}

fn fetch_btc_rest_price(cfg: &Config) -> Option<f64> {
    let binance_url = format!(
        "{}/api/v3/ticker/price?symbol=BTCUSDT",
        cfg.binance_rest_url.trim_end_matches('/')
    );
    for url in [
        binance_url.as_str(),
        "https://api.binance.us/api/v3/ticker/price?symbol=BTCUSDT",
        "https://api.coinbase.com/v2/prices/BTC-USD/spot",
        "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
    ] {
        let Some(value) = http_json(url) else {
            continue;
        };
        if let Some(price) = parse_btc_rest_price(&value) {
            return Some(price);
        }
    }
    None
}

fn parse_btc_rest_price(value: &Value) -> Option<f64> {
    parse_f64_value(value.get("price"))
        .or_else(|| {
            value
                .get("data")
                .and_then(|data| parse_f64_value(data.get("amount")))
        })
        .or_else(|| {
            value
                .get("result")
                .and_then(Value::as_object)?
                .values()
                .next()?
                .get("c")?
                .as_array()?
                .first()
                .and_then(|v| parse_f64_value(Some(v)))
        })
}

fn http_json(url: &str) -> Option<Value> {
    ureq::get(url)
        .set("User-Agent", "polymaker/0.1")
        .call()
        .ok()?
        .into_json::<Value>()
        .ok()
}

fn parse_json_string_array(value: Option<&Value>) -> Option<Vec<String>> {
    match value? {
        Value::Array(items) => Some(
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect(),
        ),
        Value::String(raw) => serde_json::from_str::<Vec<String>>(raw).ok(),
        _ => None,
    }
}

fn map_up_down_tokens(outcomes: &[String], token_ids: &[String]) -> Option<(String, String)> {
    if outcomes.len() != token_ids.len() {
        return None;
    }
    let mut up = None;
    let mut down = None;
    for (outcome, token_id) in outcomes.iter().zip(token_ids.iter()) {
        match outcome.trim().to_ascii_lowercase().as_str() {
            "up" | "yes" => up = Some(token_id.clone()),
            "down" | "no" => down = Some(token_id.clone()),
            _ => {}
        }
    }
    Some((up?, down?))
}

fn live_polymarket_once(
    cfg: &Config,
    stop: &StopFlag,
    state: &Arc<Mutex<LiveMarketState>>,
    sink: &LiveFrameSink,
) -> AppResult<()> {
    let identity = loop {
        if stopping(stop, cfg) {
            return Ok(());
        }
        if let Some(identity) = state.lock().unwrap().market.clone() {
            break identity;
        }
        heartbeat(cfg, "collector", "waiting auto market")?;
        sleep_ms(500);
    };
    let (mut socket, _) = connect(cfg.polymarket_ws_url.as_str())?;
    let sub = serde_json::json!({
        "assets_ids": [
            identity.up_token_id,
            identity.down_token_id
        ],
        "type": "market",
        "custom_feature_enabled": true
    });
    socket.send(Message::Text(sub.to_string()))?;
    tune_ws_socket(&mut socket, Duration::from_millis(1_000))?;
    heartbeat(cfg, "collector", format!("subscribed {}", identity.slug))?;
    let mut last_ping = Instant::now();

    while !stopping(stop, cfg) {
        let current_slug = state
            .lock()
            .unwrap()
            .market
            .as_ref()
            .map(|market| market.slug.clone());
        if current_slug.as_deref() != Some(identity.slug.as_str()) {
            break;
        }
        if last_ping.elapsed() >= Duration::from_secs(8) {
            socket.send(Message::Text("PING".to_string()))?;
            last_ping = Instant::now();
        }
        let msg = match socket.read() {
            Ok(msg) => msg,
            Err(err) if is_ws_timeout(&err) => continue,
            Err(err) => return Err(err.into()),
        };
        match msg {
            Message::Text(raw) => {
                if raw == "PONG" {
                    continue;
                }
                if let Ok(value) = serde_json::from_str::<Value>(&raw) {
                    if apply_polymarket_message(state, &value) {
                        if let Err(err) = push_live_frame(cfg, state, sink, "live polymarket event") {
                            let _ = heartbeat(
                                cfg,
                                "collector",
                                format!("engine channel wait: {err}"),
                            );
                        }
                    }
                }
            }
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload))?;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

fn live_btc_loop(
    cfg: Config,
    stop: StopFlag,
    state: Arc<Mutex<LiveMarketState>>,
    sink: LiveFrameSink,
) {
    if let Some(price) = fetch_btc_rest_price(&cfg) {
        {
            let mut state = state.lock().unwrap();
            state.record_btc(price);
        }
        let _ = push_live_frame(&cfg, &state, &sink, "btc rest seed");
        let _ = heartbeat(&cfg, "collector", format!("btc rest seed {price:.2}"));
    }

    while !stopping(&stop, &cfg) {
        let mut last_err = None;
        for url in btc_ws_urls(&cfg) {
            if stopping(&stop, &cfg) {
                break;
            }
            match live_btc_once(&cfg, &stop, &state, &sink, &url) {
                Ok(()) => {
                    last_err = None;
                    break;
                }
                Err(err) => {
                    let _ = heartbeat(&cfg, "collector", format!("btc ws switch {url}: {err}"));
                    last_err = Some(err);
                }
            }
        }
        if last_err.is_none() {
            continue;
        }

        if let Some(price) = fetch_btc_rest_price(&cfg) {
            {
                let mut state = state.lock().unwrap();
                state.record_btc(price);
            }
            let _ = push_live_frame(&cfg, &state, &sink, "btc rest fallback");
            let _ = heartbeat(&cfg, "collector", format!("btc rest fallback {price:.2}"));
        } else if let Some(err) = last_err {
            let _ = heartbeat(&cfg, "collector", format!("btc ws reconnect: {err}"));
        }
        sleep_ms(250);
    }
}

fn live_btc_once(
    cfg: &Config,
    stop: &StopFlag,
    state: &Arc<Mutex<LiveMarketState>>,
    sink: &LiveFrameSink,
    url: &str,
) -> AppResult<()> {
    let (mut socket, _) = connect(url)?;
    let quiet_timeout = Duration::from_millis((cfg.stale_after_ms / 2).clamp(250, 2_000));
    tune_ws_socket(&mut socket, quiet_timeout)?;
    heartbeat(cfg, "collector", format!("btc ws subscribed {url}"))?;
    let mut last_event = Instant::now();
    while !stopping(stop, cfg) {
        let msg = match socket.read() {
            Ok(msg) => msg,
            Err(err) if is_ws_timeout(&err) => {
                if last_event.elapsed() >= quiet_timeout {
                    return Err(format!("btc ws quiet for {:?}", last_event.elapsed()).into());
                }
                continue;
            }
            Err(err) => return Err(err.into()),
        };
        match msg {
            Message::Text(raw) => {
                if let Ok(value) = serde_json::from_str::<Value>(&raw) {
                    if let Some(price) = parse_btc_price(&value) {
                        last_event = Instant::now();
                        {
                            let mut state = state.lock().unwrap();
                            state.record_btc(price);
                        }
                        if let Err(err) = push_live_frame(cfg, state, sink, "live btc event") {
                            let _ = heartbeat(
                                cfg,
                                "collector",
                                format!("engine channel wait: {err}"),
                            );
                        }
                    }
                }
            }
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload))?;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

fn btc_ws_urls(cfg: &Config) -> Vec<String> {
    let mut urls = Vec::new();
    let primary = cfg.binance_ws_url.trim();
    if !primary.is_empty() {
        urls.push(primary.to_string());
    }
    let us = "wss://stream.binance.us:9443/ws/btcusdt@trade";
    if urls.iter().all(|url| url != us) {
        urls.push(us.to_string());
    }
    urls
}

fn apply_polymarket_message(state: &Arc<Mutex<LiveMarketState>>, value: &Value) -> bool {
    if let Some(items) = value.as_array() {
        let mut updated = false;
        for item in items {
            updated |= apply_polymarket_message(state, item);
        }
        return updated;
    }

    let event_type = value
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match event_type {
        "book" => {
            let Some(asset_id) = value.get("asset_id").and_then(Value::as_str) else {
                return false;
            };
            let Some(best_ask) = best_ask_from_levels(value.get("asks")) else {
                return false;
            };
            update_polymarket_ask(state, asset_id, best_ask)
        }
        "best_bid_ask" => {
            let Some(asset_id) = value.get("asset_id").and_then(Value::as_str) else {
                return false;
            };
            let Some(best_ask) = parse_f64_value(value.get("best_ask")) else {
                return false;
            };
            update_polymarket_ask(state, asset_id, best_ask)
        }
        "price_change" => {
            let Some(changes) = value.get("price_changes").and_then(Value::as_array) else {
                return false;
            };
            let mut updated = false;
            for change in changes {
                let Some(asset_id) = change.get("asset_id").and_then(Value::as_str) else {
                    continue;
                };
                let Some(best_ask) = parse_f64_value(change.get("best_ask")) else {
                    continue;
                };
                updated |= update_polymarket_ask(state, asset_id, best_ask);
            }
            updated
        }
        _ => false,
    }
}

fn update_polymarket_ask(state: &Arc<Mutex<LiveMarketState>>, asset_id: &str, best_ask: f64) -> bool {
    if !(0.0..=1.0).contains(&best_ask) || best_ask <= 0.0 {
        return false;
    }
    let mut state = state.lock().unwrap();
    let Some(market) = state.market.as_ref() else {
        return false;
    };
    let is_up = asset_id == market.up_token_id;
    let is_down = asset_id == market.down_token_id;
    if !is_up && !is_down {
        return false;
    }
    let changed = if is_up {
        (state.up_ask - best_ask).abs() > f64::EPSILON
    } else {
        (state.down_ask - best_ask).abs() > f64::EPSILON
    };
    state.started = true;
    state.last_polymarket_ts = now_ms();
    if is_up {
        state.up_ask = best_ask;
    } else if is_down {
        state.down_ask = best_ask;
    }
    changed
}

fn best_ask_from_levels(levels: Option<&Value>) -> Option<f64> {
    levels?
        .as_array()?
        .iter()
        .filter_map(|level| {
            let price = parse_f64_value(level.get("price"))?;
            let size = parse_f64_value(level.get("size")).unwrap_or(0.0);
            (size > 0.0).then_some(price)
        })
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}

fn parse_btc_price(value: &Value) -> Option<f64> {
    parse_f64_value(value.get("p"))
        .or_else(|| parse_f64_value(value.get("price")))
        .or_else(|| parse_f64_value(value.get("c")))
}

/// Shared with the gateway's user-WS parsing; kept here next to the other JSON
/// parsing helpers.
pub fn parse_f64_value(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}
