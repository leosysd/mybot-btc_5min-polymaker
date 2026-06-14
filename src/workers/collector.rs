//! Market-data collector thread: produces `MarketFrame`s (simulated or live
//! Polymarket/Binance WS + auto discovery) and sends them over the channel to
//! the quote engine. Logs every frame to book.jsonl.

use crate::config::Config;
use crate::direction::DirectionEstimator;
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

const WS_OPEN_CAPTURE_GRACE_SECS: u64 = 5;

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
    let logger = spawn_jsonl_writer(
        cfg.log_rotate_max_mb.saturating_mul(1024 * 1024),
        cfg.log_rotate_keep,
    );
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
    let mut direction = DirectionEstimator::default();
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
        let up_ask = (fair_up + 0.035 + micro_noise).clamp(0.03, 0.97);
        let down_ask = (1.0 - fair_up + 0.035 - micro_noise).clamp(0.03, 0.97);
        direction.record_trade(now_ms(), btc_price, None, None);
        let up_bid = (up_ask - 0.07).max(0.01);
        let down_bid = (down_ask - 0.07).max(0.01);
        let market_up = market_up_from_quotes(up_bid, up_ask, down_bid, down_ask);
        let mut frame = MarketFrame {
            ts_ms: now_ms(),
            market: cfg.market_slug.clone(),
            condition_id: String::new(),
            up_token_id: cfg.polymarket_up_token_id.clone(),
            down_token_id: cfg.polymarket_down_token_id.clone(),
            up_bid,
            up_ask,
            down_bid,
            down_ask,
            btc_price,
            price_to_beat,
            tau_seconds,
            vol_per_sqrt_sec: vol_now,
            source: "simulated_collector".to_string(),
            ..Default::default()
        };
        if cfg.enable_direction_shadow {
            let direction_snapshot = direction.snapshot(
                cfg,
                frame.ts_ms,
                btc_price,
                price_to_beat,
                tau_seconds,
                vol_now,
                market_up.map(|(up, _)| up),
                market_up.map(|(_, spread)| spread),
            );
            frame.apply_direction(direction_snapshot);
        }
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
        logger: spawn_jsonl_writer(
            cfg.log_rotate_max_mb.saturating_mul(1024 * 1024),
            cfg.log_rotate_keep,
        ),
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
    if cfg.enable_direction_shadow && cfg.enable_binance_book_signal {
        let cfg = cfg.clone();
        let stop = Arc::clone(stop);
        let state = Arc::clone(&state);
        thread::spawn(move || live_btc_book_loop(cfg, stop, state));
    }

    while !stopping(stop, cfg) {
        if state.lock().unwrap().frame(cfg).is_none() {
            heartbeat(cfg, "collector", "waiting live ws")?;
        }

        let now = now_ms();
        let stale = {
            let state = state.lock().unwrap();
            let active_market = state
                .market
                .as_ref()
                .is_some_and(|market| now / 1000 < market.window_end_s);
            state.started
                && active_market
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
    log_jsonl(&sink.logger, &cfg.book_path(), &frame)?;
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
    btc_open_window_start_s: u64,
    btc_open_price: f64,
    btc_source: String,
    up_bid: f64,
    up_ask: f64,
    down_bid: f64,
    down_ask: f64,
    last_btc_ts: u64,
    last_polymarket_ts: u64,
    vol: VolEstimator,
    direction: DirectionEstimator,
}

impl LiveMarketState {
    /// Record a fresh BTC price and update the volatility estimate using the
    /// real elapsed time since the previous tick.
    fn record_btc(
        &mut self,
        cfg: &Config,
        price: f64,
        qty: Option<f64>,
        buyer_is_maker: Option<bool>,
        can_capture_open: bool,
        source: &str,
    ) {
        let now = now_ms();
        let dt_sec = if self.last_btc_ts == 0 {
            1.0
        } else {
            (now.saturating_sub(self.last_btc_ts) as f64 / 1000.0).clamp(0.05, 30.0)
        };
        if can_capture_open {
            let window = cfg.market_window_secs.max(1);
            let now_s = now / 1000;
            let window_start_s = (now_s / window) * window;
            let capture_grace_secs =
                (cfg.market_switch_grace_ms / 1000).clamp(1, WS_OPEN_CAPTURE_GRACE_SECS);
            if self.btc_open_window_start_s != window_start_s {
                self.btc_open_window_start_s = window_start_s;
                self.btc_open_price = 0.0;
            }
            if self.btc_open_price <= 0.0
                && now_s.saturating_sub(window_start_s) <= capture_grace_secs
            {
                self.btc_open_price = price;
            }
        }
        self.vol.update(price, dt_sec);
        self.direction.record_trade(now, price, qty, buyer_is_maker);
        self.btc_price = price;
        self.btc_source = source.to_string();
        self.last_btc_ts = now;
        self.started = true;
    }

    fn record_btc_book(&mut self, bid: f64, bid_qty: f64, ask: f64, ask_qty: f64) {
        self.direction
            .record_book(now_ms(), bid, bid_qty, ask, ask_qty);
    }

    fn btc_open_for_window(&self, window_start_s: u64) -> Option<f64> {
        (self.btc_open_window_start_s == window_start_s && self.btc_open_price > 0.0)
            .then_some(self.btc_open_price)
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
            btc_open_window_start_s: 0,
            btc_open_price: 0.0,
            btc_source: String::new(),
            up_bid: 0.0,
            up_ask: 0.0,
            down_bid: 0.0,
            down_ask: 0.0,
            last_btc_ts: 0,
            last_polymarket_ts,
            vol: VolEstimator::new(
                cfg.vol_seed_per_sqrt_sec,
                cfg.vol_halflife_sec,
                cfg.vol_min_per_sqrt_sec,
                cfg.vol_max_per_sqrt_sec,
            ),
            direction: DirectionEstimator::default(),
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
        let market_up =
            market_up_from_quotes(self.up_bid, self.up_ask, self.down_bid, self.down_ask);
        let mut frame = MarketFrame {
            ts_ms: now,
            market: market.slug.clone(),
            condition_id: market.condition_id.clone(),
            up_token_id: market.up_token_id.clone(),
            down_token_id: market.down_token_id.clone(),
            up_bid: self.up_bid,
            up_ask: self.up_ask,
            down_bid: self.down_bid,
            down_ask: self.down_ask,
            btc_price: self.btc_price,
            price_to_beat: market.price_to_beat,
            tau_seconds,
            vol_per_sqrt_sec: self.vol.current(),
            source: if cfg.auto_discover_market {
                format!("auto_gamma_polymarket_{}", self.btc_source)
            } else {
                format!("live_polymarket_{}", self.btc_source)
            },
            ..Default::default()
        };
        if cfg.enable_direction_shadow {
            let direction_snapshot = self.direction.snapshot(
                cfg,
                now,
                self.btc_price,
                market.price_to_beat,
                tau_seconds,
                self.vol.current(),
                market_up.map(|(up, _)| up),
                market_up.map(|(_, spread)| spread),
            );
            frame.apply_direction(direction_snapshot);
        }
        Some(frame)
    }

    fn set_market(&mut self, next: LiveMarketIdentity) -> bool {
        let changed = self.market.as_ref().is_none_or(|old| old.slug != next.slug);
        if changed {
            // New window: LATCH the open-price strike exactly once. Within a window
            // the strike is FIXED (it's the settlement reference). It must NEVER be
            // overwritten by later discoveries — those carry the LIVE price whenever
            // the open-price fetch transiently fails, which would make the strike
            // track spot → S-P≈0 → the model stuck at ~50/50 (blind to direction,
            // bidding ~0.50 on both sides). That was THE bug behind "buys both
            // sides / ignores that the market already moved".
            self.up_bid = 0.0;
            self.up_ask = 0.0;
            self.down_bid = 0.0;
            self.down_ask = 0.0;
            self.last_polymarket_ts = now_ms();
            self.price_to_beat = next.price_to_beat;
            self.market = Some(next);
        }
        // Same window: keep everything latched (especially price_to_beat). Do NOT
        // overwrite — identity (slug/tokens/strike/window) is fixed for the window.
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

    // Strike (price_to_beat) MUST come from the same venue as the live price feed
    // (Binance) — a cross-venue basis would bias fair value near close. Binance
    // 1-min kline open = exact window-open price. In the first minute of a fresh
    // window that kline can temporarily be unavailable, so we fall back ONLY to a
    // Binance WS tick captured near the window boundary. We no longer lock the
    // CURRENT live price as strike; that recreates the old "S-P≈0" bug and makes
    // the model blind to direction.
    let price_to_beat = fetch_binance_start_price(cfg, start_s).or_else(|| {
        let state = state.lock().unwrap();
        state.btc_open_for_window(start_s)
    });
    let Some(price) = price_to_beat else {
        let _ = heartbeat(
            cfg,
            "collector",
            format!("waiting reliable Binance strike for {slug}"),
        );
        return Ok(None);
    };
    identity.price_to_beat = price;
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

/// Exact window-open price from Binance, via the 1-min kline whose bucket
/// starts at `window_start_s` (the 5-min window starts are minute-aligned, so the
/// bucket exists). Kline row format: [open_time_ms, OPEN(idx 1), high, low, ...].
/// Same venue as the live feed → fair value's (S - P) carries no exchange basis.
fn fetch_binance_start_price(cfg: &Config, window_start_s: u64) -> Option<f64> {
    let base = cfg.binance_rest_url.trim_end_matches('/');
    let start_ms = window_start_s.saturating_mul(1000);
    let end_ms = start_ms.saturating_add(120_000);
    let url = format!(
        "{base}/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime={start_ms}&endTime={end_ms}&limit=2"
    );
    let rows = http_json(&url)?;
    parse_binance_kline_open(&rows, start_ms)
}

fn parse_binance_kline_open(rows: &Value, start_ms: u64) -> Option<f64> {
    let rows = rows.as_array()?;
    for row in rows {
        let Some(arr) = row.as_array() else {
            continue;
        };
        let t = arr.first().and_then(Value::as_f64).unwrap_or(-1.0);
        if (t - start_ms as f64).abs() < 1.0 {
            if let Some(open) = parse_f64_value(arr.get(1)) {
                return (open > 0.0).then_some(open);
            }
        }
    }
    None
}

fn fetch_binance_rest_price(cfg: &Config) -> Option<f64> {
    // Live BTC price and strike must stay same-venue. If Binance is unavailable,
    // do not silently substitute another venue: cross-venue basis can be large
    // enough to flip fair value near expiry. No fresh Binance price means no
    // fresh quote.
    let base = cfg.binance_rest_url.trim_end_matches('/');
    let url = format!("{base}/api/v3/ticker/price?symbol=BTCUSDT");
    let value = http_json(&url)?;
    parse_btc_rest_price(&value)
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
                        if let Err(err) = push_live_frame(cfg, state, sink, "live polymarket event")
                        {
                            let _ =
                                heartbeat(cfg, "collector", format!("engine channel wait: {err}"));
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
    if let Some(price) = fetch_binance_rest_price(&cfg) {
        {
            let mut state = state.lock().unwrap();
            state.record_btc(&cfg, price, None, None, false, "binance_rest_seed");
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

        if let Some(price) = fetch_binance_rest_price(&cfg) {
            {
                let mut state = state.lock().unwrap();
                state.record_btc(&cfg, price, None, None, false, "binance_rest_fallback");
            }
            let _ = push_live_frame(&cfg, &state, &sink, "btc rest fallback");
            let _ = heartbeat(&cfg, "collector", format!("btc rest fallback {price:.2}"));
        } else if let Some(err) = last_err {
            let _ = heartbeat(&cfg, "collector", format!("btc ws reconnect: {err}"));
        }
        sleep_ms(250);
    }
}

fn live_btc_book_loop(cfg: Config, stop: StopFlag, state: Arc<Mutex<LiveMarketState>>) {
    while !stopping(&stop, &cfg) {
        if let Err(err) = live_btc_book_once(&cfg, &stop, &state) {
            let _ = heartbeat(&cfg, "collector", format!("btc book ws reconnect: {err}"));
            sleep_ms(1_000);
        }
    }
}

fn live_btc_book_once(
    cfg: &Config,
    stop: &StopFlag,
    state: &Arc<Mutex<LiveMarketState>>,
) -> AppResult<()> {
    let (mut socket, _) = connect(cfg.binance_book_ws_url.as_str())?;
    let read_timeout = Duration::from_millis(500);
    let dead_timeout = Duration::from_millis((cfg.stale_after_ms * 5).clamp(2_500, 6_000));
    tune_ws_socket(&mut socket, read_timeout)?;
    heartbeat(
        cfg,
        "collector",
        format!("btc book ws subscribed {}", cfg.binance_book_ws_url),
    )?;
    let mut last_event = Instant::now();
    while !stopping(stop, cfg) {
        let msg = match socket.read() {
            Ok(msg) => msg,
            Err(err) if is_ws_timeout(&err) => {
                if last_event.elapsed() >= dead_timeout {
                    return Err(format!("btc book ws quiet for {:?}", last_event.elapsed()).into());
                }
                continue;
            }
            Err(err) => return Err(err.into()),
        };
        match msg {
            Message::Text(raw) => {
                if let Ok(value) = serde_json::from_str::<Value>(&raw) {
                    if let Some((bid, bid_qty, ask, ask_qty)) = parse_book_ticker(&value) {
                        last_event = Instant::now();
                        state
                            .lock()
                            .unwrap()
                            .record_btc_book(bid, bid_qty, ask, ask_qty);
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

fn live_btc_once(
    cfg: &Config,
    stop: &StopFlag,
    state: &Arc<Mutex<LiveMarketState>>,
    sink: &LiveFrameSink,
    url: &str,
) -> AppResult<()> {
    let (mut socket, _) = connect(url)?;
    // Decouple two timeouts that used to be one over-aggressive value:
    //  - read_timeout: how long each socket.read() blocks (short = responsive to
    //    stop and to draining).
    //  - dead_timeout: how long with NO price before we declare the socket dead
    //    and reconnect/fall back. The old code used STALE_AFTER_MS/2 (~400ms) for
    //    BOTH, so a routine ~300-400ms delivery gap on the high-latency, jittery
    //    Dublin<->Binance(Asia) path tripped a reconnect every time → constant
    //    flapping to REST. Brief gaps are NOT a dead connection. The downstream
    //    staleness gates (engine skips stale frames, gateway rejects stale quotes)
    //    already protect pricing, so the collector only needs to drop a GENUINELY
    //    dead socket (several seconds of silence).
    let read_timeout = Duration::from_millis(500);
    let dead_timeout = Duration::from_millis((cfg.stale_after_ms * 5).clamp(2_500, 6_000));
    tune_ws_socket(&mut socket, read_timeout)?;
    // Binance trade streams start sending immediately and carry price in field "p".
    heartbeat(cfg, "collector", format!("btc ws subscribed {url}"))?;
    let mut last_event = Instant::now();
    while !stopping(stop, cfg) {
        let msg = match socket.read() {
            Ok(msg) => msg,
            Err(err) if is_ws_timeout(&err) => {
                if last_event.elapsed() >= dead_timeout {
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
                        let qty = parse_btc_quantity(&value);
                        let buyer_is_maker = parse_buyer_is_maker(&value);
                        last_event = Instant::now();
                        {
                            let mut state = state.lock().unwrap();
                            state.record_btc(cfg, price, qty, buyer_is_maker, true, "binance_ws");
                        }
                        if let Err(err) = push_live_frame(cfg, state, sink, "live btc event") {
                            let _ =
                                heartbeat(cfg, "collector", format!("engine channel wait: {err}"));
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
    // Binance ONLY for the BTC model input. The strike (price_to_beat) is also
    // sourced from Binance (see fetch_binance_start_price), so live price S and
    // strike P stay same-venue. If the WS drops, live_btc_loop falls back to
    // Binance REST before reconnecting.
    vec![cfg.binance_ws_url.clone()]
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
            let best_bid = best_bid_from_levels(value.get("bids"));
            let best_ask = best_ask_from_levels(value.get("asks"));
            if best_bid.is_none() && best_ask.is_none() {
                return false;
            }
            update_polymarket_book(state, asset_id, best_bid, best_ask)
        }
        "best_bid_ask" => {
            let Some(asset_id) = value.get("asset_id").and_then(Value::as_str) else {
                return false;
            };
            let best_bid = parse_f64_value(value.get("best_bid"));
            let best_ask = parse_f64_value(value.get("best_ask"));
            if best_bid.is_none() && best_ask.is_none() {
                return false;
            }
            update_polymarket_book(state, asset_id, best_bid, best_ask)
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
                let best_bid = parse_f64_value(change.get("best_bid"));
                let best_ask = parse_f64_value(change.get("best_ask"));
                if best_bid.is_none() && best_ask.is_none() {
                    continue;
                }
                updated |= update_polymarket_book(state, asset_id, best_bid, best_ask);
            }
            updated
        }
        _ => false,
    }
}

fn update_polymarket_book(
    state: &Arc<Mutex<LiveMarketState>>,
    asset_id: &str,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
) -> bool {
    let best_bid = best_bid.filter(|px| valid_probability_price(*px));
    let best_ask = best_ask.filter(|px| valid_probability_price(*px));
    let mut changed = false;
    let mut state = state.lock().unwrap();
    let Some(market) = state.market.as_ref() else {
        return false;
    };
    let is_up = asset_id == market.up_token_id;
    let is_down = asset_id == market.down_token_id;
    if !is_up && !is_down {
        return false;
    }
    state.started = true;
    state.last_polymarket_ts = now_ms();
    if is_up {
        if let Some(best_bid) = best_bid {
            changed |= (state.up_bid - best_bid).abs() > f64::EPSILON;
            state.up_bid = best_bid;
        }
        if let Some(best_ask) = best_ask {
            changed |= (state.up_ask - best_ask).abs() > f64::EPSILON;
            state.up_ask = best_ask;
        }
    } else if is_down {
        if let Some(best_bid) = best_bid {
            changed |= (state.down_bid - best_bid).abs() > f64::EPSILON;
            state.down_bid = best_bid;
        }
        if let Some(best_ask) = best_ask {
            changed |= (state.down_ask - best_ask).abs() > f64::EPSILON;
            state.down_ask = best_ask;
        }
    }
    changed
}

fn valid_probability_price(px: f64) -> bool {
    px.is_finite() && px > 0.0 && px <= 1.0
}

fn market_up_from_quotes(
    up_bid: f64,
    up_ask: f64,
    down_bid: f64,
    down_ask: f64,
) -> Option<(f64, f64)> {
    let up = valid_probability_price(up_bid)
        .then_some(up_bid)
        .zip(valid_probability_price(up_ask).then_some(up_ask))
        .filter(|(bid, ask)| ask >= bid)
        .map(|(bid, ask)| ((bid + ask) / 2.0, ask - bid));
    let down = valid_probability_price(down_bid)
        .then_some(down_bid)
        .zip(valid_probability_price(down_ask).then_some(down_ask))
        .filter(|(bid, ask)| ask >= bid)
        .map(|(bid, ask)| (1.0 - ((bid + ask) / 2.0), ask - bid));
    match (up, down) {
        (Some((up_mid, up_spread)), Some((down_mid, down_spread))) => Some((
            ((up_mid + down_mid) / 2.0).clamp(0.0, 1.0),
            up_spread.max(down_spread),
        )),
        (Some((up_mid, up_spread)), None) => Some((up_mid.clamp(0.0, 1.0), up_spread)),
        (None, Some((down_mid, down_spread))) => Some((down_mid.clamp(0.0, 1.0), down_spread)),
        (None, None) => None,
    }
}

fn best_bid_from_levels(levels: Option<&Value>) -> Option<f64> {
    levels?
        .as_array()?
        .iter()
        .filter_map(|level| {
            let price = parse_f64_value(level.get("price"))?;
            let size = parse_f64_value(level.get("size")).unwrap_or(0.0);
            (size > 0.0).then_some(price)
        })
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
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

fn parse_btc_quantity(value: &Value) -> Option<f64> {
    parse_f64_value(value.get("q"))
        .or_else(|| parse_f64_value(value.get("quantity")))
        .or_else(|| parse_f64_value(value.get("v")))
}

fn parse_buyer_is_maker(value: &Value) -> Option<bool> {
    value
        .get("m")
        .and_then(Value::as_bool)
        .or_else(|| value.get("buyer_is_maker").and_then(Value::as_bool))
}

fn parse_book_ticker(value: &Value) -> Option<(f64, f64, f64, f64)> {
    let bid = parse_f64_value(value.get("b")).or_else(|| parse_f64_value(value.get("bid")))?;
    let bid_qty =
        parse_f64_value(value.get("B")).or_else(|| parse_f64_value(value.get("bidQty")))?;
    let ask = parse_f64_value(value.get("a")).or_else(|| parse_f64_value(value.get("ask")))?;
    let ask_qty =
        parse_f64_value(value.get("A")).or_else(|| parse_f64_value(value.get("askQty")))?;
    (bid > 0.0 && ask > bid && bid_qty > 0.0 && ask_qty > 0.0)
        .then_some((bid, bid_qty, ask, ask_qty))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(slug: &str, strike: f64) -> LiveMarketIdentity {
        LiveMarketIdentity {
            slug: slug.to_string(),
            condition_id: String::new(),
            up_token_id: "u".to_string(),
            down_token_id: "d".to_string(),
            price_to_beat: strike,
            window_end_s: 0,
        }
    }

    #[test]
    fn strike_latches_once_per_window() {
        let cfg = Config::from_env().expect("config");
        let mut s = LiveMarketState::new(&cfg);
        // First discovery of window A latches the open strike (100).
        assert!(s.set_market(ident("win-A", 100.0)));
        assert_eq!(s.market.as_ref().unwrap().price_to_beat, 100.0);
        // A later same-window discovery carrying the LIVE price (150, the bug
        // source) must NOT move the strike.
        assert!(!s.set_market(ident("win-A", 150.0)));
        assert_eq!(
            s.market.as_ref().unwrap().price_to_beat,
            100.0,
            "strike must stay latched within a window"
        );
        // A new window latches its own open strike.
        assert!(s.set_market(ident("win-B", 200.0)));
        assert_eq!(s.market.as_ref().unwrap().price_to_beat, 200.0);
    }

    #[test]
    fn strike_fallback_requires_matching_captured_ws_open() {
        let cfg = Config::from_env().expect("config");
        let mut s = LiveMarketState::new(&cfg);
        s.btc_price = 150.0;
        s.btc_open_window_start_s = 1_000;
        s.btc_open_price = 100.0;

        assert_eq!(s.btc_open_for_window(1_000), Some(100.0));
        assert_eq!(
            s.btc_open_for_window(1_300),
            None,
            "current BTC price must not be reused as another window's strike"
        );
    }

    #[test]
    fn binance_kline_open_parser_uses_window_open() {
        let rows = serde_json::json!([
            [1_000_000_u64, "101.25", "102.0", "100.0", "101.5", "3.0"],
            [1_060_000_u64, "103.25", "104.0", "103.0", "103.5", "4.0"]
        ]);
        assert_eq!(parse_binance_kline_open(&rows, 1_000_000), Some(101.25));
        assert_eq!(parse_binance_kline_open(&rows, 1_120_000), None);
    }

    #[test]
    fn book_message_updates_best_bid_and_ask() {
        let cfg = Config::from_env().expect("config");
        let state = Arc::new(Mutex::new(LiveMarketState::new(&cfg)));
        assert!(state.lock().unwrap().set_market(ident("win-A", 100.0)));
        let value = serde_json::json!({
            "event_type": "book",
            "asset_id": "u",
            "bids": [
                {"price": "0.39", "size": "5"},
                {"price": "0.41", "size": "5"}
            ],
            "asks": [
                {"price": "0.45", "size": "5"},
                {"price": "0.44", "size": "5"}
            ]
        });

        assert!(apply_polymarket_message(&state, &value));
        let state = state.lock().unwrap();
        assert!((state.up_bid - 0.41).abs() < 1e-9);
        assert!((state.up_ask - 0.44).abs() < 1e-9);
    }
}
