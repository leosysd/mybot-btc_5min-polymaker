//! Quote-engine thread: turns each `MarketFrame` into time-aware quotes and
//! sends `QuoteIntent`s to the order gateway. Reads (never writes) the shared
//! inventory.

use crate::config::Config;
use crate::ipc::{heartbeat, now_ms, Inventory, MarketFrame, QuoteIntent};
use crate::pricing::{
    blend_market_anchor, digital_p_up, half_spread, in_warmup, market_anchor_weight,
    market_maker_bids, phase_for, post_only_bid, price_sensitivity, time_boosted_skew,
    uncertainty_width, MmParams, ModelQuote, Phase, SpreadInputs, ToxicityMonitor,
};
use crate::AppResult;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use super::state::{request_stop, stopping, MarketRx, QuoteTx, SharedInventory, StopFlag};

const RECV_TIMEOUT: Duration = Duration::from_millis(250);

pub fn run(
    cfg: Config,
    stop: StopFlag,
    inventory: SharedInventory,
    rx: MarketRx,
    quote_tx: QuoteTx,
) -> AppResult<()> {
    // Adverse-selection monitor: watches whether fair value drifts against us
    // right after each fill, and asks for wider spreads when it does.
    let mut tox = ToxicityMonitor::new(
        cfg.tox_horizon_ms,
        cfg.tox_decay,
        cfg.tox_k_widen,
        cfg.tox_max_widen,
    );
    let mut last_p_up = 0.5_f64;
    let (mut prev_up, mut prev_down) = {
        let inv = inventory.lock().unwrap();
        (inv.up_shares, inv.down_shares)
    };
    let mut last_market_ts = None::<u64>;

    while !stopping(&stop, &cfg) {
        // Detect fills via the shared inventory (the gateway / user-WS thread is
        // the authoritative writer): a rise in filled shares means we were hit,
        // so feed the toxicity monitor with the fair value that was live then.
        {
            let inv = inventory.lock().unwrap();
            let now = now_ms();
            if inv.up_shares > prev_up + 1e-9 {
                tox.on_fill(true, last_p_up, now);
            }
            if inv.down_shares > prev_down + 1e-9 {
                tox.on_fill(false, last_p_up, now);
            }
            prev_up = inv.up_shares;
            prev_down = inv.down_shares;
        }

        match rx.recv_timeout(RECV_TIMEOUT) {
            Ok(frame) => {
                last_market_ts = Some(frame.ts_ms);
                if now_ms().saturating_sub(frame.ts_ms) > cfg.stale_after_ms {
                    heartbeat(&cfg, "quote-engine", "skipped stale market frame")?;
                    continue;
                }
                let inv_snapshot = inventory.lock().unwrap().clone();
                last_p_up = handle_market_frame(&cfg, &quote_tx, &frame, &inv_snapshot, &mut tox)?;
                heartbeat(&cfg, "quote-engine", "quoted")?;
            }
            Err(RecvTimeoutError::Timeout) => {
                if let Some(ts) = last_market_ts {
                    let market_silence_ms = cfg.ws_stale_after_ms.max(cfg.stale_after_ms);
                    if !collector_owns_market_silence(&cfg)
                        && now_ms().saturating_sub(ts) > market_silence_ms
                    {
                        heartbeat(&cfg, "quote-engine", "kill switch: market stale")?;
                        request_stop(&stop, &cfg)?;
                        break;
                    }
                }
                heartbeat(&cfg, "quote-engine", "waiting")?;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}

fn collector_owns_market_silence(cfg: &Config) -> bool {
    cfg.data_mode == "live" && cfg.auto_discover_market
}

fn valid_market_px(px: f64) -> bool {
    px.is_finite() && px > 0.0 && px <= 1.0
}

fn top_bid(frame: &MarketFrame, side_is_up: bool) -> Option<f64> {
    let bid = if side_is_up {
        frame.up_bid
    } else {
        frame.down_bid
    };
    valid_market_px(bid).then_some(bid)
}

fn market_up_mid_and_spread(frame: &MarketFrame) -> Option<(f64, f64)> {
    let up = valid_market_px(frame.up_bid)
        .then_some(frame.up_bid)
        .zip(valid_market_px(frame.up_ask).then_some(frame.up_ask))
        .filter(|(bid, ask)| ask >= bid)
        .map(|(bid, ask)| ((bid + ask) / 2.0, ask - bid));
    let down = valid_market_px(frame.down_bid)
        .then_some(frame.down_bid)
        .zip(valid_market_px(frame.down_ask).then_some(frame.down_ask))
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

#[derive(Debug, Clone, Copy)]
struct SideFair {
    shadow: f64,
    quote: f64,
    anchor_weight: f64,
    anchor_active: bool,
}

impl SideFair {
    fn fair_source(self) -> &'static str {
        if self.anchor_active {
            "market_anchor"
        } else if self.anchor_weight > 0.0 {
            "model_shadow"
        } else {
            "model"
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FairSnapshot {
    model_up: f64,
    market_up: f64,
    up: SideFair,
    down: SideFair,
}

impl FairSnapshot {
    fn quote_up(self) -> f64 {
        self.up.quote
    }

    fn quote_down(self) -> f64 {
        self.down.quote
    }

    fn composite_up(self) -> f64 {
        ((self.up.quote + (1.0 - self.down.quote)) / 2.0).clamp(0.0, 1.0)
    }

    fn final_up_shadow(self) -> f64 {
        ((self.up.shadow + (1.0 - self.down.shadow)) / 2.0).clamp(0.0, 1.0)
    }

    fn side_anchor_weight(self, side: &str) -> f64 {
        if side == "Up" {
            self.up.anchor_weight
        } else {
            self.down.anchor_weight
        }
    }

    fn fair_source(self, side: &str) -> &'static str {
        if side == "Up" {
            self.up.fair_source()
        } else {
            self.down.fair_source()
        }
    }
}

fn side_anchor_weight(cfg: &Config, side_model_fair: f64, spread: f64) -> f64 {
    let max_weight = if side_model_fair < cfg.market_anchor_low_side_below {
        cfg.market_anchor_weight_low
    } else {
        cfg.market_anchor_weight_high
    };
    market_anchor_weight(max_weight, spread, cfg.market_anchor_max_spread)
}

fn fair_snapshot(cfg: &Config, frame: &MarketFrame, model_up: f64) -> FairSnapshot {
    let anchor_enabled = cfg.enable_market_anchor || cfg.market_anchor_shadow;
    let model_down = 1.0 - model_up;
    let (market_up, spread) = if anchor_enabled {
        market_up_mid_and_spread(frame).unwrap_or((0.0, 1.0))
    } else {
        (0.0, 1.0)
    };
    let market_down = 1.0 - market_up;
    let up_anchor_weight = if anchor_enabled {
        side_anchor_weight(cfg, model_up, spread)
    } else {
        0.0
    };
    let down_anchor_weight = if anchor_enabled {
        side_anchor_weight(cfg, model_down, spread)
    } else {
        0.0
    };
    let up_shadow = if up_anchor_weight > 0.0 {
        blend_market_anchor(model_up, market_up, up_anchor_weight)
    } else {
        model_up
    };
    let down_shadow = if down_anchor_weight > 0.0 {
        blend_market_anchor(model_down, market_down, down_anchor_weight)
    } else {
        model_down
    };
    let up_anchor_active = cfg.enable_market_anchor && up_anchor_weight > 0.0;
    let down_anchor_active = cfg.enable_market_anchor && down_anchor_weight > 0.0;
    FairSnapshot {
        model_up,
        market_up,
        up: SideFair {
            shadow: up_shadow,
            quote: if up_anchor_active {
                up_shadow
            } else {
                model_up
            },
            anchor_weight: up_anchor_weight,
            anchor_active: up_anchor_active,
        },
        down: SideFair {
            shadow: down_shadow,
            quote: if down_anchor_active {
                down_shadow
            } else {
                model_down
            },
            anchor_weight: down_anchor_weight,
            anchor_active: down_anchor_active,
        },
    }
}

fn value_buy_px(
    cfg: &Config,
    frame: &MarketFrame,
    side_is_up: bool,
    fair: f64,
    model_bid: f64,
) -> Option<f64> {
    if fair < cfg.value_min_fair {
        return None;
    }
    let edge_cap = fair - cfg.value_min_edge;
    if edge_cap < cfg.min_bid {
        return None;
    }
    let book_improved = top_bid(frame, side_is_up)
        .map(|bid| bid + cfg.value_aggression_ticks * cfg.tick_size)
        .unwrap_or(model_bid);
    let raw_bid = model_bid.max(book_improved).min(edge_cap).min(cfg.max_bid);
    let ask = if side_is_up {
        frame.up_ask
    } else {
        frame.down_ask
    };
    post_only_bid(
        raw_bid,
        ask,
        cfg.tick_size,
        cfg.min_bid,
        cfg.max_bid,
        cfg.post_only_margin_ticks,
    )
}

fn value_buy_quotes(
    cfg: &Config,
    frame: &MarketFrame,
    phase: Phase,
    up_fair: f64,
    down_fair: f64,
    model: ModelQuote,
) -> (Option<f64>, Option<f64>, &'static str) {
    if phase == Phase::Pull {
        return (None, None, "value_buy_pull");
    }
    let up_px = value_buy_px(cfg, frame, true, up_fair, model.up_bid);
    let down_px = value_buy_px(cfg, frame, false, down_fair, model.down_bid);
    (up_px, down_px, "value_buy")
}

fn unpaired_limit_allows(cfg: &Config, inventory: &Inventory, side: &str, size: f64) -> bool {
    let limit = cfg.max_unpaired_shares;
    if limit <= 0.0 {
        return true;
    }
    let current = inventory.effective_up() - inventory.effective_down();
    let projected = match side {
        "Up" => current + size,
        "Down" => current - size,
        _ => current,
    };
    projected.abs() <= limit + 1e-9 || projected.abs() < current.abs()
}

/// Compute the time-aware quotes for one market frame and emit them.
/// Returns the fair `p_up` used, so the caller can feed the toxicity monitor.
fn handle_market_frame(
    cfg: &Config,
    quote_tx: &QuoteTx,
    frame: &MarketFrame,
    inventory: &Inventory,
    tox: &mut ToxicityMonitor,
) -> AppResult<f64> {
    let current_inventory;
    let inventory = if inventory.market == frame.market {
        inventory
    } else {
        current_inventory = Inventory {
            market: frame.market.clone(),
            ..Default::default()
        };
        &current_inventory
    };

    // ── 1. Time-aware fair value: p_up = Phi((S - P) / W), W = sigma*sqrt(tau).
    let window = cfg.market_window_secs as f64;
    let tau = if frame.tau_seconds > 0.0 {
        frame.tau_seconds
    } else {
        window
    };
    let vol = if frame.vol_per_sqrt_sec > 0.0 {
        frame.vol_per_sqrt_sec
    } else {
        cfg.vol_seed_per_sqrt_sec
    };
    let width = uncertainty_width(vol, tau, cfg.width_floor_usd);
    let p_model_up = digital_p_up(frame.btc_price, frame.price_to_beat, width);
    let fair = fair_snapshot(cfg, frame, p_model_up);
    let p_up = fair.composite_up();

    // ── 2. Toxicity feedback: settle matured fills, get any extra widening.
    tox.on_fair(p_up, now_ms());

    // ── 2b. Opening quiet period. Live data (2026-06-10): 21 of 33 windows
    // had their first fill within 10s of the open, and the resulting one-sided
    // positions lost 11 of 13 times — informed flow hits a seconds-old market
    // exactly when our fair is still a coin flip. Don't quote a young window.
    if in_warmup(tau, window, cfg.quote_warmup_secs) {
        return Ok(p_up);
    }

    // ── 3. Spread: base + adverse-selection (sensitivity-driven) + toxicity.
    let sensitivity = price_sensitivity(frame.btc_price, frame.price_to_beat, width);
    let half = half_spread(&SpreadInputs {
        base_half_spread: cfg.base_half_spread,
        sensitivity,
        vol_per_sqrt_sec: vol,
        latency_sec: cfg.latency_sec,
        k_adverse: cfg.k_adverse,
        toxicity_widen: tox.widen(),
        min_half_spread: cfg.min_half_spread,
        max_half_spread: cfg.max_half_spread,
    });

    // ── 4. Inventory skew, ramped up as the window closes.
    let skew = time_boosted_skew(
        cfg.inventory_skew,
        tau,
        window,
        cfg.inventory_skew_time_boost,
    );
    let up_eff = inventory.effective_up();
    let down_eff = inventory.effective_down();
    let model = market_maker_bids(&MmParams {
        p_up,
        half_spread: half,
        inventory_skew: skew,
        up_inventory: up_eff,
        down_inventory: down_eff,
        max_side_inventory: cfg.max_side_inventory(),
        min_bid: cfg.min_bid,
        max_bid: cfg.max_bid,
        min_lock_edge: cfg.min_lock_edge,
    });

    let phase = phase_for(tau, cfg.endgame_pull_secs);
    let (up_px, down_px, reason) =
        value_buy_quotes(cfg, frame, phase, fair.quote_up(), fair.quote_down(), model);
    if let Some(px) = up_px {
        send_quote(
            cfg,
            quote_tx,
            frame,
            "Up",
            px,
            fair.quote_up(),
            inventory,
            reason,
            fair,
        )?;
    }
    if let Some(px) = down_px {
        send_quote(
            cfg,
            quote_tx,
            frame,
            "Down",
            px,
            fair.quote_down(),
            inventory,
            reason,
            fair,
        )?;
    }
    Ok(p_up)
}

#[allow(clippy::too_many_arguments)]
fn send_quote(
    cfg: &Config,
    quote_tx: &QuoteTx,
    frame: &MarketFrame,
    side: &str,
    price: f64,
    fair: f64,
    inventory: &Inventory,
    reason: &str,
    fair_snapshot: FairSnapshot,
) -> AppResult<()> {
    let desired_size = cfg.quote_size.round().max(1.0);
    let side_inventory = if side == "Up" {
        inventory.effective_up()
    } else {
        inventory.effective_down()
    };
    let left = (cfg.max_side_inventory() - side_inventory).floor();
    if left + 1e-9 < cfg.min_order_size() {
        return Ok(());
    }
    let size = desired_size.min(left).floor().max(cfg.min_order_size());
    if !unpaired_limit_allows(cfg, inventory, side, size) {
        return Ok(());
    }
    let quote = QuoteIntent {
        quote_id: format!("{}-{side}-{}", frame.ts_ms, now_ms()),
        ts_ms: now_ms(),
        market: frame.market.clone(),
        condition_id: frame.condition_id.clone(),
        token_id: if side == "Up" {
            frame.up_token_id.clone()
        } else {
            frame.down_token_id.clone()
        },
        side: side.to_string(),
        price,
        size,
        fair,
        model_up: fair_snapshot.model_up,
        market_up: fair_snapshot.market_up,
        final_up_shadow: fair_snapshot.final_up_shadow(),
        market_anchor_weight: fair_snapshot.side_anchor_weight(side),
        fair_source: fair_snapshot.fair_source(side).to_string(),
        inventory_up: inventory.effective_up(),
        inventory_down: inventory.effective_down(),
        reason: reason.to_string(),
    };
    // Best-effort: if the gateway is gone we're shutting down.
    let _ = quote_tx.send(quote);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_auto_discovery_waits_for_collector_liveness() {
        let mut cfg = Config::from_env().expect("config");
        cfg.data_mode = "live".to_string();
        cfg.auto_discover_market = true;
        assert!(collector_owns_market_silence(&cfg));

        cfg.auto_discover_market = false;
        assert!(!collector_owns_market_silence(&cfg));

        cfg.data_mode = "sim".to_string();
        cfg.auto_discover_market = true;
        assert!(!collector_owns_market_silence(&cfg));
    }

    fn flat_quote_test_cfg() -> Config {
        let mut cfg = Config::from_env().expect("config");
        cfg.max_bid = 0.92;
        cfg.min_bid = 0.05;
        cfg.min_lock_edge = 0.02;
        cfg.quote_size = 5.0;
        cfg.inventory_mult = 2.0;
        cfg.max_unpaired_shares = 5.0;
        cfg.post_only_margin_ticks = 1.0;
        cfg.tick_size = 0.01;
        cfg.base_half_spread = 0.01;
        cfg.min_half_spread = 0.0;
        cfg.max_half_spread = 0.25;
        cfg.latency_sec = 0.0;
        cfg.k_adverse = 0.0;
        cfg
    }

    fn value_buy_quote_test_cfg() -> Config {
        let mut cfg = flat_quote_test_cfg();
        cfg.value_min_edge = 0.03;
        cfg.value_aggression_ticks = 1.0;
        cfg.value_min_fair = 0.05;
        cfg
    }

    fn flat_frame(btc_price: f64) -> MarketFrame {
        MarketFrame {
            ts_ms: now_ms(),
            market: "btc-updown-5m-test".to_string(),
            condition_id: "condition".to_string(),
            up_token_id: "up".to_string(),
            down_token_id: "down".to_string(),
            up_bid: 0.10,
            up_ask: 0.90,
            down_bid: 0.10,
            down_ask: 0.90,
            btc_price,
            price_to_beat: 100.0,
            tau_seconds: 120.0,
            vol_per_sqrt_sec: 1.5,
            source: "test".to_string(),
        }
    }

    fn flat_fair_snapshot() -> FairSnapshot {
        let side = SideFair {
            shadow: 0.50,
            quote: 0.50,
            anchor_weight: 0.0,
            anchor_active: false,
        };
        FairSnapshot {
            model_up: 0.50,
            market_up: 0.50,
            up: side,
            down: side,
        }
    }

    #[test]
    fn send_quote_uses_remaining_live_room_when_above_exchange_minimum() {
        let mut cfg = flat_quote_test_cfg();
        cfg.dry_run = false;
        cfg.enable_real_orders = "I_UNDERSTAND_REAL_MONEY".to_string();
        cfg.quote_size = 20.0;
        cfg.inventory_mult = 4.0;
        cfg.max_unpaired_shares = 0.0;

        let frame = flat_frame(100.0);
        let inventory = Inventory {
            market: frame.market.clone(),
            up_shares: 66.0,
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();

        send_quote(
            &cfg,
            &tx,
            &frame,
            "Up",
            0.40,
            0.50,
            &inventory,
            "test",
            flat_fair_snapshot(),
        )
        .expect("quote");

        let quote = rx.try_recv().expect("remaining-room quote");
        assert_eq!(quote.size, 14.0);
    }

    #[test]
    fn send_quote_skips_remaining_live_room_below_exchange_minimum() {
        let mut cfg = flat_quote_test_cfg();
        cfg.dry_run = false;
        cfg.enable_real_orders = "I_UNDERSTAND_REAL_MONEY".to_string();
        cfg.quote_size = 20.0;
        cfg.inventory_mult = 4.0;
        cfg.max_unpaired_shares = 0.0;

        let frame = flat_frame(100.0);
        let inventory = Inventory {
            market: frame.market.clone(),
            up_shares: 76.0,
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();

        send_quote(
            &cfg,
            &tx,
            &frame,
            "Up",
            0.40,
            0.50,
            &inventory,
            "test",
            flat_fair_snapshot(),
        )
        .expect("quote");

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn value_buy_quotes_only_side_above_fair_filter() {
        let mut cfg = value_buy_quote_test_cfg();
        cfg.value_min_fair = 0.30;
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &flat_frame(110.0), &inventory, &mut tox).expect("quote");

        let quote = rx.try_recv().expect("value quote");
        assert_eq!(quote.side, "Up");
        assert_eq!(quote.reason, "value_buy");
        assert!(quote.price <= quote.fair - cfg.value_min_edge + 1e-9);
        assert!(rx.try_recv().is_err(), "Down is below VALUE_MIN_FAIR");
    }

    #[test]
    fn value_buy_quotes_both_sides_when_both_are_discounted() {
        let cfg = value_buy_quote_test_cfg();
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &flat_frame(100.0), &inventory, &mut tox).expect("quote");

        let q1 = rx.try_recv().expect("first value quote");
        let q2 = rx.try_recv().expect("second value quote");
        assert_eq!(q1.reason, "value_buy");
        assert_eq!(q2.reason, "value_buy");
        assert_ne!(q1.side, q2.side);
        assert!(q1.price <= q1.fair - cfg.value_min_edge + 1e-9);
        assert!(q2.price <= q2.fair - cfg.value_min_edge + 1e-9);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn market_anchor_can_weight_low_probability_side_more() {
        let mut cfg = value_buy_quote_test_cfg();
        cfg.enable_market_anchor = true;
        cfg.market_anchor_weight_high = 0.30;
        cfg.market_anchor_weight_low = 0.60;
        cfg.market_anchor_low_side_below = 0.50;
        cfg.market_anchor_max_spread = 0.12;

        let mut frame = flat_frame(100.0);
        frame.up_bid = 0.70;
        frame.up_ask = 0.72;
        frame.down_bid = 0.25;
        frame.down_ask = 0.27;

        let fair = fair_snapshot(&cfg, &frame, 0.80);

        assert!(
            fair.side_anchor_weight("Down") > fair.side_anchor_weight("Up"),
            "low-probability side should use the larger LOW anchor weight"
        );
        assert!(
            fair.quote_down() > 0.20,
            "low-probability side should move toward the live market fair"
        );
        assert!(
            fair.quote_up() > 0.70,
            "favorite side still stays anchored above the market favorite fair"
        );
    }

    #[test]
    fn value_buy_blocks_same_side_when_unpaired_cap_reached() {
        let mut cfg = value_buy_quote_test_cfg();
        cfg.value_min_fair = 0.30;
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            up_shares: 5.0,
            up_cost: 2.5,
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &flat_frame(110.0), &inventory, &mut tox).expect("quote");

        assert!(
            rx.try_recv().is_err(),
            "same-side add would push unpaired exposure above MAX_UNPAIRED_SHARES"
        );
    }

    #[test]
    fn value_buy_allows_opposite_side_to_reduce_unpaired_exposure() {
        let mut cfg = value_buy_quote_test_cfg();
        cfg.value_min_fair = 0.30;
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            up_shares: 20.0,
            up_cost: 10.0,
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &flat_frame(90.0), &inventory, &mut tox).expect("quote");

        let quote = rx.try_recv().expect("opposite-side reducing quote");
        assert_eq!(quote.side, "Down");
        assert_eq!(quote.reason, "value_buy");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn value_buy_pulls_quotes_in_final_seconds() {
        let cfg = value_buy_quote_test_cfg();
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        };
        let mut frame = flat_frame(100.0);
        frame.tau_seconds = 5.0;
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");

        assert!(
            rx.try_recv().is_err(),
            "final Pull phase must not open value buys"
        );
    }

    #[test]
    fn warmup_suppresses_all_quotes_in_young_window() {
        let mut cfg = flat_quote_test_cfg();
        cfg.quote_warmup_secs = 25.0;
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        // 10s into a 300s window (tau 290): inside warmup, nothing quoted.
        let mut frame = flat_frame(110.0);
        frame.tau_seconds = 290.0;
        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");
        assert!(rx.try_recv().is_err(), "warmup must suppress quotes");

        // 30s in (tau 270): warmup over, favorite quote resumes.
        frame.tau_seconds = 270.0;
        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");
        assert!(rx.try_recv().is_ok(), "quotes must resume after warmup");
    }
}
