//! Quote-engine thread: turns each `MarketFrame` into time-aware quotes and
//! sends `QuoteIntent`s to the order gateway. Reads (never writes) the shared
//! inventory.

use crate::config::Config;
use crate::ipc::{heartbeat, now_ms, Inventory, MarketFrame, QuoteIntent};
use crate::pricing::{
    digital_p_up, half_spread, in_warmup, market_maker_bids, phase_for, post_only_bid,
    price_sensitivity, time_boosted_skew, uncertainty_width, MmParams, ModelQuote, Phase,
    SpreadInputs, ToxicityMonitor,
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
    p_up: f64,
    model: ModelQuote,
) -> (Option<f64>, Option<f64>, &'static str) {
    if phase == Phase::Pull {
        return (None, None, "value_buy_pull");
    }
    let up_px = value_buy_px(cfg, frame, true, p_up, model.up_bid);
    let down_px = value_buy_px(cfg, frame, false, 1.0 - p_up, model.down_bid);
    (up_px, down_px, "value_buy")
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
    let p_up = digital_p_up(frame.btc_price, frame.price_to_beat, width);

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
    let (up_px, down_px, reason) = value_buy_quotes(cfg, frame, phase, p_up, model);
    if let Some(px) = up_px {
        send_quote(cfg, quote_tx, frame, "Up", px, p_up, inventory, reason)?;
    }
    if let Some(px) = down_px {
        send_quote(
            cfg,
            quote_tx,
            frame,
            "Down",
            px,
            1.0 - p_up,
            inventory,
            reason,
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
) -> AppResult<()> {
    let side_inventory = if side == "Up" {
        inventory.effective_up()
    } else {
        inventory.effective_down()
    };
    let left = (cfg.max_side_inventory() - side_inventory).floor();
    if left + 1e-9 < cfg.quote_size {
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
        size: cfg.quote_size.round().max(1.0),
        fair,
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
    fn value_buy_can_add_same_side_instead_of_forcing_pair() {
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

        let quote = rx.try_recv().expect("same-side value quote");
        assert_eq!(quote.side, "Up");
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
