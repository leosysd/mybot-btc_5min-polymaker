//! Quote-engine thread: turns each `MarketFrame` into time-aware quotes and
//! sends `QuoteIntent`s to the order gateway. Reads (never writes) the shared
//! inventory. ALL pricing/quoting logic is identical to the pre-refactor code;
//! only the transport (channel instead of socket) and the inventory source
//! (shared lock instead of a laggy inventory feed) changed.

use crate::config::Config;
use crate::ipc::{heartbeat, now_ms, Inventory, MarketFrame, QuoteIntent};
use crate::pricing::{
    digital_p_up, half_spread, lock_capped_bid, market_maker_bids, phase_for, post_only_bid,
    price_sensitivity, side_allowed, time_boosted_skew, uncertainty_width, MmParams, Phase,
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
                last_p_up =
                    handle_market_frame(&cfg, &quote_tx, &frame, &inv_snapshot, &mut tox)?;
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

fn rebalance_target_side(up_shares: f64, down_shares: f64) -> Option<bool> {
    const MIN_UNMATCHED_SHARES: f64 = 0.5;
    let unmatched = up_shares - down_shares;
    if unmatched > MIN_UNMATCHED_SHARES {
        Some(false) // long Up: first try buying Down to pair filled inventory
    } else if unmatched < -MIN_UNMATCHED_SHARES {
        Some(true) // long Down: first try buying Up to pair filled inventory
    } else {
        None
    }
}

#[allow(clippy::too_many_arguments)]
fn directional_edge_target_side(
    p_up: f64,
    up_px: Option<f64>,
    down_px: Option<f64>,
    up_shares: f64,
    down_shares: f64,
    quote_size: f64,
    max_unmatched: f64,
    min_edge: f64,
) -> Option<bool> {
    let side_is_up = p_up >= 0.5;
    let (fair, px, unmatched) = if side_is_up {
        (p_up, up_px?, (up_shares - down_shares).max(0.0))
    } else {
        (1.0 - p_up, down_px?, (down_shares - up_shares).max(0.0))
    };
    if max_unmatched <= 0.0 || unmatched + quote_size > max_unmatched + 1e-9 {
        return None;
    }
    (fair - px >= min_edge).then_some(side_is_up)
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
    let skew = time_boosted_skew(cfg.inventory_skew, tau, window, cfg.inventory_skew_time_boost);
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

    // ── 4b. SYMMETRIC cost-basis lock: cap BOTH sides by the opposite side's
    // average cost, so COMPLETING a pair can never exceed the lock budget
    // (1 - MIN_LOCK_EDGE). This prevents legging into a combined-cost > 1
    // pair (e.g. Down@0.53 then Up@0.50 = 1.03 guaranteed loss): once you
    // hold Down@0.53, the Up bid is capped at (1-edge)-0.53, so the 0.50 Up
    // buy is blocked. You stay directional on the side already held rather
    // than completing a losing pair. Data showed combined>1 is the steady
    // leak, so we cap both sides (the earlier asymmetric version let the
    // favorite load uncapped and produced those losing pairs).
    let up_capped = lock_capped_bid(
        true,
        model.up_bid,
        inventory.up_shares,
        inventory.up_cost,
        inventory.down_shares,
        inventory.down_cost,
        cfg.min_lock_edge,
    );
    let down_capped = lock_capped_bid(
        false,
        model.down_bid,
        inventory.up_shares,
        inventory.up_cost,
        inventory.down_shares,
        inventory.down_cost,
        cfg.min_lock_edge,
    );

    // ── 5. Endgame protocol + min-probability gate: a side is only quoted
    // if its win probability >= MIN_FAIR_TO_QUOTE (skip deep longshots).
    let phase = phase_for(tau, cfg.endgame_reduce_secs, cfg.endgame_pull_secs);
    let up_pair_px = if side_allowed(true, phase, up_eff, down_eff) {
        post_only_bid(
            up_capped,
            frame.up_ask,
            cfg.tick_size,
            cfg.min_bid,
            cfg.max_bid,
            cfg.post_only_margin_ticks,
        )
    } else {
        None
    };
    let down_pair_px = if side_allowed(false, phase, up_eff, down_eff) {
        post_only_bid(
            down_capped,
            frame.down_ask,
            cfg.tick_size,
            cfg.min_bid,
            cfg.max_bid,
            cfg.post_only_margin_ticks,
        )
    } else {
        None
    };
    let up_directional_px = if side_allowed(true, phase, up_eff, down_eff)
        && p_up >= cfg.min_fair_to_quote
    {
        post_only_bid(
            model.up_bid,
            frame.up_ask,
            cfg.tick_size,
            cfg.min_bid,
            cfg.max_bid,
            cfg.post_only_margin_ticks,
        )
    } else {
        None
    };
    let down_directional_px = if side_allowed(false, phase, up_eff, down_eff)
        && (1.0 - p_up) >= cfg.min_fair_to_quote
    {
        post_only_bid(
            model.down_bid,
            frame.down_ask,
            cfg.tick_size,
            cfg.min_bid,
            cfg.max_bid,
            cfg.post_only_margin_ticks,
        )
    } else {
        None
    };

    let rebalance_target = rebalance_target_side(inventory.up_shares, inventory.down_shares);
    let directional_target = if cfg.enable_directional_edge && rebalance_target.is_some() {
        directional_edge_target_side(
            p_up,
            up_directional_px,
            down_directional_px,
            inventory.up_shares,
            inventory.down_shares,
            cfg.quote_size,
            cfg.max_directional_inventory(),
            cfg.min_directional_edge,
        )
    } else {
        None
    };
    let (up_px, down_px, reason) = match (phase, rebalance_target) {
        (Phase::Pull, _) => (None, None, "tov3_pull"),
        (Phase::Normal, Some(true)) if up_pair_px.is_some() => {
            (up_pair_px, None, "tov3_rebalance_lock")
        }
        (Phase::Normal, Some(false)) if down_pair_px.is_some() => {
            (None, down_pair_px, "tov3_rebalance_lock")
        }
        (Phase::Normal, Some(_)) => match directional_target {
            Some(true) => (up_directional_px, None, "tov3_directional_edge"),
            Some(false) => (None, down_directional_px, "tov3_directional_edge"),
            None => (None, None, "tov3_wait_pair"),
        },
        (Phase::Normal, None) => {
            let up_px = up_pair_px.filter(|_| p_up >= cfg.min_fair_to_quote);
            let down_px = down_pair_px.filter(|_| (1.0 - p_up) >= cfg.min_fair_to_quote);
            (up_px, down_px, "tov3_normal")
        }
        (Phase::ReduceOnly, _) => (up_pair_px, down_pair_px, "tov3_reduce_only"),
    };

    if let Some(px) = up_px {
        send_quote(cfg, quote_tx, frame, "Up", px, p_up, inventory, reason)?;
    }
    if let Some(px) = down_px {
        send_quote(cfg, quote_tx, frame, "Down", px, 1.0 - p_up, inventory, reason)?;
    }
    // In Pull/ReduceOnly the pulled side simply isn't re-quoted; the gateway
    // expires its resting order by TTL (QUOTE_TTL_MS), flattening it.
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
    if left < 1.0 {
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
        size: cfg.quote_size.min(left).round().max(1.0),
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

    #[test]
    fn filled_imbalance_quotes_only_pairing_side() {
        assert_eq!(rebalance_target_side(5.0, 0.0), Some(false));
        assert_eq!(rebalance_target_side(0.0, 5.0), Some(true));
        assert_eq!(rebalance_target_side(5.0, 5.0), None);
    }

    #[test]
    fn directional_edge_is_opt_in_when_pairing_unavailable() {
        let mut cfg = Config::from_env().expect("config");
        cfg.enable_directional_edge = false;
        cfg.max_bid = 0.62;
        cfg.min_bid = 0.05;
        cfg.min_lock_edge = 0.02;
        cfg.quote_size = 5.0;
        cfg.inventory_mult = 2.0;
        cfg.directional_inventory_mult = 2.0;
        cfg.min_directional_edge = 0.04;
        cfg.min_fair_to_quote = 0.0;
        cfg.post_only_margin_ticks = 1.0;
        cfg.tick_size = 0.01;

        let frame = MarketFrame {
            ts_ms: now_ms(),
            market: "btc-updown-5m-test".to_string(),
            condition_id: "condition".to_string(),
            up_token_id: "up".to_string(),
            down_token_id: "down".to_string(),
            up_ask: 0.90,
            down_ask: 0.01,
            btc_price: 101.0,
            price_to_beat: 100.0,
            tau_seconds: 20.0,
            vol_per_sqrt_sec: 0.1,
            source: "test".to_string(),
        };
        let inventory = Inventory {
            market: frame.market.clone(),
            up_shares: 5.0,
            up_cost: 3.20,
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");
        assert!(
            rx.try_recv().is_err(),
            "default should wait for pair instead of adding directional inventory"
        );
    }

    #[test]
    fn directional_edge_requires_winner_edge_and_room() {
        assert_eq!(
            directional_edge_target_side(0.70, Some(0.64), Some(0.20), 5.0, 0.0, 5.0, 10.0, 0.04),
            Some(true)
        );
        assert_eq!(
            directional_edge_target_side(0.70, Some(0.68), Some(0.20), 5.0, 0.0, 5.0, 10.0, 0.04),
            None,
            "edge below threshold"
        );
        assert_eq!(
            directional_edge_target_side(0.70, Some(0.64), Some(0.20), 10.0, 0.0, 5.0, 10.0, 0.04),
            None,
            "directional unmatched cap reached"
        );
        assert_eq!(
            directional_edge_target_side(0.30, Some(0.20), Some(0.64), 0.0, 5.0, 5.0, 10.0, 0.04),
            Some(false)
        );
    }
}
