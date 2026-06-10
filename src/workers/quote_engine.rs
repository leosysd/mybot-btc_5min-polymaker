//! Quote-engine thread: turns each `MarketFrame` into time-aware quotes and
//! sends `QuoteIntent`s to the order gateway. Reads (never writes) the shared
//! inventory. ALL pricing/quoting logic is identical to the pre-refactor code;
//! only the transport (channel instead of socket) and the inventory source
//! (shared lock instead of a laggy inventory feed) changed.

use crate::config::Config;
use crate::ipc::{heartbeat, now_ms, Inventory, MarketFrame, QuoteIntent};
use crate::pricing::{
    digital_p_up, fair_capped_bid, half_spread, in_warmup, lock_capped_bid, market_maker_bids,
    phase_for, post_only_bid, price_sensitivity, side_allowed, time_boosted_skew,
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

fn pair_lock_mode(cfg: &Config) -> String {
    cfg.pair_lock_mode.to_ascii_uppercase()
}

fn held_side_prob(pair_side_is_up: bool, p_up: f64) -> f64 {
    if pair_side_is_up {
        1.0 - p_up // buying Up means the existing unmatched side is Down
    } else {
        p_up // buying Down means the existing unmatched side is Up
    }
}

fn pair_lock_profit(pair_side_is_up: bool, pair_px: f64, inventory: &Inventory) -> Option<f64> {
    let opposite_avg = if pair_side_is_up {
        (inventory.down_shares > 0.0).then_some(inventory.down_cost / inventory.down_shares)
    } else {
        (inventory.up_shares > 0.0).then_some(inventory.up_cost / inventory.up_shares)
    }?;
    Some(1.0 - opposite_avg - pair_px)
}

fn pair_lock_allows(
    cfg: &Config,
    pair_side_is_up: bool,
    pair_px: f64,
    inventory: &Inventory,
    p_up: f64,
    tau: f64,
) -> bool {
    match pair_lock_mode(cfg).as_str() {
        "ALWAYS" => true,
        "OFF" => false,
        "EDGE_ONLY" => {
            if tau > cfg.pair_lock_last_secs {
                return false;
            }
            let Some(lock_profit) = pair_lock_profit(pair_side_is_up, pair_px, inventory) else {
                return false;
            };
            if lock_profit + 1e-9 < cfg.pair_lock_min_profit {
                return false;
            }
            held_side_prob(pair_side_is_up, p_up) < cfg.hybrid_endgame_min_prob
        }
        _ => false,
    }
}

fn use_hybrid_maker(cfg: &Config) -> bool {
    cfg.strategy_mode.eq_ignore_ascii_case("HYBRID_MAKER")
        || cfg.strategy_mode.eq_ignore_ascii_case("TAKER_BRAIN_MAKER")
}

fn use_taker_brain_maker(cfg: &Config) -> bool {
    cfg.strategy_mode.eq_ignore_ascii_case("TAKER_BRAIN_MAKER")
}

fn valid_market_px(px: f64) -> bool {
    px.is_finite() && px > 0.0 && px <= 1.0
}

fn market_up_mid(frame: &MarketFrame) -> Option<f64> {
    let up_exact = valid_market_px(frame.up_bid)
        .then_some(frame.up_bid)
        .zip(valid_market_px(frame.up_ask).then_some(frame.up_ask))
        .map(|(bid, ask)| (bid + ask) / 2.0);
    let down_exact = valid_market_px(frame.down_bid)
        .then_some(frame.down_bid)
        .zip(valid_market_px(frame.down_ask).then_some(frame.down_ask))
        .map(|(bid, ask)| 1.0 - ((bid + ask) / 2.0));
    match (up_exact, down_exact) {
        (Some(up), Some(down)) => Some(((up + down) / 2.0).clamp(0.0, 1.0)),
        (Some(up), None) => Some(up.clamp(0.0, 1.0)),
        (None, Some(down)) => Some(down.clamp(0.0, 1.0)),
        (None, None) => None,
    }
}

fn top_bid(frame: &MarketFrame, side_is_up: bool) -> Option<f64> {
    let bid = if side_is_up {
        frame.up_bid
    } else {
        frame.down_bid
    };
    valid_market_px(bid).then_some(bid)
}

#[allow(clippy::too_many_arguments)]
fn hybrid_entry_px(
    cfg: &Config,
    frame: &MarketFrame,
    side_is_up: bool,
    fair: f64,
    model_bid: f64,
) -> Option<f64> {
    if fair < cfg.min_fair_to_quote {
        return None;
    }
    let edge_cap = fair - cfg.hybrid_entry_min_edge;
    if edge_cap < cfg.min_bid {
        return None;
    }
    let book_improved = top_bid(frame, side_is_up)
        .map(|bid| bid + cfg.hybrid_entry_aggression_ticks * cfg.tick_size)
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

#[allow(clippy::too_many_arguments)]
fn hybrid_flat_quotes(
    cfg: &Config,
    frame: &MarketFrame,
    phase: Phase,
    tau: f64,
    p_up: f64,
    model: ModelQuote,
    up_pair_px: Option<f64>,
    down_pair_px: Option<f64>,
    allow_range_lock: bool,
) -> (Option<f64>, Option<f64>, &'static str) {
    let favorite_is_up = p_up >= 0.5;
    let favorite_prob = if favorite_is_up { p_up } else { 1.0 - p_up };

    let trend_min_prob = if tau <= cfg.hybrid_endgame_secs {
        cfg.hybrid_endgame_min_prob
    } else {
        cfg.hybrid_trend_min_prob
    };
    let market_edge_ok = market_up_mid(frame)
        .map(|market_up| {
            let market_prob = if favorite_is_up {
                market_up
            } else {
                1.0 - market_up
            };
            favorite_prob - market_prob >= cfg.hybrid_trend_min_market_edge
        })
        .unwrap_or(false);

    if favorite_prob >= trend_min_prob && market_edge_ok {
        let (fair, bid) = if favorite_is_up {
            (p_up, model.up_bid)
        } else {
            (1.0 - p_up, model.down_bid)
        };
        let px = hybrid_entry_px(cfg, frame, favorite_is_up, fair, bid);
        return if favorite_is_up {
            (px, None, "tov3_hybrid_trend")
        } else {
            (None, px, "tov3_hybrid_trend")
        };
    }

    if allow_range_lock && phase == Phase::Normal && favorite_prob <= cfg.hybrid_range_max_prob {
        if let (Some(up), Some(down)) = (up_pair_px, down_pair_px) {
            if up + down <= 1.0 - cfg.min_lock_edge + 1e-9 {
                return (Some(up), Some(down), "tov3_hybrid_range_lock");
            }
        }
    }

    (None, None, "tov3_hybrid_wait")
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

    // ── 4c. Fair cap on pairing bids: never pay more than fair + premium to
    // complete a pair. Without this, holding the near-certain winner makes the
    // engine bid the lock cap (e.g. 0.55) for the nearly-worthless side — an
    // order that only fills when it converts a ~sure win into a tiny lock.
    let up_capped = fair_capped_bid(up_capped, p_up, cfg.rebalance_max_over_fair);
    let down_capped = fair_capped_bid(down_capped, 1.0 - p_up, cfg.rebalance_max_over_fair);

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
    let up_directional_px =
        if side_allowed(true, phase, up_eff, down_eff) && p_up >= cfg.min_fair_to_quote {
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
    let down_directional_px =
        if side_allowed(false, phase, up_eff, down_eff) && (1.0 - p_up) >= cfg.min_fair_to_quote {
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

    let raw_rebalance_target = rebalance_target_side(inventory.up_shares, inventory.down_shares);
    let rebalance_target = raw_rebalance_target.filter(|side_is_up| {
        let pair_px = if *side_is_up {
            up_pair_px
        } else {
            down_pair_px
        };
        pair_px
            .map(|px| pair_lock_allows(cfg, *side_is_up, px, inventory, p_up, tau))
            .unwrap_or(false)
    });
    let hold_unpaired = raw_rebalance_target.is_some() && rebalance_target.is_none();
    let directional_target = if cfg.enable_directional_edge && raw_rebalance_target.is_some() {
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
    let (up_px, down_px, reason) = if use_hybrid_maker(cfg) {
        match (phase, hold_unpaired, rebalance_target) {
            (Phase::Pull, _, _) => (None, None, "tov3_pull"),
            (_, true, _) => (None, None, "tov3_hybrid_hold_single"),
            (_, _, Some(true)) if up_pair_px.is_some() => {
                (up_pair_px, None, "tov3_hybrid_pair_lock")
            }
            (_, _, Some(false)) if down_pair_px.is_some() => {
                (None, down_pair_px, "tov3_hybrid_pair_lock")
            }
            (_, _, Some(_)) => (None, None, "tov3_hybrid_wait_pair"),
            (Phase::Normal | Phase::ReduceOnly, _, None) => hybrid_flat_quotes(
                cfg,
                frame,
                phase,
                tau,
                p_up,
                model,
                up_pair_px,
                down_pair_px,
                !use_taker_brain_maker(cfg),
            ),
        }
    } else {
        match (phase, hold_unpaired, rebalance_target) {
            (Phase::Pull, _, _) => (None, None, "tov3_pull"),
            (_, true, _) => (None, None, "tov3_hold_single"),
            (Phase::Normal, _, Some(true)) if up_pair_px.is_some() => {
                (up_pair_px, None, "tov3_rebalance_lock")
            }
            (Phase::Normal, _, Some(false)) if down_pair_px.is_some() => {
                (None, down_pair_px, "tov3_rebalance_lock")
            }
            (Phase::Normal, _, Some(_)) => match directional_target {
                Some(true) => (up_directional_px, None, "tov3_directional_edge"),
                Some(false) => (None, down_directional_px, "tov3_directional_edge"),
                None => (None, None, "tov3_wait_pair"),
            },
            (Phase::Normal, _, None) => {
                let up_px = up_pair_px.filter(|_| p_up >= cfg.min_fair_to_quote);
                let down_px = down_pair_px.filter(|_| (1.0 - p_up) >= cfg.min_fair_to_quote);
                if cfg.favorite_first_flat {
                    if p_up >= 0.5 {
                        (up_px, None, "tov3_favorite_first")
                    } else {
                        (None, down_px, "tov3_favorite_first")
                    }
                } else {
                    (up_px, down_px, "tov3_normal")
                }
            }
            (Phase::ReduceOnly, _, _) => (up_pair_px, down_pair_px, "tov3_reduce_only"),
        }
    };

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

    #[test]
    fn filled_imbalance_quotes_only_pairing_side() {
        assert_eq!(rebalance_target_side(5.0, 0.0), Some(false));
        assert_eq!(rebalance_target_side(0.0, 5.0), Some(true));
        assert_eq!(rebalance_target_side(5.0, 5.0), None);
    }

    fn flat_quote_test_cfg() -> Config {
        let mut cfg = Config::from_env().expect("config");
        cfg.strategy_mode = "LEGACY_V3".to_string();
        cfg.favorite_first_flat = true;
        cfg.max_bid = 0.92;
        cfg.min_bid = 0.05;
        cfg.min_lock_edge = 0.02;
        cfg.quote_size = 5.0;
        cfg.inventory_mult = 2.0;
        cfg.min_fair_to_quote = 0.0;
        cfg.post_only_margin_ticks = 1.0;
        cfg.tick_size = 0.01;
        cfg.base_half_spread = 0.01;
        cfg.min_half_spread = 0.0;
        cfg.max_half_spread = 0.25;
        cfg.latency_sec = 0.0;
        cfg.k_adverse = 0.0;
        cfg
    }

    fn hybrid_quote_test_cfg() -> Config {
        let mut cfg = flat_quote_test_cfg();
        cfg.strategy_mode = "HYBRID_MAKER".to_string();
        cfg.hybrid_trend_min_prob = 0.60;
        cfg.hybrid_trend_min_market_edge = 0.02;
        cfg.hybrid_range_max_prob = 0.58;
        cfg.hybrid_entry_min_edge = 0.02;
        cfg.hybrid_entry_aggression_ticks = 1.0;
        cfg.hybrid_endgame_secs = 45.0;
        cfg.hybrid_endgame_min_prob = 0.70;
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
    fn favorite_first_flat_quotes_only_higher_fair_side() {
        let cfg = flat_quote_test_cfg();
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &flat_frame(110.0), &inventory, &mut tox).expect("quote");

        let quote = rx.try_recv().expect("favorite quote");
        assert_eq!(quote.side, "Up");
        assert_eq!(quote.reason, "tov3_favorite_first");
        assert!(rx.try_recv().is_err(), "underdog side should not be quoted");
    }

    #[test]
    fn favorite_first_flat_can_choose_down() {
        let cfg = flat_quote_test_cfg();
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &flat_frame(90.0), &inventory, &mut tox).expect("quote");

        let quote = rx.try_recv().expect("favorite quote");
        assert_eq!(quote.side, "Down");
        assert_eq!(quote.reason, "tov3_favorite_first");
        assert!(rx.try_recv().is_err(), "underdog side should not be quoted");
    }

    #[test]
    fn hybrid_range_lock_quotes_both_sides_when_choppy() {
        let cfg = hybrid_quote_test_cfg();
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &flat_frame(100.0), &inventory, &mut tox).expect("quote");

        let q1 = rx.try_recv().expect("first range quote");
        let q2 = rx.try_recv().expect("second range quote");
        assert_eq!(q1.reason, "tov3_hybrid_range_lock");
        assert_eq!(q2.reason, "tov3_hybrid_range_lock");
        assert_ne!(q1.side, q2.side, "range mode should quote both sides");
        assert!(q1.price + q2.price <= 1.0 - cfg.min_lock_edge + 1e-9);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn taker_brain_waits_in_chop_instead_of_range_locking() {
        let mut cfg = hybrid_quote_test_cfg();
        cfg.strategy_mode = "TAKER_BRAIN_MAKER".to_string();
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &flat_frame(100.0), &inventory, &mut tox).expect("quote");

        assert!(
            rx.try_recv().is_err(),
            "taker brain should not double-quote a choppy market"
        );
    }

    #[test]
    fn taker_brain_still_quotes_favorite_when_trend_has_edge() {
        let mut cfg = hybrid_quote_test_cfg();
        cfg.strategy_mode = "TAKER_BRAIN_MAKER".to_string();
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        };
        let mut frame = flat_frame(105.0);
        frame.up_bid = 0.50;
        frame.up_ask = 0.54;
        frame.down_bid = 0.44;
        frame.down_ask = 0.48;
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");

        let quote = rx.try_recv().expect("trend quote");
        assert_eq!(quote.side, "Up");
        assert_eq!(quote.reason, "tov3_hybrid_trend");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn hybrid_trend_quotes_only_favorite_with_market_edge() {
        let cfg = hybrid_quote_test_cfg();
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        };
        let mut frame = flat_frame(105.0);
        frame.up_bid = 0.50;
        frame.up_ask = 0.54;
        frame.down_bid = 0.44;
        frame.down_ask = 0.48;
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");

        let quote = rx.try_recv().expect("trend quote");
        assert_eq!(quote.side, "Up");
        assert_eq!(quote.reason, "tov3_hybrid_trend");
        assert!(quote.price <= quote.fair - cfg.hybrid_entry_min_edge + 1e-9);
        assert!(
            rx.try_recv().is_err(),
            "trend mode should not quote the weak side"
        );
    }

    #[test]
    fn hybrid_waits_when_model_has_no_market_edge() {
        let cfg = hybrid_quote_test_cfg();
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        };
        let mut frame = flat_frame(105.0);
        frame.up_bid = 0.60;
        frame.up_ask = 0.64;
        frame.down_bid = 0.36;
        frame.down_ask = 0.40;
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");
        assert!(rx.try_recv().is_err(), "no edge and not range: wait");
    }

    #[test]
    fn hybrid_imbalance_only_quotes_lockable_pairing_side() {
        let cfg = hybrid_quote_test_cfg();
        let mut frame = flat_frame(101.0);
        frame.vol_per_sqrt_sec = 0.17;
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            up_shares: 5.0,
            up_cost: 2.15,
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");

        let quote = rx.try_recv().expect("pairing quote");
        assert_eq!(quote.side, "Down");
        assert_eq!(quote.reason, "tov3_hybrid_pair_lock");
        assert!(rx.try_recv().is_err(), "must not add more Up while long Up");
    }

    #[test]
    fn edge_only_pair_lock_holds_when_existing_side_still_strong() {
        let mut cfg = hybrid_quote_test_cfg();
        cfg.pair_lock_mode = "EDGE_ONLY".to_string();
        cfg.pair_lock_min_profit = 0.01;
        let mut frame = flat_frame(101.0);
        frame.vol_per_sqrt_sec = 0.17;
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            up_shares: 5.0,
            up_cost: 2.15,
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");

        assert!(
            rx.try_recv().is_err(),
            "edge-only mode should hold a still-strong single-side position"
        );
    }

    #[test]
    fn edge_only_pair_lock_does_not_pair_in_the_middle() {
        let mut cfg = hybrid_quote_test_cfg();
        cfg.pair_lock_mode = "EDGE_ONLY".to_string();
        cfg.pair_lock_min_profit = 0.01;
        let mut frame = flat_frame(100.0);
        frame.tau_seconds = 120.0;
        frame.vol_per_sqrt_sec = 0.17;
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            up_shares: 5.0,
            up_cost: 2.15,
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");

        assert!(
            rx.try_recv().is_err(),
            "edge-only mode should not force a middle-window pair"
        );
    }

    #[test]
    fn edge_only_pair_lock_pairs_when_edge_fades_and_lock_profit_is_good() {
        let mut cfg = hybrid_quote_test_cfg();
        cfg.pair_lock_mode = "EDGE_ONLY".to_string();
        cfg.pair_lock_min_profit = 0.01;
        let mut frame = flat_frame(100.0);
        frame.tau_seconds = 20.0;
        frame.vol_per_sqrt_sec = 0.17;
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            up_shares: 5.0,
            up_cost: 2.15,
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");

        let quote = rx.try_recv().expect("edge-only pairing quote");
        assert_eq!(quote.side, "Down");
        assert_eq!(quote.reason, "tov3_hybrid_pair_lock");
        assert!(
            1.0 - 0.43 - quote.price >= cfg.pair_lock_min_profit - 1e-9,
            "pairing must lock enough profit"
        );
        assert!(rx.try_recv().is_err());
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

    #[test]
    fn rebalance_never_overpays_for_nearly_dead_side() {
        let mut cfg = flat_quote_test_cfg();
        cfg.min_bid = 0.10;
        cfg.rebalance_max_over_fair = 0.05;
        // Holding Up@0.43 with BTC far above strike and tiny vol: p_up ~ 1,
        // fair Down ~ 0. The lock cap alone would allow a Down bid up to
        // 0.98-0.43=0.55; the fair cap must suppress the quote instead.
        let mut frame = flat_frame(110.0);
        frame.tau_seconds = 120.0;
        frame.vol_per_sqrt_sec = 0.05;
        let inventory = Inventory {
            market: "btc-updown-5m-test".to_string(),
            up_shares: 5.0,
            up_cost: 2.15,
            ..Default::default()
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let mut tox = ToxicityMonitor::new(2_500, 0.5, 1.5, 0.08);

        handle_market_frame(&cfg, &tx, &frame, &inventory, &mut tox).expect("quote");
        assert!(
            rx.try_recv().is_err(),
            "must not bid up to the lock cap for a ~worthless side"
        );

        // Same inventory but the opposite side still has real value
        // (p_up ~ 0.7): pairing bid survives, capped near fair.
        let mut live_frame = flat_frame(101.0);
        live_frame.tau_seconds = 120.0;
        live_frame.vol_per_sqrt_sec = 0.17;
        handle_market_frame(&cfg, &tx, &live_frame, &inventory, &mut tox).expect("quote");
        let quote = rx.try_recv().expect("pairing quote should survive");
        assert_eq!(quote.side, "Down");
        assert_eq!(quote.reason, "tov3_rebalance_lock");
        assert!(
            quote.price <= quote.fair + cfg.rebalance_max_over_fair + 1e-9,
            "pairing bid {} must respect fair {} + premium",
            quote.price,
            quote.fair
        );
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
            up_bid: 0.10,
            up_ask: 0.90,
            down_bid: 0.01,
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
