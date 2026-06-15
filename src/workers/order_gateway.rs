//! Order-gateway thread: the SOLE writer of order-driven inventory state.
//!
//! It receives `QuoteIntent`s, applies the per-side cap and cost-basis lock
//! against the ONE shared inventory (filled + pending, always current because
//! this thread — and its own user-WS child thread — are the only writers),
//! places/cancels/expires resting orders, and reconciles orphans. Every change
//! to `resting` is reflected into the shared inventory's pending immediately
//! under the lock, so the cap can never be beaten by lag/churn.

use crate::config::Config;
use crate::ipc::{
    heartbeat, now_ms, write_json, FillEvent, Inventory, OrderAccepted, OrderCancelled, QuoteIntent,
};
use crate::pricing::floor_to_tick;
use crate::real_orders::{RealOrderClient, UserWsCredentials};
use crate::AppResult;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tungstenite::{connect, Message};

use super::collector::parse_f64_value;
use super::state::{
    held_shares, is_ws_timeout, opposite_avg_cost, pending_shares, recompute_pending_from_resting,
    reset_inventory_for_market, sleep_ms, stopping, tune_ws_socket, GatewayEvent, GatewayRx,
    GatewayTx, LedgerEvent, LedgerTx, QuoteMeta, QuoteRx, ReconcileGate, RestingOrder,
    SharedInventory, SharedOrderMap, StopFlag,
};

const RECV_TIMEOUT: Duration = Duration::from_millis(250);
/// Max time placement stays paused awaiting reconcile before self-clearing, so a
/// reconcile that can't run (e.g. tokens not yet learned) never deadlocks us.
const RECONCILE_PAUSE_TIMEOUT: Duration = Duration::from_millis(5_000);

#[allow(clippy::too_many_arguments)]
pub fn run(
    cfg: Config,
    stop: StopFlag,
    inventory: SharedInventory,
    quote_rx: QuoteRx,
    ledger_tx: LedgerTx,
    reconcile_gate: ReconcileGate,
) -> AppResult<()> {
    let mut rng = super::state::FastRng::new(now_ms());
    let real_orders = if cfg.real_orders_enabled() {
        Some(RealOrderClient::connect(&cfg)?)
    } else {
        None
    };
    let mut resting = if real_orders.is_some() {
        load_active_orders(&cfg)?
    } else {
        HashMap::new()
    };
    let order_map: SharedOrderMap = Arc::new(Mutex::new(order_map_from_resting(&resting)));

    // Internal channel: the user-WS child thread tells the main loop to drop /
    // shrink the matching resting order after it has already credited the fill
    // (or registered a cancel) into the shared inventory.
    let (gw_tx, gw_rx): (GatewayTx, GatewayRx) = mpsc::channel();

    if let Some(real_orders) = &real_orders {
        if !resting.is_empty() {
            heartbeat(
                &cfg,
                "order-gateway",
                "startup cancel previous active orders",
            )?;
            cancel_all_resting_orders(
                &cfg,
                &inventory,
                real_orders,
                &order_map,
                &mut resting,
                &ledger_tx,
                "startup_cleanup",
            )?;
        }
        // Authoritative sweep: cancel any order the exchange still has open that
        // we don't know about locally (state file lost, or crashed after placing
        // before persisting). Belt-and-suspenders against orphaned live orders.
        match real_orders.cancel_all() {
            Ok(n) => heartbeat(
                &cfg,
                "order-gateway",
                format!("startup exchange sweep canceled {n}"),
            )?,
            Err(err) => heartbeat(
                &cfg,
                "order-gateway",
                format!("startup exchange sweep failed: {err}"),
            )?,
        }
        start_user_channel(
            &cfg,
            Arc::clone(&stop),
            real_orders.user_ws_credentials(),
            Arc::clone(&inventory),
            Arc::clone(&order_map),
            gw_tx.clone(),
            ledger_tx.clone(),
        )?;
    }

    // Sync initial pending into the shared inventory so the cap starts correct.
    if !resting.is_empty() {
        let mut inv = inventory.lock().unwrap();
        if let Some(any) = resting.values().next() {
            reset_inventory_for_market(&mut inv, &any.market);
        }
        recompute_pending_from_resting(&mut inv, &resting);
    }

    let mut last_reconcile = Instant::now();
    let mut last_prewarm = Instant::now();
    let mut primed: HashSet<String> = HashSet::new();
    // Per-side cooldown after a rejected order, so we don't spin and hammer the
    // exchange with crossing orders (and risk rate-limiting).
    let mut reject_until: HashMap<String, u64> = HashMap::new();
    // When set, placement is paused (an unmatched fill is awaiting reconcile).
    let mut reconcile_pause_since: Option<Instant> = None;

    let loop_result = (|| -> AppResult<()> {
        while !stopping(&stop, &cfg) {
            // Drain internal events from the user-WS thread first: it already
            // credited fills / registered cancels into the shared inventory; we
            // just update the resting map and recompute pending here.
            while let Ok(event) = gw_rx.try_recv() {
                apply_gateway_event(&cfg, &inventory, real_orders.as_ref(), &mut resting, event)?;
            }

            expire_resting_orders(
                &cfg,
                &inventory,
                real_orders.as_ref(),
                &order_map,
                &mut resting,
                &ledger_tx,
            )?;

            if let Some(real_orders) = real_orders.as_ref() {
                // Periodic exchange reconciliation: cancel orphaned orders.
                if cfg.reconcile_interval_ms > 0
                    && last_reconcile.elapsed() >= Duration::from_millis(cfg.reconcile_interval_ms)
                {
                    reconcile_orphan_orders(&cfg, real_orders, &resting)?;
                    last_reconcile = Instant::now();
                }
                // Keep the CLOB connection hot (Cloudflare cuts idle ~90s).
                if cfg.enable_prewarm
                    && cfg.prewarm_interval_ms > 0
                    && last_prewarm.elapsed() >= Duration::from_millis(cfg.prewarm_interval_ms)
                {
                    real_orders.prewarm();
                    last_prewarm = Instant::now();
                }
            }

            // P1: placement pause after an unmatched fill, until risk reconciles
            // (self-clearing after a timeout so it can never deadlock placement).
            let placement_paused = if reconcile_gate.load(Ordering::Relaxed) {
                match reconcile_pause_since {
                    None => {
                        reconcile_pause_since = Some(Instant::now());
                        true
                    }
                    Some(t) if t.elapsed() < RECONCILE_PAUSE_TIMEOUT => true,
                    Some(_) => {
                        reconcile_gate.store(false, Ordering::Relaxed);
                        reconcile_pause_since = None;
                        heartbeat(
                            &cfg,
                            "order-gateway",
                            "unmatched-fill pause timed out, resuming placement",
                        )?;
                        false
                    }
                }
            } else {
                reconcile_pause_since = None;
                false
            };

            match quote_rx.recv_timeout(RECV_TIMEOUT) {
                Ok(first) => {
                    // P1: coalesce the UNBOUNDED queue to the FRESHEST quote per
                    // side, and reject any that already went stale while queued.
                    // Real order I/O (place/cancel/prewarm) can block for seconds
                    // (we have seen post=2046ms), so old quotes pile up behind it;
                    // acting on a 1s-old price in a 5-min BTC market is dangerous.
                    let mut latest: HashMap<String, QuoteIntent> = HashMap::new();
                    let mut queued = vec![first];
                    while let Ok(q) = quote_rx.try_recv() {
                        queued.push(q);
                    }
                    for q in queued {
                        match latest.get(&q.side) {
                            Some(p) if p.ts_ms >= q.ts_ms => {}
                            _ => {
                                latest.insert(q.side.clone(), q);
                            }
                        }
                    }
                    // Channel drained above (no backlog); if paused, place nothing.
                    if placement_paused {
                        heartbeat(
                            &cfg,
                            "order-gateway",
                            "placement paused: awaiting reconcile after unmatched fill",
                        )?;
                        continue;
                    }
                    for (_qside, mut quote) in latest.into_iter() {
                        let age = now_ms().saturating_sub(quote.ts_ms);
                        if age > cfg.stale_after_ms {
                            heartbeat(
                                &cfg,
                                "order-gateway",
                                format!("skipped stale quote {} age={age}ms", quote.side),
                            )?;
                            continue;
                        }
                        // First time we see a new window's token: warm its metadata
                        // cache + connection so the first order's build/post are fast.
                        if let Some(real_orders) = real_orders.as_ref() {
                            if cfg.enable_prewarm
                                && !quote.token_id.trim().is_empty()
                                && primed.insert(quote.token_id.clone())
                            {
                                real_orders.prime_token(&quote.token_id);
                                real_orders.prewarm();
                                last_prewarm = Instant::now();
                            }
                        }

                        // New market window: reset the shared inventory for it so the
                        // cap and cost-basis lock measure against the right book.
                        {
                            let mut inv = inventory.lock().unwrap();
                            reset_inventory_for_market(&mut inv, &quote.market);
                            recompute_pending_from_resting(&mut inv, &resting);
                        }
                        // Drop order_map entries from other windows (incl. spent
                        // echo guards) so the map stays bounded and a stale-market
                        // entry can't mislead fill matching.
                        order_map
                            .lock()
                            .unwrap()
                            .retain(|_, m| m.market == quote.market);

                        // Gateway-authoritative side cap: never let filled+pending on
                        // this side exceed QUOTE_SIZE*INVENTORY_MULT. Because this
                        // thread is the sole writer of pending/fills (synchronously),
                        // held+pending is always current here — this is what stops the
                        // over-accumulation runaway.
                        let replacing_size = resting
                            .get(&quote.side)
                            .filter(|order| order.market == quote.market)
                            .map(|order| order.size)
                            .unwrap_or(0.0);
                        let (held, pending, opp_avg) = {
                            let inv = inventory.lock().unwrap();
                            (
                                held_shares(&inv, &quote.market, &quote.side),
                                pending_shares(&inv, &quote.market, &quote.side),
                                opposite_avg_cost(&inv, &quote.market, &quote.side),
                            )
                        };
                        // Counts OUTSTANDING orders, so churn/lag cannot exceed the cap.
                        let side_cap_room =
                            cfg.max_side_inventory() - held - (pending - replacing_size).max(0.0);
                        let room = side_cap_room.floor();
                        // If only partial cap room remains, place the leftover
                        // as long as it satisfies the exchange minimum size.
                        let min_order_size = cfg.min_order_size();
                        if room + 1e-9 < min_order_size {
                            heartbeat(
                            &cfg,
                            "order-gateway",
                            format!(
                                "side cap reached {} held={:.0} pending={:.0} room={:.0} (min {:.0})",
                                quote.side, held, pending, room, min_order_size
                            ),
                        )?;
                            continue;
                        }
                        if room + 1e-9 < quote.size {
                            let old_size = quote.size;
                            quote.size = room.floor().max(min_order_size);
                            heartbeat(
                                &cfg,
                                "order-gateway",
                                format!(
                                    "side cap shrinks {} size {:.0}->{:.0} room={:.0}",
                                    quote.side, old_size, quote.size, room
                                ),
                            )?;
                        }

                        let asym_room = {
                            let inv = inventory.lock().unwrap();
                            asymmetric_balance_room_after_replace(
                                &cfg,
                                &inv,
                                &quote.market,
                                &quote.side,
                                replacing_size,
                            )
                            .floor()
                        };
                        if asym_room + 1e-9 < min_order_size {
                            heartbeat(
                                &cfg,
                                "order-gateway",
                                format!(
                                    "asymmetric balance blocks {} size={:.0} ratio={:.2}",
                                    quote.side, quote.size, cfg.balance_secondary_ratio
                                ),
                            )?;
                            continue;
                        }
                        if asym_room + 1e-9 < quote.size {
                            let old_size = quote.size;
                            quote.size = asym_room.floor().max(min_order_size);
                            heartbeat(
                                &cfg,
                                "order-gateway",
                                format!(
                                    "asymmetric balance shrinks {} size {:.0}->{:.0} ratio={:.2}",
                                    quote.side, old_size, quote.size, cfg.balance_secondary_ratio
                                ),
                            )?;
                        }

                        let unpaired_allowed = {
                            let inv = inventory.lock().unwrap();
                            unpaired_limit_allows_quote_after_replace(
                                &cfg,
                                &inv,
                                &quote.market,
                                &quote.side,
                                quote.size,
                                replacing_size,
                            )
                        };
                        if !unpaired_allowed {
                            heartbeat(
                                &cfg,
                                "order-gateway",
                                format!(
                                    "unpaired cap blocks {} size={:.0} limit={:.0}",
                                    quote.side, quote.size, cfg.max_unpaired_shares
                                ),
                            )?;
                            continue;
                        }

                        // Gateway-authoritative cost-basis lock: cap THIS leg's price
                        // by the OPPOSITE side's real average fill, so completing a
                        // pair can never breach 1 - MIN_LOCK_EDGE.
                        if let Some(opp) = opp_avg {
                            let cap = floor_to_tick(1.0 - cfg.min_lock_edge - opp, cfg.tick_size);
                            if quote.price > cap {
                                if cap < cfg.min_bid {
                                    heartbeat(
                                        &cfg,
                                        "order-gateway",
                                        format!(
                                            "lock blocks {} (opp avg {:.3}, cap {:.3} < min_bid)",
                                            quote.side, opp, cap
                                        ),
                                    )?;
                                    continue;
                                }
                                heartbeat(
                                    &cfg,
                                    "order-gateway",
                                    format!(
                                        "lock caps {} {:.3}->{:.3} (opp avg {:.3})",
                                        quote.side, quote.price, cap, opp
                                    ),
                                )?;
                                quote.price = cap;
                            }
                        }

                        // Backing off this side after a recent rejection.
                        if reject_until
                            .get(&quote.side)
                            .is_some_and(|&until| now_ms() < until)
                        {
                            continue;
                        }
                        if should_keep_resting(&cfg, resting.get(&quote.side), &quote) {
                            heartbeat(&cfg, "order-gateway", "kept resting quote")?;
                            continue;
                        }
                        if resting.contains_key(&quote.side)
                            && !cancel_resting_side(
                                &cfg,
                                &inventory,
                                real_orders.as_ref(),
                                Some(&order_map),
                                &mut resting,
                                &ledger_tx,
                                &quote.side,
                                "requote",
                            )?
                        {
                            // Old order not confirmed cancelled — it may still be live.
                            // Don't place a second order on this side (would risk a
                            // double-fill past the cap); retry on the next cycle.
                            continue;
                        }
                        if !accept_resting_order(
                            &cfg,
                            &inventory,
                            real_orders.as_ref(),
                            Some(&order_map),
                            &mut resting,
                            &ledger_tx,
                            quote.clone(),
                        )? {
                            // Not placed (rejected / too small): cool this side down.
                            if real_orders.is_some() && cfg.reject_backoff_ms > 0 {
                                reject_until
                                    .insert(quote.side.clone(), now_ms() + cfg.reject_backoff_ms);
                            }
                            continue;
                        }
                        if real_orders.is_none() && rng.next_unit() <= cfg.sim_fill_chance {
                            // Dry-run simulated fill: credit the shared inventory and
                            // drop the resting order, then recompute pending.
                            simulate_fill(&inventory, &mut resting, &ledger_tx, &quote)?;
                            heartbeat(&cfg, "order-gateway", "simulated fill")?;
                        } else if real_orders.is_some() {
                            heartbeat(&cfg, "order-gateway", "real order resting")?;
                        } else {
                            heartbeat(&cfg, "order-gateway", "resting quote")?;
                        }
                    } // for each freshest-per-side quote
                }
                Err(RecvTimeoutError::Timeout) => {
                    heartbeat(&cfg, "order-gateway", "waiting")?;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        Ok(())
    })();

    let cleanup_result = if let Some(real_orders) = &real_orders {
        cancel_all_resting_orders(
            &cfg,
            &inventory,
            real_orders,
            &order_map,
            &mut resting,
            &ledger_tx,
            "shutdown",
        )
    } else {
        Ok(())
    };
    match (loop_result, cleanup_result) {
        (Err(err), _) => Err(err),
        (Ok(()), Err(err)) => Err(err),
        (Ok(()), Ok(())) => Ok(()),
    }
}

/// Apply an event from the user-WS child thread to the resting map. The shared
/// inventory's FILLED state was already updated by that thread; here we shrink /
/// remove the resting order and recompute pending so the cap is consistent.
fn apply_gateway_event(
    cfg: &Config,
    inventory: &SharedInventory,
    real_orders: Option<&RealOrderClient>,
    resting: &mut HashMap<String, RestingOrder>,
    event: GatewayEvent,
) -> AppResult<()> {
    let changed = match &event {
        GatewayEvent::Fill(fill) => apply_fill_to_resting_map(resting, fill),
        GatewayEvent::Cancelled(cancel) => apply_cancel_to_resting_map(resting, cancel),
    };
    if changed {
        persist_active_orders_if_real(cfg, real_orders, resting)?;
        let mut inv = inventory.lock().unwrap();
        recompute_pending_from_resting(&mut inv, resting);
        let label = match event {
            GatewayEvent::Fill(_) => "resting fill synced",
            GatewayEvent::Cancelled(_) => "resting cancel synced",
        };
        heartbeat(cfg, "order-gateway", label)?;
    }
    Ok(())
}

fn simulate_fill(
    inventory: &SharedInventory,
    resting: &mut HashMap<String, RestingOrder>,
    ledger_tx: &LedgerTx,
    quote: &QuoteIntent,
) -> AppResult<()> {
    resting.remove(&quote.side);
    {
        let mut inv = inventory.lock().unwrap();
        reset_inventory_for_market(&mut inv, &quote.market);
        inv.add_fill(&quote.market, &quote.side, quote.price, quote.size);
        recompute_pending_from_resting(&mut inv, resting);
    }
    let fill = FillEvent {
        quote_id: quote.quote_id.clone(),
        ts_ms: now_ms(),
        market: quote.market.clone(),
        side: quote.side.clone(),
        price: quote.price,
        size: quote.size,
        inventory_up: 0.0,
        inventory_down: 0.0,
        pnl_if_up: 0.0,
        pnl_if_down: 0.0,
        source: "dry_run_gateway".to_string(),
    };
    let _ = ledger_tx.send(LedgerEvent::Filled(fill));
    Ok(())
}

fn should_keep_resting(cfg: &Config, old: Option<&RestingOrder>, quote: &QuoteIntent) -> bool {
    let Some(old) = old else {
        return false;
    };
    if old.expires_ts_ms <= now_ms() {
        return false;
    }
    if old.market != quote.market
        || old.condition_id != quote.condition_id
        || old.token_id != quote.token_id
    {
        return false;
    }
    let threshold = cfg.tick_size * cfg.requote_threshold_ticks.max(0.0);
    (old.price - quote.price).abs() < threshold && (old.size - quote.size).abs() < 0.001
}

fn unpaired_limit_allows_quote_after_replace(
    cfg: &Config,
    inv: &Inventory,
    market: &str,
    side: &str,
    quote_size: f64,
    replacing_size: f64,
) -> bool {
    let limit = cfg.max_unpaired_shares;
    if limit <= 0.0 || quote_size <= 0.0 {
        return true;
    }
    let current_up = held_shares(inv, market, "Up") + pending_shares(inv, market, "Up");
    let current_down = held_shares(inv, market, "Down") + pending_shares(inv, market, "Down");
    let current = current_up - current_down;
    let (projected_up, projected_down) = match side {
        "Up" => (
            (current_up - replacing_size).max(0.0) + quote_size,
            current_down,
        ),
        "Down" => (
            current_up,
            (current_down - replacing_size).max(0.0) + quote_size,
        ),
        _ => (current_up, current_down),
    };
    let projected = projected_up - projected_down;
    projected.abs() <= limit + 1e-9 || projected.abs() < current.abs()
}

fn asymmetric_balance_room_after_replace(
    cfg: &Config,
    inv: &Inventory,
    market: &str,
    side: &str,
    replacing_size: f64,
) -> f64 {
    if !cfg.enable_asymmetric_balance || cfg.balance_secondary_ratio >= 1.0 {
        return f64::INFINITY;
    }
    let (same_filled, same_pending_after_replace, other_filled) = match side {
        "Up" => (
            held_shares(inv, market, "Up"),
            (pending_shares(inv, market, "Up") - replacing_size).max(0.0),
            held_shares(inv, market, "Down"),
        ),
        "Down" => (
            held_shares(inv, market, "Down"),
            (pending_shares(inv, market, "Down") - replacing_size).max(0.0),
            held_shares(inv, market, "Up"),
        ),
        _ => return f64::INFINITY,
    };
    if same_filled >= other_filled {
        return f64::INFINITY;
    }
    (other_filled * cfg.balance_secondary_ratio - same_filled - same_pending_after_replace).max(0.0)
}

fn load_active_orders(cfg: &Config) -> AppResult<HashMap<String, RestingOrder>> {
    Ok(
        crate::ipc::read_json::<HashMap<String, RestingOrder>>(&cfg.active_orders_path())?
            .unwrap_or_default(),
    )
}

fn persist_active_orders(cfg: &Config, resting: &HashMap<String, RestingOrder>) -> AppResult<()> {
    if resting.is_empty() {
        let _ = std::fs::remove_file(cfg.active_orders_path());
        return Ok(());
    }
    write_json(&cfg.active_orders_path(), resting)
}

fn persist_active_orders_if_real(
    cfg: &Config,
    real_orders: Option<&RealOrderClient>,
    resting: &HashMap<String, RestingOrder>,
) -> AppResult<()> {
    if real_orders.is_some() {
        persist_active_orders(cfg, resting)?;
    }
    Ok(())
}

fn order_map_from_resting(resting: &HashMap<String, RestingOrder>) -> HashMap<String, QuoteMeta> {
    resting
        .values()
        .filter_map(|order| {
            let order_id = order.exchange_order_id.as_ref()?;
            Some((
                order_id.clone(),
                QuoteMeta {
                    quote_id: order.quote_id.clone(),
                    market: order.market.clone(),
                    condition_id: order.condition_id.clone(),
                    side: order.side.clone(),
                    price: order.price,
                    size: order.size,
                    credited: false,
                    done: false,
                },
            ))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn accept_resting_order(
    cfg: &Config,
    inventory: &SharedInventory,
    real_orders: Option<&RealOrderClient>,
    order_map: Option<&SharedOrderMap>,
    resting: &mut HashMap<String, RestingOrder>,
    ledger_tx: &LedgerTx,
    mut quote: QuoteIntent,
) -> AppResult<bool> {
    if cfg.live_order_notional_cap > 0.0 && quote.price > 0.0 {
        let capped_size = (cfg.live_order_notional_cap / quote.price).floor();
        if capped_size + 1e-9 < cfg.min_order_size() {
            heartbeat(cfg, "order-gateway", "skipped live notional cap")?;
            return Ok(false);
        }
        quote.size = quote.size.min(capped_size);
    }
    let exchange_order_id = if let Some(real_orders) = real_orders {
        // None = exchange rejected this quote (e.g. post-only crosses book).
        // Skip it WITHOUT resting/accounting and keep the gateway running.
        let Some(ack) = real_orders.place_buy_limit(&quote)? else {
            heartbeat(
                cfg,
                "order-gateway",
                format!(
                    "order rejected, skipped {} @ {:.2}",
                    quote.side, quote.price
                ),
            )?;
            return Ok(false);
        };
        heartbeat(
            cfg,
            "order-gateway",
            format!(
                "real order {} {} build={}ms sign={}ms post={}ms 总={}ms",
                ack.order_id, ack.status, ack.build_ms, ack.sign_ms, ack.post_ms, ack.total_ms
            ),
        )?;
        if let Some(order_map) = order_map {
            order_map.lock().unwrap().insert(
                ack.order_id.clone(),
                QuoteMeta {
                    quote_id: quote.quote_id.clone(),
                    market: quote.market.clone(),
                    condition_id: quote.condition_id.clone(),
                    side: quote.side.clone(),
                    price: quote.price,
                    size: quote.size,
                    credited: false,
                    done: false,
                },
            );
        }
        Some(ack.order_id)
    } else {
        None
    };
    let expires_ts_ms = now_ms() + cfg.quote_ttl_ms;
    let order = RestingOrder {
        quote_id: quote.quote_id.clone(),
        exchange_order_id,
        market: quote.market.clone(),
        condition_id: quote.condition_id.clone(),
        token_id: quote.token_id.clone(),
        side: quote.side.clone(),
        price: quote.price,
        size: quote.size,
        expires_ts_ms,
    };
    let accepted = OrderAccepted {
        accepted_ts_ms: now_ms(),
        expires_ts_ms,
        quote: quote.clone(),
        source: if real_orders.is_some() {
            "polymarket_clob".to_string()
        } else {
            "dry_run_gateway".to_string()
        },
    };
    resting.insert(quote.side.clone(), order);
    persist_active_orders_if_real(cfg, real_orders, resting)?;
    // The order is LIVE now: reflect it in the shared inventory's pending under
    // the lock BEFORE making any further decision. This is the synchronous
    // state update that makes the cap reliable.
    {
        let mut inv = inventory.lock().unwrap();
        recompute_pending_from_resting(&mut inv, resting);
    }
    // Tell risk for jsonl logging + kill switch + inventory.json persistence.
    let _ = ledger_tx.send(LedgerEvent::Accepted(accepted));
    Ok(true)
}

fn expire_resting_orders(
    cfg: &Config,
    inventory: &SharedInventory,
    real_orders: Option<&RealOrderClient>,
    order_map: &SharedOrderMap,
    resting: &mut HashMap<String, RestingOrder>,
    ledger_tx: &LedgerTx,
) -> AppResult<()> {
    let now = now_ms();
    let expired = resting
        .iter()
        .filter_map(|(side, order)| (order.expires_ts_ms <= now).then_some(side.clone()))
        .collect::<Vec<_>>();
    for side in expired {
        cancel_resting_side(
            cfg,
            inventory,
            real_orders,
            Some(order_map),
            resting,
            ledger_tx,
            &side,
            "expired",
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn cancel_resting_side(
    cfg: &Config,
    inventory: &SharedInventory,
    real_orders: Option<&RealOrderClient>,
    order_map: Option<&SharedOrderMap>,
    resting: &mut HashMap<String, RestingOrder>,
    ledger_tx: &LedgerTx,
    side: &str,
    reason: &str,
) -> AppResult<bool> {
    let Some(order) = resting.get(side).cloned() else {
        return Ok(true);
    };
    // Only release pending (remove from resting) on a CONFIRMED-terminal cancel.
    // If the cancel is not confirmed — actively rejected OR a transient I/O error
    // (routine from Dublin under requote churn) — the order may still be live, so
    // keep it counted (conservative for the cap) and DO NOT crash the loop. The
    // periodic orphan reconcile cancels any exchange-open order we lose track of.
    // Returns false so a requote skips placing a second order on this side.
    match send_cancel(cfg, real_orders, order_map, &order, ledger_tx, reason) {
        Ok(CancelOutcome::Cancelled) => {
            // It did not fill — just release pending.
            resting.remove(side);
            persist_active_orders_if_real(cfg, real_orders, resting)?;
            let mut inv = inventory.lock().unwrap();
            recompute_pending_from_resting(&mut inv, resting);
            Ok(true)
        }
        Ok(CancelOutcome::GoneUnverified) => handle_gone_unverified_cancel(
            cfg,
            inventory,
            real_orders,
            order_map,
            resting,
            ledger_tx,
            side,
            reason,
            &order,
        ),
        Err(err) => {
            // Not confirmed terminal (rejected / transient I/O) — the order may
            // still be live. Keep it counted (conservative for the cap) and do
            // NOT crash the loop; the orphan reconcile cleans up if needed.
            heartbeat(
                cfg,
                "order-gateway",
                format!("cancel {reason} unconfirmed, keeping {side} order counted: {err}"),
            )?;
            Ok(false)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_gone_unverified_cancel(
    cfg: &Config,
    inventory: &SharedInventory,
    real_orders: Option<&RealOrderClient>,
    order_map: Option<&SharedOrderMap>,
    resting: &mut HashMap<String, RestingOrder>,
    ledger_tx: &LedgerTx,
    side: &str,
    reason: &str,
    order: &RestingOrder,
) -> AppResult<bool> {
    // The exchange says the order is no longer open, but the cancel response
    // does not tell us the real fill size/price. Do NOT invent a fill from
    // local remaining size; release pending and force the authoritative
    // position reconciler to correct held shares.
    mark_order_reconcile_pending(order_map, order);
    resting.remove(side);
    persist_active_orders_if_real(cfg, real_orders, resting)?;
    let mut inv = inventory.lock().unwrap();
    recompute_pending_from_resting(&mut inv, resting);
    drop(inv);
    let _ = ledger_tx.send(LedgerEvent::UnmatchedFill);
    heartbeat(
        cfg,
        "order-gateway",
        format!("cancel {reason} found {side} gone; awaiting reconcile"),
    )?;
    // Skip placing a replacement in the same quote cycle; the shared inventory
    // may under-count until risk-ledger reconciles.
    Ok(false)
}

/// Cancel any order the exchange reports as open that we don't track locally.
/// Catches orphans (state file lost, or placed-but-not-recorded after a crash).
/// Only UNKNOWN ids are cancelled — known ones are managed by the normal flow.
fn reconcile_orphan_orders(
    cfg: &Config,
    real_orders: &RealOrderClient,
    resting: &HashMap<String, RestingOrder>,
) -> AppResult<()> {
    let open = match real_orders.open_order_ids() {
        Ok(ids) => ids,
        Err(err) => {
            heartbeat(
                cfg,
                "order-gateway",
                format!("reconcile fetch failed: {err}"),
            )?;
            return Ok(());
        }
    };
    if open.is_empty() {
        return Ok(());
    }
    let known: HashSet<&str> = resting
        .values()
        .filter_map(|order| order.exchange_order_id.as_deref())
        .collect();
    let mut orphans = 0usize;
    for id in &open {
        if known.contains(id.as_str()) {
            continue;
        }
        match real_orders.cancel_order(id) {
            Ok(_) => orphans += 1,
            Err(err) => heartbeat(
                cfg,
                "order-gateway",
                format!("reconcile cancel {id} failed: {err}"),
            )?,
        }
    }
    if orphans > 0 {
        heartbeat(
            cfg,
            "order-gateway",
            format!("reconcile canceled {orphans} orphan order(s)"),
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn cancel_all_resting_orders(
    cfg: &Config,
    inventory: &SharedInventory,
    real_orders: &RealOrderClient,
    order_map: &SharedOrderMap,
    resting: &mut HashMap<String, RestingOrder>,
    ledger_tx: &LedgerTx,
    reason: &str,
) -> AppResult<()> {
    let sides = resting.keys().cloned().collect::<Vec<_>>();
    let mut first_error = None;
    for side in sides {
        if let Err(err) = cancel_resting_side(
            cfg,
            inventory,
            Some(real_orders),
            Some(order_map),
            resting,
            ledger_tx,
            &side,
            reason,
        ) {
            let msg = err.to_string();
            let _ = heartbeat(
                cfg,
                "order-gateway",
                format!("cancel {reason} failed: {msg}"),
            );
            if first_error.is_none() {
                first_error = Some(msg);
            }
        }
    }
    if let Some(err) = first_error {
        return Err(format!("one or more cancels failed: {err}").into());
    }
    Ok(())
}

/// Returns Ok(true) if the cancel reached a CONFIRMED-terminal state (canceled,
/// already-gone, filled, etc.) so the caller may release the order's pending.
/// Returns Err if the cancel was actively rejected (order may still be live).
/// Result of attempting to cancel a resting order.
enum CancelOutcome {
    /// We confirmed the exchange cancelled it (it did NOT fill).
    Cancelled,
    /// The order is gone but NOT because we cancelled it (exchange reports
    /// already filled / matched / not found). The response is not precise
    /// enough for local accounting, so the caller must reconcile.
    GoneUnverified,
}

fn send_cancel(
    cfg: &Config,
    real_orders: Option<&RealOrderClient>,
    order_map: Option<&SharedOrderMap>,
    order: &RestingOrder,
    ledger_tx: &LedgerTx,
    reason: &str,
) -> AppResult<CancelOutcome> {
    let mut outcome = CancelOutcome::Cancelled;
    if let (Some(real_orders), Some(exchange_order_id)) =
        (real_orders, order.exchange_order_id.as_deref())
    {
        let ack = real_orders.cancel_order(exchange_order_id)?;
        if !ack.is_terminal_noop() {
            // Not confirmed terminal — the order may still be live. Bubble up so
            // the caller keeps it counted (conservative) and does not crash.
            return Err(format!(
                "Polymarket cancel rejected for {exchange_order_id}: {}",
                ack.reason
                    .clone()
                    .unwrap_or_else(|| "unknown reason".to_string())
            )
            .into());
        }
        if ack.canceled {
            // Genuinely cancelled by us: it did not fill. KEEP the order_map entry
            // as an echo guard (mark done) instead of removing it. Removing it
            // orphaned any fill that lands AFTER the cancel (common under fast
            // requote churn): such a late trade event would be classified
            // "unknown" -> UnmatchedFill -> placement pause (the stutter). Marking
            // done lets match_and_reduce_order recognise the late event as
            // already-accounted (benign); a genuinely missed partial is still
            // recovered by the periodic on-chain reconcile. The entry is pruned
            // per window on market change. order_map is only used for fill
            // matching, so this does NOT affect pending/cap accounting.
            heartbeat(
                cfg,
                "order-gateway",
                format!("real cancel {exchange_order_id}"),
            )?;
            if let Some(order_map) = order_map {
                if let Some(meta) = order_map.lock().unwrap().get_mut(exchange_order_id) {
                    meta.done = true;
                }
            }
        } else {
            // Gone but we did NOT cancel it => it filled, vanished, or the API no
            // longer exposes it. Do not synthesize a fill here; the caller will
            // release pending and force an on-chain position reconcile.
            heartbeat(
                cfg,
                "order-gateway",
                format!(
                    "cancel noop {exchange_order_id}: {} (await reconcile)",
                    ack.reason.unwrap_or_else(|| "not active".to_string())
                ),
            )?;
            outcome = CancelOutcome::GoneUnverified;
        }
    }
    // Only record a Cancelled event for a genuine cancel; GoneUnverified is
    // reconciled by risk-ledger instead of being logged as a synthetic fill.
    if matches!(outcome, CancelOutcome::Cancelled) {
        let cancel = OrderCancelled {
            ts_ms: now_ms(),
            quote_id: order.quote_id.clone(),
            market: order.market.clone(),
            side: order.side.clone(),
            size: order.size,
            reason: reason.to_string(),
            source: if real_orders.is_some() {
                "polymarket_clob".to_string()
            } else {
                "dry_run_gateway".to_string()
            },
        };
        let _ = ledger_tx.send(LedgerEvent::Cancelled(cancel));
    }
    Ok(outcome)
}

/// Mark an ambiguous cancel as handed off to reconcile. We deliberately do not
/// credit local held inventory here because the cancel response does not carry
/// authoritative fill size/price. The order-map entry is kept as an echo guard:
/// if the user-WS later replays this order, it will be ignored instead of being
/// double-counted after reconcile.
fn mark_order_reconcile_pending(order_map: Option<&SharedOrderMap>, order: &RestingOrder) {
    let Some(order_map) = order_map else {
        return;
    };
    let Some(id) = order.exchange_order_id.as_deref() else {
        return;
    };
    let mut map = order_map.lock().unwrap();
    if let Some(meta) = map.get_mut(id) {
        meta.credited = true;
        meta.done = true;
        meta.size = 0.0;
    }
}

fn apply_fill_to_resting_map(
    resting: &mut HashMap<String, RestingOrder>,
    fill: &FillEvent,
) -> bool {
    // A fill may only reduce a resting order in the SAME market. The quote_id
    // branch previously skipped this check, so a late fill from the previous
    // window could shrink the current window's resting order -> pending under-
    // counts -> the side cap re-opens -> over-accumulation.
    let key = resting.iter().find_map(|(side, order)| {
        (order.market == fill.market
            && ((!fill.quote_id.is_empty() && order.quote_id == fill.quote_id)
                || (fill.quote_id.is_empty() && order.side == fill.side)))
            .then(|| side.clone())
    });
    let Some(key) = key else {
        return false;
    };

    let mut remove = false;
    if let Some(order) = resting.get_mut(&key) {
        let fill_size = if fill.size > 0.0 {
            fill.size.min(order.size)
        } else {
            order.size
        };
        order.size = (order.size - fill_size).max(0.0);
        remove = order.size <= 0.001;
    }
    if remove {
        resting.remove(&key);
    }
    true
}

fn apply_cancel_to_resting_map(
    resting: &mut HashMap<String, RestingOrder>,
    cancel: &OrderCancelled,
) -> bool {
    let key = resting.iter().find_map(|(side, order)| {
        ((!cancel.quote_id.is_empty() && order.quote_id == cancel.quote_id)
            || (cancel.quote_id.is_empty()
                && order.market == cancel.market
                && order.side == cancel.side))
            .then(|| side.clone())
    });
    let Some(key) = key else {
        return false;
    };
    resting.remove(&key);
    true
}

// ── user-WS thread ────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn start_user_channel(
    cfg: &Config,
    stop: StopFlag,
    credentials: UserWsCredentials,
    inventory: SharedInventory,
    order_map: SharedOrderMap,
    gw_tx: GatewayTx,
    ledger_tx: LedgerTx,
) -> AppResult<()> {
    let cfg = cfg.clone();
    thread::spawn(move || {
        user_channel_loop(
            cfg,
            stop,
            credentials,
            inventory,
            order_map,
            gw_tx,
            ledger_tx,
        )
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn user_channel_loop(
    cfg: Config,
    stop: StopFlag,
    credentials: UserWsCredentials,
    inventory: SharedInventory,
    order_map: SharedOrderMap,
    gw_tx: GatewayTx,
    ledger_tx: LedgerTx,
) {
    // Trade dedup MUST persist across reconnects: when the user WS drops and
    // reconnects, the exchange can replay recent (partial) fills. A per-
    // connection set would forget them and double-book. Keep it here.
    let mut seen_trades: HashSet<String> = HashSet::new();
    while !stopping(&stop, &cfg) {
        if let Err(err) = user_channel_once(
            &cfg,
            &stop,
            &credentials,
            &inventory,
            &order_map,
            &gw_tx,
            &ledger_tx,
            &mut seen_trades,
        ) {
            let _ = heartbeat(&cfg, "order-gateway", format!("user ws reconnect: {err}"));
            sleep_ms(1_000);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn user_channel_once(
    cfg: &Config,
    stop: &StopFlag,
    credentials: &UserWsCredentials,
    inventory: &SharedInventory,
    order_map: &SharedOrderMap,
    gw_tx: &GatewayTx,
    ledger_tx: &LedgerTx,
    seen_trades: &mut HashSet<String>,
) -> AppResult<()> {
    let (mut socket, _) = connect(cfg.polymarket_user_ws_url.as_str())?;
    tune_ws_socket(&mut socket, Duration::from_millis(250))?;

    let markets = known_condition_ids(order_map);
    let mut sub = serde_json::json!({
        "auth": {
            "apiKey": credentials.api_key.as_str(),
            "secret": credentials.secret.as_str(),
            "passphrase": credentials.passphrase.as_str(),
        },
        "type": "user"
    });
    if !markets.is_empty() {
        if let Value::Object(obj) = &mut sub {
            obj.insert("markets".to_string(), serde_json::json!(markets));
        }
    }
    socket.send(Message::Text(sub.to_string()))?;
    heartbeat(cfg, "order-gateway", "user ws subscribed")?;

    // Bound memory: trade ids accumulate forever otherwise. Replays only concern
    // RECENT trades, so dropping very old ids is safe.
    if seen_trades.len() > 100_000 {
        seen_trades.clear();
    }
    let mut subscribed = known_condition_ids(order_map)
        .into_iter()
        .collect::<HashSet<_>>();
    let mut last_ping = Instant::now();
    let mut last_subscribe_check = Instant::now();

    while !stopping(stop, cfg) {
        if last_ping.elapsed() >= Duration::from_secs(8) {
            socket.send(Message::Text("PING".to_string()))?;
            last_ping = Instant::now();
        }
        if last_subscribe_check.elapsed() >= Duration::from_millis(250) {
            let next = known_condition_ids(order_map);
            let fresh = next
                .into_iter()
                .filter(|condition_id| subscribed.insert(condition_id.clone()))
                .collect::<Vec<_>>();
            if !fresh.is_empty() {
                let update = serde_json::json!({
                    "operation": "subscribe",
                    "markets": fresh
                });
                socket.send(Message::Text(update.to_string()))?;
                heartbeat(cfg, "order-gateway", "user ws market subscribe")?;
            }
            last_subscribe_check = Instant::now();
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
                    apply_user_channel_message(
                        cfg,
                        inventory,
                        order_map,
                        gw_tx,
                        ledger_tx,
                        seen_trades,
                        &value,
                    )?;
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

fn known_condition_ids(order_map: &SharedOrderMap) -> Vec<String> {
    let mut seen = HashSet::new();
    order_map
        .lock()
        .unwrap()
        .values()
        .filter_map(|meta| {
            let condition_id = meta.condition_id.trim();
            (!condition_id.is_empty() && seen.insert(condition_id.to_string()))
                .then(|| condition_id.to_string())
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn apply_user_channel_message(
    cfg: &Config,
    inventory: &SharedInventory,
    order_map: &SharedOrderMap,
    gw_tx: &GatewayTx,
    ledger_tx: &LedgerTx,
    seen_trades: &mut HashSet<String>,
    value: &Value,
) -> AppResult<()> {
    if let Some(items) = value.as_array() {
        for item in items {
            apply_user_channel_message(
                cfg,
                inventory,
                order_map,
                gw_tx,
                ledger_tx,
                seen_trades,
                item,
            )?;
        }
        return Ok(());
    }

    let event_type = first_str(value, &["event_type"])
        .or_else(|| first_str(value, &["type"]))
        .unwrap_or_default()
        .to_ascii_lowercase();
    if event_type == "trade" {
        handle_user_trade(
            cfg,
            inventory,
            order_map,
            gw_tx,
            ledger_tx,
            seen_trades,
            value,
        )?;
    } else if event_type == "order" {
        handle_user_order(cfg, order_map, gw_tx, ledger_tx, value)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_user_trade(
    cfg: &Config,
    inventory: &SharedInventory,
    order_map: &SharedOrderMap,
    gw_tx: &GatewayTx,
    ledger_tx: &LedgerTx,
    seen_trades: &mut HashSet<String>,
    value: &Value,
) -> AppResult<()> {
    let trade_id = first_str(value, &["id", "trade_id", "transaction_hash"])
        .filter(|id| !id.trim().is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| {
            format!(
                "{}:{}",
                value
                    .get("timestamp")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                now_ms()
            )
        });

    if let Some(makers) = value.get("maker_orders").and_then(Value::as_array) {
        // Condition ids of the orders we currently hold = the live window. Used
        // to window-guard a real-time credit of our own untracked fill.
        let current_conditions: HashSet<String> =
            known_condition_ids(order_map).into_iter().collect();
        let event_condition =
            first_str(value, &["market", "condition_id"]).map(ToString::to_string);
        // TEMP DIAGNOSTIC (remove after capture): per-trade, count OUR makers and
        // how many are tracked in order_map. Decides the under-count cause:
        //  - ours_not_in_map > 0 in busy windows => placement race (handler fix).
        //  - ours far below chain fills for the window => WS delivery gap (need a
        //    placement-quantity cap instead). Logs counts only, no ids/wallets.
        {
            use std::sync::atomic::{AtomicU64, Ordering};
            static DIAG2: AtomicU64 = AtomicU64::new(0);
            if DIAG2.fetch_add(1, Ordering::Relaxed) < 600 {
                let mut ours = 0u32;
                let mut ours_in_map = 0u32;
                for m in makers {
                    if maker_order_belongs_to_us(cfg, m) {
                        ours += 1;
                        let oid = first_str(m, &["order_id", "id"]).unwrap_or("");
                        if order_map_contains(order_map, oid) {
                            ours_in_map += 1;
                        }
                    }
                }
                eprintln!(
                    "[DIAG2] evt_market={:?} makers={} ours={} ours_in_map={} ours_not_in_map={} cur_conds={} evt_cond_in_cur={}",
                    event_condition,
                    makers.len(),
                    ours,
                    ours_in_map,
                    ours - ours_in_map,
                    current_conditions.len(),
                    event_condition
                        .as_deref()
                        .map(|c| current_conditions.contains(c))
                        .unwrap_or(false),
                );
            }
        }
        let mut handled_any = false;
        let mut unmatched_hint: Option<(String, f64, f64)> = None;
        for maker in makers {
            let Some(order_id) = first_str(maker, &["order_id", "id"]) else {
                continue;
            };
            let size = parse_f64_value(maker.get("matched_amount"))
                .or_else(|| parse_f64_value(maker.get("size")))
                .or_else(|| parse_f64_value(value.get("size")))
                .unwrap_or(0.0);
            let price = parse_f64_value(maker.get("price"))
                .or_else(|| parse_f64_value(value.get("price")))
                .unwrap_or(0.0);

            // Known order: credit via the order-map path (unchanged).
            if order_map_contains(order_map, order_id) {
                let key = format!("{trade_id}:{order_id}");
                if !seen_trades.insert(key) {
                    continue;
                }
                handled_any = true;
                emit_user_fill(
                    cfg, inventory, order_map, gw_tx, ledger_tx, order_id, price, size,
                )?;
                continue;
            }

            // Unknown order id. Remember the first one as an ownership-independent
            // fallback: if we end up accounting NOTHING from this trade, we still
            // force a reconcile after the loop (so an own fill is never silently
            // dropped, even if the owner field doesn't identify us).
            if unmatched_hint.is_none() {
                unmatched_hint = Some((order_id.to_string(), price, size));
            }
            // Other traders' makers are skipped. For OUR OWN fill that we lost
            // track of — the placement race where the fill beats the order-map
            // insert (POST returns, order is live and fills before we record its
            // id) — credit it in REAL TIME from this event. This is the only
            // source fast enough: the on-chain reconcile lags (Data API
            // indexing), so relying on it lets `held` under-count and the side
            // cap over-accumulate (observed real Up=74 vs cap 40).
            if !maker_order_belongs_to_us(cfg, maker) {
                continue;
            }
            let key = format!("{trade_id}:{order_id}");
            if !seen_trades.insert(key) {
                continue;
            }
            let side = first_str(maker, &["outcome"])
                .map(ToString::to_string)
                .filter(|s| s == "Up" || s == "Down");
            // Only credit when we can place the fill in the CURRENT window:
            // its condition id is one we hold orders in (or we hold none yet —
            // the first-order race). Otherwise fall back to a reconcile.
            let same_window = side.is_some()
                && match &event_condition {
                    Some(c) => current_conditions.is_empty() || current_conditions.contains(c),
                    None => current_conditions.is_empty(),
                };
            match (side, same_window) {
                (Some(side), true) => {
                    credit_owned_fill_direct(cfg, inventory, gw_tx, ledger_tx, &side, price, size)?;
                    handled_any = true;
                }
                _ => {
                    if unmatched_hint.is_none() {
                        unmatched_hint = Some((order_id.to_string(), price, size));
                    }
                }
            }
        }
        // Couldn't credit our own fill directly (no side / wrong window): force a
        // reconcile so held still catches up rather than silently under-counting.
        if !handled_any {
            if let Some((order_id, price, size)) = unmatched_hint {
                let key = format!("{trade_id}:unmatched");
                if seen_trades.insert(key) {
                    emit_user_fill(
                        cfg, inventory, order_map, gw_tx, ledger_tx, &order_id, price, size,
                    )?;
                }
            }
        }
        return Ok(());
    }

    for key in ["maker_order_id", "order_id", "taker_order_id"] {
        let Some(order_id) = first_str(value, &[key]) else {
            continue;
        };
        let seen_key = format!("{trade_id}:{order_id}");
        if !seen_trades.insert(seen_key) {
            continue;
        }
        let price = parse_f64_value(value.get("price")).unwrap_or(0.0);
        let size = parse_f64_value(value.get("size")).unwrap_or(0.0);
        emit_user_fill(
            cfg, inventory, order_map, gw_tx, ledger_tx, order_id, price, size,
        )?;
    }
    Ok(())
}

fn order_map_contains(order_map: &SharedOrderMap, order_id: &str) -> bool {
    order_map.lock().unwrap().contains_key(order_id)
}

/// True if a maker entry in a user-WS trade is OUR order. The user channel is
/// authenticated with our api key, so our maker rows carry our `owner` (api key)
/// or `maker_address` (funder). Lets us credit an own fill whose order id we
/// have not recorded yet (placement race) without misattributing a counterparty's.
fn maker_order_belongs_to_us(cfg: &Config, maker: &Value) -> bool {
    let api_key = cfg.poly_api_key.trim();
    if !api_key.is_empty()
        && first_str(maker, &["owner", "api_key", "apiKey"])
            .is_some_and(|o| o.eq_ignore_ascii_case(api_key))
    {
        return true;
    }
    let funder = cfg.poly_funder_address.trim();
    !funder.is_empty()
        && first_str(maker, &["maker_address", "makerAddress"])
            .is_some_and(|a| a.eq_ignore_ascii_case(funder))
}

/// Credit an own fill straight into the shared inventory from the user-WS event,
/// without an order-map match. Used when the fill beat the order-map insert
/// (placement race). `held` becomes accurate immediately, so the side cap holds
/// even though the chain reconcile lags. Credits only to the CURRENT market;
/// resting/pending is reduced on that side via the gateway loop. Dedup is the
/// caller's seen_trades(trade_id:order_id).
fn credit_owned_fill_direct(
    cfg: &Config,
    inventory: &SharedInventory,
    gw_tx: &GatewayTx,
    ledger_tx: &LedgerTx,
    side: &str,
    price: f64,
    size: f64,
) -> AppResult<()> {
    if size <= 0.001 {
        return Ok(());
    }
    let market = {
        let mut inv = inventory.lock().unwrap();
        if inv.market.is_empty() {
            return Ok(());
        }
        let m = inv.market.clone();
        inv.add_fill(&m, side, price, size);
        m
    };
    let fill = FillEvent {
        quote_id: String::new(),
        ts_ms: now_ms(),
        market: market.clone(),
        side: side.to_string(),
        price,
        size,
        inventory_up: 0.0,
        inventory_down: 0.0,
        pnl_if_up: 0.0,
        pnl_if_down: 0.0,
        source: "polymarket_user_ws_owner".to_string(),
    };
    // Reduce the resting order on this side (recomputes pending); log + persist.
    let _ = gw_tx.send(GatewayEvent::Fill(fill.clone()));
    let _ = ledger_tx.send(LedgerEvent::Filled(fill));
    heartbeat(
        cfg,
        "order-gateway",
        format!("own untracked fill credited real-time {side} {size:.2}@{price:.3}"),
    )?;
    Ok(())
}

fn handle_user_order(
    cfg: &Config,
    order_map: &SharedOrderMap,
    gw_tx: &GatewayTx,
    ledger_tx: &LedgerTx,
    value: &Value,
) -> AppResult<()> {
    let status = first_str(value, &["status", "type"])
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !status.contains("cancel") {
        return Ok(());
    }
    let Some(order_id) = first_str(value, &["id", "order_id"]) else {
        return Ok(());
    };
    // KEEP the order_map entry as an echo guard (mark done) instead of removing
    // it, so a trade event that arrives AFTER this cancel is recognised as
    // already-accounted rather than "unknown" (which pauses placement). Pruned
    // per window on market change; order_map drives only fill matching, not the
    // cap, so this does not change pending/cap accounting.
    let meta = {
        let mut map = order_map.lock().unwrap();
        let Some(entry) = map.get_mut(order_id) else {
            return Ok(());
        };
        entry.done = true;
        entry.clone()
    };
    // Report the ACTUAL matched size for the cancel log. The old
    // `.max(meta.size)` inflated every partial cancel up to the full order
    // size, overstating fills in the audit trail. Clamp to the order size.
    let size = parse_f64_value(value.get("size_matched"))
        .unwrap_or(0.0)
        .clamp(0.0, meta.size);
    let cancel = OrderCancelled {
        ts_ms: now_ms(),
        quote_id: meta.quote_id,
        market: meta.market,
        side: meta.side,
        size,
        reason: "user_ws_cancel".to_string(),
        source: "polymarket_user_ws".to_string(),
    };
    // Tell the gateway loop to drop the resting order (and recompute pending),
    // and the ledger for logging / kill switch.
    let _ = gw_tx.send(GatewayEvent::Cancelled(cancel.clone()));
    let _ = ledger_tx.send(LedgerEvent::Cancelled(cancel));
    heartbeat(cfg, "order-gateway", "user ws cancel")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_user_fill(
    cfg: &Config,
    inventory: &SharedInventory,
    order_map: &SharedOrderMap,
    gw_tx: &GatewayTx,
    ledger_tx: &LedgerTx,
    order_id: &str,
    price: f64,
    size: f64,
) -> AppResult<()> {
    // Dedup vs the cancel-detected-fill path: if a TTL/requote cancel already
    // found this order filled and credited it to held, the order_map entry is
    // flagged. Consume it here without re-crediting (else we'd double-count).
    // Atomic match+dedup+reduce under one order_map lock (P0 fix).
    let meta = match match_and_reduce_order(order_map, order_id, size) {
        // Already credited by the cancel-detected-fill path — do NOT re-credit.
        FillMatch::AlreadyCredited => return Ok(()),
        FillMatch::Unknown => {
            // P2: an account fill we can't match to a known order. Could be a fill
            // that landed before order_map was written, or a restart/state gap.
            // NEVER swallow it silently — that would under-count inventory and let
            // the cap re-open. Log loud and force an immediate on-chain reconcile so
            // risk corrects the shared inventory now, not up to RECONCILE_INTERVAL later.
            eprintln!(
                "[FILL_UNMATCHED] account fill for unknown order {order_id} size={size} price={price} — forcing reconcile"
            );
            let _ = heartbeat(
                cfg,
                "order-gateway",
                format!("UNMATCHED fill {order_id} size={size} — forcing reconcile"),
            );
            let _ = ledger_tx.send(LedgerEvent::UnmatchedFill);
            return Ok(());
        }
        FillMatch::Matched(meta) => meta,
    };
    let fill_size = if size > 0.0 {
        size.min(meta.size)
    } else {
        meta.size
    };
    let fill_price = if price > 0.0 { price } else { meta.price };
    if fill_size <= 0.001 {
        return Ok(());
    }
    let fill = FillEvent {
        quote_id: meta.quote_id,
        ts_ms: now_ms(),
        market: meta.market,
        side: meta.side,
        price: fill_price,
        size: fill_size,
        inventory_up: 0.0,
        inventory_down: 0.0,
        pnl_if_up: 0.0,
        pnl_if_down: 0.0,
        source: "polymarket_user_ws".to_string(),
    };
    // Authoritative in-process update: credit the fill into the shared inventory
    // immediately under the lock. This never gets lost like the old socket path.
    let mut credited = false;
    let mut current_market = String::new();
    {
        let mut inv = inventory.lock().unwrap();
        // A late fill from a previous window must not wipe a live non-flat
        // inventory. Only adopt the fill's market when we have no exposure.
        if inv.market.is_empty() || (inv.market != fill.market && inventory_is_flat(&inv)) {
            reset_inventory_for_market(&mut inv, &fill.market);
        }
        if inv.market == fill.market {
            inv.add_fill(&fill.market, &fill.side, fill.price, fill.size);
            credited = true;
        } else {
            current_market = inv.market.clone();
        }
    }
    // Tell the gateway loop to shrink/drop the resting order (it recomputes
    // pending), and the ledger for jsonl + kill switch + inventory.json.
    let _ = gw_tx.send(GatewayEvent::Fill(fill.clone()));
    if credited {
        let _ = ledger_tx.send(LedgerEvent::Filled(fill));
        heartbeat(cfg, "order-gateway", "user ws fill")?;
    } else {
        eprintln!(
            "[FILL_MARKET_MISMATCH] order {order_id} fill_market={} inventory_market={} size={fill_size} price={fill_price} - forcing reconcile",
            fill.market, current_market
        );
        let _ = heartbeat(
            cfg,
            "order-gateway",
            format!(
                "fill market mismatch {} vs {} -> forcing reconcile",
                fill.market, current_market
            ),
        );
        let _ = ledger_tx.send(LedgerEvent::UnmatchedFill);
    }
    Ok(())
}

enum FillMatch {
    /// The fill was already credited to held via the cancel-detected-fill path;
    /// entry consumed, the caller must NOT credit again.
    AlreadyCredited,
    /// No matching order — an account fill we don't track.
    Unknown,
    /// Matched a live order; `QuoteMeta` is the pre-reduction snapshot.
    Matched(QuoteMeta),
}

/// Look up + reduce an order_map entry for a user-WS fill, ATOMICALLY under one
/// lock. The guard is the REMAINING uncredited size, not the done/credited flags:
/// an order that was cancelled (entry kept by the echo-guard change) still has
/// size > 0, so a fill that lands AFTER the cancel is CREDITED against that
/// remainder — this is what keeps `held` accurate in real time and stops the
/// cap from re-opening (the 64-vs-40 over-accumulation). Double-counting is
/// prevented two ways, both safe here: the remaining-size decrement happens in
/// this same critical section (an order can never fill beyond its size), and the
/// caller dedups whole trades via seen_trades(trade_id:order_id). A fully
/// accounted order (filled, or handed to on-chain reconcile) has size 0 and is
/// recognised as a benign echo. Reconcile SETS held to the chain value
/// (idempotent), so real-time crediting never double-counts against it.
fn match_and_reduce_order(
    order_map: &SharedOrderMap,
    order_id: &str,
    reported_size: f64,
) -> FillMatch {
    let mut map = order_map.lock().unwrap();
    let Some(meta0) = map.get(order_id) else {
        return FillMatch::Unknown;
    };
    if meta0.size <= 0.001 {
        // No uncredited size left — fully filled, or handed to reconcile
        // (size zeroed). A later trade-status echo / WS replay is benign: don't
        // re-credit / reconcile / pause. Entry kept (pruned per window).
        return FillMatch::AlreadyCredited;
    }
    let meta = meta0.clone();
    let size = if reported_size > 0.0 {
        reported_size.min(meta.size)
    } else {
        meta.size
    };
    if let Some(current) = map.get_mut(order_id) {
        current.size = (current.size - size).max(0.0);
        if current.size <= 0.001 {
            // Fully filled: KEEP the entry as an echo guard (don't remove), so a
            // later status echo / replay is recognised as benign above.
            current.done = true;
        }
    }
    FillMatch::Matched(meta)
}

fn inventory_is_flat(inv: &Inventory) -> bool {
    const TOL: f64 = 1e-9;
    inv.up_shares.abs() <= TOL
        && inv.down_shares.abs() <= TOL
        && inv.up_cost.abs() <= TOL
        && inv.down_cost.abs() <= TOL
        && inv.pending_up.abs() <= TOL
        && inv.pending_down.abs() <= TOL
}

fn first_str<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| value.get(*key)?.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::Inventory;
    use std::sync::atomic::AtomicBool;

    fn test_cfg() -> Config {
        // Minimal config: only the fields the cap logic reads matter here.
        Config::from_env().expect("config")
    }

    fn resting_order(side: &str, quote_id: &str, size: f64) -> RestingOrder {
        RestingOrder {
            quote_id: quote_id.to_string(),
            exchange_order_id: Some(format!("exchange-{quote_id}")),
            market: "btc-updown-5m-test".to_string(),
            condition_id: "condition".to_string(),
            token_id: format!("token-{side}"),
            side: side.to_string(),
            price: 0.42,
            size,
            expires_ts_ms: now_ms() + 1_000,
        }
    }

    fn fill(side: &str, quote_id: &str, size: f64) -> FillEvent {
        FillEvent {
            quote_id: quote_id.to_string(),
            ts_ms: now_ms(),
            market: "btc-updown-5m-test".to_string(),
            side: side.to_string(),
            price: 0.42,
            size,
            inventory_up: 0.0,
            inventory_down: 0.0,
            pnl_if_up: 0.0,
            pnl_if_down: 0.0,
            source: "test".to_string(),
        }
    }

    #[test]
    fn ambiguous_cancel_does_not_synthesize_fill_and_forces_reconcile() {
        let mut cfg = test_cfg();
        cfg.run_dir = std::env::temp_dir().join(format!(
            "polymaker-cancel-reconcile-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let inventory: SharedInventory = Arc::new(Mutex::new(Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        }));
        let order_map: SharedOrderMap = Arc::new(Mutex::new(HashMap::new()));
        let order = resting_order("Up", "q1", 5.0);
        let id = order.exchange_order_id.clone().unwrap();
        let mut resting = HashMap::from([("Up".to_string(), order.clone())]);
        {
            let mut inv = inventory.lock().unwrap();
            recompute_pending_from_resting(&mut inv, &resting);
        }
        order_map.lock().unwrap().insert(
            id.clone(),
            QuoteMeta {
                quote_id: order.quote_id.clone(),
                market: order.market.clone(),
                condition_id: order.condition_id.clone(),
                side: order.side.clone(),
                price: order.price,
                size: order.size,
                credited: false,
                done: false,
            },
        );
        let (ledger_tx, ledger_rx) = std::sync::mpsc::channel();

        let may_replace = handle_gone_unverified_cancel(
            &cfg,
            &inventory,
            None,
            Some(&order_map),
            &mut resting,
            &ledger_tx,
            "Up",
            "requote",
            &order,
        )
        .unwrap();

        assert!(
            !may_replace,
            "must not place a replacement in the same cycle"
        );
        assert!(resting.is_empty(), "pending order is no longer open");
        {
            let inv = inventory.lock().unwrap();
            assert_eq!(inv.up_shares, 0.0, "must not synthesize held shares");
            assert_eq!(inv.pending_up, 0.0, "pending is released");
        }
        assert!(matches!(
            ledger_rx.try_recv().unwrap(),
            LedgerEvent::UnmatchedFill
        ));

        // A later user-WS replay for the same order is ignored; risk-ledger's
        // position reconcile is now the single source of truth for this case.
        assert!(
            matches!(
                match_and_reduce_order(&order_map, &id, 5.0),
                FillMatch::AlreadyCredited
            ),
            "should be recognised as reconcile-pending"
        );
        assert_eq!(
            inventory.lock().unwrap().up_shares,
            0.0,
            "must still not be locally credited"
        );
        let _ = std::fs::remove_dir_all(&cfg.run_dir);
    }

    #[test]
    fn ws_full_fill_then_echo_is_benign_not_unknown() {
        // After a full WS fill, a later trade-status echo / reconnect replay for
        // the SAME order must be recognised as already-accounted (benign), NOT
        // treated as an unknown fill (which would spam force-reconcile + pause).
        let order_map: SharedOrderMap = Arc::new(Mutex::new(HashMap::new()));
        order_map.lock().unwrap().insert(
            "ex-x".to_string(),
            QuoteMeta {
                quote_id: "q1".to_string(),
                market: "m".to_string(),
                condition_id: String::new(),
                side: "Up".to_string(),
                price: 0.5,
                size: 5.0,
                credited: false,
                done: false,
            },
        );
        // First fill matches and fully consumes → entry kept, marked done.
        assert!(matches!(
            match_and_reduce_order(&order_map, "ex-x", 5.0),
            FillMatch::Matched(_)
        ));
        assert!(order_map.lock().unwrap().get("ex-x").unwrap().done);
        // Echo / replay of the same fill → benign (already accounted), NOT Unknown.
        assert!(matches!(
            match_and_reduce_order(&order_map, "ex-x", 5.0),
            FillMatch::AlreadyCredited
        ));
        // A genuinely unknown order id → Unknown (still triggers reconcile path).
        assert!(matches!(
            match_and_reduce_order(&order_map, "never-seen", 5.0),
            FillMatch::Unknown
        ));
    }

    #[test]
    fn fill_after_cancel_is_credited_not_lost_and_not_unknown() {
        // 彻底修复 over-accumulation: a confirmed cancel KEEPS the order_map entry
        // (echo guard) with its size intact. A fill that lands AFTER the cancel
        // (fast requote churn) must be CREDITED against the remaining size — this
        // is what keeps `held` accurate so the cap cannot re-open and over-buy
        // (the 64-vs-40 breach). It must also NOT be "unknown" (which would pause
        // placement — the stutter).
        let order_map: SharedOrderMap = Arc::new(Mutex::new(HashMap::new()));
        order_map.lock().unwrap().insert(
            "ex-c".to_string(),
            QuoteMeta {
                quote_id: "q1".to_string(),
                market: "m".to_string(),
                condition_id: String::new(),
                side: "Up".to_string(),
                price: 0.5,
                size: 5.0,
                credited: false,
                done: false,
            },
        );
        // Cancel marks the entry done but KEEPS it with size intact (what
        // send_cancel / handle_user_order now do).
        if let Some(meta) = order_map.lock().unwrap().get_mut("ex-c") {
            meta.done = true;
        }
        // A fill that lands after the cancel is CREDITED (Matched), not skipped
        // and not "unknown": held stays accurate -> cap holds -> no over-buy.
        assert!(matches!(
            match_and_reduce_order(&order_map, "ex-c", 3.0),
            FillMatch::Matched(_)
        ));
        // Remaining size shrank by the credited amount.
        assert!((order_map.lock().unwrap().get("ex-c").unwrap().size - 2.0).abs() < 1e-9);
        // Crediting the rest fully consumes it.
        assert!(matches!(
            match_and_reduce_order(&order_map, "ex-c", 2.0),
            FillMatch::Matched(_)
        ));
        // Now fully accounted (size 0) -> a further echo is benign, not a re-credit.
        assert!(matches!(
            match_and_reduce_order(&order_map, "ex-c", 5.0),
            FillMatch::AlreadyCredited
        ));
    }

    #[test]
    fn user_trade_maker_orders_skip_other_makers() {
        let cfg = test_cfg();
        let inventory: SharedInventory = Arc::new(Mutex::new(Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        }));
        let order_map: SharedOrderMap = Arc::new(Mutex::new(HashMap::new()));
        order_map.lock().unwrap().insert(
            "our-order".to_string(),
            QuoteMeta {
                quote_id: "q-ours".to_string(),
                market: "btc-updown-5m-test".to_string(),
                condition_id: "condition".to_string(),
                side: "Up".to_string(),
                price: 0.42,
                size: 5.0,
                credited: false,
                done: false,
            },
        );
        let (gw_tx, _gw_rx) = std::sync::mpsc::channel();
        let (ledger_tx, ledger_rx) = std::sync::mpsc::channel();
        let mut seen = HashSet::new();
        let event = serde_json::json!({
            "id": "trade-1",
            "event_type": "trade",
            "maker_orders": [
                {"order_id": "other-maker", "matched_amount": "24", "price": "0.79"},
                {"order_id": "our-order", "matched_amount": "5", "price": "0.42"}
            ]
        });

        handle_user_trade(
            &cfg, &inventory, &order_map, &gw_tx, &ledger_tx, &mut seen, &event,
        )
        .unwrap();

        assert_eq!(inventory.lock().unwrap().up_shares, 5.0);
        assert!(matches!(
            ledger_rx.try_recv().expect("known maker fill"),
            LedgerEvent::Filled(fill) if fill.quote_id == "q-ours" && (fill.size - 5.0).abs() < 1e-9
        ));
        assert!(
            ledger_rx.try_recv().is_err(),
            "unknown maker_orders must not emit unmatched reconcile events"
        );
    }

    #[test]
    fn user_trade_all_unknown_makers_forces_reconcile_once() {
        // Regression guard: a trade in OUR feed whose makers match NONE of our
        // tracked orders means our maker fill is untracked. It must NOT be
        // silently dropped (that under-counts held -> the side cap re-opens ->
        // over-buy past the cap). It must force exactly one on-chain reconcile.
        let cfg = test_cfg();
        let inventory: SharedInventory = Arc::new(Mutex::new(Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        }));
        let order_map: SharedOrderMap = Arc::new(Mutex::new(HashMap::new()));
        let (gw_tx, _gw_rx) = std::sync::mpsc::channel();
        let (ledger_tx, ledger_rx) = std::sync::mpsc::channel();
        let mut seen = HashSet::new();
        let event = serde_json::json!({
            "id": "trade-allunknown",
            "event_type": "trade",
            "maker_orders": [
                {"order_id": "untracked-a", "matched_amount": "20", "price": "0.58"},
                {"order_id": "untracked-b", "matched_amount": "4", "price": "0.65"}
            ]
        });

        handle_user_trade(
            &cfg, &inventory, &order_map, &gw_tx, &ledger_tx, &mut seen, &event,
        )
        .unwrap();

        // Exactly one reconcile forced, and nothing locally synthesized.
        assert!(matches!(
            ledger_rx
                .try_recv()
                .expect("untracked own fill must force reconcile"),
            LedgerEvent::UnmatchedFill
        ));
        assert!(
            ledger_rx.try_recv().is_err(),
            "must force the reconcile only once per trade"
        );
        assert_eq!(inventory.lock().unwrap().up_shares, 0.0);
        assert_eq!(inventory.lock().unwrap().down_shares, 0.0);

        // A replay of the same trade must not force a second reconcile.
        handle_user_trade(
            &cfg, &inventory, &order_map, &gw_tx, &ledger_tx, &mut seen, &event,
        )
        .unwrap();
        assert!(
            ledger_rx.try_recv().is_err(),
            "replay of the same trade must be deduped"
        );
    }

    #[test]
    fn owned_untracked_fill_is_credited_realtime_not_dropped() {
        // The real fix for over-accumulation: an OWN maker fill whose order id we
        // never recorded (placement race) is credited straight into held from the
        // event (owner + outcome), not silently dropped and not left to the
        // (laggy) chain reconcile. held stays accurate -> the side cap holds.
        let mut cfg = test_cfg();
        cfg.poly_api_key = "our-key".to_string();
        let inventory: SharedInventory = Arc::new(Mutex::new(Inventory {
            market: "btc-updown-5m-test".to_string(),
            ..Default::default()
        }));
        // Empty order_map = the placement-race / first-order case (current
        // window conditions unknown -> allowed).
        let order_map: SharedOrderMap = Arc::new(Mutex::new(HashMap::new()));
        let (gw_tx, gw_rx) = std::sync::mpsc::channel();
        let (ledger_tx, ledger_rx) = std::sync::mpsc::channel();
        let mut seen = HashSet::new();
        let event = serde_json::json!({
            "id": "trade-race",
            "event_type": "trade",
            "market": "cond-1",
            "maker_orders": [
                {"order_id": "untracked-1", "owner": "our-key",
                 "outcome": "Up", "matched_amount": "20", "price": "0.66"}
            ]
        });

        handle_user_trade(
            &cfg, &inventory, &order_map, &gw_tx, &ledger_tx, &mut seen, &event,
        )
        .unwrap();

        {
            let inv = inventory.lock().unwrap();
            assert_eq!(inv.up_shares, 20.0, "own fill credited in real time");
            assert!(
                (inv.up_cost - 20.0 * 0.66).abs() < 1e-9,
                "cost at the real fill price"
            );
        }
        assert!(matches!(
            ledger_rx.try_recv().expect("ledger fill"),
            LedgerEvent::Filled(f) if (f.size - 20.0).abs() < 1e-9
        ));
        assert!(matches!(
            gw_rx.try_recv().expect("gateway fill to shrink resting"),
            GatewayEvent::Fill(_)
        ));
        assert!(
            ledger_rx.try_recv().is_err(),
            "credited directly: must NOT also force a reconcile"
        );

        // Replay of the same trade must not double-credit.
        handle_user_trade(
            &cfg, &inventory, &order_map, &gw_tx, &ledger_tx, &mut seen, &event,
        )
        .unwrap();
        assert_eq!(
            inventory.lock().unwrap().up_shares,
            20.0,
            "replay must be deduped, no double credit"
        );
    }

    #[test]
    fn user_fill_market_mismatch_forces_reconcile_not_cross_market_credit() {
        let cfg = test_cfg();
        let inventory: SharedInventory = Arc::new(Mutex::new(Inventory {
            market: "btc-updown-5m-old".to_string(),
            down_shares: 40.0,
            down_cost: 12.0,
            ..Default::default()
        }));
        let order_map: SharedOrderMap = Arc::new(Mutex::new(HashMap::new()));
        order_map.lock().unwrap().insert(
            "new-order".to_string(),
            QuoteMeta {
                quote_id: "q-new".to_string(),
                market: "btc-updown-5m-new".to_string(),
                condition_id: "condition".to_string(),
                side: "Up".to_string(),
                price: 0.55,
                size: 5.0,
                credited: false,
                done: false,
            },
        );
        let (gw_tx, gw_rx) = std::sync::mpsc::channel();
        let (ledger_tx, ledger_rx) = std::sync::mpsc::channel();
        let mut seen = HashSet::new();
        let event = serde_json::json!({
            "id": "trade-mismatch",
            "event_type": "trade",
            "maker_orders": [
                {"order_id": "new-order", "matched_amount": "5", "price": "0.55"}
            ]
        });

        handle_user_trade(
            &cfg, &inventory, &order_map, &gw_tx, &ledger_tx, &mut seen, &event,
        )
        .unwrap();

        {
            let inv = inventory.lock().unwrap();
            assert_eq!(inv.market, "btc-updown-5m-old");
            assert_eq!(inv.up_shares, 0.0);
            assert_eq!(inv.down_shares, 40.0);
        }
        assert!(matches!(
            gw_rx.try_recv().expect("resting order still shrinks"),
            GatewayEvent::Fill(fill)
                if fill.market == "btc-updown-5m-new" && fill.side == "Up"
        ));
        assert!(matches!(
            ledger_rx.try_recv().expect("mismatch must force reconcile"),
            LedgerEvent::UnmatchedFill
        ));
        assert!(
            ledger_rx.try_recv().is_err(),
            "mismatch must not be logged as an enriched fill"
        );
    }

    #[test]
    fn user_fill_can_adopt_new_market_when_inventory_is_flat() {
        let cfg = test_cfg();
        let inventory: SharedInventory = Arc::new(Mutex::new(Inventory {
            market: "btc-updown-5m-old".to_string(),
            ..Default::default()
        }));
        let order_map: SharedOrderMap = Arc::new(Mutex::new(HashMap::new()));
        order_map.lock().unwrap().insert(
            "new-order".to_string(),
            QuoteMeta {
                quote_id: "q-new".to_string(),
                market: "btc-updown-5m-new".to_string(),
                condition_id: "condition".to_string(),
                side: "Up".to_string(),
                price: 0.55,
                size: 5.0,
                credited: false,
                done: false,
            },
        );
        let (gw_tx, _gw_rx) = std::sync::mpsc::channel();
        let (ledger_tx, ledger_rx) = std::sync::mpsc::channel();
        let mut seen = HashSet::new();
        let event = serde_json::json!({
            "id": "trade-flat-adopt",
            "event_type": "trade",
            "maker_orders": [
                {"order_id": "new-order", "matched_amount": "5", "price": "0.55"}
            ]
        });

        handle_user_trade(
            &cfg, &inventory, &order_map, &gw_tx, &ledger_tx, &mut seen, &event,
        )
        .unwrap();

        {
            let inv = inventory.lock().unwrap();
            assert_eq!(inv.market, "btc-updown-5m-new");
            assert_eq!(inv.up_shares, 5.0);
            assert_eq!(inv.down_shares, 0.0);
        }
        assert!(matches!(
            ledger_rx.try_recv().expect("flat inventory can credit"),
            LedgerEvent::Filled(fill)
                if fill.market == "btc-updown-5m-new" && fill.side == "Up"
        ));
    }

    #[test]
    fn user_fill_reduces_resting_size() {
        let mut resting_orders =
            HashMap::from([("Up".to_string(), resting_order("Up", "q1", 5.0))]);

        assert!(apply_fill_to_resting_map(
            &mut resting_orders,
            &fill("Up", "q1", 2.0)
        ));

        assert_eq!(resting_orders.get("Up").unwrap().size, 3.0);
    }

    #[test]
    fn full_user_fill_removes_resting_order() {
        let mut resting_orders =
            HashMap::from([("Down".to_string(), resting_order("Down", "q2", 5.0))]);

        assert!(apply_fill_to_resting_map(
            &mut resting_orders,
            &fill("Down", "q2", 5.0)
        ));

        assert!(resting_orders.is_empty());
    }

    #[test]
    fn fill_from_other_market_does_not_shrink_current_resting_order() {
        // A late fill from the PREVIOUS window (same quote_id reuse, or empty
        // quote_id + same side) must NOT reduce the current window's resting
        // order — that would under-count pending and re-open the side cap.
        let mut resting_orders =
            HashMap::from([("Up".to_string(), resting_order("Up", "q1", 5.0))]);

        // Same quote_id but a different market: must be ignored.
        let mut stale = fill("Up", "q1", 5.0);
        stale.market = "btc-updown-5m-OLDWINDOW".to_string();
        assert!(!apply_fill_to_resting_map(&mut resting_orders, &stale));
        assert_eq!(
            resting_orders.get("Up").unwrap().size,
            5.0,
            "stale-market fill must not shrink the live order"
        );

        // Empty quote_id + same side but a different market: also ignored.
        let mut stale_empty = fill("Up", "", 5.0);
        stale_empty.market = "btc-updown-5m-OLDWINDOW".to_string();
        assert!(!apply_fill_to_resting_map(
            &mut resting_orders,
            &stale_empty
        ));
        assert_eq!(resting_orders.get("Up").unwrap().size, 5.0);

        // Sanity: a same-market fill still applies.
        assert!(apply_fill_to_resting_map(
            &mut resting_orders,
            &fill("Up", "q1", 2.0)
        ));
        assert_eq!(resting_orders.get("Up").unwrap().size, 3.0);
    }

    #[test]
    fn user_cancel_removes_resting_order() {
        let mut resting_orders =
            HashMap::from([("Up".to_string(), resting_order("Up", "q3", 5.0))]);
        let cancel = OrderCancelled {
            ts_ms: now_ms(),
            quote_id: "q3".to_string(),
            market: "btc-updown-5m-test".to_string(),
            side: "Up".to_string(),
            size: 5.0,
            reason: "user_ws_cancel".to_string(),
            source: "test".to_string(),
        };

        assert!(apply_cancel_to_resting_map(&mut resting_orders, &cancel));

        assert!(resting_orders.is_empty());
    }

    #[test]
    fn active_orders_restore_order_map() {
        let resting_orders = HashMap::from([("Up".to_string(), resting_order("Up", "q4", 5.0))]);

        let order_map = order_map_from_resting(&resting_orders);

        let meta = order_map.get("exchange-q4").unwrap();
        assert_eq!(meta.quote_id, "q4");
        assert_eq!(meta.side, "Up");
        assert_eq!(meta.size, 5.0);
    }

    /// The whole point of the refactor: filled + pending can never exceed the
    /// per-side cap across repeated quote cycles. We drive the same cap+place+
    /// fill logic the gateway loop uses against ONE shared inventory and assert
    /// the side never goes over max_side_inventory.
    #[test]
    fn cap_holds_across_repeated_cycles() {
        let mut cfg = test_cfg();
        // Pin the cap to a known value: quote_size=5, mult=2 => cap 10.
        cfg.quote_size = 5.0;
        cfg.inventory_mult = 2.0;
        let cap = cfg.max_side_inventory();
        assert_eq!(cap, 10.0);

        let market = "btc-updown-5m-cap-test";
        let inventory: SharedInventory = Arc::new(Mutex::new(Inventory {
            market: market.to_string(),
            ..Default::default()
        }));
        let mut resting: HashMap<String, RestingOrder> = HashMap::new();
        let mut max_seen = 0.0_f64;

        // Repeatedly: check the cap, place a full quote if there's room, then
        // fill the resting order. Over many cycles filled+pending must stay <= cap.
        for cycle in 0..20 {
            let side = "Up";
            let size = cfg.quote_size;
            let (held, pending) = {
                let inv = inventory.lock().unwrap();
                (
                    held_shares(&inv, market, side),
                    pending_shares(&inv, market, side),
                )
            };
            let room = (cap - held - pending).floor();
            // The cap invariant: at every check, outstanding+filled <= cap.
            assert!(
                held + pending <= cap + 1e-9,
                "cycle {cycle}: over cap before place"
            );
            if room < size {
                // No room for a full quote: skip (PR#17 rule). Then settle by
                // pretending the resting order fills, to free pending.
                if let Some(order) = resting.remove(side) {
                    let mut inv = inventory.lock().unwrap();
                    inv.add_fill(market, side, order.price, order.size);
                    recompute_pending_from_resting(&mut inv, &resting);
                }
                continue;
            }
            // Place a full quote -> becomes pending.
            resting.insert(
                side.to_string(),
                resting_order(side, &format!("q{cycle}"), size),
            );
            {
                let mut inv = inventory.lock().unwrap();
                recompute_pending_from_resting(&mut inv, &resting);
                let total = held_shares(&inv, market, side) + pending_shares(&inv, market, side);
                max_seen = max_seen.max(total);
                assert!(
                    total <= cap + 1e-9,
                    "cycle {cycle}: filled+pending {total} over cap {cap}"
                );
            }
            // Now the order fills: pending -> filled. Total unchanged.
            if let Some(order) = resting.remove(side) {
                let mut inv = inventory.lock().unwrap();
                inv.add_fill(market, side, order.price, order.size);
                recompute_pending_from_resting(&mut inv, &resting);
                let total = held_shares(&inv, market, side) + pending_shares(&inv, market, side);
                max_seen = max_seen.max(total);
                assert!(total <= cap + 1e-9, "cycle {cycle}: filled over cap {cap}");
            }
        }
        // We should have actually loaded up to the cap, not stopped early.
        assert!(max_seen >= cap - cfg.quote_size, "cap was never approached");
        // And never exceeded it.
        assert!(max_seen <= cap + 1e-9);

        // Touch a StopFlag type so the import stays meaningful in the test cfg.
        let _stop: StopFlag = Arc::new(AtomicBool::new(false));
    }

    #[test]
    fn unpaired_cap_blocks_only_the_side_that_worsens_imbalance() {
        let mut cfg = test_cfg();
        cfg.max_unpaired_shares = 5.0;
        let inv = Inventory {
            market: "btc-updown-5m-test".to_string(),
            up_shares: 10.0,
            down_shares: 5.0,
            ..Default::default()
        };

        assert!(
            !unpaired_limit_allows_quote_after_replace(
                &cfg,
                &inv,
                "btc-updown-5m-test",
                "Up",
                5.0,
                0.0,
            ),
            "adding to the already-ahead side must be blocked"
        );
        assert!(
            unpaired_limit_allows_quote_after_replace(
                &cfg,
                &inv,
                "btc-updown-5m-test",
                "Down",
                5.0,
                0.0,
            ),
            "adding to the lagging side reduces imbalance and must be allowed"
        );
    }

    #[test]
    fn unpaired_cap_counts_replacement_quote_size() {
        let mut cfg = test_cfg();
        cfg.max_unpaired_shares = 40.0;
        let inv = Inventory {
            market: "btc-updown-5m-test".to_string(),
            down_shares: 40.0,
            pending_down: 20.0,
            ..Default::default()
        };

        assert!(
            !unpaired_limit_allows_quote_after_replace(
                &cfg,
                &inv,
                "btc-updown-5m-test",
                "Down",
                20.0,
                20.0,
            ),
            "replacing an existing Down quote still leaves 60 Down exposure and must be blocked"
        );
        assert!(
            unpaired_limit_allows_quote_after_replace(
                &cfg,
                &inv,
                "btc-updown-5m-test",
                "Up",
                20.0,
                0.0,
            ),
            "opposite-side quote reduces the 60-share imbalance"
        );
    }

    #[test]
    fn asymmetric_balance_limits_lagging_side_to_ratio() {
        let mut cfg = test_cfg();
        cfg.enable_asymmetric_balance = true;
        cfg.balance_secondary_ratio = 0.8;
        let inv = Inventory {
            market: "btc-updown-5m-test".to_string(),
            up_shares: 20.0,
            ..Default::default()
        };

        assert_eq!(
            asymmetric_balance_room_after_replace(&cfg, &inv, "btc-updown-5m-test", "Down", 0.0),
            16.0,
            "with Up 20 as main side, Down can only fill up to 16"
        );
        assert!(
            asymmetric_balance_room_after_replace(&cfg, &inv, "btc-updown-5m-test", "Up", 0.0)
                .is_infinite(),
            "the current main side is not limited by the 80% secondary cap"
        );
    }

    #[test]
    fn asymmetric_balance_after_replace_ignores_main_side_pending() {
        let mut cfg = test_cfg();
        cfg.enable_asymmetric_balance = true;
        cfg.balance_secondary_ratio = 0.8;
        let inv = Inventory {
            market: "btc-updown-5m-test".to_string(),
            up_shares: 7.0,
            pending_up: 20.0,
            ..Default::default()
        };

        assert_eq!(
            asymmetric_balance_room_after_replace(&cfg, &inv, "btc-updown-5m-test", "Down", 0.0),
            5.6000000000000005,
            "pending Up must not make Down room larger than 80% of filled Up"
        );
    }

    #[test]
    fn asymmetric_balance_after_replace_counts_secondary_pending() {
        let mut cfg = test_cfg();
        cfg.enable_asymmetric_balance = true;
        cfg.balance_secondary_ratio = 0.8;
        let inv = Inventory {
            market: "btc-updown-5m-test".to_string(),
            up_shares: 20.0,
            down_shares: 5.0,
            pending_down: 11.0,
            ..Default::default()
        };

        assert_eq!(
            asymmetric_balance_room_after_replace(&cfg, &inv, "btc-updown-5m-test", "Down", 0.0),
            0.0,
            "filled Down 5 plus pending Down 11 already reaches 80% of Up 20"
        );
        assert_eq!(
            asymmetric_balance_room_after_replace(&cfg, &inv, "btc-updown-5m-test", "Down", 11.0),
            11.0,
            "replacing the existing pending Down quote frees that pending room"
        );
    }
}
