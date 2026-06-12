//! Risk-ledger thread: jsonl logging (quotes/fills), kill switch, inventory.json
//! persistence, and periodic on-chain position reconciliation.
//!
//! It does NOT own order-driven inventory mutation — the order gateway is the
//! sole writer of fills/pending into the shared inventory. The ledger only reads
//! that inventory to log/enrich/persist, and writes it ONLY for the periodic
//! chain reconcile (correcting missed fills).

use crate::config::Config;
use crate::ipc::{heartbeat, now_ms, write_json, FillEvent, Inventory};
use crate::real_orders::{PositionReconciler, PositionSnapshot};
use crate::AppResult;
use std::sync::atomic::Ordering;
use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};

use super::state::{
    log_jsonl, request_stop, spawn_jsonl_writer, stopping, LedgerEvent, LedgerRx, ReconcileGate,
    SharedInventory, StopFlag,
};

const RECV_TIMEOUT: Duration = Duration::from_millis(250);
const POSITION_RECONCILE_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// How long suspect (maybe-filled) shares keep occupying cap room before a
/// caught-up reconcile that found no fill releases them. Generous versus the
/// usual Polygon mine + Data API indexing lag of a few seconds.
const SUSPECT_AGE_OUT_MS: u64 = 20_000;

pub fn run(
    cfg: Config,
    stop: StopFlag,
    inventory: SharedInventory,
    rx: LedgerRx,
    reconcile_gate: ReconcileGate,
) -> AppResult<()> {
    // Persist a clean starting inventory (zero pending; pending is now derived
    // from the gateway's resting map and synced into the shared inventory).
    {
        let inv = inventory.lock().unwrap();
        write_json(&cfg.inventory_path(), &*inv)?;
    }
    let logger = spawn_jsonl_writer();

    // Position reconciliation: periodically read the wallet's true on-chain share
    // holdings and correct the shared inventory if a fill was ever missed over
    // the user WS. Only in real mode (Data API is public).
    let position_reconciler = if cfg.real_orders_enabled() {
        match PositionReconciler::connect(&cfg) {
            Ok(r) => Some(r),
            Err(err) => {
                heartbeat(
                    &cfg,
                    "risk-ledger",
                    format!("position reconciler off: {err}"),
                )?;
                None
            }
        }
    } else {
        None
    };
    let mut last_position_sync = Instant::now();
    // P1: track the Up/Down token pair as an atomic group keyed by market. A
    // window switch must never let us reconcile with a NEW Up token + a STALE
    // Down token from the previous window. Cleared on market change; reconcile
    // runs only when both tokens are known AND match the live inventory market.
    let mut current_market = String::new();
    let mut up_token = String::new();
    let mut down_token = String::new();
    // P2: after a real fill (or an ambiguous cancel that parked suspect shares),
    // poll the authoritative Data API until it catches up to the locally
    // credited inventory and all suspect shares are resolved. This runs in the
    // BACKGROUND — placement is never paused; suspect shares occupying cap room
    // are what prevent repeat buying while the position is uncertain.
    let mut next_forced_reconcile: Option<Instant> = None;

    while !stopping(&stop, &cfg) {
        // Periodic on-chain position reconciliation (real mode only).
        if let Some(reconciler) = &position_reconciler {
            let now = Instant::now();
            let interval_due = cfg.reconcile_interval_ms > 0
                && last_position_sync.elapsed() >= Duration::from_millis(cfg.reconcile_interval_ms);
            let force_due = next_forced_reconcile.is_some_and(|due| now >= due);
            let periodic_allowed = interval_due && next_forced_reconcile.is_none();
            let token_ready = (!up_token.trim().is_empty() || !down_token.trim().is_empty())
                && !current_market.is_empty();
            if (periodic_allowed || force_due) && token_ready {
                last_position_sync = now;
                // Only reconcile when BOTH tokens belong to the SAME market the
                // shared inventory is currently on — never mix new/old windows.
                let market_matches = { inventory.lock().unwrap().market == current_market };
                if market_matches {
                    match reconciler.fetch(&up_token, &down_token) {
                        Ok(snap) => {
                            let mut stale_market = false;
                            let (corrected, caught_up, local_up, local_down) = {
                                let mut inv = inventory.lock().unwrap();
                                // Re-check under the lock: the gateway may have
                                // switched windows while fetch() was in flight.
                                // NEVER apply an old window's snapshot to the new
                                // window's freshly-reset inventory.
                                if inv.market != current_market {
                                    stale_market = true;
                                    (false, true, inv.up_shares, inv.down_shares)
                                } else {
                                    let pre_up = inv.up_shares;
                                    let pre_down = inv.down_shares;
                                    let mut dirty =
                                        reconcile_inventory_with_chain(&cfg, &mut inv, &snap)?;
                                    // A raise means the chain confirmed the real
                                    // position — suspects on that side are absorbed.
                                    if inv.up_shares > pre_up + 1e-9 && inv.suspect_up > 0.0 {
                                        inv.suspect_up = 0.0;
                                        dirty = true;
                                    }
                                    if inv.down_shares > pre_down + 1e-9 && inv.suspect_down > 0.0 {
                                        inv.suspect_down = 0.0;
                                        dirty = true;
                                    }
                                    // No raise after a generous indexing margin →
                                    // conclude the suspected fill never happened
                                    // and release the parked cap room.
                                    if inv.has_suspects()
                                        && now_ms().saturating_sub(inv.suspect_since_ms)
                                            > SUSPECT_AGE_OUT_MS
                                    {
                                        inv.clear_suspects();
                                        dirty = true;
                                    }
                                    let caught_up = snapshot_covers_inventory(
                                        &snap,
                                        &inv,
                                        &up_token,
                                        &down_token,
                                    ) && !inv.has_suspects();
                                    (dirty, caught_up, inv.up_shares, inv.down_shares)
                                }
                            };
                            if stale_market {
                                next_forced_reconcile = None;
                                reconcile_gate.store(false, Ordering::Relaxed);
                                continue;
                            }
                            if corrected {
                                let inv = inventory.lock().unwrap();
                                write_json(&cfg.inventory_path(), &*inv)?;
                                check_kill_switch(&cfg, &stop, &inv)?;
                            }
                            if force_due && !caught_up {
                                next_forced_reconcile =
                                    Some(Instant::now() + POSITION_RECONCILE_POLL_INTERVAL);
                                heartbeat(
                                    &cfg,
                                    "risk-ledger",
                                    format!(
                                        "position reconcile waiting: api up={:.2} down={:.2}, local up={:.2} down={:.2}",
                                        snap.up_shares, snap.down_shares, local_up, local_down
                                    ),
                                )?;
                            } else {
                                // Reconcile confirmed the exchange-facing position.
                                // Release any placement pause set by an inventory-dirty event.
                                next_forced_reconcile = None;
                                reconcile_gate.store(false, Ordering::Relaxed);
                            }
                        }
                        Err(err) => {
                            if force_due {
                                next_forced_reconcile =
                                    Some(Instant::now() + POSITION_RECONCILE_POLL_INTERVAL);
                            }
                            heartbeat(
                                &cfg,
                                "risk-ledger",
                                format!("position reconcile failed: {err}"),
                            )?;
                        }
                    }
                } else if force_due {
                    next_forced_reconcile = None;
                    reconcile_gate.store(false, Ordering::Relaxed);
                }
            }
        }

        match rx.recv_timeout(RECV_TIMEOUT) {
            Ok(LedgerEvent::Accepted(accepted)) => {
                // Learn the Up/Down token pair atomically. On a market switch,
                // clear BOTH tokens first so we never reconcile a new Up token
                // against a stale Down token from the previous window.
                if accepted.quote.market != current_market {
                    current_market = accepted.quote.market.clone();
                    up_token.clear();
                    down_token.clear();
                    next_forced_reconcile = None;
                    reconcile_gate.store(false, Ordering::Relaxed);
                }
                if !accepted.quote.token_id.trim().is_empty() {
                    match accepted.quote.side.as_str() {
                        "Up" => up_token = accepted.quote.token_id.clone(),
                        "Down" => down_token = accepted.quote.token_id.clone(),
                        _ => {}
                    }
                }
                log_jsonl(&logger, &cfg.quotes_path(), &accepted.quote)?;
                let inv = inventory.lock().unwrap();
                write_json(&cfg.inventory_path(), &*inv)?;
                check_kill_switch(&cfg, &stop, &inv)?;
                drop(inv);
                heartbeat(&cfg, "risk-ledger", "order accepted")?;
            }
            Ok(LedgerEvent::Cancelled(cancel)) => {
                let inv = inventory.lock().unwrap();
                write_json(&cfg.inventory_path(), &*inv)?;
                check_kill_switch(&cfg, &stop, &inv)?;
                drop(inv);
                heartbeat(&cfg, "risk-ledger", format!("order {}", cancel.reason))?;
            }
            Ok(LedgerEvent::Filled(fill)) => {
                // The gateway already credited the shared inventory; we enrich the
                // fill from it for the fills.jsonl log and persist inventory.json.
                let inv = inventory.lock().unwrap();
                let fill_matches_current_market = inv.market == fill.market;
                let enriched = FillEvent {
                    quote_id: fill.quote_id.clone(),
                    ts_ms: now_ms(),
                    market: fill.market.clone(),
                    token_id: fill.token_id.clone(),
                    side: fill.side.clone(),
                    price: fill.price,
                    size: fill.size,
                    inventory_up: inv.up_shares,
                    inventory_down: inv.down_shares,
                    pnl_if_up: inv.pnl_if_up(),
                    pnl_if_down: inv.pnl_if_down(),
                    source: fill.source.clone(),
                };
                log_jsonl(&logger, &cfg.fills_path(), &enriched)?;
                write_json(&cfg.inventory_path(), &*inv)?;
                check_kill_switch(&cfg, &stop, &inv)?;
                drop(inv);
                if fill_matches_current_market {
                    learn_token_pair_from_fill(
                        &mut current_market,
                        &mut up_token,
                        &mut down_token,
                        &fill,
                    );
                    // Background verification only — a matched fill is exact
                    // accounting, so placement is NOT paused (pausing cancelled
                    // the opposite leg and froze the bot one-sided).
                    next_forced_reconcile = Some(Instant::now());
                }
                heartbeat(&cfg, "risk-ledger", "fill accounted")?;
            }
            Ok(LedgerEvent::UnmatchedFill) => {
                // Gateway saw an account fill it couldn't match to a known order
                // (or an ambiguous cancel parked suspect shares). Placement is
                // NOT paused — the suspect shares already occupy cap room, which
                // is what prevents repeat buying. Force an immediate on-chain
                // reconcile to resolve them.
                next_forced_reconcile = Some(Instant::now());
                heartbeat(
                    &cfg,
                    "risk-ledger",
                    "unmatched fill -> forcing background reconcile (no pause)",
                )?;
            }
            Err(RecvTimeoutError::Timeout) => {
                heartbeat(&cfg, "risk-ledger", "waiting")?;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}

fn learn_token_pair_from_fill(
    current_market: &mut String,
    up_token: &mut String,
    down_token: &mut String,
    fill: &FillEvent,
) {
    if fill.market != *current_market {
        *current_market = fill.market.clone();
        up_token.clear();
        down_token.clear();
    }
    if fill.token_id.trim().is_empty() {
        return;
    }
    match fill.side.as_str() {
        "Up" => *up_token = fill.token_id.clone(),
        "Down" => *down_token = fill.token_id.clone(),
        _ => {}
    }
}

/// Overwrite locally-derived filled inventory with authoritative on-chain
/// holdings when they exceed the local count (recover a MISSED fill). Cost basis
/// comes from the Data API's initial_value, so PnL stays correct. Pending orders
/// are NOT touched — they aren't on-chain yet. Returns true if a correction was
/// applied.
///
/// P1 SAFETY: this only ever RAISES held, never lowers it. The bot is buy-only,
/// so within a window the position can only grow; the Data API also lags
/// indexing. Lowering held on a stale snapshot would re-open cap room
/// (`room = cap - held - pending`) and let real over-accumulation through — the
/// exact failure that snowballed to 28 shares. An over-stated held only freezes
/// a side (safe); it is reset cleanly on the next window via reset_inventory.
fn reconcile_inventory_with_chain(
    cfg: &Config,
    inventory: &mut Inventory,
    snap: &PositionSnapshot,
) -> AppResult<bool> {
    const TOL: f64 = 0.5;
    let mut changed = false;
    if snap.up_shares > inventory.up_shares + TOL {
        inventory.up_shares = snap.up_shares;
        inventory.up_cost = snap.up_cost;
        changed = true;
    }
    if snap.down_shares > inventory.down_shares + TOL {
        inventory.down_shares = snap.down_shares;
        inventory.down_cost = snap.down_cost;
        changed = true;
    }
    if changed {
        heartbeat(
            cfg,
            "risk-ledger",
            format!(
                "position reconcile (raise): up->{:.0} down->{:.0}",
                inventory.up_shares, inventory.down_shares
            ),
        )?;
        inventory.ts_ms = now_ms();
    }
    Ok(changed)
}

fn snapshot_covers_inventory(
    snap: &PositionSnapshot,
    inventory: &Inventory,
    up_token: &str,
    down_token: &str,
) -> bool {
    const TOL: f64 = 0.5;
    let up_ok = up_token.trim().is_empty() || snap.up_shares + TOL >= inventory.up_shares;
    let down_ok = down_token.trim().is_empty() || snap.down_shares + TOL >= inventory.down_shares;
    up_ok && down_ok
}

fn check_kill_switch(cfg: &Config, stop: &StopFlag, inventory: &Inventory) -> AppResult<()> {
    let worst_pnl = inventory.pnl_if_up().min(inventory.pnl_if_down());
    if cfg.max_loss > 0.0 && worst_pnl <= -cfg.max_loss {
        heartbeat(
            cfg,
            "risk-ledger",
            format!("kill switch: max loss {worst_pnl:+.2}"),
        )?;
        request_stop(stop, cfg)?;
        return Ok(());
    }

    let total_inventory = inventory.effective_up() + inventory.effective_down();
    if max_total_inventory_exceeded(cfg, inventory) {
        heartbeat(
            cfg,
            "risk-ledger",
            format!("kill switch: inventory {total_inventory:.0}"),
        )?;
        request_stop(stop, cfg)?;
    }
    Ok(())
}

fn max_total_inventory_exceeded(cfg: &Config, inventory: &Inventory) -> bool {
    let max_total_inventory = cfg.effective_max_total_inventory();
    if max_total_inventory <= 0.0 {
        return false;
    }
    let total_inventory = inventory.effective_up() + inventory.effective_down();
    total_inventory > max_total_inventory + 1e-9
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg() -> Config {
        Config::from_env().expect("config")
    }

    #[test]
    fn max_total_inventory_allows_exact_limit() {
        let mut cfg = test_cfg();
        cfg.max_total_inventory = 40.0;
        let inv = Inventory {
            up_shares: 20.0,
            down_shares: 20.0,
            ..Default::default()
        };

        assert!(!max_total_inventory_exceeded(&cfg, &inv));
    }

    #[test]
    fn max_total_inventory_blocks_above_limit() {
        let mut cfg = test_cfg();
        cfg.max_total_inventory = 40.0;
        let inv = Inventory {
            up_shares: 20.01,
            down_shares: 20.0,
            ..Default::default()
        };

        assert!(max_total_inventory_exceeded(&cfg, &inv));
    }

    #[test]
    fn snapshot_must_catch_up_before_releasing_reconcile_gate() {
        let inv = Inventory {
            up_shares: 20.0,
            down_shares: 5.0,
            ..Default::default()
        };
        let stale = PositionSnapshot {
            up_shares: 15.0,
            down_shares: 5.0,
            ..Default::default()
        };
        let fresh = PositionSnapshot {
            up_shares: 20.0,
            down_shares: 5.0,
            ..Default::default()
        };

        assert!(!snapshot_covers_inventory(
            &stale,
            &inv,
            "up-token",
            "down-token"
        ));
        assert!(snapshot_covers_inventory(
            &fresh,
            &inv,
            "up-token",
            "down-token"
        ));
    }

    #[test]
    fn snapshot_ignores_unknown_token_side() {
        let inv = Inventory {
            up_shares: 20.0,
            down_shares: 5.0,
            ..Default::default()
        };
        let snap = PositionSnapshot {
            up_shares: 20.0,
            down_shares: 0.0,
            ..Default::default()
        };

        assert!(snapshot_covers_inventory(&snap, &inv, "up-token", ""));
    }
}
