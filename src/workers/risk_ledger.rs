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
    // P2: set when the gateway reports an unmatched account fill — forces an
    // immediate reconcile on the next loop, bypassing the interval timer.
    let mut force_reconcile = false;

    while !stopping(&stop, &cfg) {
        // Periodic on-chain position reconciliation (real mode only).
        if let Some(reconciler) = &position_reconciler {
            let interval_due = cfg.reconcile_interval_ms > 0
                && last_position_sync.elapsed() >= Duration::from_millis(cfg.reconcile_interval_ms);
            let pair_ready = !up_token.trim().is_empty()
                && !down_token.trim().is_empty()
                && !current_market.is_empty();
            if (interval_due || force_reconcile) && pair_ready {
                last_position_sync = Instant::now();
                // Only reconcile when BOTH tokens belong to the SAME market the
                // shared inventory is currently on — never mix new/old windows.
                let market_matches = { inventory.lock().unwrap().market == current_market };
                if market_matches {
                    force_reconcile = false;
                    match reconciler.fetch(&up_token, &down_token) {
                        Ok(snap) => {
                            // Reconcile confirmed the on-chain position → release
                            // any placement pause set by a prior unmatched fill.
                            reconcile_gate.store(false, Ordering::Relaxed);
                            let corrected = {
                                let mut inv = inventory.lock().unwrap();
                                reconcile_inventory_with_chain(&cfg, &mut inv, &snap)?
                            };
                            if corrected {
                                let inv = inventory.lock().unwrap();
                                write_json(&cfg.inventory_path(), &*inv)?;
                                check_kill_switch(&cfg, &stop, &inv)?;
                            }
                        }
                        Err(err) => heartbeat(
                            &cfg,
                            "risk-ledger",
                            format!("position reconcile failed: {err}"),
                        )?,
                    }
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
                let enriched = FillEvent {
                    quote_id: fill.quote_id,
                    ts_ms: now_ms(),
                    market: fill.market,
                    side: fill.side,
                    price: fill.price,
                    size: fill.size,
                    inventory_up: inv.up_shares,
                    inventory_down: inv.down_shares,
                    pnl_if_up: inv.pnl_if_up(),
                    pnl_if_down: inv.pnl_if_down(),
                    source: fill.source,
                };
                log_jsonl(&logger, &cfg.fills_path(), &enriched)?;
                write_json(&cfg.inventory_path(), &*inv)?;
                check_kill_switch(&cfg, &stop, &inv)?;
                drop(inv);
                heartbeat(&cfg, "risk-ledger", "fill accounted")?;
            }
            Ok(LedgerEvent::UnmatchedFill) => {
                // Gateway saw an account fill it couldn't match to a known order.
                // PAUSE gateway placement (the shared inventory under-counts the
                // real position until we reconcile) and force an immediate
                // on-chain reconcile; the gate is released once it succeeds.
                reconcile_gate.store(true, Ordering::Relaxed);
                force_reconcile = true;
                heartbeat(
                    &cfg,
                    "risk-ledger",
                    "unmatched fill -> pausing placement + forcing reconcile",
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
    if cfg.max_total_inventory <= 0.0 {
        return false;
    }
    let total_inventory = inventory.effective_up() + inventory.effective_down();
    total_inventory > cfg.max_total_inventory + 1e-9
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
}
