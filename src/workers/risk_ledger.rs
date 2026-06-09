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
use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};

use super::state::{
    log_jsonl, request_stop, spawn_jsonl_writer, stopping, LedgerEvent, LedgerRx, SharedInventory,
    StopFlag,
};

const RECV_TIMEOUT: Duration = Duration::from_millis(250);

pub fn run(cfg: Config, stop: StopFlag, inventory: SharedInventory, rx: LedgerRx) -> AppResult<()> {
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
                heartbeat(&cfg, "risk-ledger", format!("position reconciler off: {err}"))?;
                None
            }
        }
    } else {
        None
    };
    let mut last_position_sync = Instant::now();
    let mut up_token = cfg.polymarket_up_token_id.clone();
    let mut down_token = cfg.polymarket_down_token_id.clone();

    while !stopping(&stop, &cfg) {
        // Periodic on-chain position reconciliation (real mode only).
        if let Some(reconciler) = &position_reconciler {
            if cfg.reconcile_interval_ms > 0
                && last_position_sync.elapsed() >= Duration::from_millis(cfg.reconcile_interval_ms)
                && !up_token.trim().is_empty()
                && !down_token.trim().is_empty()
            {
                last_position_sync = Instant::now();
                match reconciler.fetch(&up_token, &down_token) {
                    Ok(snap) => {
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

        match rx.recv_timeout(RECV_TIMEOUT) {
            Ok(LedgerEvent::Accepted(accepted)) => {
                // Learn the current market's Up/Down token ids for reconcile.
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
            Err(RecvTimeoutError::Timeout) => {
                heartbeat(&cfg, "risk-ledger", "waiting")?;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}

/// Overwrite locally-derived filled inventory with authoritative on-chain
/// holdings when they diverge beyond tolerance. Cost basis comes from the Data
/// API's initial_value, so PnL stays correct. Pending (resting) orders are NOT
/// touched — they aren't on-chain yet. Returns true if a correction was applied.
fn reconcile_inventory_with_chain(
    cfg: &Config,
    inventory: &mut Inventory,
    snap: &PositionSnapshot,
) -> AppResult<bool> {
    const TOL: f64 = 0.5;
    let drift_up = (snap.up_shares - inventory.up_shares).abs();
    let drift_down = (snap.down_shares - inventory.down_shares).abs();
    if drift_up < TOL && drift_down < TOL {
        return Ok(false);
    }
    heartbeat(
        cfg,
        "risk-ledger",
        format!(
            "position reconcile: up {:.0}->{:.0} down {:.0}->{:.0}",
            inventory.up_shares, snap.up_shares, inventory.down_shares, snap.down_shares
        ),
    )?;
    inventory.up_shares = snap.up_shares;
    inventory.up_cost = snap.up_cost;
    inventory.down_shares = snap.down_shares;
    inventory.down_cost = snap.down_cost;
    inventory.ts_ms = now_ms();
    Ok(true)
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
    if cfg.max_total_inventory > 0.0 && total_inventory >= cfg.max_total_inventory {
        heartbeat(
            cfg,
            "risk-ledger",
            format!("kill switch: inventory {total_inventory:.0}"),
        )?;
        request_stop(stop, cfg)?;
    }
    Ok(())
}
