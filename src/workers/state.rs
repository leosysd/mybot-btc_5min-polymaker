//! Shared state and channel plumbing for the single-process bot.
//!
//! The whole point of the thread refactor lives here: ONE authoritative
//! `Inventory` behind `Arc<Mutex<_>>`, shared by the worker threads, plus
//! `std::sync::mpsc` channels replacing the old lossy Unix datagram sockets.

use crate::config::Config;
use crate::ipc::{
    now_ms, should_stop, FillEvent, Inventory, MarketFrame, OrderAccepted, OrderCancelled,
    QuoteIntent,
};
use crate::AppResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Error as WsError, WebSocket};

/// The single authoritative inventory for the running bot. Created once in the
/// orchestrator and cloned to the threads that read/write it.
pub type SharedInventory = Arc<Mutex<Inventory>>;

/// Cross-thread stop flag. Set by the orchestrator (which also watches the
/// on-disk STOP file) and checked by every worker loop.
pub type StopFlag = Arc<AtomicBool>;

/// Set TRUE by risk when the gateway reports an unmatched account fill, to pause
/// the gateway's order placement until an on-chain reconcile confirms the true
/// position (cleared by risk after a successful reconcile). The gateway also
/// self-clears it after a timeout so a reconcile that can't run never deadlocks
/// placement. Prevents placing into an under-counted `held` during the gap.
pub type ReconcileGate = Arc<AtomicBool>;

/// Returns true if the bot should stop: either the in-process flag is set OR the
/// on-disk STOP file exists (so the CLI `polymaker stop` still works).
pub fn stopping(stop: &StopFlag, cfg: &Config) -> bool {
    stop.load(Ordering::Relaxed) || should_stop(cfg)
}

/// Write the on-disk STOP file (so external supervisors / the CLI see the stop).
pub fn write_stop_file(cfg: &Config) -> AppResult<()> {
    cfg.ensure_dirs()?;
    std::fs::write(cfg.stop_file(), now_ms().to_string())?;
    Ok(())
}

/// Request a full bot stop from inside a worker (kill switch / fatal staleness):
/// set the in-process flag AND persist the STOP file so every thread and any
/// external watcher converge on stop.
pub fn request_stop(stop: &StopFlag, cfg: &Config) -> AppResult<()> {
    stop.store(true, Ordering::Relaxed);
    write_stop_file(cfg)
}

// ── Channels ────────────────────────────────────────────────────────────────

/// collector -> quote_engine
pub type MarketTx = mpsc::Sender<MarketFrame>;
pub type MarketRx = mpsc::Receiver<MarketFrame>;

/// quote_engine -> order_gateway
pub type QuoteTx = mpsc::Sender<QuoteIntent>;
pub type QuoteRx = mpsc::Receiver<QuoteIntent>;

/// order_gateway -> risk_ledger (jsonl logging + kill switch + inventory.json).
/// risk_ledger does NOT mutate inventory for these; the gateway already did.
#[derive(Debug, Clone)]
pub enum LedgerEvent {
    Accepted(OrderAccepted),
    Cancelled(OrderCancelled),
    Filled(FillEvent),
    /// An account fill arrived that the gateway could not match to a known order
    /// (pre-insert race or restart/state gap). Tells risk to force an immediate
    /// on-chain position reconcile so the shared inventory is corrected at once.
    UnmatchedFill,
}

pub type LedgerTx = mpsc::Sender<LedgerEvent>;
pub type LedgerRx = mpsc::Receiver<LedgerEvent>;

/// user-WS thread -> order_gateway main loop. The gateway main loop owns the
/// `resting` map; the user-WS thread updates the shared inventory directly but
/// needs to tell the loop to drop/shrink the matching resting order so pending
/// is recomputed correctly.
#[derive(Debug, Clone)]
pub enum GatewayEvent {
    Fill(FillEvent),
    Cancelled(OrderCancelled),
}

pub type GatewayTx = mpsc::Sender<GatewayEvent>;
pub type GatewayRx = mpsc::Receiver<GatewayEvent>;

// ── Shared order-tracking structs ─────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestingOrder {
    pub quote_id: String,
    pub exchange_order_id: Option<String>,
    pub market: String,
    pub condition_id: String,
    pub token_id: String,
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub expires_ts_ms: u64,
}

#[derive(Clone)]
pub struct QuoteMeta {
    pub quote_id: String,
    pub market: String,
    pub condition_id: String,
    pub side: String,
    pub price: f64,
    pub size: f64,
    /// Set true when this order's fill was already credited to held via the
    /// cancel-detected-fill path (a TTL/requote cancel found it already filled).
    /// The async user-WS fill then skips re-crediting to avoid double-counting.
    pub credited: bool,
}

/// Tracks exchange-order-id -> quote metadata for live orders, shared between
/// the gateway main loop and the user-WS thread (same process).
pub type SharedOrderMap = Arc<Mutex<HashMap<String, QuoteMeta>>>;

/// Cheap deterministic RNG for the dry-run simulated-fill path.
pub struct FastRng(u64);

impl FastRng {
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub fn next_unit(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let x = self.0 >> 11;
        (x as f64) / ((1u64 << 53) as f64)
    }
}

// ── Shared-inventory helpers (the cap is built on these) ──────────────────────

/// Recompute pending_up / pending_down from the gateway's `resting` map and
/// write them into the shared inventory under the lock. Called whenever
/// `resting` changes so the cap counts outstanding orders, not just fills.
pub fn recompute_pending_from_resting(
    inv: &mut Inventory,
    resting: &HashMap<String, RestingOrder>,
) {
    let mut pending_up = 0.0;
    let mut pending_down = 0.0;
    for order in resting.values() {
        if order.market != inv.market {
            continue;
        }
        match order.side.as_str() {
            "Up" => pending_up += order.size,
            "Down" => pending_down += order.size,
            _ => {}
        }
    }
    if pending_up.abs() < 0.001 {
        pending_up = 0.0;
    }
    if pending_down.abs() < 0.001 {
        pending_down = 0.0;
    }
    inv.pending_up = pending_up;
    inv.pending_down = pending_down;
    inv.ts_ms = now_ms();
}

/// Reset the shared inventory for a new market window (zero shares/cost/pending,
/// set market). No-op if the market is unchanged or empty.
pub fn reset_inventory_for_market(inv: &mut Inventory, market: &str) {
    if market.is_empty() || inv.market == market {
        return;
    }
    *inv = Inventory {
        market: market.to_string(),
        ..Default::default()
    };
}

/// Filled shares currently held on `side` for `market` (0 if market differs).
pub fn held_shares(inv: &Inventory, market: &str, side: &str) -> f64 {
    if inv.market != market {
        return 0.0;
    }
    match side {
        "Up" => inv.up_shares,
        "Down" => inv.down_shares,
        _ => 0.0,
    }
}

/// Pending (outstanding live order) shares on `side` for `market`.
pub fn pending_shares(inv: &Inventory, market: &str, side: &str) -> f64 {
    if inv.market != market {
        return 0.0;
    }
    match side {
        "Up" => inv.pending_up,
        "Down" => inv.pending_down,
        _ => 0.0,
    }
}

/// Average filled price of the side OPPOSITE to `side`, or None if no opposite
/// inventory. Used by the gateway's cost-basis lock.
pub fn opposite_avg_cost(inv: &Inventory, market: &str, side: &str) -> Option<f64> {
    if inv.market != market {
        return None;
    }
    let (shares, cost) = match side {
        "Up" => (inv.down_shares, inv.down_cost),
        "Down" => (inv.up_shares, inv.up_cost),
        _ => return None,
    };
    if shares > 0.0 {
        Some(cost / shares)
    } else {
        None
    }
}

// ── Shared plumbing: sleep, async jsonl writer, websocket helpers ─────────────

pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms.max(1)));
}

/// Async append-only jsonl writer. A dedicated thread serializes file writes so
/// the worker loops never block on disk. Used for book/quotes/fills logs.
#[derive(Clone)]
pub struct AsyncJsonlWriter {
    tx: mpsc::Sender<JsonlJob>,
}

struct JsonlJob {
    path: PathBuf,
    line: String,
}

pub fn spawn_jsonl_writer() -> AsyncJsonlWriter {
    let (tx, rx) = mpsc::channel::<JsonlJob>();
    thread::spawn(move || {
        while let Ok(job) = rx.recv() {
            if let Some(parent) = job.path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&job.path) {
                let _ = file.write_all(job.line.as_bytes());
                let _ = file.write_all(b"\n");
            }
        }
    });
    AsyncJsonlWriter { tx }
}

pub fn log_jsonl<T: Serialize>(writer: &AsyncJsonlWriter, path: &Path, value: &T) -> AppResult<()> {
    let line = serde_json::to_string(value)?;
    writer
        .tx
        .send(JsonlJob {
            path: path.to_path_buf(),
            line,
        })
        .map_err(|err| format!("jsonl writer stopped: {err}").into())
}

pub type WsSocket = WebSocket<MaybeTlsStream<TcpStream>>;

pub fn tune_ws_socket(socket: &mut WsSocket, timeout: Duration) -> AppResult<()> {
    match socket.get_mut() {
        MaybeTlsStream::Plain(stream) => {
            stream.set_nodelay(true)?;
            stream.set_read_timeout(Some(timeout))?;
        }
        MaybeTlsStream::Rustls(stream) => {
            stream.sock.set_nodelay(true)?;
            stream.sock.set_read_timeout(Some(timeout))?;
        }
        _ => {}
    }
    Ok(())
}

pub fn is_ws_timeout(err: &WsError) -> bool {
    matches!(
        err,
        WsError::Io(io_err)
            if matches!(
                io_err.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            )
    )
}
