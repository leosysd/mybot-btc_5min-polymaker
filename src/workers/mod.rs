//! Single-process orchestrator.
//!
//! The bot used to be 4 OS processes talking over lossy Unix datagram sockets,
//! with inventory split across them — which let the order gateway's view lag and
//! over-accumulate. It is now ONE process with 4 worker THREADS sharing one
//! authoritative `Arc<Mutex<Inventory>>` and communicating over in-process
//! `std::sync::mpsc` channels. All strategy/pricing/order logic is unchanged;
//! only the plumbing (process->thread, socket->channel, split->shared inventory)
//! changed. This eliminates the drop/lag class of bugs.

mod collector;
mod order_gateway;
mod quote_engine;
mod risk_ledger;
mod state;

use crate::config::Config;
use crate::ipc::{heartbeat, read_json, Inventory};
use crate::AppResult;
use std::collections::HashMap;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use state::{RestingOrder, SharedInventory, StopFlag};

/// Run the full single-process bot: spawn the 4 worker threads, wire channels,
/// supervise, then signal stop and join. Replaces the old multi-process
/// supervisor (no child processes, no per-role sockets).
pub fn run_supervisor(cfg: Config, seconds: Option<u64>) -> AppResult<()> {
    cfg.ensure_trading_supported()?;
    cfg.ensure_dirs()?;
    if cfg.data_mode == "live" {
        cfg.ensure_live_market_config()?;
    }
    claim_supervisor_pid(&cfg)?;
    let _ = std::fs::remove_file(cfg.stop_file());

    let result = run_bot(cfg.clone(), seconds);
    release_supervisor_pid(&cfg);
    result
}

fn run_bot(cfg: Config, seconds: Option<u64>) -> AppResult<()> {
    // One authoritative inventory, loaded from disk and reset to zero pending
    // (pending is now derived from the gateway's live resting map).
    let mut inv = read_json::<Inventory>(&cfg.inventory_path())?.unwrap_or_else(|| Inventory {
        market: cfg.market_slug.clone(),
        ..Default::default()
    });
    inv.pending_up = 0.0;
    inv.pending_down = 0.0;
    let inventory: SharedInventory = Arc::new(Mutex::new(inv));

    let stop: StopFlag = Arc::new(AtomicBool::new(false));
    // Pauses gateway placement after an unmatched fill until risk reconciles.
    let reconcile_gate: state::ReconcileGate = Arc::new(AtomicBool::new(false));

    // Channels: collector -> engine -> gateway -> risk.
    let (market_tx, market_rx) = mpsc::channel();
    let (quote_tx, quote_rx) = mpsc::channel();
    let (ledger_tx, ledger_rx) = mpsc::channel();

    let mut handles: Vec<(&'static str, thread::JoinHandle<AppResult<()>>)> = Vec::new();

    {
        let cfg = cfg.clone();
        let stop = Arc::clone(&stop);
        handles.push((
            "collector",
            thread::spawn(move || collector::run(cfg, stop, market_tx)),
        ));
    }
    {
        let cfg = cfg.clone();
        let stop = Arc::clone(&stop);
        let inventory = Arc::clone(&inventory);
        handles.push((
            "quote-engine",
            thread::spawn(move || quote_engine::run(cfg, stop, inventory, market_rx, quote_tx)),
        ));
    }
    {
        let cfg = cfg.clone();
        let stop = Arc::clone(&stop);
        let inventory = Arc::clone(&inventory);
        let ledger_tx = ledger_tx.clone();
        let reconcile_gate = Arc::clone(&reconcile_gate);
        handles.push((
            "order-gateway",
            thread::spawn(move || {
                order_gateway::run(cfg, stop, inventory, quote_rx, ledger_tx, reconcile_gate)
            }),
        ));
    }
    {
        let cfg = cfg.clone();
        let stop = Arc::clone(&stop);
        let inventory = Arc::clone(&inventory);
        let reconcile_gate = Arc::clone(&reconcile_gate);
        handles.push((
            "risk-ledger",
            thread::spawn(move || {
                risk_ledger::run(cfg, stop, inventory, ledger_rx, reconcile_gate)
            }),
        ));
    }
    // Drop the orchestrator's spare ledger sender so the risk thread's channel
    // closes once the gateway thread exits.
    drop(ledger_tx);

    // Supervise: heartbeat, watch the on-disk STOP file and the optional seconds
    // limit, and notice any worker thread that finished (panicked or returned).
    let started = Instant::now();
    loop {
        if state::stopping(&stop, &cfg) {
            break;
        }
        if let Some(limit) = seconds {
            if started.elapsed() >= Duration::from_secs(limit) {
                break;
            }
        }
        // If any worker has already finished, stop the whole bot. The outer
        // supervisor (PM2 / `polymaker start`) restarts the single process — far
        // simpler and safer than restarting one thread mid-flight with shared
        // state half-updated.
        if handles.iter().any(|(_, h)| h.is_finished()) {
            eprintln!("[supervisor] a worker thread finished; stopping bot");
            break;
        }
        let _ = heartbeat(&cfg, "supervisor", "running");
        thread::sleep(Duration::from_millis(500));
    }

    // Signal stop to all threads (in-process flag + on-disk STOP file) and join.
    stop.store(true, Ordering::Relaxed);
    let _ = state::write_stop_file(&cfg);

    for (role, handle) in handles {
        match handle.join() {
            Ok(Ok(())) => eprintln!("[supervisor] stopped {role}"),
            Ok(Err(err)) => eprintln!("[supervisor] {role} error: {err}"),
            Err(_) => eprintln!("[supervisor] {role} panicked"),
        }
    }
    Ok(())
}

/// Single-process mode: the old per-role processes no longer exist. Running the
/// whole bot is `supervisor`. Keep the subcommands working (exit 0) with a
/// clear message instead of silently doing the wrong thing.
fn single_process_notice(role: &str) {
    eprintln!(
        "[{role}] single-process mode: workers are now threads. \
         Use `polymaker supervisor` (or `polymaker start`) to run the whole bot."
    );
}

pub fn run_market_data(cfg: Config) -> AppResult<()> {
    let _ = cfg;
    single_process_notice("collector");
    Ok(())
}

pub fn run_fair_value(cfg: Config) -> AppResult<()> {
    let _ = cfg;
    single_process_notice("quote-engine");
    Ok(())
}

pub fn run_maker(cfg: Config) -> AppResult<()> {
    let _ = cfg;
    single_process_notice("order-gateway");
    Ok(())
}

pub fn run_risk(cfg: Config) -> AppResult<()> {
    let _ = cfg;
    single_process_notice("risk-ledger");
    Ok(())
}

pub fn write_stop(cfg: &Config) -> AppResult<()> {
    state::write_stop_file(cfg)
}

pub fn start_background(cfg: &Config) -> AppResult<()> {
    cfg.ensure_trading_supported()?;
    cfg.ensure_dirs()?;
    if let Some(pid) = live_supervisor_pid(cfg)? {
        println!("后台服务已在运行 pid={pid}");
        return Ok(());
    }
    let _ = std::fs::remove_file(cfg.stop_file());
    let exe = std::env::current_exe()?;
    let log_path = cfg.run_dir.join("supervisor.log");
    let out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let err = out.try_clone()?;
    let child = Command::new(exe)
        .arg("supervisor")
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()?;
    std::fs::write(cfg.pid_file(), child.id().to_string())?;
    println!(
        "后台服务已启动 pid={} 日志={}",
        child.id(),
        log_path.display()
    );
    Ok(())
}

pub fn restart_background(cfg: &Config) -> AppResult<()> {
    write_stop(cfg)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if live_supervisor_pid(cfg)?.is_none() {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }
    if let Some(pid) = live_supervisor_pid(cfg)? {
        return Err(format!("旧 supervisor 仍在运行 pid={pid}，请先确认后再启动").into());
    }
    start_background(cfg)
}

pub fn clean_run_dir(cfg: &Config) -> AppResult<()> {
    // Guard: never wipe the run dir while it still records live resting orders —
    // that file is the recovery map used to cancel them on restart. Deleting it
    // would orphan real orders on the exchange.
    if let Ok(Some(active)) = read_json::<HashMap<String, RestingOrder>>(&cfg.active_orders_path())
    {
        if !active.is_empty() {
            return Err(format!(
                "检测到 {} 条活跃实盘挂单记录 ({})，拒绝清空。请先 `polymaker stop` \
                 让网关撤单后再 clean。",
                active.len(),
                cfg.active_orders_path().display()
            )
            .into());
        }
    }
    if cfg.run_dir.exists() {
        std::fs::remove_dir_all(&cfg.run_dir)?;
    }
    Ok(())
}

// ── Single-instance pid guard (cross-platform) ────────────────────────────────

fn claim_supervisor_pid(cfg: &Config) -> AppResult<()> {
    let current = std::process::id();
    if let Some(pid) = live_supervisor_pid(cfg)? {
        if pid != current {
            return Err(format!("supervisor 已在运行 pid={pid}").into());
        }
    }
    std::fs::write(cfg.pid_file(), current.to_string())?;
    Ok(())
}

fn release_supervisor_pid(cfg: &Config) {
    let current = std::process::id();
    if let Ok(Some(pid)) = read_pid(&cfg.pid_file()) {
        if pid == current {
            let _ = std::fs::remove_file(cfg.pid_file());
        }
    }
}

fn live_supervisor_pid(cfg: &Config) -> AppResult<Option<u32>> {
    let Some(pid) = read_pid(&cfg.pid_file())? else {
        return Ok(None);
    };
    if process_alive(pid) {
        Ok(Some(pid))
    } else {
        let _ = std::fs::remove_file(cfg.pid_file());
        Ok(None)
    }
}

fn read_pid(path: &Path) -> AppResult<Option<u32>> {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return Ok(None);
    };
    Ok(raw.trim().parse::<u32>().ok())
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    std::path::PathBuf::from(format!("/proc/{pid}")).exists()
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    // Best-effort: query tasklist for the pid. If we can't determine, assume the
    // process is alive (conservative — avoids double-starting a live supervisor).
    match Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH"])
        .output()
    {
        Ok(out) => String::from_utf8_lossy(&out.stdout).contains(&pid.to_string()),
        Err(_) => true,
    }
}

#[cfg(not(any(unix, windows)))]
fn process_alive(_pid: u32) -> bool {
    true
}
