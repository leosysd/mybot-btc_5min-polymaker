#[cfg(unix)]
mod unix {
    use crate::config::Config;
    use crate::ipc::{
        heartbeat, now_ms, read_json, should_stop, write_json, FillEvent, Inventory, MarketFrame,
        OrderAccepted, OrderCancelled, QuoteIntent, WireMessage,
    };
    use crate::pricing::{
        digital_p_up, half_spread, market_maker_bids, phase_for, post_only_bid, price_sensitivity,
        side_allowed, time_boosted_skew, uncertainty_width, MmParams, Phase, SpreadInputs,
        ToxicityMonitor, VolEstimator,
    };
    use crate::real_orders::{PositionReconciler, PositionSnapshot, RealOrderClient, UserWsCredentials};
    use crate::AppResult;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::collections::{HashMap, HashSet};
    use std::fs::OpenOptions;
    use std::io::{self, Write};
    use std::net::TcpStream;
    use std::os::unix::net::UnixDatagram;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::sync::{mpsc, Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};
    use tungstenite::stream::MaybeTlsStream;
    use tungstenite::{connect, Error as WsError, Message, WebSocket};

    pub fn run_supervisor(cfg: Config, seconds: Option<u64>) -> AppResult<()> {
        cfg.ensure_trading_supported()?;
        cfg.ensure_dirs()?;
        claim_supervisor_pid(&cfg)?;
        let _ = std::fs::remove_file(cfg.stop_file());
        remove_socket(&cfg.engine_socket());
        remove_socket(&cfg.gateway_socket());
        remove_socket(&cfg.risk_socket());

        let exe = std::env::current_exe()?;
        let specs = ["risk-ledger", "order-gateway", "quote-engine", "collector"];
        let mut children = Vec::new();
        for role in specs {
            children.push((role.to_string(), spawn_role(&exe, role)?));
        }

        let started = Instant::now();
        loop {
            if should_stop(&cfg) {
                break;
            }
            if let Some(limit) = seconds {
                if started.elapsed() >= Duration::from_secs(limit) {
                    break;
                }
            }

            for (role, child) in &mut children {
                if let Some(status) = child.try_wait()? {
                    eprintln!("[supervisor] {role} exited with {status}; restarting");
                    *child = spawn_role(&exe, role)?;
                }
            }
            heartbeat(&cfg, "supervisor", "running")?;
            thread::sleep(Duration::from_millis(500));
        }

        write_stop(&cfg)?;
        let deadline = Instant::now() + Duration::from_secs(12);
        while Instant::now() < deadline {
            let mut all_stopped = true;
            for (_, child) in &mut children {
                if child.try_wait()?.is_none() {
                    all_stopped = false;
                }
            }
            if all_stopped {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        for (role, mut child) in children {
            if child.try_wait()?.is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
            eprintln!("[supervisor] stopped {role}");
        }
        release_supervisor_pid(&cfg);
        Ok(())
    }

    pub fn run_market_data(cfg: Config) -> AppResult<()> {
        cfg.ensure_trading_supported()?;
        cfg.ensure_dirs()?;
        if cfg.data_mode == "live" {
            cfg.ensure_live_market_config()?;
        }
        wait_for_socket(&cfg.engine_socket(), Duration::from_secs(5))?;
        if cfg.data_mode == "live" {
            while !should_stop(&cfg) {
                match run_live_market_data(cfg.clone()) {
                    Ok(()) => return Ok(()),
                    Err(err) => {
                        let _ =
                            heartbeat(&cfg, "collector", format!("live collector retry: {err}"));
                        sleep_ms(1_000);
                    }
                }
            }
            return Ok(());
        }
        run_sim_market_data(cfg)
    }

    fn run_sim_market_data(cfg: Config) -> AppResult<()> {
        let sock = UnixDatagram::unbound()?;
        let logger = spawn_jsonl_writer();
        let mut step = 0u64;
        let center = cfg.price_to_beat;
        let window = cfg.market_window_secs.max(1);
        let dt_sec = (cfg.market_interval_ms.max(1) as f64) / 1000.0;
        let mut vol = VolEstimator::new(
            cfg.vol_seed_per_sqrt_sec,
            cfg.vol_halflife_sec,
            cfg.vol_min_per_sqrt_sec,
            cfg.vol_max_per_sqrt_sec,
        );
        // Each window mimics a real 5-min market: price_to_beat is locked at the
        // BTC price when the window opens, then tau counts down to settlement.
        let mut window_start = (now_ms() / 1000) / window * window;
        let mut price_to_beat = center;
        let mut window_initialized = false;

        while !should_stop(&cfg) {
            let phase = step as f64 / 9.0;
            let btc_price = center + 42.0 * phase.sin() + 15.0 * (phase * 0.37).cos();
            let vol_now = vol.update(btc_price, dt_sec);

            let now_s = now_ms() / 1000;
            let this_window = now_s / window * window;
            if !window_initialized || this_window != window_start {
                window_start = this_window;
                price_to_beat = btc_price; // lock the beat at the open
                window_initialized = true;
            }
            let window_end_ms = (window_start + window) * 1000;
            let tau_seconds = ((window_end_ms.saturating_sub(now_ms())) as f64 / 1000.0).max(0.0);

            // Bracket the time-aware fair value so the simulated book is sensible.
            let w = uncertainty_width(vol_now, tau_seconds, cfg.width_floor_usd);
            let fair_up = digital_p_up(btc_price, price_to_beat, w);
            let micro_noise = 0.012 * (phase * 1.7).sin();
            let frame = MarketFrame {
                ts_ms: now_ms(),
                market: cfg.market_slug.clone(),
                condition_id: String::new(),
                up_token_id: cfg.polymarket_up_token_id.clone(),
                down_token_id: cfg.polymarket_down_token_id.clone(),
                up_ask: (fair_up + 0.035 + micro_noise).clamp(0.03, 0.97),
                down_ask: (1.0 - fair_up + 0.035 - micro_noise).clamp(0.03, 0.97),
                btc_price,
                price_to_beat,
                tau_seconds,
                vol_per_sqrt_sec: vol_now,
                source: "simulated_collector".to_string(),
            };
            match send_msg(
                &sock,
                &cfg.engine_socket(),
                &WireMessage::MarketFrame(frame.clone()),
            ) {
                Ok(()) => {
                    log_jsonl(&logger, &cfg.book_path(), &frame)?;
                    heartbeat(&cfg, "collector", format!("sent step={step}"))?;
                }
                Err(err) => {
                    heartbeat(&cfg, "collector", format!("engine socket wait: {err}"))?;
                }
            }
            step = step.wrapping_add(1);
            sleep_ms(cfg.market_interval_ms);
        }
        Ok(())
    }

    fn run_live_market_data(cfg: Config) -> AppResult<()> {
        let state = Arc::new(Mutex::new(LiveMarketState::new(&cfg)));
        let sink = LiveFrameSink {
            sock: Arc::new(Mutex::new(UnixDatagram::unbound()?)),
            engine_socket: cfg.engine_socket(),
            book_path: cfg.book_path(),
            logger: spawn_jsonl_writer(),
        };

        {
            let cfg = cfg.clone();
            let state = Arc::clone(&state);
            let sink = sink.clone();
            thread::spawn(move || live_polymarket_loop(cfg, state, sink));
        }
        if cfg.auto_discover_market {
            let cfg = cfg.clone();
            let state = Arc::clone(&state);
            thread::spawn(move || market_discovery_loop(cfg, state));
        }
        {
            let cfg = cfg.clone();
            let state = Arc::clone(&state);
            let sink = sink.clone();
            thread::spawn(move || live_btc_loop(cfg, state, sink));
        }

        while !should_stop(&cfg) {
            if state.lock().unwrap().frame(&cfg).is_none() {
                heartbeat(&cfg, "collector", "waiting live ws")?;
            }

            let now = now_ms();
            let stale = {
                let state = state.lock().unwrap();
                state.started
                    && state.market.is_some()
                    && (now.saturating_sub(state.last_polymarket_ts) > cfg.ws_stale_after_ms
                        || now.saturating_sub(state.last_btc_ts) > cfg.ws_stale_after_ms)
            };
            if stale {
                heartbeat(&cfg, "collector", "kill switch: live ws stale")?;
                write_stop(&cfg)?;
                break;
            }

            sleep_ms(250);
        }
        Ok(())
    }

    pub fn run_fair_value(cfg: Config) -> AppResult<()> {
        cfg.ensure_trading_supported()?;
        cfg.ensure_dirs()?;
        wait_for_socket(&cfg.gateway_socket(), Duration::from_secs(5))?;
        let sock = bind_socket(&cfg.engine_socket())?;
        let mut inventory =
            read_json::<Inventory>(&cfg.inventory_path())?.unwrap_or_else(|| Inventory {
                market: cfg.market_slug.clone(),
                ..Default::default()
            });
        let mut buf = [0u8; 16 * 1024];
        let mut last_market_ts = None::<u64>;
        // Adverse-selection monitor: watches whether fair value drifts against
        // us right after each fill, and asks for wider spreads when it does.
        let mut tox = ToxicityMonitor::new(
            cfg.tox_horizon_ms,
            cfg.tox_decay,
            cfg.tox_k_widen,
            cfg.tox_max_widen,
        );
        let mut last_p_up = 0.5_f64;
        let mut prev_up = inventory.up_shares;
        let mut prev_down = inventory.down_shares;

        while !should_stop(&cfg) {
            match recv_msg(&sock, &mut buf)? {
                Some(WireMessage::MarketFrame(frame)) => {
                    last_market_ts = Some(frame.ts_ms);
                    if now_ms().saturating_sub(frame.ts_ms) > cfg.stale_after_ms {
                        heartbeat(&cfg, "quote-engine", "skipped stale market frame")?;
                        continue;
                    }
                    last_p_up = handle_market_frame(&cfg, &sock, &frame, &inventory, &mut tox)?;
                    heartbeat(&cfg, "quote-engine", "quoted")?;
                }
                Some(WireMessage::Inventory(next)) => {
                    // A rise in filled shares is a fill: feed the toxicity monitor
                    // with the fair value that was live when we got hit.
                    let now = now_ms();
                    if next.up_shares > prev_up + 1e-9 {
                        tox.on_fill(true, last_p_up, now);
                    }
                    if next.down_shares > prev_down + 1e-9 {
                        tox.on_fill(false, last_p_up, now);
                    }
                    prev_up = next.up_shares;
                    prev_down = next.down_shares;
                    inventory = next;
                    heartbeat(&cfg, "quote-engine", "inventory updated")?;
                }
                Some(_) => {}
                None => {
                    if let Some(ts) = last_market_ts {
                        let market_silence_ms = cfg.ws_stale_after_ms.max(cfg.stale_after_ms);
                        if now_ms().saturating_sub(ts) > market_silence_ms {
                            heartbeat(&cfg, "quote-engine", "kill switch: market stale")?;
                            write_stop(&cfg)?;
                            break;
                        }
                    }
                    heartbeat(&cfg, "quote-engine", "waiting")?;
                }
            }
        }
        Ok(())
    }

    pub fn run_maker(cfg: Config) -> AppResult<()> {
        cfg.ensure_trading_supported()?;
        cfg.ensure_dirs()?;
        wait_for_socket(&cfg.risk_socket(), Duration::from_secs(5))?;
        let sock = bind_socket(&cfg.gateway_socket())?;
        let mut buf = [0u8; 16 * 1024];
        let mut rng = FastRng::new(now_ms());
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
        if let Some(real_orders) = &real_orders {
            if !resting.is_empty() {
                heartbeat(
                    &cfg,
                    "order-gateway",
                    "startup cancel previous active orders",
                )?;
                cancel_all_resting_orders(
                    &cfg,
                    &sock,
                    real_orders,
                    &order_map,
                    &mut resting,
                    "startup_cleanup",
                )?;
            }
            // Authoritative sweep: cancel any order the exchange still has open
            // that we don't know about locally (e.g. our state file was lost, or
            // we crashed after placing but before persisting). Belt-and-suspenders
            // against orphaned live orders.
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
                real_orders.user_ws_credentials(),
                Arc::clone(&order_map),
            )?;
        }

        let mut last_reconcile = Instant::now();
        let mut last_prewarm = Instant::now();
        let mut primed: HashSet<String> = HashSet::new();
        let loop_result = (|| -> AppResult<()> {
            while !should_stop(&cfg) {
                let order_map_ref = real_orders.as_ref().map(|_| &order_map);
                expire_resting_orders(
                    &cfg,
                    &sock,
                    real_orders.as_ref(),
                    order_map_ref,
                    &mut resting,
                )?;
                if let Some(real_orders) = real_orders.as_ref() {
                    // Periodic exchange reconciliation: cancel orphaned orders.
                    if cfg.reconcile_interval_ms > 0
                        && last_reconcile.elapsed()
                            >= Duration::from_millis(cfg.reconcile_interval_ms)
                    {
                        reconcile_orphan_orders(&cfg, real_orders, &resting)?;
                        last_reconcile = Instant::now();
                    }
                    // Keep the CLOB connection hot (Cloudflare cuts idle ~90s).
                    if cfg.enable_prewarm
                        && cfg.prewarm_interval_ms > 0
                        && last_prewarm.elapsed()
                            >= Duration::from_millis(cfg.prewarm_interval_ms)
                    {
                        real_orders.prewarm();
                        last_prewarm = Instant::now();
                    }
                }
                match recv_msg(&sock, &mut buf)? {
                    Some(WireMessage::QuoteIntent(quote)) => {
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
                        if should_keep_resting(&cfg, resting.get(&quote.side), &quote) {
                            heartbeat(&cfg, "order-gateway", "kept resting quote")?;
                            continue;
                        }
                        if let Some(old) = resting.remove(&quote.side) {
                            resting.insert(quote.side.clone(), old);
                            cancel_resting_side(
                                &cfg,
                                &sock,
                                real_orders.as_ref(),
                                Some(&order_map),
                                &mut resting,
                                &quote.side,
                                "requote",
                            )?;
                        }
                        if !accept_resting_order(
                            &cfg,
                            &sock,
                            real_orders.as_ref(),
                            order_map_ref,
                            &mut resting,
                            quote.clone(),
                        )? {
                            continue;
                        }
                        if real_orders.is_none() && rng.next_unit() <= cfg.sim_fill_chance {
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
                            let risk = WireMessage::FillEvent(fill);
                            send_msg(&sock, &cfg.risk_socket(), &risk)?;
                            resting.remove(&quote.side);
                            heartbeat(&cfg, "order-gateway", "simulated fill")?;
                        } else if real_orders.is_some() {
                            heartbeat(&cfg, "order-gateway", "real order resting")?;
                        } else {
                            heartbeat(&cfg, "order-gateway", "resting quote")?;
                        }
                    }
                    Some(WireMessage::FillEvent(fill)) => {
                        if apply_fill_to_resting(&cfg, real_orders.as_ref(), &mut resting, &fill)? {
                            heartbeat(&cfg, "order-gateway", "resting fill synced")?;
                        }
                    }
                    Some(WireMessage::OrderCancelled(cancel)) => {
                        if apply_cancel_to_resting(
                            &cfg,
                            real_orders.as_ref(),
                            &mut resting,
                            &cancel,
                        )? {
                            heartbeat(&cfg, "order-gateway", "resting cancel synced")?;
                        }
                    }
                    Some(_) => {}
                    None => heartbeat(&cfg, "order-gateway", "waiting")?,
                }
            }
            Ok(())
        })();
        let cleanup_result = if let Some(real_orders) = &real_orders {
            cancel_all_resting_orders(
                &cfg,
                &sock,
                real_orders,
                &order_map,
                &mut resting,
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

    pub fn run_risk(cfg: Config) -> AppResult<()> {
        cfg.ensure_trading_supported()?;
        cfg.ensure_dirs()?;
        let sock = bind_socket(&cfg.risk_socket())?;
        let mut inventory =
            read_json::<Inventory>(&cfg.inventory_path())?.unwrap_or_else(|| Inventory {
                market: cfg.market_slug.clone(),
                ..Default::default()
            });
        inventory.pending_up = 0.0;
        inventory.pending_down = 0.0;
        write_json(&cfg.inventory_path(), &inventory)?;
        let mut pending: Vec<PendingOrder> = Vec::new();
        let mut buf = [0u8; 16 * 1024];
        let logger = spawn_jsonl_writer();

        // Position reconciliation: periodically read the wallet's true on-chain
        // share holdings and correct the locally-derived inventory if a fill was
        // ever missed over the user WS. Only in real mode (Data API is public).
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

        while !should_stop(&cfg) {
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
                            if reconcile_inventory_with_chain(&cfg, &mut inventory, &pending, &snap)? {
                                write_json(&cfg.inventory_path(), &inventory)?;
                                send_msg(
                                    &sock,
                                    &cfg.engine_socket(),
                                    &WireMessage::Inventory(inventory.clone()),
                                )?;
                                check_kill_switch(&cfg, &inventory)?;
                            }
                        }
                        Err(err) => {
                            heartbeat(&cfg, "risk-ledger", format!("position reconcile failed: {err}"))?
                        }
                    }
                }
            }
            match recv_msg(&sock, &mut buf)? {
                Some(WireMessage::OrderAccepted(accepted)) => {
                    // Learn the current market's Up/Down token ids for reconcile.
                    if !accepted.quote.token_id.trim().is_empty() {
                        match accepted.quote.side.as_str() {
                            "Up" => up_token = accepted.quote.token_id.clone(),
                            "Down" => down_token = accepted.quote.token_id.clone(),
                            _ => {}
                        }
                    }
                    reset_inventory_for_market(
                        &mut inventory,
                        &mut pending,
                        &accepted.quote.market,
                    );
                    prune_pending(&mut pending, now_ms());
                    pending.push(PendingOrder {
                        quote_id: accepted.quote.quote_id.clone(),
                        market: accepted.quote.market.clone(),
                        side: accepted.quote.side.clone(),
                        size: accepted.quote.size,
                        expires_ts_ms: accepted.expires_ts_ms,
                    });
                    recompute_pending(&mut inventory, &pending);
                    log_jsonl(&logger, &cfg.quotes_path(), &accepted.quote)?;
                    write_json(&cfg.inventory_path(), &inventory)?;
                    send_msg(
                        &sock,
                        &cfg.engine_socket(),
                        &WireMessage::Inventory(inventory.clone()),
                    )?;
                    check_kill_switch(&cfg, &inventory)?;
                    heartbeat(&cfg, "risk-ledger", "order accepted")?;
                }
                Some(WireMessage::OrderCancelled(cancel)) => {
                    reset_inventory_for_market(&mut inventory, &mut pending, &cancel.market);
                    prune_pending(&mut pending, now_ms());
                    cancel_pending(&mut pending, &cancel.quote_id, &cancel.side, cancel.size);
                    recompute_pending(&mut inventory, &pending);
                    write_json(&cfg.inventory_path(), &inventory)?;
                    send_msg(
                        &sock,
                        &cfg.engine_socket(),
                        &WireMessage::Inventory(inventory.clone()),
                    )?;
                    check_kill_switch(&cfg, &inventory)?;
                    heartbeat(&cfg, "risk-ledger", format!("order {}", cancel.reason))?;
                }
                Some(WireMessage::FillEvent(fill)) => {
                    reset_inventory_for_market(&mut inventory, &mut pending, &fill.market);
                    prune_pending(&mut pending, now_ms());
                    consume_pending(&mut pending, &fill.quote_id, &fill.side, fill.size);
                    inventory.add_fill(&fill.market, &fill.side, fill.price, fill.size);
                    recompute_pending(&mut inventory, &pending);
                    let enriched = FillEvent {
                        quote_id: fill.quote_id,
                        ts_ms: now_ms(),
                        market: fill.market,
                        side: fill.side,
                        price: fill.price,
                        size: fill.size,
                        inventory_up: inventory.up_shares,
                        inventory_down: inventory.down_shares,
                        pnl_if_up: inventory.pnl_if_up(),
                        pnl_if_down: inventory.pnl_if_down(),
                        source: fill.source,
                    };
                    log_jsonl(&logger, &cfg.fills_path(), &enriched)?;
                    write_json(&cfg.inventory_path(), &inventory)?;
                    send_msg(
                        &sock,
                        &cfg.engine_socket(),
                        &WireMessage::Inventory(inventory.clone()),
                    )?;
                    check_kill_switch(&cfg, &inventory)?;
                    heartbeat(&cfg, "risk-ledger", "fill accounted")?;
                }
                Some(_) => {}
                None => {
                    if prune_pending(&mut pending, now_ms()) {
                        recompute_pending(&mut inventory, &pending);
                        write_json(&cfg.inventory_path(), &inventory)?;
                        send_msg(
                            &sock,
                            &cfg.engine_socket(),
                            &WireMessage::Inventory(inventory.clone()),
                        )?;
                        check_kill_switch(&cfg, &inventory)?;
                    }
                    heartbeat(&cfg, "risk-ledger", "waiting")?;
                }
            }
        }
        Ok(())
    }

    pub fn write_stop(cfg: &Config) -> AppResult<()> {
        cfg.ensure_dirs()?;
        std::fs::write(cfg.stop_file(), now_ms().to_string())?;
        Ok(())
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
        // Guard: never wipe the run dir while it still records live resting
        // orders — that file is the recovery map used to cancel them on restart.
        // Deleting it would orphan real orders on the exchange.
        if let Ok(Some(active)) =
            read_json::<HashMap<String, RestingOrder>>(&cfg.active_orders_path())
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

    /// Compute the time-aware quotes for one market frame and emit them.
    /// Returns the fair `p_up` used, so the caller can feed the toxicity monitor.
    fn handle_market_frame(
        cfg: &Config,
        sock: &UnixDatagram,
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

        // ── 5. Endgame protocol: which sides may be quoted at all right now.
        let phase = phase_for(tau, cfg.endgame_reduce_secs, cfg.endgame_pull_secs);
        let up_px = if side_allowed(true, phase, up_eff, down_eff) {
            post_only_bid(model.up_bid, frame.up_ask, cfg.tick_size, cfg.min_bid, cfg.max_bid)
        } else {
            None
        };
        let down_px = if side_allowed(false, phase, up_eff, down_eff) {
            post_only_bid(
                model.down_bid,
                frame.down_ask,
                cfg.tick_size,
                cfg.min_bid,
                cfg.max_bid,
            )
        } else {
            None
        };

        let reason = match phase {
            Phase::Normal => "tov3_normal",
            Phase::ReduceOnly => "tov3_reduce_only",
            Phase::Pull => "tov3_pull",
        };
        if let Some(px) = up_px {
            send_quote(cfg, sock, frame, "Up", px, p_up, inventory, reason)?;
        }
        if let Some(px) = down_px {
            send_quote(cfg, sock, frame, "Down", px, 1.0 - p_up, inventory, reason)?;
        }
        // In Pull/ReduceOnly the pulled side simply isn't re-quoted; the gateway
        // expires its resting order by TTL (QUOTE_TTL_MS), flattening it.
        Ok(p_up)
    }

    #[allow(clippy::too_many_arguments)]
    fn send_quote(
        cfg: &Config,
        sock: &UnixDatagram,
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
        send_msg(
            sock,
            &cfg.gateway_socket(),
            &WireMessage::QuoteIntent(quote),
        )
    }

    fn spawn_role(exe: &Path, role: &str) -> AppResult<Child> {
        Ok(Command::new(exe).arg(role).spawn()?)
    }

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

    fn process_alive(pid: u32) -> bool {
        std::path::PathBuf::from(format!("/proc/{pid}")).exists()
    }

    fn bind_socket(path: &Path) -> AppResult<UnixDatagram> {
        remove_socket(path);
        let sock = UnixDatagram::bind(path)?;
        sock.set_read_timeout(Some(Duration::from_millis(100)))?;
        Ok(sock)
    }

    fn remove_socket(path: &Path) {
        let _ = std::fs::remove_file(path);
    }

    fn send_msg(sock: &UnixDatagram, path: &Path, msg: &WireMessage) -> AppResult<()> {
        let bytes = serde_json::to_vec(msg)?;
        for attempt in 0..5 {
            match sock.send_to(&bytes, path) {
                Ok(_) => return Ok(()),
                // Peer socket momentarily absent/refused (a sibling worker is
                // restarting). Retry briefly, then DROP the datagram and keep
                // running — a missed IPC frame self-heals (next tick re-sends;
                // inventory is corrected by position reconciliation). Crashing
                // the worker here just causes a restart storm.
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::ConnectionRefused
                            | io::ErrorKind::NotFound
                            | io::ErrorKind::WouldBlock
                    ) =>
                {
                    if attempt < 4 {
                        sleep_ms(20);
                        continue;
                    }
                    eprintln!(
                        "[IPC_DROP] send to {} failed ({:?}); dropped frame",
                        path.display(),
                        err.kind()
                    );
                    return Ok(());
                }
                Err(err) => return Err(err.into()),
            }
        }
        Ok(())
    }

    fn recv_msg(sock: &UnixDatagram, buf: &mut [u8]) -> AppResult<Option<WireMessage>> {
        match sock.recv(buf) {
            Ok(n) => Ok(Some(serde_json::from_slice(&buf[..n])?)),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn wait_for_socket(path: &Path, timeout: Duration) -> AppResult<()> {
        let started = Instant::now();
        while started.elapsed() < timeout {
            if path.exists() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(50));
        }
        Err(format!("socket did not appear: {}", path.display()).into())
    }

    fn sleep_ms(ms: u64) {
        thread::sleep(Duration::from_millis(ms.max(1)));
    }

    #[derive(Clone)]
    struct AsyncJsonlWriter {
        tx: mpsc::Sender<JsonlJob>,
    }

    struct JsonlJob {
        path: PathBuf,
        line: String,
    }

    fn spawn_jsonl_writer() -> AsyncJsonlWriter {
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

    fn log_jsonl<T: Serialize>(writer: &AsyncJsonlWriter, path: &Path, value: &T) -> AppResult<()> {
        let line = serde_json::to_string(value)?;
        writer
            .tx
            .send(JsonlJob {
                path: path.to_path_buf(),
                line,
            })
            .map_err(|err| format!("jsonl writer stopped: {err}").into())
    }

    #[derive(Clone)]
    struct LiveFrameSink {
        sock: Arc<Mutex<UnixDatagram>>,
        engine_socket: PathBuf,
        book_path: PathBuf,
        logger: AsyncJsonlWriter,
    }

    fn push_live_frame(
        cfg: &Config,
        state: &Arc<Mutex<LiveMarketState>>,
        sink: &LiveFrameSink,
        status: &str,
    ) -> AppResult<bool> {
        let frame = {
            let state = state.lock().unwrap();
            state.frame(cfg)
        };
        let Some(frame) = frame else {
            return Ok(false);
        };

        {
            let sock = sink.sock.lock().unwrap();
            send_msg(
                &sock,
                &sink.engine_socket,
                &WireMessage::MarketFrame(frame.clone()),
            )?;
        }
        log_jsonl(&sink.logger, &sink.book_path, &frame)?;
        heartbeat(cfg, "collector", status)?;
        Ok(true)
    }

    type WsSocket = WebSocket<MaybeTlsStream<TcpStream>>;

    fn tune_ws_socket(socket: &mut WsSocket, timeout: Duration) -> AppResult<()> {
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

    fn is_ws_timeout(err: &WsError) -> bool {
        matches!(
            err,
            WsError::Io(io_err)
                if matches!(
                    io_err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                )
        )
    }

    #[derive(Clone)]
    struct LiveMarketIdentity {
        slug: String,
        condition_id: String,
        up_token_id: String,
        down_token_id: String,
        price_to_beat: f64,
        window_end_s: u64,
    }

    #[derive(Clone)]
    struct LiveMarketState {
        started: bool,
        market: Option<LiveMarketIdentity>,
        btc_price: f64,
        price_to_beat: f64,
        up_ask: f64,
        down_ask: f64,
        last_btc_ts: u64,
        last_polymarket_ts: u64,
        vol: VolEstimator,
    }

    impl LiveMarketState {
        /// Record a fresh BTC price and update the volatility estimate using the
        /// real elapsed time since the previous tick.
        fn record_btc(&mut self, price: f64) {
            let now = now_ms();
            let dt_sec = if self.last_btc_ts == 0 {
                1.0
            } else {
                (now.saturating_sub(self.last_btc_ts) as f64 / 1000.0).clamp(0.05, 30.0)
            };
            self.vol.update(price, dt_sec);
            self.btc_price = price;
            self.last_btc_ts = now;
            self.started = true;
        }

        fn new(cfg: &Config) -> Self {
            let market = (!cfg.polymarket_up_token_id.trim().is_empty()
                && !cfg.polymarket_down_token_id.trim().is_empty())
            .then(|| LiveMarketIdentity {
                slug: cfg.market_slug.clone(),
                condition_id: String::new(),
                up_token_id: cfg.polymarket_up_token_id.clone(),
                down_token_id: cfg.polymarket_down_token_id.clone(),
                price_to_beat: cfg.price_to_beat,
                window_end_s: (now_ms() / 1000) + cfg.market_window_secs,
            });
            let last_polymarket_ts = market.as_ref().map(|_| now_ms()).unwrap_or(0);
            Self {
                started: false,
                market,
                btc_price: cfg.price_to_beat,
                price_to_beat: cfg.price_to_beat,
                up_ask: 0.0,
                down_ask: 0.0,
                last_btc_ts: 0,
                last_polymarket_ts,
                vol: VolEstimator::new(
                    cfg.vol_seed_per_sqrt_sec,
                    cfg.vol_halflife_sec,
                    cfg.vol_min_per_sqrt_sec,
                    cfg.vol_max_per_sqrt_sec,
                ),
            }
        }

        fn frame(&self, cfg: &Config) -> Option<MarketFrame> {
            let market = self.market.as_ref()?;
            let now = now_ms();
            if now / 1000 >= market.window_end_s {
                return None;
            }
            if self.last_btc_ts == 0
                || self.up_ask <= 0.0
                || self.down_ask <= 0.0
                || self.btc_price <= 0.0
            {
                return None;
            }
            let window_end_ms = market.window_end_s.saturating_mul(1000);
            let tau_seconds = (window_end_ms.saturating_sub(now) as f64 / 1000.0).max(0.0);
            Some(MarketFrame {
                ts_ms: now,
                market: market.slug.clone(),
                condition_id: market.condition_id.clone(),
                up_token_id: market.up_token_id.clone(),
                down_token_id: market.down_token_id.clone(),
                up_ask: self.up_ask,
                down_ask: self.down_ask,
                btc_price: self.btc_price,
                price_to_beat: market.price_to_beat,
                tau_seconds,
                vol_per_sqrt_sec: self.vol.current(),
                source: if cfg.auto_discover_market {
                    "auto_gamma_polymarket_binance_ws".to_string()
                } else {
                    "live_polymarket_binance_ws".to_string()
                },
            })
        }

        fn set_market(&mut self, next: LiveMarketIdentity) -> bool {
            let changed = self.market.as_ref().is_none_or(|old| old.slug != next.slug);
            if changed {
                self.up_ask = 0.0;
                self.down_ask = 0.0;
                self.last_polymarket_ts = now_ms();
            }
            self.price_to_beat = next.price_to_beat;
            self.market = Some(next);
            changed
        }
    }

    fn live_polymarket_loop(cfg: Config, state: Arc<Mutex<LiveMarketState>>, sink: LiveFrameSink) {
        while !should_stop(&cfg) {
            if let Err(err) = live_polymarket_once(&cfg, &state, &sink) {
                let _ = heartbeat(&cfg, "collector", format!("polymarket ws reconnect: {err}"));
                sleep_ms(1_000);
            }
        }
    }

    fn market_discovery_loop(cfg: Config, state: Arc<Mutex<LiveMarketState>>) {
        while !should_stop(&cfg) {
            match discover_current_market(&cfg, &state) {
                Ok(Some(identity)) => {
                    let changed = {
                        let mut state = state.lock().unwrap();
                        state.set_market(identity.clone())
                    };
                    if changed {
                        let _ = heartbeat(
                            &cfg,
                            "collector",
                            format!(
                                "auto market {} beat {:.2}",
                                identity.slug, identity.price_to_beat
                            ),
                        );
                    }
                }
                Ok(None) => {
                    let _ = heartbeat(&cfg, "collector", "auto market not found");
                }
                Err(err) => {
                    let _ = heartbeat(&cfg, "collector", format!("auto market error: {err}"));
                }
            }
            sleep_ms(cfg.market_discovery_ms);
        }
    }

    fn discover_current_market(
        cfg: &Config,
        state: &Arc<Mutex<LiveMarketState>>,
    ) -> AppResult<Option<LiveMarketIdentity>> {
        let now_s = now_ms() / 1000;
        let start_s = (now_s / cfg.market_window_secs) * cfg.market_window_secs;
        let slug = format!("{}-{start_s}", cfg.market_slug.trim_end_matches('-'));
        let Some(mut identity) = fetch_gamma_market(cfg, &slug, start_s)? else {
            return Ok(None);
        };

        let price_to_beat = fetch_binance_start_price(cfg, start_s).or_else(|| {
            let state = state.lock().unwrap();
            (state.btc_price > 0.0).then_some(state.btc_price)
        });
        if let Some(price) = price_to_beat {
            identity.price_to_beat = price;
        }
        Ok(Some(identity))
    }

    fn fetch_gamma_market(
        cfg: &Config,
        slug: &str,
        window_start_s: u64,
    ) -> AppResult<Option<LiveMarketIdentity>> {
        let base = cfg.gamma_api_url.trim_end_matches('/');
        let url = format!("{base}/events/slug/{slug}");
        let value: Value = match ureq::get(&url).set("User-Agent", "polymaker/0.1").call() {
            Ok(resp) => resp.into_json()?,
            Err(ureq::Error::Status(404, _)) => return Ok(None),
            Err(err) => return Err(format!("Gamma query failed for {slug}: {err}").into()),
        };

        let market = value
            .get("markets")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .ok_or_else(|| format!("Gamma event {slug} has no markets"))?;
        if market
            .get("closed")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Ok(None);
        }
        if !market
            .get("enableOrderBook")
            .and_then(Value::as_bool)
            .unwrap_or(true)
        {
            return Ok(None);
        }

        let outcomes = parse_json_string_array(market.get("outcomes"))
            .ok_or_else(|| format!("Gamma market {slug} missing outcomes"))?;
        let token_ids = parse_json_string_array(market.get("clobTokenIds"))
            .ok_or_else(|| format!("Gamma market {slug} missing clobTokenIds"))?;
        let (up_token_id, down_token_id) = map_up_down_tokens(&outcomes, &token_ids)
            .ok_or_else(|| format!("Gamma market {slug} does not expose Up/Down tokens"))?;

        Ok(Some(LiveMarketIdentity {
            slug: market
                .get("slug")
                .and_then(Value::as_str)
                .unwrap_or(slug)
                .to_string(),
            condition_id: market
                .get("conditionId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            up_token_id,
            down_token_id,
            price_to_beat: cfg.price_to_beat,
            window_end_s: window_start_s + cfg.market_window_secs,
        }))
    }

    fn fetch_binance_start_price(cfg: &Config, window_start_s: u64) -> Option<f64> {
        let start_ms = window_start_s * 1000;
        let end_ms = start_ms + cfg.market_switch_grace_ms.max(1_000).min(30_000);
        for base in [
            cfg.binance_rest_url.trim_end_matches('/'),
            "https://api.binance.us",
        ] {
            let url = format!(
                "{base}/api/v3/aggTrades?symbol=BTCUSDT&startTime={start_ms}&endTime={end_ms}&limit=1"
            );
            let Some(value) = http_json(&url) else {
                continue;
            };
            if let Some(price) = value
                .as_array()
                .and_then(|rows| rows.first())
                .and_then(|row| parse_f64_value(row.get("p")))
            {
                return Some(price);
            }
        }
        fetch_btc_rest_price(cfg)
    }

    fn fetch_btc_rest_price(cfg: &Config) -> Option<f64> {
        let binance_url = format!(
            "{}/api/v3/ticker/price?symbol=BTCUSDT",
            cfg.binance_rest_url.trim_end_matches('/')
        );
        for url in [
            binance_url.as_str(),
            "https://api.binance.us/api/v3/ticker/price?symbol=BTCUSDT",
            "https://api.coinbase.com/v2/prices/BTC-USD/spot",
            "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
        ] {
            let Some(value) = http_json(url) else {
                continue;
            };
            if let Some(price) = parse_btc_rest_price(&value) {
                return Some(price);
            }
        }
        None
    }

    fn parse_btc_rest_price(value: &Value) -> Option<f64> {
        parse_f64_value(value.get("price"))
            .or_else(|| {
                value
                    .get("data")
                    .and_then(|data| parse_f64_value(data.get("amount")))
            })
            .or_else(|| {
                value
                    .get("result")
                    .and_then(Value::as_object)?
                    .values()
                    .next()?
                    .get("c")?
                    .as_array()?
                    .first()
                    .and_then(|v| parse_f64_value(Some(v)))
            })
    }

    fn http_json(url: &str) -> Option<Value> {
        ureq::get(url)
            .set("User-Agent", "polymaker/0.1")
            .call()
            .ok()?
            .into_json::<Value>()
            .ok()
    }

    fn parse_json_string_array(value: Option<&Value>) -> Option<Vec<String>> {
        match value? {
            Value::Array(items) => Some(
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect(),
            ),
            Value::String(raw) => serde_json::from_str::<Vec<String>>(raw).ok(),
            _ => None,
        }
    }

    fn map_up_down_tokens(outcomes: &[String], token_ids: &[String]) -> Option<(String, String)> {
        if outcomes.len() != token_ids.len() {
            return None;
        }
        let mut up = None;
        let mut down = None;
        for (outcome, token_id) in outcomes.iter().zip(token_ids.iter()) {
            match outcome.trim().to_ascii_lowercase().as_str() {
                "up" | "yes" => up = Some(token_id.clone()),
                "down" | "no" => down = Some(token_id.clone()),
                _ => {}
            }
        }
        Some((up?, down?))
    }

    fn live_polymarket_once(
        cfg: &Config,
        state: &Arc<Mutex<LiveMarketState>>,
        sink: &LiveFrameSink,
    ) -> AppResult<()> {
        let identity = loop {
            if should_stop(cfg) {
                return Ok(());
            }
            if let Some(identity) = state.lock().unwrap().market.clone() {
                break identity;
            }
            heartbeat(cfg, "collector", "waiting auto market")?;
            sleep_ms(500);
        };
        let (mut socket, _) = connect(cfg.polymarket_ws_url.as_str())?;
        let sub = serde_json::json!({
            "assets_ids": [
                identity.up_token_id,
                identity.down_token_id
            ],
            "type": "market",
            "custom_feature_enabled": true
        });
        socket.send(Message::Text(sub.to_string()))?;
        tune_ws_socket(&mut socket, Duration::from_millis(1_000))?;
        heartbeat(&cfg, "collector", format!("subscribed {}", identity.slug))?;
        let mut last_ping = Instant::now();

        while !should_stop(cfg) {
            let current_slug = state
                .lock()
                .unwrap()
                .market
                .as_ref()
                .map(|market| market.slug.clone());
            if current_slug.as_deref() != Some(identity.slug.as_str()) {
                break;
            }
            if last_ping.elapsed() >= Duration::from_secs(8) {
                socket.send(Message::Text("PING".to_string()))?;
                last_ping = Instant::now();
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
                        if apply_polymarket_message(state, &value) {
                            if let Err(err) =
                                push_live_frame(cfg, state, sink, "live polymarket event")
                            {
                                let _ = heartbeat(
                                    cfg,
                                    "collector",
                                    format!("engine socket wait: {err}"),
                                );
                            }
                        }
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

    fn live_btc_loop(cfg: Config, state: Arc<Mutex<LiveMarketState>>, sink: LiveFrameSink) {
        if let Some(price) = fetch_btc_rest_price(&cfg) {
            {
                let mut state = state.lock().unwrap();
                state.record_btc(price);
            }
            let _ = push_live_frame(&cfg, &state, &sink, "btc rest seed");
            let _ = heartbeat(&cfg, "collector", format!("btc rest seed {price:.2}"));
        }

        while !should_stop(&cfg) {
            let mut last_err = None;
            for url in btc_ws_urls(&cfg) {
                if should_stop(&cfg) {
                    break;
                }
                match live_btc_once(&cfg, &state, &sink, &url) {
                    Ok(()) => {
                        last_err = None;
                        break;
                    }
                    Err(err) => {
                        let _ = heartbeat(&cfg, "collector", format!("btc ws switch {url}: {err}"));
                        last_err = Some(err);
                    }
                }
            }
            if last_err.is_none() {
                continue;
            }

            if let Some(price) = fetch_btc_rest_price(&cfg) {
                {
                    let mut state = state.lock().unwrap();
                    state.record_btc(price);
                }
                let _ = push_live_frame(&cfg, &state, &sink, "btc rest fallback");
                let _ = heartbeat(&cfg, "collector", format!("btc rest fallback {price:.2}"));
            } else if let Some(err) = last_err {
                let _ = heartbeat(&cfg, "collector", format!("btc ws reconnect: {err}"));
            }
            sleep_ms(250);
        }
    }

    fn live_btc_once(
        cfg: &Config,
        state: &Arc<Mutex<LiveMarketState>>,
        sink: &LiveFrameSink,
        url: &str,
    ) -> AppResult<()> {
        let (mut socket, _) = connect(url)?;
        let quiet_timeout = Duration::from_millis((cfg.stale_after_ms / 2).clamp(250, 2_000));
        tune_ws_socket(&mut socket, quiet_timeout)?;
        heartbeat(cfg, "collector", format!("btc ws subscribed {url}"))?;
        let mut last_event = Instant::now();
        while !should_stop(cfg) {
            let msg = match socket.read() {
                Ok(msg) => msg,
                Err(err) if is_ws_timeout(&err) => {
                    if last_event.elapsed() >= quiet_timeout {
                        return Err(format!("btc ws quiet for {:?}", last_event.elapsed()).into());
                    }
                    continue;
                }
                Err(err) => return Err(err.into()),
            };
            match msg {
                Message::Text(raw) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&raw) {
                        if let Some(price) = parse_btc_price(&value) {
                            last_event = Instant::now();
                            {
                                let mut state = state.lock().unwrap();
                                state.record_btc(price);
                            }
                            if let Err(err) = push_live_frame(cfg, state, sink, "live btc event") {
                                let _ = heartbeat(
                                    cfg,
                                    "collector",
                                    format!("engine socket wait: {err}"),
                                );
                            }
                        }
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

    fn btc_ws_urls(cfg: &Config) -> Vec<String> {
        let mut urls = Vec::new();
        let primary = cfg.binance_ws_url.trim();
        if !primary.is_empty() {
            urls.push(primary.to_string());
        }
        let us = "wss://stream.binance.us:9443/ws/btcusdt@trade";
        if urls.iter().all(|url| url != us) {
            urls.push(us.to_string());
        }
        urls
    }

    fn apply_polymarket_message(state: &Arc<Mutex<LiveMarketState>>, value: &Value) -> bool {
        if let Some(items) = value.as_array() {
            let mut updated = false;
            for item in items {
                updated |= apply_polymarket_message(state, item);
            }
            return updated;
        }

        let event_type = value
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match event_type {
            "book" => {
                let Some(asset_id) = value.get("asset_id").and_then(Value::as_str) else {
                    return false;
                };
                let Some(best_ask) = best_ask_from_levels(value.get("asks")) else {
                    return false;
                };
                update_polymarket_ask(state, asset_id, best_ask)
            }
            "best_bid_ask" => {
                let Some(asset_id) = value.get("asset_id").and_then(Value::as_str) else {
                    return false;
                };
                let Some(best_ask) = parse_f64_value(value.get("best_ask")) else {
                    return false;
                };
                update_polymarket_ask(state, asset_id, best_ask)
            }
            "price_change" => {
                let Some(changes) = value.get("price_changes").and_then(Value::as_array) else {
                    return false;
                };
                let mut updated = false;
                for change in changes {
                    let Some(asset_id) = change.get("asset_id").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(best_ask) = parse_f64_value(change.get("best_ask")) else {
                        continue;
                    };
                    updated |= update_polymarket_ask(state, asset_id, best_ask);
                }
                updated
            }
            _ => false,
        }
    }

    fn update_polymarket_ask(
        state: &Arc<Mutex<LiveMarketState>>,
        asset_id: &str,
        best_ask: f64,
    ) -> bool {
        if !(0.0..=1.0).contains(&best_ask) || best_ask <= 0.0 {
            return false;
        }
        let mut state = state.lock().unwrap();
        let Some(market) = state.market.as_ref() else {
            return false;
        };
        let is_up = asset_id == market.up_token_id;
        let is_down = asset_id == market.down_token_id;
        if !is_up && !is_down {
            return false;
        }
        let changed = if is_up {
            (state.up_ask - best_ask).abs() > f64::EPSILON
        } else {
            (state.down_ask - best_ask).abs() > f64::EPSILON
        };
        state.started = true;
        state.last_polymarket_ts = now_ms();
        if is_up {
            state.up_ask = best_ask;
        } else if is_down {
            state.down_ask = best_ask;
        }
        changed
    }

    fn best_ask_from_levels(levels: Option<&Value>) -> Option<f64> {
        levels?
            .as_array()?
            .iter()
            .filter_map(|level| {
                let price = parse_f64_value(level.get("price"))?;
                let size = parse_f64_value(level.get("size")).unwrap_or(0.0);
                (size > 0.0).then_some(price)
            })
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }

    fn parse_btc_price(value: &Value) -> Option<f64> {
        parse_f64_value(value.get("p"))
            .or_else(|| parse_f64_value(value.get("price")))
            .or_else(|| parse_f64_value(value.get("c")))
    }

    fn parse_f64_value(value: Option<&Value>) -> Option<f64> {
        match value? {
            Value::String(s) => s.parse::<f64>().ok(),
            Value::Number(n) => n.as_f64(),
            _ => None,
        }
    }

    struct FastRng(u64);

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct RestingOrder {
        quote_id: String,
        exchange_order_id: Option<String>,
        market: String,
        condition_id: String,
        token_id: String,
        side: String,
        price: f64,
        size: f64,
        expires_ts_ms: u64,
    }

    #[derive(Clone)]
    struct QuoteMeta {
        quote_id: String,
        market: String,
        condition_id: String,
        side: String,
        price: f64,
        size: f64,
    }

    type SharedOrderMap = Arc<Mutex<HashMap<String, QuoteMeta>>>;

    struct PendingOrder {
        quote_id: String,
        market: String,
        side: String,
        size: f64,
        expires_ts_ms: u64,
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

    fn load_active_orders(cfg: &Config) -> AppResult<HashMap<String, RestingOrder>> {
        Ok(
            read_json::<HashMap<String, RestingOrder>>(&cfg.active_orders_path())?
                .unwrap_or_default(),
        )
    }

    fn persist_active_orders(
        cfg: &Config,
        resting: &HashMap<String, RestingOrder>,
    ) -> AppResult<()> {
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

    fn order_map_from_resting(
        resting: &HashMap<String, RestingOrder>,
    ) -> HashMap<String, QuoteMeta> {
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
                    },
                ))
            })
            .collect()
    }

    fn accept_resting_order(
        cfg: &Config,
        sock: &UnixDatagram,
        real_orders: Option<&RealOrderClient>,
        order_map: Option<&SharedOrderMap>,
        resting: &mut HashMap<String, RestingOrder>,
        mut quote: QuoteIntent,
    ) -> AppResult<bool> {
        if cfg.live_order_notional_cap > 0.0 && quote.price > 0.0 {
            let capped_size = (cfg.live_order_notional_cap / quote.price).floor();
            if capped_size < 1.0 {
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
                    format!("order rejected, skipped {} @ {:.2}", quote.side, quote.price),
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
        if let Err(err) = send_msg(
            sock,
            &cfg.risk_socket(),
            &WireMessage::OrderAccepted(accepted),
        ) {
            let _ = heartbeat(
                cfg,
                "order-gateway",
                format!("risk accept ipc failed: {err}"),
            );
            let _ = cancel_resting_side(
                cfg,
                sock,
                real_orders,
                order_map,
                resting,
                &quote.side,
                "risk_ipc_failed",
            );
            return Err(err);
        }
        Ok(true)
    }

    fn expire_resting_orders(
        cfg: &Config,
        sock: &UnixDatagram,
        real_orders: Option<&RealOrderClient>,
        order_map: Option<&SharedOrderMap>,
        resting: &mut HashMap<String, RestingOrder>,
    ) -> AppResult<()> {
        let now = now_ms();
        let expired = resting
            .iter()
            .filter_map(|(side, order)| (order.expires_ts_ms <= now).then_some(side.clone()))
            .collect::<Vec<_>>();
        for side in expired {
            cancel_resting_side(cfg, sock, real_orders, order_map, resting, &side, "expired")?;
        }
        Ok(())
    }

    fn cancel_resting_side(
        cfg: &Config,
        sock: &UnixDatagram,
        real_orders: Option<&RealOrderClient>,
        order_map: Option<&SharedOrderMap>,
        resting: &mut HashMap<String, RestingOrder>,
        side: &str,
        reason: &str,
    ) -> AppResult<()> {
        let Some(order) = resting.get(side).cloned() else {
            return Ok(());
        };
        send_cancel(cfg, sock, real_orders, order_map, order, reason)?;
        resting.remove(side);
        persist_active_orders_if_real(cfg, real_orders, resting)?;
        Ok(())
    }

    /// Cancel any order the exchange reports as open that we don't track
    /// locally. This catches orphans (state file lost, or placed-but-not-recorded
    /// after a crash). We only cancel UNKNOWN ids — known ones are managed by the
    /// normal resting/fill/expire flow, so there is no race with fresh orders.
    fn reconcile_orphan_orders(
        cfg: &Config,
        real_orders: &RealOrderClient,
        resting: &HashMap<String, RestingOrder>,
    ) -> AppResult<()> {
        let open = match real_orders.open_order_ids() {
            Ok(ids) => ids,
            Err(err) => {
                heartbeat(cfg, "order-gateway", format!("reconcile fetch failed: {err}"))?;
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

    fn cancel_all_resting_orders(
        cfg: &Config,
        sock: &UnixDatagram,
        real_orders: &RealOrderClient,
        order_map: &SharedOrderMap,
        resting: &mut HashMap<String, RestingOrder>,
        reason: &str,
    ) -> AppResult<()> {
        let sides = resting.keys().cloned().collect::<Vec<_>>();
        let mut first_error = None;
        for side in sides {
            if let Err(err) = cancel_resting_side(
                cfg,
                sock,
                Some(real_orders),
                Some(order_map),
                resting,
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

    fn send_cancel(
        cfg: &Config,
        sock: &UnixDatagram,
        real_orders: Option<&RealOrderClient>,
        order_map: Option<&SharedOrderMap>,
        order: RestingOrder,
        reason: &str,
    ) -> AppResult<()> {
        if let (Some(real_orders), Some(exchange_order_id)) =
            (real_orders, order.exchange_order_id.as_deref())
        {
            let ack = real_orders.cancel_order(exchange_order_id)?;
            if !ack.is_terminal_noop() {
                return Err(format!(
                    "Polymarket cancel rejected for {exchange_order_id}: {}",
                    ack.reason.unwrap_or_else(|| "unknown reason".to_string())
                )
                .into());
            }
            heartbeat(
                cfg,
                "order-gateway",
                if ack.canceled {
                    format!("real cancel {exchange_order_id}")
                } else {
                    format!(
                        "real cancel noop {exchange_order_id}: {}",
                        ack.reason.unwrap_or_else(|| "not active".to_string())
                    )
                },
            )?;
            if let Some(order_map) = order_map {
                order_map.lock().unwrap().remove(exchange_order_id);
            }
        }
        let cancel = OrderCancelled {
            ts_ms: now_ms(),
            quote_id: order.quote_id,
            market: order.market,
            side: order.side,
            size: order.size,
            reason: reason.to_string(),
            source: if real_orders.is_some() {
                "polymarket_clob".to_string()
            } else {
                "dry_run_gateway".to_string()
            },
        };
        if let Err(err) = send_msg(
            sock,
            &cfg.risk_socket(),
            &WireMessage::OrderCancelled(cancel),
        ) {
            heartbeat(
                cfg,
                "order-gateway",
                format!("risk cancel ipc failed: {err}"),
            )?;
            if real_orders.is_none() {
                return Err(err);
            }
        }
        Ok(())
    }

    fn apply_fill_to_resting(
        cfg: &Config,
        real_orders: Option<&RealOrderClient>,
        resting: &mut HashMap<String, RestingOrder>,
        fill: &FillEvent,
    ) -> AppResult<bool> {
        let changed = apply_fill_to_resting_map(resting, fill);
        if changed {
            persist_active_orders_if_real(cfg, real_orders, resting)?;
        }
        Ok(changed)
    }

    fn apply_fill_to_resting_map(
        resting: &mut HashMap<String, RestingOrder>,
        fill: &FillEvent,
    ) -> bool {
        let key = resting.iter().find_map(|(side, order)| {
            ((!fill.quote_id.is_empty() && order.quote_id == fill.quote_id)
                || (fill.quote_id.is_empty()
                    && order.market == fill.market
                    && order.side == fill.side))
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

    fn apply_cancel_to_resting(
        cfg: &Config,
        real_orders: Option<&RealOrderClient>,
        resting: &mut HashMap<String, RestingOrder>,
        cancel: &OrderCancelled,
    ) -> AppResult<bool> {
        let changed = apply_cancel_to_resting_map(resting, cancel);
        if changed {
            persist_active_orders_if_real(cfg, real_orders, resting)?;
        }
        Ok(changed)
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

    fn start_user_channel(
        cfg: &Config,
        credentials: UserWsCredentials,
        order_map: SharedOrderMap,
    ) -> AppResult<()> {
        let cfg = cfg.clone();
        thread::spawn(move || user_channel_loop(cfg, credentials, order_map));
        Ok(())
    }

    fn user_channel_loop(cfg: Config, credentials: UserWsCredentials, order_map: SharedOrderMap) {
        while !should_stop(&cfg) {
            if let Err(err) = user_channel_once(&cfg, &credentials, &order_map) {
                let _ = heartbeat(&cfg, "order-gateway", format!("user ws reconnect: {err}"));
                sleep_ms(1_000);
            }
        }
    }

    fn user_channel_once(
        cfg: &Config,
        credentials: &UserWsCredentials,
        order_map: &SharedOrderMap,
    ) -> AppResult<()> {
        let sock = UnixDatagram::unbound()?;
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

        let mut seen_trades = HashSet::new();
        let mut subscribed = known_condition_ids(order_map)
            .into_iter()
            .collect::<HashSet<_>>();
        let mut last_ping = Instant::now();
        let mut last_subscribe_check = Instant::now();

        while !should_stop(cfg) {
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
                            &sock,
                            order_map,
                            &mut seen_trades,
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

    fn apply_user_channel_message(
        cfg: &Config,
        sock: &UnixDatagram,
        order_map: &SharedOrderMap,
        seen_trades: &mut HashSet<String>,
        value: &Value,
    ) -> AppResult<()> {
        if let Some(items) = value.as_array() {
            for item in items {
                apply_user_channel_message(cfg, sock, order_map, seen_trades, item)?;
            }
            return Ok(());
        }

        let event_type = first_str(value, &["event_type"])
            .or_else(|| first_str(value, &["type"]))
            .unwrap_or_default()
            .to_ascii_lowercase();
        if event_type == "trade" {
            handle_user_trade(cfg, sock, order_map, seen_trades, value)?;
        } else if event_type == "order" {
            handle_user_order(cfg, sock, order_map, value)?;
        }
        Ok(())
    }

    fn handle_user_trade(
        cfg: &Config,
        sock: &UnixDatagram,
        order_map: &SharedOrderMap,
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
            for maker in makers {
                let Some(order_id) = first_str(maker, &["order_id", "id"]) else {
                    continue;
                };
                let key = format!("{trade_id}:{order_id}");
                if !seen_trades.insert(key) {
                    continue;
                }
                let size = parse_f64_value(maker.get("matched_amount"))
                    .or_else(|| parse_f64_value(maker.get("size")))
                    .or_else(|| parse_f64_value(value.get("size")))
                    .unwrap_or(0.0);
                let price = parse_f64_value(maker.get("price"))
                    .or_else(|| parse_f64_value(value.get("price")))
                    .unwrap_or(0.0);
                emit_user_fill(cfg, sock, order_map, order_id, price, size)?;
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
            emit_user_fill(cfg, sock, order_map, order_id, price, size)?;
        }
        Ok(())
    }

    fn handle_user_order(
        cfg: &Config,
        sock: &UnixDatagram,
        order_map: &SharedOrderMap,
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
        let Some(meta) = order_map.lock().unwrap().remove(order_id) else {
            return Ok(());
        };
        let size = parse_f64_value(value.get("size_matched"))
            .or_else(|| parse_f64_value(value.get("size")))
            .unwrap_or(meta.size)
            .max(meta.size);
        let cancel = OrderCancelled {
            ts_ms: now_ms(),
            quote_id: meta.quote_id,
            market: meta.market,
            side: meta.side,
            size,
            reason: "user_ws_cancel".to_string(),
            source: "polymarket_user_ws".to_string(),
        };
        if let Err(err) = send_msg(
            sock,
            &cfg.gateway_socket(),
            &WireMessage::OrderCancelled(cancel.clone()),
        ) {
            heartbeat(
                cfg,
                "order-gateway",
                format!("gateway cancel sync failed: {err}"),
            )?;
        }
        send_msg(
            sock,
            &cfg.risk_socket(),
            &WireMessage::OrderCancelled(cancel),
        )?;
        heartbeat(cfg, "order-gateway", "user ws cancel")?;
        Ok(())
    }

    fn emit_user_fill(
        cfg: &Config,
        sock: &UnixDatagram,
        order_map: &SharedOrderMap,
        order_id: &str,
        price: f64,
        size: f64,
    ) -> AppResult<()> {
        let Some(meta) = reduce_order_map(order_map, order_id, size) else {
            return Ok(());
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
        if let Err(err) = send_msg(
            sock,
            &cfg.gateway_socket(),
            &WireMessage::FillEvent(fill.clone()),
        ) {
            heartbeat(
                cfg,
                "order-gateway",
                format!("gateway fill sync failed: {err}"),
            )?;
        }
        send_msg(sock, &cfg.risk_socket(), &WireMessage::FillEvent(fill))?;
        heartbeat(cfg, "order-gateway", "user ws fill")?;
        Ok(())
    }

    fn reduce_order_map(
        order_map: &SharedOrderMap,
        order_id: &str,
        reported_size: f64,
    ) -> Option<QuoteMeta> {
        let mut order_map = order_map.lock().unwrap();
        let meta = order_map.get(order_id)?.clone();
        let size = if reported_size > 0.0 {
            reported_size.min(meta.size)
        } else {
            meta.size
        };
        let should_remove = if let Some(current) = order_map.get_mut(order_id) {
            current.size = (current.size - size).max(0.0);
            current.size <= 0.001
        } else {
            false
        };
        if should_remove {
            order_map.remove(order_id);
        }
        Some(meta)
    }

    fn first_str<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
        keys.iter().find_map(|key| value.get(*key)?.as_str())
    }

    fn prune_pending(pending: &mut Vec<PendingOrder>, now: u64) -> bool {
        let before = pending.len();
        pending.retain(|o| o.expires_ts_ms > now && o.size > 0.001);
        pending.len() != before
    }

    fn consume_pending(pending: &mut Vec<PendingOrder>, quote_id: &str, side: &str, mut size: f64) {
        if !quote_id.is_empty() {
            if let Some(order) = pending.iter_mut().find(|o| o.quote_id == quote_id) {
                order.size = (order.size - size).max(0.0);
            }
            pending.retain(|o| o.size > 0.001);
            return;
        }

        for order in pending.iter_mut().filter(|o| o.side == side) {
            if size <= 0.001 {
                break;
            }
            let used = order.size.min(size);
            order.size -= used;
            size -= used;
        }
        pending.retain(|o| o.size > 0.001);
    }

    fn cancel_pending(pending: &mut Vec<PendingOrder>, quote_id: &str, side: &str, size: f64) {
        consume_pending(pending, quote_id, side, size);
    }

    fn recompute_pending(inventory: &mut Inventory, pending: &[PendingOrder]) {
        inventory.pending_up = pending
            .iter()
            .filter(|o| o.market == inventory.market)
            .filter(|o| o.side == "Up")
            .map(|o| o.size)
            .sum();
        inventory.pending_down = pending
            .iter()
            .filter(|o| o.market == inventory.market)
            .filter(|o| o.side == "Down")
            .map(|o| o.size)
            .sum();
        if inventory.pending_up.abs() < 0.001 {
            inventory.pending_up = 0.0;
        }
        if inventory.pending_down.abs() < 0.001 {
            inventory.pending_down = 0.0;
        }
        inventory.ts_ms = now_ms();
    }

    fn reset_inventory_for_market(
        inventory: &mut Inventory,
        pending: &mut Vec<PendingOrder>,
        market: &str,
    ) {
        if market.is_empty() || inventory.market == market {
            return;
        }
        pending.retain(|order| order.market == market);
        *inventory = Inventory {
            market: market.to_string(),
            ..Default::default()
        };
    }

    /// Overwrite locally-derived filled inventory with authoritative on-chain
    /// holdings when they diverge beyond tolerance. Cost basis comes from the
    /// Data API's initial_value, so PnL stays correct. Pending (resting) orders
    /// are recomputed but never touched by this — they aren't on-chain yet.
    /// Returns true if a correction was applied.
    fn reconcile_inventory_with_chain(
        cfg: &Config,
        inventory: &mut Inventory,
        pending: &[PendingOrder],
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
        recompute_pending(inventory, pending);
        Ok(true)
    }

    fn check_kill_switch(cfg: &Config, inventory: &Inventory) -> AppResult<()> {
        let worst_pnl = inventory.pnl_if_up().min(inventory.pnl_if_down());
        if cfg.max_loss > 0.0 && worst_pnl <= -cfg.max_loss {
            heartbeat(
                cfg,
                "risk-ledger",
                format!("kill switch: max loss {worst_pnl:+.2}"),
            )?;
            write_stop(cfg)?;
            return Ok(());
        }

        let total_inventory = inventory.effective_up() + inventory.effective_down();
        if cfg.max_total_inventory > 0.0 && total_inventory >= cfg.max_total_inventory {
            heartbeat(
                cfg,
                "risk-ledger",
                format!("kill switch: inventory {total_inventory:.0}"),
            )?;
            write_stop(cfg)?;
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn resting(side: &str, quote_id: &str, size: f64) -> RestingOrder {
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
        fn user_fill_reduces_resting_size() {
            let mut resting_orders = HashMap::from([("Up".to_string(), resting("Up", "q1", 5.0))]);

            assert!(apply_fill_to_resting_map(
                &mut resting_orders,
                &fill("Up", "q1", 2.0)
            ));

            assert_eq!(resting_orders.get("Up").unwrap().size, 3.0);
        }

        #[test]
        fn full_user_fill_removes_resting_order() {
            let mut resting_orders =
                HashMap::from([("Down".to_string(), resting("Down", "q2", 5.0))]);

            assert!(apply_fill_to_resting_map(
                &mut resting_orders,
                &fill("Down", "q2", 5.0)
            ));

            assert!(resting_orders.is_empty());
        }

        #[test]
        fn user_cancel_removes_resting_order() {
            let mut resting_orders = HashMap::from([("Up".to_string(), resting("Up", "q3", 5.0))]);
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
            let resting_orders = HashMap::from([("Up".to_string(), resting("Up", "q4", 5.0))]);

            let order_map = order_map_from_resting(&resting_orders);

            let meta = order_map.get("exchange-q4").unwrap();
            assert_eq!(meta.quote_id, "q4");
            assert_eq!(meta.side, "Up");
            assert_eq!(meta.size, 5.0);
        }
    }

    impl FastRng {
        fn new(seed: u64) -> Self {
            Self(seed | 1)
        }

        fn next_unit(&mut self) -> f64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let x = self.0 >> 11;
            (x as f64) / ((1u64 << 53) as f64)
        }
    }
}

#[cfg(unix)]
pub use unix::*;

#[cfg(not(unix))]
mod non_unix {
    use crate::config::Config;
    use crate::AppResult;

    pub fn run_supervisor(_cfg: Config, _seconds: Option<u64>) -> AppResult<()> {
        Err("polymaker low-latency mode requires Unix domain sockets; run it on Linux/VPS".into())
    }
    pub fn run_market_data(_cfg: Config) -> AppResult<()> {
        run_supervisor(_cfg, None)
    }
    pub fn run_fair_value(_cfg: Config) -> AppResult<()> {
        run_supervisor(_cfg, None)
    }
    pub fn run_maker(_cfg: Config) -> AppResult<()> {
        run_supervisor(_cfg, None)
    }
    pub fn run_risk(_cfg: Config) -> AppResult<()> {
        run_supervisor(_cfg, None)
    }
    pub fn write_stop(_cfg: &Config) -> AppResult<()> {
        run_supervisor(_cfg.clone(), None)
    }
    pub fn start_background(_cfg: &Config) -> AppResult<()> {
        run_supervisor(_cfg.clone(), None)
    }
    pub fn restart_background(_cfg: &Config) -> AppResult<()> {
        run_supervisor(_cfg.clone(), None)
    }
    pub fn clean_run_dir(_cfg: &Config) -> AppResult<()> {
        run_supervisor(_cfg.clone(), None)
    }
}

#[cfg(not(unix))]
pub use non_unix::*;
