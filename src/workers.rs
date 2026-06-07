#[cfg(unix)]
mod unix {
    use crate::config::Config;
    use crate::ipc::{
        append_jsonl, heartbeat, now_ms, read_json, should_stop, write_json, FillEvent, Inventory,
        MarketFrame, OrderAccepted, QuoteIntent, WireMessage,
    };
    use crate::pricing::{market_maker_bids, normal_cdf, post_only_bid};
    use crate::AppResult;
    use std::io;
    use std::os::unix::net::UnixDatagram;
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant};

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
        for (role, mut child) in children {
            let _ = child.kill();
            let _ = child.wait();
            eprintln!("[supervisor] stopped {role}");
        }
        release_supervisor_pid(&cfg);
        Ok(())
    }

    pub fn run_market_data(cfg: Config) -> AppResult<()> {
        cfg.ensure_trading_supported()?;
        cfg.ensure_dirs()?;
        wait_for_socket(&cfg.engine_socket(), Duration::from_secs(5))?;
        let sock = UnixDatagram::unbound()?;
        let mut step = 0u64;
        let price_to_beat = 68_000.0;

        while !should_stop(&cfg) {
            let phase = step as f64 / 9.0;
            let btc_price = price_to_beat + 42.0 * phase.sin() + 15.0 * (phase * 0.37).cos();
            let fair_up = normal_cdf((btc_price - price_to_beat) / cfg.btc_sigma_usd);
            let micro_noise = 0.012 * (phase * 1.7).sin();
            let frame = MarketFrame {
                ts_ms: now_ms(),
                market: cfg.market_slug.clone(),
                up_ask: (fair_up + 0.035 + micro_noise).clamp(0.03, 0.97),
                down_ask: (1.0 - fair_up + 0.035 - micro_noise).clamp(0.03, 0.97),
                btc_price,
                price_to_beat,
                source: "simulated_collector".to_string(),
            };
            send_msg(
                &sock,
                &cfg.engine_socket(),
                &WireMessage::MarketFrame(frame.clone()),
            )?;
            append_jsonl(&cfg.book_path(), &frame)?;
            heartbeat(&cfg, "collector", format!("sent step={step}"))?;
            step = step.wrapping_add(1);
            sleep_ms(cfg.market_interval_ms);
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

        while !should_stop(&cfg) {
            match recv_msg(&sock, &mut buf)? {
                Some(WireMessage::MarketFrame(frame)) => {
                    if now_ms().saturating_sub(frame.ts_ms) > cfg.stale_after_ms {
                        heartbeat(&cfg, "quote-engine", "skipped stale market frame")?;
                        continue;
                    }
                    handle_market_frame(&cfg, &sock, &frame, &inventory)?;
                    heartbeat(&cfg, "quote-engine", "quoted")?;
                }
                Some(WireMessage::Inventory(next)) => {
                    inventory = next;
                    heartbeat(&cfg, "quote-engine", "inventory updated")?;
                }
                Some(_) => {}
                None => heartbeat(&cfg, "quote-engine", "waiting")?,
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

        while !should_stop(&cfg) {
            match recv_msg(&sock, &mut buf)? {
                Some(WireMessage::QuoteIntent(quote)) => {
                    let accepted = OrderAccepted {
                        accepted_ts_ms: now_ms(),
                        expires_ts_ms: now_ms() + cfg.quote_ttl_ms,
                        quote: quote.clone(),
                        source: "dry_run_gateway".to_string(),
                    };
                    send_msg(
                        &sock,
                        &cfg.risk_socket(),
                        &WireMessage::OrderAccepted(accepted),
                    )?;
                    if rng.next_unit() <= cfg.sim_fill_chance {
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
                        heartbeat(&cfg, "order-gateway", "simulated fill")?;
                    } else {
                        heartbeat(&cfg, "order-gateway", "resting quote")?;
                    }
                }
                Some(_) => {}
                None => heartbeat(&cfg, "order-gateway", "waiting")?,
            }
        }
        Ok(())
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

        while !should_stop(&cfg) {
            match recv_msg(&sock, &mut buf)? {
                Some(WireMessage::OrderAccepted(accepted)) => {
                    prune_pending(&mut pending, now_ms());
                    pending.push(PendingOrder {
                        quote_id: accepted.quote.quote_id.clone(),
                        side: accepted.quote.side.clone(),
                        size: accepted.quote.size,
                        expires_ts_ms: accepted.expires_ts_ms,
                    });
                    recompute_pending(&mut inventory, &pending);
                    append_jsonl(&cfg.quotes_path(), &accepted.quote)?;
                    write_json(&cfg.inventory_path(), &inventory)?;
                    send_msg(
                        &sock,
                        &cfg.engine_socket(),
                        &WireMessage::Inventory(inventory.clone()),
                    )?;
                    heartbeat(&cfg, "risk-ledger", "order accepted")?;
                }
                Some(WireMessage::FillEvent(fill)) => {
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
                    append_jsonl(&cfg.fills_path(), &enriched)?;
                    write_json(&cfg.inventory_path(), &inventory)?;
                    send_msg(
                        &sock,
                        &cfg.engine_socket(),
                        &WireMessage::Inventory(inventory.clone()),
                    )?;
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
        if cfg.run_dir.exists() {
            std::fs::remove_dir_all(&cfg.run_dir)?;
        }
        Ok(())
    }

    fn handle_market_frame(
        cfg: &Config,
        sock: &UnixDatagram,
        frame: &MarketFrame,
        inventory: &Inventory,
    ) -> AppResult<()> {
        let z = (frame.btc_price - frame.price_to_beat) / cfg.btc_sigma_usd;
        let p_up = normal_cdf(z);
        let model = market_maker_bids(
            p_up,
            cfg.quote_spread,
            cfg.inventory_skew,
            inventory.effective_up(),
            inventory.effective_down(),
            cfg.max_side_inventory(),
            cfg.min_bid,
            cfg.max_bid,
        );

        let up_px = post_only_bid(
            model.up_bid,
            frame.up_ask,
            cfg.tick_size,
            cfg.min_bid,
            cfg.max_bid,
        );
        let down_px = post_only_bid(
            model.down_bid,
            frame.down_ask,
            cfg.tick_size,
            cfg.min_bid,
            cfg.max_bid,
        );
        if let Some(px) = up_px {
            send_quote(cfg, sock, frame, "Up", px, p_up, inventory)?;
        }
        if let Some(px) = down_px {
            send_quote(cfg, sock, frame, "Down", px, 1.0 - p_up, inventory)?;
        }
        Ok(())
    }

    fn send_quote(
        cfg: &Config,
        sock: &UnixDatagram,
        frame: &MarketFrame,
        side: &str,
        price: f64,
        fair: f64,
        inventory: &Inventory,
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
            side: side.to_string(),
            price,
            size: cfg.quote_size.min(left).round().max(1.0),
            fair,
            inventory_up: inventory.effective_up(),
            inventory_down: inventory.effective_down(),
            reason: "binary_mm_fair_value_inventory_skew".to_string(),
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
        sock.send_to(&bytes, path)?;
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

    struct FastRng(u64);

    struct PendingOrder {
        quote_id: String,
        side: String,
        size: f64,
        expires_ts_ms: u64,
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

    fn recompute_pending(inventory: &mut Inventory, pending: &[PendingOrder]) {
        inventory.pending_up = pending
            .iter()
            .filter(|o| o.side == "Up")
            .map(|o| o.size)
            .sum();
        inventory.pending_down = pending
            .iter()
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
