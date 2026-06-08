#[cfg(unix)]
mod unix {
    use crate::config::Config;
    use crate::ipc::{
        append_jsonl, heartbeat, now_ms, read_json, should_stop, write_json, FillEvent, Inventory,
        MarketFrame, OrderAccepted, OrderCancelled, QuoteIntent, WireMessage,
    };
    use crate::pricing::{market_maker_bids, normal_cdf, post_only_bid};
    use crate::real_orders::RealOrderClient;
    use crate::AppResult;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::io;
    use std::os::unix::net::UnixDatagram;
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};
    use tungstenite::{connect, Message};

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
        let mut step = 0u64;
        let price_to_beat = cfg.price_to_beat;

        while !should_stop(&cfg) {
            let phase = step as f64 / 9.0;
            let btc_price = price_to_beat + 42.0 * phase.sin() + 15.0 * (phase * 0.37).cos();
            let fair_up = normal_cdf((btc_price - price_to_beat) / cfg.btc_sigma_usd);
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
                source: "simulated_collector".to_string(),
            };
            match send_msg(
                &sock,
                &cfg.engine_socket(),
                &WireMessage::MarketFrame(frame.clone()),
            ) {
                Ok(()) => {
                    append_jsonl(&cfg.book_path(), &frame)?;
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
        let sock = UnixDatagram::unbound()?;
        let state = Arc::new(Mutex::new(LiveMarketState::new(&cfg)));

        {
            let cfg = cfg.clone();
            let state = Arc::clone(&state);
            thread::spawn(move || live_polymarket_loop(cfg, state));
        }
        if cfg.auto_discover_market {
            let cfg = cfg.clone();
            let state = Arc::clone(&state);
            thread::spawn(move || market_discovery_loop(cfg, state));
        }
        {
            let cfg = cfg.clone();
            let state = Arc::clone(&state);
            thread::spawn(move || live_btc_loop(cfg, state));
        }

        while !should_stop(&cfg) {
            let frame = {
                let state = state.lock().unwrap();
                state.frame(&cfg)
            };

            if let Some(frame) = frame {
                match send_msg(
                    &sock,
                    &cfg.engine_socket(),
                    &WireMessage::MarketFrame(frame.clone()),
                ) {
                    Ok(()) => {
                        append_jsonl(&cfg.book_path(), &frame)?;
                        heartbeat(&cfg, "collector", "live ws frame")?;
                    }
                    Err(err) => {
                        heartbeat(&cfg, "collector", format!("engine socket wait: {err}"))?;
                    }
                }
            } else {
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
        let mut last_market_ts = None::<u64>;

        while !should_stop(&cfg) {
            match recv_msg(&sock, &mut buf)? {
                Some(WireMessage::MarketFrame(frame)) => {
                    last_market_ts = Some(frame.ts_ms);
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
                None => {
                    if let Some(ts) = last_market_ts {
                        if now_ms().saturating_sub(ts) > cfg.stale_after_ms {
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
        let mut resting: HashMap<String, RestingOrder> = HashMap::new();
        let real_orders = if cfg.real_orders_enabled() {
            Some(RealOrderClient::connect(&cfg)?)
        } else {
            None
        };

        while !should_stop(&cfg) {
            expire_resting_orders(&cfg, &sock, real_orders.as_ref(), &mut resting)?;
            match recv_msg(&sock, &mut buf)? {
                Some(WireMessage::QuoteIntent(quote)) => {
                    if should_keep_resting(&cfg, resting.get(&quote.side), &quote) {
                        heartbeat(&cfg, "order-gateway", "kept resting quote")?;
                        continue;
                    }
                    if let Some(old) = resting.remove(&quote.side) {
                        send_cancel(&cfg, &sock, real_orders.as_ref(), old, "requote")?;
                    }
                    if !accept_resting_order(
                        &cfg,
                        &sock,
                        real_orders.as_ref(),
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
                    append_jsonl(&cfg.quotes_path(), &accepted.quote)?;
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
                    append_jsonl(&cfg.fills_path(), &enriched)?;
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
        let mut last_err = None;
        for attempt in 0..5 {
            match sock.send_to(&bytes, path) {
                Ok(_) => return Ok(()),
                Err(err)
                    if attempt < 4
                        && matches!(
                            err.kind(),
                            io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                        ) =>
                {
                    last_err = Some(err);
                    sleep_ms(20);
                }
                Err(err) => return Err(err.into()),
            }
        }
        Err(last_err
            .map(|err| err.into())
            .unwrap_or_else(|| "send_msg failed".into()))
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
    }

    impl LiveMarketState {
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
            }
        }

        fn frame(&self, cfg: &Config) -> Option<MarketFrame> {
            let market = self.market.as_ref()?;
            if now_ms() / 1000 >= market.window_end_s {
                return None;
            }
            if self.up_ask <= 0.0 || self.down_ask <= 0.0 || self.btc_price <= 0.0 {
                return None;
            }
            Some(MarketFrame {
                ts_ms: now_ms(),
                market: market.slug.clone(),
                condition_id: market.condition_id.clone(),
                up_token_id: market.up_token_id.clone(),
                down_token_id: market.down_token_id.clone(),
                up_ask: self.up_ask,
                down_ask: self.down_ask,
                btc_price: self.btc_price,
                price_to_beat: market.price_to_beat,
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

    fn live_polymarket_loop(cfg: Config, state: Arc<Mutex<LiveMarketState>>) {
        while !should_stop(&cfg) {
            if let Err(err) = live_polymarket_once(&cfg, &state) {
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

    fn live_polymarket_once(cfg: &Config, state: &Arc<Mutex<LiveMarketState>>) -> AppResult<()> {
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
            let msg = socket.read()?;
            match msg {
                Message::Text(raw) => {
                    if raw == "PONG" {
                        continue;
                    }
                    if let Ok(value) = serde_json::from_str::<Value>(&raw) {
                        apply_polymarket_message(state, &value);
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

    fn live_btc_loop(cfg: Config, state: Arc<Mutex<LiveMarketState>>) {
        while !should_stop(&cfg) {
            if let Err(err) = live_btc_once(&cfg, &state) {
                if let Some(price) = fetch_btc_rest_price(&cfg) {
                    let mut state = state.lock().unwrap();
                    state.started = true;
                    state.btc_price = price;
                    state.last_btc_ts = now_ms();
                    let _ = heartbeat(
                        &cfg,
                        "collector",
                        format!("btc ws reconnect: {err}; rest fallback {price:.2}"),
                    );
                } else {
                    let _ = heartbeat(&cfg, "collector", format!("btc ws reconnect: {err}"));
                }
                sleep_ms(1_000);
            }
        }
    }

    fn live_btc_once(cfg: &Config, state: &Arc<Mutex<LiveMarketState>>) -> AppResult<()> {
        let (mut socket, _) = connect(cfg.binance_ws_url.as_str())?;
        while !should_stop(cfg) {
            let msg = socket.read()?;
            match msg {
                Message::Text(raw) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&raw) {
                        if let Some(price) = parse_btc_price(&value) {
                            let mut state = state.lock().unwrap();
                            state.started = true;
                            state.btc_price = price;
                            state.last_btc_ts = now_ms();
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

    fn apply_polymarket_message(state: &Arc<Mutex<LiveMarketState>>, value: &Value) {
        if let Some(items) = value.as_array() {
            for item in items {
                apply_polymarket_message(state, item);
            }
            return;
        }

        let event_type = value
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match event_type {
            "book" => {
                let Some(asset_id) = value.get("asset_id").and_then(Value::as_str) else {
                    return;
                };
                let Some(best_ask) = best_ask_from_levels(value.get("asks")) else {
                    return;
                };
                update_polymarket_ask(state, asset_id, best_ask);
            }
            "best_bid_ask" => {
                let Some(asset_id) = value.get("asset_id").and_then(Value::as_str) else {
                    return;
                };
                let Some(best_ask) = parse_f64_value(value.get("best_ask")) else {
                    return;
                };
                update_polymarket_ask(state, asset_id, best_ask);
            }
            "price_change" => {
                let Some(changes) = value.get("price_changes").and_then(Value::as_array) else {
                    return;
                };
                for change in changes {
                    let Some(asset_id) = change.get("asset_id").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(best_ask) = parse_f64_value(change.get("best_ask")) else {
                        continue;
                    };
                    update_polymarket_ask(state, asset_id, best_ask);
                }
            }
            _ => {}
        }
    }

    fn update_polymarket_ask(state: &Arc<Mutex<LiveMarketState>>, asset_id: &str, best_ask: f64) {
        if !(0.0..=1.0).contains(&best_ask) || best_ask <= 0.0 {
            return;
        }
        let mut state = state.lock().unwrap();
        let Some(market) = state.market.as_ref() else {
            return;
        };
        let is_up = asset_id == market.up_token_id;
        let is_down = asset_id == market.down_token_id;
        if !is_up && !is_down {
            return;
        }
        state.started = true;
        state.last_polymarket_ts = now_ms();
        if is_up {
            state.up_ask = best_ask;
        } else if is_down {
            state.down_ask = best_ask;
        }
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

    struct RestingOrder {
        quote_id: String,
        exchange_order_id: Option<String>,
        market: String,
        token_id: String,
        side: String,
        price: f64,
        size: f64,
        expires_ts_ms: u64,
    }

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
        if old.market != quote.market || old.token_id != quote.token_id {
            return false;
        }
        let threshold = cfg.tick_size * cfg.requote_threshold_ticks.max(0.0);
        (old.price - quote.price).abs() < threshold && (old.size - quote.size).abs() < 0.001
    }

    fn accept_resting_order(
        cfg: &Config,
        sock: &UnixDatagram,
        real_orders: Option<&RealOrderClient>,
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
            let ack = real_orders.place_buy_limit(&quote)?;
            heartbeat(
                cfg,
                "order-gateway",
                format!("real order {} {}", ack.order_id, ack.status),
            )?;
            Some(ack.order_id)
        } else {
            None
        };
        let expires_ts_ms = now_ms() + cfg.quote_ttl_ms;
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
        resting.insert(
            quote.side.clone(),
            RestingOrder {
                quote_id: quote.quote_id.clone(),
                exchange_order_id,
                market: quote.market.clone(),
                token_id: quote.token_id.clone(),
                side: quote.side.clone(),
                price: quote.price,
                size: quote.size,
                expires_ts_ms,
            },
        );
        send_msg(
            sock,
            &cfg.risk_socket(),
            &WireMessage::OrderAccepted(accepted),
        )?;
        Ok(true)
    }

    fn expire_resting_orders(
        cfg: &Config,
        sock: &UnixDatagram,
        real_orders: Option<&RealOrderClient>,
        resting: &mut HashMap<String, RestingOrder>,
    ) -> AppResult<()> {
        let now = now_ms();
        let expired = resting
            .iter()
            .filter_map(|(side, order)| (order.expires_ts_ms <= now).then_some(side.clone()))
            .collect::<Vec<_>>();
        for side in expired {
            if let Some(order) = resting.remove(&side) {
                send_cancel(cfg, sock, real_orders, order, "expired")?;
            }
        }
        Ok(())
    }

    fn send_cancel(
        cfg: &Config,
        sock: &UnixDatagram,
        real_orders: Option<&RealOrderClient>,
        order: RestingOrder,
        reason: &str,
    ) -> AppResult<()> {
        if let (Some(real_orders), Some(exchange_order_id)) =
            (real_orders, order.exchange_order_id.as_deref())
        {
            real_orders.cancel_order(exchange_order_id)?;
            heartbeat(
                cfg,
                "order-gateway",
                format!("real cancel {exchange_order_id}"),
            )?;
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
        send_msg(
            sock,
            &cfg.risk_socket(),
            &WireMessage::OrderCancelled(cancel),
        )
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
