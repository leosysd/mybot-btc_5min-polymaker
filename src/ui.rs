use crate::config::Config;
use crate::ipc::{now_ms, read_json, FillEvent, Heartbeat, Inventory, MarketFrame, QuoteIntent};
use crate::workers;
use crate::AppResult;
use serde::de::DeserializeOwned;
use std::collections::VecDeque;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

const C_RESET: &str = "\x1b[0m";
const C_DIM: &str = "\x1b[2m";
const C_BOLD: &str = "\x1b[1m";
const C_GREEN: &str = "\x1b[32m";
const C_YELLOW: &str = "\x1b[33m";
const C_BLUE: &str = "\x1b[34m";
const C_CYAN: &str = "\x1b[36m";
const C_RED: &str = "\x1b[31m";

pub fn init_config(cfg: &Config) -> AppResult<()> {
    let env_path = cfg.env_file();
    if env_path.exists() {
        println!(".env 已存在，不覆盖。需要重置时先手动备份/删除 .env。");
        return Ok(());
    }
    fs::copy(cfg.env_example_file(), &env_path)?;
    println!("已创建 {}，请按需要修改参数。", env_path.display());
    Ok(())
}

pub fn run_menu(cfg: &Config) -> AppResult<()> {
    loop {
        clear_screen();
        print_banner("POLYMAKER 控制台");
        print_status_cards(cfg)?;
        println!();
        println!("{}选择操作{}", C_BOLD, C_RESET);
        println!("  {}1.{} 初始化 .env 配置", C_GREEN, C_RESET);
        println!("  {}2.{} 调整做市参数", C_GREEN, C_RESET);
        println!("  {}3.{} 查看当前状态", C_GREEN, C_RESET);
        println!("  {}4.{} 打开交易监控页", C_GREEN, C_RESET);
        println!("  {}5.{} 试跑 15 秒模拟做市", C_GREEN, C_RESET);
        println!("  {}6.{} 后台启动服务", C_GREEN, C_RESET);
        println!("  {}7.{} 停止服务", C_GREEN, C_RESET);
        println!("  {}8.{} 重启服务", C_GREEN, C_RESET);
        println!("  {}9.{} 清空运行数据", C_GREEN, C_RESET);
        println!("  {}10.{} 参数说明", C_GREEN, C_RESET);
        println!("  {}0.{} 退出", C_GREEN, C_RESET);
        println!();

        match prompt("输入编号")?.trim() {
            "1" => {
                init_config(cfg)?;
                pause()?;
            }
            "2" => {
                edit_market_maker_params(cfg)?;
                pause()?;
            }
            "3" => {
                clear_screen();
                print_status(cfg)?;
                pause()?;
            }
            "4" => {
                run_dashboard(cfg, None)?;
            }
            "5" => {
                run_smoke_test()?;
                pause()?;
            }
            "6" => {
                workers::start_background(cfg)?;
                pause()?;
            }
            "7" => {
                workers::write_stop(cfg)?;
                println!("已写入停止信号。");
                pause()?;
            }
            "8" => {
                workers::restart_background(cfg)?;
                pause()?;
            }
            "9" => {
                workers::clean_run_dir(cfg)?;
                println!("已清空 {}", cfg.run_dir.display());
                pause()?;
            }
            "10" => {
                print_param_help();
                pause()?;
            }
            "0" => return Ok(()),
            _ => {
                println!("无效选择。");
                pause()?;
            }
        }
    }
}

pub fn print_status(cfg: &Config) -> AppResult<()> {
    print_banner("POLYMAKER 状态");
    print_status_cards(cfg)?;
    println!();
    print_latest_market(cfg)?;
    print_latest_quotes(cfg)?;
    print_latest_fills(cfg)?;
    Ok(())
}

pub fn run_dashboard(cfg: &Config, seconds: Option<u64>) -> AppResult<()> {
    let started = Instant::now();
    loop {
        clear_screen();
        print_banner("POLYMAKER 交易监控");
        print_status_cards(cfg)?;
        println!();
        print_latest_market(cfg)?;
        print_latest_quotes(cfg)?;
        print_latest_fills(cfg)?;
        println!();
        println!(
            "{}按 Ctrl+C 退出监控页。当前版本为 DRY_RUN 模拟，不会真实下单。{}",
            C_DIM, C_RESET
        );

        if let Some(limit) = seconds {
            if started.elapsed() >= Duration::from_secs(limit) {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(1000));
    }
    Ok(())
}

fn print_status_cards(cfg: &Config) -> AppResult<()> {
    let inv = read_json::<Inventory>(&cfg.inventory_path())?.unwrap_or_default();
    let hbs = read_heartbeats(cfg)?;
    let now = now_ms();
    let running = hbs
        .iter()
        .filter(|h| now.saturating_sub(h.ts_ms) <= 3_000)
        .count();
    let mode = if cfg.dry_run {
        format!("{C_YELLOW}DRY_RUN{C_RESET}")
    } else {
        format!("{C_RED}LIVE{C_RESET}")
    };

    println!(
        "{}模式:{} {:<16} {}行情:{} {:<6} {}市场:{} {:<18} {}运行目录:{} {}",
        C_BOLD,
        C_RESET,
        mode,
        C_BOLD,
        C_RESET,
        cfg.data_mode,
        C_BOLD,
        C_RESET,
        cfg.market_slug,
        C_BOLD,
        C_RESET,
        cfg.run_dir.display()
    );
    println!(
        "{}心跳:{} {}/{} 活跃   {}库存:{} Up {:.0}+{:.0} / Down {:.0}+{:.0}   {}PnL情景:{} Up赢 {:+.2} / Down赢 {:+.2}",
        C_BOLD, C_RESET, running,
        expected_heartbeat_count(&hbs),
        C_BOLD, C_RESET, inv.up_shares, inv.pending_up, inv.down_shares, inv.pending_down,
        C_BOLD, C_RESET, inv.pnl_if_up(), inv.pnl_if_down()
    );

    if hbs.is_empty() {
        println!(
            "{}暂无心跳。先运行 polymaker supervisor --seconds 15 或 polymaker start。{}",
            C_DIM, C_RESET
        );
    } else {
        println!();
        println!(
            "{}",
            table_row(&[
                ("进程".to_string(), 18, Align::Left),
                ("PID".to_string(), 8, Align::Right),
                ("延迟".to_string(), 8, Align::Right),
                ("状态".to_string(), 24, Align::Left),
            ])
        );
        for hb in hbs {
            let age = now.saturating_sub(hb.ts_ms);
            let color = if age <= 3_000 { C_GREEN } else { C_RED };
            println!(
                "{}",
                table_row(&[
                    (hb.role, 18, Align::Left),
                    (hb.pid.to_string(), 8, Align::Right),
                    (
                        format!("{color}{}{C_RESET}", format_age(age)),
                        8,
                        Align::Right
                    ),
                    (hb.status, 24, Align::Left),
                ])
            );
        }
    }
    Ok(())
}

fn expected_heartbeat_count(hbs: &[Heartbeat]) -> usize {
    if hbs.iter().any(|h| h.role == "supervisor") {
        5
    } else {
        4
    }
}

fn print_latest_market(cfg: &Config) -> AppResult<()> {
    let rows = tail_jsonl::<MarketFrame>(&cfg.book_path(), 5)?;
    println!("{}最近行情{}", C_BOLD, C_RESET);
    println!(
        "{}",
        table_row(&[
            ("时间".to_string(), 8, Align::Left),
            ("BTC".to_string(), 10, Align::Right),
            ("UpAsk".to_string(), 7, Align::Right),
            ("DnAsk".to_string(), 7, Align::Right),
            ("来源".to_string(), 20, Align::Left),
        ])
    );
    for r in rows {
        println!(
            "{}",
            table_row(&[
                (fmt_ts(r.ts_ms), 8, Align::Left),
                (format!("{:.2}", r.btc_price), 10, Align::Right),
                (format!("{:.3}", r.up_ask), 7, Align::Right),
                (format!("{:.3}", r.down_ask), 7, Align::Right),
                (r.source, 20, Align::Left),
            ])
        );
    }
    println!();
    Ok(())
}

fn print_latest_quotes(cfg: &Config) -> AppResult<()> {
    let rows = tail_jsonl::<QuoteIntent>(&cfg.quotes_path(), 8)?;
    println!("{}最近报价{}", C_BOLD, C_RESET);
    println!(
        "{}",
        table_row(&[
            ("时间".to_string(), 8, Align::Left),
            ("方向".to_string(), 6, Align::Left),
            ("价格".to_string(), 7, Align::Right),
            ("份额".to_string(), 6, Align::Right),
            ("fair".to_string(), 7, Align::Right),
            ("Up仓".to_string(), 8, Align::Right),
            ("Dn仓".to_string(), 8, Align::Right),
        ])
    );
    for r in rows {
        println!(
            "{}",
            table_row(&[
                (fmt_ts(r.ts_ms), 8, Align::Left),
                (r.side, 6, Align::Left),
                (format!("{:.3}", r.price), 7, Align::Right),
                (format!("{:.0}", r.size), 6, Align::Right),
                (format!("{:.3}", r.fair), 7, Align::Right),
                (format!("{:.0}", r.inventory_up), 8, Align::Right),
                (format!("{:.0}", r.inventory_down), 8, Align::Right),
            ])
        );
    }
    println!();
    Ok(())
}

fn print_latest_fills(cfg: &Config) -> AppResult<()> {
    let rows = tail_jsonl::<FillEvent>(&cfg.fills_path(), 8)?;
    println!("{}最近成交{}", C_BOLD, C_RESET);
    println!(
        "{}",
        table_row(&[
            ("时间".to_string(), 8, Align::Left),
            ("方向".to_string(), 6, Align::Left),
            ("价格".to_string(), 7, Align::Right),
            ("份额".to_string(), 6, Align::Right),
            ("Up仓".to_string(), 8, Align::Right),
            ("Dn仓".to_string(), 8, Align::Right),
            ("Up赢PnL".to_string(), 10, Align::Right),
            ("Dn赢PnL".to_string(), 10, Align::Right),
        ])
    );
    for r in rows {
        println!(
            "{}",
            table_row(&[
                (fmt_ts(r.ts_ms), 8, Align::Left),
                (r.side, 6, Align::Left),
                (format!("{:.3}", r.price), 7, Align::Right),
                (format!("{:.0}", r.size), 6, Align::Right),
                (format!("{:.0}", r.inventory_up), 8, Align::Right),
                (format!("{:.0}", r.inventory_down), 8, Align::Right),
                (format!("{:+.2}", r.pnl_if_up), 10, Align::Right),
                (format!("{:+.2}", r.pnl_if_down), 10, Align::Right),
            ])
        );
    }
    Ok(())
}

fn edit_market_maker_params(cfg: &Config) -> AppResult<()> {
    if !cfg.env_file().exists() {
        init_config(cfg)?;
    }
    let keys = [
        ("DATA_MODE", "行情来源 sim/live"),
        ("POLYMARKET_UP_TOKEN_ID", "live Up token id"),
        ("POLYMARKET_DOWN_TOKEN_ID", "live Down token id"),
        ("PRICE_TO_BEAT", "当前5分钟BTC判定价"),
        ("QUOTE_SIZE", "每次单边挂单份数"),
        ("QUOTE_SPREAD", "做市毛价差"),
        ("QUOTE_TTL_MS", "未成交报价pending保留毫秒"),
        ("REQUOTE_THRESHOLD_TICKS", "价差变化多少tick才撤旧换新"),
        ("INVENTORY_SKEW", "库存偏移强度"),
        ("INVENTORY_MULT", "单边最大库存倍数"),
        ("MIN_BID", "最低挂买价"),
        ("MAX_BID", "最高挂买价"),
        ("MAX_LOSS", "最坏情景最大亏损"),
        ("MAX_TOTAL_INVENTORY", "Up+Down最大总库存"),
        ("MARKET_INTERVAL_MS", "模拟行情间隔毫秒"),
        ("STALE_AFTER_MS", "行情过期毫秒"),
        ("WS_STALE_AFTER_MS", "live WS断流停止毫秒"),
    ];
    println!("直接回车表示不修改。");
    for (key, desc) in keys {
        let current = env_value(&cfg.env_file(), key).unwrap_or_else(|| "(未设置)".to_string());
        let input = prompt(&format!("{key} 当前={current}  {desc}"))?;
        if !input.trim().is_empty() {
            upsert_env(&cfg.env_file(), key, input.trim())?;
        }
    }
    println!("参数已写入 .env。重启服务后生效。");
    Ok(())
}

fn run_smoke_test() -> AppResult<()> {
    let exe = std::env::current_exe()?;
    let status = Command::new(exe)
        .arg("supervisor")
        .arg("--seconds")
        .arg("15")
        .status()?;
    if !status.success() {
        return Err(format!("试跑失败: {status}").into());
    }
    println!("试跑完成。可用 polymaker dashboard 查看结果。");
    Ok(())
}

fn print_param_help() {
    println!("{}核心参数说明{}", C_BOLD, C_RESET);
    println!("  DATA_MODE         sim=本地模拟；live=真实Polymarket/BTC WS行情");
    println!("  POLYMARKET_*      live模式当前市场Up/Down token id");
    println!("  PRICE_TO_BEAT     当前5分钟BTC市场判定价/开盘价");
    println!("  QUOTE_SIZE        每次单边报价份数");
    println!("  QUOTE_SPREAD      毛价差，约束 up_bid + down_bid <= 1 - spread");
    println!("  QUOTE_TTL_MS      未成交报价 pending 保留多久，过期释放库存占用");
    println!("  REQUOTE_*         旧报价和新报价差多少 tick 才撤旧换新");
    println!("  INVENTORY_SKEW    库存偏移，多仓侧降价、少仓侧抬价");
    println!("  INVENTORY_MULT    单边最大库存 = QUOTE_SIZE * INVENTORY_MULT");
    println!("  MAX_LOSS          最坏结算情景亏损达到阈值就停止");
    println!("  MAX_TOTAL_*       Up+Down 已成交+pending 达到阈值就停止");
    println!("  MIN_BID/MAX_BID   最低/最高挂买价");
    println!("  STALE_AFTER_MS    行情过期阈值，超过则停止用旧行情报价");
    println!("  WS_STALE_AFTER_MS live WS断流阈值，超过则停止");
    println!("  DRY_RUN           当前必须为 1，本版本不会真实下单");
}

fn read_heartbeats(cfg: &Config) -> AppResult<Vec<Heartbeat>> {
    let mut out = Vec::new();
    let dir = cfg.heartbeat_dir();
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(out);
    };
    for entry in entries {
        let entry = entry?;
        if entry.path().extension().and_then(|x| x.to_str()) != Some("json") {
            continue;
        }
        if let Some(hb) = read_json::<Heartbeat>(&entry.path())? {
            out.push(hb);
        }
    }
    out.sort_by(|a, b| a.role.cmp(&b.role));
    Ok(out)
}

fn tail_jsonl<T: DeserializeOwned>(path: &Path, n: usize) -> AppResult<Vec<T>> {
    let Ok(raw) = fs::read_to_string(path) else {
        return Ok(Vec::new());
    };
    let mut lines = VecDeque::with_capacity(n);
    for line in raw.lines().filter(|line| !line.trim().is_empty()) {
        if lines.len() == n {
            lines.pop_front();
        }
        lines.push_back(line.to_string());
    }
    let mut out = Vec::new();
    for line in lines {
        if let Ok(row) = serde_json::from_str::<T>(&line) {
            out.push(row);
        }
    }
    Ok(out)
}

fn env_value(path: &Path, key: &str) -> Option<String> {
    let raw = fs::read_to_string(path).ok()?;
    for line in raw.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        if k.trim() == key {
            return Some(v.split('#').next().unwrap_or("").trim().to_string());
        }
    }
    None
}

fn upsert_env(path: &Path, key: &str, value: &str) -> AppResult<()> {
    let raw = fs::read_to_string(path).unwrap_or_default();
    let mut found = false;
    let mut lines = Vec::new();
    for line in raw.lines() {
        if let Some((k, _)) = line.split_once('=') {
            if k.trim() == key {
                lines.push(format!("{key}={value}"));
                found = true;
                continue;
            }
        }
        lines.push(line.to_string());
    }
    if !found {
        lines.push(format!("{key}={value}"));
    }
    fs::write(path, lines.join("\n") + "\n")?;
    Ok(())
}

fn prompt(label: &str) -> AppResult<String> {
    print!("{C_CYAN}?{C_RESET} {label} > ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim_end().to_string())
}

fn pause() -> AppResult<()> {
    let _ = prompt("按回车继续")?;
    Ok(())
}

fn clear_screen() {
    print!("\x1b[2J\x1b[H");
    let _ = io::stdout().flush();
}

fn print_banner(title: &str) {
    println!("{C_BLUE}╔════════════════════════════════════════════════════════════╗{C_RESET}");
    println!(
        "{C_BLUE}║{C_RESET} {C_BOLD}{}{C_RESET}{C_BLUE}║{C_RESET}",
        pad_right_display(title, 58)
    );
    println!("{C_BLUE}╚════════════════════════════════════════════════════════════╝{C_RESET}");
}

#[derive(Clone, Copy)]
enum Align {
    Left,
    Right,
}

fn table_row(cols: &[(String, usize, Align)]) -> String {
    cols.iter()
        .map(|(value, width, align)| match align {
            Align::Left => pad_right_display(value, *width),
            Align::Right => pad_left_display(value, *width),
        })
        .collect::<Vec<_>>()
        .join("  ")
}

fn pad_right_display(value: &str, width: usize) -> String {
    let pad = width.saturating_sub(display_width(value));
    format!("{value}{}", " ".repeat(pad))
}

fn pad_left_display(value: &str, width: usize) -> String {
    let pad = width.saturating_sub(display_width(value));
    format!("{}{value}", " ".repeat(pad))
}

fn display_width(value: &str) -> usize {
    let mut width = 0;
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            while let Some(next) = chars.next() {
                if next == 'm' {
                    break;
                }
            }
            continue;
        }
        if ch.is_ascii() {
            width += 1;
        } else {
            width += 2;
        }
    }
    width
}

fn fmt_ts(ts_ms: u64) -> String {
    let secs = (ts_ms / 1000) % 86_400;
    let h = secs / 3_600;
    let m = (secs % 3_600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn format_age(age_ms: u64) -> String {
    if age_ms < 1_000 {
        format!("{age_ms}ms")
    } else {
        format!("{:.1}s", age_ms as f64 / 1000.0)
    }
}
