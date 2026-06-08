use crate::config::Config;
use crate::ipc::{now_ms, FillEvent, Heartbeat, Inventory, MarketFrame, QuoteIntent};
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
const REAL_ORDER_CONFIRMATION: &str = "I_UNDERSTAND_REAL_MONEY";

const ROLES: [&str; 5] = [
    "collector",
    "order-gateway",
    "quote-engine",
    "risk-ledger",
    "supervisor",
];

pub fn init_config(cfg: &Config) -> AppResult<()> {
    repair_env_config(cfg)
}

fn repair_env_config(cfg: &Config) -> AppResult<()> {
    let env_path = cfg.env_file();
    let mut created = false;
    if !env_path.exists() {
        fs::copy(cfg.env_example_file(), &env_path)?;
        created = true;
    }

    ensure_cli_defaults(&env_path)?;
    if created {
        println!("已创建 {}，并补齐默认配置。", env_path.display());
    } else {
        println!(
            "{} 已存在，已保留原值并补齐 CLI 需要的缺失字段。",
            env_path.display()
        );
    }
    Ok(())
}

pub fn run_menu(cfg: &Config) -> AppResult<()> {
    loop {
        let active_cfg = Config::from_env().unwrap_or_else(|_| cfg.clone());
        clear_screen();
        print_banner("POLYMAKER 控制台");
        print_status_cards(&active_cfg)?;
        println!();
        println!("{}选择操作{}", C_BOLD, C_RESET);
        println!("  {}1.{} 初始化/修复 .env 配置", C_GREEN, C_RESET);
        println!(
            "  {}2.{} 切换模拟/实单 + 填 Polymarket 账户",
            C_GREEN, C_RESET
        );
        println!("  {}3.{} 调整做市参数", C_GREEN, C_RESET);
        println!("  {}4.{} 查看当前状态", C_GREEN, C_RESET);
        println!("  {}5.{} 打开交易监控页", C_GREEN, C_RESET);
        println!("  {}6.{} 试跑 15 秒模拟做市", C_GREEN, C_RESET);
        println!("  {}7.{} 后台启动服务", C_GREEN, C_RESET);
        println!("  {}8.{} 停止服务", C_GREEN, C_RESET);
        println!("  {}9.{} 重启服务", C_GREEN, C_RESET);
        println!("  {}10.{} 清空运行数据", C_GREEN, C_RESET);
        println!("  {}11.{} 参数说明", C_GREEN, C_RESET);
        println!("  {}0.{} 退出", C_GREEN, C_RESET);
        println!();

        match prompt("输入编号")?.trim() {
            "1" => {
                repair_env_config(&active_cfg)?;
                pause()?;
            }
            "2" => {
                configure_trading_profile(&active_cfg)?;
                pause()?;
            }
            "3" => {
                edit_market_maker_params(&active_cfg)?;
                pause()?;
            }
            "4" => {
                clear_screen();
                print_status(&active_cfg)?;
                pause()?;
            }
            "5" => {
                run_dashboard(&active_cfg, None)?;
            }
            "6" => {
                run_smoke_test()?;
                pause()?;
            }
            "7" => {
                workers::start_background(&active_cfg)?;
                pause()?;
            }
            "8" => {
                workers::write_stop(&active_cfg)?;
                println!("已写入停止信号。");
                pause()?;
            }
            "9" => {
                workers::restart_background(&active_cfg)?;
                pause()?;
            }
            "10" => {
                workers::clean_run_dir(&active_cfg)?;
                println!("已清空 {}", active_cfg.run_dir.display());
                pause()?;
            }
            "11" => {
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
        if cfg.dry_run {
            println!(
                "{}按 Ctrl+C 退出监控页。当前为 DRY_RUN 模拟，不会真实下单。{}",
                C_DIM, C_RESET
            );
        } else {
            println!(
                "{}按 Ctrl+C 退出监控页。当前为 LIVE 实单模式，请确认小额限额和 kill switch。{}",
                C_RED, C_RESET
            );
        }

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
    let inv = read_dashboard_json::<Inventory>(&cfg.inventory_path()).unwrap_or_default();
    let hbs = read_heartbeats(cfg)?;
    let now = now_ms();
    let running = hbs
        .iter()
        .filter(|h| now.saturating_sub(h.ts_ms) <= 3_000)
        .count();
    let mode = if cfg.dry_run && cfg.data_mode == "live" {
        format!("{C_YELLOW}LIVE行情模拟{C_RESET}")
    } else if cfg.dry_run {
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
        "{}后台:{} {}   {}停止信号:{} {}",
        C_BOLD,
        C_RESET,
        service_status(cfg),
        C_BOLD,
        C_RESET,
        stop_status(cfg)
    );
    println!(
        "{}心跳:{} {}/{} 活跃   {}库存:{} Up {:.0}+{:.0} / Down {:.0}+{:.0}   {}PnL情景:{} Up赢 {:+.2} / Down赢 {:+.2}",
        C_BOLD, C_RESET, running,
        ROLES.len(),
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
                ("运行".to_string(), 8, Align::Left),
                ("状态".to_string(), 28, Align::Left),
            ])
        );
        for role in ROLES {
            let hb = hbs.iter().find(|h| h.role == role);
            let (pid, age, running, status) = if let Some(hb) = hb {
                let age = now.saturating_sub(hb.ts_ms);
                let running = if age <= 3_000 {
                    format!("{C_GREEN}活跃{C_RESET}")
                } else {
                    format!("{C_RED}过期{C_RESET}")
                };
                (
                    hb.pid.to_string(),
                    format_age(age),
                    running,
                    hb.status.clone(),
                )
            } else {
                (
                    "-".to_string(),
                    "-".to_string(),
                    format!("{C_RED}缺失{C_RESET}"),
                    "no heartbeat".to_string(),
                )
            };
            println!(
                "{}",
                table_row(&[
                    (role.to_string(), 18, Align::Left),
                    (pid, 8, Align::Right),
                    (age, 8, Align::Right),
                    (running, 8, Align::Left),
                    (status, 28, Align::Left),
                ])
            );
        }
    }
    Ok(())
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

fn configure_trading_profile(cfg: &Config) -> AppResult<()> {
    repair_env_config(cfg)?;
    loop {
        clear_screen();
        print_banner("模式 / 账户配置");
        print_env_profile(cfg)?;
        println!();
        println!("{}选择配置{}", C_BOLD, C_RESET);
        println!(
            "  {}1.{} 切到本地模拟：sim 行情 + DRY_RUN，不下单",
            C_GREEN, C_RESET
        );
        println!(
            "  {}2.{} 切到真实行情模拟：live 行情 + DRY_RUN，不下单",
            C_GREEN, C_RESET
        );
        println!(
            "  {}3.{} 切到实单模式：live 行情 + Polymarket 真下单",
            C_RED, C_RESET
        );
        println!(
            "  {}4.{} 填 Polymarket 私钥 / 钱包地址 / L2 API",
            C_GREEN, C_RESET
        );
        println!("  {}5.{} 手动覆盖当前市场 token 和判定价", C_GREEN, C_RESET);
        println!("  {}0.{} 返回上级菜单", C_GREEN, C_RESET);
        println!();

        match prompt("输入编号")?.trim() {
            "1" => {
                switch_to_sim_mode(cfg)?;
                maybe_restart_background(cfg)?;
                return Ok(());
            }
            "2" => {
                switch_to_live_dry_run(cfg)?;
                maybe_restart_background(cfg)?;
                return Ok(());
            }
            "3" => {
                configure_real_account(cfg)?;
                if let Err(err) = switch_to_real_mode(cfg) {
                    println!("{C_RED}未切到实单：{err}{C_RESET}");
                    println!("已保留当前模式。补齐后再选择 3 切到实单。");
                    pause()?;
                    continue;
                }
                maybe_restart_background(cfg)?;
                return Ok(());
            }
            "4" => {
                configure_real_account(cfg)?;
                pause()?;
            }
            "5" => {
                configure_live_market(cfg)?;
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

fn print_env_profile(cfg: &Config) -> AppResult<()> {
    let path = cfg.env_file();
    let dry_run = env_value(&path, "DRY_RUN").unwrap_or_else(|| "1".to_string());
    let data_mode = env_value(&path, "DATA_MODE").unwrap_or_else(|| "sim".to_string());
    let auto_discover = env_value(&path, "AUTO_DISCOVER_MARKET").unwrap_or_else(|| "1".to_string());
    let enable_real = env_value(&path, "ENABLE_REAL_ORDERS").unwrap_or_default();
    let real_ready = enable_real == REAL_ORDER_CONFIRMATION;
    let private_key = secret_state(&path, "POLY_PRIVATE_KEY");
    let api_key = secret_state(&path, "POLY_API_KEY");
    let funder = env_value(&path, "POLY_FUNDER_ADDRESS").unwrap_or_default();
    let up = env_value(&path, "POLYMARKET_UP_TOKEN_ID").unwrap_or_default();
    let down = env_value(&path, "POLYMARKET_DOWN_TOKEN_ID").unwrap_or_default();
    let price = env_value(&path, "PRICE_TO_BEAT").unwrap_or_else(|| "68000".to_string());
    let signature = env_value(&path, "POLY_SIGNATURE_TYPE").unwrap_or_else(|| "proxy".to_string());

    let mode = if dry_run == "0" && real_ready {
        format!("{C_RED}实单{C_RESET}")
    } else if data_mode == "live" {
        format!("{C_YELLOW}真实行情模拟{C_RESET}")
    } else {
        format!("{C_GREEN}本地模拟{C_RESET}")
    };

    println!("{}当前模式:{} {}", C_BOLD, C_RESET, mode);
    println!(
        "{}开关:{} DRY_RUN={}  DATA_MODE={}  ENABLE_REAL_ORDERS={}",
        C_BOLD,
        C_RESET,
        dry_run,
        data_mode,
        if real_ready { "已确认" } else { "未开启" }
    );
    println!(
        "{}账户:{} 私钥={}  L2 API={}  签名类型={}  funder={}",
        C_BOLD,
        C_RESET,
        private_key,
        api_key,
        signature,
        if funder.is_empty() {
            "(未填)"
        } else {
            "(已填)"
        }
    );
    println!(
        "{}市场:{} UpToken={}  DownToken={}  PRICE_TO_BEAT={}",
        C_BOLD,
        C_RESET,
        if up.is_empty() {
            "(未填)"
        } else {
            "(已填)"
        },
        if down.is_empty() {
            "(未填)"
        } else {
            "(已填)"
        },
        price
    );
    println!(
        "{}自动发现:{} AUTO_DISCOVER_MARKET={}，开启后会按当前 5 分钟市场自动切 token。",
        C_BOLD, C_RESET, auto_discover
    );
    Ok(())
}

fn switch_to_sim_mode(cfg: &Config) -> AppResult<()> {
    ensure_cli_defaults(&cfg.env_file())?;
    upsert_env(&cfg.env_file(), "DRY_RUN", "1")?;
    upsert_env(&cfg.env_file(), "ENABLE_REAL_ORDERS", "")?;
    upsert_env(&cfg.env_file(), "DATA_MODE", "sim")?;
    upsert_env(&cfg.env_file(), "AUTO_DISCOVER_MARKET", "1")?;
    println!("已切到本地模拟：sim 行情 + DRY_RUN，不会真实下单。");
    Ok(())
}

fn switch_to_live_dry_run(cfg: &Config) -> AppResult<()> {
    ensure_cli_defaults(&cfg.env_file())?;
    upsert_env(&cfg.env_file(), "DRY_RUN", "1")?;
    upsert_env(&cfg.env_file(), "ENABLE_REAL_ORDERS", "")?;
    upsert_env(&cfg.env_file(), "DATA_MODE", "live")?;
    upsert_env(&cfg.env_file(), "AUTO_DISCOVER_MARKET", "1")?;
    println!("已切到真实行情模拟：live 行情 + DRY_RUN，不会真实下单。");
    Ok(())
}

fn switch_to_real_mode(cfg: &Config) -> AppResult<()> {
    ensure_cli_defaults(&cfg.env_file())?;
    upsert_env(&cfg.env_file(), "POLY_SIGNATURE_TYPE", "proxy")?;
    ensure_real_inputs_present(cfg)?;
    upsert_env(&cfg.env_file(), "DRY_RUN", "0")?;
    upsert_env(
        &cfg.env_file(),
        "ENABLE_REAL_ORDERS",
        REAL_ORDER_CONFIRMATION,
    )?;
    upsert_env(&cfg.env_file(), "DATA_MODE", "live")?;
    upsert_env(&cfg.env_file(), "AUTO_DISCOVER_MARKET", "1")?;
    println!("已切到实单模式：live 行情 + Polymarket 真下单。");
    println!("请确认 `LIVE_ORDER_NOTIONAL_CAP`、`MAX_LOSS`、`MAX_TOTAL_INVENTORY` 仍是小额。");
    Ok(())
}

fn ensure_real_inputs_present(cfg: &Config) -> AppResult<()> {
    let path = cfg.env_file();
    let mut missing = Vec::new();
    for (key, label) in [("POLY_PRIVATE_KEY", "私钥")] {
        if env_value(&path, key).is_none_or(|v| v.trim().is_empty()) {
            missing.push(format!("{key}({label})"));
        }
    }
    let auto_discover =
        env_value(&path, "AUTO_DISCOVER_MARKET").unwrap_or_else(|| "1".to_string()) != "0";
    if !auto_discover {
        for (key, label) in [
            ("POLYMARKET_UP_TOKEN_ID", "Up token id"),
            ("POLYMARKET_DOWN_TOKEN_ID", "Down token id"),
            ("PRICE_TO_BEAT", "BTC 判定价/开盘价"),
        ] {
            if env_value(&path, key).is_none_or(|v| v.trim().is_empty()) {
                missing.push(format!("{key}({label})"));
            }
        }
    }

    let signature = env_value(&path, "POLY_SIGNATURE_TYPE").unwrap_or_else(|| "proxy".to_string());
    if signature.trim() != "eoa"
        && env_value(&path, "POLY_FUNDER_ADDRESS").is_none_or(|v| v.trim().is_empty())
    {
        missing.push("POLY_FUNDER_ADDRESS(proxy/funder地址)".to_string());
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!("缺少 {}", missing.join("、")).into())
    }
}

fn configure_real_account(cfg: &Config) -> AppResult<()> {
    ensure_cli_defaults(&cfg.env_file())?;
    println!("直接回车表示保留原值；输入 CLEAR 表示清空该项。");
    println!("私钥不会回显。Polymarket 网站账户通常用 signature type=proxy。");

    let private_key = prompt_secret(&format!(
        "POLY_PRIVATE_KEY 当前={}  钱包私钥",
        secret_state(&cfg.env_file(), "POLY_PRIVATE_KEY")
    ))?;
    apply_optional_env(&cfg.env_file(), "POLY_PRIVATE_KEY", &private_key)?;

    let current_signature =
        env_value(&cfg.env_file(), "POLY_SIGNATURE_TYPE").unwrap_or_else(|| "proxy".to_string());
    let signature = prompt(&format!(
        "POLY_SIGNATURE_TYPE 当前={}  eoa/proxy/gnosis_safe/poly1271，Polymarket账户建议proxy",
        current_signature
    ))?;
    if signature.trim().is_empty() && current_signature.trim().is_empty() {
        upsert_env(&cfg.env_file(), "POLY_SIGNATURE_TYPE", "proxy")?;
    } else {
        apply_optional_env(&cfg.env_file(), "POLY_SIGNATURE_TYPE", &signature)?;
    }

    let funder_current = public_state(&cfg.env_file(), "POLY_FUNDER_ADDRESS");
    let funder = prompt(&format!(
        "POLY_FUNDER_ADDRESS 当前={}  Polymarket代理钱包/funder地址",
        funder_current
    ))?;
    apply_optional_env(&cfg.env_file(), "POLY_FUNDER_ADDRESS", &funder)?;

    println!();
    println!("L2 API 凭证可以先不填；不填时 SDK 会尝试用私钥创建/派生。");
    let api_key = prompt_secret(&format!(
        "POLY_API_KEY 当前={}  L2 API key",
        secret_state(&cfg.env_file(), "POLY_API_KEY")
    ))?;
    apply_optional_env(&cfg.env_file(), "POLY_API_KEY", &api_key)?;
    let secret = prompt_secret(&format!(
        "POLY_SECRET 当前={}  L2 secret",
        secret_state(&cfg.env_file(), "POLY_SECRET")
    ))?;
    apply_optional_env(&cfg.env_file(), "POLY_SECRET", &secret)?;
    let passphrase = prompt_secret(&format!(
        "POLY_PASSPHRASE 当前={}  L2 passphrase",
        secret_state(&cfg.env_file(), "POLY_PASSPHRASE")
    ))?;
    apply_optional_env(&cfg.env_file(), "POLY_PASSPHRASE", &passphrase)?;

    upsert_env_if_missing(
        &cfg.env_file(),
        "POLYMARKET_CLOB_HOST",
        "https://clob.polymarket.com",
    )?;
    upsert_env_if_missing(&cfg.env_file(), "POLY_CHAIN_ID", "137")?;
    println!("账户配置已写入 .env。");
    Ok(())
}

fn configure_live_market(cfg: &Config) -> AppResult<()> {
    ensure_cli_defaults(&cfg.env_file())?;
    println!("直接回车表示保留原值。通常保持 AUTO_DISCOVER_MARKET=1，不需要手填。");
    let auto = prompt(&format!(
        "AUTO_DISCOVER_MARKET 当前={}  1=自动发现，0=手动覆盖",
        public_state(&cfg.env_file(), "AUTO_DISCOVER_MARKET")
    ))?;
    apply_optional_env(&cfg.env_file(), "AUTO_DISCOVER_MARKET", &auto)?;
    let up = prompt(&format!(
        "POLYMARKET_UP_TOKEN_ID 当前={}  当前市场 Up CLOB token id",
        public_state(&cfg.env_file(), "POLYMARKET_UP_TOKEN_ID")
    ))?;
    apply_optional_env(&cfg.env_file(), "POLYMARKET_UP_TOKEN_ID", &up)?;

    let down = prompt(&format!(
        "POLYMARKET_DOWN_TOKEN_ID 当前={}  当前市场 Down CLOB token id",
        public_state(&cfg.env_file(), "POLYMARKET_DOWN_TOKEN_ID")
    ))?;
    apply_optional_env(&cfg.env_file(), "POLYMARKET_DOWN_TOKEN_ID", &down)?;

    let price = prompt(&format!(
        "PRICE_TO_BEAT 当前={}  题目里的BTC判定价/开盘价",
        public_state(&cfg.env_file(), "PRICE_TO_BEAT")
    ))?;
    apply_optional_env(&cfg.env_file(), "PRICE_TO_BEAT", &price)?;

    upsert_env_if_missing(
        &cfg.env_file(),
        "POLYMARKET_WS_URL",
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    )?;
    upsert_env_if_missing(
        &cfg.env_file(),
        "POLYMARKET_USER_WS_URL",
        "wss://ws-subscriptions-clob.polymarket.com/ws/user",
    )?;
    upsert_env_if_missing(
        &cfg.env_file(),
        "BINANCE_WS_URL",
        "wss://stream.binance.com:9443/ws/btcusdt@trade",
    )?;
    println!("当前市场配置已写入 .env。");
    Ok(())
}

fn edit_market_maker_params(cfg: &Config) -> AppResult<()> {
    if !cfg.env_file().exists() {
        init_config(cfg)?;
    }
    let keys = [
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
        ("LIVE_ORDER_NOTIONAL_CAP", "实单单笔名义金额上限"),
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
    println!("  AUTO_DISCOVER_*   1=自动发现当前5分钟市场并自动切换token");
    println!("  POLYMARKET_*      手动覆盖时的当前市场Up/Down token id");
    println!("  PRICE_TO_BEAT     手动覆盖时的判定价；自动模式会用开盘价/最新价兜底");
    println!("  QUOTE_SIZE        每次单边报价份数");
    println!("  QUOTE_SPREAD      毛价差，约束 up_bid + down_bid <= 1 - spread");
    println!("  QUOTE_TTL_MS      未成交报价 pending 保留多久，过期释放库存占用");
    println!("  REQUOTE_*         旧报价和新报价差多少 tick 才撤旧换新");
    println!("  INVENTORY_SKEW    库存偏移，多仓侧降价、少仓侧抬价");
    println!("  INVENTORY_MULT    单边最大库存 = QUOTE_SIZE * INVENTORY_MULT");
    println!("  MAX_LOSS          最坏结算情景亏损达到阈值就停止");
    println!("  MAX_TOTAL_*       Up+Down 已成交+pending 达到阈值就停止");
    println!("  ENABLE_REAL_*     实单确认串；菜单2切实单时会自动写入");
    println!("  POLY_PRIVATE_*    私钥；菜单2可隐藏输入，别提交到GitHub");
    println!("  POLY_SIGNATURE_*  钱包签名类型；Polymarket账户通常是 proxy + funder");
    println!("  MIN_BID/MAX_BID   最低/最高挂买价");
    println!("  STALE_AFTER_MS    行情过期阈值，超过则停止用旧行情报价");
    println!("  WS_STALE_AFTER_MS live WS断流阈值，超过则停止");
    println!("  DRY_RUN           1=模拟；0=实单，还必须设置 ENABLE_REAL_ORDERS 确认串");
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
        if let Some(hb) = read_dashboard_json::<Heartbeat>(&entry.path()) {
            out.push(hb);
        }
    }
    out.sort_by(|a, b| a.role.cmp(&b.role));
    Ok(out)
}

fn service_status(cfg: &Config) -> String {
    let Some(pid) = read_pid(&cfg.pid_file()) else {
        return format!("{C_RED}未运行{C_RESET}");
    };
    if process_alive(pid) {
        format!("{C_GREEN}运行中 pid={pid}{C_RESET}")
    } else {
        format!("{C_RED}pid={pid} 已退出{C_RESET}")
    }
}

fn stop_status(cfg: &Config) -> String {
    if cfg.stop_file().exists() {
        format!("{C_YELLOW}存在{C_RESET}")
    } else {
        format!("{C_GREEN}无{C_RESET}")
    }
}

fn read_pid(path: &Path) -> Option<u32> {
    fs::read_to_string(path).ok()?.trim().parse::<u32>().ok()
}

fn process_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

fn maybe_restart_background(cfg: &Config) -> AppResult<()> {
    let Some(pid) = read_pid(&cfg.pid_file()) else {
        println!("配置已保存。后台服务未运行，下次启动生效。");
        return Ok(());
    };
    if !process_alive(pid) {
        println!("配置已保存。旧 pid={pid} 已退出，下次启动生效。");
        return Ok(());
    }
    let input = prompt("检测到后台服务正在运行，是否立即重启让配置生效？y/N")?;
    if is_yes(&input) {
        let updated = Config::from_env().unwrap_or_else(|_| cfg.clone());
        workers::restart_background(&updated)?;
    } else {
        println!("配置已保存。稍后执行 `polymaker restart` 后生效。");
    }
    Ok(())
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

fn read_dashboard_json<T: DeserializeOwned>(path: &Path) -> Option<T> {
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str::<T>(&raw).ok()
}

fn ensure_cli_defaults(path: &Path) -> AppResult<()> {
    upsert_env_if_missing(path, "DRY_RUN", "1")?;
    upsert_env_if_missing(path, "ENABLE_REAL_ORDERS", "")?;
    upsert_env_if_missing(path, "BOT_RUN_DIR", "run")?;
    upsert_env_if_missing(path, "MARKET_SLUG", "btc-updown-5m")?;
    upsert_env_if_missing(path, "DATA_MODE", "sim")?;
    upsert_env_if_missing(path, "AUTO_DISCOVER_MARKET", "1")?;
    upsert_env_if_missing(
        path,
        "POLYMARKET_GAMMA_API_URL",
        "https://gamma-api.polymarket.com",
    )?;
    upsert_env_if_missing(path, "MARKET_WINDOW_SECS", "300")?;
    upsert_env_if_missing(path, "MARKET_DISCOVERY_MS", "2000")?;
    upsert_env_if_missing(path, "MARKET_SWITCH_GRACE_MS", "90000")?;
    upsert_env_if_blank(
        path,
        "POLYMARKET_WS_URL",
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    )?;
    upsert_env_if_blank(
        path,
        "POLYMARKET_USER_WS_URL",
        "wss://ws-subscriptions-clob.polymarket.com/ws/user",
    )?;
    upsert_env_if_blank(
        path,
        "BINANCE_WS_URL",
        "wss://stream.binance.com:9443/ws/btcusdt@trade",
    )?;
    upsert_env_if_blank(path, "BINANCE_REST_URL", "https://api.binance.com")?;
    upsert_env_if_missing(path, "POLYMARKET_UP_TOKEN_ID", "")?;
    upsert_env_if_missing(path, "POLYMARKET_DOWN_TOKEN_ID", "")?;
    upsert_env_if_blank(path, "POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")?;
    upsert_env_if_blank(path, "POLY_CHAIN_ID", "137")?;
    upsert_env_if_missing(path, "POLY_PRIVATE_KEY", "")?;
    upsert_env_if_missing(path, "POLY_API_KEY", "")?;
    upsert_env_if_missing(path, "POLY_SECRET", "")?;
    upsert_env_if_missing(path, "POLY_PASSPHRASE", "")?;
    upsert_env_if_blank(path, "POLY_SIGNATURE_TYPE", "proxy")?;
    upsert_env_if_missing(path, "POLY_FUNDER_ADDRESS", "")?;
    upsert_env_if_missing(path, "PRICE_TO_BEAT", "68000")?;
    upsert_env_if_missing(path, "QUOTE_SIZE", "5")?;
    upsert_env_if_missing(path, "QUOTE_SPREAD", "0.04")?;
    upsert_env_if_missing(path, "QUOTE_TTL_MS", "1500")?;
    upsert_env_if_missing(path, "REQUOTE_THRESHOLD_TICKS", "1")?;
    upsert_env_if_missing(path, "INVENTORY_SKEW", "0.03")?;
    upsert_env_if_missing(path, "INVENTORY_MULT", "2")?;
    upsert_env_if_missing(path, "MIN_BID", "0.05")?;
    upsert_env_if_missing(path, "MAX_BID", "0.62")?;
    upsert_env_if_missing(path, "TICK_SIZE", "0.01")?;
    upsert_env_if_missing(path, "BTC_SIGMA_USD", "35")?;
    upsert_env_if_missing(path, "SIM_FILL_CHANCE", "0.18")?;
    upsert_env_if_missing(path, "MAX_LOSS", "25")?;
    upsert_env_if_missing(path, "MAX_TOTAL_INVENTORY", "50")?;
    upsert_env_if_missing(path, "LIVE_ORDER_NOTIONAL_CAP", "5")?;
    upsert_env_if_missing(path, "MARKET_INTERVAL_MS", "120")?;
    upsert_env_if_missing(path, "STALE_AFTER_MS", "800")?;
    upsert_env_if_missing(path, "WS_STALE_AFTER_MS", "10000")?;
    Ok(())
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

fn upsert_env_if_missing(path: &Path, key: &str, value: &str) -> AppResult<()> {
    if env_value(path, key).is_none() {
        upsert_env(path, key, value)?;
    }
    Ok(())
}

fn upsert_env_if_blank(path: &Path, key: &str, value: &str) -> AppResult<()> {
    if env_value(path, key).is_none_or(|v| v.trim().is_empty()) {
        upsert_env(path, key, value)?;
    }
    Ok(())
}

fn apply_optional_env(path: &Path, key: &str, input: &str) -> AppResult<()> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    if trimmed.eq_ignore_ascii_case("CLEAR") {
        upsert_env(path, key, "")
    } else {
        upsert_env(path, key, trimmed)
    }
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

fn secret_state(path: &Path, key: &str) -> &'static str {
    if env_value(path, key).is_some_and(|v| !v.trim().is_empty()) {
        "(已填)"
    } else {
        "(未填)"
    }
}

fn public_state(path: &Path, key: &str) -> String {
    env_value(path, key)
        .filter(|v| !v.trim().is_empty())
        .map(|v| shorten_value(&v))
        .unwrap_or_else(|| "(未填)".to_string())
}

fn shorten_value(value: &str) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= 24 {
        return value.to_string();
    }
    let head = chars.iter().take(10).collect::<String>();
    let tail = chars
        .iter()
        .skip(chars.len().saturating_sub(6))
        .collect::<String>();
    format!("{head}...{tail}")
}

fn prompt(label: &str) -> AppResult<String> {
    print!("{C_CYAN}?{C_RESET} {label} > ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim_end().to_string())
}

fn prompt_secret(label: &str) -> AppResult<String> {
    print!("{C_CYAN}?{C_RESET} {label} > ");
    io::stdout().flush()?;
    let echo_disabled = Command::new("stty")
        .arg("-echo")
        .status()
        .is_ok_and(|status| status.success());
    let mut input = String::new();
    let read_result = io::stdin().read_line(&mut input);
    if echo_disabled {
        let _ = Command::new("stty").arg("echo").status();
        println!();
    }
    read_result?;
    Ok(input.trim_end().to_string())
}

fn is_yes(input: &str) -> bool {
    matches!(
        input.trim().to_ascii_lowercase().as_str(),
        "y" | "yes" | "1"
    ) || matches!(input.trim(), "是" | "好" | "确认")
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
