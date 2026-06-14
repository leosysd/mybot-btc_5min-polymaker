use crate::config::Config;
use crate::ipc::{now_ms, FillEvent, Heartbeat, Inventory, MarketFrame, QuoteIntent};
use crate::pricing::{blend_market_anchor, digital_p_up, market_anchor_weight, uncertainty_width};
use crate::workers;
use crate::AppResult;
use alloy::signers::local::PrivateKeySigner;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
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
        secure_env_permissions(&env_path)?;
        created = true;
    }

    ensure_cli_defaults(&env_path)?;
    secure_env_permissions(&env_path)?;
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
        println!("  {}12.{} 交易统计表", C_GREEN, C_RESET);
        println!("  {}13.{} 模型/盘口胜率对比", C_GREEN, C_RESET);
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
            "12" => {
                clear_screen();
                print_trade_stats(&active_cfg)?;
                pause()?;
            }
            "13" => {
                clear_screen();
                print_model_market_stats(&active_cfg)?;
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
            ("UpBid".to_string(), 7, Align::Right),
            ("UpAsk".to_string(), 7, Align::Right),
            ("DnBid".to_string(), 7, Align::Right),
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
                (format_market_px(r.up_bid), 7, Align::Right),
                (format!("{:.3}", r.up_ask), 7, Align::Right),
                (format_market_px(r.down_bid), 7, Align::Right),
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
    let signature =
        env_value(&path, "POLY_SIGNATURE_TYPE").unwrap_or_else(|| "poly1271".to_string());

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
    upsert_env(&cfg.env_file(), "POLY_SIGNATURE_TYPE", "poly1271")?;
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

    let signature =
        env_value(&path, "POLY_SIGNATURE_TYPE").unwrap_or_else(|| "poly1271".to_string());
    if signature.trim() != "eoa"
        && env_value(&path, "POLY_FUNDER_ADDRESS").is_none_or(|v| v.trim().is_empty())
    {
        missing.push("POLY_FUNDER_ADDRESS(deposit wallet/funder地址)".to_string());
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
    println!("私钥不会回显。Polymarket deposit wallet 通常用 signature type=poly1271。");

    let private_key = prompt_secret(&format!(
        "POLY_PRIVATE_KEY 当前={}  钱包私钥",
        secret_state(&cfg.env_file(), "POLY_PRIVATE_KEY")
    ))?;
    apply_optional_env(&cfg.env_file(), "POLY_PRIVATE_KEY", &private_key)?;

    let current_signature =
        env_value(&cfg.env_file(), "POLY_SIGNATURE_TYPE").unwrap_or_else(|| "poly1271".to_string());
    let signature = prompt(&format!(
        "POLY_SIGNATURE_TYPE 当前={}  eoa/proxy/gnosis_safe/poly1271，deposit wallet建议poly1271",
        current_signature
    ))?;
    if signature.trim().is_empty() && current_signature.trim().is_empty() {
        upsert_env(&cfg.env_file(), "POLY_SIGNATURE_TYPE", "poly1271")?;
    } else {
        apply_optional_env(&cfg.env_file(), "POLY_SIGNATURE_TYPE", &signature)?;
    }

    let funder_current = public_state(&cfg.env_file(), "POLY_FUNDER_ADDRESS");
    let funder = prompt(&format!(
        "POLY_FUNDER_ADDRESS 当前={}  Polymarket deposit wallet/funder地址",
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
        ("MAX_UNPAIRED_SHARES", "最大未配平份额"),
        ("MIN_BID", "最低挂买价"),
        ("MAX_BID", "最高挂买价"),
        ("MAX_LOSS", "最坏情景最大亏损"),
        ("MAX_TOTAL_INVENTORY", "Up+Down最大总库存"),
        ("LIVE_ORDER_NOTIONAL_CAP", "实单单笔名义金额上限"),
        ("MARKET_INTERVAL_MS", "模拟行情间隔毫秒"),
        ("STALE_AFTER_MS", "行情过期毫秒"),
        ("WS_STALE_AFTER_MS", "live WS断流停止毫秒"),
        ("VALUE_MIN_EDGE", "价值买入最低安全垫: fair - bid"),
        ("VALUE_AGGRESSION_TICKS", "价值买入相对买一抬高多少tick"),
        ("VALUE_MIN_FAIR", "价值买入最低模型胜率过滤"),
        ("ENABLE_DIRECTION_EDGE", "1=方向强弱调整edge"),
        ("DIRECTION_ALIGNED_EDGE", "顺方向最低安全垫"),
        ("DIRECTION_COUNTER_EDGE", "反方向最低安全垫"),
        ("ENABLE_MARKET_ANCHOR", "1=真实报价使用盘口锚定融合胜率"),
        ("MARKET_ANCHOR_SHADOW", "1=记录影子融合胜率,不改报价"),
        ("MARKET_ANCHOR_WEIGHT", "盘口锚定兼容默认权重"),
        ("MARKET_ANCHOR_WEIGHT_HIGH", "模型高胜率边盘口锚定权重"),
        ("MARKET_ANCHOR_WEIGHT_LOW", "模型低胜率边盘口锚定权重"),
        ("MARKET_ANCHOR_LOW_SIDE_BELOW", "低胜率边判定阈值"),
        ("MARKET_ANCHOR_MAX_SPREAD", "盘口健康价差上限"),
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
    println!("  PRICE_TO_BEAT     手动覆盖时的判定价；自动模式会用Binance开盘价");
    println!("  QUOTE_SIZE        每次单边报价份数");
    println!("  QUOTE_SPREAD      毛价差，约束 up_bid + down_bid <= 1 - spread");
    println!("  QUOTE_TTL_MS      未成交报价 pending 保留多久，过期释放库存占用");
    println!("  REQUOTE_*         旧报价和新报价差多少 tick 才撤旧换新");
    println!("  INVENTORY_SKEW    库存偏移，多仓侧降价、少仓侧抬价");
    println!("  INVENTORY_MULT    单边最大库存 = QUOTE_SIZE * INVENTORY_MULT");
    println!("  MAX_UNPAIRED_*    最大未配平份额，超过后只允许挂落后的一边");
    println!("  MAX_LOSS          最坏结算情景亏损达到阈值就停止");
    println!("  MAX_TOTAL_*       Up+Down 已成交+pending 达到阈值就停止");
    println!("  ENABLE_REAL_*     实单确认串；菜单2切实单时会自动写入");
    println!("  POLY_PRIVATE_*    私钥；菜单2可隐藏输入，别提交到GitHub");
    println!("  POLY_SIGNATURE_*  钱包签名类型；deposit wallet 通常是 poly1271 + funder");
    println!("  MIN_BID/MAX_BID   最低/最高挂买价");
    println!("  STALE_AFTER_MS    行情过期阈值，超过则停止用旧行情报价");
    println!("  WS_STALE_AFTER_MS live WS断流阈值，超过则停止");
    println!("  DRY_RUN           1=模拟；0=实单，还必须设置 ENABLE_REAL_ORDERS 确认串");
    println!();
    println!("{}策略大脑 v3 参数{}", C_BOLD, C_RESET);
    println!("  VOL_SEED_PER_SQRT_SEC  BTC波动率初值(σ_$/√秒)，未热身时用；之后自适应");
    println!("  VOL_HALFLIFE_SEC       波动率EWMA半衰期，越小越灵敏");
    println!("  WIDTH_FLOOR_USD        收盘前不确定性W的下限，防止gamma爆炸");
    println!("  BASE_HALF_SPREAD       基础半价差(价格单位)");
    println!("  MIN_LOCK_EDGE          双边成交锁利下限 up_bid+down_bid<=1-该值");
    println!("  LATENCY_SEC            你的撤改单延迟，越大逆选择溢价越高");
    println!("  K_ADVERSE              逆选择价差强度;ATM临近收盘会自动拉宽");
    println!("  MIN/MAX_HALF_SPREAD    半价差上下限钳制");
    println!("  ENDGAME_PULL_SECS      剩余秒数<该值撤掉所有单(纯抛硬币区)");
    println!("  INVENTORY_SKEW_TIME_BOOST 库存偏移随临近收盘加码倍数");
    println!("  TOX_*                  逆选择监控:被割就自动加宽价差");
    println!("  VALUE_MIN_EDGE         最低安全垫: 买价必须 <= 模型fair-该值");
    println!("  VALUE_AGGRESSION_TICKS 相对当前买一抬高多少tick,仍受安全垫限制");
    println!("  VALUE_MIN_FAIR         最低模型胜率过滤,低于则不挂这一边");
    println!("  ENABLE_DIRECTION_EDGE  1=按方向强弱调整edge,不禁止任何一边");
    println!("  DIRECTION_ALIGNED_EDGE 顺方向最低安全垫,越小越容易成交");
    println!("  DIRECTION_COUNTER_EDGE 反方向最低安全垫,越大越要求便宜");
    println!("  ENABLE_MARKET_ANCHOR   1=真实报价使用盘口锚定融合胜率;默认0");
    println!("  MARKET_ANCHOR_SHADOW   1=只记录FinalUp影子胜率,不改变报价");
    println!("  MARKET_ANCHOR_WEIGHT   兼容默认权重;HIGH/LOW未设置时使用它");
    println!("  MARKET_ANCHOR_WEIGHT_HIGH 模型高胜率边盘口锚定权重");
    println!("  MARKET_ANCHOR_WEIGHT_LOW  模型低胜率边盘口锚定权重");
    println!("  MARKET_ANCHOR_LOW_SIDE_BELOW 低于该模型胜率的一边按LOW权重");
    println!("  MARKET_ANCHOR_MAX_SPREAD 盘口健康价差上限,超过则权重归零");
    println!("  ENABLE_DELTA_HEDGE     0=关。对冲为占位骨架，实单模式禁止开启");
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
    let Ok(file) = fs::File::open(path) else {
        return Ok(Vec::new());
    };
    let reader = BufReader::new(file);
    let mut lines = VecDeque::with_capacity(n);
    for line in reader.lines() {
        let Ok(line) = line else {
            continue;
        };
        if line.trim().is_empty() {
            continue;
        }
        if lines.len() == n {
            lines.pop_front();
        }
        lines.push_back(line);
    }
    let mut out = Vec::new();
    for line in lines {
        if let Ok(row) = serde_json::from_str::<T>(&line) {
            out.push(row);
        }
    }
    Ok(out)
}

fn for_each_jsonl<T, F>(path: &Path, mut on_row: F) -> AppResult<()>
where
    T: DeserializeOwned,
    F: FnMut(T),
{
    let Ok(file) = fs::File::open(path) else {
        return Ok(());
    };
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let Ok(line) = line else {
            continue;
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(row) = serde_json::from_str::<T>(line) {
            on_row(row);
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct MarketStat {
    up_shares: f64,
    up_cost: f64,
    down_shares: f64,
    down_cost: f64,
    fills: u64,
}

impl MarketStat {
    fn new() -> Self {
        Self {
            up_shares: 0.0,
            up_cost: 0.0,
            down_shares: 0.0,
            down_cost: 0.0,
            fills: 0,
        }
    }
    fn pnl_if_up(&self) -> f64 {
        (self.up_shares - self.up_cost) - self.down_cost
    }
    fn pnl_if_down(&self) -> f64 {
        (self.down_shares - self.down_cost) - self.up_cost
    }

    fn total_abs_shares(&self) -> f64 {
        self.up_shares.abs() + self.down_shares.abs()
    }

    fn total_abs_cost(&self) -> f64 {
        self.up_cost.abs() + self.down_cost.abs()
    }

    fn materially_differs_from(&self, other: &Self) -> bool {
        const EPS: f64 = 0.000_001;
        self.fills != other.fills
            || (self.up_shares - other.up_shares).abs() > EPS
            || (self.down_shares - other.down_shares).abs() > EPS
            || (self.up_cost - other.up_cost).abs() > EPS
            || (self.down_cost - other.down_cost).abs() > EPS
    }
}

fn official_stats_user(cfg: &Config) -> Option<String> {
    let funder = cfg.poly_funder_address.trim();
    if !funder.is_empty() {
        return Some(funder.to_string());
    }
    let private_key = cfg.poly_private_key.trim();
    if private_key.is_empty() {
        return None;
    }
    PrivateKeySigner::from_str(private_key)
        .ok()
        .map(|signer| signer.address().to_string())
}

fn fetch_official_trade_stats(cfg: &Config, markets: &[String]) -> HashMap<String, MarketStat> {
    let Some(user) = official_stats_user(cfg) else {
        return HashMap::new();
    };
    if markets.is_empty() {
        return HashMap::new();
    }

    let wanted = markets.iter().cloned().collect::<HashSet<_>>();
    let min_start = markets.iter().filter_map(|m| market_start_secs(m)).min();
    let max_start = markets.iter().filter_map(|m| market_start_secs(m)).max();
    let start_filter = min_start.map(|s| s.saturating_sub(60));
    let end_filter = max_start
        .and_then(|s| s.checked_add(cfg.market_window_secs))
        .and_then(|s| s.checked_add(600));

    let mut out = HashMap::new();
    let mut offset = 0;
    const LIMIT: usize = 500;
    while offset <= 10_000 {
        let mut url = format!(
            "https://data-api.polymarket.com/activity?user={user}&type=TRADE&limit={LIMIT}&offset={offset}"
        );
        if let Some(start) = start_filter {
            url.push_str(&format!("&start={start}"));
        }
        if let Some(end) = end_filter {
            url.push_str(&format!("&end={end}"));
        }

        let response = match ureq::get(&url)
            .set("User-Agent", "polymaker/0.1")
            .timeout(Duration::from_secs(5))
            .call()
        {
            Ok(response) => response,
            Err(_) => break,
        };
        let value: Value = match response.into_json() {
            Ok(value) => value,
            Err(_) => break,
        };
        let Some(items) = value.as_array() else {
            break;
        };
        for item in items {
            let Some(slug) = item.get("slug").and_then(Value::as_str) else {
                continue;
            };
            if !wanted.contains(slug) {
                continue;
            }
            let stat = out.entry(slug.to_string()).or_insert_with(MarketStat::new);
            apply_official_trade_activity(stat, item);
        }
        if items.len() < LIMIT {
            break;
        }
        offset += LIMIT;
    }
    out
}

fn apply_official_trade_activity(stat: &mut MarketStat, row: &Value) -> bool {
    if !row
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|t| t.eq_ignore_ascii_case("TRADE"))
    {
        return false;
    }
    let side_mult = match row.get("side").and_then(Value::as_str) {
        Some(side) if side.eq_ignore_ascii_case("BUY") => 1.0,
        Some(side) if side.eq_ignore_ascii_case("SELL") => -1.0,
        _ => return false,
    };
    let Some(outcome) = row.get("outcome").and_then(Value::as_str) else {
        return false;
    };
    let Some(size) = value_as_f64(row.get("size")) else {
        return false;
    };
    if size <= 0.0 || !size.is_finite() {
        return false;
    }
    let cost = value_as_f64(row.get("usdcSize"))
        .or_else(|| value_as_f64(row.get("price")).map(|price| price * size))
        .unwrap_or(0.0);

    match outcome.trim().to_ascii_lowercase().as_str() {
        "up" => {
            stat.up_shares += side_mult * size;
            stat.up_cost += side_mult * cost;
        }
        "down" => {
            stat.down_shares += side_mult * size;
            stat.down_cost += side_mult * cost;
        }
        _ => return false,
    }
    stat.fills += 1;
    true
}

fn value_as_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn should_use_official_stat(
    local: &MarketStat,
    official: &MarketStat,
    market: &str,
    cfg: &Config,
) -> bool {
    if official.fills == 0 || !official.materially_differs_from(local) {
        return false;
    }
    let official_has_at_least_local_volume = official.total_abs_shares() + 0.000_001
        >= local.total_abs_shares()
        && official.total_abs_cost() + 0.000_001 >= local.total_abs_cost();
    if official_has_at_least_local_volume {
        return true;
    }
    let now_secs = now_ms() / 1_000;
    market_start_secs(market)
        .and_then(|start| start.checked_add(cfg.market_window_secs))
        .is_some_and(|end| now_secs > end.saturating_add(30))
}

fn format_shares(value: f64) -> String {
    if (value - value.round()).abs() < 0.01 {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

fn short_market(slug: &str) -> String {
    let chars: Vec<char> = slug.chars().collect();
    if chars.len() <= 22 {
        slug.to_string()
    } else {
        format!("…{}", chars[chars.len() - 21..].iter().collect::<String>())
    }
}

/// Render a slugged 5-minute market as "MM-DD HH:MM-HH:MM" in Beijing time.
fn fmt_market_window_label(slug: &str, window_secs: u64) -> String {
    if let Some(start) = market_start_secs(slug) {
        let end = start.saturating_add(window_secs);
        return format!("{}-{}", fmt_window_time(start), fmt_window_end_time(end));
    }
    short_market(slug)
}

fn market_start_secs(slug: &str) -> Option<u64> {
    slug.rsplit('-')
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|ts| (1_000_000_000..=4_000_000_000).contains(ts))
}

fn is_settlement_frame(fr: &MarketFrame, window_secs: u64, now_ms: u64) -> bool {
    const CLOSE_CAPTURE_GRACE_MS: u64 = 1_500;

    if let Some(start_secs) = market_start_secs(&fr.market) {
        let Some(end_secs) = start_secs.checked_add(window_secs) else {
            return false;
        };
        let Some(end_ms) = end_secs.checked_mul(1_000) else {
            return false;
        };
        return now_ms >= end_ms && fr.ts_ms.saturating_add(CLOSE_CAPTURE_GRACE_MS) >= end_ms;
    }

    fr.tau_seconds > 0.0 && fr.tau_seconds <= 1.0
}

/// Unix seconds -> "MM-DD HH:MM" in Beijing time (UTC+8), no external deps.
fn fmt_window_time(unix_secs: u64) -> String {
    let s = unix_secs as i64 + 8 * 3600;
    let days = s.div_euclid(86_400);
    let sod = s.rem_euclid(86_400);
    let (hh, mm) = (sod / 3600, (sod % 3600) / 60);
    let (_, mon, day) = civil_from_days(days);
    format!("{mon:02}-{day:02} {hh:02}:{mm:02}")
}

fn fmt_window_end_time(unix_secs: u64) -> String {
    let s = unix_secs as i64 + 8 * 3600;
    let sod = s.rem_euclid(86_400);
    let (hh, mm) = (sod / 3600, (sod % 3600) / 60);
    format!("{hh:02}:{mm:02}")
}

/// Days since 1970-01-01 -> (year, month, day). Howard Hinnant's algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (y + i64::from(m <= 2), m, d)
}

/// 交易统计表：按盘口聚合成交，展示双边/单边、情景 PnL、锁定毛利与汇总。
pub fn print_trade_stats(cfg: &Config) -> AppResult<()> {
    print_banner("POLYMAKER 交易统计");
    let mut order: Vec<String> = Vec::new();
    let mut agg: HashMap<String, MarketStat> = HashMap::new();
    let mut fill_rows = 0u64;
    for_each_jsonl::<FillEvent, _>(&cfg.fills_path(), |f| {
        fill_rows += 1;
        let stat = agg.entry(f.market.clone()).or_insert_with(|| {
            order.push(f.market.clone());
            MarketStat::new()
        });
        match f.side.as_str() {
            "Up" => {
                stat.up_shares += f.size;
                stat.up_cost += f.price * f.size;
            }
            "Down" => {
                stat.down_shares += f.size;
                stat.down_cost += f.price * f.size;
            }
            _ => {}
        }
        stat.fills += 1;
    })?;
    if fill_rows == 0 {
        println!(
            "{}暂无成交记录 ({})。先跑模拟或实盘产生 fills.jsonl。{}",
            C_DIM,
            cfg.fills_path().display(),
            C_RESET
        );
        return Ok(());
    }

    let official_trade_stats = fetch_official_trade_stats(cfg, &order);
    let official_won_up = fetch_official_settlements(cfg, &order);

    // Local settlement proxy fallback: only near-close frames after the scheduled
    // end are eligible. Otherwise the still-live latest frame would make the
    // current active market look settled.
    let mut local_won_up: std::collections::HashMap<String, bool> =
        std::collections::HashMap::new();
    let mut close_ts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let stats_now_ms = now_ms();
    for_each_jsonl::<MarketFrame, _>(&cfg.book_path(), |fr| {
        if fr.btc_price <= 0.0 || fr.price_to_beat <= 0.0 {
            return;
        }
        if !is_settlement_frame(&fr, cfg.market_window_secs, stats_now_ms) {
            return;
        }
        if close_ts.get(&fr.market).map_or(true, |&t| fr.ts_ms >= t) {
            close_ts.insert(fr.market.clone(), fr.ts_ms);
            local_won_up.insert(fr.market.clone(), fr.btc_price > fr.price_to_beat);
        }
    })?;

    println!(
        "{}",
        table_row(&[
            ("盘口".to_string(), 18, Align::Left),
            ("成交".to_string(), 5, Align::Right),
            ("Up份".to_string(), 6, Align::Right),
            ("Up均".to_string(), 6, Align::Right),
            ("Dn份".to_string(), 6, Align::Right),
            ("Dn均".to_string(), 6, Align::Right),
            ("双边成本".to_string(), 8, Align::Right),
            ("Up赢".to_string(), 7, Align::Right),
            ("Dn赢".to_string(), 7, Align::Right),
            ("结算".to_string(), 6, Align::Left),
            ("实现".to_string(), 8, Align::Right),
        ])
    );

    let (mut locked, mut directional) = (0u64, 0u64);
    let (mut sum_worst, mut sum_locked_edge, mut total_fills) = (0.0_f64, 0.0_f64, 0u64);
    let (mut sum_realized, mut settled, mut wins, mut losses, mut up_wins, mut dn_wins) =
        (0.0_f64, 0u64, 0u64, 0u64, 0u64, 0u64);
    let (mut official_corrected, mut official_share_delta) = (0u64, 0.0_f64);
    for market in &order {
        let local = &agg[market];
        let official = official_trade_stats.get(market);
        let use_official =
            official.is_some_and(|s| should_use_official_stat(local, s, market, cfg));
        let s = if use_official {
            let official = official.expect("checked by is_some_and");
            official_corrected += 1;
            official_share_delta += (official.total_abs_shares() - local.total_abs_shares()).abs();
            official
        } else {
            local
        };
        let fill_cell = if use_official {
            format!("{}/{}", local.fills, s.fills)
        } else {
            s.fills.to_string()
        };
        let up_avg = if s.up_shares > 0.0 {
            s.up_cost / s.up_shares
        } else {
            0.0
        };
        let dn_avg = if s.down_shares > 0.0 {
            s.down_cost / s.down_shares
        } else {
            0.0
        };
        let two_sided = s.up_shares > 0.0 && s.down_shares > 0.0;
        let combined = if two_sided {
            up_avg + dn_avg
        } else {
            up_avg.max(dn_avg)
        };
        if two_sided {
            locked += 1;
            let pairs = s.up_shares.min(s.down_shares);
            sum_locked_edge += (pairs * (1.0 - (up_avg + dn_avg))).max(0.0);
        } else {
            directional += 1;
        }
        sum_worst += s.pnl_if_up().min(s.pnl_if_down());
        total_fills += s.fills;

        // Actual settlement + realized PnL for the window (if we have a close).
        let (settle_cell, realized_cell) = match official_won_up
            .get(market)
            .copied()
            .or_else(|| local_won_up.get(market).copied())
        {
            Some(up) => {
                settled += 1;
                let realized = if up { s.pnl_if_up() } else { s.pnl_if_down() };
                sum_realized += realized;
                if realized > 0.0 {
                    wins += 1
                } else if realized < 0.0 {
                    losses += 1
                }
                if up {
                    up_wins += 1
                } else {
                    dn_wins += 1
                }
                let label = if up {
                    format!("{C_GREEN}Up{C_RESET}")
                } else {
                    format!("{C_RED}Down{C_RESET}")
                };
                let pnl = if realized >= 0.0 {
                    format!("{C_GREEN}{realized:+.2}{C_RESET}")
                } else {
                    format!("{C_RED}{realized:+.2}{C_RESET}")
                };
                (label, pnl)
            }
            None => (format!("{C_DIM}?{C_RESET}"), format!("{C_DIM}-{C_RESET}")),
        };

        println!(
            "{}",
            table_row(&[
                (
                    fmt_market_window_label(market, cfg.market_window_secs),
                    18,
                    Align::Left,
                ),
                (fill_cell, 5, Align::Right),
                (format_shares(s.up_shares), 6, Align::Right),
                (format!("{:.3}", up_avg), 6, Align::Right),
                (format_shares(s.down_shares), 6, Align::Right),
                (format!("{:.3}", dn_avg), 6, Align::Right),
                (format!("{:.3}", combined), 8, Align::Right),
                (format!("{:+.2}", s.pnl_if_up()), 7, Align::Right),
                (format!("{:+.2}", s.pnl_if_down()), 7, Align::Right),
                (settle_cell, 6, Align::Left),
                (realized_cell, 8, Align::Right),
            ])
        );
    }

    println!();
    println!(
        "  {}盘口{} {} 个   {}双边(已锁利){} {} / {}单边(方向暴露){} {}   {}总成交{} {}",
        C_BOLD,
        C_RESET,
        order.len(),
        C_GREEN,
        C_RESET,
        locked,
        C_YELLOW,
        C_RESET,
        directional,
        C_BOLD,
        C_RESET,
        total_fills
    );
    let edge_styled = if sum_locked_edge >= 0.0 {
        format!("{C_GREEN}{sum_locked_edge:+.2}{C_RESET}")
    } else {
        format!("{C_RED}{sum_locked_edge:+.2}{C_RESET}")
    };
    let worst_styled = if sum_worst >= 0.0 {
        format!("{C_GREEN}{sum_worst:+.2}{C_RESET}")
    } else {
        format!("{C_RED}{sum_worst:+.2}{C_RESET}")
    };
    println!(
        "  {}锁定毛利合计{} ≈ {}   {}最坏情景PnL合计{} {}",
        C_BOLD, C_RESET, edge_styled, C_BOLD, C_RESET, worst_styled
    );
    if official_corrected > 0 {
        println!(
            "  {}官方成交对账{} 修正 {} 盘，份额差合计约 {:.1}",
            C_CYAN, C_RESET, official_corrected, official_share_delta
        );
    }
    let realized_styled = if sum_realized >= 0.0 {
        format!("{C_GREEN}{sum_realized:+.2}{C_RESET}")
    } else {
        format!("{C_RED}{sum_realized:+.2}{C_RESET}")
    };
    let win_rate = if wins + losses > 0 {
        format!("{:.0}%", 100.0 * wins as f64 / (wins + losses) as f64)
    } else {
        "—".to_string()
    };
    println!(
        "  {}已结算{} {} 盘 (Up赢 {} / Down赢 {})   {}盈/亏{} {}/{} 胜率 {}   {}实现PnL合计{} {}",
        C_BOLD,
        C_RESET,
        settled,
        up_wins,
        dn_wins,
        C_BOLD,
        C_RESET,
        wins,
        losses,
        win_rate,
        C_BOLD,
        C_RESET,
        realized_styled
    );
    println!();
    println!(
        "{}说明:盘口列显示北京时间窗口起止。成交列若显示 a/b，表示本地 fills.jsonl 记到 a \
         笔、Polymarket 官方 Activity 记到 b 笔，统计已按官方成交修正。结算列优先读取 \
         Polymarket Gamma 官方 resolved outcomePrices;官方结果暂不可用时才用本机 BTC 临近\
         收盘行情兜底推断(临界盘可能有偏差)。实现PnL=按该结算方向的逐盘已实现盈亏。\
         官方未 resolved 且本机缺少临近收盘行情的盘口显示 ?。{}",
        C_DIM, C_RESET
    );
    Ok(())
}

fn fetch_official_settlements(
    cfg: &Config,
    markets: &[String],
) -> std::collections::HashMap<String, bool> {
    let mut out = std::collections::HashMap::new();
    for market in markets {
        if let Some(won_up) = fetch_official_settlement(cfg, market) {
            out.insert(market.clone(), won_up);
        }
    }
    out
}

fn fetch_official_settlement(cfg: &Config, slug: &str) -> Option<bool> {
    let base = cfg.gamma_api_url.trim_end_matches('/');
    let url = format!("{base}/events/slug/{slug}");
    let value: Value = ureq::get(&url)
        .set("User-Agent", "polymaker/0.1")
        .timeout(Duration::from_secs(3))
        .call()
        .ok()?
        .into_json()
        .ok()?;
    let market = value
        .get("markets")
        .and_then(Value::as_array)
        .and_then(|items| items.first())?;
    let closed = market
        .get("closed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let resolved = market
        .get("umaResolutionStatus")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("resolved"));
    if !closed && !resolved {
        return None;
    }
    let outcomes = parse_json_string_array(market.get("outcomes"))?;
    let prices = parse_json_string_array(market.get("outcomePrices"))?;
    if outcomes.len() != prices.len() {
        return None;
    }
    outcomes
        .iter()
        .zip(prices.iter())
        .filter_map(|(outcome, price)| price.parse::<f64>().ok().map(|p| (outcome, p)))
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .and_then(|(outcome, price)| {
            if price < 0.5 {
                None
            } else {
                match outcome.trim().to_ascii_lowercase().as_str() {
                    "up" => Some(true),
                    "down" => Some(false),
                    _ => None,
                }
            }
        })
}

fn parse_json_string_array(value: Option<&Value>) -> Option<Vec<String>> {
    match value? {
        Value::Array(items) => Some(
            items
                .iter()
                .filter_map(|item| item.as_str().map(ToString::to_string))
                .collect(),
        ),
        Value::String(raw) => serde_json::from_str::<Vec<String>>(raw).ok(),
        _ => None,
    }
}

#[derive(Clone)]
struct ModelMarketSample {
    ts_ms: u64,
    market: String,
    tau_seconds: f64,
    model_up: f64,
    market_up: f64,
    final_up_shadow: f64,
    anchor_weight: f64,
    exact_book: bool,
}

pub fn print_model_market_stats(cfg: &Config) -> AppResult<()> {
    print_banner("模型/盘口胜率对比");
    let mut samples = Vec::new();
    for_each_jsonl::<MarketFrame, _>(&cfg.book_path(), |fr| {
        if let Some(sample) = model_market_sample(cfg, &fr) {
            samples.push(sample);
        }
    })?;
    if samples.is_empty() {
        println!(
            "{}暂无可用行情样本 ({})。先运行 live collector 生成 book.jsonl。{}",
            C_DIM,
            cfg.book_path().display(),
            C_RESET
        );
        return Ok(());
    }

    let exact = samples
        .iter()
        .filter(|sample| sample.exact_book)
        .cloned()
        .collect::<Vec<_>>();
    let approx = samples
        .iter()
        .filter(|sample| !sample.exact_book)
        .cloned()
        .collect::<Vec<_>>();
    if !exact.is_empty() {
        print_model_market_summary("精确盘口(best bid/ask)", &exact);
    }
    if !approx.is_empty() {
        print_model_market_summary("旧日志近似(ask推算)", &approx);
    }

    let primary = if exact.is_empty() { &samples } else { &exact };
    println!();
    println!(
        "{}按剩余时间分桶{}  {}",
        C_BOLD,
        C_RESET,
        if exact.is_empty() {
            "(当前没有bid字段, 使用旧日志近似)"
        } else {
            "(使用真实best bid/ask)"
        }
    );
    println!(
        "{}",
        table_row(&[
            ("剩余秒".to_string(), 10, Align::Left),
            ("样本".to_string(), 6, Align::Right),
            ("模型Up".to_string(), 8, Align::Right),
            ("盘口Up".to_string(), 8, Align::Right),
            ("FinalUp".to_string(), 8, Align::Right),
            ("M差".to_string(), 8, Align::Right),
            ("F差".to_string(), 8, Align::Right),
        ])
    );
    for (lo, hi) in [
        (240.0, 300.0),
        (180.0, 240.0),
        (120.0, 180.0),
        (60.0, 120.0),
        (30.0, 60.0),
        (10.0, 30.0),
        (0.0, 10.0),
    ] {
        let bucket = primary
            .iter()
            .filter(|sample| sample.tau_seconds >= lo && sample.tau_seconds < hi)
            .cloned()
            .collect::<Vec<_>>();
        if bucket.is_empty() {
            continue;
        }
        let (model, market, final_up, model_diff, final_diff, _) = model_market_means(&bucket);
        println!(
            "{}",
            table_row(&[
                (format!("{lo:.0}-{hi:.0}"), 10, Align::Left),
                (bucket.len().to_string(), 6, Align::Right),
                (format!("{model:.3}"), 8, Align::Right),
                (format!("{market:.3}"), 8, Align::Right),
                (format!("{final_up:.3}"), 8, Align::Right),
                (format!("{model_diff:+.3}"), 8, Align::Right),
                (format!("{final_diff:+.3}"), 8, Align::Right),
            ])
        );
    }

    println!();
    println!("{}最近样本{}", C_BOLD, C_RESET);
    println!(
        "{}",
        table_row(&[
            ("时间".to_string(), 8, Align::Left),
            ("剩余".to_string(), 7, Align::Right),
            ("模型Up".to_string(), 8, Align::Right),
            ("盘口Up".to_string(), 8, Align::Right),
            ("FinalUp".to_string(), 8, Align::Right),
            ("F差".to_string(), 8, Align::Right),
            ("权重".to_string(), 6, Align::Right),
            ("盘口".to_string(), 6, Align::Left),
            ("市场".to_string(), 24, Align::Left),
        ])
    );
    for sample in primary
        .iter()
        .rev()
        .take(12)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
        println!(
            "{}",
            table_row(&[
                (fmt_ts(sample.ts_ms), 8, Align::Left),
                (format!("{:.0}s", sample.tau_seconds), 7, Align::Right),
                (format!("{:.3}", sample.model_up), 8, Align::Right),
                (format!("{:.3}", sample.market_up), 8, Align::Right),
                (format!("{:.3}", sample.final_up_shadow), 8, Align::Right),
                (
                    format!("{:+.3}", sample.final_up_shadow - sample.market_up),
                    8,
                    Align::Right,
                ),
                (format!("{:.2}", sample.anchor_weight), 6, Align::Right),
                (
                    if sample.exact_book {
                        "bidask"
                    } else {
                        "approx"
                    }
                    .to_string(),
                    6,
                    Align::Left,
                ),
                (sample.market.clone(), 24, Align::Left),
            ])
        );
    }
    println!();
    println!(
        "{}说明: 模型Up=本bot用BTC价格/开盘价/波动率算出的Up概率; 盘口Up=Polymarket盘口隐含Up概率; \
         FinalUp=按 MARKET_ANCHOR_WEIGHT 影子融合后的概率。新日志用best bid/ask mid; \
         老日志缺bid时用 UpAsk 和 1-DownAsk 做近似。M差=模型Up-盘口Up; F差=FinalUp-盘口Up。{}",
        C_DIM, C_RESET
    );
    Ok(())
}

#[derive(Clone, Default)]
struct DirectionBucket {
    n: u64,
    prob_sum: f64,
    outcome_sum: f64,
    brier_sum: f64,
}

impl DirectionBucket {
    fn add(&mut self, prob: f64, up_won: bool) {
        let outcome = if up_won { 1.0 } else { 0.0 };
        self.n += 1;
        self.prob_sum += prob;
        self.outcome_sum += outcome;
        self.brier_sum += (prob - outcome) * (prob - outcome);
    }

    fn avg_prob(&self) -> f64 {
        self.prob_sum / self.n.max(1) as f64
    }

    fn hit_rate(&self) -> f64 {
        self.outcome_sum / self.n.max(1) as f64
    }

    fn brier(&self) -> f64 {
        self.brier_sum / self.n.max(1) as f64
    }
}

#[derive(Clone)]
struct DirectionEval {
    label: &'static str,
    total: DirectionBucket,
    buckets: Vec<DirectionBucket>,
}

impl DirectionEval {
    fn new(label: &'static str) -> Self {
        Self {
            label,
            total: DirectionBucket::default(),
            buckets: vec![DirectionBucket::default(); 20],
        }
    }

    fn add(&mut self, prob: f64, up_won: bool) {
        let prob = prob.clamp(0.0001, 0.9999);
        self.total.add(prob, up_won);
        let idx = ((prob * 20.0).floor() as usize).min(19);
        self.buckets[idx].add(prob, up_won);
    }
}

pub fn print_direction_stats(cfg: &Config) -> AppResult<()> {
    print_banner("方向信号校准统计");
    let mut last_by_market: HashMap<String, MarketFrame> = HashMap::new();
    let now = now_ms();
    for_each_jsonl::<MarketFrame, _>(&cfg.book_path(), |fr| {
        if fr.btc_price > 0.0
            && fr.price_to_beat > 0.0
            && is_settlement_frame(&fr, cfg.market_window_secs, now)
        {
            last_by_market.insert(fr.market.clone(), fr);
        }
    })?;

    let outcomes = last_by_market
        .into_iter()
        .filter_map(|(market, fr)| {
            if fr.btc_price > fr.price_to_beat {
                Some((market, true))
            } else if fr.btc_price < fr.price_to_beat {
                Some((market, false))
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>();

    if outcomes.is_empty() {
        println!(
            "{}暂无可用结算样本 ({})。{}",
            C_DIM,
            cfg.book_path().display(),
            C_RESET
        );
        return Ok(());
    }

    let mut raw_eval = DirectionEval::new("raw_model_up");
    let mut calibrated_eval = DirectionEval::new("calibrated_model_up");
    let mut shadow_eval = DirectionEval::new("final_direction_up_shadow");
    let mut shadow_rows = 0u64;
    let mut raw_rows = 0u64;

    for_each_jsonl::<MarketFrame, _>(&cfg.book_path(), |fr| {
        let Some(up_won) = outcomes.get(&fr.market).copied() else {
            return;
        };
        let raw = if fr.raw_model_up > 0.0 {
            fr.raw_model_up
        } else if fr.btc_price > 0.0 && fr.price_to_beat > 0.0 {
            let vol = if fr.vol_per_sqrt_sec > 0.0 {
                fr.vol_per_sqrt_sec
            } else {
                cfg.vol_seed_per_sqrt_sec
            };
            let width = uncertainty_width(vol, fr.tau_seconds, cfg.width_floor_usd);
            digital_p_up(fr.btc_price, fr.price_to_beat, width)
        } else {
            return;
        };
        raw_eval.add(raw, up_won);
        raw_rows += 1;

        if fr.calibrated_model_up > 0.0 {
            calibrated_eval.add(fr.calibrated_model_up, up_won);
        }
        if fr.final_direction_up_shadow > 0.0 {
            shadow_eval.add(fr.final_direction_up_shadow, up_won);
            shadow_rows += 1;
        }
    })?;

    println!(
        "markets={} raw_samples={} shadow_samples={}",
        outcomes.len(),
        raw_rows,
        shadow_rows
    );
    print_direction_eval_summary(&raw_eval);
    if calibrated_eval.total.n > 0 {
        print_direction_eval_summary(&calibrated_eval);
    }
    if shadow_eval.total.n > 0 {
        print_direction_eval_summary(&shadow_eval);
    } else {
        println!(
            "{}暂无 final_direction_up_shadow。部署方向 v1 后再跑本命令可比较 shadow。{}",
            C_DIM, C_RESET
        );
    }

    if shadow_eval.total.n > 0 {
        println!();
        print_direction_bucket_table(&shadow_eval);
    } else {
        println!();
        print_direction_bucket_table(&raw_eval);
    }
    Ok(())
}

fn print_direction_eval_summary(eval: &DirectionEval) {
    println!(
        "{}{}{} samples={} avg_prob={:.3} actual_up={:.3} brier={:.5}",
        C_BOLD,
        eval.label,
        C_RESET,
        eval.total.n,
        eval.total.avg_prob(),
        eval.total.hit_rate(),
        eval.total.brier()
    );
}

fn print_direction_bucket_table(eval: &DirectionEval) {
    println!("{}{} 概率分桶{}", C_BOLD, eval.label, C_RESET);
    println!(
        "{}",
        table_row(&[
            ("bucket".to_string(), 11, Align::Left),
            ("samples".to_string(), 8, Align::Right),
            ("avg_p".to_string(), 8, Align::Right),
            ("hit".to_string(), 8, Align::Right),
            ("brier".to_string(), 9, Align::Right),
        ])
    );
    for (idx, bucket) in eval.buckets.iter().enumerate() {
        if bucket.n == 0 {
            continue;
        }
        let lo = idx as f64 / 20.0;
        let hi = (idx + 1) as f64 / 20.0;
        println!(
            "{}",
            table_row(&[
                (format!("{lo:.2}-{hi:.2}"), 11, Align::Left),
                (bucket.n.to_string(), 8, Align::Right),
                (format!("{:.3}", bucket.avg_prob()), 8, Align::Right),
                (format!("{:.3}", bucket.hit_rate()), 8, Align::Right),
                (format!("{:.5}", bucket.brier()), 9, Align::Right),
            ])
        );
    }
}

fn print_model_market_summary(label: &str, samples: &[ModelMarketSample]) {
    let (model, market, final_up, model_diff, final_diff, final_abs_diff) =
        model_market_means(samples);
    println!(
        "{}{}{} 样本 {}  模型Up {:.3}  盘口Up {:.3}  FinalUp {:.3}  M差 {:+.3}  F差 {:+.3}  Final绝对差 {:.3}",
        C_BOLD,
        label,
        C_RESET,
        samples.len(),
        model,
        market,
        final_up,
        model_diff,
        final_diff,
        final_abs_diff
    );
}

fn model_market_means(samples: &[ModelMarketSample]) -> (f64, f64, f64, f64, f64, f64) {
    let n = samples.len().max(1) as f64;
    let model = samples.iter().map(|sample| sample.model_up).sum::<f64>() / n;
    let market = samples.iter().map(|sample| sample.market_up).sum::<f64>() / n;
    let final_up = samples
        .iter()
        .map(|sample| sample.final_up_shadow)
        .sum::<f64>()
        / n;
    let model_diff = samples
        .iter()
        .map(|sample| sample.model_up - sample.market_up)
        .sum::<f64>()
        / n;
    let final_diff = samples
        .iter()
        .map(|sample| sample.final_up_shadow - sample.market_up)
        .sum::<f64>()
        / n;
    let final_abs_diff = samples
        .iter()
        .map(|sample| (sample.final_up_shadow - sample.market_up).abs())
        .sum::<f64>()
        / n;
    (
        model,
        market,
        final_up,
        model_diff,
        final_diff,
        final_abs_diff,
    )
}

fn model_market_sample(cfg: &Config, frame: &MarketFrame) -> Option<ModelMarketSample> {
    if frame.btc_price <= 0.0 || frame.price_to_beat <= 0.0 {
        return None;
    }
    let vol = if frame.vol_per_sqrt_sec > 0.0 {
        frame.vol_per_sqrt_sec
    } else {
        cfg.vol_seed_per_sqrt_sec
    };
    let width = uncertainty_width(vol, frame.tau_seconds, cfg.width_floor_usd);
    let model_up = digital_p_up(frame.btc_price, frame.price_to_beat, width);
    let (market_up, exact_book, book_spread) = market_up_mid(frame)?;
    let anchor_enabled = cfg.enable_market_anchor || cfg.market_anchor_shadow;
    let (final_up_shadow, anchor_weight) = if anchor_enabled {
        asymmetric_anchor_shadow_up(cfg, model_up, market_up, book_spread)
    } else {
        (model_up, 0.0)
    };
    Some(ModelMarketSample {
        ts_ms: frame.ts_ms,
        market: frame.market.clone(),
        tau_seconds: frame.tau_seconds,
        model_up,
        market_up,
        final_up_shadow,
        anchor_weight,
        exact_book,
    })
}

fn asymmetric_anchor_shadow_up(
    cfg: &Config,
    model_up: f64,
    market_up: f64,
    book_spread: f64,
) -> (f64, f64) {
    let model_down = 1.0 - model_up;
    let market_down = 1.0 - market_up;
    let up_weight = side_market_anchor_weight(cfg, model_up, book_spread);
    let down_weight = side_market_anchor_weight(cfg, model_down, book_spread);
    let up_shadow = blend_market_anchor(model_up, market_up, up_weight);
    let down_shadow = blend_market_anchor(model_down, market_down, down_weight);
    (
        ((up_shadow + (1.0 - down_shadow)) / 2.0).clamp(0.0, 1.0),
        (up_weight + down_weight) / 2.0,
    )
}

fn side_market_anchor_weight(cfg: &Config, side_model_fair: f64, book_spread: f64) -> f64 {
    let max_weight = if side_model_fair < cfg.market_anchor_low_side_below {
        cfg.market_anchor_weight_low
    } else {
        cfg.market_anchor_weight_high
    };
    market_anchor_weight(max_weight, book_spread, cfg.market_anchor_max_spread)
}

fn market_up_mid(frame: &MarketFrame) -> Option<(f64, bool, f64)> {
    let up_exact = valid_market_px(frame.up_bid)
        .then_some(frame.up_bid)
        .zip(valid_market_px(frame.up_ask).then_some(frame.up_ask))
        .filter(|(bid, ask)| ask >= bid)
        .map(|(bid, ask)| ((bid + ask) / 2.0, ask - bid));
    let down_exact = valid_market_px(frame.down_bid)
        .then_some(frame.down_bid)
        .zip(valid_market_px(frame.down_ask).then_some(frame.down_ask))
        .filter(|(bid, ask)| ask >= bid)
        .map(|(bid, ask)| (1.0 - ((bid + ask) / 2.0), ask - bid));
    match (up_exact, down_exact) {
        (Some((up, up_spread)), Some((down, down_spread))) => Some((
            ((up + down) / 2.0).clamp(0.0, 1.0),
            true,
            up_spread.max(down_spread),
        )),
        (Some((up, up_spread)), None) => Some((up.clamp(0.0, 1.0), true, up_spread)),
        (None, Some((down, down_spread))) => Some((down.clamp(0.0, 1.0), true, down_spread)),
        (None, None) => {
            if valid_market_px(frame.up_ask) && valid_market_px(frame.down_ask) {
                Some((
                    ((frame.up_ask + (1.0 - frame.down_ask)) / 2.0).clamp(0.0, 1.0),
                    false,
                    (frame.up_ask + frame.down_ask).clamp(0.0, 1.0),
                ))
            } else {
                None
            }
        }
    }
}

fn valid_market_px(px: f64) -> bool {
    px.is_finite() && px > 0.0 && px <= 1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(market: &str, ts_ms: u64, tau_seconds: f64) -> MarketFrame {
        MarketFrame {
            ts_ms,
            market: market.to_string(),
            condition_id: String::new(),
            up_token_id: String::new(),
            down_token_id: String::new(),
            up_bid: 0.0,
            up_ask: 0.0,
            down_bid: 0.0,
            down_ask: 0.0,
            btc_price: 100.0,
            price_to_beat: 99.0,
            tau_seconds,
            vol_per_sqrt_sec: 0.0,
            source: String::new(),
            ..Default::default()
        }
    }

    #[test]
    fn active_slugged_market_is_not_settled() {
        let fr = frame("btc-updown-5m-1800000000", 1_800_000_285_000, 15.0);
        assert!(!is_settlement_frame(&fr, 300, 1_800_000_285_000));
    }

    #[test]
    fn near_close_frame_after_window_end_can_settle() {
        let fr = frame("btc-updown-5m-1800000000", 1_800_000_299_300, 0.7);
        assert!(is_settlement_frame(&fr, 300, 1_800_000_301_000));
    }
}

fn read_dashboard_json<T: DeserializeOwned>(path: &Path) -> Option<T> {
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str::<T>(&raw).ok()
}

fn ensure_cli_defaults(path: &Path) -> AppResult<()> {
    upsert_env_if_missing(path, "DRY_RUN", "1")?;
    upsert_env_if_missing(path, "ENABLE_REAL_ORDERS", "")?;
    upsert_env_if_missing(path, "BOT_RUN_DIR", "run")?;
    upsert_env_if_missing(path, "ENV_SWITCH_ENABLED", "0")?;
    upsert_env_if_missing(
        path,
        "ENV_SWITCH_SCHEDULE",
        "08:00-20:00=.env.day;20:00-08:00=.env.night",
    )?;
    upsert_env_if_missing(path, "ENV_SWITCH_CHECK_SECS", "30")?;
    upsert_env_if_missing(path, "ENV_SWITCH_TZ_OFFSET_MINUTES", "480")?;
    upsert_env_if_missing(path, "ENV_SWITCH_RESTART_MODE", "stop")?;
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
    upsert_env_if_blank(path, "POLY_SIGNATURE_TYPE", "poly1271")?;
    upsert_env_if_missing(path, "POLY_FUNDER_ADDRESS", "")?;
    upsert_env_if_missing(path, "PRICE_TO_BEAT", "68000")?;
    upsert_env_if_missing(path, "QUOTE_SIZE", "5")?;
    upsert_env_if_missing(path, "QUOTE_SPREAD", "0.04")?;
    upsert_env_if_missing(path, "QUOTE_TTL_MS", "1500")?;
    upsert_env_if_missing(path, "REQUOTE_THRESHOLD_TICKS", "1")?;
    upsert_env_if_missing(path, "INVENTORY_SKEW", "0.03")?;
    upsert_env_if_missing(path, "INVENTORY_MULT", "2")?;
    upsert_env_if_missing(path, "MAX_UNPAIRED_SHARES", "19")?;
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
    // ── strategy brain v3 ──────────────────────────────────────────────
    upsert_env_if_missing(path, "VOL_SEED_PER_SQRT_SEC", "6")?;
    upsert_env_if_missing(path, "VOL_HALFLIFE_SEC", "20")?;
    upsert_env_if_missing(path, "VOL_MIN_PER_SQRT_SEC", "1.5")?;
    upsert_env_if_missing(path, "VOL_MAX_PER_SQRT_SEC", "60")?;
    upsert_env_if_missing(path, "WIDTH_FLOOR_USD", "3")?;
    upsert_env_if_missing(path, "BASE_HALF_SPREAD", "0.012")?;
    upsert_env_if_missing(path, "MIN_LOCK_EDGE", "0.02")?;
    upsert_env_if_missing(path, "LATENCY_SEC", "0.4")?;
    upsert_env_if_missing(path, "K_ADVERSE", "1")?;
    upsert_env_if_missing(path, "MIN_HALF_SPREAD", "0.005")?;
    upsert_env_if_missing(path, "MAX_HALF_SPREAD", "0.25")?;
    upsert_env_if_missing(path, "ENDGAME_PULL_SECS", "12")?;
    upsert_env_if_missing(path, "INVENTORY_SKEW_TIME_BOOST", "2")?;
    upsert_env_if_missing(path, "TOX_HORIZON_MS", "2500")?;
    upsert_env_if_missing(path, "TOX_DECAY", "0.5")?;
    upsert_env_if_missing(path, "TOX_K_WIDEN", "1.5")?;
    upsert_env_if_missing(path, "TOX_MAX_WIDEN", "0.08")?;
    upsert_env_if_missing(path, "ENABLE_DELTA_HEDGE", "0")?;
    upsert_env_if_missing(path, "HEDGE_VENUE", "binance_perp")?;
    upsert_env_if_missing(path, "RECONCILE_INTERVAL_MS", "30000")?;
    upsert_env_if_missing(path, "ENABLE_PREWARM", "1")?;
    upsert_env_if_missing(path, "PREWARM_INTERVAL_MS", "60000")?;
    upsert_env_if_missing(path, "POST_ONLY_MARGIN_TICKS", "2")?;
    upsert_env_if_missing(path, "REJECT_BACKOFF_MS", "500")?;
    upsert_env_if_missing(path, "QUOTE_WARMUP_SECS", "25")?;
    upsert_env_if_missing_with_comment(
        path,
        "VALUE_MIN_EDGE",
        "0.03",
        "最低安全垫：买价必须 <= 模型fair - 该值。0.03=至少3美分edge。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "VALUE_AGGRESSION_TICKS",
        "1",
        "相对当前买一价抬高多少tick；仍受post-only和VALUE_MIN_EDGE限制。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "VALUE_MIN_FAIR",
        "0.05",
        "最低模型胜率过滤：低于这个fair的一边不挂单。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "ENABLE_DIRECTION_EDGE",
        "0",
        "1=按方向强弱调整edge；不禁止任何一边，只改变成交难度。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "DIRECTION_ALIGNED_EDGE",
        "0.012",
        "顺方向最低安全垫；小于VALUE_MIN_EDGE时顺方向更容易成交。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "DIRECTION_COUNTER_EDGE",
        "0.04",
        "反方向最低安全垫；大于VALUE_MIN_EDGE时反方向必须更便宜。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "ENABLE_MARKET_ANCHOR",
        "0",
        "1=真实报价使用盘口锚定融合胜率；默认0只记录影子值。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "MARKET_ANCHOR_SHADOW",
        "1",
        "1=记录FinalUp影子胜率，不改变报价。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "MARKET_ANCHOR_WEIGHT",
        "0.30",
        "盘口胜率兼容默认权重；HIGH/LOW未设置时使用该值。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "MARKET_ANCHOR_WEIGHT_HIGH",
        "0.30",
        "模型高胜率边盘口融合权重；低一些可保留模型优势。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "MARKET_ANCHOR_WEIGHT_LOW",
        "0.60",
        "模型低胜率对边盘口融合权重；高一些可避免低边挂价离谱。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "MARKET_ANCHOR_LOW_SIDE_BELOW",
        "0.50",
        "模型胜率低于该值的一边按 MARKET_ANCHOR_WEIGHT_LOW 融合。",
    )?;
    upsert_env_if_missing_with_comment(
        path,
        "MARKET_ANCHOR_MAX_SPREAD",
        "0.12",
        "盘口健康价差上限；超过该价差则盘口锚定权重降为0。",
    )?;
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

fn upsert_env_if_missing_with_comment(
    path: &Path,
    key: &str,
    value: &str,
    comment: &str,
) -> AppResult<()> {
    if env_value(path, key).is_none() {
        append_env_with_comment(path, key, value, comment)?;
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
    secure_env_permissions(path)?;
    Ok(())
}

fn append_env_with_comment(path: &Path, key: &str, value: &str, comment: &str) -> AppResult<()> {
    let raw = fs::read_to_string(path).unwrap_or_default();
    let mut lines = raw.lines().map(str::to_string).collect::<Vec<_>>();
    if !lines.is_empty() && lines.last().is_some_and(|line| !line.trim().is_empty()) {
        lines.push(String::new());
    }
    lines.push(format!("# {comment}"));
    lines.push(format!("{key}={value}"));
    fs::write(path, lines.join("\n") + "\n")?;
    secure_env_permissions(path)?;
    Ok(())
}

fn secure_env_permissions(path: &Path) -> AppResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(path, perms)?;
    }
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

fn format_market_px(px: f64) -> String {
    if valid_market_px(px) {
        format!("{px:.3}")
    } else {
        "-".to_string()
    }
}

fn format_age(age_ms: u64) -> String {
    if age_ms < 1_000 {
        format!("{age_ms}ms")
    } else {
        format!("{:.1}s", age_ms as f64 / 1000.0)
    }
}
