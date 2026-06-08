mod config;
mod hedge;
mod ipc;
mod pricing;
mod real_orders;
mod ui;
mod workers;

use config::Config;
use std::error::Error;

pub type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> AppResult<()> {
    install_tls_crypto_provider();
    let cfg = Config::from_env()?;
    let mut args = std::env::args().skip(1);
    let Some(cmd) = args.next() else {
        return ui::run_menu(&cfg);
    };

    match cmd.as_str() {
        "supervisor" => {
            let seconds = parse_seconds(args.collect())?;
            workers::run_supervisor(cfg, seconds)
        }
        "start" => workers::start_background(&cfg),
        "collector" | "market-data" => workers::run_market_data(cfg),
        "quote-engine" | "engine" | "fair-value" => workers::run_fair_value(cfg),
        "order-gateway" | "gateway" | "maker" => workers::run_maker(cfg),
        "risk-ledger" | "risk" => workers::run_risk(cfg),
        "stop" => workers::write_stop(&cfg),
        "restart" => workers::restart_background(&cfg),
        "status" => ui::print_status(&cfg),
        "dashboard" | "trade" => {
            let seconds = parse_seconds(args.collect())?;
            ui::run_dashboard(&cfg, seconds)
        }
        "menu" | "m" => ui::run_menu(&cfg),
        "init" => ui::init_config(&cfg),
        "clean" => workers::clean_run_dir(&cfg),
        "help" | "-h" | "--help" => {
            print_usage();
            Ok(())
        }
        other => {
            eprintln!("未知命令: {other}");
            print_usage();
            std::process::exit(2);
        }
    }
}

fn install_tls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn parse_seconds(args: Vec<String>) -> AppResult<Option<u64>> {
    let mut seconds = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--seconds" => {
                let value = args.get(i + 1).ok_or("--seconds 需要一个秒数")?;
                seconds = Some(value.parse::<u64>()?);
                i += 2;
            }
            other => return Err(format!("未知 supervisor 参数: {other}").into()),
        }
    }
    Ok(seconds)
}

fn print_usage() {
    println!(
        "polymaker - 多进程 BTC 5 分钟二元市场做市机器人\n\
\n\
用法:\n\
  polymaker supervisor [--seconds N]  启动完整多进程；可选 N 秒后自动停止\n\
  polymaker start                     后台启动完整多进程\n\
  polymaker collector                 启动模拟行情采集进程\n\
  polymaker quote-engine              启动 fair value/报价进程\n\
  polymaker order-gateway             启动下单网关进程\n\
  polymaker risk-ledger               启动库存/风控账本进程\n\
  polymaker dashboard [--seconds N]   打开交易监控页\n\
  polymaker menu                      打开中文交互菜单\n\
  polymaker status                    查看进程心跳/库存状态\n\
  polymaker stop                      通知所有进程停止\n\
  polymaker restart                   重启后台服务\n\
  polymaker init                      初始化 .env 配置\n\
  polymaker clean                     删除 run 运行目录\n"
    );
}
