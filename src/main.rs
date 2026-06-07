mod config;
mod ipc;
mod pricing;
mod workers;

use config::Config;
use std::error::Error;

pub type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> AppResult<()> {
    let cfg = Config::from_env()?;
    let mut args = std::env::args().skip(1);
    let Some(cmd) = args.next() else {
        print_usage();
        return Ok(());
    };

    match cmd.as_str() {
        "supervisor" => {
            let seconds = parse_seconds(args.collect())?;
            workers::run_supervisor(cfg, seconds)
        }
        "collector" | "market-data" => workers::run_market_data(cfg),
        "quote-engine" | "engine" | "fair-value" => workers::run_fair_value(cfg),
        "order-gateway" | "gateway" | "maker" => workers::run_maker(cfg),
        "risk-ledger" | "risk" => workers::run_risk(cfg),
        "stop" => workers::write_stop(&cfg),
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
  polymaker collector                 启动模拟行情采集进程\n\
  polymaker quote-engine              启动 fair value/报价进程\n\
  polymaker order-gateway             启动 dry-run 下单网关进程\n\
  polymaker risk-ledger               启动库存/风控账本进程\n\
  polymaker stop                      通知所有进程停止\n\
  polymaker clean                     删除 run 运行目录\n"
    );
}
