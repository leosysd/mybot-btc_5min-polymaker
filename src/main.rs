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
            eprintln!("unknown command: {other}");
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
                let value = args.get(i + 1).ok_or("--seconds needs a value")?;
                seconds = Some(value.parse::<u64>()?);
                i += 2;
            }
            other => return Err(format!("unknown supervisor arg: {other}").into()),
        }
    }
    Ok(seconds)
}

fn print_usage() {
    println!(
        "polymaker - multi-process BTC 5m binary market maker\n\
\n\
Usage:\n\
  polymaker supervisor [--seconds N]  start all worker processes\n\
  polymaker collector                 run simulated collector worker\n\
  polymaker quote-engine              run fair-value/quote worker\n\
  polymaker order-gateway             run dry-run order gateway\n\
  polymaker risk-ledger               run inventory/risk worker\n\
  polymaker stop                      ask all workers to stop\n\
  polymaker clean                     remove run directory\n"
    );
}
