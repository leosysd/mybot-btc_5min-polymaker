use crate::config::Config;
use crate::workers;
use crate::AppResult;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
struct EnvWindow {
    start_minute: u16,
    end_minute: u16,
    env_path: String,
}

pub fn run(mut cfg: Config, once: bool) -> AppResult<()> {
    loop {
        if !cfg.env_switch_enabled {
            println!(
                "[env-switcher] disabled; set ENV_SWITCH_ENABLED=1 to enable automatic .env switching"
            );
            if once {
                return Ok(());
            }
            thread::sleep(Duration::from_secs(cfg.env_switch_check_secs.max(30)));
            if let Ok(fresh) = Config::from_env() {
                cfg = fresh;
            }
            continue;
        }

        if let Err(err) = apply_current_env(&cfg) {
            eprintln!("[env-switcher] {err}");
            if once {
                return Err(err);
            }
        }

        if once {
            return Ok(());
        }
        thread::sleep(Duration::from_secs(cfg.env_switch_check_secs));
    }
}

fn apply_current_env(cfg: &Config) -> AppResult<()> {
    cfg.ensure_dirs()?;
    let schedule = parse_schedule(&cfg.env_switch_schedule)?;
    let minute = current_local_minute(cfg.env_switch_tz_offset_minutes)?;
    let Some(window) = schedule.iter().find(|window| window.contains(minute)) else {
        return Err(format!(
            "ENV_SWITCH_SCHEDULE has no matching window for local minute {minute}"
        )
        .into());
    };

    let source = resolve_env_path(cfg, &window.env_path);
    let active = cfg.env_file();
    if source == active {
        println!(
            "[env-switcher] target already active: {}",
            display_path(&active)
        );
        return Ok(());
    }

    let source_raw = fs::read_to_string(&source)
        .map_err(|err| format!("failed to read {}: {err}", display_path(&source)))?;
    let active_raw = fs::read_to_string(&active).unwrap_or_default();
    if normalize_newlines(&source_raw) == normalize_newlines(&active_raw) {
        println!(
            "[env-switcher] unchanged: {} is already applied",
            display_path(&source)
        );
        return Ok(());
    }

    fs::write(&active, source_raw)
        .map_err(|err| format!("failed to write {}: {err}", display_path(&active)))?;
    println!(
        "[env-switcher] applied {} -> {}",
        display_path(&source),
        display_path(&active)
    );

    match cfg.env_switch_restart_mode.as_str() {
        "none" => println!("[env-switcher] restart disabled by ENV_SWITCH_RESTART_MODE=none"),
        "stop" => {
            workers::write_stop(cfg)?;
            println!("[env-switcher] wrote STOP; PM2/supervisor should restart the bot");
        }
        other => return Err(format!("unsupported ENV_SWITCH_RESTART_MODE={other}").into()),
    }

    Ok(())
}

fn parse_schedule(raw: &str) -> AppResult<Vec<EnvWindow>> {
    let mut windows = Vec::new();
    for part in raw
        .split([';', ','])
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let Some((range, env_path)) = part.split_once('=') else {
            return Err(
                format!("invalid schedule part `{part}`; expected HH:MM-HH:MM=.env.file").into(),
            );
        };
        let Some((start, end)) = range.trim().split_once('-') else {
            return Err(format!("invalid time range `{range}`; expected HH:MM-HH:MM").into());
        };
        let env_path = env_path.trim();
        if env_path.is_empty() {
            return Err(format!("missing env file in schedule part `{part}`").into());
        }
        windows.push(EnvWindow {
            start_minute: parse_hhmm(start.trim())?,
            end_minute: parse_hhmm(end.trim())?,
            env_path: env_path.to_string(),
        });
    }
    if windows.is_empty() {
        return Err("ENV_SWITCH_SCHEDULE is empty".into());
    }
    Ok(windows)
}

fn parse_hhmm(raw: &str) -> AppResult<u16> {
    let Some((hh, mm)) = raw.split_once(':') else {
        return Err(format!("invalid time `{raw}`; expected HH:MM").into());
    };
    let hour = hh.parse::<u16>()?;
    let minute = mm.parse::<u16>()?;
    if hour == 24 && minute == 0 {
        return Ok(24 * 60);
    }
    if hour >= 24 || minute >= 60 {
        return Err(format!("invalid time `{raw}`").into());
    }
    Ok(hour * 60 + minute)
}

impl EnvWindow {
    fn contains(&self, minute: u16) -> bool {
        if self.start_minute == self.end_minute {
            return true;
        }
        if self.start_minute < self.end_minute {
            self.start_minute <= minute && minute < self.end_minute
        } else {
            self.start_minute <= minute || minute < self.end_minute
        }
    }
}

fn current_local_minute(offset_minutes: i64) -> AppResult<u16> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    let local = now + offset_minutes * 60;
    Ok((local.rem_euclid(24 * 60 * 60) / 60) as u16)
}

fn resolve_env_path(cfg: &Config, raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        cfg.base_dir.join(path)
    }
}

fn normalize_newlines(raw: &str) -> String {
    raw.replace("\r\n", "\n")
}

fn display_path(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_two_env_windows() {
        let windows = parse_schedule("08:00-20:00=.env.day;20:00-08:00=.env.night").unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].start_minute, 8 * 60);
        assert_eq!(windows[0].end_minute, 20 * 60);
        assert_eq!(windows[1].env_path, ".env.night");
    }

    #[test]
    fn matching_supports_midnight_wrap() {
        let window = EnvWindow {
            start_minute: 20 * 60,
            end_minute: 8 * 60,
            env_path: ".env.night".to_string(),
        };
        assert!(window.contains(23 * 60));
        assert!(window.contains(2 * 60));
        assert!(!window.contains(12 * 60));
    }

    #[test]
    fn accepts_24h_end_time() {
        assert_eq!(parse_hhmm("24:00").unwrap(), 24 * 60);
    }
}
