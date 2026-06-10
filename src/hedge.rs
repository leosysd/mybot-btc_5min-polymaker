//! Delta-hedge module — SCAFFOLD ONLY, disabled by default.
//!
//! The v3 "professional tier" neutralises the directional risk of *unmatched*
//! binary inventory by taking an offsetting BTC perpetual position. This file
//! contains the pure delta math (safe, tested) and a hedger interface, but the
//! actual exchange wiring is intentionally NOT implemented: turning it on for
//! real trading is blocked in `Config::ensure_real_order_config`.
//!
//! Why blocked: live hedging means a second exchange, your funds at a second
//! venue, and it must sit on top of a hardened order-management path. Validate
//! the strategy brain in DRY_RUN / live-data-sim first, fix the live-order
//! safety bugs, THEN implement a real `Hedger`.
//!
//! Roadmap to make it real:
//!   1. Implement `BinancePerpHedger` (REST/WS auth, place/reduce market or
//!      limit orders on BTCUSDT perp, track position & funding).
//!   2. On each fair-value tick, compute target hedge via `target_hedge_btc`
//!      and send the *delta* between target and current position.
//!   3. Add hedge PnL + funding to risk accounting and the dashboard.
//!   4. Only then allow `ENABLE_DELTA_HEDGE=1` in live mode.

use crate::config::Config;
use crate::pricing::price_sensitivity;
use crate::AppResult;

/// Net directional exposure (in "BTC up-delta units") of unmatched binary
/// inventory. Matched pairs are risk-free and contribute nothing.
///
/// Each Up share is worth `p_up`, with `d(p_up)/dS = sensitivity`. So holding
/// `n_up` Up shares has dollar-delta `n_up * sensitivity` w.r.t. BTC; Down
/// shares have the opposite sign (Down value = 1 - p_up). The hedge target is
/// the BTC notional whose delta cancels this.
pub fn unmatched_directional_delta(
    up_shares: f64,
    down_shares: f64,
    spot: f64,
    price_to_beat: f64,
    width_usd: f64,
) -> f64 {
    let sensitivity = price_sensitivity(spot, price_to_beat, width_usd);
    // d(portfolio_value)/dS, in units of "probability per $".
    (up_shares - down_shares) * sensitivity
}

/// BTC notional (in BTC) to SHORT to neutralise the unmatched delta. Positive
/// result => short that many BTC; negative => long. `spot` converts the
/// dollar-delta into BTC units. Returns 0 when spot is unusable.
pub fn target_hedge_btc(
    up_shares: f64,
    down_shares: f64,
    spot: f64,
    price_to_beat: f64,
    width_usd: f64,
) -> f64 {
    if !(spot.is_finite() && spot > 0.0) {
        return 0.0;
    }
    // Portfolio gains `delta` of value per $1 up-move; a short of `delta/spot`
    // BTC loses the same per $1 up-move, cancelling it.
    unmatched_directional_delta(up_shares, down_shares, spot, price_to_beat, width_usd) / spot
}

/// What a hedger must do. The no-op implementation is what ships today.
pub trait Hedger {
    /// Drive the venue position toward `target_btc` (signed: + = short).
    fn rebalance(&self, target_btc: f64) -> AppResult<()>;
    fn flatten(&self) -> AppResult<()>;
}

/// Disabled hedger: records intent via logs only, never touches an exchange.
pub struct NoopHedger;

impl Hedger for NoopHedger {
    fn rebalance(&self, _target_btc: f64) -> AppResult<()> {
        Ok(())
    }
    fn flatten(&self) -> AppResult<()> {
        Ok(())
    }
}

/// Factory. Always returns the no-op hedger for now; constructing a real
/// hedger is gated and unimplemented on purpose.
pub fn build_hedger(cfg: &Config) -> AppResult<Box<dyn Hedger + Send + Sync>> {
    if cfg.enable_delta_hedge {
        return Err(format!(
            "delta hedge requested (venue={}) but not implemented; \
             ENABLE_DELTA_HEDGE must stay 0 until BinancePerpHedger lands",
            cfg.hedge_venue
        )
        .into());
    }
    Ok(Box::new(NoopHedger))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pricing::uncertainty_width;

    #[test]
    fn long_up_inventory_wants_short_btc() {
        let w = uncertainty_width(6.0, 120.0, 3.0);
        let hedge = target_hedge_btc(10.0, 0.0, 100_000.0, 100_000.0, w);
        assert!(hedge > 0.0, "long Up should want a BTC short, got {hedge}");
    }

    #[test]
    fn matched_inventory_needs_no_hedge() {
        let w = uncertainty_width(6.0, 120.0, 3.0);
        let hedge = target_hedge_btc(7.0, 7.0, 100_000.0, 100_000.0, w);
        assert!(
            hedge.abs() < 1e-9,
            "matched book is delta-flat, got {hedge}"
        );
    }

    #[test]
    fn long_down_inventory_wants_long_btc() {
        let w = uncertainty_width(6.0, 120.0, 3.0);
        let hedge = target_hedge_btc(0.0, 10.0, 100_000.0, 100_000.0, w);
        assert!(hedge < 0.0, "long Down should want a BTC long, got {hedge}");
    }
}
