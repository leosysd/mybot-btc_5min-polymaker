//! Strategy brain for the BTC 5-minute binary market maker.
//!
//! Everything here is pure, deterministic, and unit-tested. No I/O, no money.
//! The core idea: a 5-minute "will BTC be above P" market is a *digital option*.
//! The only variable that really matters is the **remaining uncertainty**
//! `W = sigma_$ * sqrt(tau)`,
//! where `sigma_$` is BTC dollar volatility per sqrt-second and `tau` is the
//! seconds left in the window. As `tau -> 0`, `W -> 0`, so the fair value snaps
//! to 0/1. A maker that ignores `W` (uses a constant) is systematically
//! mispriced and gets picked off near the close. This module makes `W` the
//! first-class citizen.

use std::collections::VecDeque;

#[derive(Debug, Clone, Copy)]
pub struct ModelQuote {
    pub up_bid: f64,
    pub down_bid: f64,
}

// ─────────────────────────────────────────────────────────────────────────
// 1. Fair value: time-aware digital pricing
// ─────────────────────────────────────────────────────────────────────────

/// Standard normal CDF (Abramowitz–Stegun 7.1.26).
pub fn normal_cdf(z: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.2316419 * z.abs());
    let poly = t
        * (0.319381530
            + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
    let pdf = normal_pdf(z);
    let cdf = 1.0 - pdf * poly;
    if z >= 0.0 {
        cdf
    } else {
        1.0 - cdf
    }
}

/// Standard normal PDF.
pub fn normal_pdf(z: f64) -> f64 {
    (-0.5 * z * z).exp() / (2.0 * std::f64::consts::PI).sqrt()
}

/// Remaining uncertainty `W = sigma_$ * sqrt(tau)`, floored so gamma stays finite
/// in the final seconds (avoids divide-by-zero and infinite quote sensitivity).
pub fn uncertainty_width(vol_per_sqrt_sec: f64, tau_sec: f64, floor_usd: f64) -> f64 {
    let vol = vol_per_sqrt_sec.max(0.0);
    let tau = tau_sec.max(0.0);
    (vol * tau.sqrt()).max(floor_usd.max(1e-6))
}

/// Fair probability that "Up" wins: P(BTC_close > P) = Phi((S - P) / W).
pub fn digital_p_up(spot: f64, price_to_beat: f64, width_usd: f64) -> f64 {
    if !spot.is_finite() || !price_to_beat.is_finite() || width_usd <= 0.0 {
        return 0.5;
    }
    let d = (spot - price_to_beat) / width_usd;
    normal_cdf(d).clamp(0.0001, 0.9999)
}

/// Sensitivity of `p_up` to a $1 move in BTC: d(p_up)/dS = phi(d) / W.
/// This is the "danger" metric — it explodes when at-the-money AND near close.
pub fn price_sensitivity(spot: f64, price_to_beat: f64, width_usd: f64) -> f64 {
    if width_usd <= 0.0 {
        return 0.0;
    }
    let d = (spot - price_to_beat) / width_usd;
    normal_pdf(d) / width_usd
}

// ─────────────────────────────────────────────────────────────────────────
// 2. Adaptive volatility (EWMA of per-second realized variance)
// ─────────────────────────────────────────────────────────────────────────

/// Online estimator of `sigma_$` (dollar stdev per sqrt-second) from an
/// irregularly-sampled BTC price stream. Uses a time-decayed EWMA of
/// `(dS)^2 / dt`, an unbiased one-sample estimate of variance-per-second.
#[derive(Debug, Clone)]
pub struct VolEstimator {
    var_per_sec: f64,
    decay_tau_sec: f64,
    min_vol: f64,
    max_vol: f64,
    last_price: Option<f64>,
    samples: u64,
}

impl VolEstimator {
    pub fn new(seed_per_sqrt_sec: f64, halflife_sec: f64, min_vol: f64, max_vol: f64) -> Self {
        let lo = min_vol.max(1e-6);
        let hi = max_vol.max(lo);
        let seed = seed_per_sqrt_sec.clamp(lo, hi);
        // halflife H satisfies 0.5 = exp(-H/tau) => tau = H / ln2.
        let decay_tau_sec = halflife_sec.max(1.0) / std::f64::consts::LN_2;
        Self {
            var_per_sec: seed * seed,
            decay_tau_sec,
            min_vol: lo,
            max_vol: hi,
            last_price: None,
            samples: 0,
        }
    }

    /// Feed a new BTC price observed `dt_sec` after the previous one.
    /// Returns the current `sigma_$` estimate.
    pub fn update(&mut self, price: f64, dt_sec: f64) -> f64 {
        if !price.is_finite() || price <= 0.0 {
            return self.current();
        }
        if let Some(prev) = self.last_price {
            let dt = dt_sec.clamp(0.05, 30.0);
            let ds = price - prev;
            let inst_var_per_sec = (ds * ds) / dt;
            let decay = (-dt / self.decay_tau_sec).exp();
            self.var_per_sec = decay * self.var_per_sec + (1.0 - decay) * inst_var_per_sec;
            self.samples = self.samples.saturating_add(1);
        }
        self.last_price = Some(price);
        self.current()
    }

    pub fn current(&self) -> f64 {
        self.var_per_sec
            .max(0.0)
            .sqrt()
            .clamp(self.min_vol, self.max_vol)
    }

    pub fn warm(&self) -> bool {
        self.samples >= 5
    }
}

// ─────────────────────────────────────────────────────────────────────────
// 3. Spread: base + adverse-selection premium (+ toxicity widening)
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct SpreadInputs {
    pub base_half_spread: f64,
    /// d(p_up)/dS = phi(d)/W — how violently fair value reacts to BTC moves.
    pub sensitivity: f64,
    pub vol_per_sqrt_sec: f64,
    /// Your cancel/reprice latency in seconds. Over this window you *cannot*
    /// react, so BTC drifts ~ vol*sqrt(latency) and your resting bid is exposed.
    pub latency_sec: f64,
    pub k_adverse: f64,
    /// Extra widening (in price units) requested by the toxicity monitor.
    pub toxicity_widen: f64,
    pub min_half_spread: f64,
    pub max_half_spread: f64,
}

/// Half-spread per side. The adverse term is the expected unavoidable
/// fair-value move during your latency window, mapped into price space:
///   adverse = k * sensitivity * (vol_$ * sqrt(latency))
/// Near the money near the close, `sensitivity` blows up, so this widens hard
/// exactly where you'd otherwise get run over.
pub fn half_spread(s: &SpreadInputs) -> f64 {
    let unavoidable_move_usd = s.vol_per_sqrt_sec.max(0.0) * s.latency_sec.max(0.0).sqrt();
    let adverse = s.k_adverse.max(0.0) * s.sensitivity.max(0.0) * unavoidable_move_usd;
    let raw = s.base_half_spread + adverse + s.toxicity_widen.max(0.0);
    let lo = s.min_half_spread.max(0.0);
    let hi = s.max_half_spread.max(lo);
    raw.clamp(lo, hi)
}

// ─────────────────────────────────────────────────────────────────────────
// 4. Quote construction: fair − half_spread, inventory skew, binary edge
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct MmParams {
    pub p_up: f64,
    pub half_spread: f64,
    /// Effective skew strength (already time-boosted by the caller).
    pub inventory_skew: f64,
    pub up_inventory: f64,
    pub down_inventory: f64,
    pub max_side_inventory: f64,
    pub min_bid: f64,
    pub max_bid: f64,
    /// Required guaranteed edge when both sides fill: up_bid + down_bid <= 1 - min_lock_edge.
    pub min_lock_edge: f64,
}

pub fn market_maker_bids(p: &MmParams) -> ModelQuote {
    let lock = p.min_lock_edge.clamp(0.0, 0.95);
    let cap_sum = (1.0 - lock).clamp(0.02, 0.999);
    let min_bid = p.min_bid.clamp(0.01, cap_sum / 2.0);
    let max_bid = p.max_bid.clamp(min_bid, 0.99);
    let fair_up = p.p_up.clamp(0.0001, 0.9999);
    let fair_down = 1.0 - fair_up;
    let half = p.half_spread.clamp(0.0, 0.45);

    // Inventory imbalance in [-1, 1]; skew pushes price down on the long side
    // and up on the short side, so quotes pull the book back toward neutral.
    let imbalance = if p.max_side_inventory > 0.0 {
        ((p.up_inventory - p.down_inventory) / p.max_side_inventory).clamp(-1.0, 1.0)
    } else {
        0.0
    };
    let skew = imbalance * p.inventory_skew.clamp(0.0, 0.45);

    let mut up_bid = (fair_up - half - skew).clamp(min_bid, max_bid);
    let mut down_bid = (fair_down - half + skew).clamp(min_bid, max_bid);
    enforce_binary_edge(&mut up_bid, &mut down_bid, cap_sum, min_bid);
    ModelQuote { up_bid, down_bid }
}

/// Post-only buy: never cross the spread. Sits at least `margin_ticks` below
/// the best ask so a small ask move between quoting and order arrival doesn't
/// turn it into a crossing (rejected) order. Floors to tick. Returns None if
/// there is no valid passive price.
pub fn post_only_bid(
    model_bid: f64,
    best_ask: f64,
    tick_size: f64,
    min_bid: f64,
    max_bid: f64,
    margin_ticks: f64,
) -> Option<f64> {
    if !model_bid.is_finite() || !best_ask.is_finite() || best_ask <= tick_size {
        return None;
    }
    let margin = margin_ticks.max(1.0);
    let post_only_cap = best_ask - margin * tick_size;
    let raw = model_bid.min(post_only_cap).min(max_bid).clamp(0.01, 0.99);
    let px = floor_to_tick(raw, tick_size);
    if px + 1e-12 < min_bid || px >= best_ask - 1e-12 {
        None
    } else {
        Some(px)
    }
}

pub fn floor_to_tick(px: f64, tick_size: f64) -> f64 {
    if tick_size <= 0.0 {
        return px;
    }
    ((px / tick_size).floor() * tick_size).clamp(0.01, 0.99)
}

fn enforce_binary_edge(up_bid: &mut f64, down_bid: &mut f64, cap_sum: f64, min_bid: f64) {
    let excess = (*up_bid + *down_bid - cap_sum).max(0.0);
    if excess <= 1e-12 {
        return;
    }
    let up_room = (*up_bid - min_bid).max(0.0);
    let down_room = (*down_bid - min_bid).max(0.0);
    let total_room = up_room + down_room;
    if total_room <= 1e-12 {
        return;
    }
    *up_bid -= excess * up_room / total_room;
    *down_bid -= excess * down_room / total_room;
}

/// Opening quiet period: true while the window is younger than `warmup_secs`.
/// Right after the open the fair value is ~0.5 (pure noise), the book is wide
/// and the vol estimator is cold, so fresh quotes are informed-flow bait:
/// the only people hitting a 4-second-old market are the ones who already saw
/// BTC move. During warmup we quote nothing at all.
pub fn in_warmup(tau_sec: f64, window_sec: f64, warmup_secs: f64) -> bool {
    if warmup_secs <= 0.0 || window_sec <= 0.0 {
        return false;
    }
    (window_sec - tau_sec) < warmup_secs
}

// ─────────────────────────────────────────────────────────────────────────
// 5. Endgame protocol + inventory-skew time boost
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Plenty of time: value-buy quotes may be sent.
    Normal,
    /// Final seconds: pull everything. Pure coin-flip / max gamma zone.
    Pull,
}

pub fn phase_for(tau_sec: f64, pull_secs: f64) -> Phase {
    if tau_sec <= pull_secs {
        Phase::Pull
    } else {
        Phase::Normal
    }
}

/// Inventory-skew strength ramps up as the window closes: being caught with
/// unmatched inventory late is far costlier (no time to unwind), so skew harder.
pub fn time_boosted_skew(base_skew: f64, tau_sec: f64, window_sec: f64, max_boost: f64) -> f64 {
    if window_sec <= 0.0 {
        return base_skew;
    }
    let frac_elapsed = (1.0 - (tau_sec / window_sec)).clamp(0.0, 1.0);
    base_skew * (1.0 + max_boost.max(0.0) * frac_elapsed)
}

// ─────────────────────────────────────────────────────────────────────────
// 6. Adverse-selection (toxicity) monitor
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct MarkoutSample {
    ts_ms: u64,
    side_is_up: bool,
    fair_at_fill: f64,
}

/// Watches whether fair value moves against us right after each fill. If our
/// fills are consistently followed by adverse fair-value drift, we are being
/// picked off — so we widen spreads until it stops. Self-correcting.
#[derive(Debug, Clone)]
pub struct ToxicityMonitor {
    ewma_markout: f64,
    decay: f64,
    horizon_ms: u64,
    k_widen: f64,
    max_widen: f64,
    pending: VecDeque<MarkoutSample>,
}

impl ToxicityMonitor {
    pub fn new(horizon_ms: u64, decay: f64, k_widen: f64, max_widen: f64) -> Self {
        Self {
            ewma_markout: 0.0,
            decay: decay.clamp(0.0, 0.999),
            horizon_ms: horizon_ms.max(200),
            k_widen: k_widen.max(0.0),
            max_widen: max_widen.max(0.0),
            pending: VecDeque::new(),
        }
    }

    /// Record that we just got filled on `side` while fair value was `fair_now`.
    pub fn on_fill(&mut self, side_is_up: bool, fair_now: f64, ts_ms: u64) {
        self.pending.push_back(MarkoutSample {
            ts_ms,
            side_is_up,
            fair_at_fill: fair_now,
        });
        if self.pending.len() > 256 {
            self.pending.pop_front();
        }
    }

    /// On each new fair-value observation, settle any matured fills and update
    /// the running markout. A fill is "toxic" when the side we bought lost
    /// fair value over the horizon (we bought Up and p_up fell, or vice-versa).
    pub fn on_fair(&mut self, fair_up_now: f64, now_ms: u64) {
        while let Some(front) = self.pending.front().copied() {
            if now_ms.saturating_sub(front.ts_ms) < self.horizon_ms {
                break;
            }
            self.pending.pop_front();
            let fair_for_side = if front.side_is_up {
                fair_up_now
            } else {
                1.0 - fair_up_now
            };
            // Positive markout = good (the thing we bought gained value).
            let markout = fair_for_side - front.fair_at_fill;
            self.ewma_markout = self.decay * self.ewma_markout + (1.0 - self.decay) * markout;
        }
    }

    /// Extra half-spread (price units) to charge for current toxicity.
    pub fn widen(&self) -> f64 {
        ((-self.ewma_markout).max(0.0) * self.k_widen).clamp(0.0, self.max_widen)
    }

    pub fn ewma(&self) -> f64 {
        self.ewma_markout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn width_shrinks_as_time_runs_out() {
        let w_open = uncertainty_width(6.0, 300.0, 3.0);
        let w_late = uncertainty_width(6.0, 10.0, 3.0);
        assert!(
            w_open > w_late,
            "W must shrink toward close: {w_open} vs {w_late}"
        );
        assert!(uncertainty_width(6.0, 0.0, 3.0) >= 3.0 - 1e-9);
    }

    #[test]
    fn fair_value_snaps_to_certainty_near_close() {
        // BTC $20 above P. Early (300s, ~$104 of uncertainty): near coin-flip.
        // Final seconds (2s, ~$8 of uncertainty): $20 is 2.4 sigma -> near-certain Up.
        let p_early = digital_p_up(100_020.0, 100_000.0, uncertainty_width(6.0, 300.0, 3.0));
        let p_late = digital_p_up(100_020.0, 100_000.0, uncertainty_width(6.0, 2.0, 3.0));
        assert!(p_early > 0.5 && p_early < 0.65, "early p_up={p_early}");
        assert!(p_late > 0.95, "late p_up should be near 1, got {p_late}");
    }

    #[test]
    fn spread_widens_near_atm_at_close() {
        let vol = 6.0;
        let w_close = uncertainty_width(vol, 6.0, 3.0);
        let sens_atm = price_sensitivity(100_000.0, 100_000.0, w_close);
        let w_open = uncertainty_width(vol, 280.0, 3.0);
        let sens_far = price_sensitivity(100_400.0, 100_000.0, w_open);
        let base = SpreadInputs {
            base_half_spread: 0.012,
            sensitivity: 0.0,
            vol_per_sqrt_sec: vol,
            latency_sec: 0.4,
            k_adverse: 1.0,
            toxicity_widen: 0.0,
            min_half_spread: 0.005,
            max_half_spread: 0.30,
        };
        let atm = half_spread(&SpreadInputs {
            sensitivity: sens_atm,
            ..base
        });
        let far = half_spread(&SpreadInputs {
            sensitivity: sens_far,
            ..base
        });
        assert!(
            atm > far,
            "ATM-at-close spread {atm} must exceed calm spread {far}"
        );
    }

    #[test]
    fn quote_keeps_binary_edge_and_bounds() {
        let q = market_maker_bids(&MmParams {
            p_up: 0.80,
            half_spread: 0.02,
            inventory_skew: 0.03,
            up_inventory: 0.0,
            down_inventory: 0.0,
            max_side_inventory: 10.0,
            min_bid: 0.05,
            max_bid: 0.62,
            min_lock_edge: 0.04,
        });
        assert!(q.up_bid + q.down_bid <= 0.96 + 1e-9);
        assert!(q.up_bid <= 0.62 + 1e-9);
        assert!(q.down_bid >= 0.05 - 1e-9);
    }

    #[test]
    fn inventory_skew_reduces_long_side() {
        let mk = |up: f64, down: f64| {
            market_maker_bids(&MmParams {
                p_up: 0.50,
                half_spread: 0.02,
                inventory_skew: 0.05,
                up_inventory: up,
                down_inventory: down,
                max_side_inventory: 10.0,
                min_bid: 0.05,
                max_bid: 0.62,
                min_lock_edge: 0.04,
            })
        };
        let neutral = mk(0.0, 0.0);
        let long_up = mk(10.0, 0.0);
        assert!(long_up.up_bid < neutral.up_bid);
        assert!(long_up.down_bid > neutral.down_bid);
    }

    #[test]
    fn post_only_bid_stays_below_ask() {
        // margin 1 tick -> 0.52; margin 3 ticks -> 0.50; both strictly below ask.
        let px1 = post_only_bid(0.55, 0.53, 0.01, 0.05, 0.62, 1.0).unwrap();
        assert!((px1 - 0.52).abs() < 1e-9);
        let px3 = post_only_bid(0.55, 0.53, 0.01, 0.05, 0.62, 3.0).unwrap();
        assert!((px3 - 0.50).abs() < 1e-9);
        assert!(px3 < px1 && px3 < 0.53);
    }

    #[test]
    fn warmup_blocks_only_the_opening_seconds() {
        // 300s window, 25s warmup: 10s elapsed (tau 290) is quiet, 30s is not.
        assert!(in_warmup(290.0, 300.0, 25.0));
        assert!(!in_warmup(270.0, 300.0, 25.0));
        assert!(!in_warmup(290.0, 300.0, 0.0), "0 disables warmup");
        // tau glitching above the window still counts as "just opened".
        assert!(in_warmup(301.0, 300.0, 25.0));
    }

    #[test]
    fn time_boost_increases_skew_toward_close() {
        let early = time_boosted_skew(0.03, 300.0, 300.0, 2.0);
        let late = time_boosted_skew(0.03, 10.0, 300.0, 2.0);
        assert!(late > early);
        assert!((early - 0.03).abs() < 1e-9);
    }

    #[test]
    fn endgame_phases_and_side_gating() {
        assert_eq!(phase_for(200.0, 12.0), Phase::Normal);
        assert_eq!(phase_for(30.0, 12.0), Phase::Normal);
        assert_eq!(phase_for(5.0, 12.0), Phase::Pull);
    }

    #[test]
    fn toxicity_widens_after_adverse_fills() {
        let mut tox = ToxicityMonitor::new(1000, 0.5, 2.0, 0.1);
        tox.on_fill(true, 0.60, 0);
        tox.on_fair(0.40, 2000);
        assert!(
            tox.ewma() < 0.0,
            "markout should be negative, got {}",
            tox.ewma()
        );
        assert!(tox.widen() > 0.0, "should request widening");

        let clean = ToxicityMonitor::new(1000, 0.5, 2.0, 0.1);
        assert_eq!(clean.widen(), 0.0);
    }

    #[test]
    fn vol_estimator_tracks_bigger_moves() {
        let mut calm = VolEstimator::new(6.0, 20.0, 1.5, 60.0);
        for i in 0..50 {
            calm.update(100_000.0 + (i % 2) as f64, 1.0);
        }
        let mut wild = VolEstimator::new(6.0, 20.0, 1.5, 60.0);
        for i in 0..50 {
            wild.update(100_000.0 + (i % 2) as f64 * 40.0, 1.0);
        }
        assert!(
            wild.current() > calm.current(),
            "wild {} vs calm {}",
            wild.current(),
            calm.current()
        );
        assert!(calm.warm() && wild.warm());
    }
}
