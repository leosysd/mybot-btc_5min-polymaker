//! Direction-signal shadow model.
//!
//! This module is intentionally pure-ish and passive: it computes extra
//! direction features for logging and calibration, but v1 does not change live
//! quoting decisions.

use crate::config::Config;
use crate::pricing::{digital_p_up, uncertainty_width};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

const EPS: f64 = 1e-9;
const MAX_HISTORY_MS: u64 = 30_000;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct DirectionSnapshot {
    pub raw_model_up: f64,
    pub calibrated_model_up: f64,
    pub momentum_up: f64,
    pub flow_up: f64,
    pub book_up: f64,
    pub market_up: f64,
    pub final_direction_up_shadow: f64,
    pub direction_confidence: f64,
    pub mom_1s: f64,
    pub mom_3s: f64,
    pub mom_10s: f64,
    pub mom_20s: f64,
    pub flow_imbalance_1s: f64,
    pub flow_imbalance_3s: f64,
    pub flow_imbalance_10s: f64,
    pub book_microprice: f64,
    pub book_imbalance: f64,
}

#[derive(Debug, Clone, Copy)]
struct PricePoint {
    ts_ms: u64,
    price: f64,
}

#[derive(Debug, Clone, Copy)]
struct FlowPoint {
    ts_ms: u64,
    buy_qty: f64,
    sell_qty: f64,
}

#[derive(Debug, Clone, Copy)]
struct BookPoint {
    ts_ms: u64,
    bid: f64,
    bid_qty: f64,
    ask: f64,
    ask_qty: f64,
}

#[derive(Debug, Clone, Default)]
pub struct DirectionEstimator {
    prices: VecDeque<PricePoint>,
    flows: VecDeque<FlowPoint>,
    book: Option<BookPoint>,
}

impl DirectionEstimator {
    pub fn record_trade(
        &mut self,
        ts_ms: u64,
        price: f64,
        qty: Option<f64>,
        buyer_is_maker: Option<bool>,
    ) {
        if price.is_finite() && price > 0.0 {
            self.prices.push_back(PricePoint { ts_ms, price });
        }
        if let (Some(qty), Some(buyer_is_maker)) = (qty, buyer_is_maker) {
            if qty.is_finite() && qty > 0.0 {
                // Binance `m=true`: buyer is maker, so the taker was a seller.
                let (buy_qty, sell_qty) = if buyer_is_maker {
                    (0.0, qty)
                } else {
                    (qty, 0.0)
                };
                self.flows.push_back(FlowPoint {
                    ts_ms,
                    buy_qty,
                    sell_qty,
                });
            }
        }
        self.prune(ts_ms);
    }

    pub fn record_book(&mut self, ts_ms: u64, bid: f64, bid_qty: f64, ask: f64, ask_qty: f64) {
        if bid.is_finite()
            && ask.is_finite()
            && bid_qty.is_finite()
            && ask_qty.is_finite()
            && bid > 0.0
            && ask > bid
            && bid_qty > 0.0
            && ask_qty > 0.0
        {
            self.book = Some(BookPoint {
                ts_ms,
                bid,
                bid_qty,
                ask,
                ask_qty,
            });
        }
        self.prune(ts_ms);
    }

    pub fn snapshot(
        &self,
        cfg: &Config,
        now_ms: u64,
        btc_price: f64,
        price_to_beat: f64,
        tau_seconds: f64,
        vol_per_sqrt_sec: f64,
        market_up: Option<f64>,
        market_spread: Option<f64>,
    ) -> DirectionSnapshot {
        let vol = if vol_per_sqrt_sec > 0.0 {
            vol_per_sqrt_sec
        } else {
            cfg.vol_seed_per_sqrt_sec
        };
        let width = uncertainty_width(vol, tau_seconds, cfg.width_floor_usd);
        let raw_model_up = digital_p_up(btc_price, price_to_beat, width);
        let calibrated_model_up = temperature_scale(raw_model_up, cfg.direction_temp);

        let mom_1s = self.momentum(now_ms, 1_000, btc_price);
        let mom_3s = self.momentum(now_ms, 3_000, btc_price);
        let mom_10s = self.momentum(now_ms, 10_000, btc_price);
        let mom_20s = self.momentum(now_ms, 20_000, btc_price);
        let momentum_score = weighted_average(&[
            (score_usd(mom_1s, width), cfg.direction_w_mom_1s.abs()),
            (score_usd(mom_3s, width), cfg.direction_w_mom_3s.abs()),
            (score_usd(mom_10s, width), cfg.direction_w_mom_10s.abs()),
        ])
        .unwrap_or(0.0);
        let momentum_up = sigmoid(momentum_score).clamp(0.0001, 0.9999);

        let flow_imbalance_1s = self.flow_imbalance(now_ms, 1_000);
        let flow_imbalance_3s = self.flow_imbalance(now_ms, 3_000);
        let flow_imbalance_10s = self.flow_imbalance(now_ms, 10_000);
        let flow_score = weighted_average(&[
            (flow_imbalance_3s, cfg.direction_w_flow_3s.abs()),
            (flow_imbalance_10s, cfg.direction_w_flow_10s.abs()),
        ])
        .unwrap_or(0.0);
        let flow_up = sigmoid(flow_score).clamp(0.0001, 0.9999);

        let (book_microprice, book_imbalance, book_score) =
            self.book_scores(cfg, now_ms).unwrap_or((0.0, 0.0, 0.0));
        let book_up = sigmoid(book_score).clamp(0.0001, 0.9999);

        let market_up_value = market_up.unwrap_or(0.0);
        let market_score = if cfg.enable_polymarket_confirm_signal
            && market_up_value > 0.0
            && market_up_value < 1.0
            && market_spread.is_some_and(|spread| spread <= cfg.market_anchor_max_spread)
        {
            logit(market_up_value)
        } else {
            0.0
        };

        let mut z = cfg.direction_w_distance * logit(calibrated_model_up);
        if cfg.enable_binance_flow_signal {
            z += cfg.direction_w_mom_1s * score_usd(mom_1s, width);
            z += cfg.direction_w_mom_3s * score_usd(mom_3s, width);
            z += cfg.direction_w_mom_10s * score_usd(mom_10s, width);
            z += cfg.direction_w_flow_3s * flow_imbalance_3s;
            z += cfg.direction_w_flow_10s * flow_imbalance_10s;
        }
        if cfg.enable_binance_book_signal {
            z += cfg.direction_w_book * book_score;
        }
        if cfg.enable_polymarket_confirm_signal {
            z += cfg.direction_w_market * market_score;
        }
        let final_direction_up_shadow = sigmoid(z).clamp(0.0001, 0.9999);
        let direction_confidence = (final_direction_up_shadow - 0.5).abs() * 2.0;

        DirectionSnapshot {
            raw_model_up,
            calibrated_model_up,
            momentum_up,
            flow_up,
            book_up,
            market_up: market_up_value,
            final_direction_up_shadow,
            direction_confidence,
            mom_1s,
            mom_3s,
            mom_10s,
            mom_20s,
            flow_imbalance_1s,
            flow_imbalance_3s,
            flow_imbalance_10s,
            book_microprice,
            book_imbalance,
        }
    }

    fn momentum(&self, now_ms: u64, lookback_ms: u64, current_price: f64) -> f64 {
        let target = now_ms.saturating_sub(lookback_ms);
        self.prior_price(target)
            .map(|past| current_price - past)
            .unwrap_or(0.0)
    }

    fn prior_price(&self, target_ms: u64) -> Option<f64> {
        self.prices
            .iter()
            .rev()
            .find(|point| point.ts_ms <= target_ms)
            .map(|point| point.price)
            .or_else(|| self.prices.front().map(|point| point.price))
    }

    fn flow_imbalance(&self, now_ms: u64, lookback_ms: u64) -> f64 {
        let start = now_ms.saturating_sub(lookback_ms);
        let (buy, sell) = self
            .flows
            .iter()
            .filter(|point| point.ts_ms >= start)
            .fold((0.0, 0.0), |(buy, sell), point| {
                (buy + point.buy_qty, sell + point.sell_qty)
            });
        let total = buy + sell;
        if total > EPS {
            ((buy - sell) / total).clamp(-1.0, 1.0)
        } else {
            0.0
        }
    }

    fn book_scores(&self, cfg: &Config, now_ms: u64) -> Option<(f64, f64, f64)> {
        let book = self.book?;
        if now_ms.saturating_sub(book.ts_ms) > cfg.stale_after_ms {
            return None;
        }
        let total = book.bid_qty + book.ask_qty;
        if total <= EPS {
            return None;
        }
        let mid = (book.bid + book.ask) / 2.0;
        let spread = (book.ask - book.bid).max(EPS);
        let microprice = (book.ask * book.bid_qty + book.bid * book.ask_qty) / total;
        let imbalance = ((book.bid_qty - book.ask_qty) / total).clamp(-1.0, 1.0);
        let micro_score = ((microprice - mid) / (spread / 2.0)).clamp(-1.0, 1.0);
        let score = ((micro_score + imbalance) / 2.0).clamp(-1.0, 1.0);
        Some((microprice, imbalance, score))
    }

    fn prune(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(MAX_HISTORY_MS);
        while self
            .prices
            .front()
            .is_some_and(|point| point.ts_ms < cutoff)
        {
            self.prices.pop_front();
        }
        while self.flows.front().is_some_and(|point| point.ts_ms < cutoff) {
            self.flows.pop_front();
        }
    }
}

pub fn temperature_scale(prob: f64, temp: f64) -> f64 {
    let temp = temp.max(0.1);
    sigmoid(logit(prob) / temp).clamp(0.0001, 0.9999)
}

fn score_usd(delta: f64, width: f64) -> f64 {
    if !delta.is_finite() || width <= 0.0 {
        0.0
    } else {
        (delta / width.max(1.0)).clamp(-1.0, 1.0)
    }
}

fn weighted_average(values: &[(f64, f64)]) -> Option<f64> {
    let (sum, weight) = values.iter().fold((0.0, 0.0), |(sum, weight), (v, w)| {
        if v.is_finite() && *w > 0.0 {
            (sum + v * w, weight + w)
        } else {
            (sum, weight)
        }
    });
    (weight > 0.0).then_some(sum / weight)
}

fn sigmoid(z: f64) -> f64 {
    if z >= 0.0 {
        let ez = (-z).exp();
        1.0 / (1.0 + ez)
    } else {
        let ez = z.exp();
        ez / (1.0 + ez)
    }
}

fn logit(prob: f64) -> f64 {
    let p = prob.clamp(0.0001, 0.9999);
    (p / (1.0 - p)).ln()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temperature_scale_cools_extreme_probabilities() {
        let cooled = temperature_scale(0.95, 2.0);
        assert!(cooled < 0.95);
        assert!(cooled > 0.5);
        let down = temperature_scale(0.05, 2.0);
        assert!(down > 0.05);
        assert!(down < 0.5);
    }

    #[test]
    fn trade_flow_uses_binance_maker_flag() {
        let mut d = DirectionEstimator::default();
        d.record_trade(1_000, 100.0, Some(2.0), Some(false));
        d.record_trade(1_500, 101.0, Some(1.0), Some(true));
        assert!((d.flow_imbalance(2_000, 2_000) - (1.0 / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn book_score_favors_larger_bid_size() {
        let mut cfg = Config::from_env().expect("config");
        cfg.stale_after_ms = 5_000;
        let mut d = DirectionEstimator::default();
        d.record_book(1_000, 100.0, 10.0, 100.1, 1.0);
        let (_, imbalance, score) = d.book_scores(&cfg, 1_100).expect("book score");
        assert!(imbalance > 0.0);
        assert!(score > 0.0);
    }
}
