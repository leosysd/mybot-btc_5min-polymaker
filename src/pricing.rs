#[derive(Debug, Clone, Copy)]
pub struct ModelQuote {
    pub up_bid: f64,
    pub down_bid: f64,
}

pub fn market_maker_bids(
    p_up: f64,
    spread: f64,
    inventory_skew: f64,
    up_inventory: f64,
    down_inventory: f64,
    max_side_inventory: f64,
    min_bid: f64,
    max_bid: f64,
) -> ModelQuote {
    let spread = spread.clamp(0.0, 0.95);
    let cap_sum = (1.0 - spread).clamp(0.02, 0.98);
    let min_bid = min_bid.clamp(0.01, cap_sum / 2.0);
    let max_bid = max_bid.clamp(min_bid, 0.99);
    let fair_up = p_up.clamp(0.01, 0.99);
    let fair_down = 1.0 - fair_up;
    let half_spread = spread / 2.0;
    let imbalance = if max_side_inventory > 0.0 {
        ((up_inventory - down_inventory) / max_side_inventory).clamp(-1.0, 1.0)
    } else {
        0.0
    };
    let skew = imbalance * inventory_skew.clamp(0.0, 0.25);

    let mut up_bid = (fair_up - half_spread - skew).clamp(min_bid, max_bid);
    let mut down_bid = (fair_down - half_spread + skew).clamp(min_bid, max_bid);
    enforce_binary_edge(&mut up_bid, &mut down_bid, cap_sum, min_bid);
    ModelQuote { up_bid, down_bid }
}

pub fn post_only_bid(model_bid: f64, best_ask: f64, tick_size: f64, min_bid: f64, max_bid: f64) -> Option<f64> {
    if !model_bid.is_finite() || !best_ask.is_finite() || best_ask <= tick_size {
        return None;
    }
    let post_only_cap = best_ask - tick_size;
    let raw = model_bid.min(post_only_cap).min(max_bid).clamp(0.01, 0.99);
    let px = floor_to_tick(raw, tick_size);
    if px + 1e-12 < min_bid || px >= best_ask - 1e-12 {
        None
    } else {
        Some(px)
    }
}

pub fn normal_cdf(z: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.2316419 * z.abs());
    let poly = t
        * (0.319381530
            + t * (-0.356563782
                + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
    let pdf = (-0.5 * z * z).exp() / (2.0 * std::f64::consts::PI).sqrt();
    let cdf = 1.0 - pdf * poly;
    if z >= 0.0 { cdf } else { 1.0 - cdf }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_keeps_binary_edge() {
        let q = market_maker_bids(0.80, 0.04, 0.03, 0.0, 0.0, 10.0, 0.05, 0.62);
        assert!(q.up_bid + q.down_bid <= 0.960000001);
        assert!(q.up_bid <= 0.62);
        assert!(q.down_bid >= 0.05);
    }

    #[test]
    fn inventory_skews_to_reduce_long_side() {
        let neutral = market_maker_bids(0.50, 0.04, 0.03, 0.0, 0.0, 10.0, 0.05, 0.62);
        let long_up = market_maker_bids(0.50, 0.04, 0.03, 10.0, 0.0, 10.0, 0.05, 0.62);
        assert!(long_up.up_bid < neutral.up_bid);
        assert!(long_up.down_bid > neutral.down_bid);
    }

    #[test]
    fn post_only_bid_stays_below_ask() {
        let px = post_only_bid(0.55, 0.53, 0.01, 0.05, 0.62).unwrap();
        assert!((px - 0.52).abs() < 1e-9);
        assert!(px < 0.53);
    }
}
