# 策略升级规划

本文记录 BTC 5 分钟 Polymarket 机器人下一阶段的模型增强方向。目标不是保证盈利，而是把当前“只靠 Binance 价格模型”的实时胜率，升级成更接近实盘交易环境的融合胜率，并用数据验证每一步是否真的改善成交质量。

## 当前问题

当前策略的核心胜率来自：

```text
Binance BTC 实时价格 vs 当前 5 分钟窗口开盘价
```

然后用数字期权近似模型计算：

```text
p_up = Phi((spot - strike) / width)
p_down = 1 - p_up
```

报价逻辑不是直接跟随 Polymarket 盘口，而是：

```text
买价 <= 模型 fair - VALUE_MIN_EDGE
```

所以会出现用户看到盘口是 `24%`，但机器人模型只认为 `16%`，最后挂到 `12%` 的情况。这不是执行错误，而是模型和盘口对实时胜率的判断不一致。

## 总体方向

新增一个融合胜率层：

```text
p_final = f(
  Binance 模型胜率,
  Polymarket 盘口隐含胜率,
  Binance 短线动量,
  Binance 订单流,
  多交易所价格偏差,
  历史校准因子
)
```

报价继续保持 maker 思维：

```text
bid <= p_final - safety_edge
```

先提升 `p_final` 的质量，再决定是否收窄 `safety_edge`。不要先用激进报价硬追成交。

## 阶段 1：盘口胜率锚定

优先级最高。

新增 Polymarket 盘口隐含胜率：

```text
p_market_up = best bid/ask mid
p_market_down = 1 - p_market_up
```

融合方式先用保守版本：

```text
p_final = w_model * p_model + w_market * p_market
```

初始建议：

```text
w_model = 0.70
w_market = 0.30
```

盘口越健康，盘口权重越高：

- bid/ask 价差越小，`w_market` 越高。
- 两边盘口都有有效 bid/ask，`w_market` 越高。
- 盘口长时间不更新或价差异常，`w_market` 降低。

验收指标：

- `model-market` 新增 `p_final` 对比列。
- 影子模式记录：`p_model`、`p_market`、`p_final`、实际报价。
- 当盘口 `24%`、模型 `16%` 时，`p_final` 不再贴着 `16%`，而是向盘口靠拢。

## 阶段 2：短线动量修正

对 5 分钟二元盘，最后几十秒和单边趋势中，价格斜率比静态价格更重要。

新增 Binance 动量特征：

```text
mom_1s  = spot_now - spot_1s_ago
mom_3s  = spot_now - spot_3s_ago
mom_10s = spot_now - spot_10s_ago
accel   = mom_1s - previous_mom_1s
```

修正方向：

- BTC 连续上冲时，提高 `p_up`。
- BTC 连续下压时，提高 `p_down`。
- 价格接近 strike 且剩余时间很少时，动量权重提高。
- 离 strike 很远时，动量权重降低，避免噪声过度影响。

验收指标：

- 日志记录 `mom_1s/mom_3s/mom_10s/accel`。
- 对比“模型慢一步”的场景，检查 `p_final` 是否比旧模型更早移动。
- 不能让震荡局里报价来回追高，必须保留 `VALUE_MIN_EDGE`。

## 阶段 3：订单流信号

价格是结果，主动成交方向往往更早。

新增 Binance trade flow：

```text
buy_pressure  = 最近 N 秒主动买成交量
sell_pressure = 最近 N 秒主动卖成交量
flow_imbalance = (buy_pressure - sell_pressure) / total_volume
```

用途：

- 主动买明显强于主动卖，提高 `p_up`。
- 主动卖明显强于主动买，提高 `p_down`。
- 成交密度突然上升时，提高撤单/重报价警觉性。

验收指标：

- 影子模式统计 flow 信号与下一段 BTC 方向的一致性。
- 只在信号显著时修正，不让小成交噪声影响报价。

## 阶段 4：多交易所价格

当前模型主要依赖 Binance。后续可引入：

```text
Binance
Coinbase
OKX
Bybit
Kraken
```

融合方式优先使用中位数或稳健加权价：

```text
spot_final = median(exchange_spots)
```

注意：

- Polymarket 最终结算源是 Chainlink BTC/USD Data Stream，不是 Binance。
- 多交易所不是越多越好，要先验证哪个价格源更贴近结算。
- 如果某个交易所延迟、卡顿、偏离过大，自动降权或剔除。

验收指标：

- 记录各交易所相对 Binance 的价差。
- 记录 `spot_final` 与最终结算方向的匹配情况。
- 不能因为某个交易所短暂异常导致错误报价。

## 阶段 5：历史校准层

模型输出的 `0.80` 不等于真实就有 `80%`。需要按历史数据校准。

按这些维度分桶：

```text
side: Up / Down
remaining_time: 240-300, 180-240, 120-180, 60-120, 30-60, 10-30, 0-10
probability_bucket: 0.05 间隔
```

统计：

```text
模型平均胜率
实际结算胜率
盘口隐含胜率
成交后 PnL
```

校准方式：

```text
p_calibrated = calibration_table.adjust(p_final, side, remaining_time)
```

例子：

如果历史发现：

```text
Down 模型 0.80 桶，实际只赢 0.70
```

那么之后 `Down 0.80` 不再按 `0.80` 报价，而是按接近 `0.70` 的校准胜率报价。

验收指标：

- 样本不足时不启用强校准。
- 校准前后分别输出 Brier score。
- 校准后不能只提高成交率，还要改善单边亏损和成交后期望。

## 实施顺序

建议分四个小 PR 或提交推进：

1. 先做日志字段和分析命令，不改变实盘报价。
2. 加入盘口锚定的影子模式，只记录 `p_final_shadow`。
3. 小权重启用盘口锚定，例如 `w_market=0.20`。
4. 再加入动量和校准层。

每一步都要能通过 `.env` 开关关闭：

```text
ENABLE_MARKET_ANCHOR=0
ENABLE_MOMENTUM_SIGNAL=0
ENABLE_FLOW_SIGNAL=0
ENABLE_CALIBRATION=0
```

## 风控原则

- 不做中途主动卖出，除非以后明确新增单独的减仓模式。
- 继续保持 post-only maker 买单。
- 不因为融合胜率提高就取消 `VALUE_MIN_EDGE`。
- 不让盘口锚定突破 `MAX_BID`、`MAX_UNPAIRED_SHARES`、`MAX_TOTAL_INVENTORY`。
- 所有新信号先跑影子模式，再进入实盘报价。

## 下一步

最先实现：

```text
盘口锚定 + 影子模式日志
```

新增输出：

```text
p_model_up
p_market_up
p_final_up_shadow
market_anchor_weight
```

等影子日志证明它能解释“盘口 24、模型 16、报价 12”这类场景后，再让 `quote-engine` 使用 `p_final` 替代纯 `p_model`。
