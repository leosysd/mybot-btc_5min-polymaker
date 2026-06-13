# BTC 5 分钟方向信号优化计划

本文只规划方向信号层的优化，不规划库存、配平、单边暴露、仓位上限或补单策略。

当前结论很明确：机器人很多亏损不是单纯“锁不住”，而是方向概率过度自信。现有 `strategy brain v3` 更像数字期权定价引擎，它根据 BTC 现价、判定价、剩余时间和波动率计算胜率；这不是完整的短线方向预测模型。

## 当前问题

当前方向概率核心近似为：

```text
model_up = Phi((spot - price_to_beat) / width)
```

它默认未来价格是零漂移随机波动，所以只要 BTC 当前站在判定线上方，且剩余时间变少，`model_up` 就容易快速升到 `0.90`、`0.95`、甚至 `0.99`。

历史亏损里已经出现过：

```text
model_up=0.991，最后 Down
model_up=0.959，最后 Down
model_up=0.159，等于强烈看 Down，最后 Up
```

这说明问题不是固定偏多或固定偏空，而是高置信概率没有经过真实胜率校准。

## 目标

方向优化的目标不是让机器人更敢下注，而是让它少给假高置信。

核心目标：

```text
1. 降低错误的 0.90+ / 0.95+ 高置信方向。
2. 让方向概率包含短线动量、主动成交压力、盘口微结构和 Polymarket 盘口确认。
3. 用 shadow 日志验证新方向概率，再决定是否替换实盘方向。
4. 用 Brier score、log loss、概率分桶命中率评估方向质量。
```

非目标：

```text
1. 不改库存倍数。
2. 不改配平逻辑。
3. 不改单边暴露限制。
4. 不因为新方向信号直接取消安全边际。
```

## 版本定义

这里的方向版本和代码里已有的 `strategy brain v3` 不是一回事。

```text
现有 strategy brain v3:
  定价引擎。负责根据 spot、strike、tau、vol 算 fair value。

方向 v1:
  shadow 方向引擎。计算新方向信号并写日志，但不影响实盘报价。

方向 v2:
  实盘可用方向模型。用 v1 数据校准权重后，替代或修正现有 model_up。

方向 v3:
  高级方向模型。包含多交易所、regime 切换、在线校准或机器学习。
```

当前优先级：先做方向 v1，再用 shadow 数据升级方向 v2。方向 v3 暂不急。

## 方向 v1：Shadow 信号引擎

方向 v1 一次性把完整方向特征算出来，但只记录，不改变实盘报价。

新增输出：

```text
raw_model_up
calibrated_model_up
momentum_up
flow_up
book_up
market_up
final_direction_up_shadow
direction_confidence
direction_source_flags
```

### 1. 概率降温

先对现有 `model_up` 做校准降温，避免假高置信。

```text
p_raw = model_up
p_calibrated = sigmoid(logit(p_raw) / T)
```

初始建议：

```text
DIRECTION_TEMP=1.5
```

大致效果：

```text
0.95 -> 约 0.82
0.99 -> 约 0.91
0.80 -> 约 0.70
```

目的：让当前定价概率先接近真实命中率，而不是把“当前站在线上”直接当成强方向。

### 2. Binance 短线动量

新增 BTC 短周期价格动量：

```text
mom_1s  = spot_now - spot_1s_ago
mom_3s  = spot_now - spot_3s_ago
mom_10s = spot_now - spot_10s_ago
mom_20s = spot_now - spot_20s_ago
```

方向含义：

```text
动量同向：提高方向可信度
动量反向：降低方向可信度
多周期冲突：降低方向置信
```

例如模型看 Up，但 `mom_1s`、`mom_3s`、`mom_10s` 都为负，则 Up 概率要降权。

### 3. Binance 主动买卖压力

基于 Binance trade stream 计算主动成交方向。

新增特征：

```text
buy_volume_1s / sell_volume_1s
buy_volume_3s / sell_volume_3s
buy_volume_10s / sell_volume_10s
flow_imbalance_1s
flow_imbalance_3s
flow_imbalance_10s
```

公式：

```text
flow_imbalance = (buy_volume - sell_volume) / max(total_volume, eps)
```

方向含义：

```text
主动买明显强：提高 Up 信号
主动卖明显强：提高 Down 信号
成交量太小：信号降权
```

### 4. Binance 盘口微价格

新增 Binance bookTicker 或 depth 数据。

优先实现：

```text
btcusdt@bookTicker
```

后续可升级：

```text
btcusdt@depth5@100ms
```

核心特征：

```text
mid = (best_bid + best_ask) / 2
microprice = (best_ask * bid_size + best_bid * ask_size) / (bid_size + ask_size)
book_imbalance = (bid_size - ask_size) / (bid_size + ask_size)
```

方向含义：

```text
microprice > mid：买盘压力偏强
microprice < mid：卖盘压力偏强
book_imbalance > 0：买盘厚
book_imbalance < 0：卖盘厚
```

### 5. Polymarket 盘口确认

Polymarket `market_up` 不直接替代方向模型，只作为确认信号。

有效条件：

```text
1. up/down 双边 bid/ask 都有效。
2. 盘口 spread 不过宽。
3. market_up 最近几秒没有 stale。
4. market_up 的变化方向和 Binance 信号一致时才加权。
```

记录：

```text
market_up_mid
market_up_change_3s
market_up_change_10s
market_spread
market_anchor_weight
```

## 方向 v1 融合公式

v1 不追求复杂机器学习，先用可解释线性分数：

```text
z =
  w_distance * distance_score
+ w_mom_1s   * momentum_1s_score
+ w_mom_3s   * momentum_3s_score
+ w_mom_10s  * momentum_10s_score
+ w_flow_3s  * flow_3s_score
+ w_flow_10s * flow_10s_score
+ w_book     * book_microprice_score
+ w_market   * polymarket_confirmation_score

final_direction_up_shadow = sigmoid(z)
```

其中：

```text
distance_score = logit(calibrated_model_up)
```

初始权重先保守：

```text
w_distance = 1.00
w_mom_1s   = 0.15
w_mom_3s   = 0.25
w_mom_10s  = 0.20
w_flow_3s  = 0.20
w_flow_10s = 0.15
w_book     = 0.15
w_market   = 0.25
```

所有权重必须可配置，并且 v1 阶段只写日志。

## 方向 v2：校准后实盘模型

方向 v2 的任务是把 v1 shadow 数据变成实盘可用方向概率。

启用条件：

```text
1. shadow 数据覆盖至少数百个 5 分钟市场。
2. final_direction_up_shadow 的 Brier score 优于 raw_model_up。
3. 0.70 / 0.80 / 0.90 概率分桶命中率比 raw_model_up 更接近真实胜率。
4. 高置信错误明显减少。
```

验证指标：

```text
Brier score
log loss
probability bucket calibration
high-confidence miss rate
Up/Down 分侧命中率
剩余时间分桶命中率
```

概率分桶：

```text
0.50-0.55
0.55-0.60
0.60-0.65
0.65-0.70
0.70-0.75
0.75-0.80
0.80-0.85
0.85-0.90
0.90-0.95
0.95-1.00
```

剩余时间分桶：

```text
240-300s
180-240s
120-180s
60-120s
30-60s
10-30s
0-10s
```

方向 v2 不一定直接替换全部 `model_up`，可以先用混合模式：

```text
model_up_live = (1 - live_weight) * raw_model_up + live_weight * final_direction_up
```

初始：

```text
live_weight = 0.25
```

验证稳定后再提高。

## 方向 v3：后续高级模型

方向 v3 暂时不作为当前开发目标。

可选方向：

```text
1. 多交易所价格中位数：Binance、Coinbase、OKX、Bybit。
2. 贴近 Polymarket 结算源的价格源研究。
3. 不同波动 regime 下切换权重。
4. 在线校准温度 T。
5. 轻量 ML 模型，例如 logistic regression / gradient boosted trees。
```

v3 的前提是 v1/v2 已经积累足够干净的特征和结果数据。

## 实施顺序

### 阶段 A：日志和数据结构

新增方向特征结构体和日志字段。

要求：

```text
不影响实盘报价
不影响库存
不影响配平
不改变现有下单决策
```

输出到 `quotes.jsonl` 或新的 `direction.jsonl`：

```text
market
ts_ms
btc_price
price_to_beat
tau_seconds
raw_model_up
calibrated_model_up
mom_1s
mom_3s
mom_10s
mom_20s
flow_imbalance_1s
flow_imbalance_3s
flow_imbalance_10s
book_microprice
book_imbalance
market_up
final_direction_up_shadow
```

### 阶段 B：Binance 信号采集

新增：

```text
trade flow ring buffer
bookTicker websocket
microprice calculator
momentum window buffer
```

容错：

```text
信号 stale 时降权
缺失时回退 raw_model_up
异常跳价时过滤
```

### 阶段 C：Shadow 回测命令

新增命令：

```text
polymaker direction-stats
```

输出：

```text
raw_model_up Brier score
final_direction_up_shadow Brier score
概率分桶实际胜率
高置信错误列表
按剩余时间分桶的表现
```

### 阶段 D：小权重实盘启用

只有 shadow 数据验证通过后，才允许：

```text
ENABLE_DIRECTION_MODEL=1
DIRECTION_LIVE_WEIGHT=0.25
```

再根据数据提高权重。

## 配置开关

建议新增：

```text
ENABLE_DIRECTION_SHADOW=1
ENABLE_DIRECTION_MODEL=0
DIRECTION_TEMP=1.5
DIRECTION_LIVE_WEIGHT=0.0

ENABLE_BINANCE_FLOW_SIGNAL=1
ENABLE_BINANCE_BOOK_SIGNAL=1
ENABLE_POLYMARKET_CONFIRM_SIGNAL=1

DIRECTION_W_DISTANCE=1.00
DIRECTION_W_MOM_1S=0.15
DIRECTION_W_MOM_3S=0.25
DIRECTION_W_MOM_10S=0.20
DIRECTION_W_FLOW_3S=0.20
DIRECTION_W_FLOW_10S=0.15
DIRECTION_W_BOOK=0.15
DIRECTION_W_MARKET=0.25
```

默认实盘不启用新方向，只 shadow。

## 验收标准

方向 v1 验收：

```text
1. 新方向字段持续写入日志。
2. 缺失 Binance book/flow 时机器人正常运行。
3. direction-stats 能输出概率校准结果。
4. 不改变实盘报价。
```

方向 v2 验收：

```text
1. Brier score 优于 raw_model_up。
2. 0.80+ 和 0.90+ 的真实命中率更接近标称概率。
3. 高置信错向样本减少。
4. 启用小权重后不出现明显成交质量恶化。
```

## 下一步

下一步直接实现方向 v1 shadow，不再继续只讨论概念。

第一批代码只做：

```text
1. 概率降温。
2. Binance 1s/3s/10s/20s 动量。
3. Binance 主动买卖压力。
4. Binance bookTicker 微价格。
5. Polymarket 盘口确认。
6. final_direction_up_shadow 日志。
7. direction-stats 分析命令。
```

实盘方向替换留到 v2，等 shadow 数据证明后再启用。
