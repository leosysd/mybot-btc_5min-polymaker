# mybot-btc_5min-polymaker

这是一个 **Rust 多进程 BTC 5 分钟二元市场做市机器人骨架**。

当前版本是低延迟架构骨架，默认 **DRY_RUN 模拟模式**，不会真实下单。真实 Polymarket 下单接口还没有接入，后面应该接在 `order-gateway` 进程里。

## 架构

这个项目按低延迟实盘思路拆成 4 个子进程：

```text
collector      -> quote-engine -> order-gateway -> risk-ledger
行情/价格采集     报价决策        下单热路径         成交/库存/风控
```

各进程职责：

- `collector`：采集行情。当前是模拟 BTC/盘口数据，后面换成 Polymarket WS + Binance/Chainlink。
- `quote-engine`：计算 fair value、Up/Down 双边报价、库存偏移。
- `order-gateway`：下单热路径。以后私钥、签名、下单、撤单、HTTP 保活都放这里。
- `risk-ledger`：记录成交、库存、两种结算情景下的 PnL，并把库存反馈给报价引擎。
- `supervisor`：启动并守护上面 4 个进程。

热路径通信使用 Unix datagram socket：

```text
run/sockets/quote-engine.sock
run/sockets/order-gateway.sock
run/sockets/risk-ledger.sock
```

JSONL 文件只做审计日志，不做热路径通信。

## VPS 安装

先安装基础依赖：

```bash
apt update
apt install -y git curl build-essential pkg-config libssl-dev
```

安装 Rust：

```bash
curl https://sh.rustup.rs -sSf | sh -s -- -y
source ~/.cargo/env
```

拉取项目：

```bash
cd /opt
git clone https://github.com/leosysd/mybot-btc_5min-polymaker.git
cd mybot-btc_5min-polymaker
```

创建配置：

```bash
cp .env.example .env
```

编译：

```bash
cargo build --release
```

## 快速试跑

先清理旧运行目录：

```bash
./target/release/polymaker clean
```

运行 15 秒模拟做市：

```bash
./target/release/polymaker supervisor --seconds 15
```

查看输出：

```bash
ls run
tail -n 5 run/book.jsonl
tail -n 5 run/quotes.jsonl
tail -n 5 run/fills.jsonl
cat run/inventory.json
```

你应该能看到：

- `book.jsonl`：collector 产生的模拟盘口
- `quotes.jsonl`：quote-engine 产生的做市报价
- `fills.jsonl`：order-gateway 模拟成交
- `inventory.json`：risk-ledger 当前库存

停止所有进程：

```bash
./target/release/polymaker stop
```

## 常用命令

查看帮助：

```bash
./target/release/polymaker help
```

单独启动报价引擎：

```bash
./target/release/polymaker quote-engine
```

单独启动下单网关：

```bash
./target/release/polymaker order-gateway
```

单独启动风控账本：

```bash
./target/release/polymaker risk-ledger
```

单独启动行情采集：

```bash
./target/release/polymaker collector
```

启动完整多进程：

```bash
./target/release/polymaker supervisor
```

## PM2 常驻运行

如果 VPS 上装了 PM2，可以这样跑：

```bash
npm install -g pm2
cargo build --release
pm2 start ecosystem.config.js
pm2 save
```

查看状态：

```bash
pm2 status
pm2 logs polymaker-quote-engine
pm2 logs polymaker-order-gateway
```

停止：

```bash
pm2 stop ecosystem.config.js
./target/release/polymaker stop
```

## 配置说明

配置文件是 `.env`，从 `.env.example` 复制出来。

核心参数：

```text
DRY_RUN=1
```

必须保持 `1`。当前版本没有真实下单实现，不能实盘。

```text
QUOTE_SIZE=5
```

每次单边报价的份数。

```text
QUOTE_SPREAD=0.04
```

做市毛价差。机器人会尽量满足：

```text
up_bid + down_bid <= 1 - QUOTE_SPREAD
```

例如 `QUOTE_SPREAD=0.04`，则两边买价总和不超过 `0.96`。

```text
INVENTORY_SKEW=0.03
```

库存偏移强度。如果 Up 库存太多，机器人会降低 Up 买价、抬高 Down 买价，让仓位往平衡方向走。

```text
INVENTORY_MULT=2
```

单边最大库存倍数。单边最大库存为：

```text
QUOTE_SIZE * INVENTORY_MULT
```

例如 `QUOTE_SIZE=5`、`INVENTORY_MULT=2`，单边最多买 `10` 份。

```text
MIN_BID=0.05
MAX_BID=0.62
```

最低/最高挂买价格。避免挂太低的垃圾单，也避免追贵。

```text
MARKET_INTERVAL_MS=120
```

模拟 collector 产生行情的间隔。后面接真实 WS 后，这个参数会弱化。

```text
STALE_AFTER_MS=800
```

行情超过这个时间没更新，quote-engine 会跳过，避免用旧行情报价。

## 做市逻辑

当前报价逻辑是：

1. 用 BTC 相对开盘价的偏移估算 `p_up`。
2. `p_down = 1 - p_up`。
3. 按 fair value 减掉半边价差，得到 Up/Down 模型报价。
4. 根据库存做偏移：
   - Up 库存多：降低 Up bid，提高 Down bid。
   - Down 库存多：降低 Down bid，提高 Up bid。
5. 报价不能吃单，必须低于当前 ask 一个 tick。
6. 单边库存达到上限后，不再继续报该边。

## 当前限制

当前版本还没有接入：

- 真实 Polymarket CLOB SDK
- 真实私钥签名
- 真实下单/撤单
- 真实 Polymarket 盘口 WS
- 真实 Binance/Chainlink 数据
- dashboard 页面

这些功能后面都应该围绕现有多进程结构继续接。

## 下一步建议

优先顺序：

1. 把 `collector` 从模拟行情换成真实 Polymarket WS + BTC 数据。
2. 把 `order-gateway` 接入 Polymarket SDK，但先保持小额 DRY_RUN 对照。
3. 加撤单/改单状态机：旧报价过期、盘口移动、库存变化时撤旧挂新。
4. 加 kill switch：最大亏损、最大库存、行情过期、WS 断连时停止报价。
5. 再做 dashboard。

不要把私钥写进仓库。真实私钥只放 VPS 的 `.env` 或系统 secret。
