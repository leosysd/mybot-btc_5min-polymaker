# mybot-btc_5min-polymaker

这是一个 **Rust 多进程 BTC 5 分钟二元市场做市机器人骨架**。

当前版本默认 **DRY_RUN 模拟模式**，不会真实下单。已经支持 `DATA_MODE=live` 自动发现当前 BTC 5 分钟 Polymarket Up/Down 市场、自动切换 CLOB token、事件驱动读取真实盘口与 BTC 价格，也接入了官方 Rust CLOB SDK 的 post-only 买单/撤单路径和 Polymarket user WS 成交/取消回报。实单需要双重确认开关和小额限额。

## 架构

这个项目按低延迟实盘思路拆成 4 个子进程：

```text
collector      -> quote-engine -> order-gateway -> risk-ledger
行情/价格采集     报价决策        下单热路径         成交/库存/风控
```

各进程职责：

- `collector`：采集行情。`DATA_MODE=sim` 用本地模拟数据；`DATA_MODE=live` 自动发现当前 5 分钟市场，用 Polymarket market WS + BTC 价格源。live 模式收到盘口或 BTC tick 后立即推给 `quote-engine`，不再靠 `MARKET_INTERVAL_MS` 轮询发帧。
- `quote-engine`：计算 fair value、Up/Down 双边报价、库存偏移。
- `order-gateway`：下单热路径。DRY_RUN 时走模拟撮合；实单时走官方 Rust CLOB SDK 热连接，发 post-only BUY limit，并按状态机撤旧换新；填好 L2 API 后会连接 Polymarket user WS，把真实成交/取消即时回报给风控账本。
- `risk-ledger`：记录成交、库存、两种结算情景下的 PnL，并把库存反馈给报价引擎；同时执行 kill switch。
- `supervisor`：启动并守护上面 4 个进程。

热路径通信使用 Unix datagram socket：

```text
run/sockets/quote-engine.sock
run/sockets/order-gateway.sock
run/sockets/risk-ledger.sock
```

JSONL 文件只做审计日志，不做热路径通信；写入在后台线程异步完成，避免阻塞报价链路。

## VPS 安装

推荐用一键安装。GitHub Actions 会在 GitHub 云端编译 Linux 二进制，并发布到 `latest` Release；VPS 只下载成品，不需要在 VPS 上编译。

```bash
curl -fsSL https://raw.githubusercontent.com/leosysd/mybot-btc_5min-polymaker/main/install.sh | bash
```

如果 `/opt/polymaker/.env` 已经存在，一键安装不会覆盖你的配置；它只会追加缺失的新字段，并生成类似 `.env.bak.20260608123000` 的备份。私钥、API key 仍然只在 VPS 的 `.env` 里手动填写，不要提交到 GitHub。

安装完成后进入目录：

```bash
cd /opt/polymaker
polymaker menu
```

一键安装会把程序放到 `/opt/polymaker`，并创建 `/usr/local/bin/polymaker`。程序会自动把 `/opt/polymaker` 当作运行目录；如果你想换目录，可以设置：

```bash
export POLYMAKER_HOME=/你的/安装目录
```

如果你想从源码编译，再按下面步骤。

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

## 交互菜单

启动中文菜单：

```bash
polymaker
```

或者：

```bash
polymaker menu
```

菜单里可以做这些事：

- 初始化/修复 `.env` 配置
- 切换本地模拟、真实行情模拟、实单模式
- 填 Polymarket 私钥、签名类型、funder 地址和 L2 API 凭证
- 填当前 5 分钟市场 Up/Down token id 与判定价
- 调整做市和风控参数
- 查看当前状态
- 打开交易监控页
- 试跑 15 秒模拟做市
- 后台启动服务
- 停止服务
- 重启服务
- 清空运行数据
- 查看参数说明

## 交易监控页

打开交易页面：

```bash
polymaker dashboard
```

只看 10 秒：

```bash
polymaker dashboard --seconds 10
```

交易页会显示：

- 进程心跳
- 当前库存
- Up 赢 / Down 赢两种情景 PnL
- 最近行情
- 最近报价
- 最近模拟成交

## 快速试跑

先清理旧运行目录：

```bash
polymaker clean
```

运行 15 秒模拟做市：

```bash
polymaker supervisor --seconds 15
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

## DRY_RUN 模拟怎么跑

当前模拟不是在真实 Polymarket 下单，而是在本地构造一套“像 5 分钟 BTC 二元市场”的小环境：

1. `collector` 生成一条假的 BTC 价格曲线，围绕 `price_to_beat` 上下波动。
2. 程序用 `normal_cdf((btc_price - price_to_beat) / BTC_SIGMA_USD)` 估算 `Up` 的 fair value。
3. `collector` 再生成模拟盘口 `UpAsk` / `DnAsk`，让盘口围绕 fair value 带一点价差和噪声。
4. `quote-engine` 只挂买单，不主动吃单；它会按 fair value 减去做市价差，并保证报价低于当前 ask 一个 tick。
5. `order-gateway` 按 `SIM_FILL_CHANCE` 随机把部分报价记为模拟成交。
6. `risk-ledger` 记录库存和两种结算情景的 PnL：`Up赢PnL` / `Dn赢PnL`。

所以监控页里的 PnL 是“如果现在这一轮最终结算为 Up/Down，会赚或亏多少”，不是已经真实赚到的钱。

## 怎么判断能不能赚钱

二元市场里，做市赚钱主要看两件事：

```text
买入 Up 的成本 + 买入 Down 的成本 < 1
```

如果两边都买到了，而且总成本低于 `1`，那么最终不管 Up 还是 Down，其中一边会兑付 `1`，差额就是锁住的毛利润。例如：

```text
Up 买 5 份，价格 0.45
Down 买 5 份，价格 0.50
总成本 = 5 * 0.45 + 5 * 0.50 = 4.75
结算收入 = 5
毛利润 = 0.25
```

如果只成交了一边，那就不是锁利，而是方向风险：买了 Up，最后 Up 才赚钱；买了 Down，最后 Down 才赚钱。机器人要靠 fair value 判断、价差、库存偏移和撤单速度，把成交尽量留在“买得便宜”的位置。

当前模拟容易出现盈利，是因为模拟盘口和随机成交比较温和，主要用于验证架构、风控和 PnL 计算。它不能证明实盘一定赚钱。真正能不能赚钱，要接真实 Polymarket 盘口后重点看：

- 你的报价是否排得进队列并实际成交。
- 成交是不是经常发生在你被价格变化打穿的时候。
- `up_bid + down_bid` 是否长期低于 `1`，并覆盖手续费、滑点和坏成交。
- 单边库存是否会越积越大。
- 5 分钟最后几十秒是否需要更激进地撤单或锁仓。

停止所有进程：

```bash
polymaker stop
```

## 常用命令

查看帮助：

```bash
polymaker help
```

单独启动报价引擎：

```bash
polymaker quote-engine
```

单独启动下单网关：

```bash
polymaker order-gateway
```

单独启动风控账本：

```bash
polymaker risk-ledger
```

单独启动行情采集：

```bash
polymaker collector
```

启动完整多进程：

```bash
polymaker supervisor
```

后台启动完整服务：

```bash
polymaker start
```

重启后台服务：

```bash
polymaker restart
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
polymaker stop
```

## 配置说明

配置文件是 `.env`，从 `.env.example` 复制出来。

核心参数：

```text
DRY_RUN=1
```

默认保持 `1`，只模拟。要实单必须同时满足：

```text
DRY_RUN=0
ENABLE_REAL_ORDERS=I_UNDERSTAND_REAL_MONEY
DATA_MODE=live
LIVE_ORDER_NOTIONAL_CAP=5
```

实单只发 post-only BUY limit，不主动吃单；`LIVE_ORDER_NOTIONAL_CAP` 必须在 `(0, 25]`，先小额验证。

```text
DATA_MODE=sim
```

行情来源：

- `sim`：本地模拟行情，适合测试架构和风控。
- `live`：自动发现当前 5 分钟 Polymarket 市场，连接真实 Polymarket market WebSocket 和 BTC 价格源。

live 模式默认自动发现，不需要手填当前市场 token：

```text
AUTO_DISCOVER_MARKET=1
POLYMARKET_GAMMA_API_URL=https://gamma-api.polymarket.com
MARKET_WINDOW_SECS=300
```

`collector` 会按 `btc-updown-5m-{窗口开始unix秒}` 查询 Gamma API，解析当前市场的 `clobTokenIds`，并把 token id 放进每条 quote。开盘价优先取交易所窗口开始附近成交价；如果 Binance 主站不可用，会 fallback 到 Binance.US、Coinbase、Kraken 的 BTC/USD 最新价。注意 Polymarket 结算源仍是 Chainlink BTC/USD Data Stream，外部交易所价格只是模型输入和兜底。

如果你一定要手动覆盖，才设置：

```text
AUTO_DISCOVER_MARKET=0
POLYMARKET_UP_TOKEN_ID=
POLYMARKET_DOWN_TOKEN_ID=
POLYMARKET_USER_WS_URL=wss://ws-subscriptions-clob.polymarket.com/ws/user
PRICE_TO_BEAT=68000
```

实单还需要填 Polymarket 账户信息。可以直接在 `polymaker menu` 的 `2. 切换模拟/实单 + 填 Polymarket 账户` 里输入，不需要手动翻 `.env`：

```text
POLY_PRIVATE_KEY=
POLY_SIGNATURE_TYPE=proxy
POLY_FUNDER_ADDRESS=
```

如果你已经有 L2 API 凭证，也可以填：

```text
POLY_API_KEY=
POLY_SECRET=
POLY_PASSPHRASE=
```

不填 L2 凭证时，SDK 会用私钥创建或派生 API key。实单进程会复用这组已认证凭证连接 Polymarket user WS，所以真实成交/取消回报不要求你额外手填 L2。不要把 `.env` 或私钥提交到 GitHub。

菜单 2 的三个常用切换：

- 本地模拟：`DATA_MODE=sim`、`DRY_RUN=1`，完全不下单。
- 真实行情模拟：`DATA_MODE=live`、`AUTO_DISCOVER_MARKET=1`、`DRY_RUN=1`，自动切市场但不下单。
- 实单模式：`DATA_MODE=live`、`AUTO_DISCOVER_MARKET=1`、`DRY_RUN=0`、`ENABLE_REAL_ORDERS=I_UNDERSTAND_REAL_MONEY`，自动切市场并走 Polymarket CLOB SDK 下单。

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
QUOTE_TTL_MS=1500
```

未成交报价在风控里保留多久。报价发给 `order-gateway` 后，会先算进 pending 库存；如果没有成交，超过 TTL 后释放。

```text
REQUOTE_THRESHOLD_TICKS=1
```

撤单/改单阈值。新报价和旧报价差距达到这个 tick 数时，`order-gateway` 会撤旧换新；没达到则保留旧挂单，减少无意义撤单。

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

例如 `QUOTE_SIZE=5`、`INVENTORY_MULT=2`，单边最多买 `10` 份。这里会同时计算已成交库存和 pending 未成交报价，避免连续报价把真实仓位打穿。

```text
MIN_BID=0.05
MAX_BID=0.62
```

最低/最高挂买价格。避免挂太低的垃圾单，也避免追贵。

```text
MARKET_INTERVAL_MS=120
```

模拟 collector 产生行情的间隔。`DATA_MODE=live` 时行情由 Polymarket/BTC WS 事件触发，这个参数不再控制 live 报价节奏。

```text
STALE_AFTER_MS=800
```

行情超过这个时间没更新，quote-engine 会跳过，避免用旧行情报价。

```text
WS_STALE_AFTER_MS=10000
MAX_LOSS=25
MAX_TOTAL_INVENTORY=50
LIVE_ORDER_NOTIONAL_CAP=5
```

安全阈值：

- `WS_STALE_AFTER_MS`：live WS 断流超过阈值，写入停止信号。
- `MAX_LOSS`：最坏结算情景亏损达到阈值，停止服务。
- `MAX_TOTAL_INVENTORY`：Up+Down 已成交+pending 总库存达到阈值，停止服务。
- `LIVE_ORDER_NOTIONAL_CAP`：真实下单单笔名义金额上限；当前 DRY_RUN 也会按它截断模拟挂单。

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

当前版本还没有接入 Web dashboard 页面；命令行 dashboard 已可用。

已经接入/优化：

- 自动发现当前 5 分钟 BTC Up/Down 市场，并自动切换 CLOB token。
- `DATA_MODE=live` 真实 Polymarket market WS + BTC WS 行情，盘口/BTC 更新后直接推给 `quote-engine`。
- 官方 Rust CLOB SDK post-only 下单/撤单。
- Polymarket user WS 真实订单成交/取消回报。
- DRY_RUN/实单共用撤单/改单状态机。
- pending 库存风控。
- kill switch：最大亏损、最大库存、行情过期、WS 断流。
- JSONL 后台异步写入。
- CLI dashboard 对齐显示。

## 下一步建议

优先顺序：

1. 用 Chainlink Data Streams 或更贴近结算源的数据替换当前交易所 BTC 价格兜底。
2. 做 Web dashboard 页面。
3. 长时间小额验证后再考虑提高 `LIVE_ORDER_NOTIONAL_CAP`。

不要把私钥写进仓库。真实私钥只放 VPS 的 `.env` 或系统 secret。
