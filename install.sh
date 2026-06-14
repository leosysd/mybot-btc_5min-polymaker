#!/usr/bin/env bash
set -euo pipefail

REPO="${POLYMAKER_REPO:-leosysd/mybot-btc_5min-polymaker}"
TAG="${POLYMAKER_TAG:-latest}"
INSTALL_DIR="${POLYMAKER_INSTALL_DIR:-/opt/polymaker}"
BIN_LINK="${POLYMAKER_BIN_LINK:-/usr/local/bin/polymaker}"
ASSET="polymaker-linux-x86_64.tar.gz"
URL="https://github.com/${REPO}/releases/download/${TAG}/${ASSET}"

if [ "$(uname -s)" != "Linux" ]; then
  echo "当前一键安装脚本只支持 Linux/VPS。"
  exit 1
fi

case "$(uname -m)" in
  x86_64|amd64) ;;
  *)
    echo "当前 release 只提供 x86_64 Linux 二进制。"
    echo "你的架构是: $(uname -m)"
    exit 1
    ;;
esac

need_root() {
  if [ "$(id -u)" -ne 0 ]; then
    echo "请用 root 执行，或加 sudo："
    echo "  sudo bash install.sh"
    exit 1
  fi
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "缺少命令: $1"
    echo "Ubuntu 可先执行: apt update && apt install -y curl tar"
    exit 1
  fi
}

need_root
need_cmd curl
need_cmd tar
need_cmd sed

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "下载 ${URL}"
curl -fsSL "$URL" -o "$tmp/$ASSET"

mkdir -p "$INSTALL_DIR"
tar -xzf "$tmp/$ASSET" -C "$INSTALL_DIR"
chmod +x "$INSTALL_DIR/polymaker"
ln -sf "$INSTALL_DIR/polymaker" "$BIN_LINK"

ENV_FILE="$INSTALL_DIR/.env"
ENV_BACKUP_DONE=0
ENV_BACKUP_PATH=""

has_env_key() {
  local key="$1"
  grep -Eq "^[[:space:]]*${key}=" "$ENV_FILE"
}

backup_env_once() {
  if [ "$ENV_BACKUP_DONE" -eq 0 ]; then
    ENV_BACKUP_PATH="$ENV_FILE.bak.$(date +%Y%m%d%H%M%S)"
    cp "$ENV_FILE" "$ENV_BACKUP_PATH"
    ENV_BACKUP_DONE=1
  fi
}

section_has_missing() {
  local key
  for key in "$@"; do
    if ! has_env_key "$key"; then
      return 0
    fi
  done
  return 1
}

append_section_header() {
  backup_env_once
  printf "\n%s\n" "$1" >> "$ENV_FILE"
}

append_env_line() {
  local key="$1"
  local comment="$2"
  local line="$3"

  if ! has_env_key "$key"; then
    if [ -n "$comment" ]; then
      printf "%s\n" "$comment" >> "$ENV_FILE"
    fi
    printf "%s\n" "$line" >> "$ENV_FILE"
  fi
}

ensure_env_defaults() {
  if [ ! -f "$ENV_FILE" ] && [ -f "$INSTALL_DIR/.env.example" ]; then
    cp "$INSTALL_DIR/.env.example" "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    echo "已创建默认配置: $ENV_FILE"
    return
  fi

  if [ ! -f "$ENV_FILE" ]; then
    return
  fi
  chmod 600 "$ENV_FILE"

  if section_has_missing ENABLE_REAL_ORDERS DATA_MODE AUTO_DISCOVER_MARKET POLYMARKET_GAMMA_API_URL MARKET_WINDOW_SECS MARKET_DISCOVERY_MS MARKET_SWITCH_GRACE_MS POLYMARKET_UP_TOKEN_ID POLYMARKET_DOWN_TOKEN_ID POLYMARKET_WS_URL POLYMARKET_USER_WS_URL BINANCE_WS_URL BINANCE_REST_URL POLYMARKET_CLOB_HOST POLY_CHAIN_ID POLY_PRIVATE_KEY POLY_API_KEY POLY_SECRET POLY_PASSPHRASE POLY_SIGNATURE_TYPE POLY_FUNDER_ADDRESS PRICE_TO_BEAT LOG_ROTATE_MAX_MB LOG_ROTATE_KEEP; then
    append_section_header "# ── 运行模式 / 真实行情 / 实单密钥（install.sh 自动补齐）──────────────"
    append_env_line ENABLE_REAL_ORDERS "# 默认模拟。实单必须同时设置 DRY_RUN=0 和 ENABLE_REAL_ORDERS=I_UNDERSTAND_REAL_MONEY。" "ENABLE_REAL_ORDERS="
    append_env_line DATA_MODE "# 行情来源：sim=本地模拟；live=Polymarket market WS + Binance BTC WS。" "DATA_MODE=sim"
    append_env_line LOG_ROTATE_MAX_MB "# book/quotes/fills 单个 jsonl 超过多少 MB 自动轮转；0=关闭。" "LOG_ROTATE_MAX_MB=256"
    append_env_line LOG_ROTATE_KEEP "# 每个 jsonl 最多保留多少个旧轮转文件。" "LOG_ROTATE_KEEP=6"
    append_env_line AUTO_DISCOVER_MARKET "# 自动发现当前 5 分钟 BTC Up/Down 市场。开启后不需要手填 token，每轮自动切换。" "AUTO_DISCOVER_MARKET=1"
    append_env_line POLYMARKET_GAMMA_API_URL "" "POLYMARKET_GAMMA_API_URL=https://gamma-api.polymarket.com"
    append_env_line MARKET_WINDOW_SECS "" "MARKET_WINDOW_SECS=300"
    append_env_line MARKET_DISCOVERY_MS "" "MARKET_DISCOVERY_MS=2000"
    append_env_line MARKET_SWITCH_GRACE_MS "" "MARKET_SWITCH_GRACE_MS=90000"
    append_env_line POLYMARKET_UP_TOKEN_ID "# AUTO_DISCOVER_MARKET=0 时才手动填写当前 5 分钟市场的 Up/Down CLOB token id。" "POLYMARKET_UP_TOKEN_ID="
    append_env_line POLYMARKET_DOWN_TOKEN_ID "" "POLYMARKET_DOWN_TOKEN_ID="
    append_env_line POLYMARKET_WS_URL "" "POLYMARKET_WS_URL=wss://ws-subscriptions-clob.polymarket.com/ws/market"
    append_env_line POLYMARKET_USER_WS_URL "" "POLYMARKET_USER_WS_URL=wss://ws-subscriptions-clob.polymarket.com/ws/user"
    append_env_line BINANCE_WS_URL "" "BINANCE_WS_URL=wss://stream.binance.com:9443/ws/btcusdt@trade"
    append_env_line BINANCE_REST_URL "" "BINANCE_REST_URL=https://api.binance.com"
    append_env_line POLYMARKET_CLOB_HOST "# 真实 CLOB 下单配置。不要把私钥提交到 GitHub。" "POLYMARKET_CLOB_HOST=https://clob.polymarket.com"
    append_env_line POLY_CHAIN_ID "" "POLY_CHAIN_ID=137"
    append_env_line POLY_PRIVATE_KEY "" "POLY_PRIVATE_KEY="
    append_env_line POLY_API_KEY "" "POLY_API_KEY="
    append_env_line POLY_SECRET "" "POLY_SECRET="
    append_env_line POLY_PASSPHRASE "" "POLY_PASSPHRASE="
    append_env_line POLY_SIGNATURE_TYPE "# eoa / proxy / gnosis_safe / poly1271。多数 Polymarket 账户/代理钱包用 proxy。" "POLY_SIGNATURE_TYPE=proxy"
    append_env_line POLY_FUNDER_ADDRESS "" "POLY_FUNDER_ADDRESS="
    append_env_line PRICE_TO_BEAT "# AUTO_DISCOVER_MARKET=0 时手动填写判定价/开盘价；自动发现会用 Binance 开盘成交价兜底。" "PRICE_TO_BEAT=68000"
  fi

  if section_has_missing REQUOTE_THRESHOLD_TICKS; then
    append_section_header "# ── 撤单 / 改单（install.sh 自动补齐）────────────────────────────"
    append_env_line REQUOTE_THRESHOLD_TICKS "# 新报价和旧报价差多少 tick 才撤旧换新；0 表示每次都换。" "REQUOTE_THRESHOLD_TICKS=1"
  fi

  if section_has_missing MAX_LOSS MAX_TOTAL_INVENTORY LIVE_ORDER_NOTIONAL_CAP; then
    append_section_header "# ── Kill switch（install.sh 自动补齐）──────────────────────────"
    append_env_line MAX_LOSS "# 最坏结算情景亏损达到该值就停止。" "MAX_LOSS=25"
    append_env_line MAX_TOTAL_INVENTORY "# Up+Down 已成交+pending 总库存达到该值就停止。" "MAX_TOTAL_INVENTORY=50"
    append_env_line LIVE_ORDER_NOTIONAL_CAP "# 真实下单单笔名义金额上限。当前 DRY_RUN 也会按它截断模拟挂单。" "LIVE_ORDER_NOTIONAL_CAP=5"
  fi

  if section_has_missing WS_STALE_AFTER_MS; then
    append_section_header "# ── 进程节奏 / WS 断流保护（install.sh 自动补齐）────────────────"
    append_env_line WS_STALE_AFTER_MS "# live WS 超过多久没有更新就触发停止信号，毫秒。" "WS_STALE_AFTER_MS=10000"
  fi

  if section_has_missing VOL_SEED_PER_SQRT_SEC WIDTH_FLOOR_USD BASE_HALF_SPREAD MIN_LOCK_EDGE \
    LATENCY_SEC K_ADVERSE MIN_HALF_SPREAD MAX_HALF_SPREAD ENDGAME_REDUCE_SECS ENDGAME_PULL_SECS \
    INVENTORY_SKEW_TIME_BOOST TOX_HORIZON_MS TOX_DECAY TOX_K_WIDEN TOX_MAX_WIDEN \
    VOL_HALFLIFE_SEC VOL_MIN_PER_SQRT_SEC VOL_MAX_PER_SQRT_SEC ENABLE_DELTA_HEDGE HEDGE_VENUE; then
    append_section_header "# ── 策略大脑 v3：时间感知定价/波动率/残局/逆选择（install.sh 自动补齐）──"
    append_env_line VOL_SEED_PER_SQRT_SEC "# BTC 波动率初值(σ_$/√秒)，之后自适应。" "VOL_SEED_PER_SQRT_SEC=6"
    append_env_line VOL_HALFLIFE_SEC "# 波动率 EWMA 半衰期(秒)。" "VOL_HALFLIFE_SEC=20"
    append_env_line VOL_MIN_PER_SQRT_SEC "" "VOL_MIN_PER_SQRT_SEC=1.5"
    append_env_line VOL_MAX_PER_SQRT_SEC "" "VOL_MAX_PER_SQRT_SEC=60"
    append_env_line WIDTH_FLOOR_USD "# 收盘前不确定性 W 的下限，防 gamma 爆炸。" "WIDTH_FLOOR_USD=3"
    append_env_line BASE_HALF_SPREAD "# 基础半价差。" "BASE_HALF_SPREAD=0.012"
    append_env_line MIN_LOCK_EDGE "# 双边锁利下限 up_bid+down_bid<=1-该值。" "MIN_LOCK_EDGE=0.02"
    append_env_line LATENCY_SEC "# 撤改单延迟(秒)，驱动逆选择溢价。" "LATENCY_SEC=0.4"
    append_env_line K_ADVERSE "# 逆选择价差强度。" "K_ADVERSE=1"
    append_env_line MIN_HALF_SPREAD "" "MIN_HALF_SPREAD=0.005"
    append_env_line MAX_HALF_SPREAD "" "MAX_HALF_SPREAD=0.25"
    append_env_line ENDGAME_REDUCE_SECS "# 剩余秒数<该值只挂减仓单。" "ENDGAME_REDUCE_SECS=60"
    append_env_line ENDGAME_PULL_SECS "# 剩余秒数<该值撤掉所有单。" "ENDGAME_PULL_SECS=12"
    append_env_line INVENTORY_SKEW_TIME_BOOST "# 库存偏移随临近收盘加码倍数。" "INVENTORY_SKEW_TIME_BOOST=2"
    append_env_line TOX_HORIZON_MS "# 逆选择监控:成交后多久评估markout。" "TOX_HORIZON_MS=2500"
    append_env_line TOX_DECAY "" "TOX_DECAY=0.5"
    append_env_line TOX_K_WIDEN "" "TOX_K_WIDEN=1.5"
    append_env_line TOX_MAX_WIDEN "" "TOX_MAX_WIDEN=0.08"
    append_env_line ENABLE_DELTA_HEDGE "# 对冲为占位骨架，实单模式禁止开启，保持 0。" "ENABLE_DELTA_HEDGE=0"
    append_env_line HEDGE_VENUE "" "HEDGE_VENUE=binance_perp"
  fi

  if section_has_missing RECONCILE_INTERVAL_MS ENABLE_PREWARM PREWARM_INTERVAL_MS; then
    append_section_header "# ── 实盘对账 / 连接保活（install.sh 自动补齐）──────────────────"
    append_env_line RECONCILE_INTERVAL_MS "# 周期拉交易所真实挂单、撤孤儿单(毫秒)。0=关。" "RECONCILE_INTERVAL_MS=30000"
    append_env_line ENABLE_PREWARM "# 开新盘预热 token 缓存 + 周期 ping CLOB 保活热连接。" "ENABLE_PREWARM=1"
    append_env_line PREWARM_INTERVAL_MS "" "PREWARM_INTERVAL_MS=60000"
  fi

  if section_has_missing POST_ONLY_MARGIN_TICKS REJECT_BACKOFF_MS; then
    append_section_header "# ── 报价/拒单防护（install.sh 自动补齐）──────────────────────"
    append_env_line POST_ONLY_MARGIN_TICKS "# post-only 买价至少低于卖一价多少 tick，降低 crosses book 拒单。" "POST_ONLY_MARGIN_TICKS=2"
    append_env_line REJECT_BACKOFF_MS "# 同一边被拒后冷却毫秒数，避免空转刷 400 被限流。0=关。" "REJECT_BACKOFF_MS=500"
  fi

  if section_has_missing ENABLE_DIRECTION_EDGE DIRECTION_ALIGNED_EDGE DIRECTION_COUNTER_EDGE \
    ENABLE_MARKET_ANCHOR MARKET_ANCHOR_SHADOW MARKET_ANCHOR_WEIGHT \
    MARKET_ANCHOR_WEIGHT_HIGH MARKET_ANCHOR_WEIGHT_LOW MARKET_ANCHOR_LOW_SIDE_BELOW \
    MARKET_ANCHOR_MAX_SPREAD; then
    append_section_header "# ── 盘口锚定胜率（install.sh 自动补齐）──────────────────────"
    append_env_line ENABLE_DIRECTION_EDGE "# 1=按方向强弱调整 edge；不禁止任何一边，只改变成交难度。" "ENABLE_DIRECTION_EDGE=0"
    append_env_line DIRECTION_ALIGNED_EDGE "# 顺方向最低安全垫；小于 VALUE_MIN_EDGE 时顺方向更容易成交。" "DIRECTION_ALIGNED_EDGE=0.012"
    append_env_line DIRECTION_COUNTER_EDGE "# 反方向最低安全垫；大于 VALUE_MIN_EDGE 时反方向必须更便宜。" "DIRECTION_COUNTER_EDGE=0.04"
    append_env_line ENABLE_MARKET_ANCHOR "# 1=真实报价使用盘口胜率修正模型。" "ENABLE_MARKET_ANCHOR=0"
    append_env_line MARKET_ANCHOR_SHADOW "# 1=记录影子融合胜率，不改变报价。" "MARKET_ANCHOR_SHADOW=1"
    append_env_line MARKET_ANCHOR_WEIGHT "# 兼容默认权重；HIGH/LOW 未设置时使用。" "MARKET_ANCHOR_WEIGHT=0.30"
    append_env_line MARKET_ANCHOR_WEIGHT_HIGH "# 模型高胜率边盘口融合权重。" "MARKET_ANCHOR_WEIGHT_HIGH=0.30"
    append_env_line MARKET_ANCHOR_WEIGHT_LOW "# 模型低胜率对边盘口融合权重。" "MARKET_ANCHOR_WEIGHT_LOW=0.60"
    append_env_line MARKET_ANCHOR_LOW_SIDE_BELOW "# 低于该模型胜率的一边按 LOW 权重融合。" "MARKET_ANCHOR_LOW_SIDE_BELOW=0.50"
    append_env_line MARKET_ANCHOR_MAX_SPREAD "# 盘口价差超过该值时锚定权重归零。" "MARKET_ANCHOR_MAX_SPREAD=0.12"
  fi

  if section_has_missing MIN_FAIR_TO_QUOTE; then
    append_section_header "# ── 最低胜率门槛（install.sh 自动补齐）──────────────────────"
    append_env_line MIN_FAIR_TO_QUOTE "# 某边胜率低于此值就不报该边(不碰深度劣势方)。0=关;建议 0.25~0.35。" "MIN_FAIR_TO_QUOTE=0"
  fi

  if [ "$ENV_BACKUP_DONE" -eq 1 ]; then
    echo "已保留原 .env，并补齐缺失字段。备份: $ENV_BACKUP_PATH"
  fi
}

trim_spaces() {
  printf "%s" "$1" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

ensure_scheduled_env_defaults() {
  local active_env="$INSTALL_DIR/.env"
  if [ ! -f "$active_env" ]; then
    return
  fi

  local schedule_line
  schedule_line="$(grep -E "^[[:space:]]*ENV_SWITCH_SCHEDULE=" "$active_env" | tail -n 1 || true)"
  if [ -z "$schedule_line" ]; then
    return
  fi

  local schedule
  schedule="${schedule_line#*=}"
  schedule="${schedule%%#*}"
  if [ -z "$(trim_spaces "$schedule")" ]; then
    return
  fi

  local old_ifs="$IFS"
  IFS=';,'
  set -- $schedule
  IFS="$old_ifs"

  local original_env_file="$ENV_FILE"
  local original_backup_done="$ENV_BACKUP_DONE"
  local original_backup_path="$ENV_BACKUP_PATH"
  local part target
  for part in "$@"; do
    case "$part" in
      *=*) target="$(trim_spaces "${part#*=}")" ;;
      *) continue ;;
    esac
    [ -n "$target" ] || continue
    case "$target" in
      /*) ENV_FILE="$target" ;;
      *) ENV_FILE="$INSTALL_DIR/$target" ;;
    esac
    [ "$ENV_FILE" != "$active_env" ] || continue
    if [ -f "$ENV_FILE" ]; then
      ENV_BACKUP_DONE=0
      ENV_BACKUP_PATH=""
      ensure_env_defaults
    fi
  done

  ENV_FILE="$original_env_file"
  ENV_BACKUP_DONE="$original_backup_done"
  ENV_BACKUP_PATH="$original_backup_path"
}

ensure_env_defaults
ensure_scheduled_env_defaults

cat <<EOF

安装完成。

二进制:
  $BIN_LINK

项目目录:
  $INSTALL_DIR

下一步:
  cd $INSTALL_DIR
  polymaker menu

快速试跑:
  polymaker clean
  polymaker supervisor --seconds 15
  polymaker dashboard --seconds 10

注意:
  默认 DRY_RUN=1，不会真实下单。
  实单必须手动填写 .env，并同时设置 DRY_RUN=0 与 ENABLE_REAL_ORDERS=I_UNDERSTAND_REAL_MONEY。
EOF
