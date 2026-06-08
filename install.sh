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
    echo "已创建默认配置: $ENV_FILE"
    return
  fi

  if [ ! -f "$ENV_FILE" ]; then
    return
  fi

  if section_has_missing ENABLE_REAL_ORDERS DATA_MODE AUTO_DISCOVER_MARKET POLYMARKET_GAMMA_API_URL MARKET_WINDOW_SECS MARKET_DISCOVERY_MS MARKET_SWITCH_GRACE_MS POLYMARKET_UP_TOKEN_ID POLYMARKET_DOWN_TOKEN_ID POLYMARKET_WS_URL POLYMARKET_USER_WS_URL BINANCE_WS_URL BINANCE_REST_URL POLYMARKET_CLOB_HOST POLY_CHAIN_ID POLY_PRIVATE_KEY POLY_API_KEY POLY_SECRET POLY_PASSPHRASE POLY_SIGNATURE_TYPE POLY_FUNDER_ADDRESS PRICE_TO_BEAT; then
    append_section_header "# ── 运行模式 / 真实行情 / 实单密钥（install.sh 自动补齐）──────────────"
    append_env_line ENABLE_REAL_ORDERS "# 默认模拟。实单必须同时设置 DRY_RUN=0 和 ENABLE_REAL_ORDERS=I_UNDERSTAND_REAL_MONEY。" "ENABLE_REAL_ORDERS="
    append_env_line DATA_MODE "# 行情来源：sim=本地模拟；live=Polymarket market WS + Binance BTC WS。" "DATA_MODE=sim"
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
    append_env_line POLYMARKET_CLOB_HOST "# 真实 CLOB 下单配置。不要把私钥提交到 GitHub。" "POLYMARKET_CLOB_HOST=https://clob-v2.polymarket.com"
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

  if [ "$ENV_BACKUP_DONE" -eq 1 ]; then
    echo "已保留原 .env，并补齐缺失字段。备份: $ENV_BACKUP_PATH"
  fi
}

ensure_env_defaults

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
