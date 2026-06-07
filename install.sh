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

if [ ! -f "$INSTALL_DIR/.env" ] && [ -f "$INSTALL_DIR/.env.example" ]; then
  cp "$INSTALL_DIR/.env.example" "$INSTALL_DIR/.env"
fi

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
  当前版本仍是 DRY_RUN 模拟骨架，不会真实下单。
EOF
