#!/usr/bin/env bash
# Limit Sell 启动脚本 — 自动激活 venv
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/venv/bin/activate"
exec python "$DIR/limit_sell.py" "$@"
