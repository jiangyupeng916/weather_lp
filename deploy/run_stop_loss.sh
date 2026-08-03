#!/usr/bin/env bash
# Stop Loss 启动脚本 — 自动激活 venv
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/venv/bin/activate"
exec python "$DIR/stop_loss.py" "$@"
