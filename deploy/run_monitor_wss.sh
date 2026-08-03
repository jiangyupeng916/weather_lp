#!/usr/bin/env bash
# Monitor WSS 启动脚本 — 自动激活 venv
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/venv/bin/activate"
exec python "$DIR/monitor_wss.py" "$@"
