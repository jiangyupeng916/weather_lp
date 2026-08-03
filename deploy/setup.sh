#!/usr/bin/env bash
# Weather Guard 服务器部署脚本
# 用法: chmod +x setup.sh && ./setup.sh
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$DIR/venv"

echo "============================================"
echo "  Weather Neg Risk Guard — 服务器部署"
echo "============================================"

# ── 1. 检查 Python ──
if ! command -v python3 &>/dev/null; then
    echo "[ERR] 未找到 python3，请先安装 Python 3.8+"
    exit 1
fi
echo "[OK] python3: $(python3 --version)"

# ── 2. 安装 python3-venv ──
PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "[*] 安装 python${PY_VER}-venv ..."
apt update -qq && apt install "python${PY_VER}-venv" -y

# ── 3. 创建虚拟环境 ──
if [ -d "$VENV_DIR" ]; then
    echo "[*] venv 已存在，删除重建 ..."
    rm -rf "$VENV_DIR"
fi
echo "[*] 创建 venv ..."
python3 -m venv "$VENV_DIR"

# ── 4. 安装依赖 ──
echo "[*] 安装依赖 ..."
source "$VENV_DIR/bin/activate"
pip install --upgrade pip -q
# weather_guard / stop_loss / limit_sell 依赖
pip install py-clob-client eth-account requests
# monitor_wss 依赖 (WSS 实时监控 + Neg Risk 套利, py_clob_client_v2 是本地包不需 pip)
pip install orjson websockets "httpx[http2]" poly_eip712_structs py_order_utils \
            web3 py_builder_relayer_client py_builder_signing_sdk
echo "[OK] 依赖安装完成"

# ── 5. 创建日志目录 ──
mkdir -p "$DIR/logs"

# ── 6. 生成 run.sh ──
cat > "$DIR/run.sh" << 'RUNEOF'
#!/usr/bin/env bash
# Weather Guard 启动脚本 — 自动激活 venv
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/venv/bin/activate"
exec python "$DIR/weather_guard.py" "$@"
RUNEOF
chmod +x "$DIR/run.sh"

# ── 7. 生成 run_stop_loss.sh ──
cat > "$DIR/run_stop_loss.sh" << 'RUNEOF'
#!/usr/bin/env bash
# Stop Loss 启动脚本 — 自动激活 venv
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/venv/bin/activate"
exec python "$DIR/stop_loss.py" "$@"
RUNEOF
chmod +x "$DIR/run_stop_loss.sh"

# ── 8. 生成 run_limit_sell.sh ──
cat > "$DIR/run_limit_sell.sh" << 'RUNEOF'
#!/usr/bin/env bash
# Limit Sell 启动脚本 — 自动激活 venv
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/venv/bin/activate"
exec python "$DIR/limit_sell.py" "$@"
RUNEOF
chmod +x "$DIR/run_limit_sell.sh"

# ── 8.5. 生成 run_monitor_wss.sh ──
cat > "$DIR/run_monitor_wss.sh" << 'RUNEOF'
#!/usr/bin/env bash
# Monitor WSS 启动脚本 — 自动激活 venv
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/venv/bin/activate"
exec python "$DIR/monitor_wss.py" "$@"
RUNEOF
chmod +x "$DIR/run_monitor_wss.sh"

# ── 9. 生成 systemd service + logrotate 配置 ──
mkdir -p "$DIR/logs"

cat > "$DIR/weather_guard.service" << SVCEOF
[Unit]
Description=Weather Neg Risk Guard
After=network.target

[Service]
Type=simple
WorkingDirectory=$DIR
ExecStart=$DIR/run.sh --live
Restart=always
RestartSec=10
StandardOutput=append:$DIR/logs/weather_guard.log
StandardError=append:$DIR/logs/weather_guard.log

[Install]
WantedBy=multi-user.target
SVCEOF

cat > "$DIR/limit_sell.service" << SVCEOF
[Unit]
Description=Limit Sell - 0.999 挂卖
After=network.target

[Service]
Type=simple
WorkingDirectory=$DIR
ExecStart=$DIR/run_limit_sell.sh --live --interval 10
Restart=always
RestartSec=10
StandardOutput=append:$DIR/logs/limit_sell.log
StandardError=append:$DIR/logs/limit_sell.log

[Install]
WantedBy=multi-user.target
SVCEOF

cat > "$DIR/monitor_wss.service" << SVCEOF
[Unit]
Description=Polymarket Neg Risk WSS Monitor
After=network.target

[Service]
Type=simple
WorkingDirectory=$DIR
ExecStart=$DIR/run_monitor_wss.sh
Restart=always
RestartSec=10
StandardOutput=append:$DIR/logs/monitor_wss.log
StandardError=append:$DIR/logs/monitor_wss.log

[Install]
WantedBy=multi-user.target
SVCEOF

cat > "$DIR/logrotate.conf" << LREOF
$DIR/logs/*.log {
    daily
    rotate 7
    compress
    missingok
    notifempty
    copytruncate
    size 10M
}
LREOF

echo ""
echo "============================================"
echo "  部署完成！"
echo "============================================"
echo ""
echo "当前参数:"
source "$VENV_DIR/bin/activate"
python -c "
with open('$DIR/weather_guard.py', encoding='utf-8') as f:
    for line in f:
        for key in ['LIVE_MODE', 'INTERVAL_MINUTES', 'EVENT_MIN_YES_BID',
                     'MARKET_BID_MIN', 'MARKET_BID_MAX', 'ORDER_SIZE']:
            if line.startswith(key):
                val = line.split('=')[1].split('#')[0].strip()
                print(f'  {key} = {val}')
                break
"
echo ""
echo "启动命令 (run.sh 自动激活 venv):"
echo ""
echo "  # dry-run 测试"
echo "  ./run.sh --dry-run"
echo ""
echo "  # 实盘下单（后台常驻）"
echo "  nohup ./run.sh --live > logs/weather_guard.log 2>&1 &"
echo ""
echo "  # 查看日志"
echo "  tail -f logs/weather_guard.log"
echo ""
echo "  # 停止"
echo "  pkill -f weather_guard.py"
echo ""
echo "── Stop Loss 止损监控 ──"
echo "  # dry-run 测试"
echo "  ./run_stop_loss.sh --dry-run"
echo ""
echo "  # 后台持续止损监控"
echo "  nohup ./run_stop_loss.sh --live --interval 5 > logs/stop_loss.log 2>&1 &"
echo ""
echo "  # 停止"
echo "  pkill -f stop_loss.py"
echo ""
echo "── systemd 服务（推荐，崩溃自动重启 + 日志轮转）──"
echo "  # 一次性安装（需 sudo）"
echo "  sudo cp weather_guard.service limit_sell.service monitor_wss.service /etc/systemd/system/"
echo "  sudo cp logrotate.conf /etc/logrotate.d/polymarket"
echo "  sudo systemctl daemon-reload"
echo "  sudo systemctl enable --now weather_guard limit_sell monitor_wss"
echo ""
echo "  # 日常运维"
echo "  sudo systemctl status weather_guard"
echo "  sudo systemctl restart weather_guard"
echo "  tail -f logs/weather_guard.log"
echo ""
echo "── Monitor WSS — Neg Risk 套利实时监控 ──"
echo "  # 修改 monitor_wss.py 顶部配置区 (TAG_ID / LIVE / MAX_AMOUNT 等) 后上传"
echo ""
echo "  # dry-run 测试 (LIVE=False)"
echo "  ./run_monitor_wss.sh"
echo ""
echo "  # 实盘 (改 monitor_wss.py LIVE=True 后)"
echo "  nohup ./run_monitor_wss.sh > logs/monitor_wss.log 2>&1 &"
echo ""
echo "  # systemd 方式 (改 LIVE=True 后)"
echo "  sudo systemctl start monitor_wss"
echo "  sudo systemctl status monitor_wss"
echo "  tail -f logs/monitor_wss.log"
echo ""
echo "请确保 .env 文件已配置正确的凭证。"
