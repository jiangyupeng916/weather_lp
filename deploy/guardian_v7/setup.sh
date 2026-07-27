#!/usr/bin/env bash
# ============================================================
# Guardian V7 服务器部署脚本
# 用法: chmod +x setup.sh && ./setup.sh
#
# 将整个项目目录上传到服务器后，在项目根目录执行此脚本。
# 项目结构:
#   guardian_v7/
#   ├── main.py  config.py  guardian.py  models.py  utils.py
#   ├── heartbeat.py  execution.py  ws_manager.py  ws_router.py
#   ├── screener/
#   ├── deploy/guardian_v7/setup.sh  (本文件)
#   ├── .env
#   └── data/
# ============================================================
set -e

# ── 确定路径 ──
# 本脚本在 deploy/guardian_v7/ 下, 项目根目录是 ../../
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "============================================"
echo "  Guardian V7 — 服务器部署"
echo "============================================"
echo ""
echo "  项目目录: $PROJECT_DIR"
echo ""

# ── 1. 检查 Python ──
if ! command -v python3 &>/dev/null; then
    echo "[ERR] 未找到 python3，请先安装 Python 3.10+"
    exit 1
fi
echo "[OK] python3: $(python3 --version)"

# ── 2. 安装 python3-venv ──
PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "[*] 安装 python${PY_VER}-venv ..."
sudo apt update -qq && sudo apt install "python${PY_VER}-venv" -y

# ── 3. 创建虚拟环境 ──
VENV_DIR="$PROJECT_DIR/venv"
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

# Guardian V7 Core 依赖
pip install py_clob_client_v2 eth-account requests python-dotenv websocket-client

# 额外依赖 (筛选器也只需要 requests, 已安装)
echo "[OK] 依赖安装完成"

# ── 5. 创建日志目录 ──
mkdir -p "$PROJECT_DIR/data"
mkdir -p "$PROJECT_DIR/logs"

# ── 6. 检查 .env ──
if [ ! -f "$PROJECT_DIR/.env" ]; then
    echo ""
    echo "⚠ 未找到 .env 文件！"
    echo "  请创建 $PROJECT_DIR/.env 并填入以下必填字段:"
    echo "    PK=0x..."
    echo "    CLOB_API_KEY=..."
    echo "    CLOB_SECRET=..."
    echo "    CLOB_PASS_PHRASE=..."
    echo "    PROXY_ADDRESS=0x..."
    echo "  其他 SCREENER_* 参数有默认值，可选配置。"
    echo ""
fi

# ── 7. 生成 run.sh ──
cat > "$PROJECT_DIR/run.sh" << 'RUNEOF'
#!/usr/bin/env bash
# Guardian V7 启动脚本 — 自动激活 venv
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/venv/bin/activate"
cd "$DIR"
exec python main.py "$@"
RUNEOF
chmod +x "$PROJECT_DIR/run.sh"
echo "[OK] run.sh 已生成"

# ── 8. 生成 systemd service ──
cat > "$SCRIPT_DIR/guardian_v7.service" << SVCEOF
[Unit]
Description=Guardian V7 - Polymarket Market Maker
After=network.target

[Service]
Type=simple
WorkingDirectory=$PROJECT_DIR
ExecStart=$PROJECT_DIR/run.sh
Restart=always
RestartSec=10
StandardOutput=append:$PROJECT_DIR/logs/guardian.log
StandardError=append:$PROJECT_DIR/logs/guardian.log

[Install]
WantedBy=multi-user.target
SVCEOF
echo "[OK] guardian_v7.service 已生成"

# ── 9. 生成 logrotate 配置 ──
cat > "$SCRIPT_DIR/logrotate.conf" << LREOF
$PROJECT_DIR/logs/*.log
$PROJECT_DIR/data/*.log {
    daily
    rotate 7
    compress
    missingok
    notifempty
    copytruncate
    size 10M
}
LREOF
echo "[OK] logrotate.conf 已生成"

# ── 10. 打印当前配置 ──
echo ""
echo "============================================"
echo "  部署完成！"
echo "============================================"
echo ""

source "$VENV_DIR/bin/activate"
python -c "
import os, sys
sys.path.insert(0, '$PROJECT_DIR')
os.chdir('$PROJECT_DIR')
from config import Config
cfg = Config()
print('当前筛选器配置:')
for k, v in sorted(vars(cfg).items()):
    if k.startswith('screener_'):
        print(f'  {k} = {v}')
print()
print(f'Maker 参数: size={cfg.maker_size} rank={cfg.maker_rank} cooldown={cfg.maker_cooldown}s')
print(f'定时任务: poll={cfg.best_bid_poll_interval}s audit={cfg.audit_interval}s')
" 2>/dev/null || echo "  (无法读取配置, 检查 .env 或 config.py)"

echo ""
echo "──── 启动命令 ────────────────────────────────────────"
echo ""
echo "  # 前台测试运行"
echo "  ./run.sh"
echo ""
echo "  # 后台常驻 (nohup)"
echo "  nohup ./run.sh > logs/guardian.log 2>&1 &"
echo ""
echo "  # 查看日志"
echo "  tail -f logs/guardian.log"
echo "  tail -f data/guardian.log"
echo "  tail -f data/trades.log"
echo ""
echo "  # 查看筛选结果 CSV"
echo "  cat data/screener_latest.csv"
echo ""
echo "  # 停止"
echo "  pkill -f main.py"
echo ""
echo "──── systemd 服务 (推荐) ─────────────────────────────"
echo ""
echo "  # 一次性安装 (需 sudo)"
echo "  sudo cp deploy/guardian_v7/guardian_v7.service /etc/systemd/system/"
echo "  sudo cp deploy/guardian_v7/logrotate.conf /etc/logrotate.d/guardian_v7"
echo "  sudo systemctl daemon-reload"
echo "  sudo systemctl enable --now guardian_v7"
echo ""
echo "  # 日常运维"
echo "  sudo systemctl status guardian_v7"
echo "  sudo systemctl restart guardian_v7"
echo "  sudo systemctl stop guardian_v7"
echo "  tail -f logs/guardian.log"
echo ""
echo "  # 查看 systemd 日志"
echo "  sudo journalctl -u guardian_v7 -f"
echo "  sudo journalctl -u guardian_v7 --since '1 hour ago'"
echo ""
echo "──── 更新代码 ────────────────────────────────────────"
echo ""
echo "  sudo systemctl stop guardian_v7"
echo "  # 上传新代码覆盖项目目录"
echo "  ./run.sh                    # 测试运行, Ctrl+C 退出"
echo "  sudo systemctl start guardian_v7"
echo ""
echo "──── 调整筛选器参数 ──────────────────────────────────"
echo ""
echo "  编辑 .env 文件中的 SCREENER_* 参数, 重启服务即可:"
echo "  sudo systemctl restart guardian_v7"
echo ""
echo "请确保 .env 文件已配置正确的凭证。"
echo "============================================"
