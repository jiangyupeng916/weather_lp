#!/bin/bash
# 启动指定实例的 Guardian bot（在独立 screen 会话中后台运行）。
# 用途：定时停用交易时段结束后（如北京时间 17:00）由 cron 调用重启。
#
# 内置查重：若同名 screen 会话已存在则跳过，防止 cron 重复启动导致同账户
# 双进程（会引发订单冲突、重复撤单）。
#
# 用法: ./start_bot.sh [实例名]   实例名默认 bot1
set -u

INSTANCE="${1:-bot1}"
WORKDIR="/root/weather_lp/guardian_v8"
LOG_TAG="$(date '+%Y-%m-%d %H:%M:%S') [start_bot $INSTANCE]"

# 查重 1：screen 会话是否已存在
if screen -list 2>/dev/null | grep -q "\.${INSTANCE}[[:space:]]"; then
    echo "$LOG_TAG screen 会话已存在，跳过启动"
    exit 0
fi

# 查重 2：进程是否已在跑（screen 之外的手动启动等）
if pgrep -f "python main.py ${INSTANCE}\$" >/dev/null; then
    echo "$LOG_TAG 进程已在运行，跳过启动"
    exit 0
fi

# 凭据文件存在性检查（防串号：缺凭据不如不启动，与 main.py 严格策略一致）
if [ ! -f "${WORKDIR}/.env.${INSTANCE}" ]; then
    echo "$LOG_TAG ⚠️ 凭据文件 .env.${INSTANCE} 不存在，拒绝启动"
    exit 1
fi

cd "$WORKDIR" || { echo "$LOG_TAG ⚠️ 无法进入 $WORKDIR"; exit 1; }
screen -dmS "$INSTANCE" bash -c "source venv/bin/activate && python main.py ${INSTANCE}"
echo "$LOG_TAG 已在 screen 会话 '$INSTANCE' 中启动"
