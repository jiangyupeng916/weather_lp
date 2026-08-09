#!/bin/bash
# 启动指定实例的 Guardian bot（在独立 screen 会话中后台运行）。
# 用途：定时停用交易时段结束后由 cron 调用重启。
#
# 查重判据 = python 进程（唯一真相）：
#   - python 真在跑 → 跳过（防同账户双进程：会引发订单冲突、重复撤单）。
#   - python 没在跑但残留同名 screen 空壳会话 → 先清掉再启动
#     （历史 bug：stop_bot 杀 python 后 screen 会话空转残留，
#      旧版按“会话名存在”查重会误判“已在跑”而跳过重启）。
#
# 用法: ./start_bot.sh [实例名]   实例名默认 bot1
set -u

INSTANCE="${1:-bot1}"
WORKDIR="/root/weather_lp/guardian_v8"
LOG_TAG="$(date '+%Y-%m-%d %H:%M:%S') [start_bot $INSTANCE]"

# 查重（唯一真相判据）：python 进程是否已在跑。
# 用本项目 venv 的完整路径做 pgrep，只匹配本项目的进程，不会误杀/误判其他项目。
if pgrep -f "${WORKDIR}/venv/bin/python.*main.py ${INSTANCE}\$" >/dev/null; then
    echo "$LOG_TAG python 进程已在运行，跳过启动（防双开）"
    exit 0
fi

# 到这里 = 没有 python 进程。若仍残留同名 screen 会话，即空壳，清掉再启动
# （否则新旧同名会话并存，screen -r ${INSTANCE} 会因多个匹配而混乱）。
if screen -list 2>/dev/null | grep -q "\.${INSTANCE}[[:space:]]"; then
    echo "$LOG_TAG 检测到无 python 进程的残留空壳会话，清理中"
    screen -ls 2>/dev/null | grep "\.${INSTANCE}[[:space:]]" | awk '{print $1}' | while read -r sid; do
        screen -S "$sid" -X quit 2>/dev/null || true
    done
    screen -wipe >/dev/null 2>&1 || true
fi

# 凭据文件存在性检查（防串号：缺凭据不如不启动，与 main.py 严格策略一致）
if [ ! -f "${WORKDIR}/.env.${INSTANCE}" ]; then
    echo "$LOG_TAG ⚠️ 凭据文件 .env.${INSTANCE} 不存在，拒绝启动"
    exit 1
fi

cd "$WORKDIR" || { echo "$LOG_TAG ⚠️ 无法进入 $WORKDIR"; exit 1; }
# 用 venv 完整路径启动，进程命令行里会带本项目路径前缀，pgrep 才能精准匹配
screen -dmS "$INSTANCE" bash -c "${WORKDIR}/venv/bin/python ${WORKDIR}/main.py ${INSTANCE}"
echo "$LOG_TAG 已在 screen 会话 '$INSTANCE' 中启动"
