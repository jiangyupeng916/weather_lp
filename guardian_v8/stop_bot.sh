#!/bin/bash
# 优雅停止指定实例的 Guardian bot（等价于在 screen 里按 Ctrl+C）。
# 用途：定时停用交易时段（如北京时间 15:00-17:00 大额交易期）由 cron 调用。
#
# 发送 SIGTERM → guardian.py 的 _on_signal 置 self.running=False →
# 主循环退出 → _shutdown() 优雅撤单（撤掉所有挂单）→ 进程退出。
# 交易代码零改动，复用已验证的优雅关闭路径。
#
# 用法: ./stop_bot.sh [实例名]   实例名默认 bot1
set -u

INSTANCE="${1:-bot1}"
LOG_TAG="$(date '+%Y-%m-%d %H:%M:%S') [stop_bot $INSTANCE]"

# 精确匹配 "python main.py <instance>"，避免误杀其他实例（bot1 不匹配 bot10）
PID="$(pgrep -f "python main.py ${INSTANCE}\$" || true)"

if [ -z "$PID" ]; then
    echo "$LOG_TAG 未找到运行中的进程，跳过"
    exit 0
fi

echo "$LOG_TAG 发送 SIGTERM → PID=$PID（优雅撤单中）"
kill -TERM $PID

# 等待优雅关闭完成（最多 30s），确认撤单跑完
for i in $(seq 1 30); do
    if ! kill -0 $PID 2>/dev/null; then
        echo "$LOG_TAG 进程已退出（优雅关闭完成），耗时 ${i}s"
        # python 退出后 screen 会话常残留为空壳，主动关掉，避免误判“还在跑”
        # 并让 screen -ls 保持干净（start_bot 也会兜底清理）。
        screen -ls 2>/dev/null | grep "\.${INSTANCE}[[:space:]]" | awk '{print $1}' | while read -r sid; do
            screen -S "$sid" -X quit 2>/dev/null || true
        done
        screen -wipe >/dev/null 2>&1 || true
        exit 0
    fi
    sleep 1
done

echo "$LOG_TAG ⚠️ 30s 内未退出，可能撤单卡住。请手动检查（勿贸然 kill -9，挂单可能未撤）"
exit 1
