# Guardian V8 使用手册

> 适用版本：V8.0（polymarket-client SDK）  
> 服务器：217.60.38.228（Ubuntu，2vCPU / 4GB / 40GB）  
> 工作目录：`/root/weather_lp/guardian_v8/`  
> 最后更新：2026-08-02

---

## 目录

1. [首次部署（仅需执行一次）](#1-首次部署仅需执行一次)
2. [启动 Bot](#2-启动-bot)
3. [挂起到后台](#3-挂起到后台)
4. [查看运行状态](#4-查看运行状态)
5. [查看日志](#5-查看日志)
6. [正常关闭](#6-正常关闭)
7. [重启 Bot](#7-重启-bot)
8. [强制关闭（紧急）](#8-强制关闭紧急)
9. [更新代码](#9-更新代码)
10. [SSH 断线后恢复](#10-ssh-断线后恢复)
11. [常见问题](#11-常见问题)
12. [命令速查表](#12-命令速查表)

---

## 1. 首次部署（仅需执行一次）

### 1.1 传输凭据文件

在**本地 Windows PowerShell** 执行：

```powershell
scp D:\cursor\guardian\guardian_v7\.env.bot1 root@217.60.38.228:/root/weather_lp/guardian_v8/.env.bot1
```

### 1.2 在服务器上追加 V8 参数

```bash
echo "HEARTBEAT_MAX_ERRORS=5" >> /root/weather_lp/guardian_v8/.env.bot1
chmod 600 /root/weather_lp/guardian_v8/.env.bot1
```

### 1.3 克隆代码仓库

```bash
cd /root
git clone https://github.com/jiangyupeng916/weather_lp.git
```

### 1.4 安装依赖

```bash
cd /root/weather_lp/guardian_v8
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 1.5 验证 SDK 连通性

```bash
python test_sdk.py bot1
```

输出最后一行应为：`测试完成：下单 ✓  查询 ✓  撤单 ✓`

---

## 2. 启动 Bot

```bash
# SSH 登录服务器
ssh root@217.60.38.228

# 新建 screen 会话
screen -S bot1

# 进入目录并激活虚拟环境
cd /root/weather_lp/guardian_v8
source venv/bin/activate

# 启动
python main.py
```

**启动成功标志：**

```
==============================
Maker-only Guardian V8.0 启动
地址: 0x...
[HEARTBEAT] 已启动 interval=7.0s
[SCREENER] xxx markets in x.xs
==============================
```

---

## 3. 挂起到后台

Bot 启动并确认正常后，将其挂入后台（SSH 断线后 bot 继续运行）：

```
Ctrl+A  然后  D
```

看到 `[detached from 12345.bot1]` 说明挂起成功。

---

## 4. 查看运行状态

```bash
screen -ls
```

| 状态 | 含义 |
|------|------|
| `Detached` | ✅ Bot 在后台正常运行 |
| `Attached` | Bot 有人正在查看 |
| 无输出 | ❌ Bot 未运行 |

---

## 5. 查看日志

### 实时日志（推荐日常监控）

```bash
tail -f /root/weather_lp/guardian_v8/data/bot1/guardian.log
```

退出：按 `Ctrl+C`

### 最近 100 行

```bash
tail -n 100 /root/weather_lp/guardian_v8/data/bot1/guardian.log
```

### 只看错误和警告

```bash
grep -E "ERROR|WARNING|CRITICAL" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -50
```

### 只看心跳状态

```bash
grep "HEARTBEAT" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -20
```

### 进入 screen 直接看

```bash
screen -r bot1
```

查看完毕后挂回后台：`Ctrl+A` + `D`

---

## 6. 正常关闭

正常关闭会**先撤销所有挂单**，再停止程序。

```bash
# 进入 screen
screen -r bot1

# 发送停止信号
Ctrl+C
```

等待日志出现：

```
开始优雅关闭...
[CANCEL ALL] xx 已取消
[HEARTBEAT] 已停止
系统已停止
```

然后退出 screen：

```bash
exit
```

---

## 7. 重启 Bot

```bash
screen -r bot1
# Ctrl+C 等待优雅关闭

python main.py
# 确认正常后 Ctrl+A + D 挂起
```

---

## 8. 强制关闭（紧急）

> ⚠️ 强制关闭不会自动撤单！订单将继续挂在交易所，直到心跳超时后被交易所自动取消。  
> 仅在 bot 无响应时使用。

```bash
screen -S bot1 -X quit
```

或找到进程手动杀死：

```bash
ps aux | grep "python main.py"
kill -9 <PID>
```

---

## 9. 更新代码

本地改完代码后推送到 GitHub：

```powershell
# 本地 Windows PowerShell
cd D:\cursor\guardian\guardian_v7
git add .
git commit -m "描述改动"
git push
```

服务器拉取最新代码：

```bash
# 服务器
cd /root/weather_lp
git pull

# 如有新依赖
cd guardian_v8
source venv/bin/activate
pip install -r requirements.txt
```

⚠️ **注意**：`.env.bot1` 不在 git 里（含私钥），不会被 `git pull` 覆盖，无需担心。

---

## 10. SSH 断线后恢复

SSH 断线不影响 bot 运行（screen 保持后台）。重新连接后：

```bash
ssh root@217.60.38.228
screen -ls        # 确认 bot 还在运行
screen -r bot1    # 恢复查看
```

---

## 11. 常见问题

### Q: Bot 启动后立即退出？

检查配置文件：

```bash
ls -la /root/weather_lp/guardian_v8/.env.bot1
```

### Q: 大量日志显示"订单丢失纠偏"？

这是心跳失败后的正常恢复行为，不是 bug：

1. 心跳连续失败 → 交易所自动取消所有订单
2. 120 秒后 Audit 检测到订单消失 → 触发"纠偏"重新下单

检查心跳：

```bash
grep "HEARTBEAT" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -20
```

### Q: 启动时报 systemd guardian_v7 自动跑起来？

```bash
systemctl stop guardian_v7.service
systemctl disable guardian_v7.service
```

### Q: screener 速度变慢（从 8s 变成 35s+）？

北京时间 18:00-22:00 网络高峰期正常现象，部署在海外服务器后自动解决。

---

## 12. 命令速查表

| 操作 | 命令 |
|------|------|
| **SSH 登录** | `ssh root@217.60.38.228` |
| **启动** | `screen -S bot1` → `cd /root/weather_lp/guardian_v8 && source venv/bin/activate && python main.py` |
| **挂起后台** | `Ctrl+A` + `D` |
| **查看状态** | `screen -ls` |
| **进入查看** | `screen -r bot1` |
| **实时日志** | `tail -f /root/weather_lp/guardian_v8/data/bot1/guardian.log` |
| **正常关闭** | `screen -r bot1` → `Ctrl+C` → 等关闭 → `exit` |
| **强制关闭** | `screen -S bot1 -X quit` |
| **更新代码** | 本地 `git push` → 服务器 `cd /root/weather_lp && git pull` |
| **禁止V7自启** | `systemctl disable guardian_v7.service` |
