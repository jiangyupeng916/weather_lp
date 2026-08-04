# Guardian V8 使用手册

> **版本**：V8.1  
> **服务器**：217.60.38.228（Ubuntu，2vCPU / 4GB / 40GB）  
> **工作目录**：`/root/weather_lp/guardian_v8/`  
> **最后更新**：2026-08-04

---

## 目录

1. [环境要求](#1-环境要求)
2. [首次部署](#2-首次部署)
3. [启动与托管](#3-启动与托管)
4. [日常监控](#4-日常监控)
5. [正常关闭与重启](#5-正常关闭与重启)
6. [代码更新](#6-代码更新)
7. [故障排查](#7-故障排查)
8. [强制关闭（紧急）](#8-强制关闭紧急)
9. [SSH 断线后恢复](#9-ssh-断线后恢复)
10. [命令速查表](#10-命令速查表)

---

## 1. 环境要求

- **Python**：3.12+
- **操作系统**：Linux/Ubuntu（推荐服务器环境）
- **工具**：Git（代码更新）、screen / tmux（后台托管）
- **网络**：稳定的互联网连接（访问 Polymarket CLOB API）

---

## 2. 首次部署

### 2.1 服务器准备

```bash
# 安装依赖
sudo apt update && sudo apt install -y python3 python3-venv git screen

# 克隆仓库
cd /root
git clone https://github.com/jiangyupeng916/weather_lp.git
cd weather_lp/guardian_v8
```

### 2.2 凭据配置

**方式一：本地传输**（Windows PowerShell）

```powershell
scp D:\cursor\guardian\guardian_v7\.env.bot1 root@217.60.38.228:/root/weather_lp/guardian_v8/
```

**方式二：服务器直接创建**

```bash
nano /root/weather_lp/guardian_v8/.env.bot1
# 填入以下必需参数：
# PRIVATE_KEY=0x...        # 账户私钥
# POLY_ADDRESS=0x...       # 钱包地址
# POLY_PASSPHRASE=...      # API passphrase
# POLY_API_KEY=...         # API key
# POLY_API_SECRET=...      # API secret
# HEARTBEAT_MAX_ERRORS=5   # 心跳最大失败次数

chmod 600 .env.bot1
```

**安全提示**：`.env.*` 文件不在 git 中（已加入 `.gitignore`），不会被 `git pull` 覆盖。

### 2.3 安装 Python 依赖

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2.4 验证 SDK 连通性

```bash
python test_sdk.py bot1
```

**预期输出**：`测试完成：下单 ✓  查询 ✓  撤单 ✓`

---

## 3. 启动与托管

### 3.1 前台启动（首次测试）

```bash
cd /root/weather_lp/guardian_v8
source venv/bin/activate
python main.py
```

**启动成功标志**：

```
==============================
Maker-only Guardian V8.0 启动
地址: 0x...
[HEARTBEAT] 已启动 interval=7.0s
[SCREENER] xxx markets in x.xs
==============================
```

### 3.2 挂后台（screen 托管）

```bash
# 新建 screen 会话
screen -S bot1

# 启动 bot
cd /root/weather_lp/guardian_v8 && source venv/bin/activate && python main.py

# 挂后台（Ctrl+A 然后 D）
# 看到 [detached from 12345.bot1] 说明成功
```

### 3.3 查看状态

```bash
screen -ls
```

| 状态 | 含义 |
|------|------|
| `Detached` | ✅ Bot 在后台正常运行 |
| `Attached` | Bot 有人正在查看 |
| 无输出 | ❌ Bot 未运行 |

---

## 4. 日常监控

### 4.1 实时日志

```bash
tail -f /root/weather_lp/guardian_v8/data/bot1/guardian.log
```

退出：按 `Ctrl+C`

### 4.2 专项检查

#### 错误和警告

```bash
grep -E "ERROR|WARNING|CRITICAL" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -50
```

#### 心跳状态

```bash
grep "HEARTBEAT" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -20
```

#### WebSocket 可用率（新增）

```bash
grep "WS STATS" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -10
```

**预期输出**：
```
[WS STATS] 可用率 98.3% | 运行 1200.0s | 断线 2 次 | 累计断线 20.5s
```

#### WebSocket 实时撤单（新增）

```bash
grep "WS bid变化" /root/weather_lp/guardian_v8/data/bot1/guardian.log | wc -l
```

**预期**：数十到数百条（说明 WS 实时监控生效）

#### 卖出触发（新增）

```bash
grep "SELL-TRIGGER" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -20
```

### 4.3 进入 screen 查看

```bash
screen -r bot1
# 查看完后 Ctrl+A + D 挂回后台
```

### 4.4 最近 100 行

```bash
tail -n 100 /root/weather_lp/guardian_v8/data/bot1/guardian.log
```

---

## 5. 正常关闭与重启

### 5.1 正常关闭（优雅撤单）

正常关闭会**先撤销所有挂单**，再停止程序。

```bash
# 进入 screen
screen -r bot1

# 发送停止信号（在 screen 内）
Ctrl+C

# 等待日志出现：
# 开始优雅关闭...
# [CANCEL ALL] xx 已取消
# [HEARTBEAT] 已停止
# 系统已停止

# 退出 screen
exit
```

### 5.2 重启

```bash
screen -r bot1
# Ctrl+C 等待优雅关闭

python main.py
# 确认正常后 Ctrl+A + D 挂后台
```

---

## 6. 代码更新

### 6.1 本地推送（Windows PowerShell）

```powershell
cd D:\cursor\guardian\guardian_v7\guardian_v8
git add <具体文件>  # 不用 git add .
git commit -m "描述改动"
git push
```

### 6.2 服务器拉取

```bash
# 1. 关闭 bot
screen -r bot1
Ctrl+C  # 等优雅关闭

# 2. 拉取代码
cd /root/weather_lp
git pull

# 3. 如有新依赖
cd guardian_v8
source venv/bin/activate
pip install -r requirements.txt

# 4. 重启
python main.py
# Ctrl+A + D 挂后台
```

**注意**：`.env.bot1` 不在 git 里（含私钥），不会被 `git pull` 覆盖。

---

## 7. 故障排查

### 7.1 启动失败

#### 检查凭据文件

```bash
ls -la /root/weather_lp/guardian_v8/.env.bot1
```

应显示权限 `600` 和正确大小。

#### 检查虚拟环境

```bash
source venv/bin/activate
python -c "import polymarket; print(polymarket.__version__)"
```

### 7.2 心跳失败 → 订单被清

**症状**：日志大量"订单丢失纠偏"

**原因**：心跳连续失败 → 交易所自动取消所有订单 → 120s 后 audit 检测到 → 触发"纠偏"重新下单（正常恢复行为，不是 bug）

**排查**：

```bash
grep "HEARTBEAT" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -20
# 看是否有连续失败
```

### 7.3 WebSocket 实时性失效（新增）

**症状**：所有撤单都在 30s 周期，无实时响应

#### 诊断步骤

**A. 检查 WS 连接状态**

```bash
grep -E "MarketWS.*已连接|WS断线" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -10
```

**预期**：看到 `[MarketWS] 已连接，订阅 N 个市场`，无频繁断线

**B. 检查 WS bid 变化日志**

```bash
grep "WS bid变化" /root/weather_lp/guardian_v8/data/bot1/guardian.log | wc -l
```

**预期**：数十到数百条

**异常**：接近 0 说明路由失效

**C. 实时性测试**

```bash
# 1. 去 Polymarket 官网手动改某市场 bid
# 2. 观察日志时间戳
tail -f /root/weather_lp/guardian_v8/data/bot1/guardian.log | grep --line-buffered -E "WS bid变化|WS断线"
```

**预期**：bid 变化后 **1-2 秒内**触发撤单（不再是 30s）

#### 临时方案：关闭 WSS 回退纯 REST

```bash
# 编辑 .env.bot1 添加：
WS_MARKET_ENABLED=false
# 重启生效
```

### 7.4 screener 速度变慢

**症状**：从 8s 变成 35s+

**原因**：北京时间 18:00-22:00 网络高峰期正常现象

**解决**：部署在海外服务器（已部署 217.60.38.228）

### 7.5 V7 自动启动冲突

```bash
systemctl stop guardian_v7.service
systemctl disable guardian_v7.service
```

### 7.6 大量"孤儿订单清理"日志

**症状**：audit 日志频繁出现 `检测到 N 份孤儿订单（已移除市场仍挂单），撤销`

**原因**：筛选器移除市场后，订单未及时撤销（已修复 commit 89a9d04）

**确认修复**：
```bash
grep "孤儿订单" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -10
```

应看到 `孤儿订单撤销完成`，且不再反复出现同一 token。

---

## 8. 强制关闭（紧急）

> ⚠️ **警告**：强制关闭不会自动撤单，订单继续挂在交易所直到心跳超时（约 15s）被交易所自动取消。  
> 仅在 bot 无响应时使用。

### 方式 1：杀 screen

```bash
screen -S bot1 -X quit
```

### 方式 2：杀进程

```bash
ps aux | grep "python main.py"
kill -9 <PID>
```

---

## 9. SSH 断线后恢复

SSH 断线不影响 bot 运行（screen 保持后台）。重新连接后：

```bash
ssh root@217.60.38.228
screen -ls        # 确认 bot 还在运行
screen -r bot1    # 恢复查看
```

---

## 10. 命令速查表

| 操作 | 命令 |
|------|------|
| **SSH 登录** | `ssh root@217.60.38.228` |
| **启动** | `screen -S bot1` → `cd /root/weather_lp/guardian_v8 && source venv/bin/activate && python main.py` |
| **挂后台** | `Ctrl+A` + `D` |
| **查看状态** | `screen -ls` |
| **进入查看** | `screen -r bot1` |
| **实时日志** | `tail -f /root/weather_lp/guardian_v8/data/bot1/guardian.log` |
| **错误检查** | `grep -E "ERROR\|WARNING" /root/weather_lp/guardian_v8/data/bot1/guardian.log \| tail -50` |
| **心跳状态** | `grep "HEARTBEAT" /root/weather_lp/guardian_v8/data/bot1/guardian.log \| tail -20` |
| **WS 可用率** | `grep "WS STATS" /root/weather_lp/guardian_v8/data/bot1/guardian.log \| tail -10` |
| **WS 撤单数** | `grep "WS bid变化" /root/weather_lp/guardian_v8/data/bot1/guardian.log \| wc -l` |
| **正常关闭** | `screen -r bot1` → `Ctrl+C` → 等关闭 → `exit` |
| **强制关闭** | `screen -S bot1 -X quit` |
| **更新代码** | 本地 `git push` → 服务器 `cd /root/weather_lp && git pull` |
| **禁止V7自启** | `systemctl disable guardian_v7.service` |

---

**Guardian V8 使用手册完**

