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
4. [多账户运行（bot1 + bot2）](#4-多账户运行bot1--bot2)
5. [日常监控](#5-日常监控)
6. [正常关闭与重启](#6-正常关闭与重启)
7. [代码更新](#7-代码更新)
8. [故障排查](#8-故障排查)
9. [强制关闭（紧急）](#9-强制关闭紧急)
10. [SSH 断线后恢复](#10-ssh-断线后恢复)
11. [命令速查表](#11-命令速查表)

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

**运行第二个账户**：再建一份 `.env.bot2`，填入**第二个账户**的私钥和凭据（格式同上）。两个文件互不影响，详见 [第 4 章 多账户运行](#4-多账户运行bot1--bot2)。

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

## 4. 多账户运行（bot1 + bot2）

Guardian V8 支持**同时运行多个账户**，每个账户是完全独立的实例：读各自的 `.env.<instance>` 凭据，写各自的 `data/<instance>/` 日志与状态，互不共享内存、互不干扰。

### 4.1 前置准备

**1) 为第二个账户创建凭据文件 `.env.bot2`**

```bash
cd /root/weather_lp/guardian_v8
nano .env.bot2
# 填入账户2的凭据（与 .env.bot1 同样的字段，换成账户2的值）：
# PK=0x...                  # 账户2私钥
# PROXY_ADDRESS=0x...       # 账户2代理钱包地址
# CLOB_API_KEY=...          # 账户2 API key（可选）
# CLOB_SECRET=...           # 账户2 API secret（可选）
# CLOB_PASS_PHRASE=...      # 账户2 passphrase（可选）
# HEARTBEAT_MAX_ERRORS=5
# 筛选器参数可按账户2需求单独调整（如 SCREENER_MIN_SIZE_UPPER）

chmod 600 .env.bot2
```

**2) 验证账户2 SDK 连通性**

```bash
python test_sdk.py bot2
```

预期：`测试完成：下单 ✓  查询 ✓  撤单 ✓`

### 4.2 如何指定实例

`main.py` 按以下优先级决定跑哪个账户：

```
命令行参数  >  环境变量 INSTANCE  >  默认 bot1
```

| 启动命令 | 实例 | 读取凭据 | 日志目录 |
|---------|------|---------|---------|
| `python main.py` | bot1 | `.env.bot1` | `data/bot1/` |
| `python main.py bot2` | bot2 | `.env.bot2` | `data/bot2/` |
| `INSTANCE=bot2 python main.py` | bot2 | `.env.bot2` | `data/bot2/` |

### 4.3 同时启动两个账户（各用一个 screen）

```bash
# ── 启动账户1 ──
screen -S bot1
cd /root/weather_lp/guardian_v8 && source venv/bin/activate && python main.py bot1
# Ctrl+A 然后 D 挂后台

# ── 启动账户2 ──
screen -S bot2
cd /root/weather_lp/guardian_v8 && source venv/bin/activate && python main.py bot2
# Ctrl+A 然后 D 挂后台
```

### 4.4 查看两个账户状态

```bash
screen -ls
# 应看到两个会话：
#   12345.bot1   (Detached)
#   12346.bot2   (Detached)

# 分别进入查看
screen -r bot1      # Ctrl+A + D 挂回
screen -r bot2      # Ctrl+A + D 挂回

# 分别看日志
tail -f data/bot1/guardian.log
tail -f data/bot2/guardian.log
```

### 4.5 分别关闭

```bash
# 关闭账户1（优雅撤单）
screen -r bot1
Ctrl+C              # 等优雅关闭
exit

# 关闭账户2（优雅撤单）
screen -r bot2
Ctrl+C              # 等优雅关闭
exit
```

> ⚠️ **重要提示**：
> - 两个账户**必须使用不同的钱包/私钥**。用同一账户跑两个实例会导致订单互相冲突、重复撤单。
> - 每个账户独立占用 API 限流额度，服务器资源（2vCPU/4GB）跑 2 个实例足够，跑更多需评估负载。
> - `.env.bot1` 和 `.env.bot2` 都不在 git 中（`.gitignore` 已配置 `.env.*`），`git pull` 不会覆盖。

---

## 5. 日常监控

> 以下命令以 `bot1` 为例。跑多账户时把路径中的 `bot1` 换成 `bot2` 即可查看第二个账户。

### 5.1 实时日志

```bash
tail -f /root/weather_lp/guardian_v8/data/bot1/guardian.log
```

退出：按 `Ctrl+C`

### 5.2 专项检查

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

### 5.3 进入 screen 查看

```bash
screen -r bot1
# 查看完后 Ctrl+A + D 挂回后台
```

### 5.4 最近 100 行

```bash
tail -n 100 /root/weather_lp/guardian_v8/data/bot1/guardian.log
```

---

## 6. 正常关闭与重启

### 6.1 正常关闭（优雅撤单）

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

### 6.2 重启

```bash
screen -r bot1
# Ctrl+C 等待优雅关闭

python main.py
# 确认正常后 Ctrl+A + D 挂后台
```

---

## 7. 代码更新

### 7.1 本地推送（Windows PowerShell）

```powershell
cd D:\cursor\guardian\guardian_v7\guardian_v8
git add <具体文件>  # 不用 git add .
git commit -m "描述改动"
git push
```

### 7.2 服务器拉取

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

## 8. 故障排查

### 8.1 启动失败

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

### 8.2 心跳失败 → 订单被清

**症状**：日志大量"订单丢失纠偏"

**原因**：心跳连续失败 → 交易所自动取消所有订单 → 120s 后 audit 检测到 → 触发"纠偏"重新下单（正常恢复行为，不是 bug）

**排查**：

```bash
grep "HEARTBEAT" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -20
# 看是否有连续失败
```

### 8.3 WebSocket 实时性失效（新增）

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

### 8.4 screener 速度变慢

**症状**：从 8s 变成 35s+

**原因**：北京时间 18:00-22:00 网络高峰期正常现象

**解决**：部署在海外服务器（已部署 217.60.38.228）

### 8.5 V7 自动启动冲突

```bash
systemctl stop guardian_v7.service
systemctl disable guardian_v7.service
```

### 8.6 大量"孤儿订单清理"日志

**症状**：audit 日志频繁出现 `检测到 N 份孤儿订单（已移除市场仍挂单），撤销`

**原因**：筛选器移除市场后，订单未及时撤销（已修复 commit 89a9d04）

**确认修复**：
```bash
grep "孤儿订单" /root/weather_lp/guardian_v8/data/bot1/guardian.log | tail -10
```

应看到 `孤儿订单撤销完成`，且不再反复出现同一 token。

### 8.7 discover 接管的订单永不撤销（已修复 commit 6562aff）

**症状**：官网订单簿上有**不满足筛选条件**的市场仍挂着单（如临近结算 min_size 涨到 100 超过 `SCREENER_MIN_SIZE_UPPER=60` 的市场），但日志里**从没**出现该 token 的 `[SYNC] 市场已从筛选器移除`。

**根因**：`_apply_discover_result` 接管交易所已有挂单时，只写入 `_markets`，**漏加 `_file_managed_ids`**。而筛选器移除循环只遍历 `_file_managed_ids - target_ids`，看不见这些单 → 三条清理路径（移除循环 / audit 孤儿 / STOPPED 清理）全部够不着 → 永久遗留，还被 poll/WS 当正常单持续重报价。

**修复**：discover 接管处补 `self._file_managed_ids.add(tid)`，把接管的单纳入筛选器常规回收范围。下一轮 screener 若该市场不达标，走正常撤单路径清掉；达标则保留。

**排查手法**（bot 停止后离线分析日志）：
```bash
cd /root/weather_lp/guardian_v8
# 找"被 discover 接管过、但从未被移除"的孤儿 token
grep "\[DISCOVER\] 新市场" data/bot1/guardian.log | grep -oE '[0-9]{18,}' | sort -u > /tmp/disc.txt
grep "从筛选器移除"        data/bot1/guardian.log | grep -oE '[0-9]{18,}' | sort -u > /tmp/rm.txt
comm -23 /tmp/disc.txt /tmp/rm.txt    # 输出即孤儿候选，空=无孤儿
```

**确认修复**：重启后跑满一轮 screener（`grep "\[SCREENER\]"`），孤儿市场应出现 `从筛选器移除` 并被撤单；官网订单簿上不达标市场的挂单在 1-2 轮后消失。

---

## 9. 强制关闭（紧急）

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

## 10. SSH 断线后恢复

SSH 断线不影响 bot 运行（screen 保持后台）。重新连接后：

```bash
ssh root@217.60.38.228
screen -ls        # 确认 bot 还在运行
screen -r bot1    # 恢复查看
```

---

## 11. 命令速查表

| 操作 | 命令 |
|------|------|
| **SSH 登录** | `ssh root@217.60.38.228` |
| **启动账户1** | `screen -S bot1` → `cd /root/weather_lp/guardian_v8 && source venv/bin/activate && python main.py bot1` |
| **启动账户2** | `screen -S bot2` → `cd /root/weather_lp/guardian_v8 && source venv/bin/activate && python main.py bot2` |
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

