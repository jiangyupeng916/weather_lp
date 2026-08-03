# Weather Neg Risk Guard

Polymarket Weather 标签 Neg Risk 市场自动扫描 + 下单守护。  
周期性拉取临近结算的市场，筛选高确定性 bid 后自动买入。

## 目录

- [快速开始](#快速开始)
- [配置参数](#配置参数)
- [运行模式](#运行模式)
- [命令行](#命令行)
- [日常运维](#日常运维)
- [筛选逻辑](#筛选逻辑)
- [状态文件](#状态文件)
- [故障排查](#故障排查)

---

## 快速开始

### 1. 环境要求

- Python 3.8+
- Debian / Ubuntu 服务器

### 2. 部署

将 `deploy/` 目录上传到服务器，进入目录执行：

```bash
chmod +x setup.sh
./setup.sh
```

`setup.sh` 自动完成：
- 安装 `python3-venv`
- 创建 Python 虚拟环境 `venv/`
- 安装 `py-clob-client`
- 生成 `run.sh` 启动脚本

### 3. 测试

```bash
./run.sh --dry-run
```

看到扫描摘要表格即为正常。

### 4. 启动

```bash
nohup ./run.sh --live > logs/weather_guard.log 2>&1 &
```

---

## 配置参数

编辑 `weather_guard.py` 顶部参数区，改完重新上传即可：

```python
# 运行模式
LIVE_MODE = True           # True=实盘下单, False=仅扫描
INTERVAL_MINUTES = 10      # 循环间隔（分钟），0=单次执行
QUIET = True               # True=仅输出命中结果

# 时间窗口
END_HOURS_AHEAD = 24       # 未来窗口（小时）
END_LOOKBACK = 24          # 回溯时间（小时）

# 筛选阈值
EVENT_MIN_YES_BID = 0.96   # Event 预筛：任一 Market 的 Yes bid >= 此值
MARKET_BID_MIN = 0.98      # Market 精选：bid 下限
MARKET_BID_MAX = 0.995     # Market 精选：bid 上限

# 下单参数
ORDER_SIZE = 5.0           # 每单 USDC 份数
ORDER_WORKERS = 8          # 下单并发数

# 地区隔离
EXCLUDED_SLUGS = ["hong-kong"]  # 排除 slug 包含关键词的地区
```

| 参数 | 说明 | 建议范围 |
|------|------|---------|
| `EVENT_MIN_YES_BID` | 预筛门槛，越低 Event 越多 | 0.90 ~ 0.96 |
| `MARKET_BID_MIN` | 命中下限 | 0.98 ~ 0.99 |
| `MARKET_BID_MAX` | 命中上限 | 0.995 ~ 0.999 |
| `ORDER_SIZE` | 每单金额 (USDC) | 5 ~ 20 |
| `INTERVAL_MINUTES` | 扫描间隔 | 5 ~ 15 |
| `END_HOURS_AHEAD` | 提前多久开始盯盘 | 12 ~ 48 |
| `EXCLUDED_SLUGS` | 排除地区（slug 关键词列表） | `["hong-kong"]` |

---

## 运行模式

### Dry-Run（不下单）

```bash
./run.sh --dry-run
```

完整扫描 + 打印命中，但不实际下单。用于测试和观察盘口。

### Live（实盘）

```bash
./run.sh --live
nohup ./run.sh --live > logs/weather_guard.log 2>&1 &
```

扫描后自动下单。每轮输出摘要到 stdout，进度信息到 stderr。

### 单次 vs 循环

- `INTERVAL_MINUTES = 0`：跑一轮就退出
- `INTERVAL_MINUTES = N`：每 N 分钟跑一轮，无限循环

命令行 `--interval` 参数会覆盖配置文件的值。

---

## 命令行

| 参数 | 说明 |
|------|------|
| `--live` | 启用实盘下单（覆盖 `LIVE_MODE`） |
| `--dry-run` | 仅扫描不下单（覆盖 `LIVE_MODE`） |
| `--interval N` / `-i N` | 循环间隔（分钟），覆盖 `INTERVAL_MINUTES` |
| `--quiet` | 静默模式 |

示例：

```bash
./run.sh --dry-run -i 2      # 每 2 分钟扫描，不下单
./run.sh --live --quiet       # 实盘静默
./run.sh                       # 按配置文件参数运行
```

---

## 日常运维

### systemd 服务管理（推荐）

`setup.sh` 会生成 `weather_guard.service`、`limit_sell.service` 和 `logrotate.conf`。安装一次即可：

```bash
# 一次性安装（需 sudo）
sudo cp weather_guard.service limit_sell.service /etc/systemd/system/
sudo cp logrotate.conf /etc/logrotate.d/polymarket
sudo systemctl daemon-reload
sudo systemctl enable --now weather_guard limit_sell
```

安装后服务会：
- **崩溃自动重启**（10 秒后）
- **服务器重启后自动拉起**
- **日志自动轮转**（每天或超过 10M 切割，保留 7 天压缩备份）

#### 日常操作

```bash
# 查看状态
sudo systemctl status weather_guard
sudo systemctl status limit_sell

# 重启
sudo systemctl restart weather_guard
sudo systemctl restart limit_sell

# 停止 / 启动
sudo systemctl stop weather_guard
sudo systemctl start weather_guard

# 查看应用日志
tail -f logs/weather_guard.log
tail -f logs/limit_sell.log

# 查看 systemd 日志（启动/重启记录）
sudo journalctl -u weather_guard -f
sudo journalctl -u limit_sell --since "1 hour ago"
```

#### 更新代码

```bash
sudo systemctl stop weather_guard
# 上传新的 weather_guard.py 覆盖
./run.sh --dry-run               # 测试
sudo systemctl start weather_guard
```

### nohup 方式（备用）

若无 sudo 权限，仍可用 nohup：

```bash
nohup ./run.sh --live > logs/weather_guard.log 2>&1 &
nohup ./run_limit_sell.sh --live --interval 10 > logs/limit_sell.log 2>&1 &

# 查看进程
ps aux | grep -E "weather_guard|limit_sell"

# 停止
pkill -f weather_guard.py
pkill -f limit_sell.py
```

注意：nohup 方式不会自动重启，日志需手动清理。

---

## 筛选逻辑

### 整体流程

```
Gamma API → 按时间窗口拉取 Weather 市场
  └── CLOB 查询所有 token 的 BUY / SELL 报价
        └── Event 预筛：任一 Market Yes bid >= 0.96
              └── Market 精选：bid ∈ [0.98, 0.995]
                    └── 下单：ask 优先吃单
```

### 下单策略

对于一个命中的 Market：

```
bid ∈ [0.98, 0.995]
  ├── ask 也在 [0.98, 0.995] → 挂 ask 价（吃单，即时成交）
  └── ask > 0.995 → 挂 bid 价（挂单排队）
```

### 去重

- 同一个 Market 只下单一次（记录在 `ordered.json`）
- 已过期的 Event 自动从观察列表清理
- 下单失败的 Market 下轮会重试

### 数据模型

```
Weather 标签 (tag_id=84)
  └── Event（Neg Risk，互斥市场组，只有一个结算 Yes）
        ├── Market A  ── Yes token + No token
        ├── Market B  ── Yes token + No token
        └── Market C  ── Yes token + No token
```

- `clobTokenIds[0]` = Yes, `clobTokenIds[1]` = No
- tick_size = 0.001（Neg Risk 专用精度）

---

## 状态文件

| 文件 | 说明 |
|------|------|
| `.env` | 凭证（私钥、API Key、合约地址） |
| `watched_events.json` | 通过预筛的 Event 观察列表 |
| `ordered.json` | 已下单 Market 记录（含下单价格快照） |
| `logs/weather_guard.log` | 运行日志 |

`watched_events.json` 和 `ordered.json` 首次运行自动生成，用于跨轮次去重。

### ordered.json 结构

```json
{
  "2519450": {
    "market_id": "2519450",
    "question": "...",
    "event_id": "587937",
    "side": "no",
    "price": 0.993,
    "bid_at_time": 0.992,
    "ask_at_time": 0.993,
    "size": 5.0,
    "order_id": "0xabc...",
    "time": "2026-06-30T02:15:00Z"
  }
}
```

`price` = 实际挂单价（ask 优先），`bid_at_time` / `ask_at_time` = 下单时的盘口快照。

---

## 故障排查

### 进程不存在

```bash
ps aux | grep weather_guard
```

无输出则进程已退出，检查日志最后几行：

```bash
tail -50 logs/weather_guard.log
```

### 常见错误

| 错误 | 原因 | 解决 |
|------|------|------|
| `ModuleNotFoundError: py_clob_client` | venv 未激活 | 用 `./run.sh` 而非 `python` |
| `[FATAL] CLOB 客户端初始化失败` | `.env` 缺失或凭证错误 | 检查 `.env` 文件 |
| `[ORDER FAIL]` | 余额不足 / 网络异常 / tick_size | 查日志详情 |
| `[CLOB error]` | CLOB API 超时 | 网络波动，会自动重试 |
| 0 命中 | 当前盘口无边 0.98+ bid | 正常，等待市场变化 |

### 余额检查

脚本不内置余额查询。在 Polymarket 网页查看 USDC 余额，或通过 SDK 查询：

```bash
source venv/bin/activate
python -c "
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
import os, sys
sys.path.insert(0, '.')
import weather_guard as wg
env = wg.load_env('.env')
client = wg.make_clob_client(env)
bal = client.get_balance_allowance()
print(bal)
"
```

### 手动清理状态

```bash
# 重置已下单记录（允许重新下单）
echo '{"orders":{},"updated_at":"","total":0}' > ordered.json

# 重置观察列表
echo '{"events":{},"updated_at":"","total":0}' > watched_events.json
```

### 时区

所有时间戳为 UTC。服务器建议设 UTC 时区：

```bash
timedatectl set-timezone UTC
```

---

## Stop Loss — 持仓止损监控

独立止损脚本，定期扫描持仓，ask 价低于阈值时限价卖出（挂单价 = ask - 0.03）。

### 与 Weather Guard 的关系

- **完全独立**，不共享状态文件，可以同时运行
- Weather Guard 负责**买入**，Stop Loss 负责**卖出止损**
- 建议两者各自后台运行

### 快速使用

```bash
# dry-run 测试
./run_stop_loss.sh --dry-run

# 单次实盘
./run_stop_loss.sh --live

# 后台持续监控（每 5 分钟扫描一次）
nohup ./run_stop_loss.sh --live --interval 5 > logs/stop_loss.log 2>&1 &
```

### 配置参数

编辑 `stop_loss.py` 顶部参数区：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `STOP_LOSS_THRESHOLD` | 0.93 | ask 价低于此值触发止损 |
| `INTERVAL_MINUTES` | 0 | 循环间隔（0=单次执行） |
| `LIVE_MODE` | False | True=实盘卖出，False=仅扫描 |

### 命令行参数

| 参数 | 说明 |
|------|------|
| `--live` | 启用实盘卖出 |
| `--dry-run` | 仅扫描不下单 |
| `--interval N` / `-i N` | 循环间隔（分钟） |
| `--threshold N` / `-t N` | 止损阈值（覆盖默认 0.93） |

示例：

```bash
./run_stop_loss.sh --live -t 0.90 -i 3    # 阈值 0.90，每 3 分钟
./run_stop_loss.sh --dry-run -t 0.95       # 测试阈值 0.95，不下单
```

### 止损逻辑

```
Data API 查代理持仓 → CLOB 查 ask 价 (SELL 侧) → ask < 阈值 → 市价卖出（FOK → FAK）
```

- 持仓查询使用 `.env` 中的 `PROXY_ADDRESS`（POLY_PROXY 合约地址）
- 卖出方式：GTC 限价单，挂单价 = ask - 0.03（如 ask=0.89 → 挂 0.86）
- 连续两轮触发才执行卖出，避免订单簿瞬间波动误触发

### 后台运维

```bash
# 查看进程
ps aux | grep stop_loss

# 停止
pkill -f stop_loss.py

# 查看日志
tail -f logs/stop_loss.log

# 重启
pkill -f stop_loss.py
sleep 2
nohup ./run_stop_loss.sh --live --interval 5 > logs/stop_loss.log 2>&1 &
```

### 同时运行多个守护

```bash
# Weather Guard — 买入扫描
nohup ./run.sh --live > logs/weather_guard.log 2>&1 &

# Stop Loss — 止损监控
nohup ./run_stop_loss.sh --live --interval 2 > logs/stop_loss.log 2>&1 &

# Limit Sell — 0.999 挂卖
nohup ./run_limit_sell.sh --live --interval 10 > logs/limit_sell.log 2>&1 &
```

---

## Limit Sell — 持仓限价挂卖

独立脚本，查询所有持仓，对没有 SELL 挂单的持仓以固定价格（默认 0.999）挂 GTC 限价卖单。循环执行，定期补挂。

### 与其他脚本的关系

- **完全独立**，不共享状态文件
- 查询 CLOB open orders，已有 SELL 挂单的 token 自动跳过（不会重复挂）
- 可以与 Weather Guard / Stop Loss 同时运行

### 快速使用

```bash
# dry-run 测试
./run_limit_sell.sh --dry-run

# 单次实盘
./run_limit_sell.sh --live

# 后台循环（每 10 分钟补挂一次）
nohup ./run_limit_sell.sh --live --interval 10 > logs/limit_sell.log 2>&1 &

# 自定义价格
./run_limit_sell.sh --live -p 0.995
```

### 配置参数

编辑 `limit_sell.py` 顶部参数区：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `SELL_PRICE` | 0.999 | 挂卖价格 |
| `INTERVAL_MINUTES` | 0 | 循环间隔（0=单次执行） |
| `LIVE_MODE` | False | True=实盘卖出 |
| `TICK_SIZE` | "0.001" | Neg Risk 市场精度 |

### 命令行参数

| 参数 | 说明 |
|------|------|
| `--live` | 启用实盘卖出 |
| `--dry-run` | 仅扫描不下单 |
| `--interval N` / `-i N` | 循环间隔（分钟） |
| `--price N` / `-p N` | 挂卖价格（覆盖默认 0.999） |

### 挂卖逻辑

```
Data API 查代理持仓 → CLOB 查 open orders → 筛选 side=SELL 的 token
  → 对未挂卖的持仓挂 GTC 限价卖单 @ 0.999
```

- 只对没有 SELL 挂单的持仓下单，避免重复挂
- 挂单价固定 0.999（可 `-p` 自定义）
- GTC 限价单，一直挂着直到成交或取消

### 后台运维

```bash
# 查看进程
ps aux | grep limit_sell

# 停止
pkill -f limit_sell.py

# 查看日志
tail -f logs/limit_sell.log

# 重启
pkill -f limit_sell.py
sleep 2
nohup ./run_limit_sell.sh --live --interval 10 > logs/limit_sell.log 2>&1 &
```

---

## Monitor WSS — Neg Risk 套利实时监控

WSS 实时监控 Polymarket Neg Risk 事件, 检测套利机会后 bottleneck-first FOK 下单 + GTC 重挂循环。不自动 convert (用户手动)。

### 与其他脚本的关系

- **完全独立**, 不共享状态文件
- Weather Guard 负责**高确定性 bid 买入**, Monitor WSS 负责**套利下单** (买 K 个 NO → convert)
- 可以与 Weather Guard / Stop Loss / Limit Sell 同时运行

### 快速使用

```bash
# dry-run 测试 (monitor_wss.py 顶部 LIVE=False)
./run_monitor_wss.sh

# 实盘 (改 monitor_wss.py LIVE=True 后)
nohup ./run_monitor_wss.sh > logs/monitor_wss.log 2>&1 &

# systemd 方式 (推荐, 崩溃自动重启)
sudo systemctl start monitor_wss
sudo systemctl status monitor_wss
tail -f logs/monitor_wss.log
```

### 配置参数

编辑 `monitor_wss.py` 顶部配置区 (第 47-77 行), 改完重新上传:

```python
# ---- 监控范围 ----
TAG_ID             = ["103040"]  # Gamma tag_id, 支持多 tag; ["103040"]=每日温度; ["1"]=sports
MIN_VOLUME         = 0

# ---- 运行模式 ----
LIVE               = False      # False=dry-run; True=实盘自动下单
MAX_AMOUNT         = 5          # 单份下单上限 (pUSD)
DURATION           = None       # None=无限; 300=跑5分钟

# ---- 套利检测 ----
MIN_PROFIT         = 0.0001
DEDUP_WINDOW       = 30

# ---- WSS 连接 ----
WSS_URL            = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL      = 10
STATUS_INTERVAL    = 30

# ---- 下单 ----
ORDER_FILL_TIMEOUT    = 10
MAX_REPEG_ROUNDS      = 5
REPEG_MAX_PRICE_MULT  = 1.05     # REPEG 涨幅保护
MAX_CONCURRENT_EXEC   = 3
FOK_FAIL_COOLDOWN     = 30
```

### 套利原理

Neg Risk 事件包含 N 个互斥子市场, 买入 K 个 NO token → 调 convertPositions → 获得 (K-1) pUSD + (N-K) YES token。

套利条件: K 个 NO 的总成本 (含 CLOB taker fee) < (K-1) pUSD。

### 执行流程

```
WSS 检测到 best_ask 变化 → check_arbitrage
  ↓ 检测到套利机会
bottleneck-first FOK 下单 (ask_size 最小的 token)
  ├─ FOK 失败 → 零仓位退出, 30s 冷却
  └─ FOK 成交 → 批量 GTC 下剩余 K-1 个
                  ↓ 超时 10s 未成交
                  取消 + REST /books 查最新 ask 重挂 (最多 5 轮)
                  ↓
最终报告 + convert 参数 (用户手动 convert)
```

### .env 额外字段

monitor_wss 需要 `.env` 包含 (deploy/.env 现有字段已足够启动, 若要启用 convert 需补全):

```
# 现有 (weather_guard 已有, 复用):
PK, CHAIN_ID, CLOB_API_KEY, CLOB_SECRET, CLOB_PASS_PHRASE, PROXY_ADDRESS

# convert 用 (暂不启用 convert 可不配):
RELAYER_API_KEY, RELAYER_API_KEY_ADDRESS, RELAYER_HOST
NEG_RISK_CTF_COLLATERAL_ADAPTER, PUSD, POLYGON_RPC
```

### 后台运维

```bash
# 查看进程
ps aux | grep monitor_wss

# 停止
pkill -f monitor_wss.py

# 查看日志
tail -f logs/monitor_wss.log

# 重启
pkill -f monitor_wss.py
sleep 2
nohup ./run_monitor_wss.sh > logs/monitor_wss.log 2>&1 &
```

### 注意事项

- **LIVE=True 会自动下单**, 首次部署建议 LIVE=False 观察 OPP 日志
- **不自动 convert**, 检测到机会并下单后, 用户需手动执行 convertPositions
- **WSS 大订阅稳定性**: 28941 token 时可能频繁 1006 断连, 建议 TAG_ID 限制范围或提高 MIN_VOLUME
- **bandwidth**: ~9 Mbps @ 1700 token, 服务器带宽需足够
