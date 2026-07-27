# Guardian V7 — 服务器部署说明

Polymarket CLOB 全自动 Maker 做市机器人。内置筛选器发现高返利市场 → 限价挂单赚取返利奖励 → 成交后限价卖出平仓。

---

## 目录

- [快速开始](#快速开始)
- [文件结构](#文件结构)
- [配置参数](#配置参数)
- [运行原理](#运行原理)
- [日常运维](#日常运维)
- [日志说明](#日志说明)
- [故障排查](#故障排查)

---

## 快速开始

### 1. 上传

将整个 `guardian_v7/` 文件夹上传到服务器：

```bash
scp -r deploy/guardian_v7/ user@your-server:/opt/
```

### 2. 安装

```bash
cd /opt/guardian_v7
chmod +x setup.sh
./setup.sh
```

`setup.sh` 自动完成：安装 python3-venv → 创建 venv → 安装依赖 → 生成 run.sh 和 systemd 服务文件。

### 3. 配置

`.env` 已预填凭证（从本地同步），如需修改筛选器参数直接编辑 `.env`：

```bash
nano .env
```

需要修改的常用参数：

| 参数 | 说明 | 示例 |
|------|------|------|
| `SCREENER_KEYWORD=temp` | 关键词过滤，`""` = 不过滤 | `temp` / `sports` / `""` |
| `SCREENER_INTERVAL=30` | 筛选间隔（秒） | `30` ~ `120` |
| `SCREENER_MIN_TOP2_BIDS=800.0` | 降低则纳入更多市场 | `400` ~ `800` |
| `SCREENER_MIN_TOP3_BIDS=1500.0` | 降低则纳入更多市场 | `800` ~ `1500` |

### 4. 测试

```bash
./run.sh
```

看到 `[SCREENER] N markets in Xs` 表示正常。`Ctrl+C` 退出。

### 5. 启动

```bash
sudo systemctl start guardian_v7
```

`setup.sh` 已自动安装 systemd 服务，直接启动即可。

---

## 文件结构

```
guardian_v7/
├── main.py              # 入口
├── config.py             # 配置（环境变量 → dataclass）
├── guardian.py           # 主控（定时循环 + 状态管理）
├── execution.py          # 执行层（限流 + 线程池 + 下单/撤单/卖单）
├── heartbeat.py          # 心跳保活（7s 间隔，订单不被交易所取消）
├── ws_manager.py         # WebSocket 管理（用户频道，trade 事件日志）
├── ws_router.py          # WS 消息路由
├── models.py             # 数据模型（MarketState、OrderInfo）
├── utils.py              # 工具函数
├── screener/             # 内置筛选器子包
│   ├── markets.py        # 拉取 /sampling-markets
│   ├── clob.py           # 批量查 /books + 评分
│   └── types.py          # 数据类
├── .env                  # 凭证 + 筛选器参数（已预填）
├── .env.example          # 凭证模板
├── setup.sh              # 一键部署脚本
├── run.sh                # 启动脚本（setup.sh 自动生成）
├── guardian_v7.service   # systemd 服务（setup.sh 自动生成）
├── logrotate.conf        # 日志轮转（setup.sh 自动生成）
├── README.md             # 本文件
├── data/                 # 运行时数据（自动创建）
│   ├── guardian.log      # 主日志
│   ├── trades.log        # 成交记录
│   └── screener_latest.csv  # 最新筛选结果
└── logs/                 # systemd 输出（自动创建）
    └── guardian.log
```

---

## 配置参数

### 凭证（必填）

已在 `.env` 中预填，无需修改：

| 参数 | 说明 |
|------|------|
| `PK` | 钱包私钥 |
| `CLOB_API_KEY` | CLOB API Key |
| `CLOB_SECRET` | CLOB API Secret |
| `CLOB_PASS_PHRASE` | CLOB Passphrase |
| `PROXY_ADDRESS` | 代理合约地址（0x304b...） |

### Maker 挂单策略

在 `config.py` 中配置（默认值）：

| 参数 | 默认 | 说明 |
|------|------|------|
| `maker_size` | 50 USDC | 每单挂单量 |
| `maker_rank` | 2 | 挂在 best_bid 第几档 |
| `maker_cooldown` | 120s | 撤单后冷却时间 |
| `tick_size` | 0.01 | 价格精度 |

### 定时任务

| 任务 | 间隔 | 职责 |
|------|------|------|
| `_run_screener()` | 30s | 内置筛选器，拉取有奖励的市场 |
| `discover()` | 30s | 接管已有订单 + 同步筛选结果 |
| `_poll_best_bids()` | 3s | 批量查价，检测 best_bid 变化 |
| `audit()` | 120s | 纠偏超价订单、检测丢单 |
| `check_positions()` | 120s | 扫描持仓 → 限价卖出 |

### 筛选器参数（全部在 `.env` 中，修改后重启即生效）

| 参数 | 默认 | 说明 |
|------|------|------|
| `SCREENER_KEYWORD` | `temp` | 关键词过滤，`""` = 全部市场 |
| `SCREENER_INTERVAL` | `30` | 筛选间隔（秒） |
| `SCREENER_MIN_DAILY_REWARDS` | `20.0` | 最小每日返利（USDC） |
| `SCREENER_MIN_DAYS_TO_EXPIRY` | `0` | 最小剩余天数（0=不限） |
| `SCREENER_MIN_MIDPOINT` | `0.15` | 概率下限（15%） |
| `SCREENER_MAX_MIDPOINT` | `0.85` | 概率上限（85%） |
| `SCREENER_MIN_SIZE_LOWER` | `0.0` | min_size 下限 |
| `SCREENER_MIN_SIZE_UPPER` | `60` | min_size 上限 |
| `SCREENER_MIN_EXISTING_SIZE` | `1500.0` | 最低现有流动性 |
| `SCREENER_MIN_TOP1_BIDS` | `50.0` | top-1 出价深度 |
| `SCREENER_MIN_TOP2_BIDS` | `800.0` | top-2 出价深度 |
| `SCREENER_MIN_TOP3_BIDS` | `1500.0` | top-3 出价深度 |

### 卖出保护

| 参数 | 默认 | 说明 |
|------|------|------|
| `sell_min_bid_gap` | 0.02 | best_bid 低于成本价 - 此值则跳过卖出 |

---

## 运行原理

### 整体流程

```
筛选器 (30s) → 发现高返利市场 → 挂限价买单 (post_only)
    → 监控 best_bid (3s) → best_bid 变化 → 撤单 → 冷却 → 重挂
    → 已成交? → 持仓扫描 (120s) → 限价卖单挂在 best_bid
    → 心跳 (7s) → 保证订单不被交易所取消
```

### 状态机

每个市场有且仅有一个状态：`NO_ORDER → PLACING → RESTING → CANCELING → COOLING → ...`

- **NO_ORDER**: 无挂单，等待下单时机
- **PLACING**: 正在向交易所提交订单（异步）
- **RESTING**: 订单已挂上，等待成交或撤单
- **CANCELING**: 正在撤销订单（异步）
- **COOLING**: 冷却中，到期后重新挂单
- **STOPPED**: 已被筛选器移除，等待清理

### 筛选器评分公式

```
reward_per_dollar = 每日返利 / (订单簿深度 + min_size)
```

按 `reward_per_dollar` 降序排列，取 top-N 个方向（受深度阈值限制）。

### 安全保护

- **Post-Only**：所有买单使用 post_only=True，绝不跨价吃单
- **Heartbeat**：每 7s REST 心跳，防交易所自动撤单
- **Audit**：每 120s 全场审计，纠正超价订单和丢失订单
- **Stale Timeout**：状态卡死 60s 自动重置

---

## 日常运维

### systemd（推荐）

```bash
# 查看状态
sudo systemctl status guardian_v7

# 查看日志
tail -f logs/guardian.log

# 重启
sudo systemctl restart guardian_v7

# 停止
sudo systemctl stop guardian_v7

# 启动
sudo systemctl start guardian_v7

# systemd 日志（启动/崩溃记录）
sudo journalctl -u guardian_v7 -f
sudo journalctl -u guardian_v7 --since "1 hour ago"
```

### 修改筛选器参数

```bash
nano .env              # 修改 SCREENER_* 参数
sudo systemctl restart guardian_v7
```

### 更新代码

```bash
sudo systemctl stop guardian_v7
# 上传新的 .py 文件覆盖
./run.sh               # 测试运行, Ctrl+C 退出
sudo systemctl start guardian_v7
```

### 查看筛选结果

```bash
cat data/screener_latest.csv
# 或查看日志中的筛选摘要
grep "SCREENER" logs/guardian.log | tail -20
```

### 查看成交记录

```bash
tail -f data/trades.log
```

### nohup 方式（无 sudo 时备用）

```bash
nohup ./run.sh > logs/guardian.log 2>&1 &

# 查看进程
ps aux | grep main.py

# 停止
pkill -f main.py
```

---

## 日志说明

| 日志文件 | 内容 | 用途 |
|----------|------|------|
| `logs/guardian.log` | systemd 重定向的全部输出 | 日常检查 |
| `data/guardian.log` | 程序内部主日志（含 debug） | 排查问题 |
| `data/trades.log` | 每笔成交 JSON 记录 | 审计盈亏 |
| `data/screener_latest.csv` | 最新筛选结果 | 人工查看 |

### 关键日志示例

```
# 正常启动
[HEARTBEAT] 已启动 interval=7.0s
Maker-only Guardian V7.0 启动
守护 31 个市场

# 筛选器运行
[SCREENER] 150 markets in 3.4s | yes:22 no:9 | next in 30s

# 挂单
[STATE] 0xabc123... RESTING 0xdef456... price=0.52

# best_bid 变化 → 撤单
[BATCH CANCEL] 批量撤单 3 个 | best_bid变化

# 卖出
[LIMIT SELL] 0xabc123... 更新卖价 0.55 → 0.56

# 心跳
[HEARTBEAT] OK

# 优雅退出
收到停止信号，优雅退出...
[CANCEL ALL] 31 已取消 | 系统关闭
```

---

## 健康检查清单

正常运行时应有以下特征：

- [ ] `[HEARTBEAT] OK` 每 7s 出现（debug 级别）
- [ ] `[SCREENER] N markets in Xs` 每 30s 出现
- [ ] `[DISCOVER] 守护 N 个市场` N 不为 0
- [ ] 无连续 `[HEARTBEAT] 失败` 或 `[HEARTBEAT] CRITICAL`
- [ ] 无 `[WS ERR]` 或频繁 `[WS] close code=`
- [ ] 无 `[BATCH CANCEL] 取消失败` 或 `[PLACE FAIL]`
- [ ] 挂单数量与筛选器 yes/no 之和大致相符

---

## 故障排查

### 进程挂了

```bash
sudo systemctl status guardian_v7    # 查看状态
tail -100 logs/guardian.log          # 查看最后 100 行
sudo journalctl -u guardian_v7 -n 50 # 查看 systemd 日志
```

systemd 服务配置了 `Restart=always` + `RestartSec=10`，崩溃后 10 秒自动重启。

### 订单被全部取消

心跳中断超过 ~15 秒导致交易所自动撤单。检查日志是否有连续 heartbeat 失败：

```bash
grep "HEARTBEAT.*失败\|HEARTBEAT.*CRITICAL" logs/guardian.log
```

### 0 个市场

1. 检查关键词：`SCREENER_KEYWORD` 是否正确（默认 `"temp"`）
2. 检查网络：`curl https://clob.polymarket.com/sampling-markets | head`
3. 调低深度阈值：降低 `SCREENER_MIN_TOP2_BIDS` 和 `SCREENER_MIN_TOP3_BIDS`

### 挂单量远超筛选器结果

已修复（V7.7），根因是筛选器移除市场时不等撤单完成就 pop 状态，导致 discover() 重新发现为孤儿。最新代码已解决此问题。

### 余额不足

Polymarket 网页查看 USDC 余额。Guardian 不内置余额查询，下单失败时会打 `[PLACE FAIL]` 日志。

### 网络问题

所有 HTTP 请求有重试机制（markets 5 次、clob 5 次）。长时间断网会导致心跳失败 → 全部订单被取消 → 恢复后自动重新筛选挂单。
