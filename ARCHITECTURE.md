# Guardian V7 架构文档

## 一、项目概述

Guardian V7 是一个 **Polymarket CLOB 交易平台的 Maker-only 自动化做市机器人**。

**核心策略**：监控订单簿买盘，在第 N 档（默认第 3 档）挂限价买单。当买单价位被超越时撤单重挂。成交后自动市价卖出持仓。

**技术栈**：Python 3.12 | `py_clob_client_v2` | `websocket-client 1.8` | `eth_account` | `requests`

---

## 二、体系结构

### 2.1 模块依赖关系（零循环导入）

```
config.py   ← 无内部依赖（冻结 dataclass，环境变量加载）
models.py   ← 无内部依赖（枚举、数据类）
utils.py    ← 无内部依赖（纯函数工具）
    │
    ├── heartbeat.py   ← config, utils
    ├── execution.py   ← config, models, utils
    ├── ws_manager.py  ← config
    │
    ├── actor.py       ← config, models, utils, execution (Guardian via TYPE_CHECKING)
    ├── ws_router.py   ← config, models (Guardian via TYPE_CHECKING)
    │
    └── guardian.py    ← 以上全部
            │
        main.py        ← config, guardian
```

### 2.2 数据流全景

```
                           ┌─── REST API ───┐
                           │                 │
                  ┌────────▼──────┐  ┌───────▼──────────┐
                  │ HeartbeatManager│  │ ExecutionLayer    │
                  │ (每7s心跳)      │  │ (限流+线程池)      │
                  │ 维持订单存活     │  │ place/cancel/sell │
                  └────────────────┘  └────────┬──────────┘
                                               │ Future
  ┌──────────────┐              ┌──────────────┤
  │ Market WS ◄──┼──────────────┤              │
  │ 订单簿快照    │              │              │
  │ best_bid     │    ┌─────────▼──────────┐   │
  │ price_change │    │  WSRouter          │   │
  └──────────────┘    │  解析JSON → 分发    │   │
                      └──┬──────────┬──────┘   │
  ┌──────────────┐      │          │          │
  │ User WS  ◄───┼──────┘          │          │
  │ trade 事件    │                 │          │
  │ order 事件    │     ┌───────────▼────┐     │
  │(CANCELLATION) │     │ Guardian 主控   │     │
  └──────────────┘     │ discover(30s)  │     │
                       │ audit(120s)    ├─────┘
                       │ positions(60s) │
                       │ prune(300s)    │
                       └───────┬────────┘
                               │ post(ActorEvent)
                       ┌───────▼────────┐
                       │ AssetActor×N   │
                       │ 串行状态机      │
                       │ 每市场一个实例   │
                       └────────────────┘
```

---

## 三、核心组件详解

### 3.1 HeartbeatManager — 心跳保活

**作用**：Polymarket 要求每 ~10 秒通过 `POST /v1/heartbeats` 发送心跳，否则**全部挂单被交易所自动取消**。这是 V6 完全缺失的关键功能。

```
协议：
  首次调用: POST {"heartbeat_id": ""}           → 200 {"heartbeat_id": "xxx"}
  后续调用: POST {"heartbeat_id": "xxx"}        → 200 {"heartbeat_id": "yyy"}
  过期 ID:  POST {"heartbeat_id": "expired"}   → 400 {"heartbeat_id": "correct_id", "error_msg": "Invalid Heartbeat ID"}
```

**容错策略**：
1. 优先使用 SDK `client.post_heartbeat()`
2. SDK 失败 → 回退到 L2 认证头直接 HTTP 请求
3. 400 响应 → 提取 `heartbeat_id` 恢复链式关系
4. 连续失败 ≥3 次 → CRITICAL 告警

**启动/停止**：心跳最先启动、最后停止（确保订单存活窗口覆盖全部运行时间）。

### 3.2 ExecutionLayer — 执行层

所有 REST 写操作（下单/撤单/市价卖出）通过此层，保证：

| 特性 | 机制 |
|------|------|
| 限流 | 两次写操作 ≥0.2s 间隔（~5 QPS） |
| 幂等 | `_place_tokens`(300s TTL) 和 `_cancel_tokens` 去重 |
| 异步 | `place()`/`cancel()`/`market_sell()` 返回 `Future`，线程池执行 |
| 价格对齐 | `PartialCreateOrderOptions(tick_size=str(...))` |

### 3.3 AssetActor — 单市场状态机

每个被守护的市场一个 Actor 实例，在独立线程中**串行**处理事件队列。

```
                        ┌──────────┐
               ┌───────►│ NO_ORDER │◄──────────┐
               │        └─────┬────┘           │
               │              │ _place()       │ cooldown_expired
               │        ┌─────▼────┐           │ 或 审计强制重挂
               │        │ PLACING  │           │
               │        └─────┬────┘           │
               │              │ PLACE_DONE(ok) │
               │        ┌─────▼────┐           │
               │        │ RESTING  ├───────────┤ best_bid 变化
               │        └──┬───┬──┘           │ target_price 变化
               │           │   │              │ WSS 重连
               │  成交匹配  │   │ 撤单         │
               │  (保持     │   │              │
               │  RESTING)  │   │              │
               │           │   │              │
               │        ┌──▼───▼──┐           │
               │        │CANCELING│           │
               │    ┌───┤  (异步) ├───┐       │
               │    │   └─────────┘   │       │
               │    │ CANCEL_DONE(ok) │       │
               │    ▼                 ▼       │
               │  ┌─────┐        ┌────────┐   │
               │  │STOP │        │RESTING │   │
               │  │PED  │        │(保持)   │   │
               │  └─────┘        └────────┘   │
               │                              │
               │                        ┌─────▼────┐
               └────────────────────────│ COOLING  │
                                        │ 120s→重挂 │
                                        └──────────┘
```

**关键设计决策**：

| 决策 | 原因 |
|------|------|
| 撤单异步（Future 回调 post 回队列） | 避免阻塞事件循环，保持串行语义 |
| 撤单失败保持 RESTING | 订单仍存活在交易所，不能重复下单 |
| 成交后保持 RESTING | 部分成交≠全部成交，由审计验证实际状态 |
| 冷却 120 秒 | 防止频繁撤单重挂触发交易所风控 |

### 3.4 WSManager + WSRouter — WebSocket 层

```
WSManager（连接生命周期）
├── _run_ws() 单线程 while 循环处理 连接→断开→重连
├── _ws_on_open(): 先发 auth/订阅 → 再启 ping 线程
│   └── ping 循环: sleep(interval) → 发 PING → sleep → ...
└── 代理支持: 自动读取 HTTP_PROXY/HTTPS_PROXY

WSRouter（消息分发）
├── 市场频道 → book / price_change / best_bid_ask / tick_size / resolved
└── 用户频道 → trade(MATCHED/CONFIRMED/FAILED) / order(CANCELLATION)
```

**PING 时序要求**：必须 `sleep → PING` 而非 `PING → sleep`。连接后立即发 PING 会导致 Polymarket 服务器拒绝连接。

### 3.5 Guardian — 主控制器

#### 定时任务

| 任务 | 间隔 | 功能 |
|------|------|------|
| `discover()` | 30s | 发现新订单、多周期清理废弃 Actor |
| `audit()` | 120s | 纠偏超价订单、检测订单丢失、状态卡死重置 |
| `check_positions()` | 60s | Data API 查持仓 → 自动市价卖出 |
| `_prune_caches()` | 300s | 清理过期缓存 |

#### 线程安全的 Actor 管理

全部 `self._actors` 访问通过加锁方法：
- `get_actor(id)` / `add_actor(id, actor)` / `remove_actor(id)`
- `list_actors()` / `list_actor_ids()` / `actor_count()`

---

## 四、线程模型

### 4.1 线程清单

| 线程 | 数量 | 持久性 | 用途 |
|------|------|--------|------|
| MainThread | 1 | 持久 | 主循环 |
| heartbeat | 1 | 持久 | REST 心跳 |
| ws-market | 1 | 持久 | 市场 WS（单线程重连） |
| ws-user | 1 | 持久 | 用户 WS（单线程重连） |
| ws-ping | 2 | 持久 | market/user 频道 PING 保活 |
| Actor-* | N | 持久 | 每个市场一个事件循环 |
| exec-N | ≤10 | 持久 | ThreadPoolExecutor 工作线程 |
| 回调线程 | 短期 | M | place/cancel Future 结果回传 |
| 卖出线程 | 短期 | K | _process_trade_sell |

### 4.2 锁层级（防死锁）

```
获取顺序（严格单向，绝不逆行）:

1. _actors_lock        (RLock, Guardian)     — 最外层
2. _trade_lock         (Lock,  Guardian)
3. _pending_sells_lock (Lock,  Guardian)
4. _sell_lock          (Lock,  Guardian)     — 最内层

+ _state_lock          (RLock, 每个Actor独立) — 独立，不与上述交叉
+ ExecutionLayer内部锁                       — 独立，不与上述交叉
```

### 4.3 线程安全关键点

1. **Actor 事件队列**：`CANCEL_DONE`/`PLACE_DONE` 事件通过 `post()` 投递到 Actor 队列，确保状态修改在 Actor 线程内执行
2. **Actor 生命周期**：`post()` 有 `_running` 检查，防止向已停止的 Actor 投递事件
3. **WS 重连互斥**：`_market_lock`/`_user_lock` 保证同一频道只有一个连接线程

---

## 五、关键 Bug 分析与修复

### 5.1 P0：交易所自动取消 → 全部放弃（本次修复的核心问题）

**因果链**：
```
Heartbeat 异常（网络抖动）
  ↓ 10~15 秒
交易所自动取消全部挂单
  ↓ 用户 WS
收到 N 个 CANCELLATION 事件
  ↓ handle_order()
is_system_cancel() = False（不是我们发起的）
  ↓ 误判为「人工撤单」
_check_abandon() 被调用 N 次
  ↓ open_orders() 返回空
has_buy = False（全部订单都没了）
  ↓
全部市场 remove_actor() + stop()
  ↓
"不再守护" —— 机器人彻底停止工作
```

**V6 也有这个 Bug**，但因为 V6 没有 Heartbeat，挂单本就活不过 10 秒（交易所自动取消），用户测试 V6 时恰好是 0 订单所以没触发。V7 的 Heartbeat 让订单能存活，但 Heartbeat 一旦出问题就会触发全量放弃。

**修复方案**：

| 修复点 | 修改前 | 修改后 |
|--------|--------|--------|
| `_check_abandon()` | 调 `open_orders()`，无买单就 `remove_actor()` 放弃 | **始终通知 Actor（EXTERNAL_CANCEL 事件），让 Actor 冷却后重挂** |
| `discover()` | 单次无买单即放弃 | **多周期检测：连续 3 次 discover（90s）无买单才放弃** |
| 新增 | — | `_abandon_pending: Dict[str, int]` 跟踪每个资产连续无买单次数 |

**新逻辑**：
1. WS 撤单事件 → 通知 Actor → Actor 冷却 120s → 重新下单
2. 只有连续 3 次 discover（间隔 30s × 3 = 90s）都无买单 → 真正放弃
3. 中间任何时候订单重新出现 → 重置计数器

### 5.2 P1：Actor 审计冗余 API 调用

**问题**：`audit()` 时 Guardian 已调用一次 `open_orders()`，每个 RESTING 状态的 Actor 又各自调用一次。N 个 Actor = N+1 次 API 调用。

**修复**：Guardian 将订单映射通过 `AUDIT` 事件 payload 传递给 Actor，Actor 直接查找本地 map。

### 5.3 P1：异步回调线程修改 Actor 状态

**问题**：`_handle_cancel_result` / `_handle_place_result` 在线程池回调线程中直接修改 `active_id`、`active_price` 等状态，与 Actor 事件循环线程存在竞态。

**修复**：新增 `CANCEL_DONE` / `PLACE_DONE` 事件类型。回调线程通过 `post()` 将结果投递回 Actor 事件队列，保持状态修改的串行性。

### 5.4 P1：首次 PING 导致 WS 拒绝连接

**问题**：`_ws_on_open()` 先启 ping 线程（立即发 PING），后发 auth。刚建立连接就收到未认证的 PING，Polymarket 服务器断开连接。

**修复**：先发 auth，后启 ping 线程；ping 循环内先 sleep 再发 PING（与 V6 行为一致）。

### 5.5 完整 Bug 修复清单

| 级别 | 问题 | 表现 | 修复 |
|------|------|------|------|
| **P0** | 交易所自动取消 → mass abandon | 全部订单消失、不再守护 | `_check_abandon` 改为通知 Actor；`discover` 多周期检测 |
| **P0** | 缺少 REST Heartbeat | 挂单 10~15 秒被取消 | 新增 `HeartbeatManager` |
| **P0** | `self.actors` 线程不安全 | 竞态崩溃 | 全部改为加锁访问器 |
| **P0** | 撤单失败静默忽略 | 重复下单 | 检查 Future 结果，失败保持 RESTING |
| **P1** | Actor 审计冗余 API 调用 | N+1 次 API 调用 | Guardian 传入订单映射 |
| **P1** | 回调线程修改状态 | 竞态条件 | CANCEL_DONE/PLACE_DONE 事件回传队列 |
| **P1** | FOK 卖出无回退 | 低流动性卖不掉 | FOK→FAK 回退链 |
| **P1** | 价格未对齐 tick_size | 下单被拒 | `round_to_tick()` |
| **P1** | 部分成交放弃订单 | 未卖出剩余仓位 | TRADE_MATCHED 保持 RESTING |
| **P1** | 余额重试仅 1 次 | 链上延迟导致卖出失败 | 使用配置的 `balance_retries` |
| **P1** | PING 时序错误 | WS 立即断开 | 先 auth 后 ping；先 sleep 后发 PING |
| **P2** | Heartbeat 400/timezone Bug | 心跳失败 | 修正端点 URL + timezone.utc + 400 恢复 |
| **P2** | WS 递归线程泄漏 | 线程数增长 | 单线程 while 循环重连 |
| **P2** | 缓存无限增长 | 内存泄漏 | `_prune_caches()` 定期清理 |

---

## 六、配置说明

| 配置组 | 字段 | 默认值 | 说明 |
|--------|------|--------|------|
| Maker | `maker_size` | 50 USDC | 每次挂单数量 |
| | `maker_rank` | 3 | 挂在买盘第几档 |
| | `maker_cooldown` | 120s | 撤单后冷却时间 |
| 执行 | `exec_interval` | 0.2s | REST 写操作限流间隔 |
| | `place_retries` | 2 | 下单重试次数 |
| 心跳 | `heartbeat_interval` | 7.0s | 小于 10s 安全阈值 |
| | `heartbeat_max_errors` | 3 | 连续失败告警阈值 |
| 卖出 | `sell_fallback_order_types` | ("FOK", "FAK") | FOK 失败降级 FAK |
| | `sell_retries` | 8 | 最大重试次数 |
| 守护 | `discover_interval` | 30s | 发现新订单间隔 |
| | `audit_interval` | 120s | 纠偏检查间隔 |
| | `ABANDON_CYCLES` | 3 | 连续 N 次无买单才放弃 |
| | `stale_timeout` | 60s | 状态卡死超时阈值 |
| WS | `ws_reconnect_delay` | 5.0s | 断线重连等待 |
| | `user_ping_interval` | 50.0s | 用户频道保活 |
| | `market_ping_interval` | 10.0s | 市场频道保活 |

---

## 七、文件清单

```
guardian_v7/
├── main.py           # 入口：日志设置 → Config → Guardian.run()
├── config.py         # 配置：冻结 dataclass + 环境变量加载
├── models.py         # 模型：ActorState, EventType, OrderInfo 等
├── utils.py          # 工具：safe_float, round_to_tick 等
├── heartbeat.py      # 心跳：HeartbeatManager 守护线程
├── execution.py      # 执行层：限流 + 幂等 + ThreadPoolExecutor
├── actor.py          # Actor：单市场状态机（异步执行 + 安全回调）
├── ws_manager.py     # WS 管理：连接生命周期 + 单线程重连
├── ws_router.py      # WS 路由：消息解析 + 分发
├── guardian.py       # 主控：定时循环 + 多周期放弃 + 线程安全
├── data/             # 日志输出目录
└── tests/            # 79 个单元测试
    ├── test_utils.py
    ├── test_models.py
    ├── test_config.py
    ├── test_heartbeat.py
    ├── test_execution.py
    ├── test_actor.py
    └── test_guardian.py
```

---

## 八、运行指南

### 环境变量（.env）

```ini
PK=0x...                    # 私钥（必填）
CLOB_API_KEY=...            # L2 认证（必填）
CLOB_SECRET=...             # L2 签名密钥（必填）
CLOB_PASS_PHRASE=...        # L2 密码短语（必填）
PROXY_ADDRESS=0x...         # 代理合约地址（可选，EOA 留空）
```

### 启动

```bash
cd guardian_v7
python main.py
```

### 健康检查

观察日志确认以下指标：
- 每 7s 出现 `POST /v1/heartbeats "HTTP/2 200 OK"`
- 无 `[USER WS ERR]` 或 `[USER WS] close` 错误
- `[DISCOVER] 守护 N 个市场` 中 N 不为 0（如果有挂单）
- 无 `[ABANDON]` 消息

### 停止

`Ctrl+C` → 优雅关闭：
1. 取消所有活跃订单
2. 停止所有 Actor
3. 断开 WebSocket
4. 停止心跳
