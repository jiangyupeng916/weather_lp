# Guardian V7 架构文档

## 一、项目概述

Guardian V7 是一个 **Polymarket CLOB 交易平台的 Maker-only 自动化做市机器人**。

**核心目的**：挂单提供流动性以获取平台返利奖励。机器人在订单簿买盘第 N 档挂限价买单，best_bid 变化时撤单重挂。买入成交后，由 `check_positions()` 定时扫描持仓并补挂同价限价卖单平仓。

**关键行为规则**：

- **系统撤单**（best_bid 变化 / target_price 变化 / WSS 重连）：Actor 冷却 120s → 重新挂单
- **外部撤单**（用户手动 / 交易所自动取消）：统一走 Actor 通知 → 冷却重挂；真正需要放弃的市场由 `discover()` 多周期检测清理
- **卖出**：不依赖 WS 事件驱动，由 `check_positions()` 每 120s 定时扫描持仓 → 补挂限价卖单

**技术栈**：Python 3.12+ | `py_clob_client_v2` | `websocket-client` | `eth_account` | `requests`

**策略文档**：详细逻辑见 [LOGIC.md](./LOGIC.md)。

***

## 二、体系结构

### 2.1 模块依赖关系（零循环导入）

```
config.py   ← 无内部依赖（冻结 dataclass，环境变量加载）
models.py   ← 无内部依赖（枚举、数据类）
utils.py    ← 无内部依赖（纯函数工具）
    │
    ├── heartbeat.py   ← config
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
                  │ HeartbeatManager│ │ ExecutionLayer    │
                  │ (每7s心跳)      │  │ (限流+线程池)      │
                  │ 维持订单存活     │  │ place/cancel/     │
                  │                │  │ limit_sell        │
                  └────────────────┘  └────────┬──────────┘
                                               │ Future
  ┌──────────────┐              ┌──────────────┤
  │ Market WS ◄──┼──────────────┤              │
  │ book 快照     │              │              │
  │ best_bid_ask │    ┌─────────▼──────────┐   │
  │ price_change │    │  WSRouter          │   │
  │ tick_size    │    │  解析JSON → 分发    │   │
  │ resolved     │    └──┬──────────┬──────┘   │
  └──────────────┘       │          │          │
                         │          │          │
  ┌──────────────┐       │          │          │
  │ User WS  ◄───┼───────┘          │          │
  │ trade 事件    │                  │          │
  │ (MATCHED/    │     ┌────────────▼────┐     │
  │  CONFIRMED/  │     │ Guardian 主控   │     │
  │  FAILED)     │     │ discover(30s)  │     │
  │ order 事件    │     │ audit(120s)    ├─────┘
  │ (PLACEMENT/  │     │ positions(120s)│
  │  CANCELLATION│     │ prune(300s)    │
  └──────────────┘     └───────┬────────┘
                               │ post(ActorEvent)
                       ┌───────▼────────┐
                       │ AssetActor×N   │
                       │ 串行状态机      │
                       │ 每市场一个实例   │
                       └────────────────┘
```

***

## 三、核心组件详解

### 3.0 卖出策略 — 定时扫描持仓（不依赖 WS 事件）

**卖出唯一路径**：`check_positions()` 每 120s 定时扫描，不依赖 WS trade 事件触发。

数据流：

```
check_positions() 每 120s 执行：
    │
    ├─ 1. 查询 Data API 持仓列表
    │
    ├─ 2. 查询 open_orders() 找出已有卖单的 token_id
    │
    ├─ 3. 对每个持仓：
    │     ├─ 已有卖单 → 跳过
    │     ├─ 正在卖出中 → 跳过（并发保护）
    │     └─ 无卖单 → onchain_balance() → limit_sell(avgPrice, balance)
    │           ├─ 成功 → trade_logger 记录 buy/sell 配对
    │           └─ 失败 → 记录错误日志，下轮重试
    │
    └─ 4. 并发保护：_selling set + _sell_lock
```

**关键设计**：
- 简单可靠：不依赖 WS CONFIRMED → 立即卖出的复杂链路
- 天然去重：查询已有卖单后再决定是否补挂
- 自愈能力：重启/事件丢失后最多 120s 自动补挂
- 数据安全：查询 open_orders 过滤已有卖单，查询 onchain_balance 确认余额后才下单

`handle_trade()` 仅记录 CONFIRMED 事件到 trade_logger 用于审计，不触发任何卖出动作。

### 3.1 config.py — 配置模块

冻结 `dataclass`，所有字段从环境变量加载，`__post_init__` 做校验。

**核心参数**：

| 配置组 | 字段 | 默认值 | 说明 |
|--------|------|--------|------|
| Maker | `maker_size` | 50 USDC | 每次挂单数量 |
| | `maker_rank` | 3 | 挂在买盘第几档 |
| | `maker_cooldown` | 120s | 撤单后冷却时间 |
| | `tick_size` | 0.01 | 价格最小变动单位 |
| 执行 | `exec_interval` | 0.2s | REST 写操作限流间隔 |
| | `place_retries` | 2 | 下单重试次数 |
| | `place_retry_delay` | 1.0s | 下单重试等待 |
| | `max_workers` | 10 | 线程池最大工作线程 |
| 超时 | `cancel_timeout` | 10.0s | 撤单 Future 等待超时 |
| | `place_timeout` | 15.0s | 下单/卖单 Future 等待超时 |
| 心跳 | `heartbeat_interval` | 7.0s | 小于 10s 安全阈值 |
| | `heartbeat_max_errors` | 3 | 连续失败告警阈值 |
| 定时 | `discover_interval` | 30s | 发现新订单间隔 |
| | `audit_interval` | 120s | 纠偏检查间隔 |
| | `position_interval` | 120s | 持仓扫描卖出间隔 |
| | `cache_prune_interval` | 300s | 缓存清理间隔 |
| | `stale_timeout` | 60s | PLACING/CANCELING/NO_ORDER 卡死超时 |
| WS | `ws_reconnect_delay` | 5.0s | 断线重连等待 |
| | `user_ping_interval` | 50.0s | 用户频道 PING 保活 |
| | `market_ping_interval` | 10.0s | 市场频道 PING 保活 |
| 卖出 | `position_threshold` | 1.0 | 最小卖出余额阈值 |

### 3.2 models.py — 数据模型

- **`ActorState`**：`NO_ORDER → PLACING → RESTING → CANCELING → COOLING → STOPPED`
- **`EventType`**：13 种事件类型，含 2 个内部事件（`CANCEL_DONE`、`PLACE_DONE`）
- **`OrderInfo`** / **`TradeRecord`** / **`MarketInfo`**：纯数据载体
- **`PlaceRequest`** / **`CancelRequest`**：执行层入参

### 3.3 utils.py — 工具函数

| 函数 | 功能 |
|------|------|
| `safe_float(v, default)` | 安全转 float，失败返回 default |
| `safe_decimal(v)` | 安全转 Decimal，失败返回 None |
| `safe_float_from_decimal(d)` | `float(str(d))` 避免 `Decimal→float` 精度丢失 |
| `round_to_tick(price, tick_size)` | 四舍五入到 tick_size 倍数 |
| `retry_call(fn, retries, delay)` | 带重试的函数调用包装 |

### 3.4 heartbeat.py — 心跳保活

**作用**：Polymarket 要求每 ~10 秒通过 `POST /v1/heartbeats` 发送心跳，否则**全部挂单被交易所自动取消**。这是 V6 完全缺失的关键功能。

**协议**：
```
首次:  POST {"heartbeat_id": ""}           → 200 {"heartbeat_id": "xxx"}
后续:  POST {"heartbeat_id": "xxx"}        → 200 {"heartbeat_id": "yyy"}
过期:  POST {"heartbeat_id": "expired"}   → 400 {"heartbeat_id": "correct_id", ...}
```

**三层容错策略**：
1. 优先 SDK `client.post_heartbeat()`
2. SDK 失败 → 从错误消息正则提取 `heartbeat_id` → 回退 L2 认证头直接 HTTP
3. 400 响应 → 提取正确 ID，更新后重试；401/403 → 直接抛出
4. 连续失败 ≥3 次 → CRITICAL 告警

**生命周期**：启动时最先 start，关闭时最后 stop（确保订单存活窗口覆盖全部运行时间）。

### 3.5 execution.py — 执行层

所有 REST 写操作通过此层，保证：

| 特性 | 机制 |
|------|------|
| 限流 | `_rate_wait()`：两次写操作至少间隔 `exec_interval` 秒 |
| 幂等-下单 | `_place_tokens`（TTL 300s）：同一 `asset+price` 不重复下单 |
| 幂等-撤单 | `_cancel_tokens`：同一 order_id 不重复撤单 |
| 异步 | 全部方法返回 `Future`，由 `ThreadPoolExecutor`（≤10 workers）执行 |
| 价格对齐 | `place()` 内调用 `round_to_tick()` + `PartialCreateOrderOptions(tick_size=...)` |

**公共方法**：

| 方法 | 用途 | 返回 |
|------|------|------|
| `place(asset_id, price, size, tick_size)` | 下限价买单（BUY） | `Future[Optional[str]]` order_id |
| `cancel(order_id, reason)` | 撤单 | `Future[bool]` |
| `limit_sell(asset_id, price, size, tick_size)` | 下限价卖单（SELL, GTC） | `Future[Optional[str]]` order_id |
| `clear_place(asset_id, price)` | 清除下单幂等 token | — |
| `clear_place_by_asset(asset_id)` | 清除某资产全部下单 token | — |
| `is_system_cancel(order_id)` | 判断是否为系统撤单（一次性） | bool |

### 3.6 actor.py — 单市场状态机

每个被守护的市场一个 `AssetActor` 实例，在独立守护线程中**串行**处理事件队列。

**状态机**：

```
                        ┌──────────┐
               ┌───────►│ NO_ORDER │◄──────────┐
               │        └─────┬────┘           │
               │   _target_price │             │ _start_cooldown()
               │   有报价时自动挂单 │             │ 或 审计强制重挂
               │        ┌─────▼────┐           │
               │        │ PLACING  │           │
               │        └─────┬────┘           │
               │     PLACE_DONE │             │
               │        (ok+oid)│             │
               │        ┌─────▼────┐           │
               │        │ RESTING  ├───────────┤ best_bid 变化
               │        └──┬───┬──┘           │ target_price 变化
               │           │   │              │ WSS 重连
               │  成交匹配  │   │ 撤单         │
               │  (保持不变) │   │              │
               │           │   │              │
               │        ┌──▼───▼──┐           │
               │        │CANCELING│           │
               │    ┌───┤  (异步) ├───┐       │
               │    │   └─────────┘   │       │
               │    │ CANCEL_OK       │ CANCEL_FAIL
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

**事件驱动设计**：

- `place()` / `cancel()` 返回 `Future`，回调线程将结果 `post()` 回 Actor 队列
- `CANCEL_DONE` / `PLACE_DONE` 内部事件确保状态修改在 Actor 线程内串行执行
- 撤单失败时保持 RESTING（订单仍存活于交易所），不清除 `active_id`
- 成交后保持 RESTING（部分成交≠全部成交），由审计验证实际订单状态

**数据源合并**：

`self.bids` 由两类 WS 事件共同维护：
- `book`（订单簿快照）：全量替换 `self.bids`
- `price_change`（增量更新）：插入/删除单个报价档位

**关键行为**：

| 场景 | 行为 |
|------|------|
| best_bid 首次接收到 | 仅记录，不触发动作 |
| best_bid 变化 + RESTING | 撤单重挂 |
| best_bid 变化 + PLACING | 设 `_pending_reeval` 标志，下单完成后立即撤单 |
| best_bid 变化 + NO_ORDER | 启动冷却 |
| target_price 变化 + RESTING | `_check_target_changed()` 触发撤单 |
| 外部撤单确认 | 清除 active_id，进入 COOLING |

### 3.7 ws_manager.py + ws_router.py — WebSocket 层

**WSManager**（连接生命周期管理）：

- `_run_ws()`：单线程 while 循环处理 连接→断开→重连，消除递归线程泄漏
- `_ws_on_open()`：先执行业务回调（auth/订阅），再启动 PING 线程
- PING 线程：先 sleep 后发 PING（避免刚连接立即发 PING 被服务器拒绝）
- 自动读取 `HTTP_PROXY`/`HTTPS_PROXY` 环境变量配置代理
- `start_market()` / `start_user()` 带互斥锁，同频道只有一个连接线程
- `market_send()`：线程安全地向市场频道发送订阅消息

**WSRouter**（消息分发）：

- 市场频道：

| event_type | 处理 |
|------------|------|
| `book` | → Actor `BOOK_SNAPSHOT` |
| `price_change` | → Actor `PRICE_CHANGE`（逐条） |
| `best_bid_ask` | → Actor `BEST_BID` |
| `tick_size_change` | → Actor `TICK_SIZE` |
| `market_resolved` | → Actor `STOP` → `remove_actor()` |

- 用户频道：

| event_type | 处理 |
|------------|------|
| `trade`（MATCHED/CONFIRMED/FAILED） | → `Guardian.handle_trade()` |
| `order`（PLACEMENT/CANCELLATION） | → `Guardian.handle_order()` |
| `channel: "user"`（initial_dump） | → 所有历史 trade 路由到 `handle_trade()` 统一处理 |

### 3.8 guardian.py — 主控制器

**定时任务**：

| 任务 | 间隔 | 功能 |
|------|------|------|
| `discover()` | 30s | 发现新订单创建 Actor、多周期检测清理废弃 Actor |
| `audit()` | 120s | 纠偏超价订单（price ≥ best_bid 即撤单）、检测订单丢失、状态卡死重置 |
| `check_positions()` | 120s | **唯一卖出路径**：查持仓 → 已有卖单跳过 → 无卖单补挂限价卖单 |
| `_prune_caches()` | 300s | 清理过期 `_ob_cache`、`_market_info`、`_processed_trades`（按时间戳有序淘汰） |

**discover() 多周期放弃逻辑**：

```
有买单 → 重置对应资产的 abandon 计数器
无买单 + 有活跃 Actor → 可能 API 异常，跳过清理
无买单 + 无活跃 Actor → 对每个 NO_ORDER 状态 Actor 累计计数
  → 连续 ABANDON_CYCLES 次（默认 3×30s=90s）→ 放弃该市场
  → COOLING 状态不计数（有定时器等待重挂）
```

- 不再有 `_check_abandon()` 立即放弃路径，所有非系统撤单统一走 Actor 冷却重挂
- `audit()` 在全局无买单时额外清理 NO_ORDER 状态的 Actor

**handle_trade() — 仅记录日志**：

```
任何 trade 事件 → 仅处理 status == "CONFIRMED"
  ├─ BUY CONFIRMED  → trade_logger 记录 buy_confirmed
  └─ SELL CONFIRMED → trade_logger 记录 sell_confirmed
```

- 不触发卖出动作，不存储中间状态
- `_processed_trades` 字典（tid → timestamp）按时间戳有序去重，`_prune_caches` 淘汰最旧条目

**handle_order() — 订单事件处理**：

```
系统撤单 → cancel_logger 记录 → Actor EXTERNAL_CANCEL → COOLING → 重挂
外部撤单 → cancel_logger 记录 → Actor EXTERNAL_CANCEL → COOLING → 重挂
          （统一处理，不区分人工/交易所，不再放弃市场）
```

**check_positions() — 唯一卖出路径**：

```
获取持仓 → 查询 open_orders() → 找到已有的 SELL 订单
  ├─ 已有卖单 → 跳过
  └─ 无卖单 → onchain_balance() → limit_sell(avgPrice, balance)
```

- 并发保护：`_selling` set + `_sell_lock`
- 自愈：重启/WS 断线后最多 120s 自动补挂卖单

**主循环启动顺序**：
1. heartbeat.start() — 心跳最先启动
2. WS 启动 + 用户频道认证
3. discover() — 发现已有订单
4. 市场频道订阅
5. 主循环（四定时任务）
6. `_shutdown()`：取消活跃订单 → 停止 Actor → 断 WS → 停工线程池 → 停心跳

***

## 四、线程模型

### 4.1 线程清单

| 线程 | 数量 | 持久性 | 用途 |
|------|------|--------|------|
| MainThread | 1 | 持久 | 主循环 |
| heartbeat | 1 | 持久 | REST 心跳 |
| ws-market | 1 | 持久 | 市场 WS（单线程重连） |
| ws-user | 1 | 持久 | 用户 WS（单线程重连） |
| ws-ping | 2 | 持久 | market/user 频道 PING |
| Actor-* | N | 持久 | 每市场一个事件循环 |
| exec-N | ≤10 | 持久 | ThreadPoolExecutor 工作线程 |
| 回调线程 | 短期 | M | place/cancel Future 结果回传 |

### 4.2 锁层级（防死锁）

```
获取顺序（严格单向）：

1. _actors_lock        (Guardian RLock)     — 最外层
2. _trade_lock         (Guardian Lock)
3. _cache_lock         (Guardian RLock)     — 缓存读写
4. _sell_lock          (Guardian Lock)      — 最内层

+ _state_lock          (每个 Actor RLock)  — 独立，不与上述交叉
+ ExecutionLayer 内部锁                     — 独立，不与上述交叉
```

### 4.3 线程安全关键设计

- **Actor 事件队列**：`CANCEL_DONE`/`PLACE_DONE` 通过 `post()` 投递，保证状态修改在 Actor 线程内执行
- **Actor 生命周期**：`post()` 检查 `_running`，阻止向已停止的 Actor 投递事件
- **WS 重连互斥**：`_market_lock`/`_user_lock` 保证同频道只有一个连接线程
- **Actor 管理**：`get_actor()/add_actor()/remove_actor()` 全部加锁保护

***

## 五、撤单处理机制

### 5.1 系统撤单（bot 主动撤单后重挂）

```
best_bid 变化 / target_price 变化 / WSS 重连
    │
    ▼
Actor._cancel(reason)
    ├─ state → CANCELING
    └─ ExecutionLayer.cancel(oid, reason) → Future
          ├─ 成功 → CANCEL_DONE(ok=True)
          │   └─ _on_cancel_done: NO_ORDER → COOLING(120s)
          │       └─ 120s 后 COOLDOWN_EXPIRED → _target_price() → place
          └─ 失败 → CANCEL_DONE(ok=False)
              ├─ active_id 已被清除 → 忽略（订单已不存在）
              └─ active_id 仍存在 → 保持 RESTING（订单仍存活）
```

### 5.2 外部撤单（统一冷却重挂）

```
外部来源 CANCELLATION 事件（用户手动 / 交易所自动取消）
    │
    ▼
User WS → Guardian.handle_order()
    ├─ is_system_cancel(oid)? → 否
    ├─ cancel_logger 记录（source="external"）
    └─ actor.post(EXTERNAL_CANCEL)
          └─ Actor: NO_ORDER → COOLING(120s) → 重挂

与 5.1（系统撤单）处理路径完全相同，不区分来源。
```

- 不再区分"人工撤单"和"交易所自动取消"，统一走保守策略
- 由 `discover()` 多周期逻辑检测真正需要放弃的市场（连续 3 次无买单 = 90s 后放弃）

### 5.3 触发方式对比

| 触发方式 | 处理流程 | 是否放弃市场 |
|----------|----------|-------------|
| best_bid 变化 | `_cancel()` → COOLING → 重挂 | ❌ 不放弃 |
| 外部撤单（任意来源） | Actor EXTERNAL_CANCEL → COOLING → 重挂 | ❌ 不放弃 |
| discover 多周期无买单 | 3 次 NO_ORDER → `remove_actor()` + `stop()` | ✅ 放弃 |
| market_resolved | actor.stop() → remove_actor() | ✅ 放弃 |

***

## 六、版本变更记录

### V7.1（2026-07-22）：代码审查整改

**死代码清理**：删除 `_sell()` / `sell_position()` / `mark_trade_processed()` / `market_sell()` 及关联配置。

**11 个 Bug 修复**：

| 级别 | 问题 | 修复 |
|------|------|------|
| P0 | 下单成功但 order_id 为空，幂等 token 未清除 | `_do_place` 空 oid 走失败路径清除 token |
| P0 | 撤单失败误放弃市场（`_system_cancels` 被清除） | 撤单失败保留 `_system_cancels`，仅清 `_cancel_tokens` |
| P0 | `_processed_trades` 清理无序（set→list 随机） | 改为 `Dict[tid, timestamp]`，按时间戳有序淘汰 |
| P1 | 卖单成交（SELL）未被记录到 trade_logger | 增加 SELL CONFIRMED 日志路径 |
| P1 | MATCHED 事件 WS 重放时未去重 | `_pending_sells` 检查避免重复处理（后随链路移除） |
| P1 | WS 引用更新无锁（`_market_ws` 竞态） | `_market_lock` 保护读写 |
| P1 | `_market_info`/`_ob_cache` 多线程无锁 | 新增 `_cache_lock`（RLock） |
| P2 | `cancel_timeout` 被复用为下单等待超时 | 新增 `place_timeout=15s` |
| P2 | 每次 CONFIRMED 创建新线程 | 改用 `exec_layer.submit()` 复用线程池（后随链路移除） |
| P2 | 非系统撤单立��放弃市场（`_check_abandon`） | 删除 `_check_abandon`，统一走 Actor 冷却重挂 |
| P2 | `maker_rank` 默认值三方不一致 | 统一为 3 |

### V7.2（2026-07-23）：卖出路径简化

- 删除事件驱动卖出链路：`_pending_sells` / `_on_trade_matched` / `_on_trade_confirmed` / `_on_trade_failed` / `_process_trade_sell`
- `handle_trade()` 简化为仅记录 CONFIRMED 日志
- `check_positions()` 成为唯一卖出路径
- 删除 `exec_layer.submit()`（无调用方）
- Actor 移除 `_on_trade_matched` 处理器

### 历史修复（V7.0 之前）

| 级别 | 问题 | 修复 |
|------|------|------|
| P0 | 交易所自动取消 → mass abandon | `discover` 多周期+排除 COOLING |
| P0 | 缺少 REST Heartbeat | 新增 `HeartbeatManager` |
| P0 | Actor 字典线程不安全 | 全部改为加锁访问器 |
| P0 | 撤单失败静默忽略 | 检查 Future，失败保持 RESTING |
| P1 | 下单失败幂等 token 未清理 | `_place_tokens` 双重清理 |
| P1 | audit N+1 API 调用 | Guardian 传入 order_map |
| P1 | 回调线程修改状态 | CANCEL_DONE/PLACE_DONE 回传队列 |
| P1 | `_on_cancel_done` 竞态 | active_id+COOLING 双重防护 |
| P1 | WS ping 时序错误 | 先 auth 后 ping |
| P2 | Heartbeat 400→401 级联 | SDK 错误正则提取 + raw REST 回退 |
| P2 | WS 递归线程泄漏 | 单线程 while 循环重连 |
| P2 | 缓存无限增长 | `_prune_caches()` 定期清理 |
| P2 | market_resolved 顺序错误 | 先 stop 后 remove |
| P2 | limit_sell 未对齐 tick_size | `round_to_tick` 对齐 |

***

## 七、文件清单与职责

```
guardian_v7/
├── main.py           # 入口：日志初始化 → 配置加载 → Guardian.run()
├── config.py         # 配置：冻结 dataclass，从 .env 加载全部参数，validate() 校验必填
├── models.py         # 模型：ActorState(6状态)、EventType(13事件)、OrderInfo、TradeRecord 等
├── utils.py          # 工具：safe_float/safe_decimal 安全转换、round_to_tick 价格对齐、retry_call 重试
├── heartbeat.py      # 心跳：HeartbeatManager 守护线程，SDK→raw REST 双重保障，heartbeat_id 链式恢复
├── execution.py      # 执行层：全局限流 + 幂等令牌 + ThreadPoolExecutor，place/cancel/limit_sell
├── actor.py          # Actor：单市场串行状态机，事件队列驱动，异步回调 → 内部事件回传
├── ws_manager.py     # WS 管理：市场/用户双频道，单线程重连，代理支持，PING 保活
├── ws_router.py      # WS 路由：JSON 解析 → Guardian 分发，initial_dump 完整处理
├── guardian.py       # 主控：定时循环 + 多周期放弃 + trade 日志 + check_positions 卖出 + 线程安全管理
├── LOGIC.md          # 策略逻辑文档（详细行为描述）
├── ARCHITECTURE.md   # 架构文档（本文件）
├── data/             # 日志输出目录
│   ├── guardian.log  # 主日志
│   ├── trades.log    # 买卖配对记录
│   ├── cancels.log   # 撤单记录（区分 system/external）
│   └── abandons.log  # 放弃市场记录
└── tests/            # 78 个单元测试
    ├── test_utils.py
    ├── test_models.py
    ├── test_config.py
    ├── test_heartbeat.py
    ├── test_execution.py
    ├── test_actor.py
    └── test_guardian.py
```

### 各模块职责速查

| 文件 | 所有者 | 核心职责 | 对外接口 |
|------|--------|----------|----------|
| `main.py` | 用户 | 启动程序 | `main()` |
| `config.py` | 全局 | 环境变量 → 冻结配置 | `Config()` dataclass |
| `models.py` | 全局 | 类型定义 | `ActorState`, `EventType`, `OrderInfo` 等 |
| `utils.py` | 全局 | 无副作用工具函数 | `safe_float()`, `round_to_tick()`, `retry_call()` |
| `heartbeat.py` | Guardian | 保持订单存活 | `start()`, `stop()` |
| `execution.py` | Guardian/Actor | 限流异步执行 | `place()`, `cancel()`, `limit_sell()`, `clear_place()`, `is_system_cancel()` |
| `actor.py` | Guardian | 单市场挂单逻辑 | `post()`, `stop()`, `force_stop()`, `state`, `active_id` |
| `ws_manager.py` | Guardian | WS 连接生命周期 | `start()`, `stop()`, `start_market()`, `start_user()`, `market_send()` |
| `ws_router.py` | Guardian | WS 消息分发 | `on_market_message()`, `on_user_message()` |
| `guardian.py` | main.py | 全盘协调 | `run()`, `handle_trade()`, `handle_order()`, `discover()` |

***

## 八、运行指南

### 环境变量（.env）

```ini
PK=0x...                    # 私钥（必填）
CLOB_API_KEY=...            # L2 API Key（必填）
CLOB_SECRET=...             # L2 API Secret（必填）
CLOB_PASS_PHRASE=...        # L2 Passphrase（必填）
PROXY_ADDRESS=0x...         # 代理合约地址（可选，EOA 钱包留空）
CLOB_API_URL=...            # CLOB API 地址（可选，默认 https://clob.polymarket.com）
```

### 启动

```bash
cd guardian_v7
python main.py
```

### 健康检查

观察控制台/guardian.log：

- 启动后立即出现 `[HEARTBEAT] 已启动 interval=7.0s`
- 每 30s `[DISCOVER] 守护 N 个市场`，N 不为 0
- 每 7s 心跳成功（debug 级别显示 `[HEARTBEAT] OK`）
- 无连续 `[HEARTBEAT] 失败` 或 `[ABANDON]` 消息
- 无 `[WS ERR]` 或频繁 `[WS] close code=...`

### 停止

`Ctrl+C` → 优雅关闭序列：

1. 取消所有活跃订单
2. 停止所有 Actor（`force_stop()`）
3. 断开 WebSocket 连接
4. 关闭线程池（`shutdown(wait=True)`）
5. 停止心跳

### 日志文件

| 文件 | 内容 |
|------|------|
| `data/guardian.log` | 全部运行日志（控制台同步输出） |
| `data/trades.log` | 买卖配对：每笔买入+卖出成对记录 |
| `data/cancels.log` | 撤单记录：系统撤单/人工撤单区分 source |
| `data/abandons.log` | 放弃记录：asset_id + market title + 原因 |
