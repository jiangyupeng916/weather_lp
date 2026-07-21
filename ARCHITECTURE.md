# Guardian V7 架构文档

## 一、项目概述

Guardian V7 是一个 **Polymarket CLOB 交易平台的 Maker-only 自动化做市机器人**。

**核心策略**：监控订单簿买盘，在第 N 档（默认第 3 档）挂限价买单。best_bid（第 1 档）变化时撤单，冷却 120s 后重挂。买入成交后等待链上结算确认（CONFIRMED），然后挂同价同量限价卖单（GTC）。

**关键行为规则**：

- **系统撤单**（best_bid 变化 / target_price 变化 / WSS 重连）：Actor 冷却 120s → 重新挂单
- **人工撤单**（用户在 Polymarket GUI 手动取消订单）：**放弃该市场，不再守护**
- **交易所自动取消**（心跳超时等）：由 `discover()` 多周期检测清理；正常情况下心跳保活机制防止发生

**技术栈**：Python 3.12+ | `py_clob_client_v2` | `websocket-client` | `eth_account` | `requests`

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

### 3.0 卖出策略 — 限价卖单（非市价抛售）

**买入成交后不再市价卖出**，而是等待链上结算确认后挂同价同量限价卖单。

数据流：

```
User WS trade 事件
    │
    ├─ status=MATCHED
    │   └─ 存入 _pending_sells（记录 fill_size/price/outcome）
    │   └─ 通知 Actor（保持 RESTING，不清除订单）
    │
    ├─ status=CONFIRMED（链上已结算，余额已到账）
    │   └─ _on_trade_confirmed() → _process_trade_sell()
    │       └─ exec_layer.limit_sell(asset_id, price=buy_price, size=fill_size, GTC)
    │           ├─ 成功 → trade_logger 记录 buy/sell 配对
    │           └─ 失败 → trade_logger 记录 FAILED
    │
    └─ status=FAILED / RETRYING
        └─ 清理 _pending_sells，不卖出
```

**关键设计**：
- `CONFIRMED` 是 WS 推送的终端状态，表示链上已达成最终性，余额确定到账，无需轮询
- `limit_sell` 价格对齐到 `tick_size` 后再下单，避免浮点精度误差被 API 拒绝
- CONFIRMED 事件可能不包含 `maker_orders` → 代码优先从 `_pending_sells` 缓存中恢复 `our_fill`

**兜底**：`check_positions()` 每 120s 扫描 Data API 持仓 + `open_orders()`，若某持仓无对应卖单，则补挂同价同量限价卖单（见 3.5）。

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
| 心跳 | `heartbeat_interval` | 7.0s | 小于 10s 安全阈值 |
| | `heartbeat_max_errors` | 3 | 连续失败告警阈值 |
| 定时 | `discover_interval` | 30s | 发现新订单间隔 |
| | `audit_interval` | 120s | 纠偏检查间隔 |
| | `position_interval` | 120s | 持仓兜底检查间隔 |
| | `cache_prune_interval` | 300s | 缓存清理间隔 |
| | `stale_timeout` | 60s | PLACING/CANCELING/NO_ORDER 卡死超时 |
| WS | `ws_reconnect_delay` | 5.0s | 断线重连等待 |
| | `user_ping_interval` | 50.0s | 用户频道 PING 保活 |
| | `market_ping_interval` | 10.0s | 市场频道 PING 保活 |
| 卖出 | `position_threshold` | 1.0 | 最小卖出余额阈值 |
| | `cancel_timeout` | 10.0s | Future 等待超时 |

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
| `market_sell(asset_id, amount, order_type)` | 市价卖出（FOK/FAK） | `Future[Optional[dict]]` |
| `clear_place(asset_id, price)` | 清除下单幂等 token | — |
| `clear_place_by_asset(asset_id)` | 清除某资产全部下单 token | — |
| `is_system_cancel(order_id)` | 判断是否为系统撤单（一次性） | bool |

`market_sell` 保留用于应急手动调用，主流程不使用。

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
| `check_positions()` | 120s | 兜底扫描：查持仓 + 查已有卖单 → 补挂限价卖单 |
| `_prune_caches()` | 300s | 清理过期 `_ob_cache`、`_market_info`、`_processed_trades` |

**discover() 多周期放弃逻辑**：

```
有买单 → 重置对应资产的 abandon 计数器
无买单 + 有活跃 Actor → 可能 API 异常，跳过清理
无买单 + 无活跃 Actor → 对每个 NO_ORDER 状态 Actor 累计计数
  → 连续 ABANDON_CYCLES 次（默认 3×30s=90s）→ 放弃该市场
  → COOLING 状态不计数（有定时器等待重挂）
```

- 人工撤单 → `_check_abandon()` 直接放弃（不等多周期）
- `audit()` 在全局无买单时额外清理 NO_ORDER 状态的 Actor

**handle_trade() — 买入成交处理**：

```
MATCHED   → 存入 _pending_sells，通知 Actor
CONFIRMED → 从 _pending_sells 恢复数据（如 maker_orders 缺失）
          → 异步线程 _process_trade_sell()
          → limit_sell(同价同量)
FAILED    → 清理 _pending_sells
```

- `_processed_trades` 去重 + `_pending_sells` 缓存解决 CONFIRMED 事件不含 maker_orders 的问题
- 初始 dump（`channel: "user"`）的 trade 也走完整 `handle_trade()` 链路

**handle_order() — 订单事件处理**：

```
系统撤单 → cancel_logger 记录 → Actor EXTERNAL_CANCEL → COOLING → 重挂
人工撤单 → cancel_logger 记录 → _check_abandon() → remove_actor() + abandon_logger
```

**check_positions() — 持仓兜底**：

```
获取持仓 → 查询 open_orders() → 找到已有的 SELL 订单
  ├─ 已有卖单 → 跳过
  └─ 无卖单 → onchain_balance() → limit_sell(avgPrice, balance)
```

- 用作 WS 事件丢失（如重启时未收到 CONFIRMED）的安全网
- 不再走市价卖出，改为补挂同成本限价卖单
- 并发保护：`_selling` set + `_sell_lock`

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
| 卖出线程 | 短期 | K | _process_trade_sell |

### 4.2 锁层级（防死锁）

```
获取顺序（严格单向）：

1. _actors_lock        (Guardian RLock)     — 最外层
2. _trade_lock         (Guardian Lock)
3. _pending_sells_lock (Guardian Lock)
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

### 5.2 人工撤单（放弃市场）

```
用户点击 Polymarket GUI「Cancel」
    │
    ▼
User WS → CANCELLATION 事件 → Guardian.handle_order()
    ├─ is_system_cancel(oid)? → 否
    └─ _check_abandon(oid, asset_id)  ← 新线程
          ├─ sleep(2s)  防抖
          ├─ remove_actor(asset_id)
          ├─ abandon_logger.info() → abandons.log
          └─ actor.stop(cancel_active=False)  ← 订单已取消，不重复取消
```

### 5.3 触发方式对比

| 触发方式 | 处理流程 | 是否放弃市场 |
|----------|----------|-------------|
| 用户手动取消 | `_check_abandon()` → `remove_actor()` + `stop()` | ✅ 放弃 |
| best_bid 变化 | `_cancel()` → COOLING → 重挂 | ❌ 不放弃 |
| discover 多周期无买单 | 3 次 NO_ORDER → `remove_actor()` + `stop()` | ✅ 放弃 |

***

## 六、已修复的关键 Bug

| 级别 | 问题 | 表现 | 修复 |
|------|------|------|------|
| P0 | 交易所自动取消 → mass abandon | 全部订单消失、不再守护 | `_check_abandon` 通知 Actor；`discover` 多周期+排除COOLING |
| P0 | 缺少 REST Heartbeat | 挂单 10~15s 被取消 | 新增 `HeartbeatManager` |
| P0 | Actor 字典线程不安全 | 竞态崩溃 | 全部改为加锁访问器 |
| P0 | 撤单失败静默忽略 | 重复下单 | 检查 Future，失败保持 RESTING |
| P0 | CONFIRMED 事件不含 maker_orders | 买入被静默跳过 | 从 `_pending_sells` 恢复 our_fill |
| P1 | 下单失败幂等 token 未清理 | 重挂被拦截 300s | `_on_place_done`+`_do_place` 双重清理 |
| P1 | audit N+1 API 调用 | 每个 Actor 重复查单 | Guardian 传入 order_map |
| P1 | 回调线程修改状态 | 竞态条件 | CANCEL_DONE/PLACE_DONE 回传队列 |
| P1 | `_on_cancel_done` 竞态 | 外部撤单后回 RESTING | 检查 active_id+COOLING 双重防护 |
| P1 | audit stale_timeout 打断冷却 | COOLING 60s 被重置 | 移除 COOLING 从 stale_timeout 检查 |
| P1 | PING 时序错误 | WS 立即断开 | 先 auth 后 ping；先 sleep 后发 |
| P1 | 初始 dump 不触发卖出 | 重启后漏卖 | 走完整 `handle_trade()` 链路 |
| P2 | Heartbeat 400→401 级联 | 心跳失败 | 正则提取 SDK 错误中的 heartbeat_id + raw REST 回退 |
| P2 | WS 递归线程泄漏 | 线程数增长 | 单线程 while 循环重连 |
| P2 | 缓存无限增长 | 内存泄漏 | `_prune_caches()` 定期清理 |
| P2 | `_sell()` 绕过执行层限流 | 潜在限流违规 | 主流程改用 `exec_layer.limit_sell()` |
| P2 | market_resolved 先移除后 stop | 顺序错误 | 先 stop 后 remove |
| P2 | limit_sell 价格未对齐 tick_size | 卖单被拒 | `round_to_tick` 对齐后再下单 |

***

## 七、文件清单与职责

```
guardian_v7/
├── main.py           # 入口：日志初始化 → 配置加载 → Guardian.run()
├── config.py         # 配置：冻结 dataclass，从 .env 加载全部参数，validate() 校验必填
├── models.py         # 模型：ActorState(6状态)、EventType(13事件)、OrderInfo、TradeRecord 等
├── utils.py          # 工具：safe_float/safe_decimal 安全转换、round_to_tick 价格对齐、retry_call 重试
├── heartbeat.py      # 心跳：HeartbeatManager 守护线程，SDK→raw REST 双重保障，heartbeat_id 链式恢复
├── execution.py      # 执行层：全局限流 + 幂等令牌 + ThreadPoolExecutor，place/cancel/limit_sell/market_sell
├── actor.py          # Actor：单市场串行状态机，事件队列驱动，异步回调 → 内部事件回传
├── ws_manager.py     # WS 管理：市场/用户双频道，单线程重连，代理支持，PING 保活
├── ws_router.py      # WS 路由：JSON 解析 → Guardian 分发，initial_dump 完整处理
├── guardian.py       # 主控：定时循环 + 多周期放弃 + trade/order 处理 + 持仓兜底 + 线程安全 Actor 管理
├── data/             # 日志输出目录
│   ├── guardian.log  # 主日志
│   ├── trades.log    # 买卖配对记录
│   ├── cancels.log   # 撤单记录
│   └── abandons.log  # 放弃市场记录
└── tests/            # 79 个单元测试
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
