# Guardian V7 架构文档

## 一、项目概述

Guardian V7 是一个 Polymarket CLOB（中心化限价订单簿）交易平台的 **Maker-only** 自动化交易守护程序。它通过 WebSocket 实时监控市场订单簿，在指定档位自动挂单（Maker 策略），并在成交后自动卖出持仓。

**技术栈**：Python 3.10+ | `py_clob_client_v2` | `websocket-client` | `eth_account` | `requests`

---

## 二、体系结构

### 2.1 模块架构

```
┌──────────────────────────────────────────────────────────────┐
│                         main.py                              │
│                   入口：配置 → 守护进程                         │
└──────────────────────┬───────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────┐
│                      guardian.py                             │
│                   主控制器（编排层）                            │
│   ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │
│   │ discover │  │  audit   │  │positions │  │ cache_prune │  │
│   └─────────┘  └──────────┘  └──────────┘  └─────────────┘  │
│   定时发现新单    纠偏超价订单    兜底持仓卖出     清理过期缓存   │
└──────┬──────────┬──────────┬──────────┬─────────────────────┘
       │          │          │          │
┌──────▼──┐ ┌─────▼────┐ ┌──▼──────┐ ┌─▼────────────┐
│ actor.py│ │execution │ │heartbeat│ │ ws_router.py  │
│ 状态机  │ │  .py     │ │  .py    │ │ WS 消息分发    │
│N个实例   │ │ 执行层   │ │ 心跳管理 │ │                │
└──────┬──┘ └──────────┘ └──────────┘ └──────┬─────────┘
       │                                     │
       │                              ┌──────▼─────────┐
       │                              │ ws_manager.py   │
       │                              │ WS 连接生命周期  │
       │                              │ 单线程持久重连   │
       │                              └─────────────────┘
       │
┌──────▼──────┐  ┌──────────┐  ┌──────────┐
│  config.py  │  │models.py │  │ utils.py │
│   配置      │  │ 数据模型  │  │ 工具函数  │
└─────────────┘  └──────────┘  └──────────┘
```

### 2.2 数据流图

```
               ┌─────────── REST API ──────────┐
               │                                │
    ┌──────────▼──────┐          ┌──────────────▼──────┐
    │  HeartbeatManager│          │   ExecutionLayer     │
    │  (每 7 秒心跳)    │          │  (限流 + 线程池)      │
    └──────────────────┘          └──────────┬───────────┘
                                             │ Future[result]
               ┌──────────────┐              │
    WSS ──────►│  WSRouter    ├──────────────┤
    Market Ch  │  解析JSON     │              │
    WSS ──────►│  分发事件     │    ┌─────────▼──────────┐
    User Ch    └──────┬───────┘    │   Actor[market_N]   │
                      │            │   串行状态机         │
                      │ post()     │   NO_ORDER → RESTING│
                      ├────────────►   → COOLING → ...   │
                      │            └─────────────────────┘
                      │ audit/discover
                      ├────────────► Guardian 控制器
                      │
                      │ trade/order
                      └────────────► Guardian.handle_*()
                                     └→ 卖出 → ExecutionLayer
```

### 2.3 模块依赖关系

```
config.py      ← 无内部依赖
models.py      ← 无内部依赖
utils.py       ← 无内部依赖
heartbeat.py   ← config, utils
execution.py   ← config, models, utils
actor.py       ← config, models, utils (guardian via TYPE_CHECKING)
ws_manager.py  ← config
ws_router.py   ← models (guardian via TYPE_CHECKING)
guardian.py    ← 以上所有
main.py        ← config, guardian
```

零循环导入。`actor.py` 和 `ws_router.py` 对 `Guardian` 的引用通过 `TYPE_CHECKING` 在运行时避免循环。

---

## 三、核心组件

### 3.1 Config（配置模块）

`config.py` 提供冻结的 `Config` 数据类，从环境变量加载全部配置：

| 配置组 | 关键字段 | 默认值 | 说明 |
|--------|---------|--------|------|
| 端点 | `host`, `ws_user`, `ws_market` | Polymarket 生产环境 | API/WebSocket 地址 |
| 认证 | `pk`, `api_key`, `api_secret`, `passphrase` | 从环境变量 | L1/L2 认证 |
| Maker | `maker_size=50`, `maker_rank=3`, `maker_cooldown=360s` | — | 挂单策略 |
| 执行 | `exec_interval=0.2s`, `place_retries=2` | — | REST 写操作限流 |
| 心跳 | `heartbeat_interval=7.0s`, `heartbeat_max_errors=3` | — | **新增** |
| 卖出 | `sell_fallback_order_types=("FOK", "FAK")` | — | **新增** 回退链 |

### 3.2 HeartbeatManager（心跳管理器）

**最关键的新增模块**。Polymarket CLOB API 要求每 ~10 秒通过 `POST /heartbeat` 发送心跳，否则所有未成交订单会被自动取消。

```python
# 架构
HeartbeatManager (独立守护线程)
├── 每 7 秒调用 client.post_heartbeat(heartbeat_id)
├── 维护 heartbeat_id 链式更新
├── 连续失败 3 次 → CRITICAL 告警
└── 备选方案：SDK 不可用时用 L2 认证头直接请求 REST API
```

**启动顺序**：心跳先于所有订单操作。
**关闭顺序**：所有订单取消完成后才停止心跳。

### 3.3 ExecutionLayer（执行层）

所有 REST 写操作通过此层执行，保证：

| 特性 | 实现 |
|------|------|
| 限流 | `_rate_wait()` 确保两次写操作至少间隔 `exec_interval` 秒 |
| 幂等 | `_place_tokens` (TTL=300s) 和 `_cancel_tokens` 防止重复 |
| 异步 | `place()`/`cancel()` 返回 `Future`，由 `ThreadPoolExecutor` 执行 |
| 价格对齐 | 下单前通过 `round_to_tick()` 对齐到 tick_size |

**关键改进**：撤单失败时 `Future.set_result(False)`，由 Actor 根据结果决定是否清除 `active_id`。

### 3.4 AssetActor（状态机）

每个活跃市场一个 Actor 实例，在独立线程中串行处理事件。

```
                    ┌──────────┐
           ┌───────►│ NO_ORDER │◄────────┐
           │        └─────┬────┘         │
           │              │ place()      │ 冷却到期
           │        ┌─────▼────┐         │
           │        │  PLACING │         │
           │        └─────┬────┘         │
           │              │ WSS 确认      │
           │        ┌─────▼────┐         │
           │        │ RESTING  │─────────┤ best_bid 变化 → cancel()
           │        └──┬───┬──┘         │
           │           │   │            │
           │  部分成交  │   │ 撤单        │
           │  (保持     │   │            │
           │  RESTING)  │   │            │
           │           │   │            │
           │        ┌──▼───▼──┐         │
           │        │CANCELING│         │
           │        └────┬────┘         │
           │             │ 撤单结果      │
           │             ▼              │
           │   ┌─────┐  OK → NO_ORDER   │
           │   │STOP │  FAIL → RESTING  │
           │   │PED  │                  │
           │   └─────┘         ┌────────┘
           │                   │
           │             ┌─────▼────┐
           └─────────────│ COOLING  │
                         └──────────┘
```

**关键改进**：
- **撤单失败保持 RESTING**（不清除 active_id）— 防止重复下单
- **部分成交不放弃订单** — 保持 RESTING，由审计发现实际状态
- **非阻塞执行** — place/cancel 通过 Future 异步执行，不阻塞事件循环
- **tick_size 对齐的目标价格** — `round_to_tick()` 确保价格被 SDK 接受

### 3.5 WSManager + WSRouter（WebSocket 层）

```
WSManager（连接生命周期）
├── start_market()  ─→ 单个持久线程，while 循环处理重连
├── start_user()    ─→ 单个持久线程，while 循环处理重连
├── market_send()   ─→ 向市场频道发送订阅消息
└── stop()          ─→ 关闭所有连接

WSRouter（消息分发）
├── on_market_message()  ─→ book / price_change / best_bid / tick_size / resolved
└── on_user_message()    ─→ trade (MATCHED→CONFIRMED) / order (CANCELLATION)
```

**关键修复**：单个线程内的 while 循环处理重连（而非 `on_close` 回调递归创建新线程），消除线程泄漏。

### 3.6 Guardian（主控制器）

管理全局状态和定时任务循环：

| 定时任务 | 默认间隔 | 功能 |
|----------|---------|------|
| `discover()` | 30s | 发现新挂单/清理僵尸 Actor |
| `audit()` | 120s | 纠偏（超价订单撤单）、状态卡死检测 |
| `check_positions()` | 60s | 持仓兜底（Data API 查询 → 自动卖出） |
| `_prune_caches()` | 300s | 清理 `_ob_cache`、`_market_info`、`_processed_trades` |

**线程安全的 Actor 管理**：全部 `self._actors` 字典访问通过 `get_actor()`/`add_actor()`/`remove_actor()`/`list_actors()` 方法，内部使用 `RLock` 保护。

---

## 四、线程模型

### 4.1 线程清单

| 线程名称 | 数量 | 持久/临时 | 用途 |
|----------|------|----------|------|
| MainThread | 1 | 持久 | 主循环（discover→audit→positions→prune） |
| heartbeat | 1 | 持久 | REST 心跳调用 |
| ws-market | 1 | 持久 | 市场 WebSocket 连接+重连 |
| ws-user | 1 | 持久 | 用户 WebSocket 连接+重连 |
| Actor workers | N | 持久 | 每个活跃市场的状态机事件循环 |
| exec-N | ≤10 | 持久 | 下单/撤单 REST 调用线程池 |
| 回调线程 | M | 短期 | 处理 place/cancel Future 结果 |
| 卖出线程 | K | 短期 | 处理交易卖出 |

### 4.2 锁层级（防死锁）

```
锁获取顺序（必须严格遵守，从不逆向）：

1. _actors_lock       (RLock, Guardian)
2. _trade_lock        (Lock, Guardian)
3. _pending_sells_lock(Lock, Guardian)
4. _sell_lock         (Lock, Guardian)
5. _state_lock        (RLock, 每个 Actor 独立)

ExecutionLayer 内部锁（_lock_rate, _lock_cancel, _lock_place）
与上述层级独立，永远不与 Guardian/Actor 锁交叉获取。
```

### 4.3 线程安全要点

1. **Actor dict 访问**：100% 通过锁保护的访问器方法
2. **Actor 状态**：内部 `_state_lock` (RLock) 保护所有状态读写
3. **Actor 订阅**：`post()` 方法有 `_running` 检查，避免向已停止的 Actor 投递事件
4. **执行层**：内部幂等令牌和速率限制都是线程安全的
5. **WS 重连**：互斥锁保证同一频道只有一个连接线程

---

## 五、关键 Bug 修复对照表

| 严重性 | 问题 | 修复方案 | 涉及模块 |
|--------|------|---------|---------|
| **P0** | 缺少 REST 心跳 | 新增 `HeartbeatManager` 守护线程 | `heartbeat.py`, `guardian.py` |
| **P0** | `self.actors` 线程不安全 | 全部访问改用 `get_actor()` 等加锁方法 | `guardian.py`, `ws_router.py` |
| **P0** | 撤单失败静默忽略 | Actor 检查 Future 结果，失败保持 RESTING | `actor.py` |
| **P1** | FOK 卖出无回退 | `("FOK", "FAK")` 回退链 | `guardian.py` |
| **P1** | 价格未对齐 tick_size | `round_to_tick()` + `safe_float_from_decimal()` | `utils.py`, `actor.py`, `execution.py` |
| **P1** | 部分成交放弃订单 | TRADE_MATCHED 后保持 RESTING | `actor.py` |
| **P1** | 余额重试仅 1 次 | 使用 `Config.balance_retries` 次循环 | `guardian.py` |
| **P1** | 浮点精度丢失 | `float(str(Decimal))` 中间转换 | `utils.py` |
| **P2** | 缓存无限增长 | 定期 `_prune_caches()` 清理 | `guardian.py` |
| **P2** | WS 递归线程泄漏 | 单线程 while 循环重连 | `ws_manager.py` |
| **P2** | Actor REST 阻塞事件循环 | ThreadPoolExecutor + Future 异步模式 | `execution.py`, `actor.py` |

---

## 六、安全考虑

1. **私钥管理**：私钥通过 `.env` 环境变量加载，不硬编码。确认 `.gitignore` 包含 `.env`
2. **API 凭证**：L2 凭证（apiKey/secret/passphrase）仅用于后端，不暴露到客户端
3. **输入验证**：`asset_id` 和 `token_id` 直接来自 Polymarket API，在 HTTP 请求中被正确编码
4. **限流保护**：`exec_interval=0.2s` 确保不超过 5 QPS（交易所限流）
5. **幂等保护**：下单和撤单有令牌去重，防止重复请求
6. **心跳保活**：心跳失败 3 次后触发 CRITICAL 级别告警
7. **信号处理**：`SIGINT`/`SIGTERM` 触发优雅关闭，先取消订单再停心跳

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
├── actor.py          # Actor：单市场状态机（非阻塞执行）
├── ws_manager.py     # WS 管理：连接生命周期 + 单线程重连
├── ws_router.py      # WS 路由：消息解析 + 分发
├── guardian.py       # 主控：定时循环 + 线程安全的 Actor 管理
└── data/             # 日志输出目录
    ├── guardian.log
    ├── trades.log
    ├── cancels.log
    └── abandons.log
```
