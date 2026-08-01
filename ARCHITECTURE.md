# Guardian V7 架构文档

## 一、项目概述

Guardian V7 是一个 **Polymarket CLOB 交易平台的 Maker-only 自动化做市机器人**。

**核心目的**：挂单提供流动性以获取平台返利奖励。机器人在订单簿买盘第 N 档挂限价买单，best_bid 变化时撤单重挂。买入成交后，由 `check_positions()` 定时扫描持仓并市价卖出（FOK）平仓。

**关键行为规则**：

- **市场来源**：由 screener 输出的 CSV 文件提供（YES + NO 两个 token 各自独立管理）
- **系统撤单**（best_bid 变化）：Actor 冷却 120s → 重新挂单
- **卖出**：不依赖 WS 事件驱动，由 `check_positions()` 每 120s 定时扫描持仓 → 市价卖出（FOK）

**技术栈**：Python 3.12+ | `py_clob_client_v2` | `websocket-client` | `eth_account` | `requests`

**策略文档**：详细逻辑见 [LOGIC.md](./LOGIC.md)。

**运行模式**：
- **原版 Polling 模式**（`python main.py`）：REST 批量轮询 best_bid（3s 间隔），适合稳定低频场景
- **WSS 实时模式**（`python -m wss.main`）：WebSocket 推送 best_bid（<100ms 延迟），适合高频实时响应

***

## 二、体系结构

### 2.1 模块依赖关系（零循环导入）

```
config.py   ← 无内部依赖（冻结 dataclass，环境变量加载）
models.py   ← 无内部依赖（ActorState, MarketState, OrderInfo）
utils.py    ← 无内部依赖（纯函数工具）
    │
    ├── heartbeat.py   ← config
    ├── execution.py   ← config, models, utils
    ├── ws_manager.py  ← config
    │
    ├── ws_router.py   ← config (Guardian via TYPE_CHECKING)
    │
    ├── screener/      ← 内置筛选器子包
    │   ├── types.py   ← 数据类 (CandidateMarket, ScoredMarket)
    │   ├── markets.py ← config (fetcher 接受 Config 对象)
    │   └── clob.py    ← config + types (评分 + 批量查订单簿)
    │
    └── guardian.py    ← 以上全部
            │
            ├── main.py        ← config, guardian（原版 Polling 模式入口）
            │
            └── wss/           ← WSS 实时模式模块
                ├── market_ws.py      ← websocket-client（市场频道 WS）
                ├── guardian_wss.py   ← guardian.py（继承 Guardian，覆写 4 方法）
                └── main.py           ← wss 入口（sys.path 修正 + GuardianWss.run()）
```

### 2.2 数据流全景

**原版 Polling 模式**：
```
                           ┌─── REST API ───────┐
                           │                     │
                  ┌────────▼──────┐  ┌───────────▼──────┐
                  │ HeartbeatManager│ │ ExecutionLayer    │
                  │ (每7s心跳)      │  │ (限流+线程池)      │
                  │ 维持订单存活     │  │ place/cancel/     │
                  │                │  │ limit_sell        │
                  └────────────────┘  └────────┬──────────┘
                                               │ Future
  ┌──────────────────┐          ┌──────────────┤
  │ screener/ 筛选器  │          │              │
  │ (每30s内存直传)   │          │              │
  │ /sampling-markets│  ┌───────▼──────────┐   │
  │ + /books 批量评分 │  │  WSRouter        │   │
  └────────┬─────────┘  │  用户频道消息分发  │   │
           │             └──┬───────────────┘   │
           │                │          │         │
  ┌────────┼────────┐       │          │         │
  │ POST /books 轮询│       │          │         │
  │ (每3s 查best_bid)│      │          │         │
  │ 200 市场批量      │  ┌───┴──────────┴──┐      │
  └────────┬─────────┘  │ User WS          │      │
           │             │ trade 事件        │      │
           │             │ (MATCHED/        │      │
  ┌────────▼───────────▼─┴─CONFIRMED/FAILED)│      │
  │     Guardian 主线程     │ order 事件      │      │
  │     _run_screener(30s)│ (PLACEMENT/     │      │
  │     discover(30s)     │  CANCELLATION)  │      │
  │     poll(3s)          └─────────────────┘      │
  │     audit(120s)                               │
  │     positions(120s)                           │
  │     prune(300s)                               │
  │     cooldown/pending(1s)                      │
  │                                               │
  │     Dict[str, MarketState]                    │
  │     (主线程直读直写，无锁)                      │
  └───────────────────────────────────────────────┘
```

**WSS 实时模式**（wss/ 模块）：
```
                           ┌─── REST API ───────┐
                           │                     │
                  ┌────────▼──────┐  ┌───────────▼──────┐
                  │ HeartbeatManager│ │ ExecutionLayer    │
                  └────────────────┘  └────────┬──────────┘
                                               │ Future
  ┌──────────────────┐          ┌──────────────┤
  │ screener/ 筛选器  │          │              │
  └────────┬─────────┘  ┌───────▼──────────┐   │
           │             │  WSRouter (User) │   │
           │             └──┬───────────────┘   │
           │                │          │         │
           │             ┌──▼──────────▼──┐      │
           │             │ User WS        │      │
           │             │ (trade/order)  │      │
           │             └────────────────┘      │
           │                                     │
  ┌────────┼─────────────────────────────┐      │
  │ MarketWS (market 频道，独立线程)      │      │
  │  - book 事件 (订单簿快照)             │      │
  │  - price_change 事件 (best_bid 变化)  │      │
  │  → Queue → 主线程 _process_ws_bids()  │      │
  └────────┬─────────────────────────────┘      │
           │                                     │
  ┌────────▼─────────────────────────────────────▼──┐
  │     GuardianWss(Guardian) 主线程               │
  │     _run_screener(30s)                        │
  │     discover(30s)                             │
  │     _process_ws_bids() ← WS Queue (实时)       │
  │     audit(120s)                               │
  │     positions(120s)                           │
  │     prune(300s)                               │
  │     cooldown/pending(1s)                      │
  │                                               │
  │     Dict[str, MarketState]                    │
  │     (主线程直读直写，无锁)                      │
  └───────────────────────────────────────────────┘
```

**关键差异**：
- **原版**：`_poll_best_bids()` 每 3s REST 批量查询 → 中延迟（3s）
- **WSS**：MarketWS 推送 `price_change` 事件 → Queue → 主线程处理 → 低延迟（<100ms）
- **WSS 跳过**：`_poll_best_bids()` 不再运行（继承但不调用）

***

## 三、核心组件详解

### 3.0 卖出策略 — BUY 成交即时触发 + 定时扫描兜底

**卖出双路径**（V7.9+）：

1. **即时触发路径**（~1-2s）：
   - `handle_trade()` 收到 BUY CONFIRMED → `_pending_sell_tokens.add(asset_id, fill_price)`
   - `_check_pending_sells()` 每 1s 消费队列，为每个 token 启动守护线程
   - `_sell_single_position(tid, fill_price)`：
     - 查 best_bid（单条 POST /books）
     - **sell_min_bid_gap 保护**：`best_bid < fill_price - gap` → 跳过（大单打薄订单簿，等市场恢复）
     - 查 open_orders 避免重复
     - **余额重试**：`onchain_balance()` 最多重试 5 次，间隔 2s（链上确认延迟）
     - 挂限价卖单 @ best_bid
   - 并发保护：`_selling` set + `_sell_lock`（同一 token 不重复触发）
   - **trade_id 去重**：`_processed_trades` 防止 MINED/CONFIRMED 双触发

2. **定时扫描兜底**（120s）：
   - `check_positions()` 扫描所有持仓
   - 批量查 best_bid（POST /books 分片）
   - 查 open_orders 找已有卖单
   - 无卖单 → 挂限价卖单 @ best_bid
   - **avgPrice 保护**：`best_bid < avgPrice - sell_min_bid_gap` → 取消现有卖单等待

数据流：

```
handle_trade() BUY CONFIRMED:
    │
    ├─ 去重：_processed_trades 检查 trade_id（同一 id 只处理一次）
    ├─ 过滤：仅处理 CONFIRMED 状态（跳过 MINED）
    │
    └─ _pending_sell_tokens[asset_id] = fill_price
            │
            ▼ (1s tick)
    _check_pending_sells() → 启动守护线程
            │
            ▼
    _sell_single_position(tid, fill_price):
        1. POST /books 查 best_bid
        2. sell_min_bid_gap 保护（跳过 = 交由 120s 兜底）
        3. open_orders 查重
        4. onchain_balance() 重试 5 次×2s（等链上确认）
        5. limit_sell @ best_bid
            ├─ 成功 → trade_logger 记录
            └─ 失败 → 下轮 check_positions 兜底

check_positions() 每 120s（兜底）:
    1. 查持仓列表
    2. 批量查 best_bid
    3. 查 open_orders 找已有卖单
    4. 对每个持仓：
        ├─ 已有卖单 → 检查 avgPrice 保护，必要时取消
        ├─ 正在卖出中（_selling）→ 跳过
        └─ 无卖单 → onchain_balance() → limit_sell @ best_bid
```

**关键设计**：
- **双保险**：即时触发失败 → 120s 兜底补挂
- **天然去重**：`_selling` set + open_orders 查询
- **自愈能力**：重启/事件丢失后最多 120s 自动补挂
- **数据安全**：查询 onchain_balance 确认链上余额后才下单
- **sell_min_bid_gap 保护**：大单打薄订单簿时跳过即时挂单，等市场深度恢复（通常数分钟内）

V7.9 前仅有 check_positions 路径，卖出延迟最坏 120s。V7.9+ 改为即时触发 ~1-2s + 120s 兜底。

### 3.0.1 handle_trade() — trade 事件处理（V7.10 修复）

```python
handle_trade(data: dict):
    # 去重：同一 trade_id 只处理一次（MINED/CONFIRMED 共用相同 id）
    with _trade_lock:
        if tid in _processed_trades:
            return
        _processed_trades[tid] = time.time()
    
    # 仅处理 CONFIRMED；MINED 时链上余额尚未到账
    if status != "CONFIRMED":
        return
    
    # 优先从 maker_orders[api_key] 读取我们的真实数据
    # （顶层字段是 taker 视角，token/price 互补）
    our_orders = [m for m in maker_orders if m.owner == api_key]
    if our_orders:
        asset_id = our_orders[0].asset_id  # Yes token
        price = our_orders[0].price        # 真实成交价
    
    if side == "BUY":
        trade_logger.info(buy_confirmed)
        _pending_sell_tokens[asset_id] = price  # 触发即时卖单
    else:
        trade_logger.info(sell_confirmed)
```

**V7.10 修复**：
- **trade_id 去重**：MINED + CONFIRMED 双事件只处理一次
- **读取正确字段**：`maker_orders[api_key]` 包含我们的真实 token/price（顶层是 taker 的互补数据）
- **示例**：买入 Yes@0.51 → 顶层显示 No@0.49（0.51+0.49=1.00），maker_orders 才是 Yes@0.51

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

- **`ActorState`**：`NO_ORDER → PLACING → RESTING → CANCELING → COOLING → STOPPED`，六状态枚举
- **`MarketState`**：集中式市场状态 dataclass，字段 `state / state_at / active_id / active_price / best_bid / best_ask / cooldown_until`，主线程直读直写
- **`OrderInfo`**：纯数据载体（order_id, price, size, side, token_id, market）

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
2. SDK 抛异常 → 从 `PolyApiException.error_msg` (dict) 中提取新 `heartbeat_id`，兼容字符串正则回退
3. 恢复到新 id → **立即用新 id 重试 SDK**（成功则免走 raw fallback）
4. SDK 重试仍失败 → 回退 L2 认证头直接 HTTP (`_raw_heartbeat`)，签名算法与 SDK 完全一致：
   - secret 用 `base64.urlsafe_b64decode` 解码
   - 消息串 = `ts + method + path + str(body).replace("'", '"')`
   - 输出 = `base64.urlsafe_b64encode(HMAC-SHA256 digest)`
   - HTTP body 使用同一 `str(body).replace` 字符串（服务器按接收字节验签）
5. raw 400 响应 → 提取正确 ID，更新后循环重试；401/403 → 抛出
6. 失败重试节奏：连续失败但未到阈值时 1s 快速重试；成功或已到阈值按 `heartbeat_interval`（7s）
7. 连续失败 ≥`heartbeat_max_errors` 次 → CRITICAL 告警

**V7.6 修复的三处 heartbeat bug**（一直存在，从未修过）：
- Bug 1：旧 `_try_recover_sdk_error` 用双引号正则匹配 `str(e)`，但 Python dict repr 用单引号 → 从未成功恢复过一次
- Bug 2：旧 `_raw_heartbeat` 签名用 `.encode()` / `json.dumps` / `hexdigest`，三处都与 SDK 不一致 → fallback 一直 401
- Bug 3：旧代码恢复到新 id 后直接走 raw（Bug 2 必然失败），未先用新 id 重试 SDK

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
| 下单兜底 | `_do_place` 异常时查 `open_orders` 确认订单是否已挂 |

**公共方法**：

| 方法 | 用途 | 返回 |
|------|------|------|
| `place(asset_id, price, size, tick_size)` | 下限价买单（BUY, Post-Only） | `Future[Optional[str]]` order_id |
| `cancel(order_id, reason)` | 撤单 | `Future[bool]` |
| `market_sell(asset_id, size, tick_size)` | 市价卖单（SELL, FOK） | `Future[Optional[str]]` order_id |
| `cancel_batch(order_ids, reason)` | 批量撤单（≤1000） | `Future[dict]` |
| `cancel_all(reason)` | 全部撤单（shutdown 用） | `Future[bool]` |
| `clear_place(asset_id, price)` | 清除下单幂等 token | — |
| `clear_place_by_asset(asset_id)` | 清除某资产全部下单 token | — |

### 3.6 guardian.py 集中式状态管理（替代 actor.py）

**架构变更（V7.4）**：删除 `AssetActor` 线程和事件队列。所有市场状态集中在主线程的 `Dict[str, MarketState]` 中，直读直写，无需锁。

**状态机**（与之前相同，但不在独立线程中）：

```
                        ┌──────────┐
               ┌───────►│ NO_ORDER │◄──────────┐
               │        └─────┬────┘           │
               │    冷却到期/bid变化│           │ _start_cooldown()
               │        ┌─────▼────┐           │
               │        │ PLACING  │           │
               │        └─────┬────┘           │
               │   Future done  │             │
               │        (ok+oid)│             │
               │        ┌─────▼────┐           │
               │        │ RESTING  ├───────────┤ best_bid 变化
               │        └──┬───┬──┘           │
               │      成交 │   │ 撤单         │
               │   (保持不变)│   │              │
               │           │   │              │
               │        ┌──▼───▼──┐           │
               │        │CANCELING│           │
               │    ┌───┤  (异步) ├───┐       │
               │    │   └─────────┘   │       │
               │    │ cancel OK       │ cancel FAIL
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

**状态变更机制**：

| 场景 | 旧（actor.py） | 新（集中式） |
|------|---------------|-------------|
| 挂单触发 | COOLDOWN_EXPIRED 事件→Actor 队列→_place() | `_check_cooldowns()` 每秒遍历 dict 检查到期 |
| 异步结果回传 | 回调线程 post 到 Actor 队列 | 回调线程设置 Future 结果，`_check_pending_ops()` 每秒轮询 `Future.done()` |
| best_bid 变化 | post BEST_BID 事件→Actor 队列 | `_poll_best_bids()` 直接读/写 MarketState |
| 审计 | post AUDIT 事件→Actor 队列 | `audit()` 直接遍历 `_markets` 检查 |
| 冷却等待 | 每个 Actor 独立 `threading.Timer` | `cooldown_until` 时间戳 + 主循环检查 |

**关键设计决策**：

- 异步结果回传不依赖回调线程改状态，回调线程只 `set_result()`，主线程轮询 `Future.done()` 后改状态
- 取消/下单的幂等保护由 ExecutionLayer 提供（不变）
- `_pending_ops` 列表追踪进行中的异步操作

### 3.7 ws_manager.py + ws_router.py — WebSocket 层

**WSManager**（连接生命周期管理）：

- `_run_ws()`：单线程 while 循环处理 连接→断开→重连，消除递归线程泄漏
- `_ws_on_open()`：先执行业务回调（auth/订阅），再启动 PING 线程
- PING 线程：先 sleep 后发 PING（避免刚连接立即发 PING 被服务器拒绝）
- 自动读取 `HTTP_PROXY`/`HTTPS_PROXY` 环境变量配置代理
- `start_user()` 带互斥锁，只有一个用户频道连接线程

**WSRouter**（消息分发，仅用户频道）：

| event_type | 处理 |
|------------|------|
| `trade`（MATCHED/CONFIRMED/FAILED） | → `Guardian.handle_trade()` |
| `order`（PLACEMENT/CANCELLATION） | → `Guardian.handle_order()` |
| `channel: "user"`（initial_dump） | → 所有历史 trade 路由到 `handle_trade()` 统一处理 |

**best_bid 变化检测**（替代市场 WS）：

Guardian 每 3s 通过 `POST /books` 批量查询所有市场的订单簿，提取 best_bid 与 Actor 本地缓存对比，变化时以 `BEST_BID` 事件推送到 Actor。

### 3.8 guardian.py — 主控制器

**定时任务**：

| 任务 | 间隔 | 功能 |
|------|------|------|
| `discover()` | 30s | 接管已有买单 + CSV 文件同步新市场、清理 STOPPED 状态 Actor |
| `audit()` | 120s | 批量 `POST /books` 查 best_bid，纠偏超价订单（price ≥ best_bid 即撤单）、检测订单丢失、状态卡死重置 |
| `check_positions()` | 120s | **唯一卖出路径**：查持仓 → 已有卖单跳过 → 无卖单市价卖出（FOK） |
| `_prune_caches()` | 300s | 清理过期 `_ob_cache`、`_market_info`、`_processed_trades`（按时间戳有序淘汰） |

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
仅记录 debug 日志，不做业务处理。
撤单闭环由 Actor._cancel() → CANCEL_DONE 内部事件完成。
```

**check_positions() — 唯一卖出路径**：

```
获取持仓 → 查询 open_orders() → 找到已有的 SELL 订单
  ├─ 已有卖单 → 跳过
  └─ 无卖单 → onchain_balance() → market_sell(balance)  # FOK 市价
```

- 并发保护：`_selling` set + `_sell_lock`
- 自愈：重启/WS 断线后最多 120s 自动补挂卖单

**主循环启动顺序**：
1. heartbeat.start() — 心跳最先启动
2. WS 启动 + 用户频道认证
3. discover() — 发现已有订单
4. 市场频道订阅
5. 主循环（五定时任务 + 筛选器）
6. `_shutdown()`：取消活跃订单 → 停止 Actor → 断 WS → 停工线程池 → 停心跳

### 3.9 screener/ — 内置筛选器子包

**作用**：替代原先外部独立运行的筛选器进程。每 30s 从 CLOB `/sampling-markets` 拉取所有有返利奖励的市场，按订单簿深度计算 `reward_per_dollar` 评分，筛选后直接内存传递给 Guardian。

**数据流**：
```
_run_screener() 每 30s:
  1. markets.fetch_and_filter(cfg)        → List[CandidateMarket]
      - GET /sampling-markets 分页拉取
      - 过滤：active + rewards + 关键词 + 中点 + min_size
  2. clob.analyze_orderbooks(candidates, cfg) → List[ScoredMarket]
      - POST /books 批量查订单簿（≤500/批，5线程并发）
      - 评分：reward_per_dollar = 日奖励 / (深度 + min_size)
  3. 过滤 existing_total_size ≥ MIN_EXISTING_SIZE
  4. _sync_from_screener(scored_list) → 直接更新 self._markets
  5. 写入 data/screener_latest.csv（调试用，非数据桥）
```

**文件职责**：

| 文件 | 职责 |
|------|------|
| `screener/types.py` | 数据类：CandidateMarket、ScoredMarket、AllocatedMarket |
| `screener/markets.py` | `fetch_and_filter(cfg)` — 拉取+过滤候选市场 |
| `screener/clob.py` | `analyze_orderbooks(candidates, cfg)` — 批量查订单簿+评分排序 |

**与旧筛选器的区别**：
- 不再是独立进程，作为 Guardian 内部定时任务运行
- 接受 `Config` 对象参数，而非硬编码的模块级 `CONFIG` 单例
- 结果直接内存传递，不再通过 CSV 文件中转
- CSV 输出保留但仅为调试用途

***

## 四、线程模型

### 4.1 线程清单

**原版 Polling 模式**：

| 线程 | 数量 | 持久性 | 用途 |
|------|------|--------|------|
| MainThread | 1 | 持久 | 主循环 + 所有市场状态管理 |
| heartbeat | 1 | 持久 | REST 心跳 |
| ws-user | 1 | 持久 | 用户 WS（单线程重连） |
| ws-ping | 1 | 持久 | 用户频道 PING |
| exec-N | ≤10 | 持久 | ThreadPoolExecutor 工作线程 |
| sell-trigger-* | 按需 | 短期 | BUY 成交后即时卖单（守护线程，完成即退出） |

**WSS 实时模式**（额外增加）：

| 线程 | 数量 | 持久性 | 用途 |
|------|------|--------|------|
| market-ws | 1 | 持久 | MarketWS 连接线程（订阅市场 book/price_change） |
| market-ws-ping | 1 | 持久 | 市场频道 PING 保活 |

**V7.4 变更**：删除 Actor-×N 线程（每市场一个）和回调线程。异步结果由 `_check_pending_ops()` 在主循环中轮询 `Future.done()` 完成回传。

**V7.9 变更**：新增 sell-trigger-* 守护线程，BUY 成交后启动，完成卖单挂单后自动退出。

**V7.10 变更**：新增 wss/ 模块，market-ws 独立线程处理市场 WS 事件。

### 4.2 锁层级（防死锁）

```
获取顺序（严格单向）：

1. _trade_lock         (Lock)     — 已处理 trade ID 去重
2. _cache_lock         (RLock)    — 缓存读写
3. _sell_lock          (Lock)     — 卖出并发保护
4. _pending_sell_lock  (Lock)     — _pending_sell_tokens 队列保护

+ ExecutionLayer 内部锁               — 独立，不与上述交叉
+ MarketWS._lock (WSS)                — 市场 WS 订阅状态保护
```

**V7.4 变更**：删除 `_actors_lock`（不再有 Actor 管理）和 `_state_lock`（状态全在主线程）。

**V7.9 变更**：新增 `_pending_sell_lock` 保护 `_pending_sell_tokens` 字典（WS 线程写，主线程读）。

**V7.10 变更**：新增 `MarketWS._lock` 保护订阅状态（主线程读 subscribed_ids，WS 线程写）。

### 4.3 线程安全关键设计

- **MarketState**：主线程独占读写，无需锁。所有状态变更在 1s 粒度内完成。
- **异步回传**：Future 在线程池线程设置结果，主线程在 `_check_pending_ops()` 中轮询 `done()` 后更新 MarketState
- **WS 重连互斥**：`_user_lock` 保证用户频道只有一个连接线程
- **执行层隔离**：ExecutionLayer 内部锁（rate limiter、幂等 token）独立，不与 Guardian 锁交叉

***

## 五、撤单处理机制

### 唯一撤单路径：系统主动撤单

```
best_bid 变化 / WSS 重连 / audit 纠偏
    │
    ▼
Actor._cancel(reason)  或  audit → exec_layer.cancel() + CANCEL_DONE
    ├─ state → CANCELING
    └─ ExecutionLayer.cancel(oid, reason) → Future
          ├─ 成功 → CANCEL_DONE(ok=True)
          │   └─ _on_cancel_done: NO_ORDER → COOLING(120s)
          │       └─ 120s 后 COOLDOWN_EXPIRED → _target_price() → place
          └─ 失败 → CANCEL_DONE(ok=False)
              ├─ active_id 已被清除 → 忽略（订单已不存在）
              └─ active_id 仍存在 → 保持 RESTING（订单仍存活）
```

WS CANCELLATION 事件不再处理（仅记录 debug 日志）。撤单完全由 `CANCEL_DONE` 内部事件闭环。

### 触发方式对比

| 触发方式 | 处理流程 | 是否放弃市场 |
|----------|----------|-------------|
| best_bid 变化（批量轮询） | `_cancel()` → COOLING → 重挂 | ❌ 不放弃 |
| audit 纠偏 | `exec_layer.cancel()` + `CANCEL_DONE` | ❌ 不放弃 |

***

## 六、版本变更记录

### V7.10（2026-07-31）：WSS 实时模式 + 孤儿订单修复

**WSS 实时模式**（wss/ 模块）：
- 新增 `wss/market_ws.py`：MarketWS 类，订阅市场 book/price_change 事件
- 新增 `wss/guardian_wss.py`：GuardianWss(Guardian) 继承，覆写 4 个方法
- 新增 `wss/main.py`：WSS 版本启动入口，sys.path 修正
- best_bid 来源：WebSocket 推送（<100ms）替代 REST 轮询（3s）
- `_route()` 处理 JSON 数组（Polymarket WS 协议）
- `_sync_ws_subscriptions()` 动态订阅/取消订阅市场

**P0：孤儿订单 bug**（commit 75e3a25）：
- **问题**：筛选器移除市场后，`discover()` 可能重新加回导致订单失去监控
- **竞态序列**：移除 → 触发 cancel → STOPPED → discover 清理 `_removed_by_screener` → open_orders 仍返回订单（API 延迟）→ 重新加回 `_markets` → 孤儿（不在 `_file_managed_ids`）
- **为何 WSS 更易触发**：实时操作快（1-2s），API 最终一致性延迟（100-500ms）变显著；原版慢（10s discover 间隔）掩盖此 bug
- **修复**：
  - `discover()` 清理 STOPPED 时不再清除 `_removed_by_screener`
  - 发现新市场时跳过 `_removed_by_screener` 中的 token

**P0：重复 SELL-TRIGGER**（commit 859c16a）：
- **问题**：同一笔交易收到 MINED + CONFIRMED 双事件 → 启动 2 个线程
- **修复**：
  - `handle_trade()` 增加 `_processed_trades` 去重（同一 trade_id 只处理一次）
  - 仅处理 CONFIRMED 状态，跳过 MINED（链上余额尚未到账）

**P1：余额延迟**（commit 859c16a）：
- **问题**：BUY CONFIRMED 后链上余额延迟到账（0-3s），第一次查询=0 直接放弃
- **修复**：`_sell_single_position()` 余额≤阈值时重试 5 次，间隔 2s

**P0：handle_trade 读取错误字段**（commit c402a0d）：
- **问题**：Polymarket trade event 顶层字段是 taker 视角（互补 token），买入 Yes@0.51 显示 No@0.49
- **修复**：优先读取 `maker_orders[api_key]` 中的真实 asset_id/price/outcome

**WSS 基础修复**（commit 287ca14）：
- `ModuleNotFoundError: No module named 'config'` → `sys.path.insert(0, _ROOT)`
- `_route()` 处理 JSON 数组（Polymarket 协议）
- SELL-TRIGGER 日志升级为 INFO/WARNING 级别
- 修复 market_ws 订阅逻辑

### V7.9（2026-07-30）：BUY 成交即时触发卖单

### V7.9（2026-07-30）：BUY 成交即时触发卖单

**背景**：原来 `check_positions()` 每 120s 才挂卖单，成交后最坏等待 120s。

**修复路径**：
- `handle_trade()` BUY CONFIRMED 后将 asset_id 加入 `_pending_sell_tokens`（WS 线程，仅做 dict 写，不阻塞）
- `_check_pending_sells()`（1s tick）消费队列，为每个 token 启动守护线程
- `_sell_single_position(tid, fill_price)`（守护线程）：
  - POST /books 单条查 best_bid
  - **sell_min_bid_gap 保护**：`best_bid < fill_price - gap` → 跳过（大单打薄订单簿，等市场恢复）
  - GET /orders 查现有卖单
  - onchain_balance
  - limit_sell @ best_bid
- 120s 的 `check_positions()` 保留作兜底（含 avgPrice 低于成本保护）

**效果**：
- 卖单延迟：最坏 120s → ~1-2s
- 防重复：复用已有 `_sell_lock` + `_selling` 集合，与 check_positions 互斥同一 token

### V7.8（2026-07-29）：多实例 + open_orders 失败修复

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

### V7.4（2026-07-24）：集中式状态管理 + Post-Only + 批量撤单

- 删除 `actor.py`：不再每市场一个线程 + 事件队列 + Timer
- 所有市场状态集中到主线程 `Dict[str, MarketState]`，直读直写，无锁
- `_check_cooldowns()` 每秒遍历冷却到期检查（替代各 Actor 独立 Timer）
- `_check_pending_ops()` 每秒轮询 `Future.done()`（替代回调线程 post 事件）
- `_poll_best_bids()` / `audit()` 直接读写 MarketState（替代 post BEST_BID/AUDIT 事件）
- 线程数：400+ 市场 → 固定 ~14 线程（无市场数量相关线程）
- CSV 同步支持市场移除（`_file_managed_ids` 追踪）
- GET /book 异步化（`run_async` 进线程池），主循环零阻塞
- 批量撤单：`cancel_orders`（≤1000）用于 poll/audit；`cancel_all` 用于 shutdown
- Post-Only：BUY 下单 `post_only=True`，绝对纯 Maker，跨价拒绝不重试
- CSV 空 token_id 自动跳过（可临时排除特定市场）

### V7.6（2026-07-25）：Heartbeat 恢复机制修复 + audit race 修复

**根因**：日志显示 195 张订单在同一秒内被 audit 标记"订单丢失纠偏"。表面是 audit 检测到 RESTING 订单不在 open_orders 中；深层是心跳中断 85+ 秒，交易所超过宽限期自动撤销挂单。

**P0：SDK 400 后 heartbeat_id 恢复失败** — `heartbeat.py:_try_recover_sdk_error`
- 旧正则 `r'"heartbeat_id"\s*:\s*"([a-f0-9-]+)"'` 期待双引号，但 SDK `PolyApiException.error_msg` 是 dict，`str(e)` 中 Python dict repr 使用**单引号** → 永不匹配
- 改为优先读 `getattr(e, "error_msg", None)`（dict）中的 `heartbeat_id`；字符串正则兼容单/双引号作为回退

**P0：raw fallback 签名与 SDK 不一致** — `heartbeat.py:_raw_heartbeat`
- 旧代码：`self._creds.api_secret.encode()` / `json.dumps(body, separators=(",",":"))` / `.hexdigest()` — 三处都与 SDK 不同
- SDK 签名（`py_clob_client_v2.signing.hmac.build_hmac_signature`）：`base64.urlsafe_b64decode(secret)` / `str(body).replace("'", '"')` / `base64.urlsafe_b64encode(digest)`
- 改为直接复用 `build_hmac_signature`，HTTP body 也用 `str(body).replace("'", '"')` 保持与签名字节一致

**P1：恢复后未立即用新 id 重试 SDK** — `heartbeat.py:_send_heartbeat`
- 旧代码 recover 后直接走 raw fallback（依赖 Bug 2 那条必然失败的路径）
- 改为 recover 成功后立即用新 id 再打一次 SDK，仍失败才走 raw

**P1：失败重试节奏过慢** — `heartbeat.py:_loop`
- 旧代码始终 `sleep(heartbeat_interval)` （7s），失败后 7s 才重试，短时抖动被放大成心跳中断
- 改为：失败但未达阈值时 `sleep(1.0)` 快速重试；成功或已达阈值按 `heartbeat_interval`

**P1：audit 误判已完成但未回写的 Future** — `guardian.py:_has_pending_op`
- 旧代码遍历 `_pending_ops` 时 `if fut.done(): continue`，跳过已完成但主循环尚未在 `_check_pending_ops` 中回写状态的 Future
- 后果：audit 抢在 `_check_pending_ops` 前跑（同一主循环迭代内），把 PLACING/CANCELING 市场当成卡死并重置；随后 `_handle_*_result` 又将状态写回，产生错位
- 修复：只要 op 还在 `_pending_ops` 列表里就视为 pending，不判断 `fut.done()`（`_check_pending_ops` 会在弹出前处理完成的 Future）



针对"同一市场出现 2 个活跃订单"的根因做修复，**不依赖事后发现**：

**P0：下单"假失败"恢复** — `execution.py:_do_place`
- 网络异常/响应丢失场景下，`create_and_post_order` 抛异常不代表订单没挂上
- 重试耗尽后调用 `_verify_order_placed()` 查询 `get_open_orders(asset_id=...)`，若已有同 asset+price 的 BUY 订单则视为下单成功，返回真实 order_id
- 确认成功 → 不清幂等 token、不进入冷却重挂 → 杜绝第 2 单产生

**P1：批量撤单失败兜底** — `guardian.py:_handle_batch_cancel_result`
- 失败分支不再回退 RESTING（保留 active_id 死循环），改为也清空 active_id 进入冷却
- 让下一轮 poll/audit 重新发现真实订单状态，避免 SDK 返回 id 格式不一致时的死循环
- `cancel_batch` 异常分支（非 dict result）也走同样的兜底路径

**P1：audit 与 pending Future 冲突** — `guardian.py:audit`
- 新增 `_has_pending_op(token_id)` 检查
- audit 遍历市场时跳过有 pending Future 的市场——状态机正在变更中，不应干预
- `_sync_from_file` 移除市场时也用同样检查，避免撤单进行中删除 MarketState

**已回滚的设计**：
- ~~`_trigger_place` 挂单前置查 `get_open_orders` 确认无残留订单~~ — 该检查每次挂单都发
  一次 HTTP，300+ 市场形成 HTTP 风暴，间接导致心跳请求超时被 401 拒绝、订单被交易所自动
  取消。P0 已堵住"假失败重挂"这个唯一根因，残留订单场景在状态机里没有真实路径，前置
  检查冗余且有害，已删除。

### V7.3（2026-07-23）：best_bid 修复 + audit 优化 + 文档清理

- 修复 `POST /books` best_bid 取反：API 文档声称 bids 降序，实际返回升序，`bids[0]` 取到最低价，改为 `bids[-1]`；asks 同理改为 `asks[-1]`
- audit 优化：best_bid 检查从逐个 `GET /book` 改为一次 `POST /books` 批量查询
- 删除已无调用者的 `best_bid()` 方法、`_ob_cache` 缓存及 `retry_call` 导入
- 新增 `_sync_from_file()`：从 CSV 文件加载筛选市场，YES/NO 各作为一个 Actor，创建后自动启动挂单周期
- 配置新增 `MARKET_FILE` 环境变量
- LOGIC.md 删除已废弃的市场放弃逻辑章节、`data/abandons.log` 条目

### V7.7（2026-07-26）：筛选器融合 + 配置统一

- 将独立筛选器项目 `polymarket-liquidity-monitor-python/` 融合为 Guardian 内部子包 `screener/`
- 筛选器作为 Guardian 定时任务 `_run_screener()` 每 30s 运行，结果直接内存传递（不再通过 CSV 中转）
- 所有筛选器参数统一到 `config.py`，通过 `SCREENER_*` 环境变量配置
- CSV 输出保留为 `data/screener_latest.csv`（仅调试用，非数据桥）
- 废弃代码删除：`allocator.py`（未使用且已损坏）、独立 `screener.py` 入口
- `screener/markets.py` 和 `screener/clob.py` 改为接受 `Config` 对象参数
- 删除 `MARKET_FILE` 环境变量（不再需要外部 CSV 输入）
- 筛选器间隔：原 1min → 30s；筛选器关键词默认 `"temp"`，可设为空字符串不过滤

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
├── config.py         # 配置：冻结 dataclass，从 .env 加载全部参数，含 SCREENER_* 筛选器参数
├── models.py         # 模型：ActorState(6状态)、MarketState(dataclass)、OrderInfo
├── utils.py          # 工具：safe_float/safe_decimal 安全转换、round_to_tick 价格对齐、retry_call 重试
├── heartbeat.py      # 心跳：HeartbeatManager 守护线程，SDK→raw REST 双重保障，heartbeat_id 链式恢复
├── execution.py      # 执行层：全局限流 + 幂等令牌 + ThreadPoolExecutor，place/cancel/limit_sell
├── ws_manager.py     # WS 管理：用户频道，单线程重连，代理支持，PING 保活
├── ws_router.py      # WS 路由：JSON 解析 → Guardian 分发，initial_dump 完整处理
├── guardian.py       # 主控：定时循环 + 筛选器任务 + trade 日志 + check_positions 卖出 + 状态管理
├── screener/         # 内置筛选器子包（V7.7 从独立项目融合）
│   ├── __init__.py   # 包入口
│   ├── types.py      # 数据类：CandidateMarket、ScoredMarket、AllocatedMarket
│   ├── markets.py    # 拉取 /sampling-markets + 过滤
│   └── clob.py       # 批量查 /books + 评分排序
├── LOGIC.md          # 策略逻辑文档（§十六：筛选器参数说明）
├── ARCHITECTURE.md   # 架构文档（本文件）
├── data/             # 日志 + CSV 输出目录
│   ├── guardian.log  # 主日志
│   ├── trades.log    # 买卖配对记录
│   └── screener_latest.csv  # 筛选结果（调试用）
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
| `execution.py` | Guardian/Actor | 限流异步执行 | `place()`, `cancel()`, `limit_sell()`, `clear_place()` |
| `ws_manager.py` | Guardian | WS 连接生命周期 | `start()`, `stop()`, `start_user()` |
| `ws_router.py` | Guardian | WS 消息分发（仅用户频道） | `on_user_message()` |
| `guardian.py` | main.py | 全盘协调 + 集中式状态管理 + 筛选器调度 | `run()`, `handle_trade()`, `handle_order()`, `discover()`, `_run_screener()` |
| `screener/markets.py` | guardian.py | 拉取+过滤候选市场 | `fetch_and_filter(cfg)` |
| `screener/clob.py` | guardian.py | 订单簿批量查询+评分排序 | `analyze_orderbooks(candidates, cfg)` |
| `screener/types.py` | screener | 筛选器数据类定义 | `CandidateMarket`, `ScoredMarket`, `AllocatedMarket` |

***

## 八、运行指南

### 环境变量（.env）

```ini
# ── 必填 ──────────────────────────────────────────────────────────────
PK=0x...                    # 私钥
CLOB_API_KEY=...            # L2 API Key
CLOB_SECRET=...             # L2 API Secret
CLOB_PASS_PHRASE=...        # L2 Passphrase

# ── 可选 ──────────────────────────────────────────────────────────────
PROXY_ADDRESS=0x...         # 代理合约地址（EOA 钱包留空）
CLOB_API_URL=...            # CLOB API 地址（默认 https://clob.polymarket.com）

# ── 筛选器（全部可选，有默认值） ────────────────────────────────────────
SCREENER_INTERVAL=30        # 筛选间隔（秒）
SCREENER_KEYWORD=temp       # 关键词过滤，空字符串=不过滤
SCREENER_MIN_DAILY_REWARDS=20.0
SCREENER_MIN_DAYS_TO_EXPIRY=0
SCREENER_MIN_MIDPOINT=0.15
SCREENER_MAX_MIDPOINT=0.85
SCREENER_MIN_SIZE_LOWER=0.0
SCREENER_MIN_SIZE_UPPER=60
SCREENER_MIN_EXISTING_SIZE=1500.0
SCREENER_MIN_TOP1_BIDS=50.0
SCREENER_MIN_TOP2_BIDS=600.0
SCREENER_MIN_TOP3_BIDS=1500.0
```

### 启动

```bash
cd guardian_v7
python main.py
```

### 健康检查

观察控制台/guardian.log：

- 启动后立即出现 `[HEARTBEAT] 已启动 interval=7.0s`
- 启动后 ~30s 出现筛选器输出：`N markets in Xs | yes:M no:K`
- 每 30s `[DISCOVER] 守护 N 个市场`，N 不为 0
- 每 7s 心跳成功（debug 级别显示 `[HEARTBEAT] OK`）
- 无连续 `[HEARTBEAT] 失败` 消息
- 无 `[WS ERR]` 或频繁 `[WS] close code=...`

### 停止

`Ctrl+C` → 优雅关闭序列：

1. 批量提交所有撤单 → 统一等待完成
2. 断开 WebSocket 连接
3. 关闭线程池（`shutdown(wait=True)`）
4. 停止心跳

### 日志文件

| 文件 | 内容 |
|------|------|
| `data/guardian.log` | 全部运行日志（控制台同步输出） |
| `data/trades.log` | 买卖配对：每笔买入+卖出成对记录 |
| `data/cancels.log` | 撤单记录（已废弃） |
