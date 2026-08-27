# Guardian V8 架构文档

> **版本**：V8.1  
> **最后更新**：2026-08-04  
> **阅读建议**：本文档整合了模块设计、策略逻辑、WebSocket 实现。新人先看 [README.md](./README.md)，运维手册见 [USAGE.md](./USAGE.md)。

---

## 目录

1. [架构概述](#一架构概述)
2. [模块依赖关系](#二模块依赖关系)
3. [核心组件详解](#三核心组件详解)
4. [线程模型与锁层级](#四线程模型与锁层级)
5. [主循环调度机制](#五主循环调度机制)
6. [策略逻辑](#六策略逻辑)
7. [WebSocket 实时监控](#七websocket-实时监控)
8. [优雅关闭](#八优雅关闭)
9. [代码审查要点](#九代码审查要点)

---

## 一、架构概述

### 1.1 项目定位

Guardian V8 是 Polymarket CLOB 平台的 **纯 Maker 自动化做市机器人**。

**核心目的**：在订单簿买盘挂限价买单提供流动性，赚取平台返利奖励；买入成交后自动卖出平仓，保持资金持续提供流动性。

**技术栈**：
- Python 3.12+
- `polymarket-client`（官方 PyPI SDK）
- `websocket-client`（实时推送）
- `httpx` / `requests`（HTTP 客户端）

### 1.2 与 V7 的关键差异

| 特性 | V7（旧版） | V8（当前版） |
|------|-----------|-------------|
| **CLOB 交互** | 手搓 REST + WebSocket | 官方 SDK（`py-clob-client` → `polymarket-client`） |
| **卖出策略** | 纯 taker FOK 市价 | **两级卖出**（即时 taker@best_bid + 兜底 maker@best_ask） |
| **凭据管理** | 明文传递私钥 | SDK 内置派生机制（`SecureClient.create`） |
| **实时监控** | 无 | WebSocket bid 变化秒级撤单（市场频道 WS） |
| **HTTP/2** | 默认启用 | 强制禁用（`main.py` monkey-patch，规避部分网络 TLS ALPN h2 被 RST） |

### 1.3 运行模式

**实盘入口**：`python main.py` → `Guardian` 类（REST 轮询 + WebSocket 推送混合）

```
main.py
  ├─ monkey-patch httpx.Client → http2=False（必须在 SDK import 前）
  ├─ INSTANCE = "bot1"（切换账号改此变量）
  ├─ load_dotenv(.env.bot1)（必须在 import config 前）
  ├─ _setup_logging()（控制台 INFO / 文件 DEBUG）
  └─ Guardian(Config(instance_name=INSTANCE)).run()
```

**价格源**：
- **best_bid**：REST 轮询（配置 3s）+ 市场频道 WebSocket 实时推送（秒级）
- **best_ask**：仅 REST（用于卖单侧，WebSocket 不推 ask）

---

## 二、模块依赖关系

### 2.1 依赖图（零循环导入）

```
config.py   ← 无内部依赖（冻结 dataclass，环境变量加载）
models.py   ← 无内部依赖（ActorState 六状态、MarketState、OrderInfo）
utils.py    ← 无内部依赖（safe_float/safe_decimal/round_to_tick）
    │
    ├── heartbeat.py   ← config（SDK get_balance_allowance 作心跳）
    ├── execution.py   ← config, utils（限流+幂等+线程池，全部 SDK 写操作）
    ├── ws_manager.py  ← config（用户频道 WS，单线程重连）
    ├── ws_router.py   ← （Guardian 反向持有；解析用户频道 JSON）
    ├── scheduler.py   ← （定时任务调度器，1s tick）
    ├── background.py  ← （后台任务分发器，线程池执行）
    │
    ├── screener/      ← 内置筛选器子包
    │   ├── types.py   ← 数据类（CandidateMarket, ScoredMarket, AllocatedMarket）
    │   ├── markets.py ← config（/sampling-markets 分页拉取 + 过滤）
    │   └── clob.py    ← config + types（/books 批量查订单簿 + 评分排序）
    │
    ├── wss/           ← 市场频道 WebSocket 实现
    │   ├── cache.py   ← （BidCache，best_bid 缓存 + 变化检测）
    │   └── market_ws.py ← （MarketWS，市场频道连接 + 订阅 + 路由）
    │
    └── guardian.py    ← 以上全部（主控 + 集中式状态）
            │
            └── main.py  ← config, guardian（实盘入口）
```

### 2.2 各模块职责

| 模块 | 职责 |
|------|------|
| `config.py` | 配置加载（dataclass frozen，从 `.env` 读取） |
| `models.py` | 数据模型（ActorState、MarketState、OrderInfo） |
| `utils.py` | 工具函数（类型转换、取整、重试） |
| `heartbeat.py` | SDK 心跳保活（7s 间隔） |
| `execution.py` | 执行层（下单/撤单，限流+幂等+线程池） |
| `ws_manager.py` + `ws_router.py` | 用户频道 WS（成交/订单事件） |
| `scheduler.py` | 定时任务调度器（1s tick，独立计时器） |
| `background.py` | 后台任务分发器（重 REST 后台化） |
| `screener/` | 内置筛选器（市场发现、订单簿评分） |
| `wss/` | 市场频道 WS（best_bid 实时推送） |
| `guardian.py` | 主控状态机（`_markets` 集中式管理） |
| `main.py` | 实盘入口（HTTP/2 禁用 + 日志配置 + 启动） |

### 2.3 文件清单

```
guardian_v8/
├── main.py              (81行)   - 入口
├── guardian.py          (1637行) - 主控状态机
├── config.py            (135行)  - 配置
├── models.py            (42行)   - 数据模型
├── utils.py             (64行)   - 工具函数
├── heartbeat.py         (197行)  - 心跳保活
├── execution.py         (330行)  - 执行层
├── ws_manager.py        (145行)  - 用户频道 WS 管理
├── ws_router.py         (44行)   - 用户频道消息路由
├── scheduler.py         (64行)   - 定时调度器
├── background.py        (77行)   - 后台分发器
├── screener/            - 筛选器子包
│   ├── __init__.py      (18行)
│   ├── types.py         (36行)
│   ├── markets.py       (107行)
│   └── clob.py          (149行)
├── wss/                 - 市场频道 WS 子包
│   ├── __init__.py      (15行)
│   ├── cache.py         (58行)
│   └── market_ws.py     (396行)
├── tests/               - 单元测试（保留）
│   ├── test_scheduler.py
│   ├── test_background.py
│   └── test_ws_market.py
├── test_sdk.py          (160行)  - SDK 连通性测试
├── requirements.txt     - Python 依赖
├── .gitignore           - Git 忽略规则
├── README.md            - 项目简介
├── ARCHITECTURE.md      - 本文档
└── USAGE.md             - 运维手册
```

---

## 三、核心组件详解

### 3.1 config.py — 配置加载

冻结 `dataclass`（`@dataclass(frozen=True)`）。

**注意**：字段直接写 `os.environ.get(...)` 的在 **import（类定义）时求值**，`field(default_factory=...)` 的在**实例化时求值**。`main.py` 已保证先 `load_dotenv` 再 `import config`，两类字段都能读到 `.env`。

**关键字段**：
- `pk`（私钥）：用 `default_factory`（实例化时读），`validate()` 强制要求
- `host`：CLOB API 端点（默认 `https://clob.polymarket.com`）
- `maker_size` / `maker_rank` / `maker_cooldown`：挂单策略参数
- `WS_MARKET_ENABLED`：市场频道 WS kill switch（`false` 回退纯 REST）

### 3.2 models.py — 数据模型

#### ActorState（市场状态枚举）

```python
NO_ORDER    # 无订单挂在交易所
PLACING     # 正在提交订单（异步）
RESTING     # 订单已挂簿，监控 best_bid
CANCELING   # 正在撤单（异步）
COOLING     # 撤单后冷却 120s 再重挂
STOPPED     # 已停止管理（筛选器移除后）
```

#### MarketState（市场状态容器）

```python
state: ActorState           # 当前状态
state_at: float             # 状态进入时间（time.time()）
active_id: Optional[str]    # 挂在交易所的订单 ID
active_price: Optional[Decimal]  # 挂单价格
best_bid: Optional[Decimal] # 最新 best_bid（变化检测用）
best_ask: Optional[Decimal] # 最新 best_ask（仅写不读，卖单侧用）
cooldown_until: float       # 冷却结束时间
```

**注意**：`best_ask` 目前**只写不读**（无任何决策读它）；`best_bid` 只用作"变化探测器"，真正下单价由 `_target_price()` 现查订单簿算 rank 档。

#### OrderInfo（订单数据载体）

```python
order_id: str
price: str
size: str
side: str           # "BUY" / "SELL"
token_id: str       # asset_id
market: str         # 市场合约地址
```

### 3.3 heartbeat.py — 心跳保活

守护线程每 `heartbeat_interval`（7s）调用 SDK `get_balance_allowance(asset_type="COLLATERAL")` 作心跳。

Polymarket 需定期心跳，否则挂单约 15s 后被交易所自动取消。启动时最先起、关闭时最后停，确保订单存活窗口全覆盖。

连续失败 ≥ `heartbeat_max_errors` 打 ERROR 后**清零计数**（当前仅告警，不触发重连）。

### 3.4 execution.py — 执行层

所有 REST **写操作**经此层，`ThreadPoolExecutor`（≤`max_workers`=10）异步执行，返回 `Future`。

| 方法 | SDK 调用 | 返回 |
|------|---------|------|
| `place(asset_id, price, size, tick)` | `place_limit_order(BUY, post_only=True)` | `Future[Optional[str]]` |
| `limit_sell(asset_id, size, price, tick)` | `place_limit_order(SELL)` | `Future[Optional[str]]` |
| `market_sell(asset_id, size, tick)` | `place_market_order(SELL, FOK)` | `Future[Optional[str]]`（保留未用） |
| `cancel(order_id, reason)` | `cancel_order` | `Future[bool]` |
| `cancel_batch(order_ids, reason)` | `cancel_orders` | `Future[dict]` |
| `cancel_all(reason)` | `cancel_all` | `Future[bool]` |
| `run_async(fn, *args)` | 任意函数进线程池 | `Future` |

**保障机制**：
- `_rate_wait()`：全局限流（写操作间隔 ≥`exec_interval`=0.2s）
- `_place_tokens`（TTL 300s）：下单幂等，避免重复提交
- `_cancel_tokens`：撤单幂等
- `_do_place` 异常后 `_verify_order_placed` 查 open_orders 兜底（响应丢失≠没挂上）

### 3.5 ws_manager.py + ws_router.py — 用户频道 WebSocket

**WSManager**：单线程 while 循环处理"连接→断开→重连"，PING 保活。

**WSRouter**：`on_user_message` 解析 JSON 数组 → `event_type=="trade"` 派发 `handle_trade`、`=="order"` 派发 `handle_order`。

**认证**：用 `client.credentials.key/secret/passphrase`（SDK 派生），不再读 `.env` 的 API key。

**用途**：
- 接收成交事件（`trade`）→ 触发即时卖出
- 接收订单事件（`order`）→ 状态确认（当前未用）

### 3.6 scheduler.py — 定时任务调度器

`PeriodicScheduler` 设计：1s tick + 独立计时器。

**核心思想**：主循环每轮调用 `check_and_trigger()`，各任务独立计时，到期时执行回调，重置计时器。

避免了旧版"阻塞 REST 拖延整个主循环"的问题（现已全部后台化）。

### 3.7 background.py — 后台任务分发器

`BackgroundDispatcher` 设计：重 REST 后台化"只算不写"。

**核心思想**：调度器触发时，任务在后台线程池执行（如 screener 分页拉取、discover 查 open_orders），完成后结果投回主线程的线程安全队列，主线程 `apply_results()` 时消费结果并应用到 `_markets`。

**收益**：即时卖单/撤单不再被 screener 阻塞拖延。

### 3.8 screener/ — 内置筛选器

**markets.py**：`fetch_and_filter(cfg)` 从 `/sampling-markets` 分页拉取有返利的市场 + 过滤

**clob.py**：`analyze_orderbooks(cfg)` 批量查 `/books` + `reward_per_dollar` 评分排序

**评分公式**：
```
reward_per_dollar = total_daily_rewards / (existing_total_size + min_size)
```
衡量每 1 USDC 投入的日返利，越高资金效率越好。

**数据流**：内存直传 `_apply_market_targets`，同时写 `data/{instance}/screener_latest.csv`（调试用）。

### 3.9 wss/ — 市场频道 WebSocket

**cache.py**：`BidCache` 缓存 best_bid + 变化检测

**market_ws.py**：`MarketWS` 市场频道连接 + 订阅 + 路由

**核心能力**：
- 动态订阅：`subscribe_more(token_ids)` / `unsubscribe(token_ids)`
- 消息路由：靠结构推断（无 type 字段），详见 [§七](#七websocket-实时监控)
- 断线重连：指数退避（最大 60s）

**回调**：bid 变化时调用 `on_bid_changed_cb(asset_id, old_bid, new_bid)` → 投线程安全队列 → 主线程消费

### 3.10 guardian.py — 主控状态机

**核心数据结构**：`_markets: Dict[str, MarketState]` 主线程独占直读直写，无锁。

**定时任务**：
- `screener`（30s）：市场发现 + 评分排序
- `discover`（30s）：已有订单接管
- `poll`（3s）：REST 轮询 best_bid（WSS 启用时变 30s 对账）
- `audit`（120s）：超价撤单 + 订单丢失恢复 + 孤儿单清理
- `position`（120s）：持仓扫描 + 兜底卖出
- `prune`（300s）：STOPPED 市场清理

**异步操作**：`_pending_ops` 列表追踪进行中的 Future，主循环轮询 `done()` 回写状态。

---

## 四、线程模型与锁层级

### 4.1 线程清单

| 线程 | 数量 | 持久性 | 用途 |
|------|------|--------|------|
| MainThread | 1 | 持久 | 主循环 + 全部 `_markets` 状态管理 |
| heartbeat | 1 | 持久 | SDK 心跳（7s 间隔） |
| ws-user | 1 | 持久 | 用户频道 WS（单线程重连） |
| ws-user-ping | 1 | 持久 | 用户频道 PING 保活 |
| ws-market | 1 | 持久 | 市场频道 WS（单线程重连） |
| ws-market-ping | 1 | 持久 | 市场频道 PING 保活 |
| exec-N | ≤10 | 持久 | ExecutionLayer 线程池（下单/撤单/卖单） |
| background-N | ≤5 | 持久 | BackgroundDispatcher 线程池（screener/discover/poll/audit） |
| sell-trigger-* | 按需 | 短期 | BUY 成交即时卖单（守护线程，完成即退） |

### 4.2 锁层级（严格单向，防死锁）

```
1. _trade_lock         (Lock)   — _processed_trades 去重
2. _cache_lock         (RLock)  — _market_info 缓存
3. _sell_lock          (Lock)   — _selling 卖出并发保护
4. _pending_sell_lock  (Lock)   — _pending_sell_tokens 队列

+ ExecutionLayer 内部锁（rate/place/cancel）— 独立，不与上述交叉
+ BidCache._lock       (Lock)   — best_bid 缓存（wss/cache.py）
+ MarketWS._ws_lock    (Lock)   — WebSocket 连接对象（wss/market_ws.py）
```

**持锁顺序规则**：按编号从小到大获取，释放顺序相反。违反此顺序可能死锁。

### 4.3 线程安全关键设计

#### 核心不变量："_markets 主线程独占无锁"

`_markets: Dict[str, MarketState]` 只有主线程能读写，其他线程绝不碰。这是整个状态机的基石。

**异步模式**：线程池只 `Future.set_result()`，主线程 `_check_pending_ops()` 轮询 `done()` 后改 `_markets`。

#### _pending_ops 竞态修复（commit de4d08f 之前的历史问题）

**旧问题**：即时卖单守护线程（`_sell_single_position`）会 `append` 到 `_pending_ops`，主线程同时遍历/pop。靠 GIL 不崩，但属脆弱点。

**修复**：引入 `_pending_ops_inbox`（线程安全队列），后台/守护线程不再直接 append，改投收件箱；主线程 `_check_pending_ops()` 开头统一 drain 收件箱进 `_pending_ops`。恢复"只有主线程碰 `_pending_ops`"不变量。

#### WebSocket 线程安全

**用户频道 WS**：`WSManager` 单线程，`on_message` 里调用 `handle_trade` / `handle_order` → 写线程安全队列（`_pending_sell_tokens` / `_ws_bid_queue`），不碰 `_markets`。

**市场频道 WS**：`MarketWS` 单线程，`on_message` 里调用 `_on_bid_changed_cb` → 写线程安全队列（`_ws_bid_queue`），不碰 `_markets`。

**主线程消费队列**：`_check_pending_sells()` / `_process_ws_bids()` drain 队列 → 应用到 `_markets`。

---

## 五、主循环调度机制

### 5.1 PeriodicScheduler 设计（1s tick + 独立计时器）

**核心思想**：主循环每轮调用 `check_and_trigger()`，各任务独立计时，到期时执行回调，重置计时器。

**优点**：
- 真正的 1s tick 粒度（旧版被阻塞 REST 拖延到 10s）
- 任务间独立，一个任务阻塞不影响其他任务触发判断

### 5.2 BackgroundDispatcher 设计（重 REST 后台化）

**核心思想**：调度器触发时，任务在后台线程池执行（如 screener 分页拉取、discover 查 open_orders），完成后结果投回主线程的线程安全队列，主线程 `apply_results()` 时消费结果并应用到 `_markets`。

**模式**：
```
调度器触发 → submit_task(fetch_fn) → 后台线程执行 → 结果投队列
                ↓
主线程每 tick: apply_results() → drain 队列 → 应用到 _markets
```

**收益**：即时卖单/撤单不再被 screener 阻塞拖延。

### 5.3 已修复的结构性问题

#### 问题 A：阻塞 REST 冻结状态机

**旧问题**：screener/discover/audit 等任务在主线程同步执行，每次耗时数秒，主循环实际周期 10s+，导致撤单/卖单延迟。

**修复**：全部后台化"只算不写"，结果丢回主线程应用；主循环回到真正 1s 粒度。

#### 问题 B：best_bid 轮询实际 10s 而非 3s

**旧问题**：配置 `best_bid_poll_interval=3s`，但主循环被 screener 阻塞，实际约 10s 才轮询一次。

**修复**：screener 后台化后，主循环真正 1s tick，poll 任务每 3s 触发一次（WSS 启用后变 30s 对账）。

### 5.4 调度任务清单

| 任务 | 间隔 | 后台化 | 用途 |
|------|------|--------|------|
| `screener` | 30s | ✅ | 市场发现 + 评分排序 |
| `discover` | 30s | ✅ | 已有订单接管（重启后恢复） |
| `poll` | 3s → 30s | ✅ | REST 轮询 best_bid（WSS 启用后变对账） |
| `audit` | 120s | ✅ | 超价撤单 + 订单丢失恢复 + 孤儿单清理 |
| `position` | 120s | ✅ | 持仓扫描 + 兜底卖出 |
| `prune` | 300s | ❌ | STOPPED 市场清理（主线程，直接操作 `_markets`） |

**注意**：`poll` 任务在 `WS_MARKET_ENABLED=true` 时间隔自动变为 30s（对账模式），`false` 时保持 3s（主力价格源）。

### 5.5 主循环伪代码

```python
def run(self):
    scheduler = PeriodicScheduler([
        ("screener", 30, self._dispatcher.submit_screener),
        ("discover", 30, self._dispatcher.submit_discover),
        ("poll", ws_rest_reconcile_interval, self._dispatcher.submit_poll),
        ("audit", 120, self._dispatcher.submit_audit),
        ("position", 120, self._dispatcher.submit_position),
        ("prune", 300, self._prune_stopped),
    ])
    
    while not self._stop_flag:
        now = time.time()
        
        # 1. 收编后台任务结果
        self._dispatcher.apply_results()
        
        # 2. 检查异步操作完成（Future.done()）
        self._check_pending_ops(now)
        
        # 3. WebSocket 实时推送队列
        if self._ws_enabled:
            self._check_ws_connection(now)
            self._process_ws_bids()
        
        # 4. 即时卖单队列
        self._check_pending_sells()
        
        # 5. 冷却到期检查
        self._check_cooldowns(now)
        
        # 6. 定时任务触发
        scheduler.check_and_trigger(now)
        
        # 7. WS 订阅同步
        if self._ws_enabled:
            self._sync_ws_subscriptions()
        
        time.sleep(1)  # 1s tick
```

---

## 六、策略逻辑

### 6.1 核心策略

Guardian 是 **纯 Maker（挂单方）** 做市机器人，运行在 Polymarket CLOB。

**核心目的**：挂限价单提供流动性，赚取平台返利奖励。只要在订单簿上挂单提供流动性就能获奖励。因此机器人只做纯 Maker：挂限价买单赚返利，成交后卖出平仓，让资金持续处于"提供流动性"状态。

**只挂单、不吃单（买入侧）**：挂单价若设置不当（跨价）会立即成为 Taker，既付手续费又白费一次挂单机会。机器人通过监控 best_bid 变化实时调价来避免跨价，并用 `post_only=True` 兜底。

### 6.2 进入策略：如何挂买单

#### 6.2.1 挂单价位

监控目标市场买盘（Bids，从高到低），挂在第 `maker_rank` 档。默认 `maker_rank=2`（买二档）。

```
买盘（Bids）:
  档位 1（best_bid）: 0.51  ← 市场最高买价
  档位 2:            0.50   ← 目标档位（maker_rank=2）
  档位 3:            0.49
```

**为什么不挂第 1 档？** best_bid 竞争最激烈、利润薄。往后挂成本更低、利润空间更大，但成交更慢。本质是**成交速度 vs 利润空间**的权衡。

#### 6.2.2 挂单量与价格对齐

- 每单固定 `maker_size=50`（50 USDC 等值）
- 挂订单簿实际第 `maker_rank` 档价（V8.3 起**不再 round**——订单簿价格本身即该市场合法
  tick 倍数；各市场 tick 各异 `0.1/0.01/0.005/0.0025/0.001/0.0001`，硬编码 0.01 会把
  档位错误吸附，如 0.952→0.95、0.933→0.93）

#### 6.2.3 Post-Only 保护

所有 BUY 用 `post_only=True`：要么挂上簿赚返利，要么被拒（`invalid post-only order: order crosses book`），**绝不以 Taker 成交**。

识别到 post-only 拒绝后跳过无意义重试，由冷却→重新查价自然恢复。这是纯 Maker 性质的最终保证。

#### 6.2.4 市场来源

两种方式并行，token_id 自动去重：

**方式一：内置筛选器（主动，主要来源）**

每 30s 从 CLOB `/sampling-markets` 拉取有返利的市场，按订单簿深度算 `reward_per_dollar` 评分排序，按深度阈值筛选 YES/NO 方向，结果**内存直传** `_markets`。

`_file_managed_ids` 跟踪哪些来自筛选器；某 token 不再出现在筛选结果中时停止监控（撤单 + 移除）。

同时写 `screener_latest.csv` 供人工查看（不参与数据流）。

**方式二：已有订单接管（被动）**

`discover()` 扫描交易所已有 BUY 单，发现未管理的订单时初始化 `MarketState` 接管。用于重启后恢复。

#### 6.2.5 批量撤单

一轮检测到多个 best_bid 变化时，收集 order_id 一次 `cancel_orders`（≤1000）批量撤，而非逐个。

关闭时 `cancel_all` 一次清空。

### 6.3 退出策略：两级卖出（即时 + 兜底）

> **这是 V8 的关键设计，两条路径用途不同、价格不同，刻意为之。**

#### 6.3.1 即时卖出路径（BUY 成交触发，~1-2s，taker@best_bid）

```
handle_trade() 收到 BUY CONFIRMED
  → _pending_sell_tokens[asset_id] = fill_price   （仅 dict 写，不阻塞）
      ↓ 主循环 1s tick
  _check_pending_sells() → 为每个 token 启动守护线程
      ↓
  _sell_single_position(tid, fill_price):
    1. 查 best_bid（单条 POST /books）
    2. sell_min_bid_gap 保护：best_bid < fill_price - gap → 跳过（大单打薄簿，交给兜底）
    3. 查 open_orders，已有相同价卖单 → 不重挂
    4. 查链上余额 onchain_balance（最多重试 5 次×2s，等链上到账）
    5. 挂限价卖单 @ best_bid
```

**为什么挂在 best_bid（会立即成交 = taker）？**

买入刚成交，要**快速平仓落袋**，趁市场没变直接跨价吃单卖出。付一点 taker 费换确定性退出。

#### 6.3.2 兜底卖出路径（每 120s 扫描，maker@best_ask）

```
check_positions() 每 120s:
  1. Data API 查全部持仓
  2. 批量查 best_ask（POST /books，asks[-1] = 最低卖价）
  3. 查 open_orders 找已有卖单
  4. 对每个持仓：
     - best_ask < 成本 - sell_min_bid_gap（市场崩了）→ 取消卖单，持有等回稳
     - 已有卖单且价 == best_ask → 保持不动（保住队列位置）
     - 否则 → 查余额 → 挂/追价限价卖单 @ best_ask
```

**为什么挂在 best_ask（不会立即成交 = maker）？**

即时路径没卖掉的（被 gap 保护跳过、余额没到账、best_bid 太低），转为**耐心 maker**：挂在卖一档等买家来吃，既赚价差又不付 taker 费。

#### 6.3.3 两级关系

- **即时路径**（taker 快速平仓）优先
- **兜底路径**（maker 耐心挂）接管即时路径未成的情况
- **双保险 + 自愈**：重启/事件丢失后最多 120s 自动补挂
- **并发保护**：`_selling` set + `_sell_lock`，同一 token 不重复触发

### 6.4 市场状态机（仅买单侧）

```
        ┌──────────┐
   ┌───→│ NO_ORDER │←────────┐
   │    └────┬─────┘         │ 冷却到期
   │  查到足够档位│           │
   │    ┌────▼─────┐         │
   │    │ PLACING  │         │
   │    └────┬─────┘         │
   │  下单成功  │            │
   │    ┌────▼─────┐ 撤单成功 │
   │    │ RESTING  ├─────────┤
   │    └──┬────┬──┘         │
   │  成交  │    │ best_bid变化│
   │ (不变) │    │            │
   │    ┌───▼────▼─┐         │
   │    │CANCELING │         │
   │  ┌─┤  (异步)  ├─┐       │
   │  │ └──────────┘ │       │
   │  │成功        失败│      │
   │  ▼              ▼       │
   │ STOPPED    RESTING(保持) │
   │                  ┌──────▼───┐
   └──────────────────│ COOLING  │
                      │120s→重挂  │
                      └──────────┘
```

| 状态 | 含义 |
|------|------|
| NO_ORDER | 无订单挂在交易所 |
| PLACING | 正在提交订单（异步） |
| RESTING | 订单已挂簿，监控 best_bid |
| CANCELING | 正在撤单（异步） |
| COOLING | 撤单后冷却 120s 再重挂 |
| STOPPED | 已停止管理（筛选器移除后） |

**注意**：卖单侧不走这个状态机——由 §6.3 的两级路径独立处理，`_selling` set 做并发保护。

### 6.5 best_bid 变化处理（价格反馈回路）

best_bid（第 1 档买价）变化时：
- **首次收到**：仅记录 `ms.best_bid`，不动作（无历史无法判断"变化"）
- **RESTING**：行情变了 → 撤单 → 冷却 120s → 重挂
- **NO_ORDER 且冷却已过**：行情活跃 → 启动冷却准备挂单
- **PLACING / CANCELING**：不处理，由 audit 兜底

**价格源**：
- 当前：REST `POST /books` 轮询（配置 3s，WSS 启用后变 30s 对账）+ 市场频道 WebSocket 实时推送（秒级）
- 真正的下单价由 `_target_price()` 挂单前现查订单簿算 rank 档，`ms.best_bid` 只用来判断"要不要撤"

### 6.6 审计纠偏

`audit()` 每 120s：
- **超价纠偏**：批量查 best_bid，`挂单价 >= best_bid`（超价/跨价风险）→ 批量撤单
- **订单丢失恢复**：RESTING 但 `active_id` 不在 open_orders → 订单丢失，重置重挂
- **卡死状态重置**：PLACING/CANCELING/NO_ORDER 卡死超时（`stale_timeout`=60s）→ 重置
- **重复订单清理**：同一 token 多笔 BUY 单 → 只保留 `active_id`，撤其余重复单
- **孤儿订单清理**（commit 89a9d04）：筛选器已移除且已从 `_markets` pop，但订单仍活在交易所 → 扫 open_orders 直接撤
- 有 pending Future 的市场**跳过**（状态机变更中，不干预）

### 6.7 关键参数

| 参数 | 值 | 说明 |
|------|-----|------|
| `maker_size` | 50 | 每单挂单量（USDC） |
| `maker_rank` | 2 | 挂买盘第几档（买二档） |
| `maker_cooldown` | 120s | 撤单后冷却时间 |
| `tick_size` | 0.01 | 保留字段（V8.3 起不再用于价格 round；市场 tick 各异，订单簿档位价即合法价） |
| `heartbeat_interval` | 7s | 心跳间隔 |
| `best_bid_poll_interval` | 3s | REST 轮询间隔（WSS 启用后变 30s） |
| `audit_interval` | 120s | 审计周期 |
| `position_interval` | 120s | 持仓扫描周期 |
| `sell_min_bid_gap` | 0.02 | 卖出价差保护（best_bid < 成本 - gap 时跳过） |
| `position_threshold` | 1.0 | 最小卖出余额阈值 |

**筛选器参数**（`SCREENER_*`，`.env` 可覆盖）：
- `keyword=temp`（天气类市场参与者少、竞争低）
- `tags=`（逗号分隔的标签 label 列表，命中任一即保留，OR 语义，大小写不敏感；空=不过滤）
- `min_daily_rewards=10`
- `min_existing_size=2000`
- `top1/2/3=100/400/1200`
- `midpoint=[0.15,0.85]`
- `max_volume_24h=inf`（24h 成交量上限，gamma `/markets` 补查；inf=不过滤）
- `max_liquidity=inf`（总流动性上限，gamma 补查；inf=不过滤）

> **成交量/流动性上限**（双接口方案）：`/sampling-markets` 只给 rewards 不给 volume/liquidity，
> 用 `condition_id` 去 gamma `/markets` 补查后按上限过滤，排除已饱和市场。两个上限都是
> `inf` 时不发起任何 gamma 请求（默认等同未加此功能）。漏掉的市场按 0 处理 → 放行。

**评分公式**：
```
reward_per_dollar = total_daily_rewards / (existing_total_size + min_size)
```
衡量每 1 USDC 投入的日返利，越高资金效率越好。

---

## 七、WebSocket 实时监控

### 7.1 架构集成

市场频道 WebSocket 已融入 `Guardian` 主循环（A2 直接集成，无 `GuardianWss` 分支）。

**数据路径**：
```
MarketWS (wss/market_ws.py)
  ├─ WebSocket 子线程接收消息
  ├─ _route() 结构推断路由
  ├─ _handle_price_change() 解析 best_bid
  └─ _on_bid_changed_cb(asset_id, old_bid, new_bid)
       ↓
  投线程安全队列 _ws_bid_queue（主线程独占 _markets，WS 线程不碰）
       ↓
  主循环 _process_ws_bids() drain 队列
       ↓
  _apply_bid_change(ms, token_id, new_bid, None)  ← 与 REST 共享同一 helper
       ↓
  best_bid 变化 → 撤单（与 REST 轮询完全相同的逻辑）
```

**REST + WS 双路径物理合并**：`_apply_bid_change()` 是唯一入口，REST 轮询和 WS 推送都调它，保证处理逻辑完全一致，不再漂移。

### 7.2 连接稳定性

**实测可用率**：98.3%（断线次数少、单次断线时长短）

**断线重连**：指数退避（初始 2s，最大 60s），`_running` 标志控制停止

**心跳保活**：每 10s 发送文本 "PING"，收到 "PONG" 更新 `_last_pong`

### 7.3 订阅机制

**动态订阅**：`subscribe_more(token_ids)` / `unsubscribe(token_ids)`，线程安全

**同步触发**：主循环每 15s 调用 `_sync_ws_subscriptions()`，对齐 `_markets` 集合 ↔ MarketWS 订阅列表

- 新市场（discover/screener 加入）→ 增订
- 移除的市场 → 退订

**连接建立时**：`on_open` 自动全量重订 `subscribed_ids()`

### 7.4 断线策略（B3 最保守）

**断线立即撤全部 RESTING**：
- 连接 `True→False` 沿触发 `_check_ws_connection()` → 批量撤全部挂单
- 目的：防止旧价订单在断线期被逆向成交

**冷却门禁暂停挂新单**：
- 断线期 `_check_cooldowns()` 检测到 `ws_paused=True` → 跳过挂单
- 目的：断线期不在场，不挂新单

**重连后自然重挂**：
- 重连沿累加断线时长统计
- 120s 冷却到期后自然重挂（不惊群）

**Kill switch**：`WS_MARKET_ENABLED=false` 完全关闭 WSS，回退纯 REST 轮询

### 7.5 稳定性统计

每 5min 输出可用率（`_log_ws_stats()`）：
```
[WS STATS] 可用率 98.3% | 运行 1200.0s | 断线 2 次 | 累计断线 20.5s
```

供判断是否需切换 B2 策略（断线时加速 REST，当前未实现）。

### 7.6 路由修复（2026-08-03，commit 31e4d12）

#### 问题现象

- WS 连接稳定，订阅成功，但实时性完全失效
- 8 次 WS 撤单 vs 677 次 REST 撤单
- 所有撤单都在 30s poll 周期，无实时响应

#### 根因

Polymarket WebSocket 消息**完全没有 `type` 或 `event_type` 字段**，旧代码依赖 `data.get("event_type")` 路由，导致所有 3100+ 条消息走默认分支（空操作）。

#### 实际消息格式

**Book 快照（数组）**：
```json
[{
    "market": "0x887e...",
    "asset_id": "59148...",
    "timestamp": "1785745870754",
    "hash": "019a8f77...",
    "bids": [{"price": "0.01", "size": "2684.26"}, ...],
    "asks": [{"price": "0.92", "size": "500"}, ...]
}]
```

**Price change（对象）**：
```json
{
    "market": "0xc120...",
    "price_changes": [
        {
            "asset_id": "60747...",
            "price": "0.32",
            "size": "27.93",
            "side": "BUY",
            "hash": "6a773e83...",
            "best_bid": "0.64",
            "best_ask": "0.65"
        }
    ]
}
```

**注意**：字段名是 **asset_id**（不是 tokenId）、**best_bid**（不是 bestBid），小写+下划线，直接在顶层（不是 payload 子树）。

#### 修复内容

**1. `_route()` 方法重写（结构推断）**

```python
def _route(self, data) -> None:
    """靠结构推断，不依赖 type 字段"""
    if isinstance(data, list):
        # 数组 → book 快照，递归展开
        for item in data:
            if isinstance(item, dict):
                self._route(item)
        return
    
    if not isinstance(data, dict):
        return
    
    # 对象 → 通过关键字段推断
    if "price_changes" in data:
        self._handle_price_change(data)
    elif "bids" in data or "asks" in data:
        self._handle_book(data)
```

**2. `_handle_price_change()` 数据路径修正**

```python
# 旧（错误）
payload = data.get("payload", {})
changes = payload.get("priceChanges", [])
token_id = change.get("tokenId", "")
new_bid = _to_decimal(change.get("bestBid"))

# 新（正确）
changes = data.get("price_changes", [])  # 顶层，无 payload
for change in changes:  # 遍历所有 change（一条消息可能含多个 token）
    asset_id = change.get("asset_id", "")
    new_bid = _to_decimal(change.get("best_bid"))
```

**3. `_handle_book()` bid-only 策略**

```python
# bid-only 策略：WS 只推 bid，ask 由 30s REST 对账维持，此处不解析 asks。
bids = data.get("bids", [])
new_bid = _to_decimal(bids[-1].get("price") if bids else None)
```

#### 验证要点

路由修复后（commit 31e4d12 + e6cb3ef），需服务器重启验证：

1. **实时性恢复**：官网手动改 bid 后 1-2s 内触发撤单（不再等 30s）
2. **WS bid 变化日志大量出现**（之前几乎为 0）
3. **无新报错**（特别是 `'list' object has no attribute 'get'`）

---

## 八、优雅关闭

### 8.1 关闭顺序

```
1. 设置 _stop_flag（主循环退出）
2. cancel_all() 撤销所有挂单（等 timeout）
3. 停止 WebSocket（用户频道 + 市场频道）
4. 停止后台线程池（BackgroundDispatcher）
5. 停止执行层线程池（ExecutionLayer）
6. 最后停止心跳（HeartbeatManager）
```

**关键设计**：撤单在心跳保护窗口内完成，确保订单真正被撤销。

### 8.2 Ctrl+C 信号处理

```python
signal.signal(signal.SIGINT, lambda sig, frame: self._handle_stop())
```

收到 `SIGINT` 后：
1. 打印 "收到停止信号，优雅退出..."
2. 执行上述关闭顺序
3. 打印 "系统已停止"
4. `sys.exit(0)`

### 8.3 强制关闭风险

**警告**：强制关闭（`kill -9` / `screen -X quit`）不会自动撤单，订单继续挂在交易所直到心跳超时（约 15s）被交易所自动取消。

仅在 bot 无响应时使用。

---

## 九、代码审查要点

> 本节保留 HANDOFF_CODE_REVIEW.md 精华，作为演进参考。

### 9.1 高风险区域

#### 1. 手搓 REST 调 CLOB 私有端点

V8 主体已切换到官方 SDK（`polymarket-client`），但部分端点（如 `/books` 批量查询、Data API 持仓查询）仍用 `requests` 直接调。

**风险**：这些端点的鉴权要求可能变化（L1 签名 vs L2 签名），需逐个确认是否需 L2 签名。

**排查方法**：对照官方文档确认端点鉴权要求（用 MCP polymarket）。

#### 2. 卖出/持仓路径

- **asks 排序方向**：`asks[0]` 是最低卖价（升序），`asks[-1]` 是最高卖价
- **余额重试**：`onchain_balance` 查询最多重试 5 次×2s，等链上到账
- **锁释放**：确保 `_sell_lock` / `_pending_sell_lock` 在异常路径也能释放

#### 3. 状态机并发正确性

- **`_pending_ops` 竞态**：已修复（commit de4d08f 之前的问题），现用收件箱模式
- **其他跨线程访问点**：确保 `_markets` 只有主线程碰，其他线程只投队列

#### 4. 数值精度

- **safe_float vs safe_decimal**：混用可能导致精度丢失
- **round_to_tick 舍入方向**：确认是向下取整（不跨价）

#### 5. 异常吞噬

- **`except Exception: return 0.0` 且不打日志**：这类地方可能静默丢失错误
- **建议**：至少打 `logger.debug`，便于事后排查

#### 6. 订单生命周期一致性

- **audit 各分支**：确保 PLACING/CANCELING/NO_ORDER 卡死都能被重置
- **`_removed_by_screener` 清理**：孤儿订单死循环已修复（commit 89a9d04）

### 9.2 审查方法

1. **画状态机转移图**：确认所有状态转移路径都有代码覆盖
2. **跨线程共享数据地图**：确认哪些数据结构被多线程访问，是否有锁保护
3. **对照官方文档**：确认端点鉴权要求（用 MCP polymarket）
4. **可疑点用诊断脚本验证**：只读脚本（如已删除的 `diag_*.py`）不改生产状态
5. **遵循工作流**：本地改 → git commit/push → 服务器 git pull 验证

### 9.3 测试覆盖

- `tests/test_scheduler.py`：定时任务调度器测试
- `tests/test_background.py`：后台任务分发器测试
- `tests/test_ws_market.py`：市场频道 WebSocket 测试（19 个用例）

**运行测试**：
```bash
python -m pytest tests/ -v
```

### 9.4 已知演进方向

1. **WebSocket 可用率监控**：如果可用率 <95%，考虑切换 B2 策略（断线时加速 REST）
2. **批量操作优化**：如果 WS 消息量过大导致主线程处理不过来，考虑批量处理 + 去重
3. **监控增强**：添加 WS 消息延迟统计（服务器时间戳 vs 收到时间）
4. **卖出策略优化**：考虑根据市场深度动态调整即时卖出 vs 兜底卖出的触发条件

---

**Guardian V8 架构文档完**

