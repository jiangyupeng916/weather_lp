# Guardian V8 架构文档

> 适用版本：V8.1（`polymarket-client` SDK）
> 实盘入口：`python main.py`（`Guardian` 类，REST 轮询价格）
> 最后更新：2026-08-03

本文件描述 **代码架构**（模块依赖、线程模型、主循环调度、锁层级）。
策略逻辑（买什么、怎么挂、怎么卖）见 [LOGIC.md](./LOGIC.md)。

---

## 一、项目概述

Guardian V8 是 Polymarket CLOB 平台的 **纯 Maker 自动化做市机器人**。

**核心目的**：在订单簿买盘挂限价买单提供流动性，赚取平台返利奖励；买入成交后自动挂卖单平仓，保持资金持续提供流动性。

**技术栈**：Python 3.12+ | `polymarket-client`（官方 PyPI SDK）| `websocket-client` | `httpx` | `requests`

**与 V7 的关键差异**：
- SDK 从 `py_clob_client_v2` 换成官方 `polymarket-client`（`SecureClient.create`）
- 卖出从 FOK 市价改为 **限价卖单**（即时 taker@best_bid + 兜底 maker@best_ask，见 LOGIC.md）
- 凭据推导：WS 用户频道 auth 用 SDK 派生的 `client.credentials`，不再读 `.env` 的 API key
- HTTP/2 禁用补丁：`main.py` 顶部 monkey-patch `httpx.Client.__init__` 强制 `http2=False`（规避部分网络 TLS ALPN h2 被 RST）

---

## 二、运行入口与模式

### 2.1 实盘入口：`main.py`

```
main.py
  ├─ monkey-patch httpx.Client → http2=False（必须在 SDK import 前）
  ├─ INSTANCE = "bot1"（切换账号改此变量）
  ├─ load_dotenv(.env.bot1)（必须在 import config 前，Config 字段 import 期求值）
  ├─ _setup_logging()（控制台 INFO / 文件 data/{instance}/guardian.log DEBUG）
  └─ Guardian(Config(instance_name=INSTANCE)).run()
```

### 2.2 两套代码：`Guardian`（在跑） vs `GuardianWss`（未启用）

| | `Guardian`（guardian.py） | `GuardianWss`（wss/guardian_wss.py） |
|---|---|---|
| best_bid 来源 | REST `POST /books` 轮询 | 市场频道 WSS 推送 + REST 30s 兜底 |
| 入口 | `main.py`（实盘用这个） | 文档写 `python -m wss.main`，**但 wss/main.py 不存在 → 跑不起来** |
| 状态 | 实盘运行中 | 代码存在，从未真正启动过 |

> **重要**：当前实盘 best_bid 完全靠 REST 轮询。`main.py` 里的用户频道 WSS 只接收成交事件（trade/order），不推送市场价格。将 WSS 推送融入 `Guardian` 是本文档 [§七 演进计划](#七演进计划) 的目标。

---

## 三、模块依赖（零循环导入）

```
config.py   ← 无内部依赖（冻结 dataclass，环境变量加载）
models.py   ← 无内部依赖（ActorState 六状态、MarketState、OrderInfo）
utils.py    ← 无内部依赖（safe_float/safe_decimal/round_to_tick/retry_call）
    │
    ├── heartbeat.py   ← config（httpx，SDK get_balance_allowance 作心跳）
    ├── execution.py   ← config, utils（限流+幂等+线程池，全部 SDK 写操作）
    ├── ws_manager.py  ← config（仅用户频道，单线程重连）
    ├── ws_router.py   ← （Guardian 反向持有；解析用户频道 JSON）
    │
    ├── screener/      ← 内置筛选器子包
    │   ├── types.py   ← 数据类（CandidateMarket, ScoredMarket, AllocatedMarket）
    │   ├── markets.py ← config（/sampling-markets 分页拉取 + 过滤）
    │   └── clob.py    ← config + types（/books 批量查订单簿 + 评分排序）
    │
    └── guardian.py    ← 以上全部（主控 + 集中式状态）
            │
            └── main.py  ← config, guardian（实盘入口）

wss/  ← 未启用的市场频道 WSS 实现（cache/market_ws/guardian_wss/guard）
```

---

## 四、核心组件

### 4.1 config.py — 配置
冻结 `dataclass`。**注意**：直接写 `os.environ.get(...)` 的字段在 **import（类定义）时求值**，`field(default_factory=...)` 的字段在**实例化时求值**。`main.py` 已保证先 `load_dotenv` 再 `import config`，两类字段都能读到 `.env`。`pk` 用 `default_factory`（实例化时读），`validate()` 只强制要求 `pk`。

### 4.2 models.py — 数据模型
- `ActorState`：`NO_ORDER / PLACING / RESTING / CANCELING / COOLING / STOPPED`
- `MarketState`：`state / state_at / active_id / active_price / best_bid / best_ask / cooldown_until`，主线程直读直写
- `OrderInfo`：纯数据载体（order_id, price, size, side, token_id, market）

> **字段现状**：`best_ask` 目前**只写不读**（无任何决策读它）；`best_bid` 只被用作"变化探测器"（判断 bid 是否变了），真正下单价由 `_target_price()` 现查订单簿算 rank 档。

### 4.3 heartbeat.py — 心跳保活
守护线程每 `heartbeat_interval`（7s）调用 SDK `get_balance_allowance(asset_type="COLLATERAL")` 作心跳。连续失败 ≥ `heartbeat_max_errors` 打 ERROR 后**清零计数**（当前仅告警，不触发重连）。

### 4.4 execution.py — 执行层
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

保障：`_rate_wait()` 全局限流（写操作间隔 ≥`exec_interval`=0.2s）；`_place_tokens`（TTL 300s）下单幂等；`_cancel_tokens` 撤单幂等；`_do_place` 异常后 `_verify_order_placed` 查 open_orders 兜底（响应丢失≠没挂上）。

### 4.5 ws_manager.py + ws_router.py — 用户频道 WS
`WSManager` 单线程 while 循环处理"连接→断开→重连"，PING 保活。`WSRouter.on_user_message` 解析 JSON 数组 → `event_type=="trade"` 派发 `handle_trade`、`=="order"` 派发 `handle_order`。auth 用 `client.credentials.key/secret/passphrase`（SDK 派生）。

### 4.6 guardian.py — 主控 + 集中式状态
`_markets: Dict[str, MarketState]` 主线程独占直读直写，无锁。`_pending_ops` 列表追踪进行中的异步操作，主循环轮询 `Future.done()` 回写状态。定时任务调度见 [§六](#六主循环调度当前实现)。

### 4.7 screener/ — 内置筛选器
`fetch_and_filter(cfg)`（`/sampling-markets` 分页 + 过滤）→ `analyze_orderbooks(cfg)`（`/books` 批量 + `reward_per_dollar` 评分）→ 内存直传 `_apply_market_targets`，同时写 `data/{instance}/screener_latest.csv`（调试用）。

---

## 五、线程模型（当前实现）

| 线程 | 数量 | 持久性 | 用途 |
|------|------|--------|------|
| MainThread | 1 | 持久 | 主循环 + 全部 `_markets` 状态管理 + **所有周期 REST（阻塞）** |
| heartbeat | 1 | 持久 | SDK 心跳 |
| ws-user | 1 | 持久 | 用户频道 WS（单线程重连） |
| ws-ping | 1 | 持久 | 用户频道 PING |
| exec-N | ≤10 | 持久 | ExecutionLayer 线程池（下单/撤单/卖单） |
| sell-trigger-* | 按需 | 短期 | BUY 成交即时卖单（守护线程，完成即退） |

### 5.1 锁层级（严格单向，防死锁）

```
1. _trade_lock         (Lock)   — _processed_trades 去重
2. _cache_lock         (RLock)  — _market_info 缓存
3. _sell_lock          (Lock)   — _selling 卖出并发保护
4. _pending_sell_lock  (Lock)   — _pending_sell_tokens 队列（WS/触发线程写，主线程读）

+ ExecutionLayer 内部锁（rate/place/cancel）— 独立，不与上述交叉
```

### 5.2 线程安全关键设计
- **`_markets` 主线程独占，无锁**：这是核心不变量。所有状态变更在主循环内完成。
- **异步回传不改状态**：线程池只 `set_result()`，主线程 `_check_pending_ops()` 轮询 `done()` 后改 `_markets`。
- **`_pending_ops` 的隐患**：即时卖单守护线程（`_sell_single_position`）会 `append` 到 `_pending_ops`，主线程同时遍历/pop。当前靠 GIL 不崩，属脆弱点（见 §七 一并处理）。

---

## 六、主循环调度（当前实现）

H1 第一小步已落地：单一 1s tick + `PeriodicScheduler` 独立计时器，删除旧的内层 `for _ in range(10)`。

```python
# guardian.run() 简化（scheduler.py: PeriodicScheduler）
scheduler = PeriodicScheduler()
scheduler.add("screener", 30,  _run_screener, run_immediately=True)  # 阻塞 REST
scheduler.add("discover", 30,  discover)                              # 阻塞 REST
scheduler.add("poll",     3,   _poll_best_bids)                       # 阻塞 REST
scheduler.add("audit",    120, audit)                                 # 阻塞 REST
scheduler.add("position", 120, check_positions)                       # 阻塞 REST
scheduler.add("prune",    300, _prune_caches)
while running:
    try:
        scheduler.tick()             # 各任务到期才触发，内部逐任务捕获异常
        now = time.time()
        _check_cooldowns(now)
        _check_pending_ops(now)      # 回写撤单/挂单结果
        _check_pending_sells()       # 消费即时卖单队列
    except Exception:
        logger.error(...)            # 不再 sleep(10)，保持卖单回写响应
    time.sleep(1)
```

`PeriodicScheduler`（`scheduler.py`）是纯调度逻辑：每任务持有 `interval` + `last_run`，`tick()` 逐个判断到期。**触发前先更新 `last_run`** → 回调抛异常也不热循环；**逐任务 try/except** → 一个任务失败不影响同 tick 其他任务。用注入的 `now_fn`（假时钟）可脱离网络单测，见 `tests/test_scheduler.py`（6 例全过）。

### 6.1 结构性问题：一个已修，一个待第二小步

**问题 B — best_bid 轮询实际约 10s 而非 3s ✅ 已修（第一小步）**
旧代码内层 `for _ in range(10): time.sleep(1)` 固定占 ~10s，外层计时器每 ~10s 才求值一次，`best_bid_poll_interval=3.0` 永远达不到。改单一 1s tick 后，各计时器每 1s 求值，poll 回到真正的 3s 粒度（无阻塞时）。

**问题 A — 阻塞 REST 冻结状态机 ⏳ 待第二小步（后台化）**
`_run_screener` / `discover` / `_poll_best_bids` / `audit` / `check_positions` 仍是**同步阻塞** `requests`，跑在主线程。尤其 `_run_screener`（`/sampling-markets` 分页 × 每页最多 5 次重试 × 退避 2/4/6/8s × timeout 30s，再叠加 `analyze_orderbooks` 所有批次）最坏堵**几十秒到数分钟**，期间 `_check_pending_ops` / `_check_pending_sells` 仍会停摆。第一小步只改了调度粒度，未解除阻塞——留给第二小步后台化。

---

## 七、演进计划

分两轮推进，每轮可单独验证 + commit/push。

### 第一轮 —— H1：主循环重构 + 重任务后台化

分两小步，每步单独 commit + 服务器验证后再进下一步。同时**保住"主线程独占 `_markets` 无锁"这一核心不变量**。

#### 第一小步 ✅ 已完成：单一 1s tick + 独立计时器

删除内层 `for _ in range(10)`，改用 `PeriodicScheduler`：主循环每 1s 转一圈，各周期任务独立计时器判断到期。**只改调度粒度，任务仍同步阻塞执行**（问题 A 不变）。修掉问题 B（poll 回到 3s 粒度）。纯调度逻辑抽成 `scheduler.py`，`tests/test_scheduler.py` 6 例本地验证。

**验证**：本地 pytest 全过；服务器上 `guardian.log` 中 `[POLL]` 日志间隔应回到 ~3s（无 screener 阻塞时），各任务按各自间隔出现。

#### 第二小步 ⏳ 待做：重 REST 后台化「只算不写」

1. **重 REST 任务后台化，且"只算不写"**：
   `_run_screener` / `discover` / `audit` / `check_positions` / `_poll_best_bids` 改为**后台线程执行纯查询/计算**，产出一个"结果对象"（如筛选目标列表、超价待撤 token、待挂卖单等），**丢回主线程的结果队列**，由 1s tick 取出后应用到 `_markets`。后台线程**绝不直接读写 `_markets`**。

2. **单飞（single-flight）**：每类后台任务同一时刻最多一个在跑。到期时若上一个还没结束，跳过本次触发（避免 screener 慢时堆叠线程）。

3. **主线程 1s tick 只做轻活**：应用后台结果、`_check_pending_ops`、`_check_pending_sells`、`_check_cooldowns`。全部非阻塞，毫秒级完成。

4. **顺带修 `_pending_ops` 竞态**：即时卖单线程不再直接 `append`，改为把"待挂卖单请求"放进结果队列，由主线程统一 append 到 `_pending_ops`。恢复"只有主线程碰 `_pending_ops`"。

**验证**：screener 慢时即时卖单/撤单回写不再被拖延；`_pending_ops` 无跨线程写入。

### 第二轮 —— WSS 实时（下一轮）

将 `wss/market_ws.py` 的市场频道 WSS **融入 `Guardian`**（不再维护 `GuardianWss` 分裂分支）：WSS 推 best_bid 变化 → 入队 → 主线程 1s tick 消费 → 触发撤单重挂；`_poll_best_bids` 降频到 30s 作**对账兜底**（用 REST 真值校正 WSS 缓存，兜住漏推送/订阅失效/连接假活）。卖单侧维持现状（REST 现查订单簿），`best_ask` 仍不启用。详见 LOGIC.md 第二轮小节。

---

## 八、优雅关闭

`_shutdown()` 顺序：`cancel_all`（等 `cancel_timeout`）→ 停 WS → 停线程池（`wait=True`）→ **最后停心跳**（确保撤单在心跳保护窗口内完成）。

---

## 九、文件清单

```
guardian_v8/
├── main.py          # 入口：http2 补丁 → dotenv → 日志 → Guardian.run()
├── config.py        # 冻结 dataclass，含 SCREENER_* 参数
├── models.py        # ActorState / MarketState / OrderInfo
├── utils.py         # safe_float/safe_decimal/round_to_tick/retry_call
├── heartbeat.py     # SDK get_balance_allowance 心跳
├── execution.py     # 限流+幂等+线程池，place/cancel/limit_sell/market_sell
├── ws_manager.py    # 用户频道 WS 单线程重连
├── ws_router.py     # 用户频道 JSON 解析 → Guardian
├── guardian.py      # 主控 + 集中式状态 + 定时任务
├── screener/        # types.py / markets.py / clob.py
├── wss/             # 未启用的市场频道 WSS（cache/market_ws/guardian_wss/guard）
├── diag_balance.py  # 诊断：余额查询对比（旧手搓REST vs SDK）
├── diag_ws_user.py  # 诊断：用户频道 WS 活测（只读）
├── ARCHITECTURE.md  # 本文件
├── LOGIC.md         # 策略逻辑
├── USAGE.md         # 部署/运维手册
└── data/{instance}/ # guardian.log / trades.log / screener_latest.csv
```
