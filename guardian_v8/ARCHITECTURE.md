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
第二小步已完成：全部重 REST（screener / discover / poll / audit / position）后台化（只算不写 + 单飞 + 结果队列），`_pending_ops` 跨线程竞态一并修复。

```python
# guardian.run() 简化（scheduler.py: PeriodicScheduler + background.py: BackgroundDispatcher）
bg_handlers = {"screener": _apply_screener_result,   # 后台任务名 → 主线程应用处理器
               "discover": _apply_discover_result,
               "poll":     _apply_poll_result,
               "audit":    _apply_audit_result,
               "position": lambda _p: None}           # check_positions 后台自闭环，无 apply

scheduler = PeriodicScheduler()
# 各任务到期时提交到后台（只算不写），单飞——上一次没跑完就跳过本次
scheduler.add("screener", 30,  lambda: _bg.submit("screener", _screener_fetch),
              run_immediately=True)
scheduler.add("discover", 30,  lambda: _bg.submit("discover", _discover_fetch))
# poll：token_ids 快照在主线程取（lambda 由 tick 在主线程调用），传给后台 _poll_fetch
scheduler.add("poll",     3,   lambda: _bg.submit("poll", _poll_fetch,
                                                  list(_markets.keys())))
scheduler.add("audit",    120, lambda: _bg.submit("audit", _audit_fetch))
scheduler.add("position", 120, lambda: _bg.submit("position", check_positions))
scheduler.add("prune",    300, _prune_caches)
while running:
    try:
        scheduler.tick()             # 各任务到期才触发，内部逐任务捕获异常
        now = time.time()
        _check_cooldowns(now)
        _check_pending_ops(now)      # 回写撤单/挂单结果
        _check_pending_sells()       # 消费即时卖单队列
        _drain_background(bg_handlers)  # 应用后台任务结果（主线程写 _markets）
    except Exception:
        logger.error(...)            # 不再 sleep(10)，保持卖单回写响应
    time.sleep(1)
```

`PeriodicScheduler`（`scheduler.py`）是纯调度逻辑：每任务持有 `interval` + `last_run`，`tick()` 逐个判断到期。**触发前先更新 `last_run`** → 回调抛异常也不热循环；**逐任务 try/except** → 一个任务失败不影响同 tick 其他任务。用注入的 `now_fn`（假时钟）可脱离网络单测，见 `tests/test_scheduler.py`（6 例全过）。

### 6.1 结构性问题：两个都已修

**问题 B — best_bid 轮询实际约 10s 而非 3s ✅ 已修（第一小步）**
旧代码内层 `for _ in range(10): time.sleep(1)` 固定占 ~10s，外层计时器每 ~10s 才求值一次，`best_bid_poll_interval=3.0` 永远达不到。改单一 1s tick 后，各计时器每 1s 求值，poll 回到真正的 3s 粒度（无阻塞时）。

**问题 A — 阻塞 REST 冻结状态机 ✅ 已修（第二小步，全部后台化）**
全部重 REST 任务（screener / discover / poll / audit / position）已从主线程剥离，跑在 `BackgroundDispatcher` 线程池里「只算不写」，产出结果经队列回传，主线程 1s tick 用 `_apply_*` 应用到 `_markets`。主线程 tick 只剩毫秒级轻活（cooldown 检查、pending 回写、结果应用），不再被任何 REST 阻塞。核心不变量「主线程独占 `_markets` 无锁」保持不变——后台线程绝不读写 `_markets`（poll 需要的 token_ids 由主线程快照后作参数传入）。

---

## 七、演进计划

分两轮推进，每轮可单独验证 + commit/push。

### 第一轮 —— H1：主循环重构 + 重任务后台化

分两小步，每步单独 commit + 服务器验证后再进下一步。同时**保住"主线程独占 `_markets` 无锁"这一核心不变量**。

#### 第一小步 ✅ 已完成：单一 1s tick + 独立计时器

删除内层 `for _ in range(10)`，改用 `PeriodicScheduler`：主循环每 1s 转一圈，各周期任务独立计时器判断到期。**只改调度粒度，任务仍同步阻塞执行**（问题 A 不变）。修掉问题 B（poll 回到 3s 粒度）。纯调度逻辑抽成 `scheduler.py`，`tests/test_scheduler.py` 6 例本地验证。

**验证**：本地 pytest 全过；服务器上 `guardian.log` 中 `[POLL]` 日志间隔应回到 ~3s（无 screener 阻塞时），各任务按各自间隔出现。

#### 第二小步 ✅ 已完成：重 REST 全部后台化「只算不写」

基础设施 + 全部 5 个重 REST 任务已迁移，`_pending_ops` 跨线程竞态已修。

1. **基础设施 `background.py`（`BackgroundDispatcher`）✅**：`ThreadPoolExecutor` + 结果队列 + 单飞注册表（锁保护的 `set`）。`submit(name, fn)` 同名在跑时返回 `False` 跳过本次；后台线程只跑纯函数，`(name, ok, result)` 入队；`drain()` 主线程非阻塞取出。异常统一在 `_run` 里捕获记录并作为失败结果入队。`tests/test_background.py` 6 例本地验证。

2. **screener 后台化 ✅**：`_run_screener` 拆为后台 `_screener_fetch`（`fetch_and_filter` + `analyze_orderbooks` + 筛选 + 构建 targets，零 `_markets` 访问）+ 主线程 `_apply_screener_result`（`_apply_market_targets` 写 `_markets` + CSV + 日志）。scheduler 到期时 `self._bg.submit("screener", self._screener_fetch)`；1s tick 末尾 `_drain_background(bg_handlers)` 取结果并按任务名分派给主线程处理器。

3. **discover 后台化 ✅**：`discover` 拆为后台 `_discover_fetch`（只调 `open_orders()`，唯一阻塞的 SDK 分页 REST，零 `_markets` 访问；返回 None 时抛异常由 dispatcher 记录）+ 主线程 `_apply_discover_result`（清理 STOPPED 市场 + 注册新市场 + `_sync_from_file` + 日志，全部写 `_markets`）。STOPPED 清理由「REST 前」移到「结果返回后」，对正确性无影响。首次触发仍在 `discover_interval` 后（不 `run_immediately`，与迁移前一致）。

4. **poll 后台化 ✅**：`_poll_best_bids` 拆为后台 `_poll_fetch(token_ids)`（批量查 `/books`，token_ids 由主线程调度时快照传入，零 `_markets` 访问）+ 主线程 `_apply_poll_result`（更新 `ms.best_bid/ask` + 收集撤单 + `_batch_cancel`）。

5. **audit 后台化 ✅**：`audit` 拆为后台 `_audit_fetch`（`open_orders()` + 批量 best_bid 两次 REST，返回 `(orders, best_bid_map)`，None 时走降级）+ 主线程 `_apply_audit_result`（状态校验 + 超价纠偏 + 重复订单清理，全部写 `_markets`）。

6. **check_positions 后台化 ✅**：整个 `check_positions` 在后台自成闭环——**零 `_markets` 访问**（只读 `positions()`/`open_orders()`/`best_ask`），卖单互斥走 `_sell_lock`+`_selling`，pending 走收件箱。无 apply 阶段，`bg_handlers["position"]` 为 no-op。

7. **`_pending_ops` 竞态已修 ✅**：新增 thread-safe 收件箱 `_pending_ops_inbox`（`queue.Queue`）。后台 `check_positions` 与守护线程 `_sell_single_position` 不再直接 `append`，改投 `_enqueue_pending_op`；主线程 `_check_pending_ops` 开头统一收编。恢复「只有主线程碰 `_pending_ops`」。顺带修 `limit_sell_triggered` 无处理器缺陷：卖单结果日志移到 ms 门禁之前（持仓 token 常不在 `_markets`，原先被门禁静默丢弃）。

**验证**：本地 `py_compile` + 12 例 pytest（scheduler 6 + background 6）全过。服务器上 screener 慢时 `[POLL]`/`[SELL-TRIGGER]`/`[LIMIT SELL]` 回写不再被拖延；`_pending_ops` 无跨线程写入。

### 第二轮 —— WSS 实时 ✅ 已完成

将 `wss/market_ws.py` 的市场频道 WSS **融入 `Guardian`**（A2 直接集成，不再维护 `GuardianWss` 分裂分支）。已删死代码：`wss/guardian_wss.py`（覆盖已改名的 `_poll_best_bids`，早已失效）、`wss/guard.py`、`wss/main.py`；`wss/__init__.py` 精简为只导出 `BidCache, MarketWS`（打破 `wss → guardian → wss` 循环导入）。

**数据面（A2）**：WS 子线程 `_enqueue_bid_change` 只把 bid 变化投 `_ws_bid_queue`（绝不碰 `_markets`）；主线程 tick `_process_ws_bids` drain 队列 → 调**共享** `_apply_bid_change(ms, tid, new_bid, new_ask=None)`。REST 路径 `_apply_poll_result` 也调同一 helper —— 两条路径物理上不可能再漂移（这正是旧子类翻车的教训）。WS 只推 bid（B1 bid-only），`new_ask=None` 不覆盖 `ms.best_ask`，由 REST 对账维持。REST `poll` 在 WS 启用时降到 30s（`ws_rest_reconcile_interval`）作兜底；`ws_sub_sync`（15s）对齐 `_markets` ↔ WS 订阅列表，启动播种后立即同步一次。

**断线策略（B3，最保守）**：主线程 `_check_ws_connection` 检测 `is_connected()` 状态翻转。True→False 立即 `_batch_cancel` 全部 RESTING（防旧价被逆向成交），并由 `_check_cooldowns` 门禁暂停挂新单（断线期不在场）；重连后各市场随 120s 冷却自然重挂（不做惊群式全量重挂）。`WS_MARKET_ENABLED=false` 是 kill switch，一关即回退纯 3s REST 行为，无需改代码。

**稳定性统计**：`_ws_disconnect_count` / `_ws_total_downtime` / `_ws_started_at` 主线程独占累计，`_log_ws_stats` 搭在 prune 任务里每 5min 输出 `[WS STATS] 断线N次 | 累计断线Xs | 运行Ys | 可用率Z%`，供 grep 判断是否需改用 B2（断线提速 REST）。

**验证**：本地 `py_compile` + 31 例 pytest（scheduler 6 + background 6 + ws_market 19）全过；实盘市场频道 smoke test 已连通（订阅 30 市场、收到 30 条真实 bid 推送、BidCache 填满、25s 稳定、干净关闭）。

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
├── wss/             # 市场频道 WSS（cache.py=BidCache / market_ws.py=MarketWS），已融入 Guardian
├── diag_balance.py  # 诊断：余额查询对比（旧手搓REST vs SDK）
├── diag_ws_user.py  # 诊断：用户频道 WS 活测（只读）
├── ARCHITECTURE.md  # 本文件
├── LOGIC.md         # 策略逻辑
├── USAGE.md         # 部署/运维手册
└── data/{instance}/ # guardian.log / trades.log / screener_latest.csv
```
