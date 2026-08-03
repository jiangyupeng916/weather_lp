# Guardian V8 策略逻辑文档

> 适用版本：V8.1
> 最后更新：2026-08-03

本文件描述 **策略逻辑**（买什么、怎么挂、怎么卖、为什么这样设计）。
代码架构（模块、线程、调度）见 [ARCHITECTURE.md](./ARCHITECTURE.md)。

---

## 一、核心策略

Guardian 是 **纯 Maker（挂单方）** 做市机器人，运行在 Polymarket CLOB。

**核心目的：挂限价单提供流动性，赚取平台返利奖励。** 只要在订单簿上挂单提供流动性就能获奖励。因此机器人只做纯 Maker：挂限价买单赚返利，成交后挂卖单平仓，让资金持续处于"提供流动性"状态。

**只挂单、不吃单（买入侧）**：挂单价若设置不当（跨价）会立即成为 Taker，既付手续费又白费一次挂单机会。机器人通过监控 best_bid 变化实时调价来避免跨价，并用 `post_only=True` 兜底。

---

## 二、进入策略：如何挂买单

### 2.1 挂单价位
监控目标市场买盘（Bids，从高到低），挂在第 `maker_rank` 档。默认 `maker_rank=2`（买二档）。

```
买盘（Bids）:
  档位 1（best_bid）: 0.51  ← 市场最高买价
  档位 2:            0.50   ← 目标档位（maker_rank=2）
  档位 3:            0.49
```

不挂第 1 档：best_bid 竞争最激烈、利润薄。往后挂成本更低、利润空间更大，但成交更慢。本质是**成交速度 vs 利润空间**的权衡。

### 2.2 挂单量与价格对齐
每单固定 `maker_size=50`（50 USDC 等值）。下单前价格 `round_to_tick` 对齐到 `tick_size`（0.01）的倍数。

### 2.3 Post-Only 保护
所有 BUY 用 `post_only=True`：要么挂上簿赚返利，要么被拒（`invalid post-only order: order crosses book`），**绝不以 Taker 成交**。识别到 post-only 拒绝后跳过无意义重试，由冷却→重新查价自然恢复。这是纯 Maker 性质的最终保证。

### 2.4 市场来源
两种方式并行，token_id 自动去重：

**方式一：内置筛选器（主动，主要来源）**
每 30s 从 CLOB `/sampling-markets` 拉取有返利的市场，按订单簿深度算 `reward_per_dollar` 评分排序，按深度阈值筛选 YES/NO 方向，结果**内存直传** `_markets`。`_file_managed_ids` 跟踪哪些来自筛选器；某 token 不再出现在筛选结果中时停止监控（撤单 + 移除）。同时写 `screener_latest.csv` 供人工查看（不参与数据流）。

**方式二：已有订单接管（被动）**
`discover()` 扫描交易所已有 BUY 单，发现未管理的订单时初始化 `MarketState` 接管。用于重启后恢复。

### 2.5 批量撤单
一轮检测到多个 best_bid 变化时，收集 order_id 一次 `cancel_orders`（≤1000）批量撤，而非逐个。关闭时 `cancel_all` 一次清空。

---

## 三、退出策略：两级卖出（即时 + 兜底）

> **这是 V8 的关键设计，两条路径用途不同、价格不同，刻意为之。**

### 3.1 即时卖出路径（BUY 成交触发，~1-2s，taker@best_bid）

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

**为什么挂在 best_bid（会立即成交 = taker）？** 买入刚成交，要**快速平仓落袋**，趁市场没变直接跨价吃单卖出。付一点 taker 费换确定性退出。

### 3.2 兜底卖出路径（每 120s 扫描，maker@best_ask）

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

**为什么挂在 best_ask（不会立即成交 = maker）？** 即时路径没卖掉的（被 gap 保护跳过、余额没到账、best_bid 太低），转为**耐心 maker**：挂在卖一档等买家来吃，既赚价差又不付 taker 费。

### 3.3 两级关系
即时路径（taker 快速平仓）优先；它没成时，兜底路径（maker 耐心挂）接管。双保险 + 自愈：重启/事件丢失后最多 120s 自动补挂。并发保护：`_selling` set + `_sell_lock`，同一 token 不重复触发。

---

## 四、市场状态机（仅买单侧）

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

卖单侧不走这个状态机——由 §三 的两级路径独立处理，`_selling` set 做并发保护。

---

## 五、best_bid 变化处理（价格反馈回路）

best_bid（第 1 档买价）变化时：
- **首次收到**：仅记录 `ms.best_bid`，不动作（无历史无法判断"变化"）
- **RESTING**：行情变了 → 撤单 → 冷却 120s → 重挂
- **NO_ORDER 且冷却已过**：行情活跃 → 启动冷却准备挂单
- **PLACING / CANCELING**：不处理，由 audit 兜底

> **价格源现状**：当前 best_bid 来自 REST `POST /books` 轮询（配置 3s，实际约 10s，原因见 ARCHITECTURE.md §6.1）。真正的下单价由 `_target_price()` 挂单前现查订单簿算 rank 档，`ms.best_bid` 只用来判断"要不要撤"。

---

## 六、审计纠偏

`audit()` 每 120s：
- 批量查 best_bid，`挂单价 >= best_bid`（超价/跨价风险）→ 批量撤单
- RESTING 但 `active_id` 不在 open_orders → 订单丢失，重置重挂
- PLACING/CANCELING/NO_ORDER 卡死超时（`stale_timeout`=60s）→ 重置
- 同一 token 多笔 BUY 单 → 只保留 `active_id`，撤其余重复单
- 筛选器已移除但订单仍活 → 重试撤单
- 有 pending Future 的市场**跳过**（状态机变更中，不干预）

---

## 七、心跳保活

Polymarket 需定期心跳，否则挂单约 15s 后被交易所自动取消。V8 用 SDK `get_balance_allowance(COLLATERAL)` 每 7s 作心跳。启动时最先起、关闭时最后停，确保订单存活窗口全覆盖。

---

## 八、关键参数

| 参数 | 值 | 说明 |
|------|-----|------|
| maker_size | 50 | 每单挂单量（USDC） |
| maker_rank | 2 | 挂买盘第几档 |
| maker_cooldown | 120s | 撤单后冷却 |
| tick_size | 0.01 | 价格最小变动 |
| heartbeat_interval | 7s | 心跳间隔 |
| best_bid_poll_interval | 3s | 轮询间隔（实际约 10s，见架构文档） |
| audit_interval | 120s | 审计周期 |
| position_interval | 120s | 持仓扫描周期 |
| sell_min_bid_gap | 0.02 | 卖出价差保护 |
| position_threshold | 1.0 | 最小卖出余额阈值 |

筛选器参数（`SCREENER_*`，`.env` 可覆盖）：keyword=temp、min_daily_rewards=10、min_existing_size=2000、top1/2/3=100/400/1200、midpoint=[0.15,0.85]。

### 评分公式
```
reward_per_dollar = total_daily_rewards / (existing_total_size + min_size)
```
衡量每 1 USDC 投入的日返利，越高资金效率越好。默认 `keyword=temp`（天气类市场参与者少、竞争低、返利效率高）。

---

## 九、演进对策略行为的影响

### 第一轮 H1（主循环重构 + 后台化）
**纯工程改动，不改任何策略语义。** 挂单档位、卖出两级逻辑、价格阈值、状态机全部不变。唯一变化：周期任务改后台线程"只算不写"、结果丢回主线程应用；主循环回到真正 1s 粒度。收益是即时卖单/撤单不再被 screener 阻塞拖延。

### 第二轮 WSS（价格源升级）
best_bid 从"REST 轮询约 10s"升级为"WSS 推送秒级 + REST 30s 对账兜底"。§五的 best_bid 变化处理逻辑**完全不变**，只是触发更快、更准。卖单侧（§三）不动，`best_ask` 仍不启用。
