# Guardian V8 交接文档 — 完整代码审查

> 用途：给下一个对话做**完整代码审查、查找剩余 bug/漏洞**的起点。
> 生成时间：2026-08-03。生成上下文：卖出全线失效排查（已修复 2 个根因）。

---

## 一、当前代码状态（V8.1）

版本 = V8.0 + 本轮两个卖出 bug 修复。GitHub 已同步至 `main`，最新 commit `731487e`。

- **仓库**：https://github.com/jiangyupeng916/weather_lp
- **本地**：`D:\cursor\guardian\guardian_v7\guardian_v8\`
- **服务器**：`/root/weather_lp/guardian_v8/`（git clone，可 git pull；venv 在同目录）
- **凭据**：`.env.bot1`（含私钥，不在 git，仅服务器有）

### 模块结构
```
guardian_v8/
  config.py        Config dataclass（frozen）+ SCREENER_* 读 env
  models.py        ActorState(6状态) / MarketState / OrderInfo
  utils.py         safe_float / safe_decimal / round_to_tick / retry_call
  heartbeat.py     心跳（httpx）
  execution.py     限流 + 幂等令牌 + ThreadPoolExecutor + 批量撤单 + Post-Only
  guardian.py      主控：集中式状态 + 8 个定时任务（★核心，最需审查）
  ws_manager.py    用户频道 WS，单线程重连，PING 保活
  ws_router.py     用户频道 JSON 解析 → Guardian 分发
  main.py          HTTP/2 禁用补丁（最顶部）+ INSTANCE 变量 + 提前 load_dotenv
  screener/        内置筛选器子包（__init__ / types / markets / clob）
  wss/             市场频道 WS 实时版（GuardianWss 子类，当前主流程未启用）
  test_sdk.py      全链路测试（下单/查询/撤单）
  diag_balance.py  余额诊断（本轮新增，纯只读）
  diag_ws_user.py  用户频道 WS 活测（本轮新增，纯只读）
  USAGE.md         使用手册
```

---

## 二、本轮已修复（勿重复排查，但可作为"同类 bug"的线索）

### Bug 1 — onchain_balance 手搓 REST 恒 401（commit `4f0ecd1`）
- `guardian.py: onchain_balance()` 曾手搓 `/balance-allowance` 只发 `POLY_ADDRESS` 头 → CLOB L2 端点必须完整 L2 签名 → 恒 401 → 静默 `return 0.0`。
- 修复：改用 SDK `client.get_balance_allowance(asset_type="CONDITIONAL", token_id=...)`。
- **审查线索**：全局搜其他"手搓 requests 调 CLOB 私有端点"的地方，同样会 401。见下方第四节。

### Bug 2 — handle_trade 去重顺序颠倒（commit `731487e`）
- 同一成交推 `MATCHED→MINED→CONFIRMED` 共用同一 trade id。旧代码先去重再判 status → CONFIRMED 被先到的 MATCHED/MINED 占坑丢弃。
- 修复：先 `if status != "CONFIRMED": return`，再去重。

### 已验证健康（无需再查）
- 用户频道 WS + 鉴权：`diag_ws_user.py` 实测正常，实时推送秒级到达。
- 订阅帧格式：与官方文档（[User Channel](https://docs.polymarket.com/api-reference/wss/user)）一致。

---

## 三、审查重点区域（按风险排序）

### 1. 手搓 REST 调 CLOB 私有端点（高危，与 Bug1 同源）
`guardian.py` 里多处用裸 `requests` 直连，需逐个确认端点是否需要鉴权：
- `onchain_balance` — 已修
- `positions()`（第~734行）→ 走 `data-api.polymarket.com/positions`，**公开端点，只需 user 参数**，应该没问题，但要确认。
- `market_info()` → `host/markets-by-token/`，公开，确认。
- `_poll_best_bids()` / `audit()` / `check_positions()` / `_fetch_single_best_bid()` → `host/books`，POST，**公开端点**，确认无需鉴权。
- **审查动作**：列出所有 `requests.get/post(self.cfg.host...)`，逐个对照官方文档确认是公开还是需 L2。私有端点一律换 SDK 方法。

### 2. 卖出/持仓路径的完整性（本轮重灾区，重点复查）
- `check_positions()`（第~1018行）：best_ask 取值 `asks[-1]`（假设降序）——**确认 /books 的 asks 排序方向**，取错方向会挂到最差价。
- `_sell_single_position()`（第~1124行）：即时卖出线程，余额重试 5 次×2s。确认 `_pending_sell_tokens` 消费、`_selling` 锁释放无死锁/泄漏。
- `sell_min_bid_gap` 保护逻辑：`best_ask/best_bid < 成本 - gap` 时跳过。确认 Decimal/float 比较无类型混用。

### 3. 状态机并发正确性（guardian.py 核心）
- `_markets` 声称"主线程直读直写无锁"，但 `_sell_single_position` 在**独立线程**里 append `_pending_ops`、读 `_markets`、调 `open_orders()`。确认这些跨线程访问是否真的安全（`_pending_ops` 是普通 list，主循环也在 append/pop）。**这是最可疑的并发点。**
- `_check_pending_ops` 遍历 + pop 与其他线程 append 是否竞态。
- `_selling` / `_pending_sell_lock` / `_trade_lock` / `_sell_lock` 覆盖范围是否有遗漏。

### 4. 数值与精度
- `safe_float` vs `safe_decimal` 混用：价格比较处若一边 float 一边 Decimal 会 TypeError 或精度误判。
- `round_to_tick` 买单舍入方向（ROUND_HALF_UP）对 maker post-only 是否会导致跨价被拒。
- `onchain_balance` 返回 `ba.balance / 1_000_000`——确认条件代币精度确实是 6 位（USDC 是 6，条件代币需确认）。

### 5. 异常吞噬（与两个 bug 同一类病根）
- 全局搜 `except Exception:` 后 `return 0.0/None/[]/False` 且**不打日志**的地方——Bug1 就是这么被藏了几个月。这类静默兜底是 bug 温床，逐个评估是否该加 error 日志。

### 6. 订单生命周期一致性
- audit() 的"重复订单检测""超价纠偏""筛选器移除重试"逻辑复杂，确认各分支不会误撤 active_id 或漏撤野单。
- `_removed_by_screener` 集合的清理时机（discover 里注释说"不清除"防孤儿，确认不会无限增长）。

---

## 四、审查方法建议

1. 先通读 `guardian.py`（1300+ 行，主控），画出：定时任务触发关系 + 状态机转移图 + 所有跨线程共享数据。
2. 对照官方文档确认每个 CLOB 端点的鉴权要求（用 MCP `polymarket` 文档服务，本地 WebFetch 被网络限制挡）。
3. 每发现一处可疑，用 `diag_*.py` 同样手法写只读脚本在服务器验证，不猜。
4. 修复遵循工作流：本地改 → 语法检查 → git commit + push → 服务器 git pull 重启验证。

## 五、工作约束（务必遵守）
- 每轮改动必须 git commit + push（见 [[preferred-workflow]]）。
- 只 `git add` 具体文件，勿 `git add .`（工作区有大量未跟踪的 data/、deploy/ 等）。
- 绝不在对话里显示 PK / CLOB_SECRET / CLOB_PASS_PHRASE / CLOB_API_KEY 的值。
- 涉及资金的改动（下单/撤单/卖出逻辑）先说明再改，验证用只读脚本。

## 六、相关记忆文件
- `sell-failure-two-bugs.md` — 本轮两 bug 详情 + 排查手法
- `guardian-v7-project-context.md` — 项目全貌、参数表、服务器信息
- `merge-and-deploy-plan.md` — 待办清单
- `preferred-workflow.md` — 工作流规则
