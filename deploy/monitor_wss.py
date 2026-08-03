"""
Neg Risk 套利监控 — WSS 实时版

架构:
  单 asyncio 事件循环 + 单 WSS 连接 + dict 路由
  - 启动: REST /events 拉取 Neg Risk 事件 → 拿到所有 NO token_id
  - 运行: WSS 订阅全部 NO token, 维护本地 level-2 订单簿
  - 触发: best_ask 变化 → check_arbitrage → (可选) 下单
  - convert: 不自动, 由用户手动执行

对比 monitor_poll.py:
  - 检测延迟: ~100ms (WSS 推送) vs ~5s (REST 轮询)
  - 数据完整: 本地订单簿含 ask_size, 不需再调 /books
  - 带宽: ~9 Mbps @ 1700 token (天气市场)

用法: 直接修改下方 ★ 配置区 ★ 的参数, 然后 python monitor_wss.py
"""
import sys, os, json, time, asyncio
from collections import defaultdict
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

import orjson
import websockets
from websockets.exceptions import ConnectionClosed

# 复用 monitor_poll.py 的成熟逻辑
from monitor_poll import (
    GAMMA, CLOB,
    fetch_neg_risk_events, build_event_entry,
    check_arbitrage, compute_execution_plan,
    print_opportunity, print_execution_plan, save_opportunity,
    place_buy_orders_batch, get_order_status, cancel_order,
    make_clob_client, load_env,
    CLOB_FEE_RATE, FEE_BIPS,
    PUSD_DECIMALS,
    ENV_FILE,
)

# ============================================================
# ★★★ 配置区 — 直接修改后运行 ★★★
# ============================================================

# ---- 监控范围 ----
TAG_ID             = ["103040"]  # Gamma tag_id 过滤, 支持多 tag; ["103040"]=每日温度; ["1"]=sports; []=全部 Neg Risk
MIN_VOLUME         = 0           # 事件最小成交量 (USD), 0=不过滤

# ---- 运行模式 ----
LIVE               = True        # False=dry-run (只打印不下单); True=实盘自动下单
MAX_AMOUNT         = 5           # 单份下单上限 (pUSD); 注意: 部分市场最小下单量为 5
DURATION           = None        # 运行时长秒; None=无限运行直到 Ctrl+C; 300=跑5分钟

# ---- 套利检测 ----
MIN_PROFIT         = 0.0001      # 最小 USDC 利润才报告; 调成负数可观察临界机会
DEDUP_WINDOW       = 30          # 同一 event 相同结果的去重窗口 (秒)

# ---- WSS 连接 ----
WSS_URL            = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL      = 10          # 心跳间隔 (秒), 服务端要求 ≤10s
STATUS_INTERVAL    = 30          # 状态打印间隔 (秒)
RECONNECT_BACKOFF  = 2           # 断线重连初始退避 (秒)
RECONNECT_MAX_BACKOFF = 60       # 最大退避 (秒)

# ---- 下单 (仅 LIVE=True 时生效, 复用 monitor_poll 默认值) ----
ORDER_FILL_TIMEOUT    = 10       # 每轮等待成交超时 (秒), 超时后取消未成交订单并重挂
ORDER_POLL_INTERVAL   = 0.5      # 轮询订单状态间隔 (秒)
MAX_REPEG_ROUNDS      = 5        # GTC 重挂最大轮数 (每轮 ORDER_FILL_TIMEOUT 秒, 超时后用最新 ask 重挂)
REPEG_MAX_PRICE_MULT  = 1.05     # REPEG 涨幅保护: 新价 ≤ orig_ask × 此倍数, 否则放弃重挂留 LIVE
MAX_CONCURRENT_EXEC   = 3        # 同时执行的 EXEC 任务上限 (防止 HTTP 连接池耗尽 / 服务端限流)
FOK_FAIL_COOLDOWN     = 30       # 同 slug FOK 失败后冷却秒数 (避免立即重试触发风暴)

# ---- 日志 ----
DEBUG_RESPONSE        = False    # True=打印 post_orders 原始响应 (调试用); False=静默
SILENCE_SDK_LOG       = True     # True=屏蔽 py_clob_client_v2 的 request error 日志

# ============================================================
# 派生常量 (无需修改)
# ============================================================

OPPS_CSV = os.path.join(os.path.dirname(__file__), "data", "arbitrage_opportunities.csv")

# 屏蔽 SDK 的 request error 日志 (否则每次 FOK 失败都会打印一整行 HTTP 400)
if SILENCE_SDK_LOG:
    import logging
    for name in ("py_clob_client_v2", "py_clob_client_v2.http_helpers", "py_clob_client_v2.http_helpers.helpers"):
        logging.getLogger(name).setLevel(logging.CRITICAL)


def _short_slug(slug: str) -> str:
    """把长 slug 缩短成可读形式, 如 'highest-temperature-in-munich-on-july-17-2026' → 'munich-jul17'"""
    parts = slug.split("-")
    # 找城市名 (通常在中间), 找日期 (july-XX)
    city = ""
    month = ""
    day = ""
    for i, p in enumerate(parts):
        if p in ("january","february","march","april","may","june","july","august","september","october","november","december"):
            month = p[:3]
            if i + 1 < len(parts):
                day = parts[i+1]
            break
        if p not in ("highest","temperature","in","on","lowest","daily","weekly","hourly"):
            if not city:
                city = p
    return f"{city}-{month}{day}" if city and month else slug[:30]


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ============================================================
# 本地 level-2 订单簿
# ============================================================

class OrderBook:
    """单个 token 的内存订单簿 (level-2)

    维护 asks/bids 两个 dict[price -> size]。
    - apply_book: 全量替换 (订阅瞬间 / trade 后)
    - apply_price_change: 增量更新单档 (挂单/撤单)
    """

    __slots__ = ("asset_id", "asks", "bids", "_best_ask", "_best_ask_size")

    def __init__(self, asset_id: str):
        self.asset_id = asset_id
        self.asks: dict[float, float] = {}
        self.bids: dict[float, float] = {}
        self._best_ask: float | None = None
        self._best_ask_size: float = 0.0

    def apply_book(self, msg: dict) -> bool:
        """全量替换订单簿。返回 best_ask 是否变化。"""
        old_best = self._best_ask
        old_size = self._best_ask_size

        self.asks = {}
        for lvl in msg.get("asks", []):
            try:
                p = float(lvl["price"])
                s = float(lvl["size"])
                if s > 0:
                    self.asks[p] = s
            except (KeyError, ValueError):
                continue

        self.bids = {}
        for lvl in msg.get("bids", []):
            try:
                p = float(lvl["price"])
                s = float(lvl["size"])
                if s > 0:
                    self.bids[p] = s
            except (KeyError, ValueError):
                continue

        self._recompute_best_ask()
        return self._best_ask != old_best or self._best_ask_size != old_size

    def apply_price_change(self, change: dict) -> bool:
        """增量更新单档。返回 best_ask 是否变化。

        change 字段: asset_id, price, size, side, best_bid, best_ask
        size=0 表示该档被删除。
        """
        old_best = self._best_ask
        old_size = self._best_ask_size

        try:
            price = float(change["price"])
            size = float(change["size"])
            side = change.get("side", "")
        except (KeyError, ValueError):
            return False

        book = self.asks if side == "SELL" else self.bids
        if size <= 0:
            book.pop(price, None)
        else:
            book[price] = size

        # 如果变化的档位就是 best_ask 或在其附近, 重算; 否则 best_ask 不变
        if side == "SELL":
            if price == self._best_ask or (self._best_ask is None) or price < (self._best_ask or float("inf")):
                self._recompute_best_ask()
        # BUY 档位变化不影响 best_ask

        return self._best_ask != old_best or self._best_ask_size != old_size

    def _recompute_best_ask(self):
        if not self.asks:
            self._best_ask = None
            self._best_ask_size = 0.0
            return
        p = min(self.asks.keys())
        self._best_ask = p
        self._best_ask_size = self.asks[p]

    @property
    def best_ask(self) -> float | None:
        return self._best_ask

    @property
    def best_ask_size(self) -> float:
        return self._best_ask_size


# ============================================================
# 全局状态 (单 asyncio 循环, 无锁)
# ============================================================

events: dict[str, dict] = {}             # slug -> event entry
asset_map: dict[str, tuple[str, str]] = {}  # no_token_id -> (slug, outcome)
orderbooks: dict[str, OrderBook] = {}    # no_token_id -> OrderBook

_last_printed: dict[str, dict] = {}      # slug -> {"key": ..., "time": ...}
_executed_slugs: set = set()
_fok_cooldown: dict[str, float] = {}     # slug -> cooldown 到期时间 (monotonic)
_exec_sem: asyncio.Semaphore | None = None  # EXEC 并发信号量 (main 中初始化)

# 统计
_stats = defaultdict(int)                # event_type -> count
_stats_bytes = defaultdict(int)          # event_type -> bytes
_stats_start = time.monotonic()
_trigger_count = 0
_arb_found_count = 0


# ============================================================
# 同步订单簿状态到 event entry (供 check_arbitrage 使用)
# ============================================================

def sync_orderbook_to_event(asset_id: str):
    """把 OrderBook 的 best_ask/ask_size 写回 event["markets"][i]"""
    slug, outcome = asset_map.get(asset_id, (None, None))
    if not slug or slug not in events:
        return None
    ob = orderbooks.get(asset_id)
    if ob is None:
        return None
    event = events[slug]
    for m in event["markets"]:
        if m["no_token_id"] == asset_id:
            m["best_ask"] = ob.best_ask
            m["ask_size"] = ob.best_ask_size
            return event
    return None


# ============================================================
# 套利检测 + 触发 (纯同步, 快)
# ============================================================

def check_and_trigger(asset_id: str, clob_client, dry_run: bool, max_amount: float):
    """asset_id 的 best_ask 变了 → 同步到 event → 检测套利 → (可选) 下单"""
    global _trigger_count, _arb_found_count
    _trigger_count += 1

    event = sync_orderbook_to_event(asset_id)
    if event is None:
        return

    # 快速预筛: 该 event 是否所有 token 都有 best_ask
    markets_with_ask = [m for m in event["markets"] if m["best_ask"] is not None and m["best_ask"] > 0]
    if len(markets_with_ask) < 2:
        return

    slug = event["slug"]

    # ★ 正在执行 / 已执行 → 完全静默, 不打印任何东西 (避免淹没日志)
    if slug in _executed_slugs:
        return

    # FOK 失败冷却期内 → 静默跳过 (避免风暴)
    now = time.monotonic()
    cd_expiry = _fok_cooldown.get(slug)
    if cd_expiry and now < cd_expiry:
        return
    elif cd_expiry and now >= cd_expiry:
        _fok_cooldown.pop(slug, None)

    result = check_arbitrage(event)
    result_key = orjson.dumps(result, option=orjson.OPT_SORT_KEYS).decode() if result else None

    # 去重: 同一 event 相同结果 DEDUP_WINDOW 秒内不重复打印
    last = _last_printed.get(slug)
    if last and last["key"] == result_key and now - last["time"] < DEDUP_WINDOW:
        return
    _last_printed[slug] = {"key": result_key, "time": now}

    if not result:
        return

    _arb_found_count += 1
    save_opportunity(slug, event, result)

    plan = compute_execution_plan(event, result)
    if not plan:
        return

    # 紧凑一行打印机会
    short = _short_slug(slug)
    bottleneck = min(plan["selected_details"], key=lambda d: d.get("size", 0))
    print(f"[{_ts()}] OPP {short:<20} K={plan['K']:>2} profit={result['usdc_profit']:.4f} "
          f"amt={plan['actual_amount']:.2f} (bn={bottleneck['outcome']} sz={bottleneck['size']:.1f})")

    if dry_run:
        print_execution_plan(slug, event, plan)
    elif clob_client is not None:
        _executed_slugs.add(slug)
        # 下单是阻塞 IO, 放到线程池 + 全局并发信号量, 不阻塞 receive 循环
        asyncio.create_task(_run_exec_with_sem(slug, event, plan, clob_client))


async def _run_exec_with_sem(slug, event, plan, clob_client):
    """用全局 semaphore 限制 EXEC 并发, 防止 HTTP 连接池耗尽 / 服务端限流"""
    global _exec_sem
    if _exec_sem is None:
        await asyncio.to_thread(execute_arbitrage_no_convert, slug, event, plan, clob_client)
        return
    async with _exec_sem:
        await asyncio.to_thread(execute_arbitrage_no_convert, slug, event, plan, clob_client)


# ============================================================
# 实盘下单 (不含 convert, 用户手动)
# ============================================================

def place_buy_orders_batch_debug(client, details, size):
    """带调试输出的批量下单: 打印 post_orders 原始响应, 正确解析 errorMsg/status

    返回: [(detail, order_id_or_None, status_str, error_msg_str, filled_size), ...]
      - order_id 为 None 表示下单失败 (errorMsg 会说明原因)
      - status="matched" 表示已立即成交, filled_size 给出成交量
      - status="live"/"" 表示已挂单等待成交
    """
    from py_clob_client_v2.clob_types import (
        OrderArgs, PartialCreateOrderOptions, OrderType, PostOrdersV2Args,
    )
    try:
        signed_args = []
        for d in details:
            order = client.create_order(
                order_args=OrderArgs(
                    token_id=d["token_id"],
                    price=d["price"],
                    size=size,
                    side="BUY",
                ),
                options=PartialCreateOrderOptions(neg_risk=True),
            )
            signed_args.append(PostOrdersV2Args(order=order, orderType=OrderType.GTC))

        res = client.post_orders(signed_args)

        # ★ 原始响应只在 DEBUG_RESPONSE=True 时打印 (定位 FAIL 根因)
        if DEBUG_RESPONSE:
            try:
                print(f"  [debug] post_orders 原始响应类型: {type(res).__name__}")
                print(f"  [debug] post_orders 原始内容: {json.dumps(res, default=str, ensure_ascii=False)[:1500]}")
            except Exception as e:
                print(f"  [debug] post_orders 响应打印失败: {e}, raw={res!r:.1500}")

        items = res.get("data", res) if isinstance(res, dict) else res
        if not isinstance(items, list):
            items = [items]

        results = []
        for i, item in enumerate(items):
            if i >= len(details):
                break
            if not isinstance(item, dict):
                results.append((details[i], None, "", f"响应项非 dict: {item!r:.200}", 0.0))
                continue

            order_id  = str(item.get("orderID") or item.get("order_id") or item.get("id") or "")
            error_msg = str(item.get("errorMsg") or "")
            status    = str(item.get("status") or "").lower()
            # BUY 单: makingAmount = USDC 付出, takingAmount = NO shares 拿到
            filled_size = 0.0
            try:
                filled_size = float(item.get("takingAmount") or 0)
            except ValueError:
                pass

            # order_id 为空 = 订单被拒
            if not order_id:
                results.append((details[i], None, status, error_msg, 0.0))
            else:
                results.append((details[i], order_id, status, error_msg, filled_size))
        return results
    except Exception as e:
        import traceback
        print(f"  [批量下单异常] {e}")
        print(f"  [traceback] {traceback.format_exc()}")
        return [(d, None, "", str(e), 0.0) for d in details]


def place_buy_order_fok(client, token_id, price, size):
    """单笔 FOK 买单 (Fill-or-Kill: 全成交或立即取消, 不留挂单)

    FOK 是市价单类型, 必须用 MarketOrderArgs(amount=USDC) 而非 OrderArgs(size=shares).
    服务端对市价单的精度规则: maker(USDC) ≤ 2 位小数, taker(shares) ≤ 5 位小数.
    USDC amount 向上取整到 2 位小数, 保证拿到的 shares ≥ 请求的 size.

    返回: (order_id_or_None, status_str, error_msg_str, filled_size)
      - order_id 为 None 表示 FOK 被拒/未成交 (预期情况, 不打印 traceback)
      - status="matched" + filled_size>0 表示 FOK 立即成交
      - filled_size = 实际拿到的 NO shares (读 takingAmount, 不是 makingAmount)
    """
    import math
    from py_clob_client_v2.clob_types import MarketOrderArgs, PartialCreateOrderOptions, OrderType
    try:
        from py_clob_client_v2.exceptions import PolyApiException
    except ImportError:
        PolyApiException = None

    # USDC 金额 = size × price, 向上取整到 2 位小数 (服务端市价单 maker 规则),
    # 保证 round_down(amount, 2) / price ≥ size → 拿到的 shares ≥ size
    usdc_amount = math.ceil(size * price * 100) / 100

    try:
        res = client.create_and_post_market_order(
            order_args=MarketOrderArgs(
                token_id=token_id,
                amount=usdc_amount,
                side="BUY",
                price=price,
                order_type=OrderType.FOK,
            ),
            options=PartialCreateOrderOptions(neg_risk=True),
            order_type=OrderType.FOK,
        )
    except Exception as e:
        # FOK 失败是预期结果 (ask 被对手抢光), 提取 clean message, 不打印 traceback
        msg = str(e)
        # PolyApiException 的 error_msg 里通常有 "order couldn't be fully filled" 或具体原因
        if PolyApiException and isinstance(e, PolyApiException):
            err = e.error_msg
            if isinstance(err, dict):
                msg = err.get("error") or str(err)
            elif err:
                msg = str(err)
        return (None, "", msg, 0.0)

    # 成功路径: 解析响应
    if DEBUG_RESPONSE:
        try:
            print(f"  [debug] FOK post_order 原始响应: {json.dumps(res, default=str, ensure_ascii=False)[:800]}")
        except Exception:
            print(f"  [debug] FOK post_order 原始响应: {res!r:.800}")

    items = res.get("data", res) if isinstance(res, dict) else res
    if not isinstance(items, list):
        items = [items]
    item = items[0] if items else {}

    order_id  = str(item.get("orderID") or item.get("order_id") or item.get("id") or "")
    error_msg = str(item.get("errorMsg") or "")
    status    = str(item.get("status") or "").lower()
    # BUY 单: makingAmount = USDC 付出, takingAmount = NO shares 拿到
    filled_size = 0.0
    try:
        filled_size = float(item.get("takingAmount") or 0)
    except ValueError:
        pass

    if not order_id:
        return (None, status, error_msg, 0.0)
    return (order_id, status, error_msg, filled_size)


def fetch_asks_rest(token_id: str):
    """REST POST /books 查询单 token 的 ask 列表 (按价格升序).

    WSS 本地 orderbook 可能因消息延迟/丢失而过时, REPEG 前用 REST 拿权威数据。
    返回: list[dict{"price": str, "size": str}] 按价格升序; 失败返回 [].
    """
    import requests as _req
    try:
        r = _req.post(f"{CLOB}/books", json=[{"token_id": token_id}], timeout=5)
        if r.status_code != 200:
            return []
        data = r.json()
        items = data.get("items", []) if isinstance(data, dict) else data
        if not items or not isinstance(items[0], dict):
            return []
        asks = items[0].get("asks", [])
        return sorted(asks, key=lambda x: float(x["price"]))
    except Exception as e:
        sys.stderr.write(f"[fetch_asks_rest] {e}\n")
        return []


def compute_repeg_price(asks_sorted, amount: float, max_price: float):
    """从低到高累加 size, 找到能覆盖 amount 的最低 price (作为 BUY 限价上限).

    BUY 限价单的 price 是 taker 愿意付出的最高价, 服务端会自动从最低 ask 吃到 price。
    返回 (target_price, cumulative_size); 在 max_price 内凑不齐 amount 时返回 (None, cumulative).
    """
    cumulative = 0.0
    for lvl in asks_sorted:
        p = float(lvl["price"])
        if p > max_price:
            break
        cumulative += float(lvl["size"])
        if cumulative >= amount:
            return p, cumulative
    return None, cumulative


def execute_arbitrage_no_convert(slug, event, plan, client):
    """bottleneck-first FOK 策略, 紧凑输出:
       1) 识别 ask_size 最小的 bottleneck, 单笔 FOK 下单 (瞬间判定)
       2) FOK 成交 → 批量 GTC 下剩余 K-1 个 @ ask
       3) FOK 失败 → 干净退出, 零仓位
       不自动取消, 不自动 convert, 用户手动核查
    """
    K = plan["K"]
    amount = plan["actual_amount"]
    details = plan["selected_details"]
    short = _short_slug(slug)
    orig_asks = {d["token_id"]: d["price"] for d in details}  # REPEG 涨幅保护基准

    # 1. 识别 bottleneck (ask_size 最小)
    bottleneck = min(details, key=lambda d: d.get("size", 0))
    remaining = [d for d in details if d is not bottleneck]

    # 2. 单笔 FOK 下 bottleneck
    print(f"[{_ts()}] EXEC {short:<20} | FOK {bottleneck['outcome']:<16} @ {bottleneck['price']:.4f} x {amount:.2f}")
    bk_order_id, bk_status, bk_err, bk_filled = place_buy_order_fok(
        client, bottleneck["token_id"], bottleneck["price"], amount
    )

    if not bk_order_id:
        # FOK 失败 = 没抢到 bottleneck, 干净退出
        err_short = (bk_err or "unknown")[:100]
        print(f"[{_ts()}]   FOK FAIL: {err_short}")
        print(f"[{_ts()}]   ABORT (zero position), cooldown {FOK_FAIL_COOLDOWN}s")
        _executed_slugs.discard(slug)
        _fok_cooldown[slug] = time.monotonic() + FOK_FAIL_COOLDOWN
        return False

    # FOK 成功
    if bk_status == "matched" and bk_filled > 0:
        print(f"[{_ts()}]   FOK OK: {bottleneck['outcome']:<16} x {bk_filled:.2f} (0x{bk_order_id[2:10]}...)")
    else:
        print(f"[{_ts()}]   FOK OK: {bottleneck['outcome']:<16} status={bk_status or 'live'} (0x{bk_order_id[2:10]}...)")

    # 3. 批量 GTC 下剩余 K-1 个
    if not remaining:
        print(f"[{_ts()}]   DONE K=1, bottleneck only")
        return True

    placed = place_buy_orders_batch_debug(client, remaining, amount)

    orders = []
    failed = []
    for d, order_id, status, error_msg, filled_size in placed:
        if order_id:
            orders.append((d, order_id, status, filled_size))
            tag = f"x {filled_size:.2f}" if status == "matched" and filled_size > 0 else f"status={status or 'live'}"
            print(f"[{_ts()}]   BATCH OK  {d['outcome']:<16} @ {d['price']:.4f} {tag}")
        else:
            failed.append((d, error_msg))
            err_short = (error_msg or "unknown")[:200]
            print(f"[{_ts()}]   BATCH FAIL {d['outcome']:<16} @ {d['price']:.4f} | {err_short}")

    # 4. 等待 + 重挂循环: 每轮 ORDER_FILL_TIMEOUT 秒, 超时后取消未成交订单并用最新 ask 重挂
    #    最多 MAX_REPEG_ROUNDS 轮, 最后一轮不重挂 (留 LIVE 订单等成交)
    pending = [(d, oid) for d, oid, status, _ in orders if status != "matched"]
    filled = {oid: fsz for _, oid, status, fsz in orders if status == "matched"}
    filled[bk_order_id] = bk_filled

    for round_num in range(1, MAX_REPEG_ROUNDS + 1):
        if not pending:
            break

        # 4a. 轮询等待 ORDER_FILL_TIMEOUT 秒
        deadline = time.monotonic() + ORDER_FILL_TIMEOUT
        while time.monotonic() < deadline:
            all_done = True
            still_live = []
            for d, oid in pending:
                if oid in filled:
                    continue
                status, size = get_order_status(client, oid)
                if status in ("MATCHED", "FILLED"):
                    filled[oid] = size
                    print(f"[{_ts()}]   FILLED  {d['outcome']:<16} x {size:.2f}")
                elif status in ("CANCELED", "CANCELLED", "EXPIRED"):
                    filled[oid] = size
                    if size > 0:
                        print(f"[{_ts()}]   PARTIAL {d['outcome']:<16} x {size:.2f}")
                elif status == "LIVE":
                    all_done = False
                    still_live.append((d, oid))
                else:
                    all_done = False
                    still_live.append((d, oid))
            if all_done:
                break
            time.sleep(ORDER_POLL_INTERVAL)

        # 移除已 filled/cancelled 的
        pending = [(d, oid) for d, oid in pending if oid not in filled]
        if not pending:
            break

        # 4b. 最后一轮: 不重挂, 留 LIVE 等成交
        if round_num == MAX_REPEG_ROUNDS:
            print(f"[{_ts()}]   MAX_REPEG reached, {len(pending)} orders still LIVE")
            break

        # 4c. 取消未成交订单, 查 partial, 用最新 ask 重挂完全未成交的
        print(f"[{_ts()}]   REPEG round {round_num}/{MAX_REPEG_ROUNDS} for {len(pending)} orders")
        for d, oid in pending:
            cancel_order(client, oid)
        time.sleep(0.3)  # 等取消生效

        to_repeg = []
        for d, oid in pending:
            status, size = get_order_status(client, oid)
            if size > 0:
                filled[oid] = size
                print(f"[{_ts()}]   PARTIAL {d['outcome']:<16} x {size:.2f} (kept before re-peg)")
            if size < amount * 0.99:
                to_repeg.append(d)

        if not to_repeg:
            pending = []
            break

        # 用 REST /books 重新查询 ask (本地 ob 可能过时), 累加 size 找 target_price
        # 涨幅保护: target_price 不得超过 orig_ask × REPEG_MAX_PRICE_MULT
        new_to_repeg = []
        for d in to_repeg:
            orig = orig_asks.get(d["token_id"], d["price"])
            max_price = orig * REPEG_MAX_PRICE_MULT
            asks_sorted = fetch_asks_rest(d["token_id"])
            if not asks_sorted:
                ob = orderbooks.get(d["token_id"])
                if ob and ob.best_ask:
                    d["price"] = ob.best_ask
                new_to_repeg.append(d)
                print(f"[{_ts()}]   REPEG (ob)  {d['outcome']:<16} @ {d['price']:.4f} (REST 不可用)")
                continue
            rest_best = float(asks_sorted[0]["price"])
            target_price, cum_size = compute_repeg_price(asks_sorted, amount, max_price)
            if target_price is None:
                print(f"[{_ts()}]   REPEG SKIP {d['outcome']:<16} ask {rest_best:.4f} 累计 {cum_size:.2f} < {amount:.2f} 或 > {max_price:.4f}, 留 LIVE")
                continue
            d["price"] = target_price
            new_to_repeg.append(d)
            print(f"[{_ts()}]   REPEG REST {d['outcome']:<16} @ {target_price:.4f} (REST ask {rest_best:.4f} 累计 {cum_size:.2f})")
        to_repeg = new_to_repeg
        if not to_repeg:
            pending = []
            break

        re_placed = place_buy_orders_batch_debug(client, to_repeg, amount)
        new_pending = []
        for d, (_, new_oid, new_status, new_err, new_fsz) in zip(to_repeg, re_placed):
            if not new_oid:
                print(f"[{_ts()}]   REPEG FAIL {d['outcome']:<16} | {(new_err or 'unknown')[:200]}")
                continue
            if new_status == "matched" and new_fsz > 0:
                filled[new_oid] = new_fsz
                print(f"[{_ts()}]   REPEG FILLED {d['outcome']:<16} x {new_fsz:.2f}")
            else:
                new_pending.append((d, new_oid))
                print(f"[{_ts()}]   REPEG NEW  {d['outcome']:<16} @ {d['price']:.4f}")
        pending = new_pending

    # 5. 最终未成交报告 (不自动取消, 留 LIVE)
    for d, oid in pending:
        if oid in filled:
            continue
        status, size = get_order_status(client, oid)
        filled[oid] = size
        if size > 0:
            print(f"[{_ts()}]   PARTIAL {d['outcome']:<16} x {size:.2f} (order_id={oid[:14]}...)")
        else:
            print(f"[{_ts()}]   LIVE    {d['outcome']:<16} 未成交 (order_id={oid[:14]}...)")

    # 6. 汇总 + convert 参数
    sizes = list(filled.values())
    full_count = sum(1 for s in sizes if s >= amount * 0.99)
    partial_count = sum(1 for s in sizes if 0 < s < amount * 0.99)
    empty_count = sum(1 for s in sizes if s == 0)
    total_filled = sum(sizes)

    print(f"[{_ts()}] RESULT {short:<20} | full {full_count}/{K} partial {partial_count}/{K} empty {empty_count}/{K} | total NO {total_filled:.2f}")
    print(f"[{_ts()}]   CONVERT: marketId={plan['market_id'][:18]}... indexSet={plan['index_set_hex']} amount={plan['amount_raw']}")
    return True


# ============================================================
# WSS 消息处理
# ============================================================

def handle_message(raw: bytes | str, clob_client, dry_run: bool, max_amount: float):
    """解析一条 WSS 消息, 更新订单簿, 触发检测。同步, 必须 < 1ms。"""
    if isinstance(raw, bytes):
        data = orjson.loads(raw)
    elif isinstance(raw, str):
        if raw in ("PONG", "pong", "ping"):
            _stats["heartbeat"] += 1
            return
        data = orjson.loads(raw)
    else:
        return

    if not isinstance(data, dict):
        # 可能是数组 (book 初始批量快照)
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    _handle_book(item, clob_client, dry_run, max_amount)
        return

    etype = data.get("event_type")
    if etype is None:
        return

    _stats[etype] += 1
    _stats_bytes[etype] += len(raw) if isinstance(raw, (str, bytes)) else 0

    try:
        if etype == "book":
            _handle_book(data, clob_client, dry_run, max_amount)
        elif etype == "price_change":
            _handle_price_change(data, clob_client, dry_run, max_amount)
        elif etype == "best_bid_ask":
            # 冗余信号, price_change 已经覆盖。可选触发。
            _handle_best_bid_ask(data, clob_client, dry_run, max_amount)
        elif etype == "market_resolved":
            _handle_market_resolved(data)
        # last_trade_price / tick_size_change / new_market: 暂不处理
    except Exception as e:
        # 单条消息异常不影响整体循环
        sys.stderr.write(f"[handle_message] {etype} 处理失败: {e}\n")


def _handle_book(data, clob_client, dry_run, max_amount):
    asset_id = data.get("asset_id")
    if not asset_id or asset_id not in asset_map:
        return
    ob = orderbooks.get(asset_id)
    if ob is None:
        ob = OrderBook(asset_id)
        orderbooks[asset_id] = ob
    if ob.apply_book(data):
        check_and_trigger(asset_id, clob_client, dry_run, max_amount)


def _handle_price_change(data, clob_client, dry_run, max_amount):
    changes = data.get("price_changes", [])
    for change in changes:
        asset_id = change.get("asset_id")
        if not asset_id or asset_id not in asset_map:
            continue
        ob = orderbooks.get(asset_id)
        if ob is None:
            ob = OrderBook(asset_id)
            orderbooks[asset_id] = ob
        if ob.apply_price_change(change):
            check_and_trigger(asset_id, clob_client, dry_run, max_amount)


def _handle_best_bid_ask(data, clob_client, dry_run, max_amount):
    """best_bid_ask 消息: 直接用 best_ask 触发 (但不含 ask_size, 用现有 orderbook 的 size)"""
    asset_id = data.get("asset_id")
    if not asset_id or asset_id not in asset_map:
        return
    ob = orderbooks.get(asset_id)
    if ob is None:
        return
    try:
        new_ask = float(data["best_ask"])
    except (KeyError, ValueError):
        return
    # 如果 best_bid_ask 报的 best_ask 和我们本地算的不一致, 以服务端为准, 重算 size
    if ob.best_ask != new_ask:
        # 本地可能 missing 该档, 直接触发检测 (用现有 size 或 0)
        # 这里不强行改 ob 状态, 只触发
        check_and_trigger(asset_id, clob_client, dry_run, max_amount)


def _handle_market_resolved(data):
    asset_id = data.get("asset_id")
    if asset_id and asset_id in asset_map:
        slug, _ = asset_map.get(asset_id, (None, None))
        orderbooks.pop(asset_id, None)
        # 不移除整个 event (其他 token 可能还在), 只清理这个 token
        # 实际上 resolved 是整个 market, 但我们订阅的是 NO token, 移除该 token 即可
        if slug:
            print(f"  [market_resolved] {asset_id[:12]}... (slug={slug})")


# ============================================================
# WSS 客户端
# ============================================================

async def wss_receive_loop(ws, clob_client, dry_run: bool, max_amount: float, stop_event: asyncio.Event):
    """主接收循环。每条消息同步处理 (< 1ms), 慢操作 spawn task。"""
    while not stop_event.is_set():
        try:
            raw = await ws.recv()
        except ConnectionClosed as e:
            print(f"[{_ts()}] WSS closed: code={e.code}")
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            sys.stderr.write(f"[wss] recv error: {e}\n")
            await asyncio.sleep(0.5)
            continue

        # 同步处理 (orjson + dict 操作 < 100μs)
        try:
            handle_message(raw, clob_client, dry_run, max_amount)
        except Exception as e:
            sys.stderr.write(f"[wss] handle 异常: {e}\n")


async def wss_heartbeat(ws, stop_event: asyncio.Event):
    """每 10s 发 PING"""
    while not stop_event.is_set():
        try:
            await ws.send("PING")
        except ConnectionClosed:
            return
        except Exception as e:
            sys.stderr.write(f"[wss] heartbeat 异常: {e}\n")
            return
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=PING_INTERVAL)
        except asyncio.TimeoutError:
            continue


async def wss_status_printer(stop_event: asyncio.Event):
    """每 STATUS_INTERVAL 秒打印一行紧凑统计"""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=STATUS_INTERVAL)
        except asyncio.TimeoutError:
            pass
        if stop_event.is_set():
            return
        elapsed = time.monotonic() - _stats_start
        if elapsed <= 0:
            continue
        total_msgs = sum(_stats.values())
        total_bytes = sum(_stats_bytes.values())
        ready = sum(1 for ob in orderbooks.values() if ob.best_ask is not None)
        print(f"[{_ts()}] STATUS {elapsed:.0f}s | {total_msgs} msg ({total_msgs/elapsed:.0f}/s) | "
              f"{total_bytes/elapsed/1024:.0f} KB/s | ob {ready}/{len(orderbooks)} | "
              f"trig {_trigger_count} hit {_arb_found_count} exec {len(_executed_slugs)}")


async def wss_connect_and_run(assets_ids: list[str], clob_client, dry_run: bool, max_amount: float,
                              duration: int | None):
    """连 WSS, 订阅, 跑主循环。断线自动重连。"""
    if not assets_ids:
        print("[wss] 没有 token 可订阅, 退出")
        return

    sub_msg = orjson.dumps({
        "assets_ids": assets_ids,
        "type": "market",
        "custom_feature_enabled": True,
    }).decode()

    backoff = RECONNECT_BACKOFF
    stop_event = asyncio.Event()
    if duration:
        asyncio.create_task(_duration_timer(duration, stop_event))

    while not stop_event.is_set():
        try:
            async with websockets.connect(WSS_URL, max_size=None, ping_interval=None) as ws:
                await ws.send(sub_msg)
                print(f"[{_ts()}] WSS connected, subscribed {len(assets_ids)} tokens")
                backoff = RECONNECT_BACKOFF  # 连上后重置退避

                # 并行: receive_loop + heartbeat + status
                # 用 FIRST_COMPLETED: 任一任务结束就退出, 取消其余, 触发重连
                tasks = [
                    asyncio.create_task(wss_receive_loop(ws, clob_client, dry_run, max_amount, stop_event)),
                    asyncio.create_task(wss_heartbeat(ws, stop_event)),
                    asyncio.create_task(wss_status_printer(stop_event)),
                ]
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for t in pending:
                    t.cancel()
                    try:
                        await t
                    except (asyncio.CancelledError, Exception):
                        pass
                # 打印 done 中可能的异常
                for t in done:
                    if t.exception() and not isinstance(t.exception(), asyncio.CancelledError):
                        sys.stderr.write(f"[wss] task 异常: {t.exception()}\n")
        except asyncio.CancelledError:
            stop_event.set()
            return
        except Exception as e:
            sys.stderr.write(f"[wss] connect error: {e}\n")

        if stop_event.is_set():
            break
        print(f"[{_ts()}] WSS reconnect in {backoff}s...")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=backoff)
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, RECONNECT_MAX_BACKOFF)


async def _duration_timer(duration: int, stop_event: asyncio.Event):
    """duration 秒后设置 stop_event"""
    await asyncio.sleep(duration)
    print(f"[{_ts()}] WSS duration {duration}s reached, exiting")
    stop_event.set()


# ============================================================
# 启动
# ============================================================

def init_events(tag_ids: list[str] | None, min_volume: float):
    """拉取事件, 构建 events/asset_map/orderbooks. tag_ids 为空 list 时拉全部 Neg Risk."""
    seen_slugs: set[str] = set()
    for tag_id in (tag_ids or [None]):
        raw_events = fetch_neg_risk_events(tag_id, min_volume)
        for e in raw_events:
            slug = e.get("slug")
            if not slug or slug in seen_slugs:
                continue
            entry, new_tokens = build_event_entry(e)
            if not entry["markets"]:
                continue
            seen_slugs.add(slug)
            events[entry["slug"]] = entry
            for no_id, slug2, outcome in new_tokens:
                asset_map[no_id] = (slug2, outcome)
                orderbooks[no_id] = OrderBook(no_id)

    total_tokens = len(asset_map)
    print(f"[{_ts()}] INIT {len(events)} events / {total_tokens} NO tokens")
    return total_tokens


def main():
    global _exec_sem
    # 把本地配置同步到 monitor_poll 模块全局 (check_arbitrage / compute_execution_plan 会读)
    import monitor_poll
    monitor_poll.MIN_PROFIT = MIN_PROFIT
    monitor_poll.MAX_AMOUNT = MAX_AMOUNT

    tag_ids = TAG_ID if isinstance(TAG_ID, list) else [TAG_ID]
    tag_ids = [t for t in tag_ids if t]
    dry_run = not LIVE

    # EXEC 并发信号量 (仅在 LIVE 模式需要)
    _exec_sem = asyncio.Semaphore(MAX_CONCURRENT_EXEC) if not dry_run else None

    mode = "DRY-RUN" if dry_run else f"LIVE (max_amount={MAX_AMOUNT})"
    tag_disp = "+".join(tag_ids) if tag_ids else "all"
    print(f"[{_ts()}] START {mode} | tag={tag_disp} | min_profit={MIN_PROFIT} | dur={DURATION or '∞'}")

    total_tokens = init_events(tag_ids, MIN_VOLUME)
    if total_tokens == 0:
        print("[main] 没有 token 可监控, 退出")
        return

    clob_client = None
    if not dry_run:
        env = load_env(ENV_FILE)
        clob_client = make_clob_client(env)
        print(f"[{_ts()}] CLOB ready (proxy={env.get('PROXY_ADDRESS', '?')[:10]}...)")

    assets_ids = list(asset_map.keys())
    print(f"[{_ts()}] WSS subscribing {len(assets_ids)} tokens, Ctrl+C to exit")

    try:
        asyncio.run(wss_connect_and_run(
            assets_ids, clob_client, dry_run, MAX_AMOUNT, DURATION
        ))
    except KeyboardInterrupt:
        print(f"\n[{_ts()}] interrupted")

    elapsed = time.monotonic() - _stats_start
    total_msgs = sum(_stats.values())
    print(f"[{_ts()}] FINAL {elapsed:.0f}s | {total_msgs} msg | trig {_trigger_count} | hit {_arb_found_count} | exec {len(_executed_slugs)}")


if __name__ == "__main__":
    main()
