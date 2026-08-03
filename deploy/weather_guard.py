#!/usr/bin/env python3
"""
Weather Neg Risk 高确定性下单框架

周期扫描 Weather 标签 Neg Risk 事件 → Event 预筛 → Market 精选 → 下单

用法:
  python weather_guard.py              # 单次运行
  python weather_guard.py --interval 10  # 每 10 分钟循环
  python weather_guard.py --dry-run     # 仅扫描，不实际下单（默认）
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                        ▎可配置参数 — 改这里，点运行                          ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 优先使用本地 v2 SDK（支持 order_type），fallback 到 pip 版
for _SDK_PATH in (
    os.path.join(SCRIPT_DIR, "py_clob_client_v2"),                          # deploy 目录内
    os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", "py-clob-client", "py-clob-client-v2-main", "py-clob-client-v2-main")),  # 本地开发
):
    if os.path.isdir(_SDK_PATH):
        sys.path.insert(0, _SDK_PATH)
        break

try:
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds, OrderArgs, PartialCreateOrderOptions, OrderType
    _IS_V2 = True
except ImportError:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds, BookParams, OrderArgs, PartialCreateOrderOptions  # type: ignore[no-redef]
    _IS_V2 = False

# ── 运行模式 ──────────────────────────────────────────────────────────────────
LIVE_MODE = True           # True=实盘下单 / False=仅扫描(dry-run)
INTERVAL_MINUTES = 10        # 循环间隔（分钟），0=单次执行，如 10=每10分钟扫描一次
QUIET = True                 # True=静默模式，仅输出命中结果

# ── 时间窗口 ──────────────────────────────────────────────────────────────────
END_HOURS_AHEAD = 24       # 未来窗口（小时）：endDate 距现在 ≤ 此值
END_LOOKBACK = 24          # 回溯时间（小时）：endDate 已过但仍 active 的市场也纳入

# ── 筛选阈值 ──────────────────────────────────────────────────────────────────
EVENT_MIN_YES_BID = 0.95   # Event 预筛：任一 Market 的 Yes bid ≥ 此值才进入观察
MARKET_BID_MIN = 0.98      # Market 精选：bid 下限
MARKET_BID_MAX = 0.995     # Market 精选：bid 上限

# ── 下单参数 ──────────────────────────────────────────────────────────────────
ORDER_SIZE = 5.0           # 每单 USDC 份数
ORDER_WORKERS = 3          # 下单并发数（太高会被限流）

# ── 地区隔离 ──────────────────────────────────────────────────────────────────
EXCLUDED_SLUGS = ["hong-kong"]  # slug 包含任一关键词的 market 直接排除（大小写不敏感）

# ── 性能（一般无需修改）───────────────────────────────────────────────────────
GAMMA_WORKERS = 8
CLOB_BATCH_SIZE = 500
CLOB_WORKERS = 8

# ═══════════════════════════════════════════════════════════════════════════════

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"
WEATHER_TAG_ID = 84

# 状态文件
WATCHED_FILE = os.path.join(SCRIPT_DIR, "watched_events.json")
ORDERED_FILE = os.path.join(SCRIPT_DIR, "ordered.json")

# .env 路径
ENV_FILE = os.path.join(SCRIPT_DIR, ".env")


# ═══════════════════════════════════════════════════════════════════════════
#  数据结构
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TokenInfo:
    token_id: str
    side: str           # "yes" | "no"
    index: int          # 0=yes, 1=no
    clob_bid: float
    clob_ask: float


@dataclass
class MarketInfo:
    market_id: str
    question: str
    slug: str
    outcomes: list[str]
    gamma_prices: list[float]
    tokens: list[TokenInfo]   # [yes_token, no_token]
    end_date: str
    remaining_hours: float
    volume: float
    liquidity: float
    tick_size: float
    neg_risk: bool


@dataclass
class EventInfo:
    event_id: str
    title: str
    slug: str
    end_date: str
    neg_risk: bool
    markets: list[MarketInfo]
    total_volume: float = 0.0


@dataclass
class MarketHit:
    """一条命中待下单记录"""
    market_id: str
    question: str
    event_id: str
    event_title: str
    side: str          # "yes" | "no"
    token_id: str
    bid: float
    ask: float
    tick_size: float
    end_date: str
    volume: float


# ═══════════════════════════════════════════════════════════════════════════
#  HTTP 工具
# ═══════════════════════════════════════════════════════════════════════════

def _http_get(url: str, timeout: int = 30, retries: int = 3) -> bytes:
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep((attempt + 1) * 2)
    raise last_err  # type: ignore[misc]


# ═══════════════════════════════════════════════════════════════════════════
#  Gamma API — 分页拉取
# ═══════════════════════════════════════════════════════════════════════════

def fetch_all_markets(end_hours_ahead: int, end_lookback: int) -> list[dict]:
    """分页拉取 Weather 标签全部活跃市场（分批探测，遇空即停）"""
    now = datetime.now(timezone.utc)
    end_min = now - timedelta(hours=end_lookback)
    end_max = now + timedelta(hours=end_hours_ahead)

    def _fetch_page(offset: int) -> list[dict]:
        params = (
            f"tag_id={WEATHER_TAG_ID}&active=true&closed=false"
            f"&end_date_min={end_min.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            f"&end_date_max={end_max.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            f"&limit=100&offset={offset}"
        )
        url = f"{GAMMA_API}/markets?{params}"
        try:
            return json.loads(_http_get(url, timeout=60))
        except Exception:
            return []  # 网络错误视为空页

    all_markets: list[dict] = []
    empty_streak = 0

    # 分批提交，遇空 3 连即停（防止网络抖动误杀）
    for batch_start in range(0, 12000, GAMMA_WORKERS * 100):
        batch_offsets = list(range(batch_start, batch_start + GAMMA_WORKERS * 100, 100))

        with ThreadPoolExecutor(max_workers=GAMMA_WORKERS) as executor:
            futures = {executor.submit(_fetch_page, o): o for o in batch_offsets}
            for f in as_completed(futures):
                page = f.result()
                if not page:
                    empty_streak += 1
                    continue
                empty_streak = 0
                all_markets.extend(page)

        # 连续 2 批全部为空 → 后面不会再有数据
        if empty_streak >= GAMMA_WORKERS * 2:
            break

    return all_markets


# ═══════════════════════════════════════════════════════════════════════════
#  CLOB /prices — 批量查 bid + ask
# ═══════════════════════════════════════════════════════════════════════════

def fetch_prices_sdk(client: ClobClient, token_ids: list[str], side: str = "BUY") -> dict[str, float]:
    """通过 CLOB SDK get_prices 批量查指定方向报价"""
    batches = [token_ids[i:i + CLOB_BATCH_SIZE]
               for i in range(0, len(token_ids), CLOB_BATCH_SIZE)]

    all_results: dict[str, float] = {}

    def _fetch_one(batch: list[str]) -> dict[str, float]:
        try:
            if _IS_V2:
                params = [{"token_id": tid, "side": side} for tid in batch]
            else:
                params = [BookParams(token_id=tid, side=side) for tid in batch]
            result = client.get_prices(params)
            return {tid: float(data.get(side, 0))
                    for tid, data in result.items()}
        except Exception as e:
            print(f"  [CLOB error] batch {len(batch)} tokens: {e}", file=sys.stderr)
            return {}

    with ThreadPoolExecutor(max_workers=CLOB_WORKERS) as executor:
        futures = {executor.submit(_fetch_one, b): i for i, b in enumerate(batches)}
        for f in as_completed(futures):
            all_results.update(f.result())

    return all_results


# ═══════════════════════════════════════════════════════════════════════════
#  环境 & CLOB 客户端
# ═══════════════════════════════════════════════════════════════════════════

def load_env(env_path: str) -> dict:
    """加载 .env 文件"""
    env = {}
    if not os.path.exists(env_path):
        return env
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k] = v
    return env


def make_clob_client(env: dict) -> ClobClient:
    """根据 .env 创建 CLOB 客户端（对齐 market_monitor.py）"""
    required = ["CLOB_API_KEY", "CLOB_SECRET", "CLOB_PASS_PHRASE", "PK"]
    missing = [k for k in required if k not in env]
    if missing:
        raise RuntimeError(f".env 缺少: {', '.join(missing)}")

    chain_id = int(env.get("CHAIN_ID", "137"))
    creds = ApiCreds(
        api_key=env["CLOB_API_KEY"],
        api_secret=env["CLOB_SECRET"],
        api_passphrase=env["CLOB_PASS_PHRASE"],
    )

    funder = env.get("PROXY_ADDRESS", "") or env.get("FUNDER_ADDRESS", "")
    funder = funder.strip()
    sig_type_str = env.get("SIGNATURE_TYPE", "").strip()

    if sig_type_str:
        signature_type = int(sig_type_str)
    elif funder:
        signature_type = 1   # POLY_PROXY
    else:
        signature_type = 0   # EOA

    return ClobClient(
        host=CLOB_HOST,
        chain_id=chain_id,
        key=env["PK"],
        creds=creds,
        funder=funder or None,
        signature_type=signature_type,
    )


def place_order(client: ClobClient, token_id: str, price: float, size: float,
                tick_size: float = 0.01) -> str | None:
    """下一笔 GTC 限价买单，返回 order_id，失败返回 None"""
    try:
        kwargs = dict(
            order_args=OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side="BUY",
            ),
            options=PartialCreateOrderOptions(tick_size=str(tick_size)),
        )
        try:
            kwargs["order_type"] = OrderType.GTC
        except NameError:
            pass  # pip 版 SDK 无 OrderType，忽略
        res = client.create_and_post_order(**kwargs)
        return str(res.get("orderID") or res.get("order_id", "")) or None
    except Exception as e:
        print(f"    [ORDER FAIL] token={token_id[:20]}... price={price:.4f}: {e}",
              file=sys.stderr)
        return None


# ═══════════════════════════════════════════════════════════════════════════
#  数据组装
# ═══════════════════════════════════════════════════════════════════════════

def build_events(markets_raw: list[dict], prices: dict[str, float]) -> list[EventInfo]:
    """将 Gamma 原始数据 + CLOB 价格组装为 EventInfo 列表"""
    now = datetime.now(timezone.utc)
    event_map: dict[str, EventInfo] = {}

    for m in markets_raw:
        tids = json.loads(m.get("clobTokenIds", "[]"))
        if len(tids) < 2:
            continue

        try:
            outcomes = json.loads(m.get("outcomes", "[]"))
            raw_prices = m.get("outcomePrices", "[]")
            gamma_prices = [float(p) for p in json.loads(raw_prices)] if raw_prices else [0, 0]
        except (json.JSONDecodeError, ValueError):
            continue

        if len(outcomes) < 2 or len(gamma_prices) < 2:
            continue

        # Token 方向: outcomes = ['Yes', 'No'], clobTokenIds[0]=Yes, clobTokenIds[1]=No
        yes_bid = prices.get(tids[0], 0)
        no_bid = prices.get(tids[1], 0)
        yes_ask, no_ask = 0.0, 0.0   # SELL 价格在命中后再查

        end_str = m.get("endDate", m.get("endDateIso", ""))
        try:
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            remaining = max(0.0, (end_dt - now).total_seconds() / 3600)
        except (ValueError, TypeError):
            remaining = 0.0

        tick_size = float(m.get("orderPriceMinTickSize", 0.01))

        mi = MarketInfo(
            market_id=m["id"],
            question=m["question"],
            slug=m["slug"],
            outcomes=outcomes,
            gamma_prices=gamma_prices,
            tokens=[
                TokenInfo(token_id=tids[0], side="yes", index=0, clob_bid=yes_bid, clob_ask=yes_ask),
                TokenInfo(token_id=tids[1], side="no", index=1, clob_bid=no_bid, clob_ask=no_ask),
            ],
            end_date=end_str,
            remaining_hours=round(remaining, 1),
            volume=float(m.get("volume", 0)),
            liquidity=float(m.get("liquidity", 0)),
            tick_size=tick_size,
            neg_risk=m.get("negRisk", False),
        )

        # 按 Event 分组
        events = m.get("events", [])
        ev = events[0] if events else {}
        eid = ev.get("id", m["id"])

        if eid not in event_map:
            event_map[eid] = EventInfo(
                event_id=eid,
                title=ev.get("title", m["question"]),
                slug=ev.get("slug", m["slug"]),
                end_date=ev.get("endDate", end_str),
                neg_risk=ev.get("negRisk", False),
                markets=[],
            )
        event_map[eid].markets.append(mi)

    # 计算 Event 总交易量
    for ev in event_map.values():
        ev.total_volume = sum(m.volume for m in ev.markets)

    return sorted(event_map.values(), key=lambda e: e.end_date)


# ═══════════════════════════════════════════════════════════════════════════
#  筛选逻辑
# ═══════════════════════════════════════════════════════════════════════════

def filter_events(events: list[EventInfo]) -> list[EventInfo]:
    """Event 预筛: 任一 Market 的 Yes bid >= EVENT_MIN_YES_BID"""
    passed = []
    for ev in events:
        for m in ev.markets:
            yes_bid = m.tokens[0].clob_bid
            if yes_bid >= EVENT_MIN_YES_BID:
                passed.append(ev)
                break
    return passed


def find_hits(event: EventInfo, ordered_ids: set[str]) -> list[MarketHit]:
    """在 Event 中找 MARKET_BID_MIN ≤ bid ≤ MARKET_BID_MAX 的 Market（排除已下单的）"""
    hits = []
    for m in event.markets:
        if m.market_id in ordered_ids:
            continue  # 已下单，跳过

        for token in m.tokens:
            if MARKET_BID_MIN <= token.clob_bid <= MARKET_BID_MAX:
                hits.append(MarketHit(
                    market_id=m.market_id,
                    question=m.question,
                    event_id=event.event_id,
                    event_title=event.title,
                    side=token.side,
                    token_id=token.token_id,
                    bid=token.clob_bid,
                    ask=token.clob_ask,
                    tick_size=m.tick_size,
                    end_date=m.end_date,
                    volume=m.volume,
                ))
    return hits


# ═══════════════════════════════════════════════════════════════════════════
#  状态持久化
# ═══════════════════════════════════════════════════════════════════════════

def load_watched_events() -> dict:
    """加载观察列表"""
    if not os.path.exists(WATCHED_FILE):
        return {}
    try:
        with open(WATCHED_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("events", {})
    except Exception:
        return {}


def save_watched_events(watched: dict):
    """保存观察列表"""
    data = {
        "events": watched,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total": len(watched),
    }
    with open(WATCHED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_ordered() -> dict:
    """加载已下单记录"""
    if not os.path.exists(ORDERED_FILE):
        return {}
    try:
        with open(ORDERED_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("orders", {})
    except Exception:
        return {}


def save_ordered(ordered: dict):
    """保存已下单记录"""
    data = {
        "orders": ordered,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total": len(ordered),
    }
    with open(ORDERED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════════════════
#  输出格式化
# ═══════════════════════════════════════════════════════════════════════════

def print_summary(events: list[EventInfo], candidates: list[EventInfo],
                  all_hits: list[MarketHit], watched: dict, ordered: dict,
                  elapsed: float, live_mode: bool = False):
    """打印本轮扫描摘要"""
    print()
    print("=" * 75)
    print(f"  扫描完成  |  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}  |  {elapsed:.1f}s")
    print("=" * 75)
    print(f"  总 Market: {sum(len(e.markets) for e in events)}  |  "
          f"总 Event: {len(events)}  |  "
          f"通过预筛: {len(candidates)}")
    print(f"  观察中 Event: {len(watched)}  |  "
          f"已下单 Market: {len(ordered)}  |  "
          f"本轮命中: {len(all_hits)}")
    print()

    if not all_hits:
        print("  本轮无新增命中 Market。")
        print("-" * 75)
        print(f"  [STDOUT] 本轮 {len(ordered)} 个已下单 | {len(watched)} 个观察中 Event")
        return

    taker_count = sum(1 for h in all_hits if MARKET_BID_MIN <= h.ask <= MARKET_BID_MAX)
    print(f"  吃单 (ask): {taker_count}  |  挂单 (bid): {len(all_hits) - taker_count}")
    print()
    print(f"  {'─' * 78}")
    print(f"  {'Market ID':<12} {'Side':<6} {'Bid':<8} {'Ask':<8} {'Vol':<10} {'Question'}")
    print(f"  {'─' * 78}")
    for h in all_hits:
        use_ask = MARKET_BID_MIN <= h.ask <= MARKET_BID_MAX
        marker = "*" if use_ask else " "
        print(f"  {h.market_id:<12} {h.side:<6} {h.bid:<8.4f} {h.ask:<8.4f} "
              f"${h.volume:>8,.0f}  {marker}{h.question[:43]}")
    print(f"  {'─' * 78}")
    if taker_count:
        print(f"  * = ask 价吃单（即时成交）")
    print(f"  [STDOUT] 本轮 {len(ordered)} 个已下单 | {len(watched)} 个观察中 Event | "
          f"{'LIVE' if live_mode else 'DRY-RUN'}")


# ═══════════════════════════════════════════════════════════════════════════
#  主循环
# ═══════════════════════════════════════════════════════════════════════════

def cleanup_expired_events(watched: dict, lookback_hours: int):
    """移除 end_date 已过期超过 lookback_hours 的 Event"""
    now = datetime.now(timezone.utc)
    expired = []
    for eid, info in watched.items():
        try:
            end_dt = datetime.fromisoformat(
                info["end_date"].replace("Z", "+00:00")
            )
            if (now - end_dt).total_seconds() > lookback_hours * 3600:
                expired.append(eid)
        except (ValueError, TypeError):
            pass
    for eid in expired:
        del watched[eid]
    if expired:
        print(f"  清理 {len(expired)} 个过期 Event", file=sys.stderr)


def run_cycle(ordered: dict, watched: dict, client: ClobClient,
              live_mode: bool = False) -> tuple[dict, dict]:
    """执行一次完整扫描周期，返回更新后的 (ordered, watched)"""
    t0 = time.time()

    # ── Step 1: Gamma 分页拉取 ──
    print("[1/4] Gamma API: 分页拉取 Weather 48h 市场...", file=sys.stderr, flush=True)
    markets_raw = fetch_all_markets(END_HOURS_AHEAD, END_LOOKBACK)
    t1 = time.time()
    print(f"      -> {len(markets_raw)} 个 Market, {t1 - t0:.1f}s", file=sys.stderr, flush=True)

    # 过滤排除地区
    if EXCLUDED_SLUGS:
        before = len(markets_raw)
        markets_raw = [m for m in markets_raw
                       if not any(kw in m.get("slug", "").lower() for kw in EXCLUDED_SLUGS)]
        if before != len(markets_raw):
            print(f"      排除 {before - len(markets_raw)} 个排除地区 market", file=sys.stderr, flush=True)

    if not markets_raw:
        return ordered, watched

    # ── Step 2: 收集所有 token + 查 CLOB ──
    all_tids: list[str] = []
    for m in markets_raw:
        tids = json.loads(m.get("clobTokenIds", "[]"))
        all_tids.extend(tids)

    print(f"[2/4] CLOB get_prices: {len(all_tids)} tokens...", file=sys.stderr, flush=True)
    prices = fetch_prices_sdk(client, list(set(all_tids)))
    t2 = time.time()
    print(f"      -> {len(prices)} prices, {t2 - t1:.1f}s", file=sys.stderr, flush=True)

    # ── Step 3: 组装 + Event 预筛 ──
    print(f"[3/4] 组装 Event + 预筛 (Yes bid ≥ {EVENT_MIN_YES_BID})...", file=sys.stderr, flush=True)
    events = build_events(markets_raw, prices)
    candidates = filter_events(events)
    t3 = time.time()
    print(f"      -> {len(events)} Events, {len(candidates)} 通过预筛, {t3 - t2:.1f}s", file=sys.stderr, flush=True)

    # ── Step 4: 更新观察列表 ──
    new_watched = 0
    for ev in candidates:
        if ev.event_id not in watched:
            qualifying_mkt = next(
                (m for m in ev.markets if m.tokens[0].clob_bid >= EVENT_MIN_YES_BID),
                ev.markets[0],
            )
            watched[ev.event_id] = {
                "title": ev.title,
                "end_date": ev.end_date,
                "first_seen": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "qualifying_market": qualifying_mkt.market_id,
                "qualifying_bid": qualifying_mkt.tokens[0].clob_bid,
                "total_markets": len(ev.markets),
            }
            new_watched += 1
        else:
            # 更新已有记录
            watched[ev.event_id]["last_checked"] = (
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            )

    if new_watched:
        print(f"      -> 新增 {new_watched} 个 Event 到观察列表", file=sys.stderr)

    # ── Step 5: Market 精选（对所有 candidates） ──
    ordered_ids = set(ordered.keys())
    all_hits: list[MarketHit] = []
    for ev in candidates:
        hits = find_hits(ev, ordered_ids)
        all_hits.extend(hits)

    t4 = time.time()
    print(f"      -> {len(all_hits)} 个 Market 命中 [{MARKET_BID_MIN}, {MARKET_BID_MAX}], {t4 - t3:.1f}s", file=sys.stderr, flush=True)

    # ── Step 5.5: 对命中的 token 查 SELL（ask） ──
    if all_hits:
        hit_tids = list({h.token_id for h in all_hits})
        sell_prices = fetch_prices_sdk(client, hit_tids, side="SELL")
        for h in all_hits:
            h.ask = sell_prices.get(h.token_id, 0.0)
        print(f"      -> {len(sell_prices)} SELL prices for hit tokens, {time.time() - t4:.1f}s",
              file=sys.stderr, flush=True)

    # ── Step 6: 下单 ──
    new_orders = 0
    if live_mode and all_hits:
        print(f"[5/5] 下单: {len(all_hits)} 个 Market ({ORDER_WORKERS} workers)...",
              file=sys.stderr, flush=True)
        t_order = time.time()

        def _do_order(hit: MarketHit) -> tuple[MarketHit, str | None, float]:
            # 优先吃单: ask 也在范围内直接挂 ask 价成交，否则挂 bid 排队
            if MARKET_BID_MIN <= hit.ask <= MARKET_BID_MAX:
                price = hit.ask
            else:
                price = hit.bid
            oid = place_order(client, hit.token_id, price, ORDER_SIZE, hit.tick_size)
            return hit, oid, price

        with ThreadPoolExecutor(max_workers=ORDER_WORKERS) as executor:
            futures = {executor.submit(_do_order, h): h for h in all_hits}
            for f in as_completed(futures):
                hit, oid, price = f.result()
                if oid:
                    ordered[hit.market_id] = {
                        "market_id": hit.market_id,
                        "question": hit.question,
                        "event_id": hit.event_id,
                        "event_title": hit.event_title,
                        "side": hit.side,
                        "token_id": hit.token_id,
                        "price": price,
                        "bid_at_time": hit.bid,
                        "ask_at_time": hit.ask,
                        "size": ORDER_SIZE,
                        "order_id": oid,
                        "time": datetime.now(timezone.utc).strftime(
                            "%Y-%m-%dT%H:%M:%SZ"),
                    }
                    new_orders += 1

        save_ordered(ordered)
        print(f"      下单完成: {new_orders}/{len(all_hits)} 成功, "
              f"{time.time() - t_order:.1f}s", file=sys.stderr, flush=True)
    elif all_hits:
        print(f"[5/5] [DRY-RUN] 命中 {len(all_hits)} 个 Market（将 LIVE_MODE=True 启用下单）",
              file=sys.stderr, flush=True)

    # ── Step 7: 保存状态 ──
    save_watched_events(watched)

    elapsed = time.time() - t0
    print_summary(events, candidates, all_hits, watched, ordered, elapsed, live_mode)
    sys.stdout.flush()

    return ordered, watched


def main():
    # 命令行可选覆盖配置
    parser = argparse.ArgumentParser(description="Weather Neg Risk 高确定性下单框架")
    parser.add_argument("--live", action="store_true", default=None,
                        help="启用实盘下单（覆盖 LIVE_MODE）")
    parser.add_argument("--dry-run", action="store_true", default=None,
                        help="仅扫描不下单（覆盖 LIVE_MODE）")
    parser.add_argument("--interval", "-i", type=int, default=None,
                        help="循环间隔（分钟），覆盖 INTERVAL_MINUTES")
    parser.add_argument("--quiet", action="store_true", default=None,
                        help="静默模式")
    args = parser.parse_args()

    # 合并配置
    live_mode = LIVE_MODE
    if args.live:
        live_mode = True
    elif args.dry_run:
        live_mode = False
    interval = args.interval if args.interval is not None else INTERVAL_MINUTES
    quiet = args.quiet if args.quiet is not None else QUIET

    # UTF-8 输出
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    if quiet:
        sys.stderr = open(os.devnull, "w")
    else:
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

    print("=" * 70, file=sys.stderr)
    print(f" Weather Neg Risk Guard", file=sys.stderr)
    print(f" 时间窗口: endDate 距现在 ≤ {END_HOURS_AHEAD}h（回溯 {END_LOOKBACK}h）", file=sys.stderr)
    print(f" Event预筛: Yes bid ≥ {EVENT_MIN_YES_BID}  |  "
          f"Market精选: [{MARKET_BID_MIN}, {MARKET_BID_MAX}]", file=sys.stderr)
    print(f" 下单: {'启用 (LIVE)' if live_mode else '禁用 (DRY-RUN)'}  |  "
          f"份数: {ORDER_SIZE} USDC  |  "
          f"循环: {'每 ' + str(interval) + ' 分钟' if interval > 0 else '单次'}", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    # 加载状态 & 初始化客户端
    watched = load_watched_events()
    ordered = load_ordered()
    cleanup_expired_events(watched, END_LOOKBACK)

    env = load_env(ENV_FILE)
    try:
        client = make_clob_client(env)
    except RuntimeError as e:
        print(f"[FATAL] CLOB 客户端初始化失败: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"状态文件: {len(watched)} 个观察 Event, {len(ordered)} 个已下单 Market\n",
          file=sys.stderr)

    iteration = 0
    while True:
        iteration += 1
        if interval > 0:
            print(f"\n{'─' * 60}", file=sys.stderr)
            print(f" 第 {iteration} 轮  |  "
                  f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
                  file=sys.stderr)
            print(f"{'─' * 60}", file=sys.stderr)

        try:
            ordered, watched = run_cycle(ordered, watched, client, live_mode)
            cleanup_expired_events(watched, END_LOOKBACK)
        except Exception as e:
            print(f"\n[ERROR] 本轮异常: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)

        if interval <= 0:
            break

        print(f"\n  等待 {interval} 分钟后下一轮...", file=sys.stderr)
        time.sleep(interval * 60)


if __name__ == "__main__":
    main()
