"""
Neg Risk 套利监控 — 轮询版（替代 WSS 版）

架构:
  单线程主循环 + ThreadPoolExecutor 并发批量请求
  - 快循环: 每 POLL_INTERVAL 秒, POST /books 批量拉取订单簿 → 检测套利
  - 慢循环: 每 EVENT_REFRESH_INTERVAL 秒, 从 Gamma API 刷新事件列表（新增/结束自动同步）

对比 WSS 版的优势:
  - 无撤单噪声: 每轮拿到的是真实 resting 在 orderbook 上的单子
  - 无多线程共享状态: events/asset_map 仅在主线程修改, 无锁
  - 无 WSS 重连复杂性
  - 事件列表自动跟踪市场变化（新增/结束）

用法:
  python monitor_poll.py
  python monitor_poll.py --tag 103040          # 只监控每日温度
  python monitor_poll.py --poll 3              # 3s 轮询
  python monitor_poll.py --refresh 600         # 10min 刷新事件
"""
import sys, json, time, os, csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from collections import OrderedDict
import requests

# Windows 控制台默认 GBK, 强制 UTF-8 避免中文/特殊字符乱码
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

BASE = os.path.dirname(__file__)
DATA = os.path.join(BASE, "data")
GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"

# ============================================================
# 可调参数
# ============================================================

POLL_INTERVAL            = 5       # 订单簿轮询间隔（秒）
EVENT_REFRESH_INTERVAL   = 30000     # 事件列表刷新间隔（秒）= 5 min
BATCH_SIZE               = 500     # POST /books 单次 token 上限
MAX_WORKERS              = 15      # 并发批量请求数（/books 限 500 req/10s，15 并发安全）

SCAN_LIMIT               = 5000    # 事件扫描上限
PAGE_SIZE                = 100     # Gamma API 分页
GAMMA_RATE               = 0.3     # Gamma API 请求间隔

MIN_VOLUME               = 0       # event 最小成交量过滤（USD）
TAG_ID                   = "103040"   # 可选 tag 过滤（如 "103040" = 每日温度）

CLOB_FEE_RATE            = 0.05    # CLOB taker 费率（天气=0.05）
FEE_BIPS                 = 0       # NegRiskAdapter convertPositions 手续费 bps
MIN_PROFIT               = 0.0001   # 最小 USDC 利润才报告

# ============================================================
# 自动下单参数
# ============================================================
MAX_AMOUNT               = 8       # 每次执行最大份额（用户: 每份 ≤ 8 USDC）
PUSD_DECIMALS            = 6       # pUSD 小数位（与 USDC 一致）
DRY_RUN                  = True    # True=只打印执行计划不下单; --live 切实盘
NEG_RISK_CTF_COLLATERAL_ADAPTER = "0xadA2005600Dec949baf300f4C6120000bDB6eAab"

# CLOB 下单参数 (P1)
ORDER_FILL_TIMEOUT       = 10      # 等待成交超时（秒）
ORDER_POLL_INTERVAL      = 0.5     # 轮询订单状态间隔（秒）
# 智能路径检测: deploy/ 同目录有 .env / py_clob_client_v2 时用同目录, 否则用 download-new/ 相对路径
ENV_FILE = os.path.join(BASE, ".env") if os.path.isfile(os.path.join(BASE, ".env")) \
           else os.path.join(BASE, "test", ".env")
SDK_PATH = BASE if os.path.isdir(os.path.join(BASE, "py_clob_client_v2")) \
           else os.path.join(BASE, "..", "..", "mcp-lab", "deploy")

# convertPositions 合约 ABI (v2 NegRiskCtfCollateralAdapter, 签名与 v1 一致)
CONVERT_ABI = [{
    "name": "convertPositions",
    "type": "function",
    "stateMutability": "nonpayable",
    "inputs": [
        {"name": "_marketId", "type": "bytes32"},
        {"name": "_indexSet", "type": "uint256"},
        {"name": "_amount",   "type": "uint256"},
    ],
    "outputs": [],
}]

EVENTS_CSV    = os.path.join(DATA, "neg_risk_events.csv")
OPPS_CSV      = os.path.join(DATA, "arbitrage_opportunities.csv")
CSV_COLS      = ["timestamp", "event_slug", "event_title", "K", "raw_sum", "fee_sum",
                 "total_cost", "threshold", "max_amount", "usdc_profit",
                 "selected_outcomes", "prices", "yes_tokens"]

# 常用 tag_id（数据来源: Gamma API /events?active=true&negRisk=true，2026-07-15 扫描 920 事件）
# 顶层分类
TAG_POLITICS   = "2"       # Politics (675 事件)
TAG_SPORTS     = "1"       # Sports (156 事件)
TAG_ELECTIONS  = "144"     # Elections (655 事件)
TAG_ECONOMY    = "100328"  # Economy (38 事件)
TAG_GAMES      = "100639"  # Games (48 事件)
TAG_WORLD      = "101970"  # World (33 事件)

# 选举子类
TAG_US_ELECTION       = "1101"    # US Election (600)
TAG_MIDTERMS          = "102289"  # Midterms (521)
TAG_HOUSE_ELECTIONS   = "103899"  # House Elections (433)
TAG_NOV4_ELECTIONS    = "102786"  # Nov 4 Elections (432)
TAG_PRIMARIES         = "264"     # Primaries (76)
TAG_GLOBAL_ELECTIONS  = "1597"    # Global Elections (61)
TAG_MAIN_ELECTION     = "104743"  # Main Election (41)
TAG_SENATE_MIDTERMS   = "104093"  # Senate midterms (35)
TAG_GOVERNOR_MIDTERMS = "104094"  # Governor midterms (36)

# 州级 Midterm
TAG_CALIFORNIA_MIDTERM = "104045"  # (52)
TAG_TEXAS_MIDTERM      = "104040"  # (40)
TAG_FLORIDA_MIDTERM    = "104015"  # (30)
TAG_NY_MIDTERM         = "104051"  # (27)

# 体育子类
TAG_SOCCER  = "100350"  # Soccer (50)
TAG_MLB     = "100381"  # MLB (37)
TAG_BASEBALL = "678"    # baseball (36)
TAG_CRICKET = "517"     # Cricket (25)

# 天气 / 气候 / 科学
TAG_WEATHER             = "84"      # Weather (5)
TAG_CLIMATE_SCIENCE     = "103037"  # Climate & Science (6)
TAG_DAILY_TEMPERATURE   = "103040"  # Daily Temperature（季节性，当前无活跃事件）
TAG_HIGHEST_TEMPERATURE = "104596"  # Highest Temperature（季节性）
TAG_GLOBAL_TEMP         = "832"     # Global Temp (1)

# 加密货币
TAG_CRYPTO         = "21"    # Crypto (3)
TAG_CRYPTO_PRICES  = "1312"  # Crypto Prices (2)
TAG_BITCOIN        = "235"   # Bitcoin (2)


# ============================================================
# Step 1: 事件发现（合并自 fetch_neg_events.py）
# ============================================================

def fetch_neg_risk_events(tag_id=None, min_volume=0):
    """从 Gamma API 拉取活跃 Neg Risk 事件"""
    events = []
    offset = 0
    while offset < SCAN_LIMIT:
        params = {"active": "true", "closed": "false",
                  "limit": PAGE_SIZE, "offset": offset}
        if tag_id:
            params["tag_id"] = tag_id
        try:
            r = requests.get(f"{GAMMA}/events", params=params, timeout=15)
        except Exception as e:
            print(f"  [events] 请求失败 offset={offset}: {e}")
            break
        if r.status_code != 200:
            break
        batch = r.json()
        if not batch:
            break
        # ★ 无论是否传 tag_id, 都必须过滤 negRisk=True
        # (之前传 tag_id 时跳过过滤, 导致非 Neg Risk 事件混入, 签名用错交易所合约)
        events.extend(e for e in batch if e.get("negRisk"))
        offset += PAGE_SIZE
        if len(batch) < PAGE_SIZE:
            break
        time.sleep(GAMMA_RATE)

    if min_volume > 0:
        events = [e for e in events if float(e.get("volume", 0)) >= min_volume]
    return events


def save_events_csv(events):
    """保存事件列表到 CSV（可观测性）"""
    os.makedirs(DATA, exist_ok=True)
    cols = ["eventId", "slug", "negRisk", "subMarkets", "title", "tags", "volume"]
    with open(EVENTS_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for e in events:
            tags = ", ".join(t["label"] for t in e.get("tags", []))
            w.writerow([
                e.get("id", ""),
                e.get("slug", ""),
                e.get("negRisk", False),
                len(e.get("markets", [])),
                e.get("title", "")[:80],
                tags,
                e.get("volume", 0),
            ])


# ============================================================
# Step 2: 子市场结构构建
# ============================================================

def fetch_event_markets(slug):
    """查单个 event 的子市场结构（带 2 次重试）"""
    for attempt in range(3):
        try:
            r = requests.get(f"{GAMMA}/events", params={"slug": slug}, timeout=15)
            if r.status_code == 200 and r.json():
                return r.json()[0]
        except Exception as e:
            if attempt == 2:
                print(f"  [markets] {slug} 查询失败: {e}")
            time.sleep(1)
    return None


def build_event_entry(e):
    """从 Gamma event 构建监控用 entry, 返回 (entry, [(no_token_id, slug, outcome), ...])"""
    slug = e["slug"]
    markets = []
    new_tokens = []
    for m in e.get("markets", []):
        # 跳过非 Neg Risk 的子市场 (即使父事件是 Neg Risk, 也可能有独立子市场)
        if not m.get("negRisk"):
            continue
        cids_raw = m.get("clobTokenIds", "[]")
        cids = json.loads(cids_raw) if isinstance(cids_raw, str) else cids_raw
        if len(cids) < 2:
            continue
        yes_id, no_id = cids[0], cids[1]
        outcome_label = m.get("groupItemTitle", m.get("question", "?"))[:50]
        # Gamma API 返回 orderMinSize (服务端最小下单量, 不同子市场可能不同)
        try:
            min_order_size = float(m.get("orderMinSize") or m.get("minimum_order_size") or 0)
        except (TypeError, ValueError):
            min_order_size = 0.0
        markets.append({
            "question":            m.get("question", ""),
            "conditionId":         m.get("conditionId", ""),
            "outcome":             outcome_label,
            "yes_token_id":        yes_id,
            "no_token_id":         no_id,
            "best_ask":            None,
            "ask_size":            0.0,
            "minimum_order_size":  min_order_size,
        })
        new_tokens.append((no_id, slug, outcome_label))

    entry = {
        "slug":            slug,
        "eventId":         e.get("id"),
        "title":           e.get("title", ""),
        "negRiskMarketID": e.get("negRiskMarketID", ""),
        "markets":         markets,
    }
    return entry, new_tokens


def add_event(e, events, asset_map, token_list):
    """添加单个 event 到监控状态"""
    entry, new_tokens = build_event_entry(e)
    if not entry["markets"]:
        return
    events[e["slug"]] = entry
    for no_id, slug, outcome in new_tokens:
        asset_map[no_id] = (slug, outcome)
        token_list.append(no_id)


def remove_event(slug, events, asset_map, token_list):
    """移除单个 event 及其 token"""
    entry = events.pop(slug, None)
    if not entry:
        return
    remove_ids = {m["no_token_id"] for m in entry["markets"]}
    for tid in remove_ids:
        asset_map.pop(tid, None)
    token_list[:] = [t for t in token_list if t not in remove_ids]


# ============================================================
# Step 3a: 快层 — POST /prices 批量获取 best ask (轻量, 无 ask_size)
# ============================================================

def fetch_prices_batch(token_ids):
    """POST /prices side=SELL, 返回 {asset_id: best_ask}"""
    if not token_ids:
        return {}
    body = [{"token_id": tid, "side": "SELL"} for tid in token_ids]
    try:
        r = requests.post(f"{CLOB}/prices", json=body, timeout=20)
        if r.status_code != 200:
            print(f"  [prices] HTTP {r.status_code}: {r.text[:200]}")
            return {}
        data = r.json()
        result = {}
        for tid, val in data.items():
            if isinstance(val, dict) and val.get("SELL"):
                result[tid] = float(val["SELL"])
        return result
    except Exception as e:
        print(f"  [prices] 请求异常 ({len(token_ids)} token): {e}")
        return {}


def poll_all_prices(token_list):
    """并发分块拉取全部 token 的 best ask, 返回 {asset_id: best_ask}"""
    if not token_list:
        return {}
    chunks = [token_list[i:i + BATCH_SIZE]
              for i in range(0, len(token_list), BATCH_SIZE)]
    if len(chunks) == 1:
        return fetch_prices_batch(chunks[0])
    merged = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_prices_batch, chunk): chunk for chunk in chunks}
        for f in as_completed(futures):
            merged.update(f.result())
    return merged


def apply_price_updates(prices, events, asset_map):
    """快层: 只更新 best_ask, 不碰 ask_size"""
    for asset_id, best_ask in prices.items():
        slug, _ = asset_map.get(asset_id, (None, None))
        if not slug or slug not in events:
            continue
        for m in events[slug]["markets"]:
            if m["no_token_id"] == asset_id:
                m["best_ask"] = best_ask
                break


# ============================================================
# Step 3b: 深层 — POST /books 批量获取订单簿 (含 ask_size, 按需调用)
# ============================================================

def fetch_books_batch(token_ids):
    """单次 POST /books 请求, 返回 {asset_id: (best_ask, ask_size)}"""
    if not token_ids:
        return {}
    body = [{"token_id": tid} for tid in token_ids]
    try:
        r = requests.post(f"{CLOB}/books", json=body, timeout=20)
        if r.status_code != 200:
            print(f"  [books] HTTP {r.status_code}: {r.text[:200]}")
            return {}
        data = r.json()
        items = data.get("items", []) if isinstance(data, dict) else data
        result = {}
        for item in items:
            asset_id = item.get("asset_id", "")
            asks = item.get("asks", [])
            best = min(asks, key=lambda x: float(x["price"])) if asks else None
            best_ask = float(best["price"]) if best else None
            ask_size = float(best["size"]) if best else 0.0
            result[asset_id] = (best_ask, ask_size)
        return result
    except Exception as e:
        print(f"  [books] 请求异常 ({len(token_ids)} token): {e}")
        return {}


def poll_all_orderbooks(token_list):
    """并发分块拉取全部 token 的订单簿, 返回 {asset_id: (best_ask, ask_size)}"""
    if not token_list:
        return {}
    chunks = [token_list[i:i + BATCH_SIZE]
              for i in range(0, len(token_list), BATCH_SIZE)]
    if len(chunks) == 1:
        return fetch_books_batch(chunks[0])

    merged = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_books_batch, chunk): chunk for chunk in chunks}
        for f in as_completed(futures):
            merged.update(f.result())
    return merged


def apply_orderbook_updates(updates, events, asset_map):
    """主线程: 用轮询结果更新 events 状态"""
    for asset_id, (best_ask, ask_size) in updates.items():
        slug, _ = asset_map.get(asset_id, (None, None))
        if not slug or slug not in events:
            continue
        for m in events[slug]["markets"]:
            if m["no_token_id"] == asset_id:
                m["best_ask"] = best_ask
                m["ask_size"] = ask_size
                break


# ============================================================
# Step 4: 套利检测（与 WSS 版逻辑一致）
# ============================================================

def has_arbitrage_potential(event):
    """快层轻量检测: 只用 best_ask 价格 (不含 ask_size), 判断是否有套利可能"""
    convert_discount = 1 - FEE_BIPS / 10000
    costs = []
    for m in event["markets"]:
        p = m["best_ask"]
        if p is not None and p > 0:
            costs.append(p + CLOB_FEE_RATE * p * (1 - p))
    if len(costs) < 2:
        return False
    costs.sort()
    max_k = min(len(costs), len(event["markets"]) - 1)
    for K in range(2, max_k + 1):
        if sum(costs[:K]) < (K - 1) * convert_discount:
            return True
    return False


def check_arbitrage(event):
    """检测套利, 含 CLOB taker fee + convert fee (需要 best_ask + ask_size)"""
    convert_discount = 1 - FEE_BIPS / 10000

    offers = []
    for m in event["markets"]:
        if m["best_ask"] is not None and m["ask_size"] > 0:
            p = m["best_ask"]
            taker_fee = CLOB_FEE_RATE * p * (1 - p)
            offers.append({
                "price":     p,
                "taker_fee": taker_fee,
                "cost":      p + taker_fee,
                "size":      m["ask_size"],
                "outcome":   m["outcome"],
            })

    if len(offers) < 2:
        return None

    offers.sort(key=lambda x: x["cost"])

    # K 范围: 1 到 min(len(offers), N-1), 合约约束 K ≤ N-1
    max_k = min(len(offers), len(event["markets"]) - 1)
    best = None
    for K in range(1, max_k + 1):
        selected   = offers[:K]
        raw_sum    = sum(o["price"] for o in selected)
        fee_sum    = sum(o["taker_fee"] for o in selected)
        total_cost = sum(o["cost"] for o in selected)
        max_amt    = min(o["size"] for o in selected)
        threshold  = (K - 1) * convert_discount

        if total_cost < threshold:
            usdc_profit = (threshold - total_cost) * max_amt
            if usdc_profit < MIN_PROFIT:
                continue
            if best is None or usdc_profit > best["usdc_profit"]:
                best = {
                    "K":           K,
                    "selected":    [s["outcome"] for s in selected],
                    "prices":      [round(s["price"], 4) for s in selected],
                    "fees":        [round(s["taker_fee"], 6) for s in selected],
                    "cost":        round(total_cost, 6),
                    "raw_sum":     round(raw_sum, 6),
                    "fee_sum":     round(fee_sum, 6),
                    "threshold":   round(threshold, 6),
                    "max_amount":  round(max_amt, 2),
                    "usdc_profit": round(usdc_profit, 4),
                    "yes_tokens":  [{"outcome": o["outcome"], "price": round(o["price"], 4)}
                                    for o in offers[K:]],
                }
    return best


# ============================================================
# Step 5: 去重 + 输出
# ============================================================

_last_printed = {}      # slug → {"key": json, "time": monotonic}
_csv_header_written = False


def save_opportunity(slug, event, result):
    global _csv_header_written
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = {
        "timestamp":          ts,
        "event_slug":         slug,
        "event_title":        event["title"],
        "K":                  result["K"],
        "raw_sum":            result["raw_sum"],
        "fee_sum":            result["fee_sum"],
        "total_cost":         result["cost"],
        "threshold":          result["threshold"],
        "max_amount":         result["max_amount"],
        "usdc_profit":        result["usdc_profit"],
        "selected_outcomes":  " | ".join(result["selected"]),
        "prices":             " | ".join(str(p) for p in result["prices"]),
        "yes_tokens":         " | ".join(y["outcome"] for y in result["yes_tokens"]) if result["yes_tokens"] else "",
    }
    os.makedirs(DATA, exist_ok=True)
    write_header = not _csv_header_written and not os.path.exists(OPPS_CSV)
    with open(OPPS_CSV, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS)
        if write_header:
            w.writeheader()
            _csv_header_written = True
        w.writerow(row)


def print_opportunity(slug, event, result):
    ts = datetime.now().strftime("%H:%M:%S")
    prices_str = " + ".join(f"{o}={p}" for o, p in zip(result["selected"], result["prices"]))
    print(f"\n{'='*60}")
    print(f"[{ts}] 套利机会: {event['title'][:60]}")
    print(f"  K={result['K']}, sum(ask)={result['raw_sum']} + fee={result['fee_sum']}"
          f" = total={result['cost']} < {result['threshold']} (K-1)")
    print(f"  买入 NO: {prices_str}")
    print(f"  max_amount={result['max_amount']}, USDC 利润={result['usdc_profit']}")
    if result["yes_tokens"]:
        print(f"  白送 YES: {[y['outcome'] for y in result['yes_tokens']]}")
    print(f"{'='*60}\n")


def check_and_print(slug, event, clob_client=None, relay_client=None):
    """检测 + 去重 + 保存 + 打印 + (可选) 实盘执行（单线程, 无锁）"""
    result = check_arbitrage(event)
    result_key = json.dumps(result, sort_keys=True) if result else None

    last = _last_printed.get(slug)
    if last and last["key"] == result_key:
        return
    _last_printed[slug] = {"key": result_key, "time": time.monotonic()}

    if not result:
        return
    save_opportunity(slug, event, result)
    print_opportunity(slug, event, result)

    plan = compute_execution_plan(event, result)
    if not plan:
        return

    if DRY_RUN:
        print_execution_plan(slug, event, plan)
    elif clob_client is not None:
        if slug in _executed_slugs:
            print(f"  [跳过] {slug} 已执行过, 避免重复")
            return
        _executed_slugs.add(slug)
        execute_arbitrage(slug, event, plan, clob_client, relay_client)


# ============================================================
# Step 5b: 执行计划（dry-run 用）
# ============================================================

def compute_index_set(event, selected_outcomes):
    """构建 convertPositions 的 indexSet (位掩码)
    bit i = 1 表示选中 event["markets"][i] 的 NO (按 Gamma API markets[] 顺序)
    """
    selected = set(selected_outcomes)
    index_set = 0
    for idx, m in enumerate(event["markets"]):
        if m["outcome"] in selected:
            index_set |= (1 << idx)
    return index_set


def compute_execution_plan(event, result):
    """根据检测结果计算实际执行计划 (amount, cost, indexSet, convert 参数)"""
    selected_outcomes = result["selected"]

    # 实际下单量: 受 orderbook ask_size 和用户上限双重约束
    max_from_orderbook = result["max_amount"]
    actual_amount = min(max_from_orderbook, MAX_AMOUNT)
    if actual_amount <= 0:
        return None

    # ★ 服务端最小下单量约束: 不同子市场 orderMinSize 可能不同 (如 5), 取 max
    # 若 max_min > actual_amount, 尝试提到 max_min (不超过 MAX_AMOUNT 和 bottleneck ask_size)
    # 若 max_min > MAX_AMOUNT 或 max_min > max_from_orderbook → 无法满足, 跳过该 plan
    market_min_map = {m["outcome"]: m.get("minimum_order_size", 0.0)
                      for m in event["markets"] if m["outcome"] in selected_outcomes}
    max_min = max(market_min_map.values()) if market_min_map else 0.0
    if max_min > actual_amount:
        if max_min <= MAX_AMOUNT and max_min <= max_from_orderbook:
            actual_amount = max_min
        else:
            short = event.get("slug", "?")[:30]
            print(f"  [skip] {short}: orderMinSize={max_min:.2f} > amount={actual_amount:.2f} "
                  f"(bottleneck ask_size={max_from_orderbook:.2f}, MAX_AMOUNT={MAX_AMOUNT})")
            return None

    # 资金计算 (amount 份的总成本/收入)
    cost_per_share   = result["cost"]        # sum(price + fee) for K tokens
    revenue_per_share = result["threshold"]   # (K-1) * (1 - feeBips/10000)
    total_cost       = cost_per_share * actual_amount
    total_revenue    = revenue_per_share * actual_amount
    net_profit       = total_revenue - total_cost

    # indexSet (位掩码)
    index_set = compute_index_set(event, selected_outcomes)

    # convertPositions 调用参数
    market_id  = event.get("negRiskMarketID", "")
    amount_raw = int(round(actual_amount * 10**PUSD_DECIMALS))

    # 选中 token 的明细 (按成本升序, 与 result["selected"] 一致)
    price_map = dict(zip(result["selected"], result["prices"]))
    fee_map   = dict(zip(result["selected"], result["fees"]))
    size_map  = {}
    for m in event["markets"]:
        if m["outcome"] in price_map:
            size_map[m["outcome"]] = m.get("ask_size", 0.0)

    selected_details = []
    for outcome in selected_outcomes:
        selected_details.append({
            "outcome": outcome,
            "price":   price_map.get(outcome, 0),
            "fee":     fee_map.get(outcome, 0),
            "cost":    price_map.get(outcome, 0) + fee_map.get(outcome, 0),
            "size":    size_map.get(outcome, 0),
            "minimum_order_size": market_min_map.get(outcome, 0.0),
            "token_id": next((m["no_token_id"] for m in event["markets"]
                              if m["outcome"] == outcome), ""),
        })

    return {
        "K":                 result["K"],
        "actual_amount":     actual_amount,
        "max_from_orderbook": max_from_orderbook,
        "capped_by_user":    max_from_orderbook > MAX_AMOUNT,
        "cost_per_share":    cost_per_share,
        "revenue_per_share": revenue_per_share,
        "total_cost":        total_cost,
        "total_revenue":     total_revenue,
        "net_profit":        net_profit,
        "index_set":         index_set,
        "index_set_hex":     hex(index_set),
        "market_id":         market_id,
        "amount_raw":        amount_raw,
        "selected_details":  selected_details,
        "yes_tokens":        result["yes_tokens"],
        "N":                 len(event["markets"]),
    }


def print_execution_plan(slug, event, plan):
    """打印 dry-run 执行计划"""
    ts = datetime.now().strftime("%H:%M:%S")
    cap_note = " (用户上限生效)" if plan["capped_by_user"] else " (orderbook 限制)"
    K = plan["K"]
    N = plan["N"]
    yes_count = N - K

    print(f"\n{'─'*70}")
    print(f"[DRY-RUN {ts}] 执行计划: {event['title'][:50]}")
    print(f"  slug: {slug}")
    print(f"  N={N} 子市场, K={K} (买入 {K} 个 NO), 白送 {yes_count} 种 YES")
    print(f"{'─'*70}")

    # 选中 token 明细
    print(f"  选中 NO token (按含费成本升序):")
    print(f"  {'#':>3}  {'outcome':<18} {'ask':>7} {'fee':>7} {'cost':>7} {'size':>9}  token_id")
    for i, d in enumerate(plan["selected_details"], 1):
        tid_short = d["token_id"][:10] + "..." if d["token_id"] else "?"
        print(f"  {i:>3}  {d['outcome']:<18} {d['price']:>7.4f} {d['fee']:>7.4f} "
              f"{d['cost']:>7.4f} {d['size']:>9.2f}  {tid_short}")

    # 资金计算
    print(f"\n  资金计算:")
    print(f"    单份成本:   {plan['cost_per_share']:.6f} pUSD")
    print(f"    单份收入:   {plan['revenue_per_share']:.6f} pUSD (K-1={K-1})")
    print(f"    单份利润:   {plan['revenue_per_share'] - plan['cost_per_share']:.6f} pUSD")
    print(f"    orderbook max_amount: {plan['max_from_orderbook']:.2f}{cap_note}")
    print(f"    执行量:     min({plan['max_from_orderbook']:.2f}, {MAX_AMOUNT}) = {plan['actual_amount']:.2f} 份")
    print(f"    总成本:     {plan['actual_amount']:.2f} × {plan['cost_per_share']:.6f} = {plan['total_cost']:.4f} pUSD")
    print(f"    总收入:     {plan['actual_amount']:.2f} × {plan['revenue_per_share']:.6f} = {plan['total_revenue']:.4f} pUSD")
    print(f"    净利润:     {plan['net_profit']:.4f} pUSD")

    # convertPositions 参数
    print(f"\n  convertPositions 调用参数 (待执行):")
    print(f"    合约:      {NEG_RISK_CTF_COLLATERAL_ADAPTER}")
    print(f"    marketId:  {plan['market_id'] or '(缺失!)'}")
    print(f"    indexSet:  {plan['index_set_hex']} ({bin(plan['index_set'])})")
    print(f"    amount:    {plan['amount_raw']} ({plan['actual_amount']:.2f} × 10^{PUSD_DECIMALS})")

    # YES token
    if plan["yes_tokens"]:
        print(f"\n  白送 YES token ({yes_count} 种, 各 {plan['actual_amount']:.2f} 份):")
        for y in plan["yes_tokens"]:
            print(f"    {y['outcome']:<18} (当前 ask={y['price']:.4f})")

    print(f"\n  [DRY-RUN] 未实际下单。加 --live 启用实盘 (需 P1 实现)。")
    print(f"{'─'*70}\n")


# ============================================================
# Step 6: 事件列表刷新（增量同步）
# ============================================================

def refresh_events(events, asset_map, token_list, tag_id, min_volume):
    """从 Gamma API 拉取最新事件列表, 增量同步到监控状态"""
    print(f"\n[刷新事件列表] tag={tag_id or 'all'}, min_volume={min_volume}...")
    fresh = fetch_neg_risk_events(tag_id, min_volume)
    if not fresh:
        print("  未获取到事件, 保留当前监控列表")
        return 0, 0

    fresh_slugs = {e["slug"] for e in fresh}
    current_slugs = set(events.keys())

    # 移除已结束/不再匹配的事件
    removed = current_slugs - fresh_slugs
    for slug in removed:
        remove_event(slug, events, asset_map, token_list)
        print(f"  - 移除: {slug}")

    # 新增事件
    added = 0
    for e in fresh:
        if e["slug"] not in events:
            add_event(e, events, asset_map, token_list)
            added += 1
            print(f"  + 新增: {e['slug']} ({len(e.get('markets', []))} 子市场)")
            time.sleep(GAMMA_RATE)

    save_events_csv(fresh)
    print(f"  刷新完成: +{added} 新增, -{len(removed)} 移除, 当前 {len(events)} event / {len(token_list)} token")
    return added, len(removed)


# ============================================================
# 状态打印
# ============================================================

def print_status(events, cycle, latency, candidates=0, deep_tokens=0):
    now = datetime.now().strftime("%H:%M:%S")
    total = sum(1 for ev in events.values() for m in ev["markets"]
                if m["best_ask"] is not None)
    expected = sum(len(ev["markets"]) for ev in events.values())
    deep_str = f" | 深层: {candidates} 候选 / {deep_tokens} token" if candidates else ""
    print(f"[{now}] #{cycle} 耗时 {latency:.1f}s | 价格就绪: {total}/{expected}{deep_str}")


# ============================================================
# Step 7: CLOB 实盘下单 (P1)
# ============================================================

_executed_slugs = set()      # 已执行的 slug, 防止重复


def load_env(env_path=ENV_FILE):
    """加载 .env 文件, 返回 dict"""
    env = {}
    if not os.path.exists(env_path):
        return env
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def make_clob_client(env=None):
    """初始化 ClobClient (复用 test/market_monitor.py 逻辑)"""
    if env is None:
        env = load_env()
    if SDK_PATH not in sys.path:
        sys.path.insert(0, SDK_PATH)
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds

    required = ["CLOB_API_KEY", "CLOB_SECRET", "CLOB_PASS_PHRASE", "PK"]
    missing = [k for k in required if k not in env]
    if missing:
        raise RuntimeError(f".env 缺少凭证: {', '.join(missing)}")

    chain_id = int(env.get("CHAIN_ID", "137"))
    creds = ApiCreds(
        api_key=env["CLOB_API_KEY"],
        api_secret=env["CLOB_SECRET"],
        api_passphrase=env["CLOB_PASS_PHRASE"],
    )
    funder = env.get("PROXY_ADDRESS", "").strip()
    signature_type = 1 if funder else 0  # POLY_PROXY or EOA

    client = ClobClient(
        host=CLOB,
        chain_id=chain_id,
        key=env["PK"],
        creds=creds,
        funder=funder or None,
        signature_type=signature_type,
    )
    return client


def query_balance(client):
    """查询 pUSD 余额 (返回 USDC 单位, 失败返回 -1)"""
    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
    try:
        result = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        return float(result.get("balance", 0)) / 1_000_000
    except Exception as e:
        print(f"  [余额查询失败] {e}")
        return -1


def place_buy_order(client, token_id, price, size):
    """下一笔 BUY 限价单 (GTC, neg_risk=True), 返回 order_id 或 None"""
    from py_clob_client_v2.clob_types import OrderArgs, PartialCreateOrderOptions, OrderType
    try:
        res = client.create_and_post_order(
            order_args=OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side="BUY",
            ),
            options=PartialCreateOrderOptions(neg_risk=True),
            order_type=OrderType.GTC,
        )
        order_id = res.get("orderID") or res.get("order_id", "")
        return str(order_id) if order_id else None
    except Exception as e:
        print(f"  [下单失败] token={token_id[:12]}... price={price:.4f}: {e}")
        return None


def place_buy_orders_batch(client, details, size):
    """批量下 K 笔 BUY 限价单 (单次 HTTP), 返回 [(detail, order_id), ...]"""
    from py_clob_client_v2.clob_types import (
        OrderArgs, PartialCreateOrderOptions, OrderType, PostOrdersV2Args,
    )
    try:
        # 1. 创建 K 个签名订单
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

        # 2. 批量提交 (单次 HTTP 请求)
        res = client.post_orders(signed_args)

        # 3. 解析返回的 order_id 列表
        items = res.get("data", res) if isinstance(res, dict) else res
        if not isinstance(items, list):
            items = [items]

        results = []
        for i, item in enumerate(items):
            if i >= len(details):
                break
            order_id = ""
            if isinstance(item, dict):
                order_id = item.get("orderID") or item.get("order_id") or item.get("id", "")
            results.append((details[i], str(order_id) if order_id else None))
        return results
    except Exception as e:
        print(f"  [批量下单失败] {e}")
        return [(d, None) for d in details]


def get_order_status(client, order_id):
    """查询订单状态, 返回 (status_str, size_matched)"""
    try:
        order = client.get_order(order_id)
        status = str(order.get("status", "UNKNOWN")).upper()
        size_matched = float(order.get("size_matched", 0))
        return status, size_matched
    except Exception as e:
        print(f"  [查询订单失败] {order_id[:12]}...: {e}")
        return "ERROR", 0.0


def cancel_order(client, order_id):
    """取消单笔订单"""
    from py_clob_client_v2.clob_types import OrderPayload
    try:
        client.cancel_order(OrderPayload(orderID=order_id))
        return True
    except Exception as e:
        print(f"  [取消订单失败] {order_id[:12]}...: {e}")
        return False


# ============================================================
# Step 7b: convertPositions 合约调用 (P2, 走 Relayer 免 gas)
# ============================================================

def make_relay_client(env=None):
    """初始化 RelayClient (用 builder API 凭证, POLY_PROXY 模式)"""
    if env is None:
        env = load_env()
    from py_builder_relayer_client.client import RelayClient
    from py_builder_relayer_client.models import RelayerTxType
    from py_builder_signing_sdk.config import BuilderConfig
    from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds

    required = ["PK", "CLOB_API_KEY", "CLOB_SECRET", "CLOB_PASS_PHRASE"]
    missing = [k for k in required if k not in env]
    if missing:
        raise RuntimeError(f".env 缺少凭证: {', '.join(missing)}")

    relayer_url = env.get("RELAYER_HOST", "https://relayer-v2.polymarket.com")
    chain_id = int(env.get("CHAIN_ID", "137"))

    builder_config = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(
            key=env["CLOB_API_KEY"],
            secret=env["CLOB_SECRET"],
            passphrase=env["CLOB_PASS_PHRASE"],
        )
    )

    return RelayClient(
        relayer_url=relayer_url,
        chain_id=chain_id,
        private_key=env["PK"],
        builder_config=builder_config,
        relay_tx_type=RelayerTxType.PROXY,
    )


def execute_convert_positions(relay_client, market_id_hex, index_set, amount_raw):
    """通过 Relayer 调用 convertPositions (免 gas), 返回 tx_hash 或 None"""
    from web3 import Web3
    from py_builder_relayer_client.models import Transaction

    # 1. marketId hex → bytes32
    if not market_id_hex or not market_id_hex.startswith("0x"):
        print(f"  [convert] marketId 无效: {market_id_hex}")
        return None
    try:
        market_id_bytes = bytes.fromhex(market_id_hex[2:])
        if len(market_id_bytes) != 32:
            print(f"  [convert] marketId 长度异常: {len(market_id_bytes)} (应为 32)")
            return None
    except Exception as e:
        print(f"  [convert] marketId 解析失败: {e}")
        return None

    # 2. 编码 convertPositions calldata
    try:
        contract = Web3().eth.contract(
            address=NEG_RISK_CTF_COLLATERAL_ADAPTER, abi=CONVERT_ABI
        )
        data = contract.encode_abi(
            abi_element_identifier="convertPositions",
            args=[market_id_bytes, index_set, amount_raw],
        )
    except Exception as e:
        print(f"  [convert] ABI 编码失败: {e}")
        return None

    # 3. 通过 Relayer 提交 (免 gas)
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n  [{ts}] convertPositions 通过 Relayer 提交...")
    print(f"    合约:     {NEG_RISK_CTF_COLLATERAL_ADAPTER}")
    print(f"    marketId: {market_id_hex}")
    print(f"    indexSet: {hex(index_set)}")
    print(f"    amount:   {amount_raw} ({amount_raw / 10**PUSD_DECIMALS:.2f} 份)")

    try:
        tx = Transaction(
            to=NEG_RISK_CTF_COLLATERAL_ADAPTER,
            data=data,
            value="0",
        )
        response = relay_client.execute([tx], "Neg risk convertPositions")

        tx_id = getattr(response, "transaction_id", None) or "?"
        print(f"    Relayer tx_id: {tx_id}")
        print(f"    等待链上确认...")

        result = response.wait()

        tx_hash = getattr(result, "transaction_hash", None) if result else None
        state = getattr(result, "state", "?") if result else "?"

        if state == "STATE_CONFIRMED" and tx_hash:
            print(f"    OK 链上确认! tx_hash: {tx_hash}")
            return tx_hash
        else:
            print(f"    FAIL 状态: {state}")
            return None
    except Exception as e:
        print(f"  [convert] Relayer 提交失败: {e}")
        return None


def execute_arbitrage(slug, event, plan, client, relay_client=None):
    """实盘执行套利: 批量下单 → 等待成交 → 取消未成交 → (可选) convertPositions"""
    ts = datetime.now().strftime("%H:%M:%S")
    K = plan["K"]
    amount = plan["actual_amount"]
    total_cost = plan["total_cost"]

    print(f"\n{'='*70}")
    print(f"[LIVE {ts}] 执行套利: {event['title'][:50]}")
    print(f"  K={K}, amount={amount:.2f}, 预计成本={total_cost:.4f} pUSD")

    # 1. 批量下 K 笔限价买单 (单次 HTTP 请求)
    print(f"  批量下单: {K} 笔 BUY limit @ ask (GTC, neg_risk=True)...")
    placed = place_buy_orders_batch(client, plan["selected_details"], amount)

    orders = []  # [(detail, order_id)]
    for d, order_id in placed:
        if order_id:
            orders.append((d, order_id))
            print(f"    OK    {d['outcome']:<18} @ {d['price']:.4f} x {amount:.2f} -> {order_id[:12]}...")
        else:
            print(f"    FAIL  {d['outcome']:<18} @ {d['price']:.4f}")

    if len(orders) < K:
        print(f"  [只成功下单 {len(orders)}/{K}, 取消已下的, 放弃执行]")
        for _, oid in orders:
            cancel_order(client, oid)
        return False

    # 2. 等待成交 (轮询 get_order, 超时 ORDER_FILL_TIMEOUT 秒)
    print(f"  等待成交 (超时 {ORDER_FILL_TIMEOUT}s, 每 {ORDER_POLL_INTERVAL}s 轮询)...")
    filled = {}  # order_id -> size_matched
    deadline = time.monotonic() + ORDER_FILL_TIMEOUT
    while time.monotonic() < deadline:
        all_done = True
        for d, oid in orders:
            if oid in filled:
                continue
            status, size = get_order_status(client, oid)
            if status in ("MATCHED", "FILLED"):
                filled[oid] = size
                print(f"    FILLED  {d['outcome']:<18} {size:.2f}")
            elif status in ("CANCELED", "CANCELLED", "EXPIRED"):
                filled[oid] = size  # 可能部分成交后被取消
                print(f"    {status}  {d['outcome']:<18} (matched={size:.2f})")
            elif status == "LIVE":
                all_done = False
            else:
                all_done = False
        if all_done:
            break
        time.sleep(ORDER_POLL_INTERVAL)

    # 3. 取消未成交订单
    unfilled = [(d, oid) for d, oid in orders if oid not in filled]
    if unfilled:
        print(f"  超时, 取消 {len(unfilled)} 笔未成交订单...")
        for d, oid in unfilled:
            cancel_order(client, oid)
            # 取消后再查一次, 看是否在取消前已部分成交
            status, size = get_order_status(client, oid)
            filled[oid] = size
            if size > 0:
                print(f"    PARTIAL {d['outcome']:<18} 成交 {size:.2f} 后取消")
            else:
                print(f"    EMPTY   {d['outcome']:<18} 未成交, 已取消")

    # 4. 汇总结果
    sizes = list(filled.values())
    full_count = sum(1 for s in sizes if s >= amount * 0.99)
    partial_count = sum(1 for s in sizes if 0 < s < amount * 0.99)
    empty_count = sum(1 for s in sizes if s == 0)
    # convertPositions 需要等量, 取最小成交量
    convertible_amount = min(sizes) if sizes else 0

    print(f"\n  执行结果:")
    print(f"    全部成交: {full_count}/{K}")
    print(f"    部分成交: {partial_count}/{K}")
    print(f"    未成交:   {empty_count}/{K}")
    print(f"    可 convert 的 amount (最小成交量): {convertible_amount:.2f}")

    # 5. convertPositions (如果有足够成交量)
    if convertible_amount > 0 and relay_client is not None:
        convert_amount_raw = int(round(convertible_amount * 10**PUSD_DECIMALS))
        tx_hash = execute_convert_positions(
            relay_client,
            plan["market_id"],
            plan["index_set"],
            convert_amount_raw,
        )
        if tx_hash:
            print(f"\n  套利闭环完成!")
            print(f"    收入: {convertible_amount:.2f} × (K-1) = {convertible_amount * (K-1):.2f} pUSD")
            print(f"    YES token: {plan['N'] - K} 种 × {convertible_amount:.2f} 份")
            print(f"    tx_hash: {tx_hash}")
            print(f"{'='*70}\n")
            return True
        else:
            print(f"\n  convertPositions 失败! 持有的 NO token 需手动处理")
            print(f"{'='*70}\n")
            return False
    elif convertible_amount > 0 and relay_client is None:
        print(f"  (relay_client 未初始化, 跳过 convert. NO token 已买入, 需手动 convert)")
        print(f"{'='*70}\n")
        return False
    elif convertible_amount == 0:
        print(f"  FAIL 全部未成交, 机会已消失")
        print(f"{'='*70}\n")
        return False


# ============================================================
# Main
# ============================================================

def main():
    global POLL_INTERVAL, EVENT_REFRESH_INTERVAL, TAG_ID, MIN_VOLUME, DRY_RUN, MAX_AMOUNT

    for i, a in enumerate(sys.argv):
        if a == "--tag" and i + 1 < len(sys.argv):
            TAG_ID = sys.argv[i + 1]
        elif a == "--poll" and i + 1 < len(sys.argv):
            POLL_INTERVAL = int(sys.argv[i + 1])
        elif a == "--refresh" and i + 1 < len(sys.argv):
            EVENT_REFRESH_INTERVAL = int(sys.argv[i + 1])
        elif a == "--min-volume" and i + 1 < len(sys.argv):
            MIN_VOLUME = float(sys.argv[i + 1])
        elif a == "--live":
            DRY_RUN = False
        elif a == "--max-amount" and i + 1 < len(sys.argv):
            MAX_AMOUNT = float(sys.argv[i + 1])

    mode = "DRY-RUN (只打印执行计划)" if DRY_RUN else "LIVE (实盘!)"
    print(f"Neg Risk 套利监控 (轮询版)")
    print(f"  模式: {mode}")
    print(f"  轮询间隔: {POLL_INTERVAL}s | 事件刷新: {EVENT_REFRESH_INTERVAL}s")
    print(f"  tag={TAG_ID or 'all'} | min_volume={MIN_VOLUME}")
    print(f"  批量大小: {BATCH_SIZE} token/请求 | 并发: {MAX_WORKERS}")
    print(f"  执行上限: {MAX_AMOUNT} 份/次 | convert 合约: {NEG_RISK_CTF_COLLATERAL_ADAPTER[:14]}...")

    # 实盘模式: 初始化 ClobClient + RelayClient
    clob_client = None
    relay_client = None
    if not DRY_RUN:
        try:
            clob_client = make_clob_client()
            signer = clob_client.get_address()
            balance = query_balance(clob_client)
            print(f"  CLOB 签名地址: {signer}")
            print(f"  pUSD 余额: {balance:.2f}")
        except Exception as e:
            print(f"  [ClobClient 初始化失败] {e}")
            print(f"  自动降级为 DRY-RUN 模式")
            DRY_RUN = True
            clob_client = None

    if not DRY_RUN:
        try:
            relay_client = make_relay_client()
            print(f"  Relayer: 已初始化 (POLY_PROXY 模式, 免 gas)")
        except Exception as e:
            print(f"  [RelayClient 初始化失败] {e}")
            print(f"  convertPositions 将不可用 (CLOB 下单仍可执行)")
            relay_client = None

    events = OrderedDict()
    asset_map = {}
    token_list = []

    # 初始加载
    print("\n[初始加载事件列表...]")
    refresh_events(events, asset_map, token_list, TAG_ID, MIN_VOLUME)
    if not events:
        print("没有可监控的事件, 退出")
        sys.exit(1)

    print(f"\n开始监控 {len(events)} event / {len(token_list)} token")
    print(f"套利机会保存至: {OPPS_CSV}\n")

    last_refresh = time.monotonic()
    cycle = 0

    try:
        while True:
            now = time.monotonic()

            # 慢循环: 刷新事件列表
            if now - last_refresh >= EVENT_REFRESH_INTERVAL:
                refresh_events(events, asset_map, token_list, TAG_ID, MIN_VOLUME)
                last_refresh = now
                if not events:
                    print("事件列表为空, 等待下一轮刷新...")
                    time.sleep(POLL_INTERVAL)
                    continue

            # 快层: /prices 拿全部 token 的 best ask
            t0 = time.monotonic()
            prices = poll_all_prices(token_list)
            apply_price_updates(prices, events, asset_map)

            # 轻量检测: 只用价格筛选候选事件
            candidates = [slug for slug, ev in events.items()
                          if has_arbitrage_potential(ev)]

            # 深层: 仅对候选事件查 /books 拿 ask_size
            deep_count = 0
            if candidates:
                candidate_tokens = []
                for slug in candidates:
                    for m in events[slug]["markets"]:
                        if m["best_ask"] is not None and m["best_ask"] > 0:
                            candidate_tokens.append(m["no_token_id"])
                if candidate_tokens:
                    books = poll_all_orderbooks(candidate_tokens)
                    apply_orderbook_updates(books, events, asset_map)
                    deep_count = len(candidate_tokens)

                # 完整检测 (含 ask_size → max_amount → 真实利润)
                for slug in candidates:
                    check_and_print(slug, events[slug], clob_client, relay_client)

            cycle += 1
            latency = time.monotonic() - t0
            print_status(events, cycle, latency, len(candidates), deep_count)

            # 等待下一轮（扣除已耗时间）
            sleep_time = max(0, POLL_INTERVAL - latency)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\n停止监控 (共 {cycle} 轮)")


if __name__ == "__main__":
    main()
