#!/usr/bin/env python3
"""
持仓止损监控 — 独立脚本

定期扫描实际持仓，ask 价低于阈值时限价卖出。

用法:
  python stop_loss.py              # 单次 DRY-RUN
  python stop_loss.py --live       # 单次实盘
  python stop_loss.py --live --interval 5  # 每 5 分钟循环
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
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── SDK 路径 ──────────────────────────────────────────────────────────────────
for _SDK_PATH in (
    os.path.join(SCRIPT_DIR, "py_clob_client_v2"),
    os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", "py-clob-client", "py-clob-client-v2-main", "py-clob-client-v2-main")),
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
    from py_clob_client.clob_types import ApiCreds, OrderArgs, PartialCreateOrderOptions  # type: ignore[no-redef]
    _IS_V2 = False

# ═══════════════════════════════════════════════════════════════════════════════
#  可配置参数
# ═══════════════════════════════════════════════════════════════════════════════

LIVE_MODE = False
INTERVAL_MINUTES = 2   # 0=单次执行
STOP_LOSS_THRESHOLD = 0.93   # ask 低于此值触发止损
CONFIRM_ROUNDS = 2            # 连续触发轮次才执行卖出

# 跨轮次状态：记录上一轮触发的 token，用于连续确认
_pending: dict[str, float] = {}  # token_id → ask_price
PRICE_WORKERS = 8
CLOB_BATCH_SIZE = 500

DATA_API = "https://data-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"
ENV_FILE = os.path.join(SCRIPT_DIR, ".env")


# ═══════════════════════════════════════════════════════════════════════════════
#  数据结构
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Position:
    token_id: str
    size: float
    avg_price: float
    title: str
    outcome: str
    condition_id: str
    cur_price: float  # CLOB ask


# ═══════════════════════════════════════════════════════════════════════════════
#  HTTP 工具
# ═══════════════════════════════════════════════════════════════════════════════

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
    raise last_err


# ═══════════════════════════════════════════════════════════════════════════════
#  环境 & CLOB 客户端
# ═══════════════════════════════════════════════════════════════════════════════

def load_env(env_path: str) -> dict:
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
        signature_type = 1
    else:
        signature_type = 0

    return ClobClient(
        host=CLOB_HOST,
        chain_id=chain_id,
        key=env["PK"],
        creds=creds,
        funder=funder or None,
        signature_type=signature_type,
    )


def derive_address(pk: str) -> str:
    """从私钥推导钱包地址"""
    from eth_account import Account
    acct = Account.from_key(pk)
    return acct.address


# ═══════════════════════════════════════════════════════════════════════════════
#  持仓查询 — Data API
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_positions(address: str) -> list[dict]:
    """通过 Data API 查询当前持仓"""
    url = f"{DATA_API}/positions?user={address}&sizeThreshold=0.01"
    try:
        data = json.loads(_http_get(url, timeout=15))
        if isinstance(data, list):
            return data
        return []
    except Exception as e:
        print(f"  [WARN] 持仓查询失败: {e}", file=sys.stderr)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
#  CLOB 价格查询
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_prices_sdk(client: ClobClient, token_ids: list[str],
                     side: str = "BUY") -> dict[str, float]:
    batches = [token_ids[i:i + CLOB_BATCH_SIZE]
               for i in range(0, len(token_ids), CLOB_BATCH_SIZE)]

    all_results: dict[str, float] = {}

    def _fetch_one(batch: list[str]) -> dict[str, float]:
        try:
            if _IS_V2:
                params = [{"token_id": tid, "side": side} for tid in batch]
            else:
                from py_clob_client.clob_types import BookParams
                params = [BookParams(token_id=tid, side=side) for tid in batch]
            result = client.get_prices(params)
            return {tid: float(data.get(side, 0))
                    for tid, data in result.items()}
        except Exception as e:
            print(f"  [CLOB error] {e}", file=sys.stderr)
            return {}

    with ThreadPoolExecutor(max_workers=PRICE_WORKERS) as executor:
        futures = {executor.submit(_fetch_one, b): i for i, b in enumerate(batches)}
        for f in as_completed(futures):
            all_results.update(f.result())

    return all_results


# ═══════════════════════════════════════════════════════════════════════════════
#  卖出
# ═══════════════════════════════════════════════════════════════════════════════

def place_sell_order(client: ClobClient, pos: Position) -> str | None:
    """止损限价卖出，挂单价 = ask - 0.03，返回 order_id，失败返回 None"""
    sell_price = max(pos.cur_price - 0.03, 0.01)
    try:
        kwargs = dict(
            order_args=OrderArgs(
                token_id=pos.token_id,
                price=sell_price,
                size=pos.size,
                side="SELL",
            ),
            options=PartialCreateOrderOptions(tick_size=str(0.01)),
        )
        try:
            kwargs["order_type"] = OrderType.GTC
        except NameError:
            pass
        res = client.create_and_post_order(**kwargs)
        return str(res.get("orderID") or res.get("order_id", "")) or None
    except Exception as e:
        print(f"    [SELL FAIL] token={pos.token_id[:20]}... size={pos.size:.2f} price={sell_price:.4f}: {e}",
              file=sys.stderr)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════════════════════════════════════

def run_cycle(client: ClobClient, address: str, live_mode: bool = False):
    """执行一次完整扫描周期"""
    global _pending
    t0 = time.time()

    # Step 1: 查持仓
    print("[1/3] 查询持仓...", file=sys.stderr, flush=True)
    raw_positions = fetch_positions(address)

    if not raw_positions:
        elapsed = time.time() - t0
        print("=" * 75)
        print(f"  止损扫描  |  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}  |  {elapsed:.1f}s")
        print("=" * 75)
        print("  当前无持仓。")
        return

    # 组装 Position 对象
    positions: list[Position] = []
    for p in raw_positions:
        try:
            size = float(p.get("size", 0))
            if size <= 0:
                continue
            positions.append(Position(
                token_id=p.get("asset", p.get("tokenId", "")),
                size=size,
                avg_price=float(p.get("avgPrice", 0)),
                title=p.get("title", p.get("conditionId", "")),
                outcome=p.get("outcome", ""),
                condition_id=p.get("conditionId", ""),
                cur_price=0.0,
            ))
        except (ValueError, TypeError):
            continue

    print(f"      -> {len(positions)} 个持仓", file=sys.stderr, flush=True)

    # Step 2: 查 CLOB ask 价
    token_ids = list({p.token_id for p in positions})
    print(f"[2/3] 查询 CLOB ask 价: {len(token_ids)} tokens...", file=sys.stderr, flush=True)
    ask_prices = fetch_prices_sdk(client, token_ids, side="SELL")

    for p in positions:
        p.cur_price = ask_prices.get(p.token_id, 0.0)

    # Step 3: 连续确认机制
    triggered_this = [p for p in positions if 0 < p.cur_price < STOP_LOSS_THRESHOLD]
    triggered_ids = {p.token_id for p in triggered_this}
    safe = len(positions) - len(triggered_this)

    # 清理已回升的（上一轮触发但本轮未触发）
    cleared = sum(1 for tid in list(_pending) if tid not in triggered_ids)
    _pending = {tid: price for tid, price in _pending.items() if tid in triggered_ids}

    # 更新本轮新触发的 token 到 _pending
    first_time = [p for p in triggered_this if p.token_id not in _pending]
    confirmed = [p for p in triggered_this if p.token_id in _pending]
    for p in first_time:
        _pending[p.token_id] = p.cur_price

    print(f"[3/3] 止损检查: 阈值 <{STOP_LOSS_THRESHOLD}, "
          f"安全 {safe}, 首次触发 {len(first_time)}, "
          f"连续触发 {len(confirmed)}, 已解除 {cleared}", file=sys.stderr, flush=True)

    # 连续触发 → 执行卖出
    sold = 0
    failed = 0
    if confirmed and live_mode:
        print(f"      执行卖出 ({'LIVE' if live_mode else 'DRY-RUN'})...", file=sys.stderr, flush=True)
        for p in confirmed:
            oid = place_sell_order(client, p)
            if oid:
                sold += 1
                _pending.pop(p.token_id, None)  # 卖出成功，移除待确认
            else:
                failed += 1
    elif confirmed:
        print(f"      [DRY-RUN] 将卖出 {len(confirmed)} 个持仓（连续触发 {CONFIRM_ROUNDS} 轮）", file=sys.stderr, flush=True)

    # 打印摘要
    elapsed = time.time() - t0
    print()
    print("=" * 95)
    print(f"  止损扫描  |  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}  |  {elapsed:.1f}s  |  "
          f"{'LIVE' if live_mode else 'DRY-RUN'}")
    print("=" * 95)
    status_parts = [f"总持仓: {len(positions)}", f"安全: {safe}"]
    if cleared:
        status_parts.append(f"已解除: {cleared}")
    status_parts.append(f"待确认: {len(first_time)}")
    status_parts.append(f"连续触发: {len(confirmed)}")
    status_parts.append(f"已卖出: {sold}")
    if failed:
        status_parts.append(f"失败: {failed}")
    print(f"  {'  |  '.join(status_parts)}")

    if not positions:
        return

    print()
    print(f"  {'─' * 100}")
    print(f"  {'Token ID':<20} {'持仓':>8} {'均价':>8} {'Ask':>8} {'状态':>10} {'操作'}")
    print(f"  {'─' * 100}")
    for p in positions:
        if p.token_id in triggered_ids:
            if p.token_id in {c.token_id for c in confirmed}:
                status = "!!连续触发"
            else:
                status = "待确认"
        else:
            status = "-"
        sell_price = max(p.cur_price - 0.03, 0.01)
        action = ""
        if status == "!!连续触发":
            if live_mode and p.token_id not in {c.token_id for c in confirmed if sold}:
                pass  # handled below
            if live_mode:
                action = f"SELL ✓ @ {sell_price:.4f}" if p.cur_price > 0 else "N/A"
            else:
                action = f"SELL @ {sell_price:.4f} [DRY]"
        print(f"  {p.token_id[:18]:<20} {p.size:>8.1f} {p.avg_price:>8.4f} {p.cur_price:>8.4f} "
              f"{status:>10}  {action}")
    print(f"  {'─' * 100}")
    print()


def main():
    global STOP_LOSS_THRESHOLD

    parser = argparse.ArgumentParser(description="持仓止损监控")
    parser.add_argument("--live", action="store_true", default=None,
                        help="启用实盘卖出")
    parser.add_argument("--dry-run", action="store_true", default=None,
                        help="仅扫描不下单")
    parser.add_argument("--interval", "-i", type=int, default=None,
                        help="循环间隔（分钟）")
    parser.add_argument("--threshold", "-t", type=float, default=None,
                        help=f"止损阈值（默认 {STOP_LOSS_THRESHOLD}）")
    args = parser.parse_args()

    live_mode = LIVE_MODE
    if args.live:
        live_mode = True
    elif args.dry_run:
        live_mode = False
    interval = args.interval if args.interval is not None else INTERVAL_MINUTES

    if args.threshold is not None:
        STOP_LOSS_THRESHOLD = args.threshold

    # UTF-8 输出
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

    print("=" * 70, file=sys.stderr)
    print(" 持仓止损监控", file=sys.stderr)
    print(f" 止损阈值: ask < {STOP_LOSS_THRESHOLD}  |  "
          f"卖出: {'启用 (LIVE)' if live_mode else '禁用 (DRY-RUN)'}  |  "
          f"循环: {'每 ' + str(interval) + ' 分钟' if interval > 0 else '单次'}", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    # 初始化
    env = load_env(ENV_FILE)
    try:
        client = make_clob_client(env)
    except RuntimeError as e:
        print(f"[FATAL] CLOB 客户端初始化失败: {e}", file=sys.stderr)
        sys.exit(1)

    # 持仓在代理地址下（POLY_PROXY），优先使用代理地址
    proxy = (env.get("PROXY_ADDRESS", "") or env.get("FUNDER_ADDRESS", "")).strip()
    if proxy:
        address = proxy
        print(f"代理地址: {address}\n", file=sys.stderr)
    else:
        pk = env.get("PK", "")
        try:
            address = derive_address(pk)
            print(f"钱包地址: {address}\n", file=sys.stderr)
        except Exception as e:
            print(f"[FATAL] 无法从 PK 推导地址: {e}", file=sys.stderr)
            sys.exit(1)

    iteration = 0
    while True:
        iteration += 1
        if interval > 0:
            print(f"{'─' * 60}", file=sys.stderr)
            print(f" 第 {iteration} 轮  |  "
                  f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
                  file=sys.stderr)
            print(f"{'─' * 60}", file=sys.stderr)

        try:
            run_cycle(client, address, live_mode)
        except Exception as e:
            print(f"\n[ERROR] 本轮异常: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)

        if interval <= 0:
            break

        print(f"  等待 {interval} 分钟后下一轮...", file=sys.stderr)
        time.sleep(interval * 60)


if __name__ == "__main__":
    main()
