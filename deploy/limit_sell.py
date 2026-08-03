#!/usr/bin/env python3
"""
持仓限价挂卖 — 查询持仓并对未挂卖单的 token 以 0.999 价格挂 GTC 限价卖单。

用法:
  python limit_sell.py                    # 单次 DRY-RUN
  python limit_sell.py --live             # 单次实盘
  python limit_sell.py --live --interval 10  # 每 10 分钟循环
  python limit_sell.py --live -p 0.995    # 自定义价格
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
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
INTERVAL_MINUTES = 0        # 0=单次执行
SELL_PRICE = 0.999          # 挂卖价格
TICK_SIZE = "0.001"         # Neg Risk 市场精度

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
    title: str
    outcome: str


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
    from eth_account import Account
    return Account.from_key(pk).address


# ═══════════════════════════════════════════════════════════════════════════════
#  持仓查询 — Data API
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_positions(address: str) -> list[dict]:
    url = f"{DATA_API}/positions?user={address}&sizeThreshold=0.01"
    try:
        data = json.loads(_http_get(url, timeout=15))
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"  [WARN] 持仓查询失败: {e}", file=sys.stderr)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
#  Open orders 查询 — CLOB SDK
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_sell_order_tokens(client: ClobClient) -> set[str]:
    """返回当前已有 SELL 挂单的 token_id 集合"""
    try:
        orders = client.get_open_orders() or []
        return {o.get("asset_id", "") for o in orders if o.get("side") == "SELL"}
    except Exception as e:
        print(f"  [WARN] 查询 open orders 失败: {e}", file=sys.stderr)
        return set()


# ═══════════════════════════════════════════════════════════════════════════════
#  限价卖单
# ═══════════════════════════════════════════════════════════════════════════════

def place_sell_order(client: ClobClient, token_id: str, size: float, price: float) -> str | None:
    try:
        kwargs = dict(
            order_args=OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side="SELL",
            ),
            options=PartialCreateOrderOptions(tick_size=TICK_SIZE),
        )
        try:
            kwargs["order_type"] = OrderType.GTC
        except NameError:
            pass
        res = client.create_and_post_order(**kwargs)
        return str(res.get("orderID") or res.get("order_id", "")) or None
    except Exception as e:
        print(f"    [SELL FAIL] token={token_id[:20]}... size={size:.2f} price={price}: {e}",
              file=sys.stderr)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════════════════════════════════════

def run_cycle(client: ClobClient, address: str, sell_price: float, live_mode: bool = False):
    t0 = time.time()

    # Step 1: 查持仓
    print("[1/3] 查询持仓...", file=sys.stderr, flush=True)
    raw_positions = fetch_positions(address)

    if not raw_positions:
        elapsed = time.time() - t0
        print("=" * 75)
        print(f"  限价挂卖  |  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}  |  {elapsed:.1f}s")
        print("=" * 75)
        print("  当前无持仓。")
        return

    positions: list[Position] = []
    for p in raw_positions:
        try:
            size = float(p.get("size", 0))
            if size <= 0:
                continue
            positions.append(Position(
                token_id=p.get("asset", p.get("tokenId", "")),
                size=size,
                title=p.get("title", ""),
                outcome=p.get("outcome", ""),
            ))
        except (ValueError, TypeError):
            continue

    print(f"      -> {len(positions)} 个持仓", file=sys.stderr, flush=True)

    # Step 2: 查 open orders 中已有 SELL 的 token
    print("[2/3] 查询已有 SELL 挂单...", file=sys.stderr, flush=True)
    selling_tokens = fetch_sell_order_tokens(client)
    print(f"      -> {len(selling_tokens)} 个 token 已有 SELL 挂单", file=sys.stderr, flush=True)

    # Step 3: 对未挂卖的持仓下限价卖单
    print(f"[3/3] 挂卖 @ {sell_price} ({'LIVE' if live_mode else 'DRY-RUN'})...", file=sys.stderr, flush=True)

    to_sell = [p for p in positions if p.token_id and p.token_id not in selling_tokens]
    already_selling = len(positions) - len(to_sell)

    placed = 0
    failed = 0
    if to_sell and live_mode:
        for p in to_sell:
            oid = place_sell_order(client, p.token_id, p.size, sell_price)
            if oid:
                placed += 1
            else:
                failed += 1
    elif to_sell:
        print(f"      [DRY-RUN] 将挂出 {len(to_sell)} 个卖单", file=sys.stderr, flush=True)

    # 摘要
    elapsed = time.time() - t0
    print()
    print("=" * 95)
    print(f"  限价挂卖  |  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}  |  {elapsed:.1f}s  |  "
          f"{'LIVE' if live_mode else 'DRY-RUN'}  |  价格: {sell_price}")
    print("=" * 95)
    parts = [f"总持仓: {len(positions)}", f"已挂卖: {already_selling}", f"待挂卖: {len(to_sell)}"]
    if live_mode:
        parts.append(f"本轮挂出: {placed}")
        if failed:
            parts.append(f"失败: {failed}")
    print(f"  {'  |  '.join(parts)}")

    print()
    print(f"  {'─' * 90}")
    print(f"  {'Token ID':<22} {'持仓':>8} {'状态':>8} {'操作'}")
    print(f"  {'─' * 90}")
    for p in positions:
        if not p.token_id:
            status = "无效"
            action = "skip"
        elif p.token_id in selling_tokens:
            status = "已挂卖"
            action = "skip"
        else:
            status = "待挂卖"
            if live_mode:
                action = f"SELL ✓ @ {sell_price}" if placed > 0 else "SELL FAIL"
            else:
                action = f"SELL @ {sell_price} [DRY]"
        print(f"  {p.token_id[:20]:<22} {p.size:>8.1f} {status:>8}  {action}")
    print(f"  {'─' * 90}")
    print()


def main():
    global SELL_PRICE

    parser = argparse.ArgumentParser(description="持仓限价挂卖")
    parser.add_argument("--live", action="store_true", default=None, help="启用实盘卖出")
    parser.add_argument("--dry-run", action="store_true", default=None, help="仅扫描不下单")
    parser.add_argument("--interval", "-i", type=int, default=None, help="循环间隔（分钟）")
    parser.add_argument("--price", "-p", type=float, default=None, help=f"挂卖价格（默认 {SELL_PRICE}）")
    args = parser.parse_args()

    live_mode = LIVE_MODE
    if args.live:
        live_mode = True
    elif args.dry_run:
        live_mode = False
    interval = args.interval if args.interval is not None else INTERVAL_MINUTES
    if args.price is not None:
        SELL_PRICE = args.price

    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

    print("=" * 70, file=sys.stderr)
    print(" 持仓限价挂卖", file=sys.stderr)
    print(f" 挂卖价格: {SELL_PRICE}  |  "
          f"卖出: {'启用 (LIVE)' if live_mode else '禁用 (DRY-RUN)'}  |  "
          f"循环: {'每 ' + str(interval) + ' 分钟' if interval > 0 else '单次'}", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    env = load_env(ENV_FILE)
    try:
        client = make_clob_client(env)
    except RuntimeError as e:
        print(f"[FATAL] CLOB 客户端初始化失败: {e}", file=sys.stderr)
        sys.exit(1)

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
            run_cycle(client, address, SELL_PRICE, live_mode)
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
