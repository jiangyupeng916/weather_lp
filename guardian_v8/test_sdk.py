#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""V8 SDK 挂单测试 — 以0.01低价挂单（不成交），验证下单/查询/撤单全链路。

用法: python test_sdk.py [bot1|bot2]
"""

import sys
import os
import time
import requests

# ── 加载 .env ──────────────────────────────────────────────────────────────────
instance = sys.argv[1] if len(sys.argv) > 1 else "bot1"
env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f".env.{instance}")

from dotenv import load_dotenv
if os.path.exists(env_file):
    load_dotenv(env_file)
    print(f"[✓] 加载 {env_file}")
else:
    print(f"[✗] 找不到 {env_file}")
    sys.exit(1)

# 与 config.py 的命名优先级保持一致：官方命名优先，旧短名 fallback
pk         = os.environ.get("SIGNER_PRIVATE_KEY") or os.environ.get("PK", "")
wallet     = os.environ.get("POLYMARKET_WALLET_ADDRESS") or os.environ.get("WALLET_ADDRESS", "")
proxy      = os.environ.get("PROXY_ADDRESS", "")
relay_key  = os.environ.get("POLYMARKET_RELAYER_API_KEY") or os.environ.get("RELAYER_API_KEY", "")
relay_addr = os.environ.get("POLYMARKET_RELAYER_API_KEY_ADDRESS") or os.environ.get("RELAYER_API_KEY_ADDRESS", "")

if not pk:
    print("[✗] PK 环境变量未设置")
    sys.exit(1)

print(f"[i] 实例={instance}  wallet={wallet or proxy or '(未设→SDK默认)'}  relayer={'(设置)' if (relay_key and relay_addr) else '(未设置)'}")

# ── 初始化 SecureClient ────────────────────────────────────────────────────────
print("\n── Step 1: 初始化 SecureClient ────────────────────────────────")
from polymarket import RelayerApiKey, SecureClient

api_key = None
if relay_key and relay_addr:
    api_key = RelayerApiKey(key=relay_key, address=relay_addr)

client = SecureClient.create(
    private_key=pk,
    wallet=wallet or proxy or None,
    api_key=api_key,
)
address = client.wallet
print(f"[✓] client.wallet = {address}")

# ── 获取一个活跃市场的 token_id ───────────────────────────────────────────────
print("\n── Step 2: 获取测试市场 token_id ──────────────────────────────")
CLOB = "https://clob.polymarket.com"
r = requests.get(
    f"{CLOB}/sampling-markets",
    params={"next_cursor": "", "count": 5},
    timeout=10,
)
r.raise_for_status()
markets_data = r.json()

token_id = None
for m in (markets_data.get("data") or []):
    tokens = m.get("tokens") or []
    for t in tokens:
        tid = t.get("token_id", "")
        if tid:
            token_id = tid
            title = m.get("question", m.get("title", "未知"))[:50]
            outcome = t.get("outcome", "")
            break
    if token_id:
        break

if not token_id:
    print("[✗] 无法获取测试用 token_id")
    sys.exit(1)

print(f"[✓] token_id = {token_id[:20]}...")
print(f"    市场: {title} | outcome: {outcome}")

# ── 查询当前挂单（基线） ────────────────────────────────────────────────────────
print("\n── Step 3: 查询当前开放订单（基线） ────────────────────────────")
pages = client.list_open_orders()
baseline = []
for page in pages:
    baseline.extend(list(page.items))
print(f"[✓] 当前开放订单数: {len(baseline)}")

# ── 下单 0.01（绝对不成交的低价 Maker 挂单） ──────────────────────────────────
print("\n── Step 4: 以 price=0.01 挂 BUY 限价单（post_only） ───────────")
TEST_PRICE = "0.01"
TEST_SIZE  = "5"   # 5 份，Polymarket 最小合法量

resp = client.place_limit_order(
    token_id=token_id,
    side="BUY",
    price=TEST_PRICE,
    size=TEST_SIZE,
    post_only=True,
)

print(f"[i] resp.ok     = {resp.ok}")
print(f"[i] resp.status = {getattr(resp, 'status', 'N/A')}")
print(f"[i] resp.order_id = {getattr(resp, 'order_id', 'N/A')}")

if not resp.ok:
    code = getattr(resp, "code", "N/A")
    msg  = getattr(resp, "message", "N/A")
    print(f"[✗] 下单失败: code={code}  message={msg}")
    sys.exit(1)

order_id = resp.order_id
print(f"[✓] 下单成功  order_id = {order_id}")

# ── 确认订单出现在 open_orders ─────────────────────────────────────────────────
print("\n── Step 5: 确认订单出现在 open_orders ─────────────────────────")
time.sleep(1)
pages = client.list_open_orders()
found = False
for page in pages:
    for o in page.items:
        if o.id == order_id:
            found = True
            print(f"[✓] 在 open_orders 中找到: id={o.id[:20]}... price={o.price} side={o.side} size={o.original_size}")
            break

if not found:
    print(f"[!] open_orders 中未找到 {order_id[:20]}...（可能延迟，继续尝试撤单）")

# ── 撤单 ──────────────────────────────────────────────────────────────────────
print("\n── Step 6: 撤单 ────────────────────────────────────────────────")
cancel_result = client.cancel_order(order_id=order_id)
canceled_list = list(cancel_result.canceled) if cancel_result.canceled else []
not_canceled  = dict(cancel_result.not_canceled) if cancel_result.not_canceled else {}

print(f"[i] canceled     = {canceled_list}")
print(f"[i] not_canceled = {not_canceled}")

if order_id in canceled_list:
    print(f"[✓] 撤单成功")
elif not_canceled.get(order_id):
    print(f"[✗] 撤单失败: {not_canceled[order_id]}")
else:
    print(f"[!] 撤单响应中未找到本订单 ID（可能已自动成交或重复撤单）")

# ── 再次确认订单消失 ───────────────────────────────────────────────────────────
print("\n── Step 7: 确认订单已从 open_orders 中消失 ────────────────────")
time.sleep(1)
pages = client.list_open_orders()
still_open = False
for page in pages:
    for o in page.items:
        if o.id == order_id:
            still_open = True
            break

if still_open:
    print(f"[!] 订单仍在 open_orders（可能延迟，请手动确认）")
else:
    print(f"[✓] 订单已从 open_orders 中消失")

print("\n══════════════════════════════════════════════════")
print(f"  测试完成：下单 ✓  查询 ✓  撤单 {'✓' if not still_open else '?'}")
print("══════════════════════════════════════════════════")
