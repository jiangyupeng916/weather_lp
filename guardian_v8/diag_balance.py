#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""余额查询诊断 — 证明卖出失效根因。

对同一个真实持仓 token，对比两种查询方式：
  A) 旧实现：手搓 REST /balance-allowance，只发 POLY_ADDRESS 头
  B) 新实现：SDK client.get_balance_allowance（内部签完整 L2 头）

用法: python diag_balance.py [bot1|bot2]
"""

import os
import sys

import requests

# ── 加载 .env（与 test_sdk.py 一致）──────────────────────────────────────────
instance = sys.argv[1] if len(sys.argv) > 1 else "bot1"
env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f".env.{instance}")

from dotenv import load_dotenv
if os.path.exists(env_file):
    load_dotenv(env_file)
    print(f"[✓] 加载 {env_file}")
else:
    print(f"[✗] 找不到 {env_file}")
    sys.exit(1)

pk    = os.environ.get("PK", "")
proxy = os.environ.get("PROXY_ADDRESS", "")
HOST  = os.environ.get("CLOB_API_URL", "https://clob.polymarket.com")
DATA  = "https://data-api.polymarket.com"

if not pk:
    print("[✗] PK 未设置")
    sys.exit(1)

# ── 初始化 SecureClient ──────────────────────────────────────────────────────
from polymarket import SecureClient
client = SecureClient.create(private_key=pk, wallet=proxy or None)
address = client.wallet
print(f"[i] 实例={instance}  地址={address}")

# ── 取一个真实持仓的 token_id ────────────────────────────────────────────────
r = requests.get(
    f"{DATA}/positions",
    params={"user": address, "sizeThreshold": 1.0},
    timeout=10,
)
positions = r.json() if r.status_code == 200 else []
print(f"[i] data-api 返回持仓数: {len(positions)}")

if not positions:
    print("[✗] 无持仓，无法诊断")
    sys.exit(1)

p = positions[0]
tid   = p.get("asset", "")
size  = p.get("size", "?")
title = p.get("title", "?")[:40]
entry = p.get("avgPrice", "?")
print(f"[i] 测试持仓: {title}")
print(f"    token_id={tid[:24]}...  size={size}  avgPrice={entry}")

print("\n" + "=" * 60)
print("A) 旧实现：手搓 REST，只发 POLY_ADDRESS")
print("=" * 60)
try:
    ra = requests.get(
        f"{HOST}/balance-allowance",
        params={"asset_type": "CONDITIONAL", "token_id": tid},
        headers={"POLY_ADDRESS": address},
        timeout=10,
    )
    print(f"   HTTP 状态: {ra.status_code}")
    body = ra.text[:300]
    print(f"   响应体: {body}")
    if ra.status_code != 200:
        print("   → 旧代码在此 return 0.0（静默，无日志）→ 卖出被跳过")
    else:
        j = ra.json()
        print(f"   → balance={j.get('balance')}")
except Exception as e:
    print(f"   异常: {e}")

print("\n" + "=" * 60)
print("B) 新实现：SDK get_balance_allowance（签完整 L2 头）")
print("=" * 60)
try:
    ba = client.get_balance_allowance(asset_type="CONDITIONAL", token_id=tid)
    print(f"   balance(base units) = {ba.balance}")
    print(f"   balance(shares)     = {ba.balance / 1_000_000}")
    print(f"   → 修复后 onchain_balance 返回此值，卖出可正常触发")
except Exception as e:
    print(f"   异常: {e}")

print("\n" + "=" * 60)
print("结论：若 A 非 200 而 B 返回真实份额，则根因确认。")
print("=" * 60)
