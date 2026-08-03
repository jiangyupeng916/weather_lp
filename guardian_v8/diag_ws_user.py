#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""用户频道 WebSocket 活测 — 纯只读，验证成交推送链路是否正常。

做什么：
  1. 用 SecureClient 派生实时凭据（和 bot 完全一致）
  2. 连 wss://.../ws/user，发和 guardian.py 一模一样的订阅帧
     （auth + type=user + initial_dump=True）
  3. 把收到的每一帧原始消息打印出来
  4. 每 10s 发一次 PING（官方要求），跑满时长后自动退出

绝对安全：用户频道只接收推送，本脚本不发任何下单/撤单请求，不碰资金。

用法：
  python diag_ws_user.py [bot1|bot2] [时长秒，默认90]

判读：
  - 连上瞬间因 initial_dump=True，服务端会把当前挂单 dump 回来
    → 立刻看到帧 = WS + 鉴权 100% 正常
  - 若长时间只有 PONG、无任何 dump/order/trade 帧
    → 鉴权或订阅有问题，需进一步查
"""

# ── HTTP/2 禁用补丁（和 main.py 一致，兼容本地 VPN TUN 模式）─────────────────
import httpx as _httpx
_orig = _httpx.Client.__init__
def _no_h2(self, *a, **k):
    k["http2"] = False
    _orig(self, *a, **k)
_httpx.Client.__init__ = _no_h2
# ─────────────────────────────────────────────────────────────────────────────

import sys
import os
import json
import time
import threading
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ── 加载 .env ────────────────────────────────────────────────────────────────
instance = sys.argv[1] if len(sys.argv) > 1 else "bot1"
duration = int(sys.argv[2]) if len(sys.argv) > 2 else 90
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
if not pk:
    print("[✗] PK 未设置")
    sys.exit(1)

WS_USER = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ── 派生凭据 ──────────────────────────────────────────────────────────────────
print(f"[i] 实例={instance}  时长={duration}s")
print("── 初始化 SecureClient（派生实时凭据）──────────────────────")
from polymarket import SecureClient
client = SecureClient.create(private_key=pk, wallet=proxy or None)
creds = client.credentials
print(f"[✓] wallet   = {client.wallet}")
print(f"[✓] api_key  = {creds.key[:8]}...（已脱敏）")
print(f"[✓] secret   = {'(已获取)' if creds.secret else '(空!)'}")
print(f"[✓] passphrase = {'(已获取)' if creds.passphrase else '(空!)'}")

# ── 订阅帧（与 guardian.py run() 完全一致）────────────────────────────────────
SUB_FRAME = {
    "auth": {
        "apiKey": creds.key,
        "secret": creds.secret,
        "passphrase": creds.passphrase,
    },
    "type": "user",
    "markets": [],
    "assets_ids": [],
    "initial_dump": True,
}

import websocket  # websocket-client

_msg_count = 0
_start = time.time()
_stop = threading.Event()


def on_open(ws):
    print(f"\n[{_ts()}] ✓ WS 连接已建立，发送订阅帧...")
    ws.send(json.dumps(SUB_FRAME))
    print(f"[{_ts()}] ✓ 订阅帧已发送（type=user, initial_dump=True）")
    print(f"[{_ts()}] 等待服务端推送（initial_dump 应立即返回当前挂单）...\n")

    def _ping():
        while not _stop.is_set():
            time.sleep(10)
            if _stop.is_set():
                break
            try:
                ws.send("PING")
            except Exception:
                break
    threading.Thread(target=_ping, daemon=True).start()


def on_message(ws, raw):
    global _msg_count
    if raw == "PONG":
        print(f"[{_ts()}] · PONG（心跳正常）")
        return
    _msg_count += 1
    print(f"[{_ts()}] ★ 消息 #{_msg_count}:")
    try:
        data = json.loads(raw)
        # 摘要：如果是数组（initial_dump），逐条打关键字段
        if isinstance(data, list):
            print(f"    [数组，{len(data)} 条]")
            for i, it in enumerate(data):
                et = it.get("event_type") or it.get("type", "?")
                aid = (it.get("asset_id") or "")[:16]
                pr = it.get("price", "?")
                sd = it.get("side", "?")
                st = it.get("status", "?")
                print(f"      {i}: event={et} side={sd} price={pr} status={st} asset={aid}...")
        else:
            et = data.get("event_type") or data.get("type", "?")
            print(f"    event_type={et}")
            print(f"    {json.dumps(data, ensure_ascii=False)[:400]}")
    except Exception:
        print(f"    (非JSON) {raw[:200]}")


def on_error(ws, err):
    print(f"[{_ts()}] ✗ WS 错误: {err}")


def on_close(ws, code, msg):
    print(f"[{_ts()}] WS 关闭 code={code} reason={msg}")


# ── 定时退出 ──────────────────────────────────────────────────────────────────
def _timer(ws):
    time.sleep(duration)
    _stop.set()
    print(f"\n[{_ts()}] 时长到（{duration}s），关闭连接...")
    try:
        ws.close()
    except Exception:
        pass


app = websocket.WebSocketApp(
    WS_USER,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
)
threading.Thread(target=_timer, args=(app,), daemon=True).start()

app.run_forever()

# ── 结论 ──────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print(f"  运行 {int(time.time() - _start)}s | 收到非心跳消息 {_msg_count} 条")
if _msg_count > 0:
    print("  → WS + 鉴权正常。成交推送链路通畅。")
    print("    （之前 trades.log 空，应是观察窗口内无新成交，非 WS 故障）")
else:
    print("  → 全程无推送。若你当前有活跃挂单，initial_dump 却没返回，")
    print("    说明鉴权或订阅有问题，需进一步排查。")
print("=" * 60)
