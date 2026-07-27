#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""WebSocket 消息路由器 — 用户频道消息解析和分发"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from guardian import Guardian

logger = logging.getLogger("guardian.ws_router")


class WSRouter:
    """解析原始 WS 消息，通过 Guardian 的线程安全接口分发。"""

    def __init__(self, guardian: "Guardian"):
        self._guardian = guardian

    # ── 用户频道消息 ──────────────────────────────────────────────────────────
    def on_user_message(self, _ws, raw: str):
        if raw == "PONG":
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        self._route_user(data)

    def _route_user(self, data: dict):
        etype = data.get("event_type", "")
        mtype = data.get("type", "")

        if etype == "trade" or mtype == "TRADE":
            self._guardian.handle_trade(data)
        elif etype == "order":
            self._guardian.handle_order(data)
        elif data.get("channel") == "user":
            for t in data.get("data", []):
                if t.get("id") and t.get("side") and t.get("asset_id"):
                    self._guardian.handle_trade(t)
