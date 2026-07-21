#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""WebSocket 消息路由器 — 解析和分发市场/用户频道消息"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from config import Config
from models import EventType, ActorEvent
from utils import safe_decimal, safe_float

if TYPE_CHECKING:
    from guardian import Guardian

logger = logging.getLogger("guardian.ws_router")


class WSRouter:
    """解析原始 WS 消息，通过 Guardian 的线程安全接口分发。"""

    def __init__(self, guardian: "Guardian"):
        self._guardian = guardian

    # ── 市场频道消息 ──────────────────────────────────────────────────────────
    def on_market_message(self, _ws, raw: str):
        if raw == "PONG":
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        for evt in (data if isinstance(data, list) else [data]):
            self._route_market(evt)

    def _route_market(self, evt: dict):
        etype = evt.get("event_type", "")
        aid = evt.get("asset_id", "")
        actor = self._guardian.get_actor(aid)
        if not actor:
            return

        if etype == "book":
            actor.post(ActorEvent(EventType.BOOK_SNAPSHOT, evt))
        elif etype == "price_change":
            for ch in evt.get("price_changes", []):
                a2 = ch.get("asset_id", "")
                ac = self._guardian.get_actor(a2)
                if ac:
                    ac.post(ActorEvent(EventType.PRICE_CHANGE, ch))
        elif etype == "best_bid_ask":
            bb = safe_decimal(evt.get("best_bid"))
            if bb is not None:
                actor.post(ActorEvent(EventType.BEST_BID, {
                    "best_bid": str(bb),
                    "best_ask": evt.get("best_ask"),
                }))
        elif etype == "tick_size_change":
            actor.post(ActorEvent(EventType.TICK_SIZE, {
                "new_tick_size": evt.get("new_tick_size", str(Config.tick_size)),
            }))
        elif etype == "market_resolved":
            logger.warning("[RESOLVED] %s 停止守护", aid[:20])
            if actor:
                actor.stop(cancel_active=True)
            self._guardian.remove_actor(aid)

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
            # 初始 dump：路由所有历史交易到 handle_trade 正常处理
            for t in data.get("data", []):
                if t.get("id") and t.get("side") and t.get("asset_id"):
                    self._guardian.handle_trade(t)
