"""用户频道事件只入队，状态机由主循环独占写入。"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger("guardian.ws_router")


class WSRouter:
    def __init__(self, guardian):
        self._guardian = guardian

    def on_user_message(self, _ws, raw: str):
        if raw == "PONG":
            return
        try:
            self._route_user(json.loads(raw))
        except (ValueError, TypeError) as exc:
            logger.debug("忽略无效用户 WS 消息: %s", exc)

    def _route_user(self, data):
        if isinstance(data, list):
            for item in data:
                self._route_user(item)
            return
        if not isinstance(data, dict):
            return
        event = str(data.get("event_type") or data.get("eventType") or "").lower()
        kind = str(data.get("type") or "").upper()
        if event == "trade" or kind == "TRADE":
            self._guardian.enqueue_user_event("trade", data)
        elif event == "order" or kind in ("PLACEMENT", "UPDATE", "CANCELLATION"):
            self._guardian.enqueue_user_event("order", data)
        elif data.get("channel") == "user":
            self._route_user(data.get("data", []))
