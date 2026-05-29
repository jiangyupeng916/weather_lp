#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""WebSocket 连接管理器 — 单线程持久重连 + 心跳

关键改进：
 - 单线程 while 循环处理重连，消除递归线程泄漏
 - 连接生命周期与 message handler 分离
 - 代理支持（HTTP_PROXY/HTTPS_PROXY）
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

import websocket

from config import Config

logger = logging.getLogger("guardian.ws")


class WSManager:
    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._running = False

        # 市场频道
        self._market_ws: Optional[websocket.WebSocketApp] = None
        self._market_thread: Optional[threading.Thread] = None
        self._market_lock = threading.Lock()

        # 用户频道
        self._user_ws: Optional[websocket.WebSocketApp] = None
        self._user_thread: Optional[threading.Thread] = None
        self._user_lock = threading.Lock()

        # 代理配置
        self._ws_kwargs = self._parse_proxy()

    def _parse_proxy(self) -> dict:
        proxy_url = self._cfg.proxy_url
        if not proxy_url:
            return {}
        try:
            from urllib.parse import urlparse
            p = urlparse(proxy_url)
            if p.scheme in ("http", "https") and p.hostname and p.port:
                logger.info("[WS PROXY] 使用代理: %s", proxy_url)
                return {
                    "http_proxy_host": p.hostname,
                    "http_proxy_port": p.port,
                    "proxy_type": "http",
                }
        except Exception as e:
            logger.warning("[WS PROXY] 解析失败: %s", e)
        return {}

    @property
    def running(self) -> bool:
        return self._running

    def start(self):
        self._running = True

    def stop(self):
        self._running = False
        # 关闭 WebSocket 连接
        for ws in (self._market_ws, self._user_ws):
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass

    # ── 市场频道 ──────────────────────────────────────────────────────────────
    def start_market(
        self,
        on_open: Callable[[websocket.WebSocketApp], None],
        on_message: Callable[[websocket.WebSocketApp, str], None],
    ):
        with self._market_lock:
            if self._market_thread and self._market_thread.is_alive():
                return
            self._market_thread = threading.Thread(
                target=self._run_ws,
                args=(
                    self._cfg.ws_market,
                    on_open,
                    on_message,
                    self._cfg.market_ping_interval,
                    "market",
                ),
                daemon=True,
                name="ws-market",
            )
            self._market_thread.start()

    def market_send(self, data: dict):
        ws = self._market_ws
        if ws and ws.sock and getattr(ws.sock, "connected", False):
            try:
                ws.send(json.dumps(data))
            except Exception as e:
                logger.error("[MKT WS] 发送失败: %s", e)

    # ── 用户频道 ──────────────────────────────────────────────────────────────
    def start_user(
        self,
        on_open: Callable[[websocket.WebSocketApp], None],
        on_message: Callable[[websocket.WebSocketApp, str], None],
    ):
        with self._user_lock:
            if self._user_thread and self._user_thread.is_alive():
                return
            self._user_thread = threading.Thread(
                target=self._run_ws,
                args=(
                    self._cfg.ws_user,
                    on_open,
                    on_message,
                    self._cfg.user_ping_interval,
                    "user",
                ),
                daemon=True,
                name="ws-user",
            )
            self._user_thread.start()

    # ── 通用 WS 连接循环（P2 修复：单线程内重连，无泄漏） ──────────────────
    def _run_ws(
        self,
        url: str,
        on_open: Callable,
        on_message: Callable,
        ping_interval: float,
        label: str,
    ):
        """单个持久线程，处理连接 → 断开 → 重连循环。"""
        while self._running:
            app = websocket.WebSocketApp(
                url,
                on_open=lambda ws: self._ws_on_open(ws, on_open, ping_interval),
                on_message=on_message,
                on_error=lambda ws, e: logger.error("[%s WS ERR] %s", label.upper(), e),
                on_close=lambda ws, code, msg: logger.warning(
                    "[%s WS] close code=%s reason=%s", label.upper(), code, msg
                ),
            )

            if label == "market":
                self._market_ws = app
            else:
                self._user_ws = app

            try:
                app.run_forever(**self._ws_kwargs)
            except Exception as e:
                logger.error("[%s WS] run_forever 异常: %s", label.upper(), e)

            if not self._running:
                break

            logger.warning("[%s WS] 断开，%ds 后重连...", label.upper(), self._cfg.ws_reconnect_delay)
            time.sleep(self._cfg.ws_reconnect_delay)

    def _ws_on_open(self, ws, on_open_cb, ping_interval: float):
        # 先执行业务回调（发送 auth/订阅），再启动 ping
        try:
            on_open_cb(ws)
        except Exception as e:
            logger.error("[WS] on_open 异常: %s", e)

        # 启动 ping 线程（先 sleep 再发，避免刚连上就发 PING 导致服务器拒绝）
        def _ping():
            while self._running:
                time.sleep(ping_interval)
                if not self._running:
                    break
                if not ws.sock or not getattr(ws.sock, "connected", False):
                    break
                try:
                    ws.send("PING")
                except Exception:
                    break

        threading.Thread(target=_ping, daemon=True, name="ws-ping").start()
