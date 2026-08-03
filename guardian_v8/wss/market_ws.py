#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MarketWS — Polymarket 市场频道 WebSocket 连接管理。

公开频道，无需认证。
URL: wss://ws-subscriptions-clob.polymarket.com/ws/market

协议要点：
- 初始订阅: {"assets_ids": [...], "type": "market"}
- 动态增订: {"assets_ids": [...], "operation": "subscribe"}
- 动态退订: {"assets_ids": [...], "operation": "unsubscribe"}
- 心跳:     每 PING_INTERVAL 秒发送文本 "PING"，服务器回复 "PONG"
- 事件:     book（全量快照）/ price_change（含 best_bid/best_ask 字段）
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from decimal import Decimal, InvalidOperation
from typing import Callable, List, Optional, Set

import websocket

from .cache import BidCache

logger = logging.getLogger("wss.market_ws")


def _to_decimal(val) -> Optional[Decimal]:
    """安全转 Decimal，失败返回 None。"""
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None


class MarketWS:
    """Polymarket 市场频道 WS 管理器。

    - 自动重连（断线后等待 reconnect_delay 秒）
    - 文本 PING / PONG 心跳
    - 心跳 watchdog：超过 ping_interval×2 无 PONG → 主动关闭触发重连
    - 动态订阅/退订（无需断线重连）
    - 解析 book / price_change 事件 → 更新 BidCache → 触发 on_bid_changed 回调

    回调均在 WS 子线程中被调用，调用方不应在回调中执行耗时操作。
    """

    def __init__(
        self,
        cache: BidCache,
        url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        reconnect_delay: float = 5.0,
        ping_interval: float = 10.0,
        proxy_url: Optional[str] = None,
        # 回调
        on_bid_changed: Optional[Callable[[str, Optional[Decimal], Decimal], None]] = None,
        on_disconnect: Optional[Callable[[], None]] = None,
        on_reconnect: Optional[Callable[[], None]] = None,
    ) -> None:
        self._cache = cache
        self._url = url
        self._reconnect_delay = reconnect_delay
        self._ping_interval = ping_interval
        self._proxy_url = proxy_url

        # 回调
        self._on_bid_changed_cb = on_bid_changed
        self._on_disconnect_cb = on_disconnect
        self._on_reconnect_cb = on_reconnect

        # 连接状态
        self._running = False
        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_lock = threading.Lock()
        self._last_pong: float = 0.0
        self._first_connect = True   # 首次连接不触发 on_reconnect

        # 订阅状态（source of truth）
        self._subscribed_ids: Set[str] = set()
        self._subscribed_ids_lock = threading.Lock()

        # 后台线程
        self._loop_thread: Optional[threading.Thread] = None
        self._watchdog_thread: Optional[threading.Thread] = None

    # ── 生命周期 ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """启动重连循环线程和心跳 watchdog 线程。"""
        if self._running:
            return
        self._running = True
        self._first_connect = True
        self._loop_thread = threading.Thread(
            target=self._run_loop, daemon=True, name="market-ws-loop"
        )
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, daemon=True, name="market-ws-watchdog"
        )
        self._loop_thread.start()
        self._watchdog_thread.start()
        logger.info("[MarketWS] 启动，URL=%s", self._url)

    def stop(self) -> None:
        """停止重连循环，关闭当前连接。"""
        self._running = False
        with self._ws_lock:
            ws = self._ws
        if ws:
            try:
                ws.close()
            except Exception:
                pass
        logger.info("[MarketWS] 已停止")

    def join(self, timeout: float = 5.0) -> None:
        """等待后台线程退出。"""
        if self._loop_thread:
            self._loop_thread.join(timeout=timeout)

    # ── 订阅管理 ─────────────────────────────────────────────────────────────

    def subscribe_initial(self, token_ids: List[str]) -> None:
        """设置初始订阅列表（start() 前调用）。连接后自动发送订阅包。"""
        with self._subscribed_ids_lock:
            self._subscribed_ids.update(token_ids)

    def subscribe_more(self, token_ids: List[str]) -> None:
        """动态增加订阅（连接后立即发送；未连接时仅更新待订阅集合）。"""
        with self._subscribed_ids_lock:
            new_ids = [t for t in token_ids if t not in self._subscribed_ids]
            if not new_ids:
                return
            self._subscribed_ids.update(new_ids)
        logger.debug("[MarketWS] 订阅 +%d 个市场", len(new_ids))
        self._send(json.dumps({"assets_ids": new_ids, "operation": "subscribe"}))

    def unsubscribe(self, token_ids: List[str]) -> None:
        """动态退订（连接后立即发送；未连接时仅更新待订阅集合）。"""
        with self._subscribed_ids_lock:
            removed = [t for t in token_ids if t in self._subscribed_ids]
            for t in removed:
                self._subscribed_ids.discard(t)
        if removed:
            logger.debug("[MarketWS] 退订 %d 个市场", len(removed))
            self._send(json.dumps({"assets_ids": removed, "operation": "unsubscribe"}))
        for t in removed:
            self._cache.remove(t)

    def subscribed_count(self) -> int:
        with self._subscribed_ids_lock:
            return len(self._subscribed_ids)

    def subscribed_ids(self) -> set:
        """返回当前已订阅的 token_id 集合（副本）。"""
        with self._subscribed_ids_lock:
            return set(self._subscribed_ids)

    def is_connected(self) -> bool:
        """当前是否有活跃连接（主线程用于断线检测）。

        _on_open 时置 self._ws=ws，_on_close/_watchdog 超时时置 None，
        均在 _ws_lock 保护下——此处同锁读取，线程安全。
        """
        with self._ws_lock:
            return self._ws is not None

    # ── 内部：发送 ───────────────────────────────────────────────────────────

    def _send(self, msg: str) -> bool:
        """线程安全发送文本消息。连接不存在时返回 False。"""
        with self._ws_lock:
            ws = self._ws
        if ws is None:
            return False
        try:
            ws.send(msg)
            return True
        except Exception as e:
            logger.debug("[MarketWS] send 失败: %s", e)
            return False

    # ── 内部：重连循环 ────────────────────────────────────────────────────────

    def _run_loop(self) -> None:
        while self._running:
            app = websocket.WebSocketApp(
                self._url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            proxy_kwargs = self._build_proxy_kwargs()
            try:
                app.run_forever(**proxy_kwargs)
            except Exception as e:
                logger.error("[MarketWS] run_forever 异常: %s", e)
            if self._running:
                logger.info("[MarketWS] %.1fs 后重连...", self._reconnect_delay)
                time.sleep(self._reconnect_delay)

    def _build_proxy_kwargs(self) -> dict:
        """解析代理 URL，返回 websocket run_forever 所需 kwargs。"""
        proxy_url = self._proxy_url or ""
        if not proxy_url:
            return {}
        try:
            from urllib.parse import urlparse
            p = urlparse(proxy_url)
            return {
                "proxy_type": p.scheme,
                "http_proxy_host": p.hostname,
                "http_proxy_port": p.port,
            }
        except Exception:
            return {}

    # ── 内部：心跳 watchdog ───────────────────────────────────────────────────

    def _watchdog_loop(self) -> None:
        """定期检查 PONG 超时，超时则主动关闭连接触发重连。"""
        while self._running:
            time.sleep(15)
            if not self._running:
                break
            with self._ws_lock:
                ws = self._ws
            if ws is None:
                continue  # 尚未连接或已断线，跳过
            if self._last_pong == 0.0:
                continue  # 还没收到过 PONG，等首次连接稳定
            elapsed = time.time() - self._last_pong
            threshold = self._ping_interval * 2
            if elapsed > threshold:
                logger.warning(
                    "[MarketWS] 心跳超时 %.1f/%.1f s，主动关闭触发重连", elapsed, threshold
                )
                try:
                    ws.close()
                except Exception:
                    pass
                # on_close 会触发 on_disconnect 回调和重连逻辑

    # ── WS 回调 ───────────────────────────────────────────────────────────────

    def _on_open(self, ws) -> None:
        with self._ws_lock:
            self._ws = ws
        self._last_pong = time.time()  # 重置 pong 计时

        # 订阅所有当前市场
        with self._subscribed_ids_lock:
            ids = list(self._subscribed_ids)
        if ids:
            ws.send(json.dumps({"assets_ids": ids, "type": "market"}))
            logger.info("[MarketWS] 已连接，订阅 %d 个市场", len(ids))
        else:
            logger.info("[MarketWS] 已连接（暂无市场，等待动态订阅）")

        # 启动本连接的文本 PING 线程
        threading.Thread(
            target=self._ping_loop,
            args=(ws,),
            daemon=True,
            name="market-ping",
        ).start()

        # 首次连接不触发 on_reconnect（由 guard 在 start() 中完成初始化）
        if not self._first_connect and self._on_reconnect_cb:
            self._on_reconnect_cb()
        self._first_connect = False

    def _on_message(self, ws, raw: str) -> None:
        if raw == "PONG":
            self._last_pong = time.time()
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        self._route(data)

    def _on_error(self, ws, error) -> None:
        logger.error("[MarketWS] 错误: %s", error)

    def _on_close(self, ws, code, msg) -> None:
        with self._ws_lock:
            if self._ws is ws:
                self._ws = None
        logger.warning("[MarketWS] 连接断开 code=%s msg=%s", code, msg)
        if self._running and self._on_disconnect_cb:
            self._on_disconnect_cb()

    def _ping_loop(self, ws) -> None:
        """向服务器发送文本 PING，直到该连接关闭或 running=False。"""
        while self._running:
            time.sleep(self._ping_interval)
            with self._ws_lock:
                current = self._ws
            if current is None or current is not ws:
                break  # 连接已更换或关闭
            try:
                ws.send("PING")
            except Exception:
                break

    # ── 消息路由与解析 ────────────────────────────────────────────────────────

    def _route(self, data) -> None:
        # Polymarket WS 每条消息是 JSON 数组，可能包含多个事件对象
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._route(item)
            return

        etype = data.get("event_type", "")
        if etype == "book":
            self._handle_book(data)
        elif etype == "price_change":
            self._handle_price_change(data)
        # last_trade_price / tick_size_change 等暂不处理

    def _handle_book(self, data: dict) -> None:
        """全量订单簿快照：bids 从低到高排列，bids[-1] 是 best_bid。"""
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return

        bids = data.get("bids", [])
        asks = data.get("asks", [])

        new_bid = _to_decimal(bids[-1].get("price") if bids else None)
        new_ask = _to_decimal(asks[0].get("price") if asks else None)

        if new_bid is None:
            return

        old_bid, changed = self._cache.update(asset_id, new_bid)
        if changed and self._on_bid_changed_cb:
            self._on_bid_changed_cb(asset_id, old_bid, new_bid)

    def _handle_price_change(self, data: dict) -> None:
        """价格变动推送：直接含 best_bid / best_ask 字段。"""
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return

        new_bid = _to_decimal(data.get("best_bid"))
        if new_bid is None:
            return

        old_bid, changed = self._cache.update(asset_id, new_bid)
        if changed and self._on_bid_changed_cb:
            self._on_bid_changed_cb(asset_id, old_bid, new_bid)
