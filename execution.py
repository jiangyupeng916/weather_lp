#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""执行层 — 全局限流 + 幂等 + 线程池

所有 REST 写操作（下单/撤单）通过此层执行，保证：
 - 限流：两次写操作之间至少间隔 exec_interval 秒
 - 幂等：重复的 place/cancel 请求被过滤
 - 异步：返回 Future，不阻塞 Actor 事件循环
 - 价格对齐：下单前自动对齐到 tick_size
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from decimal import Decimal
from typing import Dict, Optional, Set

from py_clob_client_v2 import (
    ClobClient,
    OrderArgs,
    MarketOrderArgs,
    OrderPayload,
    PartialCreateOrderOptions,
    OrderType,
)

from config import Config
from models import PlaceRequest, CancelRequest
from utils import safe_float_from_decimal, round_to_tick

logger = logging.getLogger("guardian.exec")


class ExecutionLayer:
    """限流 + 幂等 + 线程池执行层。"""

    PLACE_TOKEN_TTL = 300.0
    CANCEL_TOKEN_MAX = 10000

    def __init__(self, client: ClobClient, cfg: Config):
        self._client = client
        self._cfg = cfg
        self._cancel_tokens: Set[str] = set()
        self._place_tokens: Dict[str, float] = {}
        self._system_cancels: Set[str] = set()
        self._lock_cancel = threading.Lock()
        self._lock_place = threading.Lock()
        self._lock_rate = threading.Lock()
        self._last_exec = 0.0
        self._executor = ThreadPoolExecutor(
            max_workers=cfg.max_workers, thread_name_prefix="exec"
        )

    def shutdown(self, wait: bool = True):
        self._executor.shutdown(wait=wait)

    # ── 限流 ──────────────────────────────────────────────────────────────────
    def _rate_wait(self):
        with self._lock_rate:
            gap = self._cfg.exec_interval - (time.time() - self._last_exec)
            if gap > 0:
                time.sleep(gap)
            self._last_exec = time.time()

    # ── 撤单 ──────────────────────────────────────────────────────────────────
    def _prune_cancel_tokens(self):
        with self._lock_cancel:
            if len(self._cancel_tokens) > self.CANCEL_TOKEN_MAX:
                to_remove = list(self._cancel_tokens)[: self.CANCEL_TOKEN_MAX // 2]
                for oid in to_remove:
                    self._cancel_tokens.discard(oid)
                    self._system_cancels.discard(oid)

    def cancel(self, order_id: str, reason: str = "") -> Future:
        """异步撤单，返回 Future[bool]。"""
        fut: Future = Future()

        if not order_id:
            fut.set_result(False)
            return fut

        self._prune_cancel_tokens()
        with self._lock_cancel:
            if order_id in self._cancel_tokens:
                fut.set_result(True)
                return fut
            self._cancel_tokens.add(order_id)
            self._system_cancels.add(order_id)

        self._executor.submit(self._do_cancel, order_id, reason, fut)
        return fut

    def _do_cancel(self, order_id: str, reason: str, fut: Future):
        self._rate_wait()
        try:
            self._client.cancel_order(OrderPayload(orderID=order_id))
            logger.info("[CANCEL OK] %s... | %s", order_id[:20], reason)
            fut.set_result(True)
        except Exception as e:
            with self._lock_cancel:
                self._cancel_tokens.discard(order_id)
                self._system_cancels.discard(order_id)
            logger.error("[CANCEL FAIL] %s... | %s", order_id[:20], e)
            fut.set_result(False)

    # ── 下单 ──────────────────────────────────────────────────────────────────
    def place(self, asset_id: str, price: Decimal, size: Decimal, tick_size: Decimal) -> Future:
        """异步下单，返回 Future[Optional[str]]（order_id 或 None）。"""
        fut: Future = Future()

        # 价格对齐到 tick_size
        aligned = round_to_tick(price, tick_size)

        token = f"place:{asset_id}:{aligned}"
        with self._lock_place:
            now = time.time()
            expired = [t for t, ts in self._place_tokens.items() if now - ts > self.PLACE_TOKEN_TTL]
            for t in expired:
                self._place_tokens.pop(t, None)
            if token in self._place_tokens:
                fut.set_result(None)
                return fut
            self._place_tokens[token] = now

        self._executor.submit(self._do_place, asset_id, aligned, size, tick_size, token, fut)
        return fut

    def _do_place(self, asset_id: str, price: Decimal, size: Decimal, tick_size: Decimal, token: str, fut: Future):
        price_f = safe_float_from_decimal(price)
        size_f = safe_float_from_decimal(size)

        for attempt in range(1, self._cfg.place_retries + 1):
            self._rate_wait()
            try:
                res = self._client.create_and_post_order(
                    order_args=OrderArgs(
                        token_id=asset_id,
                        price=price_f,
                        size=size_f,
                        side="BUY",
                    ),
                    options=PartialCreateOrderOptions(tick_size=str(tick_size)),
                )
                order_id = res.get("orderID") or res.get("order_id")
                logger.info("[PLACE OK] %s... price=%s id=%s", asset_id[:16], price, str(order_id)[:20] if order_id else "N/A")
                fut.set_result(order_id)
                return
            except Exception as e:
                logger.error("[PLACE FAIL] %s... price=%s attempt=%d/%d | %s", asset_id[:16], price, attempt, self._cfg.place_retries, e)
                if attempt < self._cfg.place_retries:
                    time.sleep(self._cfg.place_retry_delay)
                else:
                    with self._lock_place:
                        self._place_tokens.pop(token, None)
                    fut.set_result(None)

    # ── 市价卖出 ──────────────────────────────────────────────────────────────
    def market_sell(self, asset_id: str, amount: float, order_type: OrderType) -> Future:
        """异步市价卖出，返回 Future[Optional[dict]]。"""
        fut: Future = Future()
        self._executor.submit(self._do_market_sell, asset_id, amount, order_type, fut)
        return fut

    def _do_market_sell(self, asset_id: str, amount: float, order_type: OrderType, fut: Future):
        self._rate_wait()
        try:
            res = self._client.create_and_post_market_order(
                order_args=MarketOrderArgs(token_id=asset_id, amount=amount, side="SELL"),
                options=PartialCreateOrderOptions(),
                order_type=order_type,
            )
            fut.set_result(res)
        except Exception as e:
            logger.error("[SELL FAIL] %s... | %s", asset_id[:20], e)
            fut.set_result(None)

    # ── 限价卖单 ──────────────────────────────────────────────────────────────
    def limit_sell(self, asset_id: str, price: float, size: float, tick_size: Decimal) -> Future:
        """异步下限价卖单，返回 Future[Optional[str]]（order_id 或 None）。"""
        fut: Future = Future()
        self._executor.submit(self._do_limit_sell, asset_id, price, size, tick_size, fut)
        return fut

    def _do_limit_sell(self, asset_id: str, price: float, size: float, tick_size: Decimal, fut: Future):
        self._rate_wait()
        try:
            res = self._client.create_and_post_order(
                order_args=OrderArgs(
                    token_id=asset_id,
                    price=price,
                    size=size,
                    side="SELL",
                ),
                options=PartialCreateOrderOptions(tick_size=str(tick_size)),
            )
            order_id = res.get("orderID") or res.get("order_id")
            logger.info("[LIMIT SELL OK] %s... price=%s size=%s id=%s",
                        asset_id[:16], price, size, str(order_id)[:20] if order_id else "N/A")
            fut.set_result(order_id)
        except Exception as e:
            logger.error("[LIMIT SELL FAIL] %s... price=%s size=%s | %s",
                         asset_id[:16], price, size, e)
            fut.set_result(None)

    # ── 令牌管理 ──────────────────────────────────────────────────────────────
    def clear_place(self, asset_id: str, price: Decimal):
        with self._lock_place:
            self._place_tokens.pop(f"place:{asset_id}:{price}", None)

    def clear_place_by_asset(self, asset_id: str):
        with self._lock_place:
            prefix = f"place:{asset_id}:"
            to_remove = [t for t in self._place_tokens if t.startswith(prefix)]
            for t in to_remove:
                self._place_tokens.pop(t, None)

    def is_system_cancel(self, order_id: str) -> bool:
        """判断是否为系统发起的撤单，若是则移除令牌并返回 True。"""
        self._prune_cancel_tokens()
        with self._lock_cancel:
            if order_id in self._system_cancels:
                self._system_cancels.discard(order_id)
                return True
            return False
