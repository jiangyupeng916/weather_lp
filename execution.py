#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""执行层 — 全局限流 + 幂等 + 线程池

所有 REST 写操作（下单/撤单）通过此层执行，保证：
 - 限流：两次写操作之间至少间隔 exec_interval 秒
 - 幂等：重复的 place/cancel 请求被过滤
 - 异步：返回 Future，不阻塞主循环
 - 价格对齐：下单前自动对齐到 tick_size
 - 批量撤单：DELETE /orders（≤1000）、DELETE /cancel-all
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from decimal import Decimal
from typing import Dict, List, Optional, Set

from py_clob_client_v2 import (
    ClobClient,
    OrderArgs,
    OrderPayload,
    PartialCreateOrderOptions,
    OpenOrderParams,
    MarketOrderArgs,
    OrderType,
)

from config import Config
from utils import safe_float, safe_float_from_decimal, round_to_tick

logger = logging.getLogger("guardian.exec")


class ExecutionLayer:
    """限流 + 幂等 + 线程池执行层。"""

    PLACE_TOKEN_TTL = 300.0
    CANCEL_TOKEN_MAX = 10000
    BATCH_CANCEL_MAX = 1000

    def __init__(self, client: ClobClient, cfg: Config):
        self._client = client
        self._cfg = cfg
        self._cancel_tokens: Set[str] = set()
        self._place_tokens: Dict[str, float] = {}
        self._lock_cancel = threading.Lock()
        self._lock_place = threading.Lock()
        self._lock_rate = threading.Lock()
        self._last_exec = 0.0
        self._executor = ThreadPoolExecutor(
            max_workers=cfg.max_workers, thread_name_prefix="exec"
        )

    def shutdown(self, wait: bool = True):
        self._executor.shutdown(wait=wait)

    def run_async(self, fn, *args, **kwargs) -> Future:
        """在线程池中执行任意函数，返回 Future[result]。

        用于将阻塞操作（如 GET /book）从主线程剥离，避免主循环卡顿。
        """
        fut = Future()
        def _wrapper():
            try:
                fut.set_result(fn(*args, **kwargs))
            except Exception as e:
                fut.set_exception(e)
        self._executor.submit(_wrapper)
        return fut

    # ── 限流 ──────────────────────────────────────────────────────────────────
    def _rate_wait(self):
        with self._lock_rate:
            gap = self._cfg.exec_interval - (time.time() - self._last_exec)
            if gap > 0:
                time.sleep(gap)
            self._last_exec = time.time()

    # ── 单笔撤单 ──────────────────────────────────────────────────────────────
    def _prune_cancel_tokens(self):
        with self._lock_cancel:
            if len(self._cancel_tokens) > self.CANCEL_TOKEN_MAX:
                to_remove = list(self._cancel_tokens)[: self.CANCEL_TOKEN_MAX // 2]
                for oid in to_remove:
                    self._cancel_tokens.discard(oid)

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

        self._executor.submit(self._do_cancel, order_id, reason, fut)
        return fut

    def _do_cancel(self, order_id: str, reason: str, fut: Future):
        self._rate_wait()
        try:
            self._client.cancel_order(OrderPayload(orderID=order_id))
            logger.debug("[CANCEL OK] %s... | %s", order_id[:20], reason)
            fut.set_result(True)
        except Exception as e:
            with self._lock_cancel:
                self._cancel_tokens.discard(order_id)
            logger.error("[CANCEL FAIL] %s... | %s", order_id[:20], e)
            fut.set_result(False)

    # ── 批量撤单（SDK cancel_orders） ──────────────────────────────────────────
    def cancel_batch(self, order_ids: List[str], reason: str = "") -> Future:
        """批量撤单，返回 Future[dict] → {"canceled": [...], "not_canceled": {...}}。"""
        fut: Future = Future()
        if not order_ids:
            fut.set_result({"canceled": [], "not_canceled": {}})
            return fut

        unique = list(dict.fromkeys(order_ids))
        self._executor.submit(self._do_cancel_batch, unique, reason, fut)
        return fut

    def _do_cancel_batch(self, order_ids: List[str], reason: str, fut: Future):
        self._rate_wait()
        try:
            result = self._client.cancel_orders(order_ids)
            canceled = result.get("canceled", [])
            not_canceled = result.get("not_canceled", {})
            logger.debug("[BATCH CANCEL] %d/%d 已取消 | %s",
                        len(canceled), len(order_ids), reason)
            for oid, err in not_canceled.items():
                logger.error("[BATCH CANCEL FAIL] %s... | %s", oid[:20], err)
            fut.set_result({"canceled": canceled, "not_canceled": not_canceled})
        except Exception as e:
            logger.error("[BATCH CANCEL ERR] %d 条 | %s", len(order_ids), e)
            fut.set_result({"canceled": [], "not_canceled": {oid: str(e) for oid in order_ids}})

    # ── 全部撤单（SDK cancel_all） ────────────────────────────────────────────
    def cancel_all(self, reason: str = "") -> Future:
        """取消所有活跃订单，一次 API 调用。返回 Future[bool]。"""
        fut: Future = Future()
        self._executor.submit(self._do_cancel_all, reason, fut)
        return fut

    def _do_cancel_all(self, reason: str, fut: Future):
        self._rate_wait()
        try:
            result = self._client.cancel_all()
            canceled = result.get("canceled", [])
            not_canceled = result.get("not_canceled", {})
            logger.info("[CANCEL ALL] %d 已取消 | %s", len(canceled), reason)
            if not_canceled:
                logger.error("[CANCEL ALL] %d 取消失败: %s", len(not_canceled), not_canceled)
            fut.set_result(True)
        except Exception as e:
            logger.error("[CANCEL ALL ERR] %s | %s", reason, e)
            fut.set_result(False)

    # ── 下单 ──────────────────────────────────────────────────────────────────
    def place(self, asset_id: str, price: Decimal, size: Decimal, tick_size: Decimal) -> Future:
        """异步下单，返回 Future[Optional[str]]（order_id 或 None）。"""
        fut: Future = Future()

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

        post_only_rejected = False
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
                    post_only=True,
                )
                order_id = res.get("orderID") or res.get("order_id")
                if order_id:
                    logger.debug("[PLACE OK] %s... price=%s id=%s",
                                asset_id[:16], price, str(order_id)[:20])
                    fut.set_result(order_id)
                    return
                logger.error("[PLACE FAIL] %s... price=%s attempt=%d/%d | API 返回无 order_id: %s",
                             asset_id[:16], price, attempt, self._cfg.place_retries, res)
            except Exception as e:
                msg = str(e)
                logger.error("[PLACE FAIL] %s... price=%s attempt=%d/%d | %s",
                             asset_id[:16], price, attempt, self._cfg.place_retries, msg)
                if "post-only" in msg.lower() or "crosses" in msg.lower():
                    post_only_rejected = True
                    break
            if attempt < self._cfg.place_retries and not post_only_rejected:
                time.sleep(self._cfg.place_retry_delay)

        # 网络异常兜底：响应丢失不代表订单没挂上。查 open_orders 确认。
        if not post_only_rejected:
            confirmed_id = self._verify_order_placed(asset_id, price)
            if confirmed_id:
                logger.info("[PLACE RECOVERY] %s... price=%s 响应丢失但订单已挂 id=%s",
                            asset_id[:16], price, str(confirmed_id)[:20])
                fut.set_result(confirmed_id)
                return

        with self._lock_place:
            self._place_tokens.pop(token, None)
        fut.set_result(None)

    def _verify_order_placed(self, asset_id: str, price: Decimal) -> Optional[str]:
        """网络异常后查询 open_orders，确认该 asset+price 是否已有挂单。

        防止"API 已下单成功但响应丢失"场景下重复挂单。返回 order_id 或 None。
        """
        try:
            raw = self._client.get_open_orders(OpenOrderParams(asset_id=asset_id))
        except Exception as e:
            logger.error("[PLACE RECOVERY] %s... 查询 open_orders 失败: %s",
                         asset_id[:16], e)
            return None
        if not raw:
            return None
        target = float(price)
        for o in raw:
            if str(o.get("side", "")).upper() != "BUY":
                continue
            if abs(safe_float(o.get("price", 0)) - target) < 1e-9:
                return o.get("id") or o.get("order_id")
        return None

    # ── 市价卖单（FOK） ─────────────────────────────────────────────────────
    def market_sell(self, asset_id: str, size: float, tick_size: Decimal) -> Future:
        """异步市价卖出（FOK，全成或全撤），返回 Future[Optional[str]]（order_id 或 None）。

        size 为持仓份额（shares），SDK 会自动计算市价。FOK 语义保证要么全部成交，
        要么全部撤销，不会产生部分成交残留。
        """
        fut: Future = Future()
        self._executor.submit(self._do_market_sell, asset_id, size, tick_size, fut)
        return fut

    def _do_market_sell(self, asset_id: str, size: float, tick_size: Decimal, fut: Future):
        self._rate_wait()
        try:
            res = self._client.create_and_post_market_order(
                order_args=MarketOrderArgs(
                    token_id=asset_id,
                    amount=size,
                    side="SELL",
                ),
                options=PartialCreateOrderOptions(tick_size=str(tick_size)),
                order_type=OrderType.FOK,
            )
            order_id = res.get("orderID") or res.get("order_id")
            logger.debug("[MARKET SELL OK] %s... size=%s id=%s",
                        asset_id[:16], size, str(order_id)[:20] if order_id else "N/A")
            fut.set_result(order_id)
        except Exception as e:
            logger.error("[MARKET SELL FAIL] %s... size=%s | %s",
                         asset_id[:16], size, e)
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
