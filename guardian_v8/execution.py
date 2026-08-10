#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""执行层 — 全局限流 + 幂等 + 线程池（V8：polymarket-client SDK）

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

from config import Config
from utils import safe_float, safe_float_from_decimal, round_to_tick

logger = logging.getLogger("guardian.exec")


class ExecutionLayer:
    """限流 + 幂等 + 线程池执行层。"""

    PLACE_TOKEN_TTL = 300.0
    CANCEL_TOKEN_MAX = 10000
    BATCH_CANCEL_MAX = 1000

    def __init__(self, client, cfg: Config):
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
            # V8: cancel_order(order_id=...) — 不再需要 OrderPayload 包装
            self._client.cancel_order(order_id=order_id)
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
            # V8: cancel_orders(order_ids=[...]) → CancelOrdersResponse 对象（属性访问）
            result = self._client.cancel_orders(order_ids=order_ids)
            canceled = list(result.canceled) if result.canceled else []
            not_canceled = dict(result.not_canceled) if result.not_canceled else {}
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
            # V8: cancel_all() → CancelOrdersResponse 对象（属性访问）
            result = self._client.cancel_all()
            canceled = list(result.canceled) if result.canceled else []
            not_canceled = dict(result.not_canceled) if result.not_canceled else {}
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

        # V8.3: 不再 round——调用方传入的价格已来自订单簿实际档位（市场合法 tick 倍数）。
        # 硬编码 tick 会让 0.001-tick 市场被吸附到 0.01 档（如 0.933→0.93）。
        aligned = price

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

    def _do_place(self, asset_id: str, price: Decimal, size: Decimal,
                  tick_size: Decimal, token: str, fut: Future):
        price_str = str(price)
        size_str = str(size)

        post_only_rejected = False
        for attempt in range(1, self._cfg.place_retries + 1):
            self._rate_wait()
            try:
                # V8: place_limit_order(token_id, side, price, size, post_only)
                # price/size 传字符串，SDK 内部处理精度
                resp = self._client.place_limit_order(
                    token_id=asset_id,
                    side="BUY",
                    price=price_str,
                    size=size_str,
                    post_only=True,
                )
                # V8: OrderResponse 对象，resp.ok 表示成功，resp.order_id 为订单 ID
                if resp.ok and resp.order_id:
                    logger.debug("[PLACE OK] %s... price=%s id=%s",
                                asset_id[:16], price, str(resp.order_id)[:20])
                    fut.set_result(resp.order_id)
                    return
                logger.error("[PLACE FAIL] %s... price=%s attempt=%d/%d | ok=%s status=%s",
                             asset_id[:16], price, attempt, self._cfg.place_retries,
                             resp.ok, getattr(resp, "status", "N/A"))
            except Exception as e:
                msg = str(e)
                logger.error("[PLACE FAIL] %s... price=%s attempt=%d/%d | %s",
                             asset_id[:16], price, attempt, self._cfg.place_retries, msg)
                if "post-only" in msg.lower() or "crosses" in msg.lower() \
                        or "post_only_rejected" in msg.lower():
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

        V8: list_open_orders(token_id=...) 返回 sync paginator，
        每页 page.items 是 OpenOrder 对象元组（属性访问）。
        """
        try:
            pages = self._client.list_open_orders(token_id=asset_id)
        except Exception as e:
            logger.error("[PLACE RECOVERY] %s... 查询 open_orders 失败: %s",
                         asset_id[:16], e)
            return None
        target = float(price)
        try:
            for page in pages:
                for o in page.items:
                    if str(o.side).upper() != "BUY":
                        continue
                    if abs(float(o.price) - target) < 1e-9:
                        return o.id
        except Exception as e:
            logger.error("[PLACE RECOVERY] %s... 遍历 open_orders 失败: %s",
                         asset_id[:16], e)
        return None

    # ── 限价卖单 ────────────────────────────────────────────────────────────
    def limit_sell(self, asset_id: str, size: float, price: Decimal, tick_size: Decimal) -> Future:
        """异步限价卖出，返回 Future[Optional[str]]（order_id 或 None）。"""
        fut: Future = Future()
        self._executor.submit(self._do_limit_sell, asset_id, size, price, tick_size, fut)
        return fut

    def _do_limit_sell(self, asset_id: str, size: float, price: Decimal,
                       tick_size: Decimal, fut: Future):
        price_str = str(price)
        size_str = str(size)
        self._rate_wait()
        try:
            # V8: place_limit_order SELL — 限价卖单不需要 post_only
            resp = self._client.place_limit_order(
                token_id=asset_id,
                side="SELL",
                price=price_str,
                size=size_str,
            )
            order_id = resp.order_id if resp.ok else None
            logger.debug("[LIMIT SELL OK] %s... price=%s size=%s id=%s",
                        asset_id[:16], price, size,
                        str(order_id)[:20] if order_id else "N/A")
            fut.set_result(order_id)
        except Exception as e:
            logger.error("[LIMIT SELL FAIL] %s... price=%s size=%s | %s",
                         asset_id[:16], price, size, e)
            fut.set_result(None)

    # ── 市价卖单（FOK） ─────────────────────────────────────────────────────
    def market_sell(self, asset_id: str, size: float, tick_size: Decimal) -> Future:
        """异步市价卖出（FOK，全成或全撤），返回 Future[Optional[str]]（order_id 或 None）。

        V8: place_market_order(token_id, side, amount, order_type="FOK")
        size 为持仓份额（shares），FOK 保证要么全部成交要么全部撤销。
        """
        fut: Future = Future()
        self._executor.submit(self._do_market_sell, asset_id, size, tick_size, fut)
        return fut

    def _do_market_sell(self, asset_id: str, size: float, tick_size: Decimal, fut: Future):
        self._rate_wait()
        try:
            # V8: place_market_order — amount 为份额，order_type="FOK"
            resp = self._client.place_market_order(
                token_id=asset_id,
                side="SELL",
                amount=size,
                order_type="FOK",
            )
            order_id = resp.order_id if resp.ok else None
            logger.debug("[MARKET SELL OK] %s... size=%s id=%s",
                        asset_id[:16], size,
                        str(order_id)[:20] if order_id else "N/A")
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
