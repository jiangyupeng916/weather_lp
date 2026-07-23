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

import hashlib
import hmac
import json
import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Set

import requests

from py_clob_client_v2 import (
    ClobClient,
    OrderArgs,
    OrderPayload,
    PartialCreateOrderOptions,
)

from config import Config
from utils import safe_float_from_decimal, round_to_tick

logger = logging.getLogger("guardian.exec")


class ExecutionLayer:
    """限流 + 幂等 + 线程池执行层。"""

    PLACE_TOKEN_TTL = 300.0
    CANCEL_TOKEN_MAX = 10000
    BATCH_CANCEL_MAX = 1000

    def __init__(self, client: ClobClient, cfg: Config, signer_address: str, creds):
        self._client = client
        self._cfg = cfg
        self._signer_address = signer_address
        self._api_creds = creds
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

    # ── L2 签名 ────────────────────────────────────────────────────────────────
    def _l2_headers(self, method: str, path: str, body: str) -> dict:
        ts = str(int(datetime.now(timezone.utc).timestamp()))
        sig_msg = ts + method + path + body
        signature = hmac.new(
            self._api_creds.api_secret.encode(),
            sig_msg.encode(),
            hashlib.sha256,
        ).hexdigest()
        return {
            "Content-Type": "application/json",
            "POLY_ADDRESS": self._signer_address,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": ts,
            "POLY_API_KEY": self._api_creds.api_key,
            "POLY_PASSPHRASE": self._api_creds.api_passphrase,
        }

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
            logger.info("[CANCEL OK] %s... | %s", order_id[:20], reason)
            fut.set_result(True)
        except Exception as e:
            with self._lock_cancel:
                self._cancel_tokens.discard(order_id)
            logger.error("[CANCEL FAIL] %s... | %s", order_id[:20], e)
            fut.set_result(False)

    # ── 批量撤单（DELETE /orders，≤1000） ─────────────────────────────────────
    def cancel_batch(self, order_ids: List[str], reason: str = "") -> Future:
        """批量撤单，返回 Future[dict] → {"canceled": [...], "not_canceled": {...}}。"""
        fut: Future = Future()
        if not order_ids:
            fut.set_result({"canceled": [], "not_canceled": {}})
            return fut

        unique = list(dict.fromkeys(order_ids))  # 去重保序
        self._executor.submit(self._do_cancel_batch, unique, reason, fut)
        return fut

    def _do_cancel_batch(self, order_ids: List[str], reason: str, fut: Future):
        self._rate_wait()
        all_canceled = []
        all_not_canceled = {}
        for chunk in [order_ids[i:i + self.BATCH_CANCEL_MAX]
                      for i in range(0, len(order_ids), self.BATCH_CANCEL_MAX)]:
            try:
                path = "/orders"
                url = f"{self._cfg.host}{path}"
                serialized = json.dumps(chunk, separators=(",", ":"))
                headers = self._l2_headers("DELETE", path, serialized)
                r = requests.delete(url, headers=headers, data=serialized, timeout=15)
                r.raise_for_status()
                result = r.json()
                canceled = result.get("canceled", [])
                not_canceled = result.get("not_canceled", {})
                all_canceled.extend(canceled)
                all_not_canceled.update(not_canceled)
                logger.info("[BATCH CANCEL] %d/%d 已取消 | %s",
                            len(canceled), len(chunk), reason)
                for oid, err in not_canceled.items():
                    logger.error("[BATCH CANCEL FAIL] %s... | %s", oid[:20], err)
            except Exception as e:
                logger.error("[BATCH CANCEL ERR] chunk %d 条 | %s", len(chunk), e)
                for oid in chunk:
                    all_not_canceled[oid] = str(e)
        fut.set_result({"canceled": all_canceled, "not_canceled": all_not_canceled})

    # ── 全部撤单（DELETE /cancel-all） ────────────────────────────────────────
    def cancel_all(self, reason: str = "") -> Future:
        """取消所有活跃订单，一次 API 调用。返回 Future[bool]。"""
        fut: Future = Future()
        self._executor.submit(self._do_cancel_all, reason, fut)
        return fut

    def _do_cancel_all(self, reason: str, fut: Future):
        self._rate_wait()
        try:
            path = "/cancel-all"
            url = f"{self._cfg.host}{path}"
            headers = self._l2_headers("DELETE", path, "")
            r = requests.delete(url, headers=headers, timeout=15)
            r.raise_for_status()
            result = r.json()
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
                    logger.info("[PLACE OK] %s... price=%s id=%s",
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

        with self._lock_place:
            self._place_tokens.pop(token, None)
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
