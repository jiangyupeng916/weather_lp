#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""REST 心跳管理器 — 维持订单存活，防止交易所自动撤单。

CLOB API 要求每 ~10 秒发送心跳，否则所有未成交订单被自动取消。
本模块每 heartbeat_interval 秒调用一次 POST /heartbeat。
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Optional

import requests

from config import Config
from utils import retry_call

logger = logging.getLogger("guardian.heartbeat")


class HeartbeatManager:
    """REST 心跳管理器，独立守护线程。"""

    def __init__(self, client, cfg: Config, address: str, creds):
        self._client = client
        self._cfg = cfg
        self._address = address
        self._creds = creds
        self._heartbeat_id: str = ""
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._error_count = 0
        self._lock = threading.Lock()

    @property
    def heartbeat_id(self) -> str:
        with self._lock:
            return self._heartbeat_id

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="heartbeat")
        self._thread.start()
        logger.info("[HEARTBEAT] 已启动 interval=%.1fs", self._cfg.heartbeat_interval)

    def stop(self):
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info("[HEARTBEAT] 已停止")

    def _loop(self):
        while self._running:
            try:
                self._send_heartbeat()
                self._error_count = 0
            except Exception as e:
                self._error_count += 1
                logger.error("[HEARTBEAT] 失败 (连续 %d 次): %s", self._error_count, e)
                if self._error_count >= self._cfg.heartbeat_max_errors:
                    logger.critical(
                        "[HEARTBEAT] 连续失败 %d 次！订单将在 ~%d 秒后被交易所自动取消！",
                        self._error_count, self._cfg.heartbeat_interval
                    )
            time.sleep(self._cfg.heartbeat_interval)

    def _send_heartbeat(self):
        try:
            resp = self._client.post_heartbeat(self._heartbeat_id)
        except Exception:
            resp = self._raw_heartbeat()

        new_id = resp.get("heartbeat_id", "") if isinstance(resp, dict) else ""
        with self._lock:
            self._heartbeat_id = new_id or self._heartbeat_id
        logger.debug("[HEARTBEAT] OK id=%s", str(self._heartbeat_id)[:16])

    def _raw_heartbeat(self) -> dict:
        """SDK post_heartbeat 不可用时的回退方案，使用 L2 认证头直接请求。"""
        import hmac
        import hashlib
        from datetime import timezone as tz_utc
        from datetime import datetime

        url = f"{self._cfg.host}/heartbeat"
        body = {"heartbeat_id": self._heartbeat_id}
        serialized = json.dumps(body, separators=(",", ":"), ensure_ascii=False)

        ts = str(int(datetime.now(tz_utc).timestamp()))
        sig_msg = ts + "POST" + "/heartbeat" + serialized
        signature = hmac.new(
            self._creds.api_secret.encode(),
            sig_msg.encode(),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "Content-Type": "application/json",
            "POLY_ADDRESS": self._address,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": ts,
            "POLY_API_KEY": self._creds.api_key,
            "POLY_PASSPHRASE": self._creds.api_passphrase,
        }
        r = requests.post(url, headers=headers, data=serialized, timeout=10)
        r.raise_for_status()
        return r.json()
