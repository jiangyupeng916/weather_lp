#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""REST 心跳管理器 — 维持订单存活，防止交易所自动撤单。（V8：内联 HMAC，无旧 SDK 依赖）

CLOB API 要求每 ~10 秒发送心跳，否则所有未成交订单被自动取消。
协议：
 - 首次心跳使用空字符串 heartbeat_id
 - 每次请求包含上一次返回的 heartbeat_id
 - 若发送过期 ID，服务器返回 400 + 正确的 heartbeat_id，更新后重试
"""

from __future__ import annotations

import base64
import hashlib
import hmac as _hmac
import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from config import Config

logger = logging.getLogger("guardian.heartbeat")


def _build_hmac_signature(api_secret: str, ts: str, method: str, path: str, body: str) -> str:
    """与 Polymarket CLOB L2 签名完全兼容的 HMAC-SHA256。

    算法：
     1. secret = base64.urlsafe_b64decode(api_secret)  正确补齐填充位
     2. message = ts + method + path + body
     3. digest  = HMAC-SHA256(secret, message.encode("utf-8"))
     4. 返回    = base64.urlsafe_b64encode(digest).decode("ascii")
    """
    padding = (-len(api_secret)) % 4          # 0/1/2/3 个 = 号，而非固定 ==
    secret = base64.urlsafe_b64decode(api_secret + "=" * padding)
    message = ts + method + path + body
    digest = _hmac.new(secret, message.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii")


class HeartbeatManager:
    """REST 心跳管理器，独立守护线程。

    V8 变更：
     - 构造函数移除 creds 参数，改从 cfg 直接读取（api_key/api_secret/passphrase 可选）
     - _raw_heartbeat() 使用内联 HMAC，不再依赖 py_clob_client_v2.signing.hmac
     - 若 cfg 未配置 api 凭据，raw fallback 不可用，仅依赖 SDK
    """

    def __init__(self, client, cfg: Config, address: str):
        self._client = client
        self._cfg = cfg
        self._address = address
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
                        self._error_count, self._cfg.heartbeat_interval,
                    )
            # 失败但未达阈值时快速重试，成功或已到阈值按正常间隔
            if 0 < self._error_count < self._cfg.heartbeat_max_errors:
                time.sleep(min(1.0, self._cfg.heartbeat_interval))
            else:
                time.sleep(self._cfg.heartbeat_interval)

    def _send_heartbeat(self):
        # SecureClient 没有 post_heartbeat()，直接走原始 REST（凭据取自 client.credentials）
        try:
            resp = self._raw_heartbeat()
            self._update_id(resp)
            logger.debug("[HEARTBEAT] OK id=%s", str(self._heartbeat_id)[:16])
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)
        except Exception:
            raise

    def _try_recover_sdk_error(self, e: Exception) -> bool:
        """从 SDK 异常中尝试提取 heartbeat_id，返回是否恢复成功。"""
        # 优先：从异常 error_msg 属性（dict）结构化提取
        error_msg = getattr(e, "error_msg", None)
        new_id = ""
        if isinstance(error_msg, dict):
            new_id = str(error_msg.get("heartbeat_id", "") or "")
        # 回退：字符串正则兼容单/双引号
        if not new_id:
            m = re.search(r"""['"]heartbeat_id['"]\s*:\s*['"]([a-f0-9-]+)['"]""", str(e))
            if m:
                new_id = m.group(1)
        if not new_id:
            return False
        with self._lock:
            old = self._heartbeat_id
            self._heartbeat_id = new_id
        logger.info(
            "[HEARTBEAT] 从 SDK 错误恢复 heartbeat_id: %s (旧: %s)",
            new_id[:16], old[:16] if old else "空",
        )
        return True

    def _handle_http_error(self, e: httpx.HTTPStatusError):
        """处理 HTTP 错误响应。400 含正确 heartbeat_id，401/403 直接抛出。"""
        if e.response is None:
            raise e
        sc = e.response.status_code
        if sc in (401, 403) or sc != 400:
            raise e
        try:
            body = e.response.json()
            correct_id = body.get("heartbeat_id", "")
        except Exception:
            raise e

        if correct_id:
            with self._lock:
                old = self._heartbeat_id
                self._heartbeat_id = correct_id
            logger.info(
                "[HEARTBEAT] 从 %d 响应恢复 heartbeat_id: %s (旧: %s)",
                sc, correct_id[:16], old[:16] if old else "空",
            )
        else:
            raise e

    def _update_id(self, resp: dict):
        new_id = resp.get("heartbeat_id", "") if isinstance(resp, dict) else ""
        with self._lock:
            self._heartbeat_id = new_id or self._heartbeat_id

    def _raw_heartbeat(self) -> dict:
        """使用 L2 认证头直接请求心跳端点（内联 HMAC，不依赖旧 SDK）。

        签名规则（与 CLOB L2 auth 完全一致）：
         - body_for_sig = str({"heartbeat_id": "..."}).replace("'", '"')
         - HTTP body 与签名 body 完全相同（服务器会重新计算验证）
        """
        path = "/v1/heartbeats"
        url = f"{self._cfg.host}{path}"
        body = {"heartbeat_id": self._heartbeat_id}
        # SDK 用 str(body).replace("'", '"') 作签名 body，HTTP body 必须完全一致
        body_for_sig = str(body).replace("'", '"')

        ts = str(int(datetime.now(timezone.utc).timestamp()))
        creds = self._client.credentials   # ApiKeyCreds: .key / .secret / .passphrase
        # L2 认证中 POLY_ADDRESS 必须是签名者的 EOA 地址（signer），
        # 不是 Proxy 合约地址（wallet）。SDK _make_l2_header_resolver_sync 同样用 signer.address。
        signer_address = str(self._client.signer)
        signature = _build_hmac_signature(
            creds.secret, ts, "POST", path, body_for_sig,
        )

        headers = {
            "Content-Type": "application/json",
            "POLY_ADDRESS": signer_address,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": ts,
            "POLY_API_KEY": creds.key,
            "POLY_PASSPHRASE": creds.passphrase,
        }
        r = httpx.post(url, headers=headers, content=body_for_sig, timeout=10)
        r.raise_for_status()
        return r.json()
