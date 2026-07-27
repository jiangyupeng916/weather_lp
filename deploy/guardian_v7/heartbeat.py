#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""REST 心跳管理器 — 维持订单存活，防止交易所自动撤单。

CLOB API 要求每 ~10 秒发送心跳，否则所有未成交订单被自动取消。
协议：
 - 首次心跳使用空字符串 heartbeat_id
 - 每次请求包含上一次返回的 heartbeat_id
 - 若发送过期 ID，服务器返回 400 + 正确的 heartbeat_id，更新后重试
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import requests

from config import Config

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
                        self._error_count, self._cfg.heartbeat_interval,
                    )
            # 失败但未达阈值时快速重试（不超过 1s 且不超过 heartbeat_interval），
            # 成功或已到阈值按正常间隔
            if 0 < self._error_count < self._cfg.heartbeat_max_errors:
                time.sleep(min(1.0, self._cfg.heartbeat_interval))
            else:
                time.sleep(self._cfg.heartbeat_interval)

    def _send_heartbeat(self):
        # 优先使用 SDK
        try:
            resp = self._client.post_heartbeat(self._heartbeat_id)
            self._update_id(resp)
            logger.debug("[HEARTBEAT] OK id=%s", str(self._heartbeat_id)[:16])
            return
        except Exception as e:
            # 从 SDK 错误恢复 heartbeat_id（SDK 400 响应体包含正确 ID）
            recovered = self._try_recover_sdk_error(e)
            # 恢复到新 id → 立即用新 id 重试 SDK，避免走 raw fallback 的复杂签名路径
            if recovered:
                try:
                    resp = self._client.post_heartbeat(self._heartbeat_id)
                    self._update_id(resp)
                    logger.info("[HEARTBEAT] OK (recovered) id=%s",
                                str(self._heartbeat_id)[:16])
                    return
                except Exception:
                    pass  # 仍失败则走 raw fallback

        # SDK 失败 → 回退到原始 REST（L2 认证头）
        try:
            resp = self._raw_heartbeat()
            self._update_id(resp)
            logger.debug("[HEARTBEAT] OK (raw) id=%s", str(self._heartbeat_id)[:16])
        except requests.HTTPError as e:
            self._handle_http_error(e)
        except Exception:
            raise

    def _try_recover_sdk_error(self, e: Exception) -> bool:
        """从 SDK 异常中尝试提取 heartbeat_id，返回是否恢复成功。

        SDK PolyApiException 在 400 时 error_msg 属性直接是 resp.json() 得到的 dict
        (见 py_clob_client_v2/exceptions.py)。之前用正则匹配 str(e)，Python dict repr
        使用单引号 → 双引号正则永不匹配。改为优先读 error_msg 属性，字符串正则作回退。
        """
        # 优先：从 PolyApiException.error_msg (dict) 结构化提取
        error_msg = getattr(e, "error_msg", None)
        new_id = ""
        if isinstance(error_msg, dict):
            new_id = str(error_msg.get("heartbeat_id", "") or "")
        # 回退：字符串正则兼容单/双引号
        if not new_id:
            import re
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

    def _handle_http_error(self, e: requests.HTTPError):
        """处理 HTTP 错误响应。
        - 400：服务器返回了正确的 heartbeat_id，更新本地状态
        - 401/403：认证失败，直接抛出
        """
        if e.response is None:
            raise e
        sc = e.response.status_code
        if sc in (401, 403):
            raise e
        if sc != 400:
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
        """使用 L2 认证头直接请求心跳端点。签名必须与 SDK 完全一致
        (py_clob_client_v2.signing.hmac.build_hmac_signature)：
         - secret: base64.urlsafe_b64decode
         - 消息:   ts + method + path + str(body).replace("'", '"')
         - 输出:   base64.urlsafe_b64encode(HMAC-SHA256 digest)
        之前用 .encode() / json.dumps / hexdigest，三处都与 SDK 不一致，
        fallback 从初始化那天起一直 401。
        """
        from py_clob_client_v2.signing.hmac import build_hmac_signature

        path = "/v1/heartbeats"
        url = f"{self._cfg.host}{path}"
        body = {"heartbeat_id": self._heartbeat_id}
        # SDK 用 str(body).replace("'", '"') 作签名 body，HTTP body 必须完全一致
        # （服务器会按接收到的 body 字节重新计算签名）
        body_for_sig = str(body).replace("'", '"')

        ts = str(int(datetime.now(timezone.utc).timestamp()))
        signature = build_hmac_signature(
            self._creds.api_secret, ts, "POST", path, body_for_sig,
        )

        headers = {
            "Content-Type": "application/json",
            "POLY_ADDRESS": self._address,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": ts,
            "POLY_API_KEY": self._creds.api_key,
            "POLY_PASSPHRASE": self._creds.api_passphrase,
        }
        r = requests.post(url, headers=headers, data=body_for_sig, timeout=10)
        r.raise_for_status()
        return r.json()
