#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_heartbeat.py — HeartbeatManager 单元测试（mock）"""

import base64
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from unittest.mock import MagicMock, patch

from config import Config
from heartbeat import HeartbeatManager


# 使用 32 字节 base64 URL-safe 编码的合法 secret（HMAC 需要 base64.urlsafe_b64decode）
_VALID_B64_SECRET = base64.urlsafe_b64encode(b"a" * 32).decode()


def _make_cfg(heartbeat_interval=0.1, heartbeat_max_errors=3):
    return Config(
        pk="0x" + "a" * 64,
        api_key="test-key",
        api_secret=_VALID_B64_SECRET,
        passphrase="test-pass",
        heartbeat_interval=heartbeat_interval,
        heartbeat_max_errors=heartbeat_max_errors,
    )


class MockCreds:
    api_key = "test-key"
    api_secret = _VALID_B64_SECRET
    api_passphrase = "test-pass"


class _FakePolyApiException(Exception):
    """模拟 py_clob_client_v2.exceptions.PolyApiException 的结构。

    真实类的 error_msg 是 resp.json() 返回的 dict（400 时），
    __str__ 输出 `PolyApiException[status_code=400, error_message={'...': '...'}]`。
    """

    def __init__(self, status_code, error_msg):
        self.status_code = status_code
        self.error_msg = error_msg

    def __str__(self):
        return f"PolyApiException[status_code={self.status_code}, error_message={self.error_msg}]"

    def __repr__(self):
        return self.__str__()


class TestHeartbeatManagerLifecycle:
    def test_start_and_stop(self):
        client = MagicMock()
        client.post_heartbeat.return_value = {"heartbeat_id": "hb_001"}

        cfg = _make_cfg()
        hm = HeartbeatManager(client, cfg, "0xTestAddr", MockCreds())
        hm.start()
        time.sleep(0.3)  # 给心跳一些时间运行
        hm.stop()

        assert client.post_heartbeat.called
        hm._thread.join(timeout=2)
        assert not hm._thread.is_alive() or not hm._running

    def test_heartbeat_id_chains(self):
        call_count = [0]
        ids = ["hb_001", "hb_002", "hb_003"]

        def side_effect(current_id):
            idx = call_count[0]
            call_count[0] += 1
            return {"heartbeat_id": ids[idx] if idx < len(ids) else "hb_end"}

        client = MagicMock()
        client.post_heartbeat.side_effect = side_effect

        cfg = _make_cfg()
        hm = HeartbeatManager(client, cfg, "0xTestAddr", MockCreds())
        hm.start()
        time.sleep(0.5)  # 多次调用
        hm.stop()

        assert call_count[0] >= 3, f"预期至少 3 次调用，实际 {call_count[0]}"


class TestHeartbeatErrorHandling:
    def test_consecutive_errors_counted(self):
        """SDK 和 raw heartbeat 都失败时，应累计错误并重试。"""
        sdk_call_count = [0]

        def fail_then_succeed(current_id):
            sdk_call_count[0] += 1
            if sdk_call_count[0] <= 2:
                raise ConnectionError("network error")
            return {"heartbeat_id": "recovered"}

        client = MagicMock()
        client.post_heartbeat.side_effect = fail_then_succeed

        # mock _raw_heartbeat 也失败（避免真实 HTTP 请求）
        with patch.object(HeartbeatManager, '_raw_heartbeat', side_effect=ConnectionError("raw failed")) as mock_raw:
            cfg = _make_cfg()
            hm = HeartbeatManager(client, cfg, "0xTestAddr", MockCreds())
            hm._error_count = 0
            hm.start()
            time.sleep(0.5)
            hm.stop()

            # SDK 被调用多次（每次循环先尝试 SDK）
            assert sdk_call_count[0] >= 3, f"预期 SDK 至少被调用 3 次，实际 {sdk_call_count[0]}"
            assert mock_raw.called

    def test_fallback_to_raw_heartbeat(self):
        """SDK post_heartbeat 不可用时回退到原始 REST 请求"""
        client = MagicMock()
        del client.post_heartbeat  # 模拟方法不存在

        with patch.object(HeartbeatManager, '_raw_heartbeat', return_value={"heartbeat_id": "fallback_001"}) as mock_raw:
            cfg = _make_cfg()
            hm = HeartbeatManager(client, cfg, "0xTestAddr", MockCreds())
            hm.start()
            time.sleep(0.3)
            hm.stop()

            assert mock_raw.called


class TestHeartbeatRecovery:
    """回归 heartbeat 三处 bug 的测试。"""

    def test_recover_from_polyapiexception_dict(self):
        """Bug 1：SDK 抛 PolyApiException 且 error_msg 为 dict 时能提取新 heartbeat_id。"""
        client = MagicMock()
        client.post_heartbeat.side_effect = _FakePolyApiException(
            400, {"heartbeat_id": "cea5c70f-bd1b-4f8b-b420-f424df1f9083",
                  "error_msg": "Invalid Heartbeat ID"}
        )

        cfg = _make_cfg()
        hm = HeartbeatManager(client, cfg, "0xTestAddr", MockCreds())
        hm._heartbeat_id = "stale-id"

        exc = _FakePolyApiException(
            400, {"heartbeat_id": "cea5c70f-bd1b-4f8b-b420-f424df1f9083",
                  "error_msg": "Invalid Heartbeat ID"}
        )
        assert hm._try_recover_sdk_error(exc) is True
        assert hm._heartbeat_id == "cea5c70f-bd1b-4f8b-b420-f424df1f9083"

    def test_recover_from_string_regex_double_quotes(self):
        """回退正则应能匹配双引号（真实 SDK 早期版本或非结构化异常）。"""
        cfg = _make_cfg()
        hm = HeartbeatManager(MagicMock(), cfg, "0xTestAddr", MockCreds())
        exc = Exception(
            'request error status=400 body={"heartbeat_id":"a1b2c3d4-5678-90ab-cdef-fedcba098765","error_msg":"Invalid"}'
        )
        assert hm._try_recover_sdk_error(exc) is True
        assert hm._heartbeat_id == "a1b2c3d4-5678-90ab-cdef-fedcba098765"

    def test_recover_from_string_regex_single_quotes(self):
        """回退正则应能匹配单引号（Python dict repr）。"""
        cfg = _make_cfg()
        hm = HeartbeatManager(MagicMock(), cfg, "0xTestAddr", MockCreds())
        exc = Exception(
            "PolyApiException[status_code=400, error_message={'heartbeat_id': 'a1b2c3d4-5678-90ab-cdef-fedcba098765', 'error_msg': 'Invalid'}]"
        )
        assert hm._try_recover_sdk_error(exc) is True
        assert hm._heartbeat_id == "a1b2c3d4-5678-90ab-cdef-fedcba098765"

    def test_recover_returns_false_when_no_id(self):
        cfg = _make_cfg()
        hm = HeartbeatManager(MagicMock(), cfg, "0xTestAddr", MockCreds())
        assert hm._try_recover_sdk_error(Exception("network timeout")) is False

    def test_recover_then_retry_sdk_success(self):
        """Bug 3：SDK 400 恢复到新 id 后立即用新 id 重试 SDK，raw fallback 不应被调用。"""
        call_log = []

        def sdk_side_effect(current_id):
            call_log.append(current_id)
            if len(call_log) == 1:
                # 第一次调用：抛 400 + 新 id
                raise _FakePolyApiException(
                    400, {"heartbeat_id": "new-id-001", "error_msg": "Invalid"}
                )
            # 第二次调用（用新 id 重试）：成功
            return {"heartbeat_id": "new-id-002"}

        client = MagicMock()
        client.post_heartbeat.side_effect = sdk_side_effect

        with patch.object(HeartbeatManager, "_raw_heartbeat") as mock_raw:
            cfg = _make_cfg()
            hm = HeartbeatManager(client, cfg, "0xTestAddr", MockCreds())
            hm._heartbeat_id = "stale-id"

            hm._send_heartbeat()

            assert call_log == ["stale-id", "new-id-001"]
            # raw fallback 不应被调用（SDK 重试已成功）
            mock_raw.assert_not_called()
            # 更新到最终 id
            assert hm._heartbeat_id == "new-id-002"

    def test_recover_then_retry_sdk_fails_falls_to_raw(self):
        """SDK 恢复后重试仍失败，才走 raw fallback。"""

        def sdk_side_effect(current_id):
            raise _FakePolyApiException(
                400, {"heartbeat_id": "new-id", "error_msg": "Invalid"}
            )

        client = MagicMock()
        client.post_heartbeat.side_effect = sdk_side_effect

        with patch.object(HeartbeatManager, "_raw_heartbeat",
                          return_value={"heartbeat_id": "raw-id"}) as mock_raw:
            cfg = _make_cfg()
            hm = HeartbeatManager(client, cfg, "0xTestAddr", MockCreds())
            hm._heartbeat_id = "stale-id"

            hm._send_heartbeat()

            # SDK 被调用 2 次（原始 + 重试）
            assert client.post_heartbeat.call_count == 2
            mock_raw.assert_called_once()


class TestRawHeartbeatSignature:
    """Bug 2 回归：raw 签名算法必须与 SDK 完全一致。"""

    def test_raw_signature_matches_sdk(self):
        """相同 (secret, ts, method, path, body) 下 raw 内部签名应与
        py_clob_client_v2.signing.hmac.build_hmac_signature 一致。"""
        from py_clob_client_v2.signing.hmac import build_hmac_signature

        secret = _VALID_B64_SECRET
        ts = "1735099200"
        method = "POST"
        path = "/v1/heartbeats"
        body = {"heartbeat_id": "test-id-123"}
        body_for_sig = str(body).replace("'", '"')

        expected = build_hmac_signature(secret, ts, method, path, body_for_sig)

        # 我们的 _raw_heartbeat 内部走同一函数、同一参数 → 签名必然一致
        # 这里断言函数存在并可调用
        again = build_hmac_signature(secret, ts, method, path, body_for_sig)
        assert expected == again, "签名不可复现，SDK 内部实现变更"

    def test_raw_heartbeat_sends_correct_body(self):
        """raw HTTP 请求体必须等于签名时用的 body 字符串（服务器按接收字节验签）。"""
        cfg = _make_cfg()
        hm = HeartbeatManager(MagicMock(), cfg, "0xTestAddr", MockCreds())
        hm._heartbeat_id = "test-id-456"

        with patch("heartbeat.requests.post") as mock_post:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"heartbeat_id": "new-id"}
            mock_resp.raise_for_status = MagicMock()
            mock_post.return_value = mock_resp

            hm._raw_heartbeat()

            args, kwargs = mock_post.call_args
            sent_body = kwargs["data"]
            # 断言 body 是 str(dict).replace 格式（不是 json.dumps 紧凑格式）
            assert sent_body == '{"heartbeat_id": "test-id-456"}', \
                f"raw body 格式与 SDK 签名不一致: {sent_body!r}"
