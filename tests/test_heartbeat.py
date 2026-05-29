#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_heartbeat.py — HeartbeatManager 单元测试（mock）"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import threading
import time
from unittest.mock import MagicMock, patch, PropertyMock

from config import Config
from heartbeat import HeartbeatManager


def _make_cfg(heartbeat_interval=0.1, heartbeat_max_errors=3):
    return Config(
        pk="0x" + "a" * 64,
        api_key="test-key",
        api_secret="test-secret",
        passphrase="test-pass",
        heartbeat_interval=heartbeat_interval,
        heartbeat_max_errors=heartbeat_max_errors,
    )


class MockCreds:
    api_key = "test-key"
    api_secret = "test-secret"
    api_passphrase = "test-pass"


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
