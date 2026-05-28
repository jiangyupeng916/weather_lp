#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_execution.py — ExecutionLayer 单元测试（mock ClobClient）"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from concurrent.futures import Future
from decimal import Decimal
from unittest.mock import MagicMock, patch

from config import Config
from execution import ExecutionLayer
from py_clob_client_v2 import OrderType


def _make_cfg(**overrides):
    defaults = {
        "pk": "0x" + "a" * 64,
        "api_key": "test-key",
        "api_secret": "test-secret",
        "passphrase": "test-pass",
        "exec_interval": 0.01,
        "place_retries": 2,
        "place_retry_delay": 0.01,
        "max_workers": 2,
    }
    defaults.update(overrides)
    return Config(**defaults)


class TestExecutionLayerCancel:
    def test_cancel_success(self):
        client = MagicMock()
        client.cancel_order.return_value = {"canceled": ["0xabc"]}

        cfg = _make_cfg()
        layer = ExecutionLayer(client, cfg)

        fut = layer.cancel("0xabc", "测试")
        result = fut.result(timeout=5)

        assert result is True
        client.cancel_order.assert_called_once()

    def test_cancel_failure(self):
        client = MagicMock()
        client.cancel_order.side_effect = ConnectionError("network down")

        cfg = _make_cfg()
        layer = ExecutionLayer(client, cfg)

        fut = layer.cancel("0xabc", "测试")
        result = fut.result(timeout=5)

        assert result is False

    def test_cancel_empty_order_id(self):
        client = MagicMock()
        cfg = _make_cfg()
        layer = ExecutionLayer(client, cfg)

        fut = layer.cancel("", "测试")
        result = fut.result(timeout=5)

        assert result is False
        client.cancel_order.assert_not_called()

    def test_cancel_idempotent(self):
        client = MagicMock()
        client.cancel_order.return_value = {"canceled": ["0xabc"]}

        cfg = _make_cfg()
        layer = ExecutionLayer(client, cfg)

        fut1 = layer.cancel("0xabc", "第一次")
        r1 = fut1.result(timeout=5)

        fut2 = layer.cancel("0xabc", "第二次-应被去重")
        r2 = fut2.result(timeout=5)

        assert r1 is True
        assert r2 is True
        assert client.cancel_order.call_count == 1  # 第二次被去重

    def test_is_system_cancel(self):
        client = MagicMock()
        client.cancel_order.return_value = {"canceled": ["0xtest"]}

        cfg = _make_cfg()
        layer = ExecutionLayer(client, cfg)

        layer.cancel("0xtest", "test").result(timeout=5)

        # 系统撤单确认后，is_system_cancel 第一次应返回 True
        assert layer.is_system_cancel("0xtest") is True
        # 第二次应返回 False（已消费）
        assert layer.is_system_cancel("0xtest") is False
        # 非系统撤单
        assert layer.is_system_cancel("unknown_id") is False


class TestExecutionLayerPlace:
    def test_place_success(self):
        client = MagicMock()
        client.create_and_post_order.return_value = {"orderID": "0xplaced_001"}

        cfg = _make_cfg()
        layer = ExecutionLayer(client, cfg)

        fut = layer.place("asset_1", Decimal("0.50"), Decimal("10"), Decimal("0.01"))
        oid = fut.result(timeout=5)

        assert oid == "0xplaced_001"

    def test_place_price_alignment(self):
        """价格在提交前会对齐到 tick_size"""
        client = MagicMock()
        client.create_and_post_order.return_value = {"orderID": "0xok"}

        cfg = _make_cfg()
        layer = ExecutionLayer(client, cfg)

        # 传入 0.0699 会被四舍五入到 0.07
        fut = layer.place("asset_1", Decimal("0.0699"), Decimal("10"), Decimal("0.01"))
        fut.result(timeout=5)

        call_args = client.create_and_post_order.call_args
        order_args = call_args[1]["order_args"]
        # 价格应该被对齐为 0.07
        assert order_args.price == 0.07, f"预期 0.07，实际 {order_args.price}"

    def test_place_idempotent(self):
        client = MagicMock()
        client.create_and_post_order.return_value = {"orderID": "0xplaced_001"}

        cfg = _make_cfg()
        layer = ExecutionLayer(client, cfg)

        fut1 = layer.place("asset_1", Decimal("0.50"), Decimal("10"), Decimal("0.01"))
        f1 = fut1.result(timeout=5)

        fut2 = layer.place("asset_1", Decimal("0.50"), Decimal("10"), Decimal("0.01"))
        f2 = fut2.result(timeout=5)

        assert f1 == "0xplaced_001"
        assert f2 is None  # 重复请求返回 None
        assert client.create_and_post_order.call_count == 1

    def test_place_retry_failure(self):
        client = MagicMock()
        client.create_and_post_order.side_effect = Exception("always fail")

        cfg = _make_cfg()
        layer = ExecutionLayer(client, cfg)

        fut = layer.place("asset_1", Decimal("0.50"), Decimal("10"), Decimal("0.01"))
        oid = fut.result(timeout=5)

        assert oid is None
        assert client.create_and_post_order.call_count == cfg.place_retries


class TestExecutionLayerRateLimit:
    def test_rate_wait_enforced(self):
        client = MagicMock()
        client.cancel_order.return_value = {"canceled": ["ok"]}

        cfg = _make_cfg(exec_interval=0.1)
        layer = ExecutionLayer(client, cfg)

        t0 = time.time()
        f1 = layer.cancel("0xa1", "test1")
        f1.result(timeout=5)
        f2 = layer.cancel("0xa2", "test2")
        f2.result(timeout=5)
        elapsed = time.time() - t0

        # 两次操作之间至少间隔 exec_interval
        assert elapsed >= cfg.exec_interval - 0.05, f"elapsed={elapsed}"


class TestExecutionLayerTokenMgmt:
    def test_clear_place(self):
        client = MagicMock()
        client.create_and_post_order.return_value = {"orderID": "0xok"}
        cfg = _make_cfg()
        layer = ExecutionLayer(client, cfg)

        layer.clear_place("asset_1", Decimal("0.50"))
        # 清除后可以重新下单
        fut = layer.place("asset_1", Decimal("0.50"), Decimal("10"), Decimal("0.01"))
        oid = fut.result(timeout=5)
        assert oid == "0xok"

    def test_clear_place_by_asset(self):
        client = MagicMock()
        client.create_and_post_order.return_value = {"orderID": "0xok"}
        cfg = _make_cfg()
        layer = ExecutionLayer(client, cfg)

        # 先下一个单
        layer.place("asset_1", Decimal("0.50"), Decimal("10"), Decimal("0.01")).result(timeout=5)

        # 按资产清除
        layer.clear_place_by_asset("asset_1")

        # 清除后可以重新下单
        fut = layer.place("asset_1", Decimal("0.50"), Decimal("10"), Decimal("0.01"))
        oid = fut.result(timeout=5)
        assert oid == "0xok"
        assert client.create_and_post_order.call_count == 2
