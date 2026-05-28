#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_guardian.py — Guardian 主控制器单元测试（mock 外部依赖）"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from decimal import Decimal
from unittest.mock import MagicMock, patch, PropertyMock

from config import Config
from models import ActorState, EventType, ActorEvent, OrderInfo
from guardian import Guardian


def _make_cfg(**overrides):
    defaults = {
        "pk": "0x" + "a" * 64,
        "api_key": "test-key",
        "api_secret": "test-secret",
        "passphrase": "test-pass",
        "maker_size": Decimal("50"),
        "maker_rank": 3,
        "maker_cooldown": 360.0,
        "balance_retries": 3,
        "balance_delay": 0.01,
        "position_threshold": 1.0,
        "discover_interval": 0.5,
        "audit_interval": 0.5,
        "position_interval": 0.5,
        "cache_prune_interval": 0.5,
        "heartbeat_interval": 0.1,
        "proxy": "",
    }
    defaults.update(overrides)
    return Config(**defaults)


class TestGuardianActorManagement:
    """P0 修复：线程安全的 Actor 管理"""

    def test_add_get_actor(self):
        cfg = _make_cfg()
        # Mock external dependencies
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            mock_actor = MagicMock()
            mock_actor.state = ActorState.RESTING
            mock_actor.active_id = "0x123"

            g.add_actor("asset_1", mock_actor)
            assert g.get_actor("asset_1") is mock_actor
            assert g.actor_count() == 1

    def test_remove_actor(self):
        cfg = _make_cfg()
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            mock_actor = MagicMock()

            g.add_actor("asset_1", mock_actor)
            removed = g.remove_actor("asset_1")
            assert removed is mock_actor
            assert g.get_actor("asset_1") is None
            assert g.actor_count() == 0

    def test_list_actors(self):
        cfg = _make_cfg()
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            a1 = MagicMock()
            a2 = MagicMock()

            g.add_actor("asset_1", a1)
            g.add_actor("asset_2", a2)
            actors = g.list_actors()
            assert len(actors) == 2
            assert a1 in actors
            assert a2 in actors

    def test_list_actor_ids(self):
        cfg = _make_cfg()
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            g.add_actor("asset_a", MagicMock())
            g.add_actor("asset_b", MagicMock())

            ids = g.list_actor_ids()
            assert "asset_a" in ids
            assert "asset_b" in ids


class TestGuardianTradeHandling:
    def test_mark_trade_processed(self):
        cfg = _make_cfg()
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            g.mark_trade_processed("trade_001")
            assert "trade_001" in g._processed_trades

    def test_handle_trade_not_buy_side_ignored(self):
        cfg = _make_cfg()
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            # SELL 方向的成交应被忽略
            g.handle_trade({
                "id": "t_sell",
                "status": "MATCHED",
                "asset_id": "asset_1",
                "side": "SELL",
                "price": "0.50",
                "maker_orders": [{"owner": "test-key", "matched_amount": "5"}],
            })
            # 不应该进入 pending_sells
            assert "t_sell" not in g._pending_sells

    def test_handle_trade_duplicate_ignored(self):
        cfg = _make_cfg()
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            g.mark_trade_processed("trade_dup")
            # 第二次应该被忽略
            g.handle_trade({
                "id": "trade_dup",
                "status": "MATCHED",
                "asset_id": "asset_1",
                "side": "BUY",
                "price": "0.50",
                "maker_orders": [{"owner": "test-key", "matched_amount": "5"}],
            })
            assert "trade_dup" not in g._pending_sells


class TestGuardianCachePrune:
    def test_prune_ob_cache(self):
        cfg = _make_cfg(cache_ttl=0.01)
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            # 添加过期条目
            g._ob_cache["old_key"] = (time.time() - 100, 0.50)
            g._ob_cache["new_key"] = (time.time(), 0.60)

            g._prune_caches()
            assert "old_key" not in g._ob_cache
            assert "new_key" in g._ob_cache

    def test_prune_market_info(self):
        cfg = _make_cfg(cache_max_size=3)
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            for i in range(10):
                g._market_info[f"key_{i}"] = {"title": f"Market {i}"}

            g._prune_caches()
            # 应该被修剪到约一半
            assert len(g._market_info) <= 2  # cache_max_size // 2 = 1


class TestGuardianOrderEvents:
    def test_handle_order_system_cancel(self):
        cfg = _make_cfg()
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            g.exec_layer._system_cancels.add("0xsys_cancel_123")

            # 系统撤单确认
            g.handle_order({
                "type": "CANCELLATION",
                "id": "0xsys_cancel_123",
                "side": "BUY",
                "asset_id": "asset_1",
            })
            # 不应触发 _check_abandon
            assert g.get_actor("asset_1") is None

    def test_handle_order_manual_cancel(self):
        """人工撤单应触发 abandon 检查"""
        cfg = _make_cfg(cooldown_delay=0.01)
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")), \
             patch.object(Guardian, 'open_orders', return_value=[]):
            g = Guardian(cfg)
            mock_actor = MagicMock()
            mock_actor.state = ActorState.RESTING
            mock_actor.active_id = "0xmanual_cancel"
            g.add_actor("asset_1", mock_actor)

            g.handle_order({
                "type": "CANCELLATION",
                "id": "0xmanual_cancel",
                "side": "BUY",
                "asset_id": "asset_1",
            })
            time.sleep(0.2)

            # open_orders 返回空 → 应 abandon
            assert g.get_actor("asset_1") is None


class TestGuardianSellPosition:
    def test_balance_retry_loop(self):
        """P1 修复：余额重试使用配置的次数"""
        cfg = _make_cfg(balance_retries=3, balance_delay=0.01, position_threshold=1.0)
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            call_count = [0]

            def mock_balance(token_id):
                call_count[0] += 1
                return 0.0  # 始终返回 0

            g.onchain_balance = mock_balance

            g.sell_position("asset_1")
            # 余额为 0 时，应重试 balance_retries 次
            assert call_count[0] == cfg.balance_retries, f"预期 {cfg.balance_retries} 次，实际 {call_count[0]}"


class TestGuardianFOKSell:
    def test_sell_zero_size(self):
        cfg = _make_cfg()
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            result = g._sell("asset_1", 0.0)
            assert result is False

    def test_sell_duplicate_ignored(self):
        """同一个 asset 不能并发卖出"""
        cfg = _make_cfg()
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            g._selling.add("asset_1")
            # 已经在 selling 集合中
            result = g._sell("asset_1", 5.0)
            assert result is False
