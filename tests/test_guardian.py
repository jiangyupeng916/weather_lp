#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_guardian.py — Guardian 主控制器单元测试（mock 外部依赖）"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
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
            with g._trade_lock:
                g._processed_trades["trade_dup"] = time.time()
            # 已处理过的 trade 应被忽略
            g.handle_trade({
                "id": "trade_dup",
                "status": "MATCHED",
                "asset_id": "asset_1",
                "side": "BUY",
                "price": "0.50",
                "maker_orders": [{"owner": "test-key", "matched_amount": "5"}],
            })
            assert "trade_dup" not in g._pending_sells

    def test_handle_trade_sell_confirmed_logged(self):
        """Bug #4: 卖单 CONFIRMED 应记录到 trade_logger，不触发卖出动作"""
        cfg = _make_cfg()
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            g.market_info = MagicMock(return_value={"title": "TestMarket", "outcome": "Yes"})

            with patch('guardian.trade_logger') as mock_tl:
                g.handle_trade({
                    "id": "t_sell_ok",
                    "status": "CONFIRMED",
                    "asset_id": "asset_1",
                    "side": "SELL",
                    "price": "0.55",
                    "outcome": "Yes",
                    "maker_orders": [{"owner": "test-key", "matched_amount": "10"}],
                })

                # 应记录到 trade_logger
                assert mock_tl.info.call_count == 1
                logged = json.loads(mock_tl.info.call_args[0][0])
                assert logged["sell_confirmed"]["token_id"] == "asset_1"
                assert logged["sell_confirmed"]["size"] == 10.0
                assert logged["sell_confirmed"]["price"] == 0.55

            # 不应进入 _pending_sells（买单才进）
            assert "t_sell_ok" not in g._pending_sells
            # 应标记为已处理
            assert "t_sell_ok" in g._processed_trades

    def test_handle_trade_matched_dedup(self):
        """Bug #5: MATCHED 事件重放时不应重复处理"""
        cfg = _make_cfg()
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            mock_actor = MagicMock()
            g.add_actor("asset_1", mock_actor)

            trade_data = {
                "id": "t_match",
                "status": "MATCHED",
                "asset_id": "asset_1",
                "side": "BUY",
                "price": "0.50",
                "maker_orders": [{"owner": "test-key", "matched_amount": "5", "order_id": "o1"}],
            }

            # 第一次处理
            g.handle_trade(trade_data)
            assert "t_match" in g._pending_sells
            assert mock_actor.post.call_count == 1

            # WS 重连后重放第二次 → 应跳过
            g.handle_trade(trade_data)
            # post 不应再次被调用
            assert mock_actor.post.call_count == 1


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
        """Bug #11: 非系统撤单统一走 Actor 通知 → 冷却重挂，不直接放弃市场"""
        cfg = _make_cfg(cooldown_delay=0.01)
        with patch('guardian.ClobClient'), \
             patch('guardian.Account.from_key', return_value=MagicMock(address="0xTest")):
            g = Guardian(cfg)
            mock_actor = MagicMock()
            mock_actor.state = ActorState.RESTING
            mock_actor.active_id = "0xext_cancel"
            g.add_actor("asset_1", mock_actor)

            g.handle_order({
                "type": "CANCELLATION",
                "id": "0xext_cancel",
                "side": "BUY",
                "asset_id": "asset_1",
            })

            # Actor 不应被移除，应收到 EXTERNAL_CANCEL 事件进入冷却重挂
            assert g.get_actor("asset_1") is mock_actor
            mock_actor.post.assert_called_once()
            call_args = mock_actor.post.call_args[0][0]
            assert call_args.type == EventType.EXTERNAL_CANCEL
            assert call_args.payload["order_id"] == "0xext_cancel"
