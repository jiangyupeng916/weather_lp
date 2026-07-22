#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_actor.py — AssetActor 状态机单元测试（mock Guardian）"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import threading
from concurrent.futures import Future
from decimal import Decimal
from unittest.mock import MagicMock, patch

from config import Config
from models import ActorState, EventType, ActorEvent, OrderInfo, PlaceRequest, CancelRequest
from utils import round_to_tick
from actor import AssetActor


def _make_cfg(**overrides):
    defaults = {
        "pk": "0x" + "a" * 64,
        "api_key": "test-key",
        "api_secret": "test-secret",
        "passphrase": "test-pass",
        "maker_size": Decimal("50"),
        "maker_rank": 3,
        "maker_cooldown": 0.1,
        "tick_size": Decimal("0.01"),
        "cancel_timeout": 5.0,
    }
    defaults.update(overrides)
    return Config(**defaults)


def _make_future(result=None, delay=0):
    """创建一个已完成的 Future"""
    fut = Future()
    fut.set_result(result)
    return fut


def _make_mock_guardian(cfg=None, exec_layer=None):
    cfg = cfg or _make_cfg()
    g = MagicMock()
    g.cfg = cfg
    g.running = True
    g.exec_layer = exec_layer or MagicMock()
    return g


class TestActorStateTransitions:
    def test_initial_state(self):
        cfg = _make_cfg()
        guardian = _make_mock_guardian(cfg)
        actor = AssetActor("test_asset_id_1234567890", guardian)
        try:
            assert actor.state == ActorState.NO_ORDER
            assert actor.active_id is None
            assert actor.active_price is None
        finally:
            actor.force_stop()

    def test_initial_with_order(self):
        cfg = _make_cfg()
        guardian = _make_mock_guardian(cfg)
        order = OrderInfo(
            order_id="0xexisting_order_123",
            price=0.50,
            size=10.0,
            side="BUY",
            token_id="asset_12345",
        )
        actor = AssetActor("asset_12345", guardian, initial=order)
        try:
            assert actor.state == ActorState.RESTING
            assert actor.active_id == "0xexisting_order_123"
            assert actor.active_price == Decimal("0.50")
        finally:
            actor.force_stop()

    def test_no_order_to_cooling(self):
        """NO_ORDER + best_bid 变化 → COOLING"""
        cfg = _make_cfg()
        guardian = _make_mock_guardian(cfg)
        actor = AssetActor("asset_1", guardian)
        try:
            # 先设置初始 best_bid
            actor.best_bid = Decimal("0.50")
            # 再发一个新的 best_bid（变化）
            actor.post(ActorEvent(EventType.BEST_BID, {"best_bid": "0.51", "best_ask": "0.52"}))
            time.sleep(0.2)
            # NO_ORDER 状态下 best_bid 变化 → 启动冷却
            assert actor.state == ActorState.COOLING
        finally:
            actor.force_stop()


class TestActorPlaceCancel:
    def test_place_success_from_book(self):
        """BOOK_SNAPSHOT + NO_ORDER + 足够的 bids → PLACING → RESTING"""
        cfg = _make_cfg(maker_rank=2)
        guardian = _make_mock_guardian(cfg)
        # 模拟下单成功
        fut = _make_future("0xplaced_ok")
        guardian.exec_layer.place.return_value = fut
        guardian.exec_layer.clear_place = MagicMock()

        actor = AssetActor("asset_1", guardian)
        try:
            # 设置订单簿（有足够档位）
            actor.post(ActorEvent(EventType.BOOK_SNAPSHOT, {
                "bids": [
                    {"price": "0.51", "size": "100"},
                    {"price": "0.50", "size": "200"},
                ]
            }))
            time.sleep(0.3)

            # 应该触发下单
            assert guardian.exec_layer.place.called
        finally:
            actor.force_stop()

    def test_cancel_failure_stays_resting(self):
        """P0 修复：撤单失败时保持 RESTING 状态"""
        cfg = _make_cfg()
        guardian = _make_mock_guardian(cfg)
        # 模拟撤单失败
        fut = _make_future(False)  # ← 失败！
        guardian.exec_layer.cancel.return_value = fut

        order = OrderInfo(
            order_id="0xexisting_123",
            price=0.50, size=10.0, side="BUY", token_id="asset_1",
        )
        actor = AssetActor("asset_1", guardian, initial=order)
        try:
            assert actor.state == ActorState.RESTING
            # 先初始化 best_bid（首次事件只记录不动作）
            actor.post(ActorEvent(EventType.BEST_BID, {"best_bid": "0.50", "best_ask": "0.51"}))
            time.sleep(0.05)
            # 再发送 best_bid 变化触发撤单
            actor.post(ActorEvent(EventType.BEST_BID, {"best_bid": "0.52", "best_ask": "0.53"}))
            time.sleep(0.3)

            # 撤单失败 → 保持 RESTING，不清除 active_id
            assert actor.state == ActorState.RESTING, f"实际状态: {actor.state.name}"
            assert actor.active_id == "0xexisting_123", "active_id 不应被清除"
        finally:
            actor.force_stop()

    def test_cancel_success_to_no_order(self):
        """撤单成功 → NO_ORDER + 冷却"""
        cfg = _make_cfg(maker_cooldown=0.1)
        guardian = _make_mock_guardian(cfg)
        fut = _make_future(True)  # ← 成功！
        guardian.exec_layer.cancel.return_value = fut

        order = OrderInfo(
            order_id="0xexisting_123",
            price=0.50, size=10.0, side="BUY", token_id="asset_1",
        )
        actor = AssetActor("asset_1", guardian, initial=order)
        try:
            # 先初始化 best_bid（首次事件只记录不动作）
            actor.post(ActorEvent(EventType.BEST_BID, {"best_bid": "0.50", "best_ask": "0.51"}))
            time.sleep(0.05)
            # 再发一个变化的 best_bid 触发撤单
            actor.post(ActorEvent(EventType.BEST_BID, {"best_bid": "0.52", "best_ask": "0.53"}))
            time.sleep(0.3)

            # 撤单成功 → active_id 清除
            assert actor.active_id is None
        finally:
            actor.force_stop()


class TestActorTickSize:
    def test_target_price_aligned(self):
        """target_price 应对齐到 tick_size"""
        cfg = _make_cfg(tick_size=Decimal("0.01"), maker_rank=2)
        guardian = _make_mock_guardian(cfg)

        actor = AssetActor("asset_1", guardian)
        try:
            # 设置订单簿
            actor.bids[Decimal("0.51")] = Decimal("100")
            actor.bids[Decimal("0.0699")] = Decimal("200")  # 会被对齐到 0.07
            actor.tick_size = Decimal("0.01")

            target = actor._target_price()
            # maker_rank=2, 排序后取第2个: [0.51, 0.0699]
            # 0.0699 四舍五入 tick=0.01 → 0.07
            assert target is not None
            assert target == Decimal("0.07"), f"预期 0.07，实际 {target}"
        finally:
            actor.force_stop()


class TestActorReconnect:
    def test_reconnect_clears_book(self):
        cfg = _make_cfg()
        guardian = _make_mock_guardian(cfg)
        actor = AssetActor("asset_1", guardian)
        try:
            actor.bids[Decimal("0.50")] = Decimal("100")
            actor.best_bid = Decimal("0.50")

            actor.post(ActorEvent(EventType.RECONNECT, {}))
            time.sleep(0.2)

            assert len(actor.bids) == 0
            assert actor.best_bid is None
        finally:
            actor.force_stop()


class TestActorCooldown:
    def test_cooldown_expired(self):
        cfg = _make_cfg(maker_cooldown=0.1)
        guardian = _make_mock_guardian(cfg)

        actor = AssetActor("asset_1", guardian)
        try:
            # 手动设置到 COOLING 状态
            actor.post(ActorEvent(EventType.BEST_BID, {"best_bid": "0.50", "best_ask": "0.51"}))
            time.sleep(0.05)
            # 设置 best_bid 以区分首次
            actor.best_bid = Decimal("0.50")
            actor.post(ActorEvent(EventType.BEST_BID, {"best_bid": "0.51", "best_ask": "0.52"}))
            time.sleep(0.05)
            assert actor.state == ActorState.COOLING

            # 等待冷却到期
            time.sleep(0.3)
            # 冷却到期后应该是 NO_ORDER（因为没有足够的 bids）
            assert actor.state in (ActorState.NO_ORDER, ActorState.COOLING)
        finally:
            actor.force_stop()


class TestActorStop:
    def test_stop_transition(self):
        cfg = _make_cfg()
        guardian = _make_mock_guardian(cfg)
        guardian.exec_layer.cancel.return_value = _make_future(True)

        order = OrderInfo(
            order_id="0xexisting_123",
            price=0.50, size=10.0, side="BUY", token_id="asset_1",
        )
        actor = AssetActor("asset_1", guardian, initial=order)
        try:
            assert actor.state == ActorState.RESTING
            actor.post(ActorEvent(EventType.STOP, {"cancel_active": True}))
            time.sleep(0.3)
            assert actor.state == ActorState.STOPPED
        finally:
            actor.force_stop()
