#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_models.py — 数据模型单元测试"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import (
    ActorState, EventType, ActorEvent, OrderInfo,
    TradeRecord, MarketInfo, PlaceRequest, CancelRequest,
)
from decimal import Decimal


class TestActorState:
    def test_enum_values_unique(self):
        values = [s.value for s in ActorState]
        assert len(values) == len(set(values)), "枚举值不唯一"

    def test_expected_states(self):
        names = {s.name for s in ActorState}
        expected = {"NO_ORDER", "PLACING", "RESTING", "CANCELING", "COOLING", "STOPPED"}
        assert names == expected


class TestEventType:
    def test_enum_values_unique(self):
        values = [e.value for e in EventType]
        assert len(values) == len(set(values)), "枚举值不唯一"


class TestActorEvent:
    def test_create(self):
        evt = ActorEvent(EventType.BEST_BID, {"best_bid": "0.50"})
        assert evt.type == EventType.BEST_BID
        assert evt.payload["best_bid"] == "0.50"

    def test_default_payload(self):
        evt = ActorEvent(EventType.STOP)
        assert evt.payload == {}


class TestOrderInfo:
    def test_create(self):
        o = OrderInfo(
            order_id="0xabc123",
            price=0.50,
            size=10.0,
            side="BUY",
            token_id="token_123",
            market="0xmarket",
        )
        assert o.order_id == "0xabc123"
        assert o.price == 0.50
        assert o.side == "BUY"


class TestTradeRecord:
    def test_create(self):
        t = TradeRecord(
            trade_id="tid_1",
            asset_id="asset_1",
            fill_size=5.0,
            price=0.50,
            outcome="Yes",
            timestamp=1234567890.0,
        )
        assert t.trade_id == "tid_1"
        assert t.fill_size == 5.0


class TestMarketInfo:
    def test_defaults(self):
        mi = MarketInfo()
        assert mi.title == "未知"
        assert mi.tick_size is None
        assert mi.neg_risk is None


class TestPlaceRequest:
    def test_create(self):
        pr = PlaceRequest(
            asset_id="asset_1",
            price=Decimal("0.50"),
            size=Decimal("10"),
            tick_size=Decimal("0.01"),
        )
        assert pr.asset_id == "asset_1"
        assert pr.price == Decimal("0.50")


class TestCancelRequest:
    def test_create(self):
        cr = CancelRequest(order_id="0xabc123", reason="best_bid变化")
        assert cr.order_id == "0xabc123"
        assert cr.reason == "best_bid变化"
