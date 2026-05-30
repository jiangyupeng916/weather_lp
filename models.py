#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据模型 — 枚举、dataclass、类型别名"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, auto
from typing import Any, Dict, Optional


class ActorState(Enum):
    NO_ORDER = auto()
    PLACING = auto()
    RESTING = auto()
    CANCELING = auto()
    COOLING = auto()
    STOPPED = auto()


class EventType(Enum):
    BOOK_SNAPSHOT = auto()
    PRICE_CHANGE = auto()
    BEST_BID = auto()
    TICK_SIZE = auto()
    TRADE_MATCHED = auto()
    RECONNECT = auto()
    STOP = auto()
    AUDIT = auto()
    COOLDOWN_EXPIRED = auto()
    EXTERNAL_CANCEL = auto()
    ORDER_PLACED = auto()
    # 内部事件：异步操作结果回传
    CANCEL_DONE = auto()   # payload: {order_id, ok, reason}
    PLACE_DONE = auto()    # payload: {order_id, price, ok}


@dataclass
class ActorEvent:
    type: EventType
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderInfo:
    order_id: str
    price: float
    size: float
    side: str
    token_id: str
    market: str = ""


@dataclass
class TradeRecord:
    trade_id: str
    asset_id: str
    fill_size: float
    price: float
    outcome: str
    timestamp: float = 0.0


@dataclass
class MarketInfo:
    title: str = "未知"
    outcome: str = ""
    tick_size: Optional[str] = None
    neg_risk: Optional[bool] = None


@dataclass
class PlaceRequest:
    asset_id: str
    price: Decimal
    size: Decimal
    tick_size: Decimal


@dataclass
class CancelRequest:
    order_id: str
    reason: str = ""
