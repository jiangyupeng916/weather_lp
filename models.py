#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据模型 — 枚举、dataclass、类型别名"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict


class ActorState(Enum):
    NO_ORDER = auto()
    PLACING = auto()
    RESTING = auto()
    CANCELING = auto()
    COOLING = auto()
    STOPPED = auto()


class EventType(Enum):
    BEST_BID = auto()       # 轮询检测到 best_bid 变化
    STOP = auto()
    AUDIT = auto()
    COOLDOWN_EXPIRED = auto()
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




