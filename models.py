#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据模型 — 枚举、dataclass、类型别名"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, auto
from typing import Optional


class ActorState(Enum):
    NO_ORDER = auto()
    PLACING = auto()
    RESTING = auto()
    CANCELING = auto()
    COOLING = auto()
    STOPPED = auto()


@dataclass
class MarketState:
    """集中式市场状态，主线程直读直写，无需锁。"""
    state: ActorState = ActorState.NO_ORDER
    state_at: float = field(default_factory=time.time)
    active_id: Optional[str] = None
    active_price: Optional[Decimal] = None
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    cooldown_until: float = 0.0


@dataclass
class OrderInfo:
    order_id: str
    price: float
    size: float
    side: str
    token_id: str
    market: str = ""
