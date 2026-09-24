#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""wss — Polymarket 市场频道 WebSocket 组件包。

仅提供市场频道 WS 连接管理与 bid 缓存，供 guardian.py 直接集成（A2 方案）。
旧的独立 bot（GuardianWss / WssGuard / wss.main）已移除——
Guardian 直接持有 MarketWS，主线程独占 _markets 无锁的不变量由 Guardian 保证。

    from wss import BidCache, MarketWS
"""

from .cache import BidCache
from .market_ws import MarketWS

__all__ = ["BidCache", "MarketWS"]
