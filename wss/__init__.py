#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""wss — Polymarket 市场频道 WebSocket + 完整 bot 包。

独立运行（完整 bot，替代 python main.py）:
    python -m wss.main   (从项目根目录执行，加载 wss/.env.wss)

模块导入（可选，适合集成/测试场景）:
    from wss import BidCache, MarketWS, WssGuard, GuardianWss
"""

from .cache import BidCache
from .market_ws import MarketWS
from .guard import WssGuard
from .guardian_wss import GuardianWss

__all__ = ["BidCache", "MarketWS", "WssGuard", "GuardianWss"]
