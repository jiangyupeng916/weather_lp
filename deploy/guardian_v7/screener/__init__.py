#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""内置筛选器子包 — 拉取有返利奖励的市场，按 reward/dollar 评分筛选。

结果直接内存传递给 Guardian，同时输出 CSV 供人工查看。
"""

from screener.types import CandidateMarket, ScoredMarket, AllocatedMarket
from screener.markets import fetch_and_filter
from screener.clob import analyze_orderbooks

__all__ = [
    "fetch_and_filter",
    "analyze_orderbooks",
    "CandidateMarket",
    "ScoredMarket",
    "AllocatedMarket",
]
