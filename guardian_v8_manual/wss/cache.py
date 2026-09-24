#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BidCache — 线程安全的最优买价缓存。

key:   token_id (str)
value: best_bid (Decimal)
"""

from __future__ import annotations

import threading
from decimal import Decimal
from typing import Dict, Optional, Tuple


class BidCache:
    """线程安全的 best_bid 内存缓存。

    由 MarketWS 的 WS 回调线程写入，可由任意线程读取。
    """

    def __init__(self) -> None:
        self._data: Dict[str, Decimal] = {}
        self._lock = threading.Lock()

    # ── 写 ────────────────────────────────────────────────────────────────────

    def update(self, token_id: str, new_bid: Decimal) -> Tuple[Optional[Decimal], bool]:
        """更新 best_bid，返回 (旧值, 是否发生变化)。

        若价格未变则 changed=False，调用方可跳过后续动作。
        """
        with self._lock:
            old = self._data.get(token_id)
            changed = old != new_bid
            self._data[token_id] = new_bid
            return old, changed

    def remove(self, token_id: str) -> None:
        """移除一个市场的缓存条目（取消订阅时调用）。"""
        with self._lock:
            self._data.pop(token_id, None)

    # ── 读 ────────────────────────────────────────────────────────────────────

    def get(self, token_id: str) -> Optional[Decimal]:
        """返回当前 best_bid，不存在时返回 None。"""
        with self._lock:
            return self._data.get(token_id)

    def snapshot(self) -> Dict[str, Decimal]:
        """返回当前缓存的完整副本（用于调试/日志）。"""
        with self._lock:
            return dict(self._data)

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)
