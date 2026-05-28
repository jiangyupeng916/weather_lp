#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""工具函数 — 安全的类型转换、价格对齐、重试"""

from __future__ import annotations

import time
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


def safe_float(v: Any, default: float = 0.0) -> float:
    """安全转换为 float，失败返回 default。"""
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        return float(s) if s else default
    except Exception:
        return default


def safe_decimal(v: Any) -> Optional[Decimal]:
    """安全转换为 Decimal，失败返回 None。"""
    try:
        return Decimal(str(v)) if v is not None else None
    except Exception:
        return None


def retry_call(fn: Callable[[], T], retries: int = 3, delay: float = 1.0) -> T:
    """带重试的调用，重试次数用尽后抛出最后一次异常。"""
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == retries:
                raise
            time.sleep(delay)
    raise RuntimeError("retry_call unreachable")


def round_to_tick(price: Decimal, tick_size: Decimal) -> Decimal:
    """将价格四舍五入到最近的 tick_size 倍数（买入订单舍入到更优价格）。"""
    if tick_size <= 0:
        return price
    try:
        normalized = (price / tick_size).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return normalized * tick_size
    except (InvalidOperation, ZeroDivisionError):
        return price


def safe_float_from_decimal(d: Decimal) -> float:
    """通过字符串中间转换避免 Decimal → float 浮点精度丢失。

    例如: Decimal("0.07") → "0.07" → 0.07 (精确)
    而不是: Decimal("0.07") → 0.06999999999999999 (精度丢失)
    """
    return float(str(d))
