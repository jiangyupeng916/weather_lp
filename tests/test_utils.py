#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_utils.py — 工具函数单元测试"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from decimal import Decimal, ROUND_HALF_DOWN
from utils import safe_float, safe_decimal, retry_call, round_to_tick, safe_float_from_decimal


class TestSafeFloat:
    def test_none(self):
        assert safe_float(None) == 0.0
        assert safe_float(None, -1.0) == -1.0

    def test_numbers(self):
        assert safe_float(42) == 42.0
        assert safe_float(3.14) == 3.14

    def test_strings(self):
        assert safe_float("3.14") == 3.14
        assert safe_float("  5.0  ") == 5.0
        assert safe_float("") == 0.0

    def test_invalid(self):
        assert safe_float("abc") == 0.0
        assert safe_float([1, 2]) == 0.0


class TestSafeDecimal:
    def test_valid(self):
        assert safe_decimal("0.01") == Decimal("0.01")
        assert safe_decimal(42) == Decimal("42")
        assert safe_decimal(3.14) is not None

    def test_none(self):
        assert safe_decimal(None) is None

    def test_invalid(self):
        assert safe_decimal("abc") is None


class TestRetryCall:
    def test_success_first_try(self):
        calls = []
        result = retry_call(lambda: (calls.append(1), "ok")[1], retries=3, delay=0.01)
        assert result == "ok"
        assert len(calls) == 1

    def test_retry_then_success(self):
        counter = [0]

        def flaky():
            counter[0] += 1
            if counter[0] < 3:
                raise ValueError("fail")
            return "ok"

        result = retry_call(flaky, retries=5, delay=0.01)
        assert result == "ok"
        assert counter[0] == 3

    def test_all_retries_exhausted(self):
        counter = [0]

        def always_fails():
            counter[0] += 1
            raise RuntimeError("always")

        try:
            retry_call(always_fails, retries=2, delay=0.01)
            assert False, "应当抛出异常"
        except RuntimeError as e:
            assert "always" in str(e)
        assert counter[0] == 2


class TestRoundToTick:
    def test_basic(self):
        assert round_to_tick(Decimal("0.0699"), Decimal("0.01")) == Decimal("0.07")
        assert round_to_tick(Decimal("0.0700"), Decimal("0.01")) == Decimal("0.07")
        assert round_to_tick(Decimal("0.071"), Decimal("0.01")) == Decimal("0.07")

    def test_fine_tick(self):
        assert round_to_tick(Decimal("0.4255"), Decimal("0.001")) == Decimal("0.426")
        assert round_to_tick(Decimal("0.4250"), Decimal("0.001")) == Decimal("0.425")
        assert round_to_tick(Decimal("0.4249"), Decimal("0.001")) == Decimal("0.425")

    def test_large_tick(self):
        assert round_to_tick(Decimal("0.07"), Decimal("0.05")) == Decimal("0.05")
        assert round_to_tick(Decimal("0.10"), Decimal("0.05")) == Decimal("0.10")
        assert round_to_tick(Decimal("0.099"), Decimal("0.05")) == Decimal("0.10")

    def test_zero_tick(self):
        assert round_to_tick(Decimal("0.07"), Decimal("0")) == Decimal("0.07")

    def test_negative_tick(self):
        assert round_to_tick(Decimal("0.07"), Decimal("-0.01")) == Decimal("0.07")


class TestSafeFloatFromDecimal:
    def test_precision(self):
        d = Decimal("0.07")
        f = safe_float_from_decimal(d)
        assert f == 0.07
        # 关键测试：不会出现 0.069999...
        assert str(f) == "0.07"

    def test_common_prices(self):
        cases = [
            (Decimal("0.50"), 0.50),
            (Decimal("0.01"), 0.01),
            (Decimal("0.99"), 0.99),
            (Decimal("0.001"), 0.001),
        ]
        for d, expected in cases:
            f = safe_float_from_decimal(d)
            assert f == expected, f"{d} -> {f} != {expected}"
