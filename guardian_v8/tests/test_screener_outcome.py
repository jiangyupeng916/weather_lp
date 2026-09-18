#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SCREENER_OUTCOME 方向过滤单元测试 —— 纯逻辑，mock 网络层。"""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from guardian import Guardian  # noqa: E402
from screener.types import ScoredMarket  # noqa: E402


def _make_scored():
    """构造一个 YES 和 NO 两个方向都达标的 ScoredMarket。"""
    return ScoredMarket(
        condition_id="0xcond",
        question="测试市场",
        end_date=None,
        days_to_expiry=float("inf"),
        yes_token_id="yes_token",
        no_token_id="no_token",
        total_daily_rewards=10.0,
        min_size=10.0,
        max_spread=0.05,
        midpoint=0.5,
        existing_total_size=3000.0,   # >= screener_min_existing_size
        yes_top3_bids=1500.0,          # >= 1200
        yes_top2_bids=500.0,           # >= 400
        yes_top1_bids=150.0,           # >= 100
        no_top3_bids=1500.0,
        no_top2_bids=500.0,
        no_top1_bids=150.0,
    )


def _run_screener(outcome):
    g = Guardian.__new__(Guardian)
    g.cfg = SimpleNamespace(
        screener_outcome=outcome,
        screener_min_top3_bids=1200.0,
        screener_min_top2_bids=400.0,
        screener_min_top1_bids=100.0,
        screener_min_existing_size=2000.0,
    )
    scored = [_make_scored()]
    with patch("screener.fetch_and_filter", return_value=[]), \
            patch("screener.enrich_and_filter", side_effect=lambda c, cfg: c), \
            patch("screener.analyze_orderbooks", return_value=scored):
        return g._screener_fetch()


def test_outcome_both():
    """both：YES 和 NO 都挂（默认，向后兼容）。"""
    r = _run_screener("both")
    assert [t[0] for t in r["targets"]] == ["yes_token", "no_token"]
    assert r["yes_count"] == 1 and r["no_count"] == 1


def test_outcome_yes_only():
    """yes：只挂 YES，NO 达标也不挂。"""
    r = _run_screener("yes")
    assert [t[0] for t in r["targets"]] == ["yes_token"]
    assert r["yes_count"] == 1 and r["no_count"] == 0


def test_outcome_no_only():
    """no：只挂 NO，YES 达标也不挂。"""
    r = _run_screener("no")
    assert [t[0] for t in r["targets"]] == ["no_token"]
    assert r["yes_count"] == 0 and r["no_count"] == 1


def test_validate_rejects_invalid_outcome():
    """非法取值：validate 抛 EnvironmentError（fail-fast）。"""
    from config import Config
    c = Config.__new__(Config)
    # frozen dataclass，用 object.__setattr__ 绕过
    for k, v in {
        "pk": "0xabc",
        "wallet": "",
        "proxy": "",
        "relayer_api_key": None,
        "relayer_api_key_address": None,
        "screener_outcome": "invalid",
    }.items():
        object.__setattr__(c, k, v)
    with pytest.raises(EnvironmentError):
        c.validate()


def test_validate_accepts_valid_outcome():
    """合法取值 both/yes/no：validate 不因 outcome 报错。"""
    from config import Config
    for ok in ("both", "yes", "no"):
        c = Config.__new__(Config)
        for k, v in {
            "pk": "0xabc",
            "wallet": "",
            "proxy": "",
            "relayer_api_key": None,
            "relayer_api_key_address": None,
            "screener_outcome": ok,
        }.items():
            object.__setattr__(c, k, v)
        c.validate()  # 不抛异常即通过
