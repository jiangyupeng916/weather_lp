"""多区间筛选：配置、端点、旧范围兼容性及步长组合，全部 mock 网络。"""

import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import Config
from screener.clob import analyze_orderbooks
from screener.types import CandidateMarket


def _config(monkeypatch, ranges, tick=""):
    if ranges is None:
        monkeypatch.delenv("SCREENER_MIDPOINT_RANGES", raising=False)
    else:
        monkeypatch.setenv("SCREENER_MIDPOINT_RANGES", ranges)
    monkeypatch.setenv("SCREENER_TICK_SIZE", tick)
    return Config(screener_min_midpoint=0.15, screener_max_midpoint=0.85)


@pytest.mark.parametrize("value", [None, "", "   "])
def test_empty_config_uses_legacy_range(monkeypatch, value):
    assert _config(monkeypatch, value).screener_midpoint_ranges == ()


@pytest.mark.parametrize("value,expected", [
    ("0:0.1,0.9:1", ((0.0, 0.1), (0.9, 1.0))),
    (" 0 : 0.1 , 0.9 : 1 ", ((0.0, 0.1), (0.9, 1.0))),
    ("0.9:1,0:0.1", ((0.9, 1.0), (0.0, 0.1))),
    ("0:0.6,0.4:1", ((0.0, 0.6), (0.4, 1.0))),
    ("0.1:0.1", ((0.1, 0.1),)),
])
def test_valid_config(monkeypatch, value, expected):
    assert _config(monkeypatch, value).screener_midpoint_ranges == expected


@pytest.mark.parametrize("value", [
    "0.1", "0-0.1", "0:0.1:0.2", ":0.1", "0:", "bad:1",
    "0:0.1,", ",0.9:1", "0:0.1,,0.9:1", "0:0.1,wrong",
    "-0.1:0.1", "0.9:1.1", "0.9:0.1", "NaN:1", "0:NaN", "0:Infinity",
])
def test_invalid_config_fails_at_startup(monkeypatch, value):
    with pytest.raises(EnvironmentError, match="SCREENER_MIDPOINT_RANGES"):
        _config(monkeypatch, value)


def _screen(cfg, bid, ask, yes_tick="0.001", no_tick="0.001"):
    market = CandidateMarket(
        condition_id="condition", question="Test market", end_date=None,
        days_to_expiry=float("inf"), yes_token_id="yes", no_token_id="no",
        total_daily_rewards=10.0, min_size=10.0, max_spread=0.05,
    )
    yes_book = {
        "asset_id": "yes", "tick_size": yes_tick,
        "bids": [] if bid is None else [{"price": str(bid), "size": "500"}],
        "asks": [] if ask is None else [{"price": str(ask), "size": "500"}],
    }
    no_book = {
        "asset_id": "no", "tick_size": no_tick,
        "bids": [{"price": "0.48", "size": "500"}],
        "asks": [{"price": "0.52", "size": "500"}],
    }
    response = Mock(status_code=200)
    response.json.return_value = [yes_book, no_book]
    with patch("screener.clob.requests.post", return_value=response):
        return analyze_orderbooks([market], cfg)


@pytest.mark.parametrize("midpoint,accepted", [
    (0, True), (0.05, True), (0.1, True), (0.1001, False),
    (0.5, False), (0.8999, False), (0.9, True), (0.95, True), (1, True),
])
def test_union_overrides_legacy_bounds(monkeypatch, midpoint, accepted):
    cfg = _config(monkeypatch, "0:0.1,0.9:1")
    assert bool(_screen(cfg, midpoint, midpoint)) is accepted


@pytest.mark.parametrize("bid,ask,expected", [
    ("0.85", "0.95", 0.9), ("0.05", "0.15", 0.1),
])
def test_inclusive_endpoints_use_decimal_average(monkeypatch, bid, ask, expected):
    cfg = _config(monkeypatch, "0:0.1,0.9:1")
    result = _screen(cfg, bid, ask)
    assert len(result) == 1
    assert result[0].midpoint == expected


@pytest.mark.parametrize("midpoint,accepted", [
    (0.1, False), (0.15, True), (0.5, True), (0.85, True), (0.9, False),
])
def test_disabled_preserves_legacy_bounds(monkeypatch, midpoint, accepted):
    cfg = _config(monkeypatch, None)
    assert bool(_screen(cfg, midpoint, midpoint)) is accepted
    assert _screen(_config(monkeypatch, ""), midpoint, midpoint) == _screen(cfg, midpoint, midpoint)


@pytest.mark.parametrize("bid,ask,accepted", [
    (None, "0.05", True), ("0.95", None, True), (None, None, False),
])
def test_one_sided_and_empty_books_keep_existing_midpoint_fallback(monkeypatch, bid, ask, accepted):
    cfg = _config(monkeypatch, "0:0.1,0.9:1")
    assert bool(_screen(cfg, bid, ask)) is accepted


def test_tick_and_midpoint_conditions_are_both_required(monkeypatch):
    cfg = _config(monkeypatch, "0:0.1,0.9:1", tick="0.001")
    assert len(_screen(cfg, "0.04", "0.06")) == 1
    assert _screen(cfg, "0.04", "0.06", no_tick="0.01") == []
    assert _screen(cfg, "0.49", "0.51") == []


def test_multiple_arbitrary_ranges_are_supported(monkeypatch):
    cfg = _config(monkeypatch, "0:0.1,0.3:0.4,0.9:1")
    assert len(_screen(cfg, "0.34", "0.36")) == 1
    assert _screen(cfg, "0.69", "0.71") == []
