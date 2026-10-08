"""价格步长筛选测试：配置及真实筛选路径，全部 mock 网络。"""

import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import Config
from screener.clob import analyze_orderbooks
from screener.types import CandidateMarket


def _config_from_env(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("SCREENER_TICK_SIZE", raising=False)
    else:
        monkeypatch.setenv("SCREENER_TICK_SIZE", value)
    return Config()


@pytest.mark.parametrize("value", [None, "", "   "])
def test_config_defaults_to_disabled(monkeypatch, value):
    assert _config_from_env(monkeypatch, value).screener_tick_size is None


@pytest.mark.parametrize("value", [
    "0.1", "0.01", "0.005", "0.0025", "0.001", "0.0001", " 0.0010 ",
])
def test_config_reads_exact_decimal(monkeypatch, value):
    assert _config_from_env(monkeypatch, value).screener_tick_size == Decimal(value.strip())


@pytest.mark.parametrize("value", ["invalid", "0", "-0.001", "0.003", "NaN", "sNaN", "Infinity"])
def test_invalid_config_fails_at_startup(monkeypatch, value):
    with pytest.raises(EnvironmentError, match="SCREENER_TICK_SIZE"):
        _config_from_env(monkeypatch, value)


def _run_screening(yes_tick, no_tick, required_tick):
    market = CandidateMarket(
        condition_id="condition", question="Test market", end_date=None,
        days_to_expiry=float("inf"), yes_token_id="yes", no_token_id="no",
        total_daily_rewards=10.0, min_size=10.0, max_spread=0.05,
    )
    cfg = SimpleNamespace(
        host="https://example.invalid", screener_tick_size=required_tick,
        screener_min_midpoint=0.15, screener_max_midpoint=0.85,
    )
    payload = []
    for token, tick in (("yes", yes_tick), ("no", no_tick)):
        book = {
            "asset_id": token,
            "bids": [{"price": "0.48", "size": "500"}],
            "asks": [{"price": "0.52", "size": "500"}],
        }
        if tick is not None:
            book["tick_size"] = tick
        payload.append(book)
    response = Mock(status_code=200)
    response.json.return_value = payload
    with patch("screener.clob.requests.post", return_value=response) as request:
        results = analyze_orderbooks([market], cfg)
    # 直接复用原有 /books 请求，不额外请求 /tick-size。
    request.assert_called_once_with(
        "https://example.invalid/books",
        json=[{"token_id": "yes"}, {"token_id": "no"}], timeout=30,
    )
    return results


@pytest.mark.parametrize("yes_tick,no_tick", [
    ("0.001", "0.001"), (0.001, "0.0010"), ("1e-3", "0.001"),
])
def test_matching_markets_pass(yes_tick, no_tick):
    result = _run_screening(yes_tick, no_tick, Decimal("0.001"))
    assert len(result) == 1
    assert result[0].condition_id == "condition"


@pytest.mark.parametrize("bad_tick", [
    "0.1", "0.01", "0.005", "0.0025", "0.0001",
    None, "", "invalid", "NaN", "sNaN", "Infinity",
])
@pytest.mark.parametrize("bad_side", ["yes", "no"])
def test_either_side_mismatching_or_unknown_is_rejected(bad_tick, bad_side):
    ticks = {"yes": "0.001", "no": "0.001"}
    ticks[bad_side] = bad_tick
    assert _run_screening(ticks["yes"], ticks["no"], Decimal("0.001")) == []


@pytest.mark.parametrize("yes_tick,no_tick", [
    (None, None), ("invalid", "NaN"), ("0.01", "0.0001"),
])
def test_disabled_filter_preserves_previous_scores(yes_tick, no_tick):
    baseline = _run_screening(None, None, None)
    assert len(baseline) == 1
    assert _run_screening(yes_tick, no_tick, None) == baseline
