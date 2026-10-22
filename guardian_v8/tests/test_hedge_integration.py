"""Offline configuration and executor integration regressions."""
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal as D
from pathlib import Path
import sys
import threading
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import Config
from execution import ExecutionLayer


def config(**changes):
    params = dict(pk="fake", wallet="", proxy="", relayer_api_key=None,
                  relayer_api_key_address=None, exit_mode="sell", hedge_poll_interval=5)
    params.update(changes)
    return Config(**params)


def test_default_mode_is_legacy_and_env_selects_new_mode(monkeypatch):
    monkeypatch.delenv("EXIT_MODE", raising=False)
    assert Config().exit_mode == "sell"
    monkeypatch.setenv("EXIT_MODE", "hedge_merge")
    assert Config().exit_mode == "hedge_merge"


def test_matched_trigger_is_opt_in_and_validated(monkeypatch):
    monkeypatch.delenv("HEDGE_TRIGGER", raising=False)
    assert Config().hedge_trigger == "confirmed"
    monkeypatch.setenv("HEDGE_TRIGGER", "matched")
    assert Config().hedge_trigger == "matched"
    with pytest.raises(EnvironmentError, match="HEDGE_TRIGGER"):
        config(hedge_trigger="invalid").validate()


def test_startup_defaults_to_live_positions_and_resume_remains_available(monkeypatch):
    monkeypatch.delenv("HEDGE_STARTUP_MODE", raising=False)
    assert Config().hedge_startup_mode == "positions"
    monkeypatch.setenv("HEDGE_STARTUP_MODE", "resume")
    assert Config().hedge_startup_mode == "resume"
    with pytest.raises(EnvironmentError, match="HEDGE_STARTUP_MODE"):
        config(hedge_startup_mode="invalid").validate()


def test_mode_requires_relayer_pair_and_valid_values():
    with pytest.raises(EnvironmentError, match="EXIT_MODE"):
        config(exit_mode="typo").validate()
    with pytest.raises(EnvironmentError, match="RELAYER"):
        config(exit_mode="hedge_merge").validate()
    for interval in (0, float("nan"), 61):
        with pytest.raises(EnvironmentError, match="HEDGE_POLL"):
            config(hedge_poll_interval=interval).validate()
    with pytest.raises(EnvironmentError, match="SELL_MIN_BID_GAP"):
        config(exit_mode="hedge_merge", relayer_api_key="test", relayer_api_key_address="test",
               sell_min_bid_gap=D("NaN")).validate()


def exec_cfg():
    return NS(max_workers=1, exec_interval=0, place_retries=2, place_retry_delay=0)


def test_queued_maker_guard_prevents_write_and_clears_inflight():
    client = NS(place_limit_order=Mock(), cancel_all=Mock(return_value=NS(canceled=[], not_canceled={})))
    layer = ExecutionLayer(client, exec_cfg())
    layer.place_guard = lambda _: False
    future = layer.place("yes", D("0.85"), D(20), D("0.01"))
    assert future.result(timeout=2) is None
    layer.drain_and_cancel_all("test")
    client.place_limit_order.assert_not_called()
    assert not layer.maker_busy(["yes"])


def test_shutdown_waits_for_inflight_maker_before_final_cancel():
    entered, release = threading.Event(), threading.Event()
    calls = []

    def place(**kwargs):
        calls.append("place-start")
        entered.set()
        assert release.wait(3)
        calls.append("place-end")
        return NS(ok=True, order_id="ordinary-maker")

    def cancel():
        calls.append("cancel-all")
        return NS(canceled=["ordinary-maker"], not_canceled={})

    layer = ExecutionLayer(NS(place_limit_order=place, cancel_all=cancel), exec_cfg())
    layer.place("yes", D("0.85"), D(20), D("0.01"))
    assert entered.wait(2)
    assert layer.maker_busy(["yes"])
    with ThreadPoolExecutor(max_workers=1) as worker:
        shutdown = worker.submit(layer.drain_and_cancel_all, "test")
        release.set()
        assert shutdown.result(timeout=3)
    assert calls == ["place-start", "place-end", "cancel-all"]
    assert not layer.maker_busy(["yes"])


def test_maker_recovery_does_not_adopt_an_exit_order_at_same_price():
    client = NS(list_open_orders=lambda **kwargs: [NS(items=[
        NS(id="hedge", side="BUY", price=D("0.16")),
        NS(id="reward", side="BUY", price=D("0.16"))])])
    layer = ExecutionLayer(client, exec_cfg())
    try:
        layer.exit_order_owned = lambda oid: oid == "hedge"
        assert layer._verify_order_placed("no", D("0.16")) == "reward"
    finally:
        layer.shutdown()
