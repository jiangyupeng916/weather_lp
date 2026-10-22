"""Minimum marketable BUY errors have bounded logs, with orders still tracked."""
from unittest.mock import Mock

from test_hedge_exit import env, fill_source, confirm_hedge


MESSAGE = "invalid amount for a marketable BUY order ($0.7216), min size: $1"


def test_small_buy_error_logs_three_times_but_keeps_same_signed_order_and_can_recover(env, monkeypatch, caplog):
    engine, client, sdk = env
    market = fill_source(engine)
    original = sdk.post
    submit = Mock(side_effect=RuntimeError(MESSAGE))
    monkeypatch.setattr(sdk, "post", submit)
    for _ in range(8):
        engine.step()
    messages = [r.getMessage() for r in caplog.records if MESSAGE in r.getMessage()]
    assert len(messages) == 3 and "3/3" in messages[-1]
    assert "reconciliation continues" in messages[-1]
    assert submit.call_count == 8 and len(sdk.prepared) == 1
    assert all(c.args[0] == market["order"]["signed"] for c in submit.call_args_list)
    assert market["order"]["state"] == "unknown" and engine.owns(market["order"]["id"])
    monkeypatch.setattr(sdk, "post", original)
    engine.step()
    confirm_hedge(engine, client)
    engine.step()
    assert len(sdk.prepared) == 1 and len(sdk.merges) == 1


def test_other_post_failures_remain_visible_after_minimum_amount_logs_are_suppressed(env, monkeypatch, caplog):
    engine, _, sdk = env
    fill_source(engine)
    submit = Mock(side_effect=[RuntimeError(MESSAGE)] * 5 + [TimeoutError("response lost")] * 4)
    monkeypatch.setattr(sdk, "post", submit)
    for _ in range(9):
        engine.step()
    messages = [r.getMessage() for r in caplog.records if "[HEDGE ORDER UNKNOWN]" in r.getMessage()]
    assert sum(MESSAGE in m for m in messages) == 3
    assert sum("response lost" in m for m in messages) == 4
