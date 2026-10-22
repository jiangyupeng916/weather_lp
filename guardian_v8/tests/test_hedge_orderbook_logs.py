"""Missing order books stop log spam without stopping exit recovery (offline)."""
import copy
import logging
from decimal import Decimal as D
from unittest.mock import Mock

import pytest

from test_hedge_exit import env, fill_source, trade, confirm_hedge


MISSING_BOOK = "No orderbook exists for the requested token id"


def missing_logs(caplog):
    return [r.getMessage() for r in caplog.records
            if r.name == "guardian.hedge" and MISSING_BOOK in r.getMessage()]


@pytest.mark.parametrize("path", ["buy_source", "buy_opposite", "recovery_sell"])
def test_missing_book_logs_three_times_and_resumes_exit_when_restored(env, monkeypatch, caplog, path):
    engine, client, sdk = env
    market = fill_source(engine)
    market["recover_sell"] = path == "recovery_sell"
    token = "no" if path == "buy_opposite" else "yes"
    original = client.get_order_book

    def unavailable(*, token_id):
        if token_id == token:
            raise RuntimeError(MISSING_BOOK)
        return original(token_id=token_id)

    read = Mock(side_effect=unavailable)
    monkeypatch.setattr(client, "get_order_book", read)
    caplog.set_level(logging.INFO, logger="guardian.hedge")
    before = copy.deepcopy(market["inventory"])
    for _ in range(8):
        engine.step()

    messages = missing_logs(caplog)
    assert len(messages) == 3
    assert "1/3" in messages[0] and "3/3" in messages[-1]
    assert "background checks continue" in messages[-1]
    assert sum(c.kwargs["token_id"] == token for c in read.call_args_list) == 8
    assert market["inventory"] == before and not market["blocked"]
    assert engine.blocks("yes") and engine.blocks("no")
    assert not sdk.prepared and not sdk.sells and not sdk.merges

    monkeypatch.setattr(client, "get_order_book", original)
    engine.step()
    assert len(sdk.sells if path == "recovery_sell" else sdk.prepared) == 1
    assert any("HEDGE BOOK RESTORED" in r.getMessage() for r in caplog.records)


def test_missing_book_counts_each_token_and_resets_only_after_that_token_recovers(env, monkeypatch, caplog):
    engine, client, _ = env
    market = fill_source(engine)
    original = client.get_order_book
    missing = {"yes"}

    def unavailable(*, token_id):
        if token_id in missing:
            raise RuntimeError(MISSING_BOOK)
        return original(token_id=token_id)

    monkeypatch.setattr(client, "get_order_book", unavailable)
    caplog.set_level(logging.INFO, logger="guardian.hedge")
    # Successful source reads must not reset the unavailable opposite's count.
    for token in ("yes", "no", "yes"):
        missing.clear()
        missing.add(token)
        for _ in range(6):
            market["next_quote"] = 0
            engine.step()

    messages = missing_logs(caplog)
    assert len(messages) == 9
    assert sum("token=yes " in m for m in messages) == 6
    assert sum("token=no " in m for m in messages) == 3


@pytest.mark.parametrize("message", [
    "The read operation timed out", "404: order not found", "Too many requests (429)",
])
def test_other_errors_remain_visible_on_every_failure(env, monkeypatch, caplog, message):
    engine, client, sdk = env
    fill_source(engine)
    read = Mock(side_effect=RuntimeError(message))
    monkeypatch.setattr(client, "get_order_book", read)
    for _ in range(5):
        engine.step()
    messages = [r.getMessage() for r in caplog.records if "[HEDGE RETRY]" in r.getMessage()]
    assert len(messages) == 5 and all(message in m for m in messages)
    assert not sdk.prepared


@pytest.mark.parametrize("live_order", [False, True])
def test_suppressed_book_errors_do_not_block_fill_reconciliation_or_merge(env, monkeypatch, caplog, live_order):
    engine, client, sdk = env
    market = fill_source(engine)
    if live_order:
        engine.step()
        market["next_quote"] = 0
    original = client.get_order_book

    def unavailable(*, token_id):
        if token_id == "no":
            raise RuntimeError(MISSING_BOOK)
        return original(token_id=token_id)

    read = Mock(side_effect=unavailable)
    monkeypatch.setattr(client, "get_order_book", read)
    for _ in range(6):
        engine.step()
    assert len(missing_logs(caplog)) == 3
    calls = read.call_count
    if live_order:
        confirm_hedge(engine, client)
    else:
        fill_source(engine, tid="other", oid="other-order", token="no", price="0.15")
    engine.step()
    assert sdk.merges == [("condition", D(20))]
    sdk.tx_state = "STATE_CONFIRMED"
    engine.step()
    assert all(D(inv["qty"]) == 0 for inv in market["inventory"].values())
    assert not engine.blocks("yes") and not engine.blocks("no")
    assert read.call_count == calls


def test_missing_book_log_limits_are_independent_for_other_markets(env, monkeypatch, caplog):
    engine, client, sdk = env
    fill_source(engine)
    sdk.tokens = lambda condition: ("yes", "no") if condition == "condition" else ("other-yes", "other-no")
    event = trade(tid="other", oid="other-order", token="other-yes")
    event["market"] = "other-condition"
    engine.ingest(event)
    monkeypatch.setattr(client, "get_order_book", Mock(side_effect=RuntimeError(MISSING_BOOK)))
    for _ in range(6):
        engine.step()
    messages = missing_logs(caplog)
    assert len(messages) == 6
    assert sum("condition=condition " in m for m in messages) == 3
    assert sum("condition=other-condition " in m for m in messages) == 3
