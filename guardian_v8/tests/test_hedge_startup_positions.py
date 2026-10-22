"""Rebuild exits from current holdings, without replaying prior-run state."""
import copy
from decimal import Decimal as D
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from hedge_exit import HedgeExit, Journal
from test_hedge_exit import env, trade, remote, fill_source, book


@pytest.fixture
def live(env):
    previous, client, sdk = env
    previous.close()
    previous.cfg.hedge_startup_mode = "positions"
    sdk.wallet_balances = (D(0), D(0))
    engine = HedgeExit(client, previous.cfg, previous.journal.path, lambda: None, sdk=sdk)
    yield engine, client, sdk
    engine.stop()
    engine.close()


def holdings(client, sdk, yes="20", no="0", yes_price="0.85", no_price="0.15"):
    client.positions = [NS(condition_id="condition", asset_id=t, size=D(q), avg_price=D(p))
                        for t, q, p in (("yes", yes, yes_price), ("no", no, no_price)) if D(q)]
    sdk.wallet_balances = (D(yes), D(no))


def activate(engine):
    engine.step()
    assert not engine.ready and not engine.state["initialized"]
    engine.step()
    assert engine.ready and engine.state["initialized"]


def test_current_single_side_uses_reported_average_cost_and_exact_gap(live):
    engine, client, sdk = live
    holdings(client, sdk)
    activate(engine)
    inv = engine.state["markets"]["condition"]["inventory"]
    assert D(inv["yes"]["qty"]) == 20 and D(inv["yes"]["cost"]) == 17
    assert sdk.prepared[0]["shares"] == "20.00" and sdk.prepared[0]["token"] == "no"
    assert not sdk.merges


def test_current_paired_positions_merge_without_another_buy(live):
    engine, client, sdk = live
    holdings(client, sdk, no="20")
    activate(engine)
    assert sdk.merges == [("condition", D(20))]
    assert not sdk.prepared
    sdk.tx_state = "STATE_CONFIRMED"
    engine.step()
    assert all(D(p["qty"]) == 0 for p in engine.state["markets"]["condition"]["inventory"].values())


def test_current_unequal_sides_only_buy_the_net_shortfall(live):
    engine, client, sdk = live
    holdings(client, sdk, no="7")
    activate(engine)
    assert sdk.prepared[0]["shares"] == "13.00"


def test_supported_sdk_position_model_fields_are_used_without_guessing(live):
    from polymarket.models.data.portfolio import Position
    engine, client, sdk = live
    condition = "0x" + "11" * 32
    client.positions = [Position.model_validate({
        "conditionId": condition, "asset": "101", "size": "20", "avgPrice": "0.85"})]
    sdk.tokens = lambda _: ("101", "102")
    sdk.wallet_balances = (D(20), D(0))
    client.books.update({"101": book(), "102": book("0.14", "0.16")})
    activate(engine)
    assert sdk.prepared[0]["shares"] == "20.00" and sdk.prepared[0]["token"] == "102"
    assert D(engine.state["markets"][condition]["inventory"]["101"]["cost"]) == 17


def test_all_position_pages_are_included_before_netting(live, monkeypatch):
    engine, client, sdk = live
    holdings(client, sdk, no="7")
    pages = [NS(items=[p]) for p in client.positions]
    monkeypatch.setattr(client, "list_positions", lambda **kwargs: pages)
    activate(engine)
    assert sdk.prepared[0]["shares"] == "13.00"


def test_empty_account_starts_without_historical_inventory(live):
    engine, _, sdk = live
    activate(engine)
    assert not engine.state["markets"] and not sdk.prepared and not sdk.merges


def test_prior_journal_is_archived_without_parsing_even_if_corrupt(env, monkeypatch):
    previous, client, sdk = env
    path = previous.journal.path
    content = b"unreadable old journal: not json"
    path.write_bytes(content)
    previous.close()
    previous.cfg.hedge_startup_mode = "positions"
    monkeypatch.setattr(Journal, "read", Mock(side_effect=AssertionError("Do not load old state")))
    engine = HedgeExit(client, previous.cfg, path, lambda: None, sdk=sdk)
    try:
        assert not engine.state["markets"] and not engine.state["owned_orders"]
        assert not engine.state["seen"] and not engine.state["pending_legs"]
        archives = list((path.parent / "archive").glob("*.json"))
        assert len(archives) == 1 and archives[0].read_bytes() == content
    finally:
        engine.close()


def test_archive_failure_does_not_overwrite_old_state_or_leave_the_lock_held(env, monkeypatch):
    previous, client, sdk = env
    path = previous.journal.path
    content = path.read_bytes()
    previous.close()
    previous.cfg.hedge_startup_mode = "positions"
    monkeypatch.setattr(Journal, "archive", Mock(side_effect=OSError("archive unavailable")))
    with pytest.raises(OSError, match="archive unavailable"):
        HedgeExit(client, previous.cfg, path, lambda: None, sdk=sdk)
    assert path.read_bytes() == content
    lock = Journal(path)
    lock.close()


def test_restart_after_manual_cleanup_ignores_old_unknown_signed_order(env):
    previous, client, sdk = env
    fill_source(previous)
    sdk.post_timeout, sdk.accept_timeout = True, False
    previous.step()
    old_payload = copy.deepcopy(sdk.posts[-1])
    previous.close()
    previous.cfg.hedge_startup_mode = "positions"
    sdk.post_timeout = False
    holdings(client, sdk, yes="0", no="8")
    engine = HedgeExit(client, previous.cfg, previous.journal.path, lambda: None, sdk=sdk)
    try:
        activate(engine)
        assert len(sdk.posts) == 2 and sdk.posts[-1] != old_payload
        assert sdk.prepared[-1]["shares"] == "8.00" and sdk.prepared[-1]["token"] == "yes"
        assert len(engine.state["owned_orders"]) == 1
        inv = engine.state["markets"]["condition"]["inventory"]
        assert D(inv["yes"]["qty"]) == 0 and D(inv["no"]["qty"]) == 8
    finally:
        engine.close()


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_old_live_orders_are_canceled_before_snapshot_and_new_exit(live, side):
    engine, client, sdk = live
    holdings(client, sdk)
    client.orders["old"] = remote("old", side=side)
    engine.step()
    assert sdk.cancels == ["old"] and not sdk.prepared
    assert not engine.ready
    activate(engine)
    assert sdk.prepared[0]["shares"] == "20.00"


def test_cancel_race_waits_for_trade_settlement_then_counts_snapshot_once(live):
    engine, client, sdk = live
    holdings(client, sdk)
    client.orders["old"] = remote("old", "yes", matched="7", associated=("late",))
    event = trade("late", "old", qty="7", status="MATCHED")
    client.trades["late"] = event
    engine.step()  # cancel old order
    with pytest.raises(RuntimeError, match="in-flight"):
        engine.step()
    assert not sdk.prepared and not engine.ready
    event["status"] = "CONFIRMED"
    activate(engine)
    assert sdk.prepared[0]["shares"] == "20.00"  # not 20 + the old 7-share fill
    assert D(engine.state["fills"]["old"]["CONFIRMED"]) == 7


def test_missing_associated_trade_details_cannot_be_treated_as_no_fill(live):
    engine, client, sdk = live
    holdings(client, sdk)
    client.orders["old"] = remote("old", "yes", matched="7", associated=("late",))
    engine.step()
    with pytest.raises(RuntimeError, match="trade details"):
        engine.step()
    assert not sdk.prepared and not engine.ready


def test_data_api_lag_waits_for_chain_agreement_instead_of_hedging_stale_shares(live):
    engine, client, sdk = live
    holdings(client, sdk, yes="10")
    sdk.wallet_balances = (D(20), D(0))
    with pytest.raises(RuntimeError, match="balances to agree"):
        engine.step()
    assert not sdk.prepared and not engine.state["initialized"]
    holdings(client, sdk)
    activate(engine)
    assert sdk.prepared[0]["shares"] == "20.00"


def test_recent_fill_discovers_position_not_yet_visible_in_data_api(live):
    engine, client, sdk = live
    client.trades["recent"] = trade("recent")
    sdk.wallet_balances = (D(20), D(0))
    with pytest.raises(RuntimeError, match="balances to agree"):
        engine.step()
    assert not sdk.prepared


def test_trade_arriving_during_snapshot_requires_a_new_stable_snapshot(live, monkeypatch):
    engine, client, sdk = live
    holdings(client, sdk)
    original = client.list_positions
    def positions(**kwargs):
        client.trades["new"] = trade("new", qty="20")
        return original(**kwargs)
    monkeypatch.setattr(client, "list_positions", positions)
    engine.step()  # history changed during read
    assert not sdk.prepared and not engine.state["initialized"]
    activate(engine)
    assert sdk.prepared[0]["shares"] == "20.00"


def test_position_cost_changes_restart_stability_check(live):
    engine, client, sdk = live
    holdings(client, sdk)
    engine.step()
    holdings(client, sdk, yes_price="0.83")
    engine.step()
    assert not engine.state["initialized"] and not sdk.prepared
    engine.step()
    assert D(engine.state["markets"]["condition"]["inventory"]["yes"]["cost"]) == D("16.6")


@pytest.mark.parametrize("timestamp", [True, False])
def test_late_snapshot_trade_ws_updates_never_count_position_twice(live, timestamp):
    engine, client, sdk = live
    holdings(client, sdk)
    event = trade()
    event["match_time"] = str(int(time.time()))
    client.trades[event["id"]] = event
    activate(engine)
    late = copy.deepcopy(event)
    if not timestamp:
        late.pop("match_time")
    for status in ("CONFIRMED", "MATCHED", "CONFIRMED"):
        late["status"] = status
        engine.notify(copy.deepcopy(late))
        engine.step()
    assert len(sdk.prepared) == 1 and sdk.prepared[0]["shares"] == "20.00"
    assert D(engine.state["markets"]["condition"]["inventory"]["yes"]["qty"]) == 20


def test_only_trades_after_the_snapshot_are_added_to_new_inventory(live):
    engine, client, sdk = live
    holdings(client, sdk, yes="20", yes_price="0.99")  # no immediate hedge at current quote
    activate(engine)
    assert not sdk.prepared
    event = trade("new", "new-reward", qty="3", price="0.85")
    event["match_time"] = str(engine.state["since"] + 1)
    engine.ingest(event)
    inv = engine.state["markets"]["condition"]["inventory"]["yes"]
    assert D(inv["qty"]) == 23 and D(inv["cost"]) == D("22.35")


@pytest.mark.parametrize("fault", ["missing_price", "foreign_token", "negative_size", "duplicate"])
def test_incomplete_or_inconsistent_positions_never_trigger_an_order(live, fault):
    engine, client, sdk = live
    holdings(client, sdk)
    if fault == "missing_price":
        client.positions[0].avg_price = None
    elif fault == "foreign_token":
        client.positions[0].asset_id = "foreign"
    elif fault == "negative_size":
        client.positions[0].size = D(-1)
    else:
        client.positions.append(copy.deepcopy(client.positions[0]))
    with pytest.raises(ValueError):
        engine.step()
    assert not sdk.prepared and not sdk.merges and not engine.ready
