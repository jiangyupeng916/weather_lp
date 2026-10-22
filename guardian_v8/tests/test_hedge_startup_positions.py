"""Rebuild exits from current holdings, without replaying prior-run state."""
import copy
from decimal import Decimal as D
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

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
    with patch("hedge_exit.time.time", return_value=time.time()):
        engine.step()
    assert not engine.ready and engine.state["initialized"]
    with patch("hedge_exit.time.time", return_value=engine.state["since"] + 0.01):
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
def test_initialization_does_not_read_or_cancel_existing_orders(live, monkeypatch, side):
    engine, client, sdk = live
    holdings(client, sdk)
    client.orders["old"] = remote("old", side=side)
    monkeypatch.setattr(client, "list_open_orders", Mock(side_effect=AssertionError("Not needed for snapshot")))
    monkeypatch.setattr(client, "get_order", Mock(side_effect=AssertionError("Not needed for snapshot")))
    engine._bootstrap()
    assert engine.state["initialized"] and not sdk.cancels and not sdk.prepared
    assert client.orders["old"].status == "LIVE"


def test_initialization_does_not_load_historical_or_pending_trades(live, monkeypatch):
    engine, client, sdk = live
    holdings(client, sdk)
    client.trades["old"] = trade("old", status="MATCHED")
    monkeypatch.setattr(client, "list_account_trades", Mock(side_effect=AssertionError("Do not replay old trades")))
    engine._bootstrap()
    assert engine.state["initialized"] and not engine.state["pending_trades"]
    assert not engine.state["fills"] and not engine.state["seen"]
    assert D(engine.state["markets"]["condition"]["inventory"]["yes"]["qty"]) == 20


def test_initialization_uses_position_api_even_when_chain_quantity_differs(live):
    engine, client, sdk = live
    holdings(client, sdk, yes="10")
    sdk.wallet_balances = (D(20), D(0))
    activate(engine)
    assert sdk.prepared[0]["shares"] == "10.00" and sdk.balance_reads == 0


def test_chain_rpc_failure_does_not_block_single_side_snapshot_initialization(live, monkeypatch):
    engine, client, sdk = live
    holdings(client, sdk)
    monkeypatch.setattr(sdk, "balances", Mock(side_effect=TimeoutError("RPC unavailable")))
    activate(engine)
    assert sdk.prepared[0]["shares"] == "20.00"


def test_actual_merge_still_waits_for_sufficient_chain_shares(live):
    engine, client, sdk = live
    holdings(client, sdk, no="20")
    sdk.wallet_balances = (D(20), D(0))
    activate(engine)
    assert not sdk.merges and sdk.balance_reads == 1
    sdk.wallet_balances = (D(20), D(20))
    engine.step()
    assert sdk.merges == [("condition", D(20))]


def test_position_query_failure_cannot_be_treated_as_empty_holdings(live, monkeypatch):
    engine, client, sdk = live
    monkeypatch.setattr(client, "list_positions", Mock(side_effect=TimeoutError("Data API unavailable")))
    with pytest.raises(TimeoutError, match="Data API"):
        engine.step()
    assert not sdk.prepared and not engine.state["initialized"]
    assert not engine.ready


def test_initialization_needs_only_one_complete_position_read(live, monkeypatch):
    engine, client, sdk = live
    holdings(client, sdk, yes_price="0.83")
    read = Mock(wraps=client.list_positions)
    monkeypatch.setattr(client, "list_positions", read)
    engine._bootstrap()
    assert engine.state["initialized"] and read.call_count == 1
    assert D(engine.state["markets"]["condition"]["inventory"]["yes"]["cost"]) == D("16.6")


def test_reward_placement_waits_for_snapshot_second_boundary(live):
    engine, client, sdk = live
    holdings(client, sdk)
    with patch("hedge_exit.time.time", return_value=time.time()):
        engine.step()
    assert engine.state["initialized"] and not engine.ready and not sdk.prepared
    with patch("hedge_exit.time.time", return_value=engine.state["since"] - 0.01):
        engine.step()
        assert engine.blocks("yes") and not sdk.prepared
    with patch("hedge_exit.time.time", return_value=engine.state["since"]):
        engine.step()
        assert engine.ready and len(sdk.prepared) == 1


def test_undated_new_ws_trade_is_recovered_using_rest_original_match_time(live):
    engine, client, sdk = live
    holdings(client, sdk, yes_price="0.99")
    activate(engine)
    event = trade("new", "new-reward", qty="3", price="0.85")
    event["match_time"] = str(engine.state["since"])
    client.trades[event["id"]] = event
    ws = copy.deepcopy(event)
    ws.pop("match_time")
    engine.notify(ws)
    engine.step()
    inv = engine.state["markets"]["condition"]["inventory"]["yes"]
    assert D(inv["qty"]) == 23 and D(inv["cost"]) == D("22.35")


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
