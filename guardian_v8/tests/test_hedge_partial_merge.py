"""Recycle confirmed pairs without canceling or resizing a resting hedge BUY."""
import copy
from decimal import Decimal as D
import time
from unittest.mock import Mock

import pytest

from hedge_exit import HedgeExit
from test_hedge_exit import env, fill_source, trade


def live_buy(env):
    engine, client, sdk = env
    market = fill_source(engine)
    engine.step()
    market["next_quote"] = time.time() + 3600
    return market


def partial(engine, client, tid="part-1", qty="12", price="0.16", status="CONFIRMED"):
    order = engine.state["markets"]["condition"]["order"]
    event = trade(tid, order["id"], "no", qty, price, status=status, taker=True)
    client.trades[tid] = event
    remote = client.orders[order["id"]]
    remote.size_matched += D(qty)
    remote.associate_trades = (*remote.associate_trades, tid)
    remote.status = "MATCHED" if remote.size_matched >= remote.original_size else "LIVE"
    engine.ingest(event)
    return event


def test_confirmed_partial_merge_keeps_the_live_buy_and_its_remaining_gap(env):
    engine, client, sdk = env
    market = live_buy(env)
    order = copy.deepcopy(market["order"])
    partial(engine, client)
    sdk.wallet_balances = (D(20), D(12))
    engine.step()
    assert sdk.merges == [("condition", D(12))]
    assert market["order"]["id"] == order["id"] and not sdk.cancels
    assert market["order"]["signed"] == order["signed"]
    assert len(sdk.prepared) == len(sdk.posts) == 1
    assert market["inventory"]["yes"]["qty"] == "20"
    assert D(market["inventory"]["no"]["qty"]) == 12  # not deducted before confirmation
    for _ in range(4):
        engine.step()
    assert len(sdk.merges) == 1

    sdk.tx_state = "STATE_CONFIRMED"
    sdk.wallet_balances = (D(8), D(0))
    engine.step()
    assert D(market["inventory"]["yes"]["qty"]) == 8
    assert D(market["inventory"]["yes"]["cost"]) == D("6.8")
    assert D(market["inventory"]["no"]["qty"]) == 0
    assert market["order"]["id"] == order["id"]
    assert D(market["order"]["shares"]) - D(engine.state["fills"][order["id"]]["CONFIRMED"]) == 8
    assert not sdk.cancels and len(sdk.prepared) == 1


def test_multiple_partial_merges_close_the_exact_original_twenty_shares(env):
    engine, client, sdk = env
    market = live_buy(env)
    for index, (qty, price, before) in enumerate((("12", "0.16", "20"), ("4", "0.15", "8"), ("4", "0.14", "4"))):
        partial(engine, client, tid=f"part-{index}", qty=qty, price=price)
        sdk.wallet_balances = (D(before), D(qty))
        sdk.tx_state = "STATE_NEW"
        engine.step()
        assert sdk.merges[-1] == ("condition", D(qty))
        sdk.tx_state = "STATE_CONFIRMED"
        sdk.wallet_balances = (D(before) - D(qty), D(0))
        engine.step()
    assert sum(q for _, q in sdk.merges) == 20
    assert all(D(p["qty"]) == 0 and D(p["cost"]) == 0 for p in market["inventory"].values())
    assert not market["order"] and len(sdk.prepared) == 1 and not sdk.cancels


def test_more_fills_during_merge_are_accounted_before_the_next_merge(env):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client)
    sdk.wallet_balances = (D(20), D(12))
    engine.step()
    partial(engine, client, tid="last", qty="8")
    engine.step()
    assert sdk.merges == [("condition", D(12))]
    assert D(market["inventory"]["no"]["qty"]) == 20
    sdk.tx_state = "STATE_CONFIRMED"
    sdk.wallet_balances = (D(8), D(8))
    engine.step()
    assert sdk.merges == [("condition", D(12)), ("condition", D(8))]
    assert D(market["inventory"]["yes"]["qty"]) == D(market["inventory"]["no"]["qty"]) == 8
    assert not market["order"] and len(sdk.prepared) == 1


def test_partial_merge_waits_for_trade_confirmation_even_if_chain_tokens_are_visible(env):
    engine, client, sdk = env
    live_buy(env)
    event = partial(engine, client, status="MATCHED")
    sdk.wallet_balances = (D(20), D(12))
    engine.step()
    assert not sdk.merges
    event["status"] = "CONFIRMED"
    engine.ingest(event)
    engine.step()
    assert sdk.merges == [("condition", D(12))]


def test_partial_pairs_merge_before_quote_reads_or_a_collateral_check(env, monkeypatch):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client)
    market["next_quote"] = 0
    client.collateral = D(0)
    def unavailable(**kwargs):
        raise AssertionError("Paired shares should merge before reading quotes or collateral")
    monkeypatch.setattr(client, "get_order_book", unavailable)
    monkeypatch.setattr(client, "get_balance_allowance", unavailable)
    engine.step()
    assert sdk.merges == [("condition", D(12))] and not sdk.cancels


@pytest.mark.parametrize("read", ["balances", "approved"])
def test_merge_read_failure_does_not_block_normal_order_repricing(env, monkeypatch, read):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client)
    market["next_quote"] = 0
    oid = market["order"]["id"]
    monkeypatch.setattr(sdk, read, Mock(side_effect=TimeoutError("RPC unavailable")))
    engine.step()
    assert sdk.cancels == [oid] and market["order"]["canceling"]
    assert not sdk.merges and not market["blocked"] and len(sdk.prepared) == 1


def test_changed_gap_is_reconciled_before_preserving_a_buy_for_partial_merge(env):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client)
    fill_source(engine, tid="extra-no", oid="reward-no", token="no", qty="4", price="0.15")
    sdk.wallet_balances = (D(20), D(16))
    market["next_quote"] = 0
    oid = market["order"]["id"]
    engine.step()
    assert sdk.cancels == [oid] and not sdk.merges
    engine.step()
    assert sdk.merges == [("condition", D(16))] and len(sdk.prepared) == 1


def test_source_failure_cancels_remaining_buy_even_while_partial_merge_is_pending(env):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client)
    engine.step()
    oid = market["order"]["id"]
    engine.trigger = engine.cfg.hedge_trigger = "matched"
    fill_source(engine, tid="late-source", oid="late-order", qty="3", status="MATCHED")
    fill_source(engine, tid="late-source", oid="late-order", qty="3", status="FAILED")
    engine.step()
    assert sdk.cancels == [oid] and market["order"]["canceling"]
    assert market["recover_sell"] and market["tx"]["kind"] == "merge"
    assert sdk.merges == [("condition", D(12))] and len(sdk.prepared) == 1


def test_provisional_source_is_not_used_to_merge_a_confirmed_opposite_fill(env):
    engine, client, sdk = env
    engine.trigger = engine.cfg.hedge_trigger = "matched"
    fill_source(engine, status="MATCHED")
    engine.step()
    partial(engine, client)
    sdk.wallet_balances = (D(20), D(12))
    engine.step()
    assert not sdk.merges
    assert D(engine.state["markets"]["condition"]["inventory"]["yes"]["qty"]) == 0
    fill_source(engine, status="CONFIRMED")
    engine.step()
    assert sdk.merges == [("condition", D(12))]


def test_partial_merge_uses_only_chain_available_pairs_with_exact_precision(env):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client, qty="12.345678")
    sdk.wallet_balances = (D("10.123456"), D("6.654321"))
    engine.step()
    assert sdk.merges == [("condition", D("6.654321"))]
    sdk.tx_state = "STATE_CONFIRMED"
    sdk.wallet_balances = (D("3.469135"), D(0))
    engine.step()
    inv = market["inventory"]
    assert D(inv["yes"]["qty"]) - D(inv["no"]["qty"]) == D("7.654322")
    assert len(sdk.prepared) == 1 and not sdk.cancels


def test_partial_merge_authorizes_and_then_merges_without_canceling_the_buy(env):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client)
    sdk.is_approved = False
    engine.step()
    assert sdk.approvals == ["condition"] and not sdk.merges
    assert market["tx"]["kind"] == "approve" and not sdk.cancels
    sdk.is_approved, sdk.tx_state = True, "STATE_CONFIRMED"
    engine.step()
    assert sdk.merges == [("condition", D(12))]
    assert len(sdk.prepared) == 1 and not sdk.cancels


def test_partial_merge_survives_resume_without_a_second_submission(env):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client)
    engine.step()
    oid = market["order"]["id"]
    path = engine.journal.path
    engine.close()
    recovered = HedgeExit(client, engine.cfg, path, lambda: None, sdk=sdk)
    try:
        recovered.last_poll = time.time()
        recovered.step()
        assert sdk.merges == [("condition", D(12))]
        assert recovered.state["markets"]["condition"]["order"]["id"] == oid
        sdk.tx_state = "STATE_CONFIRMED"
        sdk.wallet_balances = (D(8), D(0))
        recovered.step()
        assert D(recovered.state["markets"]["condition"]["inventory"]["yes"]["qty"]) == 8
        assert len(sdk.prepared) == 1 and not sdk.cancels
    finally:
        recovered.close()


def test_lost_partial_merge_response_retains_the_reservation_and_does_not_resubmit(env):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client)
    sdk.submit_timeout = True
    engine.step()
    for _ in range(4):
        engine.step()
    assert sdk.merges == [("condition", D(12))]
    assert market["blocked"] and market["tx"] and market["order"]
    assert len(sdk.prepared) == 1 and not sdk.cancels


def test_failed_partial_merge_does_not_deduct_inventory_or_create_a_new_buy(env):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client)
    engine.step()
    sdk.tx_state = "STATE_FAILED"
    engine.step()
    assert sdk.merges == [("condition", D(12)), ("condition", D(12))]
    assert D(market["inventory"]["yes"]["qty"]) == 20
    assert D(market["inventory"]["no"]["qty"]) == 12
    assert len(sdk.prepared) == 1 and not sdk.cancels


def test_duplicate_partial_fill_after_merge_does_not_buy_or_merge_it_again(env):
    engine, client, sdk = env
    market = live_buy(env)
    event = partial(engine, client)
    engine.step()
    sdk.tx_state = "STATE_CONFIRMED"
    sdk.wallet_balances = (D(8), D(0))
    engine.step()
    for _ in range(3):
        engine.ingest(event)
        engine.step()
    assert sdk.merges == [("condition", D(12))]
    assert len(sdk.prepared) == 1 and D(market["inventory"]["no"]["qty"]) == 0


@pytest.mark.parametrize("kind", ["SELL", "unknown", "prepared", "unsettled", "canceling"])
def test_partial_merge_does_not_spend_reserved_or_uncertain_outcome_inventory(env, kind):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client)
    if kind == "SELL":
        market["order"]["side"] = "SELL"
    elif kind in ("unknown", "prepared"):
        market["order"]["state"] = kind
    else:
        market["order"][kind] = True
    assert not engine._merge_pairs("condition", market)
    assert not sdk.merges and not sdk.approvals


def test_partial_merge_respects_another_markets_inflight_transaction(env):
    engine, client, sdk = env
    market = live_buy(env)
    partial(engine, client)
    other = engine._market("other")
    engine._submit_transaction("other", other, D(1), False)
    engine.step()
    assert not market["tx"] and not sdk.merges and len(sdk.prepared) == 1
    sdk.tx_state = "STATE_CONFIRMED"
    engine.step()  # other market's approval clears after this market's turn
    engine.step()
    assert sdk.merges == [("condition", D(12))]
