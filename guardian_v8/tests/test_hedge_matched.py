"""MATCHED source hedging and confirmed-only orphan SELL recovery, offline."""
import copy
from decimal import Decimal as D
import time
from types import SimpleNamespace as NS

import pytest

from test_hedge_exit import env, trade, remote, book, confirm_hedge
from hedge_exit import HedgeExit


@pytest.fixture
def early(env):
    engine, client, sdk = env
    engine.trigger = engine.cfg.hedge_trigger = "matched"
    engine.state["trigger"] = "matched"
    engine._persist()
    return env


def source(engine, status="MATCHED", **kwargs):
    engine.ingest(trade(status=status, **kwargs))
    return engine.state["markets"]["condition"]


def orphan(early, qty="20"):
    engine, client, sdk = early
    market = source(engine, qty=qty)
    engine.step()
    confirm_hedge(engine, client, qty=qty)
    source(engine, "FAILED", qty=qty)
    engine.step()
    return market


def test_matched_buys_exact_own_leg_before_confirmation(early):
    engine, _, sdk = early
    event = trade(qty="7.5")
    event["status"] = "MATCHED"
    event["size"] = "100"  # whole trade is not our maker-leg quantity
    engine.notify(event)
    engine.step()
    market = engine.state["markets"]["condition"]
    assert sdk.prepared[0]["shares"] == "7.50"
    assert sdk.prepared[0]["price"] == "0.16"  # unchanged original bid inversion
    assert market["inventory"]["yes"]["qty"] == "0"
    assert engine.blocks("yes") and engine.blocks("no")
    assert sdk.balance_reads == 0


def test_confirmation_and_duplicate_updates_never_buy_again(early):
    engine, _, sdk = early
    market = source(engine)
    engine.step()
    for status in ("MATCHED", "MINED", "RETRYING", "CONFIRMED", "CONFIRMED", "MATCHED"):
        source(engine, status)
        engine.step()
    assert len(sdk.prepared) == 1
    assert market["inventory"]["yes"]["qty"] == "20"
    assert not engine.state["pending_legs"]


@pytest.mark.parametrize("status", ["MINED", "RETRYING", "MATCHED_NOT_BROADCASTED"])
def test_rest_recovery_can_first_observe_a_later_match_stage(early, status):
    engine, _, sdk = early
    source(engine, status)
    engine.step()
    assert len(sdk.prepared) == 1


def test_two_provisional_sources_net_and_never_merge_early(early):
    engine, _, sdk = early
    source(engine)
    source(engine, tid="other", oid="reward-no", token="no", price="0.14")
    engine.step()
    assert not sdk.prepared and not sdk.merges
    source(engine, "CONFIRMED")
    engine.step()
    assert not sdk.merges
    source(engine, "CONFIRMED", tid="other", oid="reward-no", token="no", price="0.14")
    engine.step()
    assert sdk.merges == [("condition", D(20))]


def test_partial_source_waits_for_cancel_but_not_for_confirmation(early):
    engine, client, sdk = early
    source(engine)
    client.orders["reward-1"] = remote("reward-1", "yes", qty="50", matched="20",
                                       associated=("fill-1",))
    engine.step()
    assert sdk.cancels == ["reward-1"] and not sdk.prepared
    engine.step()
    assert sdk.prepared[0]["shares"] == "20.00"
    assert "fill-1" in engine.state["pending_trades"]


def test_fully_matched_source_needs_no_cancel_or_confirmation_wait(early):
    engine, client, sdk = early
    source(engine)
    client.orders["reward-1"] = remote("reward-1", "yes", matched="20", associated=("fill-1",))
    engine.step()
    assert len(sdk.prepared) == 1 and not sdk.cancels


def test_hedge_matched_is_reserved_not_a_new_source_or_mergeable_balance(early):
    engine, client, sdk = early
    market = source(engine)
    engine.step()
    oid = market["order"]["id"]
    engine.ingest(trade("hedge-fill", oid, "no", status="MATCHED", taker=True, price="0.16"))
    client.orders[oid].size_matched = D(20)
    client.orders[oid].associate_trades = ("hedge-fill",)
    engine.step()
    assert market["inventory"]["no"]["qty"] == "0"
    assert len(sdk.prepared) == 1 and not sdk.merges
    source(engine, "CONFIRMED")
    engine.step()
    assert len(sdk.prepared) == 1 and not sdk.merges


def test_failed_source_cancels_unfilled_buy_without_more_buys(early):
    engine, _, sdk = early
    market = source(engine)
    engine.step()
    oid = market["order"]["id"]
    source(engine, "FAILED")
    engine.step()
    assert sdk.cancels == [oid]
    engine.step()
    assert not market["recover_sell"] and not sdk.sells
    assert len(sdk.prepared) == 1


def test_failed_source_sells_confirmed_opposite_and_finishes(early):
    engine, client, sdk = early
    market = orphan(early)
    assert len(sdk.prepared) == 1
    assert sdk.sells[0]["shares"] == "20.00"
    assert sdk.sells[0]["price"] == "0.14"  # original immediate SELL bid rule
    order = market["order"]
    assert order["side"] == "SELL" and engine.owns(order["id"])
    engine.ingest(trade("sell-fill", order["id"], "no", status="MATCHED", taker=True,
                        side="SELL", price="0.14"))
    client.orders[order["id"]].size_matched = D(20)
    engine.step()
    assert market["inventory"]["no"]["qty"] == "20"
    engine.ingest(trade("sell-fill", order["id"], "no", status="CONFIRMED", taker=True,
                        side="SELL", price="0.14"))
    engine.step()
    assert not market["recover_sell"] and market["order"] is None
    assert D(market["inventory"]["no"]["qty"]) == 0
    assert len(sdk.prepared) == 1 and len(sdk.sells) == 1


def test_failed_source_waits_for_hedge_settlement_before_selling(early):
    engine, client, sdk = early
    market = source(engine)
    engine.step()
    oid = market["order"]["id"]
    client.orders[oid].size_matched = D(20)
    event = trade("hedge-fill", oid, "no", status="MATCHED", taker=True, price="0.16")
    client.orders[oid].associate_trades = ("hedge-fill",)
    client.trades["hedge-fill"] = event
    engine.ingest(event)
    source(engine, "FAILED")
    engine.step()
    assert not sdk.sells and not sdk.merges
    confirm_hedge(engine, client)
    engine.step()
    assert len(sdk.sells) == 1


def test_failed_source_cancels_unfilled_remainder_then_sells_only_partial(early):
    engine, client, sdk = early
    market = source(engine)
    engine.step()
    confirm_hedge(engine, client, qty="12")
    source(engine, "FAILED")
    engine.step()
    engine.step()
    assert len(sdk.prepared) == 1
    assert sdk.sells[0]["shares"] == "12.00"


def test_source_partial_failure_merges_confirmed_pairs_then_sells_surplus(early):
    engine, client, sdk = early
    source(engine, qty="10")
    source(engine, tid="second", oid="second-source", qty="10")
    engine.step()
    confirm_hedge(engine, client)
    source(engine, "CONFIRMED", qty="10")
    source(engine, "FAILED", tid="second", oid="second-source", qty="10")
    engine.step()
    assert sdk.merges == [("condition", D(10))] and not sdk.sells
    sdk.tx_state = "STATE_CONFIRMED"
    engine.step()
    assert sdk.sells[0]["shares"] == "10.00"
    assert len(sdk.prepared) == 1


def test_recovery_keeps_original_immediate_and_passive_sell_thresholds(early):
    engine, client, sdk = early
    client.books["no"] = book("0.10", "0.16")
    market = orphan(early)
    assert not sdk.sells
    market["next_quote"] = 0
    engine.step()
    assert sdk.sells[0]["price"] == "0.16"


def test_recovery_below_minimum_never_rounds_up(early):
    engine, client, sdk = early
    source(engine)
    engine.step()
    confirm_hedge(engine, client, qty="4.99", canceled=True)
    source(engine, "FAILED")
    engine.step()
    assert not sdk.sells and len(sdk.prepared) == 1


def test_recovery_max_hold_uses_fok_sell_despite_gap(early):
    engine, client, sdk = early
    market = source(engine)
    engine.step()
    confirm_hedge(engine, client)
    engine.cfg.max_hold_enabled = True
    market["inventory"]["no"]["first"] = time.time() - 20_000
    client.books["no"] = book("0.01", "0.02")
    source(engine, "FAILED")
    engine.step()
    assert sdk.sells[0]["forced"] is True and sdk.sells[0]["shares"] == "20.00"


def test_unknown_buy_is_not_reposted_after_source_failed(early):
    engine, _, sdk = early
    source(engine)
    sdk.post_timeout, sdk.accept_timeout = True, False
    engine.step()
    source(engine, "FAILED")
    engine.step()
    assert len(sdk.posts) == 1 and len(sdk.prepared) == 1


def test_recovery_sell_unknown_response_and_restart_keep_same_signed_order(early):
    engine, client, sdk = early
    source(engine)
    engine.step()
    confirm_hedge(engine, client)
    source(engine, "FAILED")
    sdk.post_timeout, sdk.accept_timeout = True, False
    engine.step()
    payload = copy.deepcopy(sdk.posts[-1])
    path = engine.journal.path
    engine.close()
    recovered = HedgeExit(client, engine.cfg, path, lambda: None, sdk=sdk)
    try:
        recovered.last_poll = time.time()
        recovered.step()
        assert sdk.posts[-1] == payload
        assert len(sdk.sells) == 1 and len(sdk.prepared) == 1
        assert recovered.state["markets"]["condition"]["recover_sell"]
    finally:
        recovered.close()


def test_early_journal_survives_restart_without_rebuy(early):
    engine, client, sdk = early
    source(engine)
    engine.step()
    path = engine.journal.path
    engine.close()
    recovered = HedgeExit(client, engine.cfg, path, lambda: None, sdk=sdk)
    try:
        recovered.last_poll = time.time()
        recovered.step()
        source(recovered, "CONFIRMED")
        recovered.step()
        assert len(sdk.prepared) == 1
    finally:
        recovered.close()


def test_cannot_disable_matched_while_provisional_fill_is_unresolved(early):
    engine, client, sdk = early
    source(engine)
    engine.close()
    engine.cfg.hedge_trigger = "confirmed"
    with pytest.raises(RuntimeError, match="Unsettled early"):
        HedgeExit(client, engine.cfg, engine.journal.path, lambda: None, sdk=sdk)


def test_changed_pending_leg_freezes_instead_of_overhedging(early):
    engine, _, sdk = early
    market = source(engine)
    source(engine, qty="21")
    engine.step()
    assert market["blocked"] and not sdk.prepared


def test_missing_ws_bucket_waits_for_rest_instead_of_counting_twice(early):
    engine, _, sdk = early
    event = trade(status="MATCHED")
    event.pop("bucket_index")
    engine.ingest(event)
    engine.step()
    assert not sdk.prepared and engine.blocks("yes")
    event["bucket_index"] = 2
    engine.ingest(event)
    engine.step()
    event["status"] = "CONFIRMED"
    engine.ingest(event)
    engine.step()
    assert len(sdk.prepared) == 1
    assert engine.state["markets"]["condition"]["inventory"]["yes"]["qty"] == "20"


def test_version_one_journal_migrates_without_replaying_existing_inventory(env):
    engine, client, sdk = env
    source(engine, "CONFIRMED")
    old_seen = copy.deepcopy(engine.state["seen"])
    engine.state["version"] = 1
    engine.state.pop("pending_legs")
    engine.state.pop("trigger", None)
    engine.journal.write(engine.state)
    engine.close()
    engine.cfg.hedge_trigger = "matched"
    recovered = HedgeExit(client, engine.cfg, engine.journal.path, lambda: None, sdk=sdk)
    try:
        assert recovered.state["version"] == 3
        assert recovered.state["seen"] == old_seen
        source(recovered, "CONFIRMED")
        recovered.last_poll = time.time()
        recovered.step()
        assert recovered.state["markets"]["condition"]["inventory"]["yes"]["qty"] == "20"
        assert sdk.prepared[0]["shares"] == "20.00"
    finally:
        recovered.close()


def test_no_recovery_sell_until_real_balance_is_spendable(early):
    engine, client, sdk = early
    sdk.wallet_balances = (D(0), D(0))
    market = orphan(early)
    assert not sdk.sells and market["inventory"]["no"]["qty"] == "20"
    market["next_quote"] = 0
    sdk.wallet_balances = (D(0), D(20))
    engine.step()
    assert sdk.sells[0]["shares"] == "20.00"


def test_recovery_fok_not_filled_releases_only_definite_first_refusal(early):
    engine, client, sdk = early
    market = source(engine)
    engine.step()
    confirm_hedge(engine, client)
    market["inventory"]["no"]["first"] = time.time() - 20_000
    engine.cfg.max_hold_enabled = True
    source(engine, "FAILED")
    sdk.post = lambda signed: NS(ok=False, code="fok_not_filled")
    engine.step()
    assert market["order"] is None
    assert market["inventory"]["no"]["qty"] == "20"


def test_source_bucket_change_freezes_instead_of_creating_an_extra_source(early):
    engine, _, sdk = early
    market = source(engine)
    engine.step()
    changed = trade(status="CONFIRMED")
    changed["bucket_index"] = 3
    engine.ingest(changed)
    engine.step()
    assert market["blocked"] and len(sdk.prepared) == 1


def test_status_separates_provisional_from_confirmed_inventory(early):
    from hedge_status import summarize
    engine, _, _ = early
    source(engine)
    row = summarize(engine.state)["markets"][0]
    assert row["provisional"]["yes"] == "20"
    assert row["inventory"]["yes"] == "0"


def test_unmatched_recovery_fok_keeps_original_retry_interval(early):
    engine, client, sdk = early
    market = source(engine)
    engine.step()
    confirm_hedge(engine, client)
    market["inventory"]["no"]["first"] = time.time() - 20_000
    engine.cfg.max_hold_enabled = True
    source(engine, "FAILED")
    engine.step()
    first = market["order"]
    client.orders[first["id"]].status = "UNMATCHED"
    engine.step()
    assert market["order"] is None and len(sdk.sells) == 1
    assert market["next_quote"] > time.time()


def test_recovery_exchange_approval_is_journaled_and_waits_before_sell(early):
    engine, _, sdk = early
    sdk.is_sell_approved = False
    market = orphan(early)
    assert market["tx"]["kind"] == "sell_approve"
    assert not sdk.sells and sdk.sell_approvals == [("condition", "no")]
    engine.step()
    assert len(sdk.sell_approvals) == 1
    sdk.is_sell_approved, sdk.tx_state = True, "STATE_CONFIRMED"
    engine.step()
    assert sdk.sells[0]["shares"] == "20.00"


def test_failed_ws_without_bucket_stops_buy_while_rest_reconciles(early):
    engine, _, sdk = early
    market = source(engine)
    engine.step()
    oid = market["order"]["id"]
    failed = trade(status="FAILED")
    failed.pop("bucket_index")
    engine.ingest(failed)
    engine.step()
    assert market["recover_sell"] and sdk.cancels == [oid]
    assert len(sdk.prepared) == 1
