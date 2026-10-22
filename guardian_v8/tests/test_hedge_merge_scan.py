"""Runtime holdings discovery, trade replay, and merge reconciliation safety."""
import copy
from decimal import Decimal as D
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

import pytest

from hedge_exit import HedgeExit, has_exposure
from hedge_status import summarize
from test_hedge_exit import env, fill_source, remote, trade
from test_hedge_partial_merge import live_buy, partial


def holdings(client, sdk, yes="20", no="12", condition="condition"):
    client.positions = [NS(condition_id=condition, asset_id=t, size=D(q), avg_price=D(p))
                        for t, q, p in (("yes", yes, ".85"), ("no", no, ".15")) if D(q)]
    sdk.wallet_balances = (D(yes), D(no))


def scan(engine):
    engine.last_merge_scan = -float("inf")
    engine.step()


def next_boundary(engine):
    cutoff = max((m.get("inventory_since", 0) for m in engine.state["markets"].values()), default=0)
    with patch("hedge_exit.time.time", return_value=max(time.time(), cutoff + .01)):
        engine.step()


def test_runtime_untracked_pairs_are_merged_and_single_residual_is_not_bought(env):
    engine, client, sdk = env
    holdings(client, sdk)
    scan(engine)
    market = engine.state["markets"]["condition"]
    assert engine.ready and market["scan_only"] and not sdk.merges
    assert engine.blocks("yes") and engine.blocks("no")
    assert D(market["inventory"]["yes"]["cost"]) == 17
    assert D(market["inventory"]["no"]["cost"]) == D("1.8")
    next_boundary(engine)
    assert sdk.merges == [("condition", D(12))] and not sdk.prepared
    sdk.tx_state = "STATE_CONFIRMED"
    sdk.wallet_balances = (D(8), D(0))
    next_boundary(engine)
    assert D(market["inventory"]["yes"]["qty"]) == 8
    assert D(market["inventory"]["no"]["qty"]) == 0
    assert not sdk.prepared and not sdk.sells


def test_timer_does_not_query_until_due_and_zero_disables_discovery(env, monkeypatch):
    engine, client, sdk = env
    holdings(client, sdk)
    fetch = Mock(wraps=client.list_positions)
    monkeypatch.setattr(client, "list_positions", fetch)
    with patch("hedge_exit.time.monotonic", return_value=engine.last_merge_scan + 59):
        engine.step()
    fetch.assert_not_called()
    engine.cfg.hedge_merge_scan_interval = 0
    scan(engine)
    fetch.assert_not_called()
    engine.cfg.hedge_merge_scan_interval = 60
    scan(engine)
    fetch.assert_called_once_with(size_threshold=0, page_size=100)


def test_discovery_reads_all_pages_and_includes_small_pairs(env, monkeypatch):
    engine, client, sdk = env
    holdings(client, sdk, ".0032199", ".0012349")
    monkeypatch.setattr(client, "list_positions", lambda **kwargs: [NS(items=[p]) for p in client.positions])
    scan(engine)
    next_boundary(engine)
    assert sdk.merges == [("condition", D(".001234"))] and not sdk.prepared


def test_stale_positions_after_merge_do_not_recreate_burned_inventory(env):
    engine, client, sdk = env
    holdings(client, sdk, "20", "20")
    scan(engine)
    next_boundary(engine)
    sdk.tx_state = "STATE_CONFIRMED"
    sdk.wallet_balances = (D(0), D(0))
    next_boundary(engine)
    client.orders["new-reward"] = remote("new-reward", token="yes")
    scan(engine)  # positions API still returns 20/20
    market = engine.state["markets"]["condition"]
    assert all(D(p["qty"]) == 0 for p in market["inventory"].values())
    assert len(sdk.merges) == 1 and not sdk.prepared
    assert not sdk.cancels


def test_delayed_pre_snapshot_fills_do_not_reappear_or_trigger_another_buy(env):
    engine, client, sdk = env
    holdings(client, sdk, "20", "20")
    scan(engine)
    market = engine.state["markets"]["condition"]
    cutoff = market["inventory_since"]
    for token, price in (("yes", ".85"), ("no", ".15")):
        event = trade(f"late-{token}", f"old-{token}", token, "20", price)
        event["match_time"] = cutoff - 1
        engine.ingest(event)
    assert [D(p["qty"]) for p in market["inventory"].values()] == [20, 20]
    next_boundary(engine)
    sdk.wallet_balances = (D(0), D(0))
    sdk.tx_state = "STATE_CONFIRMED"
    next_boundary(engine)
    assert not sdk.prepared and len(sdk.merges) == 1


def test_untracked_market_new_fills_at_the_fence_use_the_normal_exit_flow(env):
    engine, client, sdk = env
    holdings(client, sdk, "20", "20")
    scan(engine)
    next_boundary(engine)
    market = engine.state["markets"]["condition"]
    sdk.tx_state = "STATE_CONFIRMED"
    sdk.wallet_balances = (D(0), D(0))
    next_boundary(engine)
    event = trade("new-fill", "new-order", "yes", "5", ".85")
    event["match_time"] = market["inventory_since"]
    engine.ingest(event)
    assert not market["scan_only"]
    next_boundary(engine)
    assert sdk.prepared[-1]["shares"] == "5.00"


def test_reconciliation_repairs_a_missing_opposite_position_for_an_existing_exit(env):
    engine, client, sdk = env
    market = fill_source(engine)
    first = market["inventory"]["yes"]["first"]
    holdings(client, sdk)
    client.positions[0].avg_price = D(".70")  # stale API average cannot change known costs
    scan(engine)
    assert not market.get("scan_only") and not sdk.prepared
    assert D(market["inventory"]["yes"]["cost"]) == 17
    assert market["inventory"]["yes"]["first"] == first
    next_boundary(engine)
    assert sdk.merges == [("condition", D(12))]
    sdk.tx_state = "STATE_CONFIRMED"
    sdk.wallet_balances = (D(8), D(0))
    next_boundary(engine)
    assert sdk.prepared[0]["shares"] == "8.00"


def test_scan_backfills_an_old_missing_fill_and_keeps_the_exact_live_buy(env):
    engine, client, sdk = env
    market = live_buy(env)
    original = copy.deepcopy(market["order"])
    event = partial(engine, client, qty="12")
    # Model a fill that REST knows but neither WS nor the short overlap saw.
    key = next(k for k in engine.state["seen"] if k.startswith("part-1:"))
    engine.state["seen"].pop(key)
    engine.state["terminal_trades"].pop(event["id"])
    engine.state["fills"].pop(original["id"])
    market["inventory"]["no"] = {"qty": "0", "cost": "0", "first": 0}
    holdings(client, sdk)
    scan(engine)
    assert not market.get("merge_scan")
    assert market["order"]["id"] == original["id"]
    assert market["order"]["signed"] == original["signed"]
    assert sdk.merges == [("condition", D(12))] and not sdk.cancels


@pytest.mark.parametrize("guard", ["unknown", "unsettled", "canceling", "sell", "blocked", "recovery", "pending"])
def test_scan_does_not_rebase_guarded_positions(env, guard):
    engine, client, sdk = env
    market = live_buy(env)
    if guard in ("unknown", "unsettled", "canceling", "sell"):
        if guard == "unknown":
            market["order"]["state"] = "unknown"
            client.orders.clear()
            sdk.post_timeout = True
            sdk.accept_timeout = False
        elif guard == "sell":
            market["order"]["side"] = "SELL"
            client.orders[market["order"]["id"]].side = "SELL"
        else:
            market["order"][guard] = True
    elif guard == "blocked":
        market["blocked"] = "manual reconciliation required"
    elif guard == "recovery":
        market["recover_sell"] = True
    else:
        engine.ingest(trade("pending", "pending-order", "no", "12", ".15", status="MATCHED"))
    before = copy.deepcopy(market["inventory"])
    holdings(client, sdk)
    scan(engine)
    assert market["inventory"] == before and not market.get("inventory_since")
    assert not sdk.merges


def test_new_pending_trade_from_history_prevents_unsafe_snapshot(env):
    engine, client, sdk = env
    holdings(client, sdk)
    client.trades["not-final"] = trade("not-final", status="MINED")
    scan(engine)
    market = engine.state["markets"]["condition"]
    assert not market.get("inventory_since") and not sdk.merges
    assert not market.get("merge_scan") and engine.blocks("yes")


def test_scan_waits_for_maker_write_and_cancels_then_settles_reward_orders(env):
    engine, client, sdk = env
    holdings(client, sdk)
    engine.maker_busy = lambda tokens: True
    client.orders["maker"] = remote("maker", token="yes", qty="20")
    scan(engine)
    market = engine.state["markets"]["condition"]
    assert market["merge_scan"] and engine.blocks("yes") and not sdk.cancels
    assert has_exposure(engine.state)
    assert summarize(engine.state)["markets"][0]["merge_scan"]
    engine.maker_busy = lambda tokens: False
    engine.step()
    assert sdk.cancels == ["maker"] and not sdk.merges
    engine.step()
    assert market.get("inventory_since") and not market.get("merge_scan")
    next_boundary(engine)
    assert sdk.merges == [("condition", D(12))]


@pytest.mark.parametrize("failure", ["positions", "trades", "balances"])
def test_scan_read_failure_does_not_pause_an_existing_exit(env, monkeypatch, failure):
    engine, client, sdk = env
    market = fill_source(engine)
    holdings(client, sdk)
    target, method = {"positions": (client, "list_positions"), "trades": (client, "list_account_trades"),
                      "balances": (sdk, "balances")}[failure]
    monkeypatch.setattr(target, method, Mock(side_effect=TimeoutError("scan unavailable")))
    scan(engine)
    assert engine.ready and not engine.fatal and not market.get("merge_scan")
    assert sdk.prepared[0]["shares"] == "20.00" and not sdk.merges


def test_inflight_merge_is_not_rebased_or_submitted_again(env):
    engine, client, sdk = env
    holdings(client, sdk, "20", "20")
    scan(engine)
    next_boundary(engine)
    market = engine.state["markets"]["condition"]
    before = copy.deepcopy(market["inventory"])
    for _ in range(3):
        scan(engine)
    assert market["inventory"] == before and len(sdk.merges) == 1
    assert not sdk.prepared


def test_scan_rebase_cutoff_and_merge_only_mode_survive_resume(env):
    engine, client, sdk = env
    holdings(client, sdk)
    scan(engine)
    before = copy.deepcopy(engine.state["markets"]["condition"])
    engine.close()
    recovered = HedgeExit(client, engine.cfg, engine.journal.path, lambda: None, sdk=sdk)
    try:
        assert recovered.state["version"] == 3
        assert recovered.state["markets"]["condition"] == before
        next_boundary(recovered)
        assert sdk.merges == [("condition", D(12))]
        sdk.tx_state = "STATE_CONFIRMED"
        sdk.wallet_balances = (D(8), D(0))
        next_boundary(recovered)
        assert not sdk.prepared
    finally:
        recovered.close()


def test_invalid_token_pair_does_not_merge_other_conditions_tokens(env):
    engine, client, sdk = env
    holdings(client, sdk)
    client.positions[1].asset_id = "unrelated"
    scan(engine)
    assert not sdk.merges and not sdk.prepared


def test_single_sided_discovery_does_not_create_buy_orders(env):
    engine, client, sdk = env
    holdings(client, sdk, "20", "0")
    scan(engine)
    assert not engine.state["markets"] and not sdk.merges and not sdk.prepared


def test_api_repeated_page_is_rejected_before_any_new_merges(env):
    engine, client, sdk = env
    holdings(client, sdk)
    client.positions.append(client.positions[0])
    scan(engine)
    assert not engine.state["markets"] and not sdk.merges and not sdk.prepared


def test_two_discovered_markets_share_the_single_wallet_transaction_queue(env):
    engine, client, sdk = env
    holdings(client, sdk, "20", "20")
    client.positions += [NS(condition_id="other", asset_id=t, size=D(5), avg_price=D(".5"))
                         for t in ("yes", "no")]
    sdk.balances = lambda cid: (D(20), D(20)) if cid == "condition" else (D(5), D(5))
    scan(engine)
    next_boundary(engine)
    assert len(sdk.merges) == 1
    sdk.tx_state = "STATE_CONFIRMED"
    sdk.balances = lambda cid: (D(0), D(0)) if cid == "condition" else (D(5), D(5))
    next_boundary(engine)
    assert sdk.merges == [("condition", D(20)), ("other", D(5))]


def test_discovered_pairs_obtain_approval_before_merging(env):
    engine, client, sdk = env
    holdings(client, sdk)
    sdk.is_approved = False
    scan(engine)
    next_boundary(engine)
    assert sdk.approvals == ["condition"] and not sdk.merges
    sdk.is_approved = True
    sdk.tx_state = "STATE_CONFIRMED"
    next_boundary(engine)
    assert sdk.merges == [("condition", D(12))]


def test_unknown_merge_submission_is_not_retried_by_the_scanner(env):
    engine, client, sdk = env
    holdings(client, sdk)
    sdk.submit_timeout = True
    scan(engine)
    next_boundary(engine)
    for _ in range(3):
        scan(engine)
    assert len(sdk.merges) == 1
    assert engine.state["markets"]["condition"]["blocked"]


def test_persist_failure_never_submits_a_scan_merge(env, monkeypatch):
    engine, client, sdk = env
    holdings(client, sdk)
    monkeypatch.setattr(engine.journal, "write", Mock(side_effect=OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        scan(engine)
    assert engine.fatal and not engine.ready and not sdk.merges and not sdk.prepared


def test_merge_only_market_allows_the_existing_matched_trigger_for_new_source_fills(env):
    engine, client, sdk = env
    holdings(client, sdk, "20", "20")
    scan(engine)
    next_boundary(engine)
    market = engine.state["markets"]["condition"]
    sdk.tx_state = "STATE_CONFIRMED"
    sdk.wallet_balances = (D(0), D(0))
    next_boundary(engine)
    engine.trigger = "matched"
    event = trade("new-early", "new-source", "yes", "5", ".85", status="MATCHED")
    event["match_time"] = market["inventory_since"]
    engine.ingest(event)
    next_boundary(engine)
    assert not market["scan_only"] and sdk.prepared[-1]["shares"] == "5.00"


def test_undated_ws_is_deferred_to_rest_after_a_runtime_rebase(env):
    engine, client, sdk = env
    holdings(client, sdk)
    scan(engine)
    market = engine.state["markets"]["condition"]
    event = trade("undated")
    event.pop("match_time")
    before = copy.deepcopy(market["inventory"])
    engine.ingest(event)
    assert market["inventory"] == before and engine.last_poll == 0


def test_deferred_scan_with_no_ledger_exposure_survives_resume(env):
    engine, client, sdk = env
    holdings(client, sdk)
    engine.maker_busy = lambda tokens: True
    scan(engine)
    assert engine.state["markets"]["condition"]["merge_scan"]
    engine.close()
    recovered = HedgeExit(client, engine.cfg, engine.journal.path, lambda: None, sdk=sdk)
    try:
        assert recovered.blocks("yes")
        recovered.step()
        next_boundary(recovered)
        assert sdk.merges == [("condition", D(12))] and not sdk.prepared
    finally:
        recovered.close()


def test_snapshot_chain_read_failure_after_quiesce_preserves_original_cost(env, monkeypatch):
    engine, client, sdk = env
    market = fill_source(engine)
    holdings(client, sdk)
    monkeypatch.setattr(sdk, "balances", Mock(side_effect=[(D(20), D(12)), TimeoutError("after drain")]))
    scan(engine)
    assert D(market["inventory"]["yes"]["qty"]) == 20
    assert D(market["inventory"]["no"]["qty"]) == 0
    assert not market.get("merge_scan") and sdk.prepared[0]["shares"] == "20.00"
