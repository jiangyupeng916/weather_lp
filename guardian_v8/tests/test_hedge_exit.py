"""Offline safety scenarios: no SDK client construction, signing keys or network."""
import copy
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace as NS
from decimal import Decimal as D
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hedge_exit import HedgeExit, Journal, ensure_sell_safe, hedge_quote, own_legs


def book(bid="0.84", ask="0.86", tick="0.001", minimum="5"):
    return NS(bids=[NS(price=D(bid), size=D(100))] if bid else [],
              asks=[NS(price=D(ask), size=D(100))] if ask else [],
              tick_size=D(tick), min_order_size=D(minimum))


def trade(tid="fill-1", oid="reward-1", token="yes", qty="20", price="0.85",
          status="CONFIRMED", taker=False, side="BUY"):
    data = dict(id=tid, market="condition", asset_id=token, size=qty, price=price,
                status=status, side=side, match_time="1000", bucket_index=0,
                trader_side="TAKER" if taker else "MAKER", taker_order_id=oid,
                maker_address="wallet" if taker else "other", owner="key" if taker else "other-key")
    data["maker_orders"] = [] if taker else [dict(
        order_id=oid, asset_id=token, matched_amount=qty, price=price,
        side=side, maker_address="wallet", owner="key")]
    return data


def remote(oid, token="no", qty="20", matched="0", status="LIVE", associated=(), side="BUY"):
    return NS(id=oid, token_id=token, side=side, market="condition", price=D("0.16"),
              original_size=D(qty), size_matched=D(matched), status=status,
              associate_trades=associated)


class PaperClient:
    wallet = "wallet"
    credentials = NS(key="key")

    def __init__(self):
        self.orders = {}
        self.trades = {}
        self.positions = []
        self.books = {"yes": book(), "no": book("0.14", "0.16")}
        self.collateral = D(100_000_000)
        self.fail_reads = False

    def get_order_book(self, *, token_id):
        return self.books[token_id]

    def get_order(self, *, order_id):
        if self.fail_reads or order_id not in self.orders:
            raise TimeoutError("unavailable")
        return self.orders[order_id]

    def list_open_orders(self, **kwargs):
        return [NS(items=[o for o in self.orders.values() if o.status == "LIVE"])]

    def list_account_trades(self, **kwargs):
        if self.fail_reads:
            raise TimeoutError("trade query unavailable")
        items = list(self.trades.values())
        if "id" in kwargs:
            items = [t for t in items if t["id"] == kwargs["id"]]
        return [NS(items=items)]

    def list_positions(self, **kwargs):
        return [NS(items=self.positions)]

    def get_balance_allowance(self, **kwargs):
        assert kwargs == {"asset_type": "COLLATERAL"}, "Source balance must not gate a hedge"
        return NS(balance=self.collateral)


class PaperSDK:
    def __init__(self, client):
        self.client = client
        self.prepared = []
        self.posts = []
        self.cancels = []
        self.merges = []
        self.approvals = []
        self.sells = []
        self.sell_approvals = []
        self.is_sell_approved = True
        self.wallet_balances = (D(20), D(20))
        self.balance_reads = 0
        self.is_approved = True
        self.tx_state = "STATE_NEW"
        self.post_timeout = False
        self.accept_timeout = True
        self.reject = False
        self.submit_timeout = False

    def tokens(self, condition):
        return ("yes", "no")

    def prepare(self, condition, token, price, shares, passive, forced):
        oid = f"hedge-{len(self.prepared) + 1}"
        signed = dict(id=oid, token=token, price=str(price), shares=str(shares),
                      passive=passive, forced=forced)
        self.prepared.append(signed)
        return signed, oid

    def post(self, signed):
        self.posts.append(copy.deepcopy(signed))
        if not self.post_timeout or self.accept_timeout:
            self.client.orders.setdefault(signed["id"], remote(
                signed["id"], signed["token"], signed["shares"], side=signed.get("side", "BUY")))
        if self.post_timeout:
            raise TimeoutError("response lost")
        if self.reject:
            self.client.orders.pop(signed["id"], None)
            return NS(ok=False, code="post_only_would_cross")
        return NS(ok=True, order_id=signed["id"])

    def cancel(self, oid):
        self.cancels.append(oid)
        self.client.orders[oid].status = "CANCELED"

    def balances(self, condition):
        self.balance_reads += 1
        return self.wallet_balances

    def sell_balance(self, condition, token):
        return self.wallet_balances[0 if token == "yes" else 1]

    def prepare_sell(self, condition, token, price, shares, forced):
        signed = dict(id=f"sell-{len(self.sells) + 1}", token=token, side="SELL",
                      price=str(price), shares=str(shares), forced=forced)
        self.sells.append(signed)
        return signed, signed["id"]

    def approved(self, condition, *, sell=False, token=None):
        return self.is_sell_approved if sell else self.is_approved

    def approve(self, condition, *, sell=False, token=None):
        if sell:
            self.sell_approvals.append((condition, token))
            return NS(transaction_id="sell-approval-tx", transaction_hash=None)
        self.approvals.append(condition)
        return NS(transaction_id="approval-tx", transaction_hash=None)

    def merge(self, condition, shares):
        self.merges.append((condition, shares))
        if self.submit_timeout:
            raise TimeoutError("merge submission response lost")
        return NS(transaction_id="merge-tx", transaction_hash=None)

    def transaction_state(self, tx):
        return self.tx_state


@pytest.fixture
def env(tmp_path):
    client = PaperClient()
    sdk = PaperSDK(client)
    cfg = NS(hedge_poll_interval=5, position_interval=120, max_hold_enabled=False,
             max_hold_hours=4, sell_min_bid_gap=D("0.02"), sell_position_gap=D("0.02"))
    engine = HedgeExit(client, cfg, tmp_path / "state.json", lambda: None, sdk=sdk)
    engine.state.update(initialized=True, since=0)
    engine.last_poll = time.time()
    engine._persist()
    yield engine, client, sdk
    engine.stop()
    engine.close()


def fill_source(engine, **kwargs):
    engine.ingest(trade(**kwargs))
    return engine.state["markets"]["condition"]


def confirm_hedge(engine, client, qty="20", canceled=False):
    order = engine.state["markets"]["condition"]["order"]
    event = trade("hedge-fill", order["id"], "no", qty, order["price"], taker=True)
    client.trades[event["id"]] = event
    client.orders[order["id"]].size_matched = D(qty)
    client.orders[order["id"]].status = "CANCELED" if canceled else "MATCHED"
    engine.ingest(event)


@pytest.mark.parametrize("tick", ["0.1", "0.01", "0.005", "0.0025", "0.001", "0.0001"])
def test_flip_never_rounds_past_cap(tick):
    result = hedge_quote(book("0.831", "0.87"), book(tick=tick), D("0.85"), D("0.02"))
    assert result <= D("0.169")
    assert result % D(tick) == 0


def test_immediate_and_passive_threshold_flip():
    assert hedge_quote(book("0.83", "0.86"), book(), D("0.85"), D("0.02")) == D("0.17")
    assert hedge_quote(book("0.829", "0.86"), book(), D("0.85"), D("0.02")) is None
    assert hedge_quote(book("0.82", "0.86"), book(), D("0.85"), D("0.02"), passive=True) == D("0.14")
    assert hedge_quote(book("", ""), book(), D("0.85"), D("0.02")) is None


def test_exact_20_shares_without_source_balance_wait(env):
    engine, _, sdk = env
    fill_source(engine)
    engine.step()
    assert sdk.prepared[0]["shares"] == "20.00"
    assert sdk.prepared[0]["token"] == "no"
    assert sdk.prepared[0]["price"] == "0.16"
    assert sdk.balance_reads == 0


def test_duplicate_and_late_matched_do_not_create_more_exposure(env):
    engine, _, sdk = env
    fill_source(engine)
    fill_source(engine)
    engine.ingest(trade(status="MATCHED"))
    engine.step()
    assert sdk.prepared[0]["shares"] == "20.00"
    assert not engine.state["pending_trades"]
    engine.step()
    assert len(sdk.prepared) == 1


def test_failed_original_never_hedged(env):
    engine, _, sdk = env
    engine.ingest(trade(status="MATCHED"))
    engine.ingest(trade(status="FAILED"))
    engine.step()
    assert sdk.prepared == []


def test_per_maker_leg_token_and_quantity_preserved():
    data = trade()
    data["maker_orders"].append(dict(order_id="reward-2", asset_id="no", side="BUY",
                                    matched_amount="7", price="0.14", maker_address="wallet"))
    legs = list(own_legs(data, "wallet", "key"))
    assert [(x[1], x[3]) for x in legs] == [("yes", D(20)), ("no", D(7))]


def test_counterparty_leg_is_not_ours():
    assert list(own_legs(trade(), "someone-else", "different-key")) == []


def test_confirmed_opposite_fill_merges_without_recursive_buy(env):
    engine, client, sdk = env
    market = fill_source(engine)
    engine.step()
    confirm_hedge(engine, client)
    engine.step()
    assert sdk.merges == [("condition", D(20))]
    assert len(sdk.prepared) == 1
    assert market["inventory"]["yes"]["qty"] == "20"
    sdk.tx_state = "STATE_CONFIRMED"
    engine.step()
    assert D(market["inventory"]["yes"]["qty"]) == 0
    assert D(market["inventory"]["no"]["qty"]) == 0


def test_two_original_sides_net_before_buy(env):
    engine, _, sdk = env
    fill_source(engine)
    fill_source(engine, tid="source-no", oid="reward-no", token="no", qty="20", price="0.14")
    engine.step()
    assert not sdk.prepared
    assert sdk.merges == [("condition", D(20))]


def test_partial_fill_cancel_buys_only_remaining_before_merging(env):
    engine, client, sdk = env
    market = fill_source(engine)
    engine.step()
    confirm_hedge(engine, client, qty="12", canceled=True)
    engine.step()
    assert sdk.prepared[-1]["shares"] == "8.00"
    assert sdk.prepared[-1]["passive"] is True
    assert not sdk.merges


def test_cancel_race_waits_for_unsettled_fill(env):
    engine, client, sdk = env
    market = fill_source(engine)
    engine.step()
    oid = market["order"]["id"]
    client.orders[oid] = remote(oid, matched="6", status="CANCELED", associated=("late",))
    client.trades["late"] = trade("late", oid, "no", "6", "0.16", status="MATCHED", taker=True)
    market["next_quote"] = 0
    engine.step()
    assert len(sdk.prepared) == 1
    assert not sdk.merges
    client.trades["late"]["status"] = "CONFIRMED"
    engine.step()
    assert sdk.prepared[-1]["shares"] == "14.00"
    assert not sdk.merges


def test_self_cross_prevention_cancels_reward_and_accounts_late_fill(env):
    engine, client, sdk = env
    fill_source(engine)
    client.orders["reward-rest"] = remote("reward-rest", "yes")
    engine.step()
    assert sdk.cancels == ["reward-rest"]
    assert not sdk.prepared
    client.orders["reward-rest"].size_matched = D(3)
    client.orders["reward-rest"].associate_trades = ("late-source",)
    client.trades["late-source"] = trade("late-source", "reward-rest", qty="3", status="MATCHED")
    engine.step()
    assert not sdk.prepared
    client.trades["late-source"]["status"] = "CONFIRMED"
    engine.step()
    assert sdk.prepared[0]["shares"] == "23.00"


def test_inflight_reward_submission_gates_hedge(env):
    engine, _, sdk = env
    fill_source(engine)
    engine.maker_busy = lambda tokens: True
    engine.step()
    assert not sdk.prepared
    engine.maker_busy = lambda tokens: False
    engine.step()
    assert len(sdk.prepared) == 1


def test_timeout_after_acceptance_recovers_same_order(env):
    engine, _, sdk = env
    fill_source(engine)
    sdk.post_timeout = True
    engine.step()
    engine.step()
    assert len(sdk.prepared) == len(sdk.posts) == 1


def test_timeout_before_acceptance_replays_same_signature(env):
    engine, _, sdk = env
    fill_source(engine)
    sdk.post_timeout, sdk.accept_timeout = True, False
    engine.step()
    engine.step()
    assert len(sdk.prepared) == 1
    assert sdk.posts[0] == sdk.posts[1]


def test_unknown_rejection_cannot_release_an_order_reservation(env):
    engine, _, sdk = env
    market = fill_source(engine)
    sdk.post = lambda signed: NS(ok=False, code="unknown")
    engine.step()
    assert market["order"]["state"] == "unknown"
    market["next_quote"] = 0
    engine.step()
    assert len(sdk.prepared) == 1


def test_definite_post_only_rejection_releases_first_submission(env):
    engine, _, sdk = env
    market = fill_source(engine)
    sdk.reject = True
    engine.step()
    assert market["order"] is None


def test_unknown_order_survives_restart_and_cannot_switch_to_sell(env):
    engine, client, sdk = env
    fill_source(engine)
    sdk.post_timeout, sdk.accept_timeout = True, False
    engine.step()
    path = engine.journal.path
    engine.close()
    with pytest.raises(RuntimeError, match="Unfinished"):
        ensure_sell_safe(path)
    recovered = HedgeExit(client, engine.cfg, path, lambda: None, sdk=sdk)
    try:
        assert recovered.owns("hedge-1")
        recovered.last_poll = time.time()
        recovered.step()
        assert len(sdk.prepared) == 1
        assert sdk.posts[0] == sdk.posts[-1]
    finally:
        recovered.close()


def test_merge_never_resubmitted_while_pending_or_after_restart(env):
    engine, client, sdk = env
    fill_source(engine)
    fill_source(engine, tid="other", oid="other-order", token="no", price="0.15")
    engine.step()
    path = engine.journal.path
    engine.close()
    recovered = HedgeExit(client, engine.cfg, path, lambda: None, sdk=sdk)
    try:
        recovered.last_poll = time.time()
        recovered.step()
        assert len(sdk.merges) == 1
        sdk.tx_state = "STATE_CONFIRMED"
        recovered.step()
        assert len(sdk.merges) == 1
    finally:
        recovered.close()


def test_lost_merge_submission_response_freezes_market(env):
    engine, _, sdk = env
    market = fill_source(engine)
    fill_source(engine, tid="other", oid="other-order", token="no", price="0.15")
    sdk.submit_timeout = True
    engine.step()
    engine.step()
    assert len(sdk.merges) == 1
    assert market["blocked"]


def test_merge_waits_for_both_balances_and_operator_approval(env):
    engine, _, sdk = env
    fill_source(engine)
    fill_source(engine, tid="other", oid="other-order", token="no", price="0.15")
    sdk.wallet_balances = (D(20), D(0))
    engine.step()
    assert not sdk.merges
    sdk.wallet_balances = (D(20), D(20))
    sdk.is_approved = False
    engine.step()
    assert sdk.approvals == ["condition"]
    assert not sdk.merges
    sdk.is_approved, sdk.tx_state = True, "STATE_CONFIRMED"
    engine.step()
    assert len(sdk.merges) == 1


def test_insufficient_collateral_does_not_create_reservation(env):
    engine, client, sdk = env
    market = fill_source(engine)
    client.collateral = 1
    engine.step()
    assert not sdk.prepared and not market["order"]
    assert engine.blocks("yes") and engine.blocks("no")


def test_small_partial_fill_is_not_rounded_up(env):
    engine, _, sdk = env
    fill_source(engine, qty="4.999")
    engine.step()
    assert not sdk.prepared


def test_max_hold_uses_share_exact_aggressive_limit(env):
    engine, _, sdk = env
    market = fill_source(engine)
    engine.cfg.max_hold_enabled = True
    market["inventory"]["yes"]["first"] = time.time() - 20_000
    engine.step()
    assert sdk.prepared[0]["forced"] is True
    assert sdk.prepared[0]["shares"] == "20.00"
    assert sdk.prepared[0]["price"] == "0.16"


def test_forced_exit_uses_depth_for_requested_shares_without_cash_budget():
    opposite = book()
    opposite.asks = [NS(price=D("0.19"), size=D(12)), NS(price=D("0.17"), size=D(8))]
    assert hedge_quote(None, opposite, D("0.85"), D("0.02"), forced=True, shares=D(20)) == D("0.19")
    assert hedge_quote(None, opposite, D("0.85"), D("0.02"), forced=True, shares=D(5)) == D("0.17")


def test_multiple_market_merges_share_one_wallet_nonce(env):
    engine, _, sdk = env
    first = fill_source(engine)
    second = engine._market("condition-two")
    engine._submit_transaction("condition", first, D(10), True)
    engine._submit_transaction("condition-two", second, D(5), True)
    assert sdk.merges == [("condition", D(10))]
    assert second["tx"] is None


def test_merge_accounting_conflict_is_atomic(env):
    engine, _, sdk = env
    market = fill_source(engine)
    market["inventory"]["no"]["qty"] = "5"
    market["tx"] = dict(kind="merge", shares="10", id="merge-tx", hash=None)
    sdk.tx_state = "STATE_CONFIRMED"
    assert engine._transaction("condition", market)
    assert market["blocked"]
    assert market["inventory"]["yes"]["qty"] == "20"


def test_unexpected_overfill_is_blocked_not_recursively_hedged(env):
    engine, client, sdk = env
    market = fill_source(engine)
    engine.step()
    confirm_hedge(engine, client, qty="21")
    engine.step()
    assert market["blocked"]
    assert len(sdk.prepared) == 1


def test_status_command_hides_signed_payload(env):
    from hedge_status import summarize
    engine, _, _ = env
    fill_source(engine)
    engine.step()
    output = summarize(engine.state)
    assert output["markets"][0]["order"]["id"] == "hedge-1"
    assert "signed" not in json.dumps(output)


def test_trade_query_failure_never_means_empty_positions(env):
    engine, client, sdk = env
    fill_source(engine)
    client.fail_reads = True
    engine.last_poll = 0
    with pytest.raises(TimeoutError):
        engine.step()
    assert not sdk.prepared


def test_journal_write_failure_prevents_post(env, monkeypatch):
    engine, _, sdk = env
    fill_source(engine)
    monkeypatch.setattr(engine.journal, "write", Mock(side_effect=OSError("disk full")))
    with pytest.raises(OSError):
        engine.step()
    assert engine.fatal and not sdk.posts


def test_wallet_process_lock_and_corrupt_journal(tmp_path):
    path = tmp_path / "state.json"
    first = Journal(path)
    try:
        with pytest.raises(RuntimeError, match="already"):
            Journal(path)
        path.write_text("not-json", encoding="utf-8")
        with pytest.raises(json.JSONDecodeError):
            first.read()
    finally:
        first.close()


def test_first_activation_rejects_legacy_positions(env):
    engine, client, _ = env
    engine.state["initialized"] = False
    client.positions = [NS(condition_id="condition", size=D(20))]
    with pytest.raises(RuntimeError, match="empty outcome"):
        engine.step()


def test_order_ownership_filters_guardian_reward_management(env):
    from guardian import Guardian
    engine, _, _ = env
    fill_source(engine)
    engine.step()
    guardian = Guardian.__new__(Guardian)
    guardian.hedge_exit = engine
    orders = [NS(order_id="hedge-1"), NS(order_id="reward-1")]
    assert [o.order_id for o in guardian._reward_orders(orders)] == ["reward-1"]
    assert guardian._hedge_blocks("yes")
    assert guardian._hedge_blocks("no")
    guardian.positions = Mock(side_effect=AssertionError("Legacy seller must not run"))
    guardian.check_positions()


def test_user_ws_array_routes_every_fill():
    from ws_router import WSRouter
    guardian = NS(handle_trade=Mock(), handle_order=Mock())
    WSRouter(guardian).on_user_message(None, json.dumps([
        dict(event_type="trade", **trade()), dict(event_type="trade", **trade(tid="fill-2"))]))
    assert guardian.handle_trade.call_count == 2
