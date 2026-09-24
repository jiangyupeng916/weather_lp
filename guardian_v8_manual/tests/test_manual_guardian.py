from concurrent.futures import Future
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import Config
from manual_guardian import ManualGuardian
from models import ActorState, OrderInfo


def completed(value):
    future = Future()
    future.set_result(value)
    return future


def order(oid="manual-1", tid="token-1"):
    return OrderInfo(oid, 0.42, 25, "BUY", tid)


def bot(tmp_path, monkeypatch):
    monkeypatch.setattr("manual_guardian.__file__", str(tmp_path / "manual_guardian.py"))
    client = SimpleNamespace(wallet="0xabc", credentials=SimpleNamespace(
        key="key", secret="secret", passphrase="passphrase"))
    guardian = ManualGuardian(Config(pk="dummy", ws_market_enabled=False), client=client)
    guardian.market_info = lambda _tid: {"title": "测试市场", "outcome": "Yes"}
    guardian.exec_layer.cancel = lambda *_args: completed(True)
    return guardian


def test_manual_cancel_stops_and_new_order_reenters(tmp_path, monkeypatch):
    guard = bot(tmp_path, monkeypatch)
    try:
        guard._apply_discover_result([order()])
        assert guard._markets["token-1"].state is ActorState.RESTING
        guard.ws_router._route_user({"event_type": "order", "type": "CANCELLATION",
                                    "id": "manual-1", "asset_id": "token-1"})
        guard._process_user_events()
        assert "token-1" not in guard._markets
        guard._apply_discover_result([order()])  # 旧 REST 快照不可重新接管
        assert "token-1" not in guard._markets
        guard._apply_discover_result([order("manual-2")])
        assert guard._markets["token-1"].active_id == "manual-2"
    finally:
        guard.exec_layer.shutdown()


def test_system_cancel_event_and_late_snapshot_do_not_exit(tmp_path, monkeypatch):
    guard = bot(tmp_path, monkeypatch)
    try:
        guard._apply_discover_result([order()])
        guard._apply_bid_change("token-1", Decimal("0.40"))  # 初始化
        guard._apply_bid_change("token-1", Decimal("0.41"))  # 追价撤单
        assert guard._markets["token-1"].state is ActorState.CANCELING
        guard.ws_router._route_user({"event_type": "order", "type": "CANCELLATION",
                                    "id": "manual-1", "asset_id": "token-1"})
        guard._process_user_events()
        guard._check_pending_ops()
        assert guard._markets["token-1"].state is ActorState.COOLING
        guard._apply_discover_result([order()])  # REST 最终一致性旧快照
        assert guard._markets["token-1"].state is ActorState.COOLING
    finally:
        guard.exec_layer.shutdown()


def test_missing_order_fails_closed_and_position_query_failure_keeps_timer(tmp_path, monkeypatch):
    guard = bot(tmp_path, monkeypatch)
    try:
        guard._apply_discover_result([order()])
        guard._apply_discover_result([])
        assert not guard._markets
        guard._holding_since["token-1"] = 123.0
        guard.positions = lambda: None
        guard.check_positions()
        assert guard._holding_since["token-1"] == 123.0
    finally:
        guard.exec_layer.shutdown()


def test_confirmed_buy_exits_and_queues_sell(tmp_path, monkeypatch):
    guard = bot(tmp_path, monkeypatch)
    try:
        guard._apply_discover_result([order()])
        payload = {"event_type": "trade", "type": "TRADE", "status": "MATCHED",
                   "id": "trade-1", "asset_id": "token-1", "side": "SELL", "price": "0.42",
                   "maker_orders": [{"order_id": "manual-1", "asset_id": "token-1",
                                     "matched_amount": "5", "price": "0.42"}]}
        guard.ws_router._route_user(payload)
        guard._process_user_events()
        assert "token-1" in guard._markets
        payload["status"] = "CONFIRMED"
        guard.ws_router._route_user(payload)
        guard._process_user_events()
        assert "token-1" not in guard._markets
        assert guard._pending_sell_tokens["token-1"] == Decimal("0.42")
    finally:
        guard.exec_layer.shutdown()


def test_cancel_event_before_discovery_blocks_stale_snapshot(tmp_path, monkeypatch):
    guard = bot(tmp_path, monkeypatch)
    try:
        guard.ws_router._route_user({"event_type": "order", "type": "CANCELLATION",
                                    "id": "0xABCD", "asset_id": "token-1"})
        guard._process_user_events()
        guard._apply_discover_result([order("abcd")])
        assert "token-1" not in guard._markets
    finally:
        guard.exec_layer.shutdown()
