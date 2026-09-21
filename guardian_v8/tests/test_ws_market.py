#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""市场频道 WS 集成单元测试 —— 纯逻辑，无网络无实盘。

覆盖 Round 2 新增的三块主线程逻辑：
  1. _apply_bid_change   —— REST/WS 共用的 bid→状态机（防漂移核心）
  2. _process_ws_bids    —— 消费 WS 队列 + 收集撤单
  3. _sync_ws_subscriptions —— _markets ↔ WS 订阅列表 diff
断线检测 _check_ws_connection 也一并验证（撤单 + 统计计数）。

所有 Guardian 实例用 object.__new__ 构造裸壳，只挂被测方法需要的属性，
绝不触发 __init__（不连 SDK、不发网络请求、不下真单）。
"""

import queue
import sys
import time
import types
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from guardian import Guardian  # noqa: E402
from models import ActorState, MarketState  # noqa: E402


# ── 测试脚手架 ────────────────────────────────────────────────────────────────

def _bare_guardian(maker_cooldown=120.0, ws_enabled=True):
    """构造不触发 __init__ 的裸 Guardian，只挂被测逻辑所需属性。"""
    g = object.__new__(Guardian)
    g.cfg = types.SimpleNamespace(maker_cooldown=maker_cooldown)
    g._markets = {}
    g._removed_by_screener = set()
    g._ws_enabled = ws_enabled
    g._ws_bid_queue = queue.Queue()
    # 断线统计
    g._ws_was_connected = False
    g._ws_disconnect_count = 0
    g._ws_total_downtime = 0.0
    g._ws_last_down_at = 0.0
    g._ws_started_at = time.time()
    # 捕获批量撤单调用（避免真的发单）
    g._batch_cancel_calls = []
    g._batch_cancel = lambda tids, reason="": g._batch_cancel_calls.append((list(tids), reason))
    return g


class _FakeMarketWS:
    """记录订阅调用的假 MarketWS。"""

    def __init__(self, subscribed=None, connected=True):
        self._subscribed = set(subscribed or [])
        self._connected = connected
        self.sub_calls = []
        self.unsub_calls = []

    def subscribed_ids(self):
        return set(self._subscribed)

    def subscribe_more(self, ids):
        self.sub_calls.append(list(ids))
        self._subscribed.update(ids)

    def unsubscribe(self, ids):
        self.unsub_calls.append(list(ids))
        for i in ids:
            self._subscribed.discard(i)

    def is_connected(self):
        return self._connected


# ── _apply_bid_change：init 分支 ──────────────────────────────────────────────

def test_apply_bid_init_with_ask():
    """首次 best_bid=None → 初始化 bid+ask，返回 (processed=True, cancel=False)。"""
    g = _bare_guardian()
    ms = MarketState(state=ActorState.RESTING)
    processed, needs_cancel = g._apply_bid_change(ms, "tok", Decimal("0.30"), Decimal("0.32"))
    assert processed is True
    assert needs_cancel is False
    assert ms.best_bid == Decimal("0.30")
    assert ms.best_ask == Decimal("0.32")


def test_apply_bid_init_ask_none_preserves_ask():
    """WS 路径 new_ask=None：init 时不写 ask（保持 None）。"""
    g = _bare_guardian()
    ms = MarketState(state=ActorState.RESTING)
    processed, _ = g._apply_bid_change(ms, "tok", Decimal("0.30"), None)
    assert processed is True
    assert ms.best_bid == Decimal("0.30")
    assert ms.best_ask is None


# ── _apply_bid_change：无变化 ─────────────────────────────────────────────────

def test_apply_bid_unchanged_noop():
    """bid 未变 → (False, False)，不动状态。"""
    g = _bare_guardian()
    ms = MarketState(state=ActorState.RESTING, best_bid=Decimal("0.30"))
    processed, needs_cancel = g._apply_bid_change(ms, "tok", Decimal("0.30"), None)
    assert processed is False
    assert needs_cancel is False
    assert ms.state is ActorState.RESTING


# ── _apply_bid_change：RESTING 变化 → 撤单 ────────────────────────────────────

def test_apply_bid_change_resting_triggers_cancel():
    """RESTING 下 bid 变化 → (True, True)，更新 bid。"""
    g = _bare_guardian()
    ms = MarketState(state=ActorState.RESTING, best_bid=Decimal("0.30"))
    processed, needs_cancel = g._apply_bid_change(ms, "tok", Decimal("0.31"), None)
    assert processed is True
    assert needs_cancel is True
    assert ms.best_bid == Decimal("0.31")


def test_apply_bid_change_resting_ask_none_preserves_old_ask():
    """WS 路径 bid 变化但 new_ask=None → 保留旧 ask（REST 对账写入的值）。"""
    g = _bare_guardian()
    ms = MarketState(state=ActorState.RESTING,
                     best_bid=Decimal("0.30"), best_ask=Decimal("0.35"))
    g._apply_bid_change(ms, "tok", Decimal("0.31"), None)
    assert ms.best_ask == Decimal("0.35")  # 未被覆盖


# ── _apply_bid_change：NO_ORDER + 冷却到期 ────────────────────────────────────

def test_apply_bid_change_no_order_cooldown_elapsed_starts_cooldown():
    """NO_ORDER 且冷却到期、未被筛选器移除 → 重启冷却（COOLING）。"""
    g = _bare_guardian()
    ms = MarketState(state=ActorState.NO_ORDER, best_bid=Decimal("0.30"),
                     cooldown_until=time.time() - 1)  # 已到期
    processed, needs_cancel = g._apply_bid_change(ms, "tok", Decimal("0.31"), None)
    assert processed is True
    assert needs_cancel is False
    assert ms.state is ActorState.COOLING
    assert ms.cooldown_until > time.time()


def test_apply_bid_change_no_order_removed_goes_stopped():
    """NO_ORDER + 冷却到期 + 在 _removed_by_screener → STOPPED。"""
    g = _bare_guardian()
    g._removed_by_screener.add("tok")
    ms = MarketState(state=ActorState.NO_ORDER, best_bid=Decimal("0.30"),
                     cooldown_until=time.time() - 1)
    processed, needs_cancel = g._apply_bid_change(ms, "tok", Decimal("0.31"), None)
    assert processed is True
    assert needs_cancel is False
    assert ms.state is ActorState.STOPPED


def test_apply_bid_change_no_order_cooldown_active_no_transition():
    """NO_ORDER 但冷却未到期 → 只更新 bid，不重启冷却。"""
    g = _bare_guardian()
    future = time.time() + 60
    ms = MarketState(state=ActorState.NO_ORDER, best_bid=Decimal("0.30"),
                     cooldown_until=future)
    processed, needs_cancel = g._apply_bid_change(ms, "tok", Decimal("0.31"), None)
    assert processed is True
    assert needs_cancel is False
    assert ms.state is ActorState.NO_ORDER
    assert ms.cooldown_until == future


# ── _process_ws_bids：队列消费 ────────────────────────────────────────────────

def test_process_ws_bids_drains_and_batches_cancels():
    """队列里两个 RESTING 变价 token → 一次批量撤单收集两者。"""
    g = _bare_guardian()
    g._markets["a"] = MarketState(state=ActorState.RESTING, best_bid=Decimal("0.30"))
    g._markets["b"] = MarketState(state=ActorState.RESTING, best_bid=Decimal("0.40"))
    g._ws_bid_queue.put(("a", Decimal("0.31")))
    g._ws_bid_queue.put(("b", Decimal("0.41")))
    g._process_ws_bids()
    assert len(g._batch_cancel_calls) == 1
    tids, reason = g._batch_cancel_calls[0]
    assert set(tids) == {"a", "b"}
    assert reason == "WS bid变化"


def test_process_ws_bids_skips_stopped_and_unknown():
    """STOPPED 市场与不在 _markets 的 token 都跳过，无撤单。"""
    g = _bare_guardian()
    g._markets["a"] = MarketState(state=ActorState.STOPPED, best_bid=Decimal("0.30"))
    g._ws_bid_queue.put(("a", Decimal("0.31")))       # STOPPED → 跳过
    g._ws_bid_queue.put(("ghost", Decimal("0.50")))   # 不存在 → 跳过
    g._process_ws_bids()
    assert g._batch_cancel_calls == []


def test_process_ws_bids_empty_queue_noop():
    """空队列 → 无异常、无撤单。"""
    g = _bare_guardian()
    g._process_ws_bids()
    assert g._batch_cancel_calls == []


# ── _process_ws_trades：有成交就撤单（全档位生效）─────────────────────────────

def _trade_guardian(cancel_on_trade, maker_rank=1):
    """裸 Guardian + 成交队列 + 策略开关。"""
    g = _bare_guardian()
    g.cfg.cancel_on_trade = cancel_on_trade
    g.cfg.maker_rank = maker_rank
    g._ws_trade_queue = queue.Queue()
    return g


def test_process_ws_trades_rank2_cancels():
    """RANK=2 + CANCEL_ON_TRADE=true → 撤单。

    本次放开了「仅 RANK=1 生效」的档位限制，这是新增行为。
    """
    g = _trade_guardian(True, maker_rank=2)
    g._markets["a"] = MarketState(state=ActorState.RESTING, active_id="oid-a")
    g._ws_trade_queue.put(("a", Decimal("0.50"), "BUY"))
    g._process_ws_trades()
    assert len(g._batch_cancel_calls) == 1
    tids, reason = g._batch_cancel_calls[0]
    assert tids == ["a"] and reason == "市场成交"


def test_process_ws_trades_rank1_still_cancels():
    """RANK=1 行为不变 —— 回归保护。"""
    g = _trade_guardian(True, maker_rank=1)
    g._markets["a"] = MarketState(state=ActorState.RESTING, active_id="oid-a")
    g._ws_trade_queue.put(("a", Decimal("0.50"), "SELL"))
    g._process_ws_trades()
    assert len(g._batch_cancel_calls) == 1
    assert g._batch_cancel_calls[0][1] == "市场成交"


def test_process_ws_trades_disabled_drains_without_cancel():
    """CANCEL_ON_TRADE=false → 不撤单，但队列仍被清空（不积压）。"""
    g = _trade_guardian(False, maker_rank=1)
    g._markets["a"] = MarketState(state=ActorState.RESTING, active_id="oid-a")
    g._ws_trade_queue.put(("a", Decimal("0.50"), "BUY"))
    g._process_ws_trades()
    assert g._batch_cancel_calls == []
    assert g._ws_trade_queue.empty(), "队列应被清空"


def test_process_ws_trades_skips_non_resting():
    """只有 RESTING 且有 active_id 的 token 才撤单；COOLING/无单 跳过。"""
    g = _trade_guardian(True, maker_rank=2)
    g._markets["cooling"] = MarketState(state=ActorState.COOLING, active_id="oid-c")
    g._markets["noorder"] = MarketState(state=ActorState.NO_ORDER)
    g._ws_trade_queue.put(("cooling", Decimal("0.5"), "BUY"))
    g._ws_trade_queue.put(("noorder", Decimal("0.5"), "BUY"))
    g._ws_trade_queue.put(("ghost", Decimal("0.5"), "BUY"))   # 不在 _markets
    g._process_ws_trades()
    assert g._batch_cancel_calls == []


# ── _sync_ws_subscriptions：订阅 diff ─────────────────────────────────────────

def test_sync_subscriptions_adds_new_markets():
    """_markets 有、WS 未订 → subscribe_more。"""
    g = _bare_guardian()
    g._market_ws = _FakeMarketWS(subscribed=[])
    g._markets = {"a": MarketState(), "b": MarketState()}
    g._sync_ws_subscriptions()
    assert set(g._market_ws.sub_calls[0]) == {"a", "b"}
    assert g._market_ws.unsub_calls == []


def test_sync_subscriptions_removes_stale():
    """WS 已订、_markets 没有 → unsubscribe。"""
    g = _bare_guardian()
    g._market_ws = _FakeMarketWS(subscribed=["a", "old"])
    g._markets = {"a": MarketState()}
    g._sync_ws_subscriptions()
    assert g._market_ws.unsub_calls[0] == ["old"]
    assert g._market_ws.sub_calls == []


def test_sync_subscriptions_noop_when_aligned():
    """集合一致 → 不发任何订阅/退订。"""
    g = _bare_guardian()
    g._market_ws = _FakeMarketWS(subscribed=["a", "b"])
    g._markets = {"a": MarketState(), "b": MarketState()}
    g._sync_ws_subscriptions()
    assert g._market_ws.sub_calls == []
    assert g._market_ws.unsub_calls == []


def test_sync_subscriptions_disabled_noop():
    """ws_enabled=False → 直接返回，不碰 MarketWS。"""
    g = _bare_guardian(ws_enabled=False)
    g._market_ws = _FakeMarketWS(subscribed=[])
    g._markets = {"a": MarketState()}
    g._sync_ws_subscriptions()
    assert g._market_ws.sub_calls == []


# ── _check_ws_connection：断线撤单 + 统计 ─────────────────────────────────────

def test_ws_disconnect_cancels_resting_and_counts():
    """连接 True→False：撤全部 RESTING（含 active_id）+ 断线计数 +1。"""
    g = _bare_guardian()
    g._market_ws = _FakeMarketWS(connected=False)
    g._ws_was_connected = True
    g._markets["a"] = MarketState(state=ActorState.RESTING, active_id="oid-a")
    g._markets["b"] = MarketState(state=ActorState.RESTING, active_id="oid-b")
    g._markets["c"] = MarketState(state=ActorState.COOLING)  # 非 RESTING 不撤
    now = time.time()
    g._check_ws_connection(now)
    assert g._ws_disconnect_count == 1
    assert g._ws_last_down_at == now
    assert g._ws_was_connected is False
    assert len(g._batch_cancel_calls) == 1
    tids, reason = g._batch_cancel_calls[0]
    assert set(tids) == {"a", "b"}
    assert reason == "WS断线"


def test_ws_reconnect_accumulates_downtime():
    """连接 False→True：累加本次断线时长，清 _ws_last_down_at。"""
    g = _bare_guardian()
    g._market_ws = _FakeMarketWS(connected=True)
    g._ws_was_connected = False
    g._ws_last_down_at = time.time() - 5.0  # 断了 5 秒
    g._check_ws_connection(time.time())
    assert g._ws_total_downtime >= 5.0
    assert g._ws_last_down_at == 0.0
    assert g._ws_was_connected is True
    assert g._batch_cancel_calls == []  # 重连不撤单


def test_ws_connection_stable_no_action():
    """持续连接（True→True）→ 无撤单、无计数变化。"""
    g = _bare_guardian()
    g._market_ws = _FakeMarketWS(connected=True)
    g._ws_was_connected = True
    g._check_ws_connection(time.time())
    assert g._ws_disconnect_count == 0
    assert g._batch_cancel_calls == []


def test_ws_disconnect_disabled_noop():
    """ws_enabled=False → 断线检测直接返回。"""
    g = _bare_guardian(ws_enabled=False)
    g._market_ws = _FakeMarketWS(connected=False)
    g._ws_was_connected = True
    g._check_ws_connection(time.time())
    assert g._ws_disconnect_count == 0
    assert g._batch_cancel_calls == []
