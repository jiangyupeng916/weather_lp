#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""持仓超时强平（max_hold）单元测试 —— 纯逻辑，无网络无实盘。

只验证 check_positions 中新增的超时计时/触发/清理逻辑，通过桩替换所有
网络方法（positions/onchain_balance/open_orders）与 exec_layer，不连交易所。
"""

import sys
import time
from pathlib import Path
from decimal import Decimal
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from guardian import Guardian  # noqa: E402


class _FakeExec:
    def __init__(self):
        self.market_sells = []   # [(tid, shares)]
        self.limit_sells = []
        self.cancels = []

    def market_sell(self, tid, shares, tick):
        self.market_sells.append((tid, shares))
        return SimpleNamespace(_done=True)  # 占位 future，测试不消费

    def limit_sell(self, tid, shares, price, tick):
        self.limit_sells.append((tid, shares, price))
        return SimpleNamespace(_done=True)

    def cancel(self, oid, reason=""):
        self.cancels.append((oid, reason))


def _make_guardian(max_hold_hours=4.0, enabled=True):
    """不走 __init__（避免连交易所），手动装配 check_positions 所需的最小状态。"""
    g = Guardian.__new__(Guardian)
    import threading
    g.cfg = SimpleNamespace(
        max_hold_enabled=enabled,
        max_hold_hours=max_hold_hours,
        position_threshold=1.0,
        sell_min_bid_gap=Decimal("0.02"),
        tick_size=Decimal("0.01"),
        host="http://x",
    )
    g._holding_since = {}
    g._selling = set()
    g._sell_lock = threading.Lock()
    g._pending_ops_inbox = __import__("queue").Queue()
    g.exec_layer = _FakeExec()
    # 网络/IO 桩
    g._pos = []
    g._balances = {}
    g._orders = []
    g.positions = lambda: g._pos
    g.onchain_balance = lambda tid: g._balances.get(tid, 0.0)
    g.open_orders = lambda: g._orders
    g._chunk_list = staticmethod(lambda lst, n: [lst[i:i+n] for i in range(0, len(lst), n)])
    return g


def _drain_inbox(g):
    ops = []
    while True:
        try:
            ops.append(g._pending_ops_inbox.get_nowait())
        except Exception:
            break
    return ops


def test_first_seen_records_timestamp():
    """首次看到持仓 → 记录时间戳；未超时不卖。"""
    g = _make_guardian()
    g._pos = [{"asset": "tokA", "avgPrice": "0.5"}]
    g._balances = {"tokA": 100.0}
    # 无 best_ask 数据（不发网络），best_ask_map 为空 → maker 逻辑对 tokA 跳过
    g.check_positions()
    assert "tokA" in g._holding_since
    assert g.exec_layer.market_sells == []


def test_overdue_triggers_market_sell():
    """持有超过 max_hold_hours → FOK 市价全卖。"""
    g = _make_guardian(max_hold_hours=4.0)
    g._pos = [{"asset": "tokA", "avgPrice": "0.5"}]
    g._balances = {"tokA": 100.0}
    # 预置首见时间为 5 小时前
    g._holding_since["tokA"] = time.time() - 5 * 3600
    g.check_positions()
    assert g.exec_layer.market_sells == [("tokA", 100.0)]
    ops = _drain_inbox(g)
    assert any(op[2] == "market_sell" for op in ops)


def test_overdue_ignores_crash_protection():
    """超时强平忽略 sell_min_bid_gap：即便价格远低于成本也市价卖。"""
    g = _make_guardian(max_hold_hours=4.0)
    # avgPrice=0.9，若走 maker 逻辑会被崩盘保护拦截；超时应无视
    g._pos = [{"asset": "tokA", "avgPrice": "0.9"}]
    g._balances = {"tokA": 100.0}
    g._holding_since["tokA"] = time.time() - 5 * 3600
    g.check_positions()
    assert g.exec_layer.market_sells == [("tokA", 100.0)]


def test_disabled_switch_no_market_sell():
    """kill switch 关闭 → 即便超时也不市价卖。"""
    g = _make_guardian(max_hold_hours=4.0, enabled=False)
    g._pos = [{"asset": "tokA", "avgPrice": "0.5"}]
    g._balances = {"tokA": 100.0}
    g._holding_since["tokA"] = time.time() - 5 * 3600
    g.check_positions()
    assert g.exec_layer.market_sells == []


def test_sold_position_cleared_from_tracking():
    """持仓消失（卖光）→ 从 _holding_since 清理，避免内存泄漏。"""
    g = _make_guardian()
    g._holding_since["tokA"] = time.time() - 100
    g._holding_since["tokB"] = time.time() - 100
    g._pos = [{"asset": "tokA", "avgPrice": "0.5"}]  # tokB 已消失
    g._balances = {"tokA": 100.0}
    g.check_positions()
    assert "tokA" in g._holding_since
    assert "tokB" not in g._holding_since


def test_empty_positions_clears_all_tracking():
    """空持仓也要清理计时表（early-return 前处理）。"""
    g = _make_guardian()
    g._holding_since = {"tokA": time.time() - 100, "tokB": time.time() - 100}
    g._pos = []
    g.check_positions()
    assert g._holding_since == {}


def test_below_threshold_not_sold():
    """余额低于 position_threshold → 不市价卖（避免无效小单）。"""
    g = _make_guardian(max_hold_hours=4.0)
    g._pos = [{"asset": "tokA", "avgPrice": "0.5"}]
    g._balances = {"tokA": 0.5}  # < threshold 1.0
    g._holding_since["tokA"] = time.time() - 5 * 3600
    g.check_positions()
    assert g.exec_layer.market_sells == []


def test_balance_query_failure_skips_sale():
    """余额查询失败（onchain_balance 返回 None）→ 跳过而非崩溃或误卖。

    回归保护：若下游漏处理 None，`None <= 1.0` 会抛 TypeError；
    若退化为「None 当 0 处理」，则查不到余额时强平被静默跳过（逃生通道失效）。
    """
    g = _make_guardian(max_hold_hours=4.0)
    g._pos = [{"asset": "tokA", "avgPrice": "0.5"}]
    g._holding_since["tokA"] = time.time() - 5 * 3600  # 已超时
    g.onchain_balance = lambda tid: None                # 查询失败
    g.check_positions()                                 # 不应抛 TypeError
    assert g.exec_layer.market_sells == []
