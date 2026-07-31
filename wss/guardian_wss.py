#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GuardianWss — Guardian + 市场频道 WebSocket（完整独立 bot）

继承 guardian.py 的 Guardian 类，覆盖 bid 轮询逻辑：
- 用 MarketWS 替代 _poll_best_bids 的 REST 轮询（亚秒级触发撤单）
- REST 降频至 30s 作为兜底核对
- WS 断线立即 cancel_all

其余功能（筛选器、挂单、持仓、心跳、卖单触发）完全继承，无需重复实现。

运行方式：
    python -m wss.main   # 从项目根目录执行
"""

from __future__ import annotations

import logging
import os
import queue
import time
from decimal import Decimal
from typing import List, Optional, Tuple

# 确保项目根目录在 sys.path（python -m wss.main 时已自动加入，此处作保险）
import sys
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from guardian import Guardian
from models import ActorState

from .cache import BidCache
from .market_ws import MarketWS

logger = logging.getLogger("wss.guardian_wss")


class GuardianWss(Guardian):
    """Guardian + 市场频道 WS 完整 bot。

    与 Guardian 的三处差异：
    1. _poll_best_bids()  → 降频至 30s REST 兜底（原 3s）
    2. _check_cooldowns() → 额外消费 WS bid 队列 + 同步 WS 订阅
    3. run()              → 包裹 MarketWS 的启动/停止
    """

    _WS_REST_RECONCILE_INTERVAL = 30.0  # REST 兜底间隔（秒）
    _WS_SUB_SYNC_INTERVAL       = 30.0  # WS 订阅核对间隔（秒）

    def __init__(self, cfg=None):
        super().__init__(cfg)

        # ── 市场频道 WS 配置（读 .env.wss） ─────────────────────────────────
        ws_market_url = os.environ.get(
            "WS_MARKET_URL",
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        )
        market_ping_interval = float(os.environ.get("MARKET_PING_INTERVAL", "10"))
        proxy_url = (
            os.environ.get("HTTPS_PROXY")
            or os.environ.get("HTTP_PROXY")
            or os.environ.get("ALL_PROXY")
            or None
        )

        # ── WS 组件 ─────────────────────────────────────────────────────────
        self._bid_cache = BidCache()
        self._market_ws = MarketWS(
            cache=self._bid_cache,
            url=ws_market_url,
            reconnect_delay=self.cfg.ws_reconnect_delay,
            ping_interval=market_ping_interval,
            proxy_url=proxy_url,
            on_bid_changed=self._enqueue_bid_change,
            on_disconnect=self._on_market_ws_disconnect,
            on_reconnect=self._on_market_ws_reconnect,
        )

        # 线程安全队列：WS 子线程写 → 主循环读
        self._ws_bid_queue: queue.Queue = queue.Queue()

        # 计时器（0.0 = 立即触发，首个 1s tick 就执行）
        self._last_rest_bid_poll = 0.0
        self._last_ws_sub_sync   = 0.0  # WS 订阅核对计时器

        logger.info(
            "[WssBot] 市场WS初始化完成 url=%s ping=%.0fs reconnect=%.0fs",
            ws_market_url, market_ping_interval, self.cfg.ws_reconnect_delay,
        )

    # ── WS 回调（运行在 WS 子线程，禁止直接读写 _markets） ─────────────────────

    def _enqueue_bid_change(
        self, token_id: str, old_bid: Optional[Decimal], new_bid: Decimal
    ) -> None:
        """bid 变化事件 → 入队列，由主循环消费。"""
        self._ws_bid_queue.put((token_id, new_bid))

    def _on_market_ws_disconnect(self) -> None:
        """WS 断线 → 立即 cancel_all，等待重连。

        exec_layer.cancel_all() 内部使用线程池，线程安全，可在 WS 线程调用。
        """
        logger.warning("[WssBot] 市场WS断线，立即取消全部挂单")
        self.exec_layer.cancel_all("市场WS断线")
        # 重置同步计时器：重连后立即重新核对订阅列表
        self._last_ws_sub_sync = 0.0

    def _on_market_ws_reconnect(self) -> None:
        """WS 重连成功 → 重置订阅同步计时器（由主循环在下一个 1s tick 触发同步）。

        MarketWS.on_open 已用 _subscribed_ids 自动重订阅，此处只需确保增量同步。
        """
        self._last_ws_sub_sync = 0.0
        logger.info("[WssBot] 市场WS重连成功，将在下一个tick同步订阅")

    # ── 主循环注入：_check_cooldowns 每 1s 被调用 ──────────────────────────────

    def _check_cooldowns(self, now: float) -> None:
        """覆盖：冷却检查 → 消费 WS bid 队列 → 同步 WS 订阅列表。"""
        super()._check_cooldowns(now)
        self._process_ws_bids()
        self._sync_ws_subscriptions(now)

    # ── WS bid 队列消费 ────────────────────────────────────────────────────────

    def _process_ws_bids(self) -> None:
        """主线程消费 WS bid 队列，逻辑与 _poll_best_bids 完全相同。

        唯一区别：bid 来自 WS 推送而非 REST，延迟从 ≤3s 降至毫秒级。
        """
        cancels: List[str] = []

        while True:
            try:
                token_id, new_bid = self._ws_bid_queue.get_nowait()
            except queue.Empty:
                break

            ms = self._markets.get(token_id)
            if not ms or ms.state is ActorState.STOPPED:
                continue

            # 首次初始化 best_bid（等同 _poll_best_bids 的 init 分支）
            if ms.best_bid is None:
                ms.best_bid = new_bid
                logger.debug("[WssBot] %s  bid初始化=%s", token_id[:16], new_bid)
                continue

            if new_bid == ms.best_bid:
                continue

            # bid 变化 → 更新缓存，触发状态机
            ms.best_bid = new_bid
            logger.debug(
                "[WssBot] %s  bid=%s state=%s", token_id[:16], new_bid, ms.state.name
            )

            if ms.state is ActorState.RESTING:
                # 有挂单，bid 变了 → 撤单后重挂
                cancels.append(token_id)
            elif ms.state is ActorState.NO_ORDER and ms.cooldown_until <= time.time():
                # 无挂单且不在冷却 → 重启冷却触发重新挂单
                if token_id in self._removed_by_screener:
                    ms.state = ActorState.STOPPED
                    ms.state_at = time.time()
                else:
                    self._start_cooldown(ms, self.cfg.maker_cooldown)

        if cancels:
            self._batch_cancel(cancels, "WS bid变化")

    # ── WS 订阅同步（每 30s 对齐 _markets ↔ MarketWS._subscribed_ids） ─────────

    def _sync_ws_subscriptions(self, now: float) -> None:
        """将 _markets 中的市场集合同步到 MarketWS 订阅列表。

        每 30s 执行一次，_last_ws_sub_sync=0.0 时立即执行（用于启动和重连）。
        覆盖场景：
        - 启动时 discover() 发现已有挂单的市场（1s 后即订阅）
        - 筛选器新增/移除市场（最迟 30s 内同步）
        - WS 重连后增量补订
        """
        if now - self._last_ws_sub_sync < self._WS_SUB_SYNC_INTERVAL:
            return
        self._last_ws_sub_sync = now

        current_tids = set(self._markets.keys())
        subscribed   = self._market_ws.subscribed_ids()

        to_sub   = current_tids - subscribed
        to_unsub = subscribed   - current_tids

        if to_sub:
            self._market_ws.subscribe_more(list(to_sub))
        if to_unsub:
            self._market_ws.unsubscribe(list(to_unsub))

        if to_sub or to_unsub:
            logger.debug(
                "[WssBot] WS订阅同步: 当前%d个市场 +%d -%d",
                len(current_tids), len(to_sub), len(to_unsub),
            )

    # ── 覆盖 _poll_best_bids：降频至 30s REST 兜底 ─────────────────────────────

    def _poll_best_bids(self) -> None:
        """REST 轮询降频至 30s。

        正常情况下 WS 处理所有 bid 变化；此处作为 WS 漏推送时的兜底。
        """
        now = time.time()
        if now - self._last_rest_bid_poll < self._WS_REST_RECONCILE_INTERVAL:
            return
        self._last_rest_bid_poll = now
        logger.debug("[WssBot] REST bid 核对（30s 兜底）")
        super()._poll_best_bids()

    # ── 覆盖 run()：在父类主循环外包裹 MarketWS 生命周期 ─────────────────────────

    def run(self) -> None:
        """启动市场 WS → 执行完整 Guardian 主循环 → 关闭市场 WS。"""
        logger.info("[WssBot] 启动市场WS...")
        self._market_ws.start()
        try:
            super().run()
        finally:
            logger.info("[WssBot] 停止市场WS...")
            self._market_ws.stop()
            self._market_ws.join(timeout=3.0)
