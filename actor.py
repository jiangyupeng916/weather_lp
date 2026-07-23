#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AssetActor — 单市场串行状态机

每个 AssetActor 管理一个市场的 Maker 挂单生命周期。
关键改进：
 - 非阻塞执行：place/cancel 通过 ExecutionLayer 异步执行
 - 撤单失败保护：取消失败时保持 RESTING 状态，不清除 active_id
 - tick_size 对齐：_target_price 返回 tick_size 对齐的价格
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Optional

from config import Config
from models import (
    ActorState,
    EventType,
    ActorEvent,
    OrderInfo,
)
from utils import safe_decimal, round_to_tick

if TYPE_CHECKING:
    from guardian import Guardian

logger = logging.getLogger("guardian.actor")


class AssetActor:
    def __init__(
        self,
        asset_id: str,
        guardian: "Guardian",
        initial: Optional[OrderInfo] = None,
    ):
        self.asset_id = asset_id
        self.guardian = guardian
        self.cfg: Config = guardian.cfg
        self._q: queue.Queue = queue.Queue()
        self._running = True
        self._worker = threading.Thread(target=self._loop, daemon=True)

        # ── 订单簿状态 ────────────────────────────────────────────────────────
        self.tick_size: Decimal = self.cfg.tick_size
        self.best_bid: Optional[Decimal] = None
        self.best_ask: Optional[Decimal] = None

        # ── 状态机 ────────────────────────────────────────────────────────────
        self._state_lock = threading.RLock()
        self._state = ActorState.NO_ORDER
        self._state_at = time.time()
        self.active_id: Optional[str] = None
        self.active_price: Optional[Decimal] = None
        self._cooldown_timer: Optional[threading.Timer] = None

        # ── 接管已有订单 ──────────────────────────────────────────────────────
        if initial:
            self.active_id = initial.order_id
            self.active_price = safe_decimal(initial.price)
            self._to(ActorState.RESTING, f"接管订单 price={initial.price}")

        self._worker.start()

    # ── 属性 ──────────────────────────────────────────────────────────────────
    @property
    def state(self) -> ActorState:
        with self._state_lock:
            return self._state

    @property
    def state_at(self) -> float:
        with self._state_lock:
            return self._state_at

    # ── 公共接口 ──────────────────────────────────────────────────────────────
    def post(self, event: ActorEvent):
        if not self._running:
            return
        self._q.put(event)

    def stop(self, cancel_active: bool = True):
        self.post(ActorEvent(EventType.STOP, {"cancel_active": cancel_active}))

    def force_stop(self):
        self._running = False
        self._clear_cooldown()
        self._worker.join(timeout=5)

    # ── 内部循环 ──────────────────────────────────────────────────────────────
    def _loop(self):
        logger.info("[ACTOR START] %s", self.asset_id[:16])
        while self._running:
            try:
                evt = self._q.get(timeout=5)
            except queue.Empty:
                continue
            try:
                self._handle(evt)
            except Exception as e:
                logger.error("[ACTOR ERR] %s %s | %s", self.asset_id[:16], evt.type.name, e, exc_info=True)
        logger.info("[ACTOR STOP] %s", self.asset_id[:16])

    def _handle(self, evt: ActorEvent):
        handlers = {
            EventType.BEST_BID: self._on_best_bid,
            EventType.STOP: self._on_stop,
            EventType.AUDIT: self._on_audit,
            EventType.COOLDOWN_EXPIRED: self._on_cooldown_expired,
            EventType.CANCEL_DONE: self._on_cancel_done,
            EventType.PLACE_DONE: self._on_place_done,
        }
        fn = handlers.get(evt.type)
        if fn:
            fn(evt.payload)

    # ── 状态机 ────────────────────────────────────────────────────────────────
    def _to(self, new: ActorState, reason: str = ""):
        with self._state_lock:
            old = self._state
            self._state = new
            self._state_at = time.time()
        logger.info("[STATE] %s %s -> %s | %s", self.asset_id[:16], old.name, new.name, reason)

    # ── 事件处理器 ────────────────────────────────────────────────────────────
    def _on_best_bid(self, p: dict):
        new_bid = safe_decimal(p.get("best_bid"))
        new_ask = safe_decimal(p.get("best_ask"))
        if new_bid is None:
            return

        # 首次 best_bid：只记录，不触发动作
        if self.best_bid is None:
            self.best_bid = new_bid
            self.best_ask = new_ask
            logger.info("[BID INIT] %s best_bid=%s", self.asset_id[:16], new_bid)
            return

        if new_bid == self.best_bid:
            return

        self.best_bid = new_bid
        self.best_ask = new_ask
        logger.info("[BID] %s best_bid=%s state=%s", self.asset_id[:16], new_bid, self.state.name)

        if self.state is ActorState.RESTING:
            self._cancel("best_bid变化")
        elif self.state is ActorState.NO_ORDER and self._cooldown_timer is None:
            self._start_cooldown()

    def _on_stop(self, p: dict):
        cancel_active = p.get("cancel_active", True)
        self._running = False
        self._clear_cooldown()
        logger.info("[STOP] %s", self.asset_id[:16])
        if cancel_active and self.active_id:
            self.guardian.exec_layer.cancel(self.active_id, "停止守护")
        self._to(ActorState.STOPPED, "已停止")

    def _on_audit(self, p: dict):
        now = time.time()
        orders_map: dict = p.get("orders", {})

        # 卡死的 PLACING/CANCELING 状态重置
        if self.state in (ActorState.CANCELING, ActorState.PLACING) and (now - self.state_at) > self.cfg.stale_timeout:
            logger.warning("[AUDIT] %s %s 超时重置", self.asset_id[:16], self.state.name)
            self.guardian.exec_layer.clear_place_by_asset(self.asset_id)
            self.active_id = None
            self.active_price = None
            self._to(ActorState.NO_ORDER, "审计超时")
            self._start_cooldown()
            return

        # RESTING 状态验证订单仍存在（使用 Guardian 传入的订单映射，避免重复 API 调用）
        if self.state is ActorState.RESTING and self.active_id:
            if self.active_id not in orders_map:
                logger.warning("[AUDIT] %s 订单丢失纠偏", self.asset_id[:16])
                self.active_id = None
                self.active_price = None
                self._to(ActorState.NO_ORDER, "审计纠偏-订单丢失")
                self._start_cooldown()

        # NO_ORDER 卡死强制重挂（COOLING 有定时器，不在此检测）
        if self.state == ActorState.NO_ORDER and (now - self.state_at) > self.cfg.stale_timeout:
            logger.warning("[AUDIT] %s NO_ORDER 卡死强制重挂", self.asset_id[:16])
            self._start_cooldown()

    def _on_cooldown_expired(self, _p: dict):
        if self.state is not ActorState.COOLING:
            logger.info("[COOLING ABORT] %s 状态已变为 %s", self.asset_id[:16], self.state.name)
            return
        target = self._target_price()
        if target is None:
            logger.warning("[RETRY] %s bids不足%d档，10s后重试", self.asset_id[:16], self.cfg.maker_rank)
            self._start_cooldown(duration=10.0)
            return
        self._place(target)

    # ── 动作 ──────────────────────────────────────────────────────────────────
    def _cancel(self, reason: str = ""):
        if not self.active_id:
            self._to(ActorState.NO_ORDER, f"无活动单 | {reason}")
            return

        oid = self.active_id
        self._to(ActorState.CANCELING, f"{oid[:20]}... | {reason}")

        fut = self.guardian.exec_layer.cancel(oid, reason)
        # 回调线程 post 结果回事件队列，不直接修改状态
        def _on_done():
            try:
                ok = fut.result(timeout=self.cfg.cancel_timeout)
            except Exception as e:
                ok = False
                logger.error("[CANCEL FUT] 超时: %s", e)
            self.post(ActorEvent(EventType.CANCEL_DONE, {"order_id": oid, "ok": ok, "reason": reason}))

        threading.Thread(target=_on_done, daemon=True).start()

    def _on_cancel_done(self, p: dict):
        oid = p.get("order_id", "")
        ok = p.get("ok", False)
        reason = p.get("reason", "")

        if not ok:
            if self.active_id is None:
                # active_id 已被 audit 清除，订单已不存在，忽略取消失败
                logger.warning("[CANCEL FAIL] %s 订单已不存在，忽略取消失败", oid[:20])
                return
            logger.error("[CANCEL FAIL] %s 订单仍存活在交易所！保持 RESTING", oid[:20])
            self._to(ActorState.RESTING, "撤单失败-订单仍存活")
            return

        self.active_id = None
        self.active_price = None
        if self.state is ActorState.COOLING:
            logger.info("[CANCEL OK] %s 已在 COOLING，跳过", oid[:20])
            return
        self._to(ActorState.NO_ORDER, f"撤单成功 | {reason}")
        self._start_cooldown()

    def _place(self, target: Decimal):
        self._to(ActorState.PLACING, f"target={target}")

        fut = self.guardian.exec_layer.place(self.asset_id, target, self.cfg.maker_size, self.tick_size)
        def _on_done():
            try:
                oid = fut.result(timeout=self.cfg.cancel_timeout)
            except Exception as e:
                oid = None
                logger.error("[PLACE FUT] 超时: %s", e)
            self.post(ActorEvent(EventType.PLACE_DONE, {
                "order_id": oid or "", "price": target, "ok": oid is not None,
            }))

        threading.Thread(target=_on_done, daemon=True).start()

    def _on_place_done(self, p: dict):
        oid = p.get("order_id", "")
        target = p.get("price")
        ok = p.get("ok", False)

        if ok and oid:
            self.active_id = oid
            self.active_price = target
            self._to(ActorState.RESTING, f"{oid[:20]}... price={target}")
            if target is not None:
                self.guardian.exec_layer.clear_place(self.asset_id, target)
        else:
            self.active_id = None
            self.active_price = None
            self._to(ActorState.NO_ORDER, "下单失败")
            if target is not None:
                self.guardian.exec_layer.clear_place(self.asset_id, target)
            self._start_cooldown()

    # ── 冷却 ──────────────────────────────────────────────────────────────────
    def _start_cooldown(self, duration: Optional[float] = None):
        dur = duration if duration is not None else self.cfg.maker_cooldown
        self._clear_cooldown()
        self._to(ActorState.COOLING, f"冷却 {dur:.0f}s")

        def _on_timer():
            if self._running and self.guardian.running:
                self.post(ActorEvent(EventType.COOLDOWN_EXPIRED))

        self._cooldown_timer = threading.Timer(dur, _on_timer)
        self._cooldown_timer.daemon = True
        self._cooldown_timer.start()

    def _clear_cooldown(self):
        if self._cooldown_timer:
            self._cooldown_timer.cancel()
            self._cooldown_timer = None

    # ── 价格计算 ──────────────────────────────────────────────────────────────
    def _target_price(self) -> Optional[Decimal]:
        """通过 REST API 获取订单簿买盘，返回第 maker_rank 档的价格。"""
        bids = self.guardian.get_order_book_bids(self.asset_id)
        if len(bids) < self.cfg.maker_rank:
            return None
        raw = bids[self.cfg.maker_rank - 1]
        return round_to_tick(raw, self.tick_size)



