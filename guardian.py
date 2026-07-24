#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Guardian V7 — 主控制器（集中式状态管理）

职责：
 - 所有市场状态集中管理（Dict[str, MarketState]，主线程直读直写，无锁）
 - 定时任务：discover / poll_best_bids / audit / check_positions / cache_prune / cooldown / pending
 - 批量轮询 best_bid 替代 WebSocket 市场频道
 - 交易日志：记录 CONFIRMED 事件到 trade_logger
 - 启动/关闭编排（心跳先于订单，关闭时订单先于心跳）
"""

from __future__ import annotations

import csv
import json
import logging
import os
import random
import signal
import threading
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from eth_account import Account
from py_clob_client_v2 import (
    ClobClient,
    ApiCreds,
    OpenOrderParams,
    BalanceAllowanceParams,
    AssetType,
)

from config import Config
from models import ActorState, MarketState, OrderInfo
from utils import safe_float, safe_decimal, round_to_tick, safe_float_from_decimal
from heartbeat import HeartbeatManager
from execution import ExecutionLayer
from ws_manager import WSManager
from ws_router import WSRouter

logger = logging.getLogger("guardian")


def _file_logger(name: str) -> logging.Logger:
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(data_dir, exist_ok=True)
    lg = logging.getLogger(name)
    lg.setLevel(logging.INFO)
    lg.propagate = False
    h = logging.FileHandler(os.path.join(data_dir, f"{name}.log"), encoding="utf-8")
    h.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
    lg.addHandler(h)
    return lg


trade_logger = _file_logger("trades")


class Guardian:
    def __init__(self, cfg: Config = None):
        self.cfg = cfg or Config()
        self.running = True

        # ── 初始化 CLOB 客户端 ────────────────────────────────────────────────
        self.address = self.cfg.proxy or Account.from_key(self.cfg.pk).address
        self.creds = ApiCreds(
            api_key=self.cfg.api_key,
            api_secret=self.cfg.api_secret,
            api_passphrase=self.cfg.passphrase,
        )
        self.client = ClobClient(
            host=self.cfg.host,
            chain_id=self.cfg.chain_id,
            key=self.cfg.pk,
            creds=self.creds,
            funder=self.cfg.proxy or None,
            signature_type=self.cfg.signature_type,
        )

        # ── 组件初始化（按依赖顺序） ───────────────────────────────────────────
        self.heartbeat = HeartbeatManager(self.client, self.cfg, self.address, self.creds)
        self.exec_layer = ExecutionLayer(self.client, self.cfg)
        self.ws_manager = WSManager(self.cfg)
        self.ws_router = WSRouter(self)

        # ── 市场状态（集中式，主线程直读直写） ─────────────────────────────────
        self._markets: Dict[str, MarketState] = {}
        self._file_managed_ids: Set[str] = set()
        # pending: [(future, token_id, op_type, metadata), ...]
        self._pending_ops: List[Tuple[Any, str, str, Dict[str, Any]]] = []

        # ── 交易处理 ──────────────────────────────────────────────────────────
        self._sell_lock = threading.Lock()
        self._selling: Set[str] = set()
        self._processed_trades: Dict[str, float] = {}
        self._trade_lock = threading.Lock()

        # ── 缓存 ──────────────────────────────────────────────────────────────
        self._cache_lock = threading.RLock()
        self._market_info: Dict[str, dict] = {}

        # ── 信号处理 ──────────────────────────────────────────────────────────
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

        logger.info("=" * 60)
        logger.info("Maker-only Guardian V7.0 启动")
        logger.info("地址: %s | 档: %d | 量: %s | 冷却: %.0fs | 心跳: %.1fs",
                    self.address, self.cfg.maker_rank, self.cfg.maker_size,
                    self.cfg.maker_cooldown, self.cfg.heartbeat_interval)
        logger.info("=" * 60)

    # ── 信号处理 ──────────────────────────────────────────────────────────────
    def _on_signal(self, *_):
        logger.info("收到停止信号，优雅退出...")
        self.running = False

    # ── 查询 ──────────────────────────────────────────────────────────────────
    def open_orders(self) -> List[OrderInfo]:
        try:
            raw = self.client.get_open_orders(OpenOrderParams())
            return [
                OrderInfo(
                    order_id=o.get("id") or o.get("order_id", ""),
                    price=safe_float(o.get("price", 0)),
                    size=safe_float(o.get("size", 0)),
                    side=o.get("side", ""),
                    token_id=o.get("asset_id", ""),
                    market=o.get("market", ""),
                )
                for o in (raw or [])
            ]
        except Exception as e:
            logger.error("查询订单失败: %s", e)
            return []

    def market_info(self, asset_id: str) -> dict:
        with self._cache_lock:
            if asset_id in self._market_info:
                return self._market_info[asset_id]
        info = {"title": "未知", "outcome": ""}
        try:
            r = requests.get(f"{self.cfg.host}/markets-by-token/{asset_id}", timeout=10)
            if r.status_code == 200:
                d = r.json()
                item = d[0] if isinstance(d, list) and d else (d if isinstance(d, dict) else {})
                info["title"] = item.get("question", item.get("title", "未知"))
        except Exception:
            pass
        with self._cache_lock:
            self._market_info[asset_id] = info
        return info

    def get_order_book_bids(self, token_id: str) -> List[Decimal]:
        """通过 REST API 获取买盘价格列表，从高到低排序。"""
        try:
            ob = self.client.get_order_book(token_id)
            bids_raw = ob.get("bids", []) if ob else []
            prices: List[Decimal] = []
            for b in bids_raw:
                pr = safe_decimal(b.get("price"))
                sz = safe_decimal(b.get("size"))
                if pr and sz and sz > 0:
                    prices.append(pr)
            prices.sort(reverse=True)
            return prices
        except Exception as e:
            logger.error("获取订单簿失败: %s... | %s", token_id[:20], e)
            return []

    @staticmethod
    def _chunk_list(lst: List, size: int) -> List[List]:
        return [lst[i:i + size] for i in range(0, len(lst), size)]

    # ── 价格计算 ──────────────────────────────────────────────────────────────
    def _target_price(self, token_id: str) -> Optional[Decimal]:
        bids = self.get_order_book_bids(token_id)
        if len(bids) < self.cfg.maker_rank:
            return None
        raw = bids[self.cfg.maker_rank - 1]
        return round_to_tick(raw, self.cfg.tick_size)

    # ── 冷却 ──────────────────────────────────────────────────────────────────
    def _start_cooldown(self, ms: MarketState, duration: float):
        ms.state = ActorState.COOLING
        ms.state_at = time.time()
        ms.cooldown_until = time.time() + duration

    # ── 撤单动作 ──────────────────────────────────────────────────────────────
    def _trigger_cancel(self, token_id: str, reason: str = ""):
        ms = self._markets.get(token_id)
        if not ms:
            return
        if not ms.active_id:
            if ms.state != ActorState.COOLING:
                ms.state = ActorState.NO_ORDER
                ms.state_at = time.time()
            return
        oid = ms.active_id
        ms.state = ActorState.CANCELING
        ms.state_at = time.time()
        logger.info("[STATE] %s CANCELING %s... | %s", token_id[:16], oid[:20], reason)
        fut = self.exec_layer.cancel(oid, reason)
        self._pending_ops.append((fut, token_id, "cancel", {"order_id": oid, "reason": reason}))

    # ── 批量撤单 ──────────────────────────────────────────────────────────────
    def _batch_cancel(self, token_ids: List[str], reason: str = ""):
        """收集 market state 中的 active_id，批量发 DELETE /orders。"""
        order_ids = []
        for tid in token_ids:
            ms = self._markets.get(tid)
            if ms and ms.active_id and ms.state is not ActorState.CANCELING:
                order_ids.append(ms.active_id)
                ms.state = ActorState.CANCELING
                ms.state_at = time.time()
        if order_ids:
            logger.info("[BATCH CANCEL] 批量撤单 %d 个 | %s", len(order_ids), reason)
            fut = self.exec_layer.cancel_batch(order_ids, reason)
            self._pending_ops.append((fut, "_batch_", "cancel_batch",
                                      {"order_ids": order_ids, "reason": reason}))

    def _handle_batch_cancel_result(self, token_ids: List[str], reason: str,
                                      result: dict):
        """根据批量撤单结果更新状态：成功的进入冷却，失败的也进入冷却（让 audit/poll 下轮重查）。

        失败分支也进入冷却而非回退 RESTING，避免 SDK 返回 id 格式不一致时
        形成死循环。同时清空 active_id，让下一轮 poll/audit 重新发现真实状态。
        """
        canceled_set = set(result.get("canceled", []))
        for tid in token_ids:
            ms = self._markets.get(tid)
            if not ms or ms.state is not ActorState.CANCELING:
                continue
            if ms.active_id in canceled_set:
                logger.info("[BATCH CANCEL] %s 取消成功", tid[:16])
            else:
                logger.error("[BATCH CANCEL] %s 取消失败，清空 active_id 进入冷却兜底",
                             tid[:16])
            ms.active_id = None
            ms.active_price = None
            ms.state = ActorState.NO_ORDER
            ms.state_at = time.time()
            self._start_cooldown(ms, self.cfg.maker_cooldown)

    # ── 挂单动作（异步两步） ──────────────────────────────────────────────────
    def _trigger_place(self, token_id: str):
        """Step 1: 异步获取订单簿，避免 GET /book 阻塞主循环。"""
        ms = self._markets.get(token_id)
        if not ms:
            return
        # 流程级兜底：进入 PLACING 前确认该 token 没有任何残留活跃 BUY 订单
        if self._has_existing_buy_order(token_id):
            logger.warning("[PLACE GUARD] %s 已有活跃买单，跳过本次挂单",
                           token_id[:16])
            self._start_cooldown(ms, self.cfg.maker_cooldown)
            return
        ms.state = ActorState.PLACING
        ms.state_at = time.time()
        fut = self.exec_layer.run_async(self._target_price, token_id)
        self._pending_ops.append((fut, token_id, "target_price", {}))

    def _has_existing_buy_order(self, token_id: str) -> bool:
        """查询该 token_id 是否已有活跃 BUY 订单（含孤儿订单）。"""
        try:
            raw = self.client.get_open_orders(OpenOrderParams(asset_id=token_id))
        except Exception as e:
            logger.warning("[PLACE GUARD] %s 查询失败: %s，保守放行",
                            token_id[:16], e)
            return False
        for o in (raw or []):
            if str(o.get("side", "")).upper() == "BUY":
                return True
        return False

    # ── 异步结果处理 ──────────────────────────────────────────────────────────
    def _has_pending_op(self, token_id: str) -> bool:
        """检查该 token 是否有未完成的异步操作。"""
        for fut, tid, _op, _meta in self._pending_ops:
            if tid == token_id and not fut.done():
                return True
        return False

    def _handle_cancel_result(self, ms: MarketState, token_id: str, ok: bool,
                               order_id: str, reason: str):
        if not ok:
            if ms.active_id is None:
                logger.warning("[CANCEL FAIL] %s 订单已不存在，忽略取消失败", order_id[:20])
                return
            logger.error("[CANCEL FAIL] %s 订单仍存活在交易所！保持 RESTING", order_id[:20])
            ms.state = ActorState.RESTING
            ms.state_at = time.time()
            return

        logger.info("[CANCEL OK] %s... | %s", order_id[:20], reason)
        ms.active_id = None
        ms.active_price = None
        if ms.state == ActorState.COOLING:
            return
        ms.state = ActorState.NO_ORDER
        ms.state_at = time.time()
        self._start_cooldown(ms, self.cfg.maker_cooldown)

    def _handle_place_result(self, ms: MarketState, token_id: str, ok: bool,
                              order_id: Optional[str], price: Decimal):
        if ok and order_id:
            ms.active_id = order_id
            ms.active_price = price
            ms.state = ActorState.RESTING
            ms.state_at = time.time()
            logger.info("[STATE] %s RESTING %s... price=%s", token_id[:16], order_id[:20], price)
            self.exec_layer.clear_place(token_id, price)
        else:
            logger.error("[PLACE FAIL] %s price=%s", token_id[:16], price)
            ms.active_id = None
            ms.active_price = None
            ms.state = ActorState.NO_ORDER
            ms.state_at = time.time()
            self.exec_layer.clear_place(token_id, price)
            self._start_cooldown(ms, self.cfg.maker_cooldown)

    # ── 定时检查 ──────────────────────────────────────────────────────────────
    def _check_cooldowns(self, now: float):
        for token_id, ms in list(self._markets.items()):
            if ms.state == ActorState.COOLING and now >= ms.cooldown_until:
                self._trigger_place(token_id)

    def _check_pending_ops(self, now: float):
        completed = []
        for i, (fut, token_id, op, meta) in enumerate(self._pending_ops):
            if not fut.done():
                continue
            completed.append(i)
            ms = self._markets.get(token_id)
            if not ms:
                continue

            try:
                result = fut.result(timeout=0)
            except Exception:
                result = False if op == "cancel" else None

            if op == "cancel":
                self._handle_cancel_result(ms, token_id, bool(result),
                                            meta["order_id"], meta.get("reason", ""))
            elif op == "cancel_batch":
                if isinstance(result, dict):
                    self._handle_batch_cancel_result(meta["order_ids"],
                                                      meta.get("reason", ""), result)
                else:
                    # 批量撤单异常：统一清空 active_id 进入冷却兜底
                    for tid in meta["order_ids"]:
                        ms_sub = self._markets.get(tid)
                        if ms_sub and ms_sub.state is ActorState.CANCELING:
                            ms_sub.active_id = None
                            ms_sub.active_price = None
                            ms_sub.state = ActorState.NO_ORDER
                            ms_sub.state_at = time.time()
                            self._start_cooldown(ms_sub, self.cfg.maker_cooldown)
            elif op == "target_price":
                # Step 2: 拿到订单簿价格后，提交实际下单
                target = result
                if target is None:
                    logger.warning("[RETRY] %s bids不足%d档，10s后重试",
                                   token_id[:16], self.cfg.maker_rank)
                    self._start_cooldown(ms, 10.0)
                else:
                    logger.info("[STATE] %s PLACING target=%s", token_id[:16], target)
                    fut = self.exec_layer.place(token_id, target,
                                                self.cfg.maker_size, self.cfg.tick_size)
                    self._pending_ops.append((fut, token_id, "place", {"price": target}))
            elif op == "place":
                success = result is not None
                self._handle_place_result(ms, token_id, success,
                                           result if success else None, meta["price"])

        for i in reversed(completed):
            self._pending_ops.pop(i)

    # ── 市场文件同步 ──────────────────────────────────────────────────────────
    def _load_market_targets(self) -> Optional[List[Tuple[str, str]]]:
        """读取市场筛选 CSV 文件，返回 [(token_id, title), ...] 或 None（出错时）。"""
        csv_path = self.cfg.market_file
        if not csv_path:
            return None
        try:
            with open(csv_path, newline="", encoding="utf-8") as f:
                r = csv.reader(f)
                headers = [h.strip() for h in next(r)]
                targets = []
                for row in r:
                    d = {headers[i]: row[i].strip() for i in range(min(len(headers), len(row)))}
                    yes_id = d.get("yes_token_id", "")
                    no_id = d.get("no_token_id", "")
                    title = d.get("Market", "")
                    if yes_id:
                        targets.append((yes_id, f"{title} [YES]" if title else ""))
                    if no_id:
                        targets.append((no_id, f"{title} [NO]" if title else ""))
                return targets
        except Exception as e:
            logger.error("[SYNC] 读取市场文件失败: %s", e)
            return None

    def _sync_from_file(self):
        """从 CSV 文件同步市场：新增则创建状态，移除则停止监控并取消订单。"""
        csv_path = self.cfg.market_file
        if not csv_path:
            return
        targets = self._load_market_targets()
        if targets is None:
            return
        csv_ids = {t[0] for t in targets}

        # 移除：CSV 中不再存在的 file-managed 市场
        removed = self._file_managed_ids - csv_ids
        for token_id in list(removed):
            ms = self._markets.get(token_id)
            if ms and ms.active_id:
                if ms.state is ActorState.CANCELING or self._has_pending_op(token_id):
                    logger.info("[SYNC] %s 正在撤单中，延后移除", token_id[:20])
                    continue
                logger.info("[SYNC] 市场已从文件移除，停止监控 %s", token_id[:20])
                self._trigger_cancel(token_id, "从CSV移除")
                ms.active_id = None
                ms.active_price = None
            self._markets.pop(token_id, None)
            self._file_managed_ids.discard(token_id)

        # 新增：CSV 中新出现的市场
        for token_id, title in targets:
            if token_id not in self._markets:
                logger.info("[SYNC] 新市场 %s | %s", token_id[:20], title[:50])
                ms = MarketState()
                self._markets[token_id] = ms
                stagger = random.uniform(self.cfg.cooldown_delay, 30.0)
                self._start_cooldown(ms, stagger)
            self._file_managed_ids.add(token_id)

    # ── 订单发现 ──────────────────────────────────────────────────────────────
    def discover(self):
        orders = self.open_orders()
        buys = {o.token_id: o for o in orders if o.side.upper() == "BUY"}
        now = time.time()

        # 清理 STOPPED 状态市场
        stopped = [tid for tid, ms in self._markets.items() if ms.state == ActorState.STOPPED]
        for tid in stopped:
            self._markets.pop(tid, None)
            self._file_managed_ids.discard(tid)
            logger.info("[DISCOVER] 清理 STOPPED 市场 %s", tid[:20])

        # 发现新市场（已有挂单）
        for tid in set(buys.keys()) - set(self._markets.keys()):
            o = buys[tid]
            logger.info("[DISCOVER] 新市场 %s price=%s", tid[:20], o.price)
            self._markets[tid] = MarketState(
                state=ActorState.RESTING,
                state_at=now,
                active_id=o.order_id,
                active_price=safe_decimal(o.price),
            )

        # 从 CSV 文件同步
        self._sync_from_file()

        logger.info("[DISCOVER] 守护 %d 个市场", len(self._markets))

    # ── 批量轮询最佳买价 ──────────────────────────────────────────────────────
    def _poll_best_bids(self):
        token_ids = list(self._markets.keys())
        if not token_ids:
            return

        polled = 0
        cancels: List[str] = []
        for chunk in self._chunk_list(token_ids, 500):
            try:
                r = requests.post(
                    f"{self.cfg.host}/books",
                    json=[{"token_id": tid} for tid in chunk],
                    timeout=10,
                )
                if r.status_code != 200:
                    logger.error("[POLL] 批量查询失败 HTTP %s", r.status_code)
                    continue
                books = r.json()
            except Exception as e:
                logger.error("[POLL] 批量查询异常: %s", e)
                continue

            for item in (books if isinstance(books, list) else []):
                aid = item.get("asset_id", "")
                ms = self._markets.get(aid)
                if not ms or ms.state is ActorState.STOPPED:
                    continue

                bids = item.get("bids", [])
                if not bids:
                    continue

                best_bid_str = bids[-1].get("price", "")
                best_ask_str = ""
                asks = item.get("asks", [])
                if asks:
                    best_ask_str = asks[-1].get("price", "")

                if not best_bid_str:
                    continue

                new_bid = safe_decimal(best_bid_str)
                new_ask = safe_decimal(best_ask_str)

                if ms.best_bid is None:
                    ms.best_bid = new_bid
                    ms.best_ask = new_ask
                    logger.info("[BID INIT] %s best_bid=%s", aid[:16], new_bid)
                    polled += 1
                    continue

                if new_bid == ms.best_bid:
                    continue

                ms.best_bid = new_bid
                ms.best_ask = new_ask
                logger.info("[BID] %s best_bid=%s state=%s", aid[:16], new_bid, ms.state.name)
                polled += 1

                if ms.state is ActorState.RESTING:
                    cancels.append(aid)
                elif ms.state is ActorState.NO_ORDER and ms.cooldown_until <= time.time():
                    self._start_cooldown(ms, self.cfg.maker_cooldown)

        if cancels:
            self._batch_cancel(cancels, "best_bid变化")

        logger.debug("[POLL] 轮询 %d 个 Actor", polled)

    # ── 持仓查询 ──────────────────────────────────────────────────────────────
    def positions(self) -> List[dict]:
        try:
            r = requests.get(
                f"{self.cfg.data_api}/positions",
                params={"user": self.address, "sizeThreshold": self.cfg.position_threshold},
                timeout=10,
            )
            return r.json() if r.status_code == 200 else []
        except Exception as e:
            logger.error("查询持仓失败: %s", e)
            return []

    def onchain_balance(self, token_id: str) -> float:
        try:
            bal = self.client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            )
            val = bal.get("balance")
            if val is None:
                return 0.0
            return int(val) / 1_000_000
        except Exception as e:
            logger.error("余额查询失败: %s", e)
            return 0.0

    # ── 审计 ──────────────────────────────────────────────────────────────────
    def audit(self):
        logger.info("[AUDIT] 开始...")
        orders = self.open_orders()
        order_map = {o.order_id: o for o in orders}
        buys = [o for o in orders if o.side.upper() == "BUY"]
        now = time.time()

        # 批量查询 best_bid
        best_bid_map: Dict[str, Decimal] = {}
        if buys:
            for chunk in self._chunk_list(list({o.token_id for o in buys}), 500):
                try:
                    r = requests.post(
                        f"{self.cfg.host}/books",
                        json=[{"token_id": tid} for tid in chunk],
                        timeout=10,
                    )
                    if r.status_code == 200:
                        for item in r.json():
                            bids = item.get("bids", [])
                            if bids:
                                best_bid_map[item["asset_id"]] = Decimal(bids[-1].get("price", "0"))
                except Exception as e:
                    logger.error("[AUDIT] 批量查询 best_bid 失败: %s", e)

        # 逐个市场审计
        for token_id, ms in list(self._markets.items()):
            # 有 pending Future 的市场跳过——状态机正在变更中，不应干预
            if self._has_pending_op(token_id):
                logger.debug("[AUDIT] %s 有 pending 操作，跳过", token_id[:16])
                continue

            # 卡死的 PLACING/CANCELING 状态重置
            if ms.state in (ActorState.CANCELING, ActorState.PLACING) \
                    and (now - ms.state_at) > self.cfg.stale_timeout:
                logger.warning("[AUDIT] %s %s 超时重置", token_id[:16], ms.state.name)
                self.exec_layer.clear_place_by_asset(token_id)
                ms.active_id = None
                ms.active_price = None
                ms.state = ActorState.NO_ORDER
                ms.state_at = now
                self._start_cooldown(ms, self.cfg.maker_cooldown)
                continue

            # RESTING 状态验证订单仍存在
            if ms.state is ActorState.RESTING and ms.active_id:
                if ms.active_id not in order_map:
                    logger.warning("[AUDIT] %s 订单丢失纠偏", token_id[:16])
                    ms.active_id = None
                    ms.active_price = None
                    ms.state = ActorState.NO_ORDER
                    ms.state_at = now
                    self._start_cooldown(ms, self.cfg.maker_cooldown)

            # NO_ORDER 卡死强制重挂
            if ms.state is ActorState.NO_ORDER \
                    and (now - ms.state_at) > self.cfg.stale_timeout:
                logger.warning("[AUDIT] %s NO_ORDER 卡死强制重挂", token_id[:16])
                self._start_cooldown(ms, self.cfg.maker_cooldown)

        # 纠偏超价订单
        overpriced = []
        for o in buys:
            bb = best_bid_map.get(o.token_id)
            if bb is not None:
                o_price_dec = Decimal(str(o.price))
                if o_price_dec >= bb:
                    overpriced.append(o.token_id)

        if overpriced:
            self._batch_cancel(overpriced, "审计纠偏")
        logger.info("[AUDIT] 完成 %d 买单检查 | 纠偏 %d 个", len(buys), len(overpriced))

    # ── 交易处理 ──────────────────────────────────────────────────────────────
    def handle_trade(self, data: dict):
        tid = data.get("id", "")
        status = str(data.get("status", "")).upper()
        asset_id = data.get("asset_id") or data.get("token_id", "")
        price = safe_float(data.get("price", 0))
        side = str(data.get("side", "")).upper()
        outcome = data.get("outcome", "")

        with self._trade_lock:
            if tid in self._processed_trades:
                return

        logger.info("[TRADE] id=%s status=%s side=%s", tid[:16], status, side)

        if not asset_id or status != "CONFIRMED":
            return

        with self._trade_lock:
            self._processed_trades[tid] = time.time()

        maker_orders = data.get("maker_orders") or []
        our_fill = sum(
            safe_float(m.get("matched_amount", 0))
            for m in maker_orders
            if (m.get("owner") or m.get("order_owner", "")) == self.cfg.api_key
        )

        mi = self.market_info(asset_id)
        if side == "BUY":
            trade_logger.info(json.dumps({
                "buy_confirmed": {
                    "trade_id": tid, "token_id": asset_id, "side": "BUY",
                    "size": our_fill, "price": price,
                    "title": mi.get("title", "未知"), "outcome": outcome,
                }
            }, ensure_ascii=False))
            logger.info("[BUY CONFIRMED] %s | %s | size=%.4f price=%s",
                        mi.get("title", "未知")[:40], outcome, our_fill, price)
        else:
            trade_logger.info(json.dumps({
                "sell_confirmed": {
                    "trade_id": tid, "token_id": asset_id, "side": "SELL",
                    "size": our_fill, "price": price,
                    "title": mi.get("title", "未知"), "outcome": outcome,
                }
            }, ensure_ascii=False))
            logger.info("[SELL CONFIRMED] %s | %s | size=%.4f price=%s",
                        mi.get("title", "未知")[:40], outcome, our_fill, price)

    # ── 订单事件 ──────────────────────────────────────────────────────────────
    def handle_order(self, data: dict):
        otype = str(data.get("type", ""))
        oid = str(data.get("id", ""))
        logger.debug("[ORDER EVENT] type=%s id=%s", otype, oid[:20])

    # ── 持仓兜底 ──────────────────────────────────────────────────────────────
    def check_positions(self):
        pos_list = self.positions()
        if not pos_list:
            return

        orders = self.open_orders()
        sell_tokens = {o.token_id for o in orders if o.side.upper() == "SELL"}

        logger.info("[POSITION] 发现 %d 个持仓", len(pos_list))
        placed = 0
        for p in pos_list:
            tid = p.get("asset", "")
            if not tid:
                continue

            if tid in sell_tokens:
                continue

            with self._sell_lock:
                if tid in self._selling:
                    continue
                self._selling.add(tid)

            try:
                bal = self.onchain_balance(tid)
                if bal <= self.cfg.position_threshold:
                    continue

                logger.info("[MARKET SELL] %s | %.4f shares",
                            p.get("title", "未知")[:40], bal)

                fut = self.exec_layer.market_sell(tid, bal, self.cfg.tick_size)
                try:
                    sell_oid = fut.result(timeout=self.cfg.place_timeout)
                except Exception:
                    sell_oid = None

                if sell_oid:
                    trade_logger.info(json.dumps({
                        "buy": {
                            "token_id": tid, "side": "BUY",
                            "size": bal,
                            "price": safe_float(p.get("avgPrice", 0)),
                            "title": p.get("title", ""), "outcome": p.get("outcome", ""),
                            "source": "polling",
                        },
                        "sell": {
                            "order_id": sell_oid, "token_id": tid, "side": "SELL",
                            "size": bal, "type": "MARKET_FOK",
                        },
                    }, ensure_ascii=False))
                    logger.info("[MARKET SELL OK] %s id=%s", tid[:20], str(sell_oid)[:20])
                    placed += 1
                else:
                    logger.error("[MARKET SELL FAIL] %s 市价卖单失败", tid[:20])
            finally:
                with self._sell_lock:
                    self._selling.discard(tid)

            time.sleep(0.3)

        if placed:
            logger.info("[POSITION] 市价卖出 %d 个", placed)

    # ── 缓存清理 ──────────────────────────────────────────────────────────────
    def _prune_caches(self):
        with self._cache_lock:
            if len(self._market_info) > self.cfg.cache_max_size:
                keys = list(self._market_info.keys())
                for k in keys[:len(keys) - self.cfg.cache_max_size // 2]:
                    self._market_info.pop(k, None)
                logger.debug("[CACHE] 清理 _market_info")

        with self._trade_lock:
            if len(self._processed_trades) > self.cfg.trade_max_size:
                sorted_items = sorted(self._processed_trades.items(), key=lambda x: x[1])
                to_remove = [tid for tid, _ in sorted_items[:self.cfg.trade_max_size // 2]]
                for tid in to_remove:
                    self._processed_trades.pop(tid, None)
                logger.debug("[CACHE] 清理 _processed_trades %d 条", len(to_remove))

    # ── 主循环 ────────────────────────────────────────────────────────────────
    def run(self):
        # 1. 先启动心跳（在发现任何订单之前）
        self.heartbeat.start()

        # 2. 启动 WebSocket
        self.ws_manager.start()

        # 用户频道
        self.ws_manager.start_user(
            on_open=lambda ws: ws.send(json.dumps({
                "auth": {
                    "apiKey": self.cfg.api_key,
                    "secret": self.cfg.api_secret,
                    "passphrase": self.cfg.passphrase,
                },
                "type": "user",
                "markets": [],
                "assets_ids": [],
                "initial_dump": True,
            })),
            on_message=self.ws_router.on_user_message,
        )

        # 3. 发现已有订单
        self.discover()

        # 4. 主循环
        last_discover = last_poll = last_audit = last_position = last_prune = time.time()
        while self.running:
            try:
                now = time.time()

                if now - last_discover >= self.cfg.discover_interval:
                    self.discover()
                    last_discover = now

                if now - last_poll >= self.cfg.best_bid_poll_interval:
                    self._poll_best_bids()
                    last_poll = now

                if now - last_audit >= self.cfg.audit_interval:
                    self.audit()
                    last_audit = now

                if now - last_position >= self.cfg.position_interval:
                    self.check_positions()
                    last_position = now

                if now - last_prune >= self.cfg.cache_prune_interval:
                    self._prune_caches()
                    last_prune = now

                for _ in range(10):
                    if not self.running:
                        break
                    now = time.time()
                    self._check_cooldowns(now)
                    self._check_pending_ops(now)
                    time.sleep(1)
            except Exception as e:
                logger.error("主循环异常: %s", e, exc_info=True)
                time.sleep(10)

        # 6. 优雅关闭
        self._shutdown()

    def _shutdown(self):
        logger.info("开始优雅关闭...")

        # 一次 API 调用取消所有活跃订单
        fut = self.exec_layer.cancel_all("系统关闭")
        try:
            fut.result(timeout=self.cfg.cancel_timeout)
        except Exception:
            pass

        # 停止 WebSocket
        self.ws_manager.stop()

        # 停止执行层线程池
        self.exec_layer.shutdown(wait=True)

        # 最后停止心跳（订单已取消后）
        self.heartbeat.stop()

        logger.info("=" * 60 + "\n系统已停止\n" + "=" * 60)
