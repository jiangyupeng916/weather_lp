#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Guardian V7 — 主控制器

职责：
 - 管理 AssetActor 生命周期（线程安全）
 - 定时任务：discover / poll_best_bids / audit / check_positions / cache_prune
 - 批量轮询 best_bid 替代 WebSocket 市场频道
 - 交易日志：记录 CONFIRMED 事件到 trade_logger
 - 启动/关闭编排（心跳先于订单，关闭时订单先于心跳）
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple

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
from models import ActorState, EventType, ActorEvent, OrderInfo
from utils import safe_float, safe_decimal, round_to_tick, safe_float_from_decimal
from heartbeat import HeartbeatManager
from execution import ExecutionLayer
from actor import AssetActor
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

        # ── Actor 管理（P0 修复：全部通过锁保护的访问器） ──────────────────────
        self._actors: Dict[str, AssetActor] = {}
        self._actors_lock = threading.RLock()

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

    # ── Actor 线程安全访问器（P0 修复） ───────────────────────────────────────
    def get_actor(self, asset_id: str) -> Optional[AssetActor]:
        with self._actors_lock:
            return self._actors.get(asset_id)

    def add_actor(self, asset_id: str, actor: AssetActor):
        with self._actors_lock:
            self._actors[asset_id] = actor

    def remove_actor(self, asset_id: str) -> Optional[AssetActor]:
        with self._actors_lock:
            return self._actors.pop(asset_id, None)

    def list_actors(self) -> List[AssetActor]:
        with self._actors_lock:
            return list(self._actors.values())

    def list_actor_ids(self) -> List[str]:
        with self._actors_lock:
            return list(self._actors.keys())

    def actor_count(self) -> int:
        with self._actors_lock:
            return len(self._actors)

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

    def _poll_best_bids(self):
        """批量查询所有市场的 best_bid，检测变化后通知 Actor。"""
        token_ids = self.list_actor_ids()
        if not token_ids:
            return

        try:
            r = requests.post(
                f"{self.cfg.host}/books",
                json=[{"token_id": tid} for tid in token_ids],
                timeout=10,
            )
            if r.status_code != 200:
                logger.error("[POLL] 批量查询失败 HTTP %s", r.status_code)
                return
            books = r.json()
        except Exception as e:
            logger.error("[POLL] 批量查询异常: %s", e)
            return

        polled = 0
        for item in (books if isinstance(books, list) else []):
            aid = item.get("asset_id", "")
            actor = self.get_actor(aid)
            if not actor or actor.state is ActorState.STOPPED:
                continue

            bids = item.get("bids", [])
            if not bids:
                continue

            # POST /books 实际返回升序，best_bid 在最后
            best_bid_str = bids[-1].get("price", "")
            best_ask_str = ""
            asks = item.get("asks", [])
            if asks:
                # POST /books 实际返回降序，best_ask 在最后
                best_ask_str = asks[-1].get("price", "")

            if best_bid_str:
                actor.post(ActorEvent(EventType.BEST_BID, {
                    "best_bid": best_bid_str,
                    "best_ask": best_ask_str,
                }))
                polled += 1

        logger.debug("[POLL] 轮询 %d 个 Actor", polled)

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

    # ── 订单发现 ──────────────────────────────────────────────────────────────
    def discover(self):
        orders = self.open_orders()
        buys = {o.token_id: o for o in orders if o.side.upper() == "BUY"}

        # 清理 STOPPED 状态 Actor
        for aid in self.list_actor_ids():
            actor = self.get_actor(aid)
            if actor and actor.state is ActorState.STOPPED:
                self.remove_actor(aid)
                actor.force_stop()
                logger.info("[DISCOVER] 清理 STOPPED 市场 %s", aid[:20])

        # 发现新市场
        for asset_id in set(buys.keys()) - set(self.list_actor_ids()):
            o = buys[asset_id]
            logger.info("[DISCOVER] 新市场 %s price=%s", asset_id[:20], o.price)
            actor = AssetActor(asset_id, self, initial=o)
            self.add_actor(asset_id, actor)

        logger.info("[DISCOVER] 守护 %d 个市场", self.actor_count())

    # ── 审计 ──────────────────────────────────────────────────────────────────
    def audit(self):
        logger.info("[AUDIT] 开始...")
        orders = self.open_orders()
        order_map = {o.order_id: o for o in orders}

        # 将订单列表传给每个 Actor，避免每个 Actor 单独调 API
        for a in self.list_actors():
            a.post(ActorEvent(EventType.AUDIT, {"orders": order_map}))

        buys = [o for o in orders if o.side.upper() == "BUY"]

        # 批量查询 best_bid，替代逐个 GET /book
        best_bid_map: Dict[str, Decimal] = {}
        if buys:
            token_ids = list({o.token_id for o in buys})
            try:
                r = requests.post(
                    f"{self.cfg.host}/books",
                    json=[{"token_id": tid} for tid in token_ids],
                    timeout=10,
                )
                if r.status_code == 200:
                    for item in r.json():
                        bids = item.get("bids", [])
                        if bids:
                            best_bid_map[item["asset_id"]] = Decimal(bids[-1].get("price", "0"))
            except Exception as e:
                logger.error("[AUDIT] 批量查询 best_bid 失败: %s", e)

        to_cancel: List[Tuple[OrderInfo, str]] = []
        for o in buys:
            bb = best_bid_map.get(o.token_id)
            if bb is not None:
                o_price_dec = Decimal(str(o.price))
                if o_price_dec >= bb:
                    to_cancel.append((o, f"审计: price {o.price} >= best_bid {bb}"))

        for o, reason in to_cancel:
            self.exec_layer.cancel(o.order_id, reason)
            actor = self.get_actor(o.token_id)
            if actor:
                actor.post(ActorEvent(EventType.CANCEL_DONE, {
                    "order_id": o.order_id, "ok": True, "reason": reason,
                }))
            time.sleep(self.cfg.cancel_delay)
        logger.info("[AUDIT] 完成 %d 买单检查 | 纠偏 %d 个", len(buys), len(to_cancel))

    # ── 交易处理 ──────────────────────────────────────────────────────────────
    def handle_trade(self, data: dict):
        """记录 CONFIRMED trade 事件到 trade_logger，不触发卖出（卖出由 check_positions 负责）。"""
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
        """订单事件：只记录日志，不做业务处理。"""
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

            # 已有卖单挂着 → 跳过
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

                price = safe_float(p.get("avgPrice", 0))
                if price <= 0:
                    continue

                sell_price = safe_float_from_decimal(
                    round_to_tick(Decimal(str(price)), self.cfg.tick_size)
                )
                logger.info("[POLL SELL] %s | %.4f @ %s", p.get("title", "未知")[:40], bal, sell_price)

                fut = self.exec_layer.limit_sell(tid, sell_price, bal, self.cfg.tick_size)
                try:
                    sell_oid = fut.result(timeout=self.cfg.place_timeout)
                except Exception:
                    sell_oid = None

                if sell_oid:
                    trade_logger.info(json.dumps({
                        "buy": {
                            "token_id": tid, "side": "BUY",
                            "size": bal, "price": price,
                            "title": p.get("title", ""), "outcome": p.get("outcome", ""),
                            "source": "polling",
                        },
                        "sell": {
                            "order_id": sell_oid, "token_id": tid, "side": "SELL",
                            "size": bal, "price": sell_price,
                        },
                    }, ensure_ascii=False))
                    logger.info("[POLL SELL OK] %s id=%s", tid[:20], str(sell_oid)[:20])
                    placed += 1
                else:
                    logger.error("[POLL SELL FAIL] %s 限价卖单下单失败", tid[:20])
            finally:
                with self._sell_lock:
                    self._selling.discard(tid)

            time.sleep(0.3)

        if placed:
            logger.info("[POSITION] 兜底限价卖单 %d 个", placed)

    # ── 缓存清理（P2 修复） ────────────────────────────────────────────────────
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
                    time.sleep(1)
            except Exception as e:
                logger.error("主循环异常: %s", e, exc_info=True)
                time.sleep(10)

        # 6. 优雅关闭
        self._shutdown()

    def _shutdown(self):
        logger.info("开始优雅关闭...")

        # 取消所有活跃订单
        for a in self.list_actors():
            if a.active_id:
                fut = self.exec_layer.cancel(a.active_id, "系统关闭")
                try:
                    fut.result(timeout=self.cfg.cancel_timeout)
                except Exception:
                    pass

        # 停止所有 Actor
        for a in self.list_actors():
            a.force_stop()

        # 停止 WebSocket
        self.ws_manager.stop()

        # 停止执行层线程池
        self.exec_layer.shutdown(wait=True)

        # 最后停止心跳（订单已取消后）
        self.heartbeat.stop()

        logger.info("=" * 60 + "\n系统已停止\n" + "=" * 60)
