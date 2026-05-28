#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Guardian V7 — 主控制器

职责：
 - 管理 AssetActor 生命周期（线程安全）
 - 定时任务：discover / audit / check_positions / cache_prune
 - 交易处理：MATCHED → CONFIRMED → 卖出
 - 订单事件处理：撤单 / 人工撤单检测 / 放弃
 - 启动/关闭编排（心跳先于订单，关闭时订单先于心跳）
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple

import requests
from eth_account import Account
from py_clob_client_v2 import (
    ClobClient,
    ApiCreds,
    OpenOrderParams,
    MarketOrderArgs,
    BalanceAllowanceParams,
    AssetType,
    PartialCreateOrderOptions,
    OrderType,
)

from config import Config
from models import ActorState, EventType, ActorEvent, OrderInfo, MarketInfo
from utils import safe_float, safe_decimal, retry_call
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
cancel_logger = _file_logger("cancels")
abandon_logger = _file_logger("abandons")


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
        self._processed_trades: Set[str] = set()
        self._trade_lock = threading.Lock()
        self._pending_sells: Dict[str, dict] = {}
        self._pending_sells_lock = threading.Lock()

        # ── 缓存 ──────────────────────────────────────────────────────────────
        self._ob_cache: Dict[str, Tuple[float, float]] = {}
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
        self._market_info[asset_id] = info
        return info

    def best_bid(self, token_id: str) -> Optional[float]:
        def _fetch():
            ob = self.client.get_order_book(token_id)
            bids = ob.get("bids", []) if ob else []
            prices = [safe_float(b.get("price", 0)) for b in bids]
            prices = [p for p in prices if p > 0]
            return max(prices) if prices else None

        if self.cfg.cache_ttl > 0 and token_id in self._ob_cache:
            t, v = self._ob_cache[token_id]
            if time.time() - t < self.cfg.cache_ttl:
                return v
        try:
            val = retry_call(_fetch, retries=3, delay=1.0)
            if val is not None:
                self._ob_cache[token_id] = (time.time(), val)
            return val
        except Exception as e:
            logger.error("best_bid 失败: %s... | %s", token_id[:20], e)
            return None

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

    # ── 卖出（P1 修复：FOK 回退链 + 余额重试修复） ─────────────────────────────
    def _sell(self, asset_id: str, size: float, buy_info: Optional[dict] = None) -> bool:
        if size <= 0:
            return False
        with self._sell_lock:
            if asset_id in self._selling:
                return False
            self._selling.add(asset_id)

        mi = self.market_info(asset_id)
        title = (buy_info or {}).get("title") or mi.get("title", "未知")
        outcome = (buy_info or {}).get("outcome") or mi.get("outcome", "")
        retryable = [
            "not enough balance", "insufficient balance", "insufficient funds",
            "invalid amount", "invalid size", "request exception",
            "timeout", "timed out", "connection error", "temporarily unavailable",
            "unavailable", "503", "502", "504", "read timeout", "connect timeout",
        ]
        fok_specific = ["fully filled", "fill or kill", "fok", "couldn't"]

        order_types_to_try = list(self.cfg.sell_fallback_order_types)
        ot_map = {"FOK": OrderType.FOK, "FAK": OrderType.FAK, "GTC": OrderType.GTC}

        try:
            for type_idx, ot_name in enumerate(order_types_to_try):
                ot = ot_map.get(ot_name, OrderType.FOK)
                for attempt in range(1, self.cfg.sell_retries + 1):
                    try:
                        logger.info("[SELL] %s/%d type=%s | %s | %.4f",
                                    attempt, self.cfg.sell_retries, ot_name, title[:35], size)
                        res = self.client.create_and_post_market_order(
                            order_args=MarketOrderArgs(token_id=asset_id, amount=size, side="SELL"),
                            options=PartialCreateOrderOptions(),
                            order_type=ot,
                        )
                        sell_info = {
                            "token_id": asset_id, "size": size,
                            "price": res.get("takingAmount", ""),
                            "title": title, "outcome": outcome,
                        }
                        if buy_info:
                            trade_logger.info(json.dumps(
                                {"buy": buy_info, "sell": sell_info}, ensure_ascii=False
                            ))
                        logger.info("[SELL OK] %s | %.4f", title[:35], size)
                        return True
                    except Exception as e:
                        msg = str(e).lower()
                        # FOK 特有失败 → 尝试下一个类型
                        if any(kw in msg for kw in fok_specific) and type_idx < len(order_types_to_try) - 1:
                            logger.warning("[SELL FALLBACK] FOK 失败，降级为 FAK/GTC")
                            break
                        if any(s in msg for s in retryable) and attempt < self.cfg.sell_retries:
                            delay = min(0.5 * (2 ** (attempt - 1)), 3.0)
                            logger.warning("[SELL RETRY] delay=%.1fs | %s", delay, e)
                            time.sleep(delay)
                            continue
                        logger.error("[SELL FAIL] %s | %.4f | %s", title[:35], size, e)
                        if buy_info:
                            trade_logger.error(json.dumps(
                                {"buy": buy_info, "sell": {"status": "FAILED", "error": str(e)}},
                                ensure_ascii=False,
                            ))
                        return False
            return False
        finally:
            with self._sell_lock:
                self._selling.discard(asset_id)

    def sell_position(self, asset_id: str, buy_info: Optional[dict] = None):
        # P1 修复：使用配置的重试次数
        for attempt in range(1, self.cfg.balance_retries + 1):
            bal = self.onchain_balance(asset_id)
            logger.info("  链上余额(attempt %d/%d): %.2f", attempt, self.cfg.balance_retries, bal)
            if bal > self.cfg.position_threshold:
                self._sell(asset_id, bal, buy_info)
                return
            if bal > 0:
                break
            if attempt < self.cfg.balance_retries:
                time.sleep(self.cfg.balance_delay)
        logger.warning("余额不足: %s... 余额=%.2f", asset_id[:20], bal)

    # ── 订单发现 ──────────────────────────────────────────────────────────────
    def discover(self):
        orders = self.open_orders()
        buys = {o.token_id: o for o in orders if o.side.upper() == "BUY"}

        with self._actors_lock:
            existing_ids = list(self._actors.keys())

        # 清理 STOPPED 状态 Actor
        for aid in existing_ids:
            actor = self.get_actor(aid)
            if actor and actor.state is ActorState.STOPPED:
                self.remove_actor(aid)
                actor.force_stop()
                logger.info("[DISCOVER] 清理 STOPPED 市场 %s", aid[:20])

        # 反向清理
        active_count = sum(
            1 for a in self.list_actors()
            if a.state in (ActorState.RESTING, ActorState.PLACING) and a.active_id
        )
        if not buys and active_count > 0:
            logger.warning("[DISCOVER] open_orders 无买单但仍有活跃 Actor，可能 API 异常，跳过反向清理")
            to_remove = []
        else:
            to_remove = [
                aid for aid in self.list_actor_ids()
                if aid not in buys
                and (lambda a: a and a.state in (ActorState.NO_ORDER,) and not a.active_id)(self.get_actor(aid))
            ]

        for aid in to_remove:
            actor = self.remove_actor(aid)
            if actor:
                mi = self.market_info(aid)
                msg = f"asset_id={aid} | title={mi.get('title','未知')[:50]} | reason=discover反向清理-无买单"
                abandon_logger.info(msg)
                logger.info("[ABANDON] 放弃市场 %s | discover反向清理", aid[:20])
                actor.stop(cancel_active=False)

        # 发现新市场
        for asset_id in set(buys.keys()) - set(self.list_actor_ids()):
            o = buys[asset_id]
            logger.info("[DISCOVER] 新市场 %s price=%s", asset_id[:20], o.price)
            actor = AssetActor(asset_id, self, initial=o)
            self.add_actor(asset_id, actor)
            self._sub_market(asset_id)

        logger.info("[DISCOVER] 守护 %d 个市场", self.actor_count())

    def _sub_market(self, asset_id: str):
        self.ws_manager.market_send({
            "assets_ids": [asset_id],
            "type": "market",
            "custom_feature_enabled": True,
        })

    # ── 审计 ──────────────────────────────────────────────────────────────────
    def audit(self):
        logger.info("[AUDIT] 开始...")
        for a in self.list_actors():
            a.post(ActorEvent(EventType.AUDIT))

        orders = self.open_orders()
        buys = [o for o in orders if o.side.upper() == "BUY"]

        if not buys:
            active_count = sum(
                1 for a in self.list_actors()
                if a.state in (ActorState.RESTING, ActorState.PLACING) and a.active_id
            )
            if active_count > 0:
                logger.warning("[AUDIT] open_orders 无买单但仍有活跃 Actor，可能 API 异常，跳过清理")
                return

            to_remove = [
                aid for aid in self.list_actor_ids()
                if (lambda a: a and not a.active_id and a.state in (ActorState.NO_ORDER,))(self.get_actor(aid))
            ]
            for aid in to_remove:
                actor = self.remove_actor(aid)
                if actor:
                    mi = self.market_info(aid)
                    msg = f"asset_id={aid} | title={mi.get('title','未知')[:50]} | reason=审计清理-全局无买单"
                    abandon_logger.info(msg)
                    logger.info("[ABANDON] 放弃市场 %s | 审计清理", aid[:20])
                    actor.stop(cancel_active=False)
            logger.info("[AUDIT] 无买单，清理完毕")
            return

        to_cancel: List[Tuple[OrderInfo, str]] = []
        for o in buys:
            bb = self.best_bid(o.token_id)
            if bb is not None:
                bb_dec = Decimal(str(bb))
                o_price_dec = Decimal(str(o.price))
                if o_price_dec >= bb_dec:
                    to_cancel.append((o, f"审计: price {o.price} >= best_bid {bb}"))

        for o, reason in to_cancel:
            actor = self.get_actor(o.token_id)
            if actor:
                actor.post(ActorEvent(EventType.EXTERNAL_CANCEL, {"order_id": o.order_id}))
            else:
                self.exec_layer.cancel(o.order_id, reason)
            time.sleep(self.cfg.cancel_delay)
        logger.info("[AUDIT] 完成 | 纠偏 %d 个", len(to_cancel))

    # ── 交易处理 ──────────────────────────────────────────────────────────────
    def mark_trade_processed(self, tid: str):
        with self._trade_lock:
            self._processed_trades.add(tid)

    def handle_trade(self, data: dict):
        tid = data.get("id", "")
        status = str(data.get("status", "")).upper()
        asset_id = data.get("asset_id") or data.get("token_id", "")
        price = safe_float(data.get("price", 0))
        side = str(data.get("side", "")).upper()
        maker_orders = data.get("maker_orders") or []
        our_fill = sum(
            safe_float(m.get("matched_amount", 0))
            for m in maker_orders
            if (m.get("owner") or m.get("order_owner", "")) == self.cfg.api_key
        )

        logger.info("[TRADE] id=%s status=%s side=%s our_fill=%.4f", tid[:16], status, side, our_fill)

        if not asset_id or our_fill <= 0 or side != "BUY":
            return

        with self._trade_lock:
            if tid in self._processed_trades:
                return

        if status == "MATCHED":
            self._on_trade_matched(tid, asset_id, our_fill, price, data.get("outcome", ""), maker_orders)
        elif status == "CONFIRMED":
            self._on_trade_confirmed(tid, asset_id, our_fill, price, data.get("outcome", ""))
        elif status in ("FAILED", "RETRYING"):
            self._on_trade_failed(tid)

    def _on_trade_matched(self, tid: str, asset_id: str, fill_size: float, price: float, outcome: str, maker_orders: list):
        with self._pending_sells_lock:
            self._pending_sells[tid] = {
                "asset_id": asset_id, "fill_size": fill_size, "price": price,
                "outcome": outcome, "time": time.time(),
            }

        actor = self.get_actor(asset_id)
        if actor:
            matched_oid = ""
            for m in maker_orders:
                if (m.get("owner") or m.get("order_owner", "")) == self.cfg.api_key:
                    matched_oid = m.get("order_id", "")
                    break
            actor.post(ActorEvent(EventType.TRADE_MATCHED, {"matched_order_id": matched_oid}))

    def _on_trade_confirmed(self, tid: str, asset_id: str, fill_size: float, price: float, outcome: str):
        with self._pending_sells_lock:
            pending = self._pending_sells.pop(tid, None)

        with self._trade_lock:
            self._processed_trades.add(tid)

        if pending is None:
            logger.warning("[CONFIRMED] %s 无匹配 pending，使用事件数据", tid[:16])
            actual = (fill_size, price, outcome)
        else:
            actual = (pending["fill_size"], pending["price"], pending["outcome"])

        threading.Thread(
            target=self._process_trade_sell,
            args=(tid, asset_id, *actual),
            daemon=True,
        ).start()

    def _on_trade_failed(self, tid: str):
        with self._pending_sells_lock:
            self._pending_sells.pop(tid, None)
        with self._trade_lock:
            self._processed_trades.add(tid)
        logger.warning("[TRADE FAILED] %s 已清理", tid[:16])

    def _process_trade_sell(self, trade_id: str, asset_id: str, fill_size: float, price: float, outcome: str):
        mi = self.market_info(asset_id)
        title = mi.get("title", "未知")
        logger.info("[CONFIRMED SELL] %s | %s | 成交:%.4f | price=%s", title[:40], outcome, fill_size, price)
        if fill_size <= 0:
            return
        self.sell_position(asset_id, {
            "trade_id": trade_id, "token_id": asset_id, "side": "BUY",
            "size": fill_size, "price": price, "title": title, "outcome": outcome,
        })

    # ── 订单事件 ──────────────────────────────────────────────────────────────
    def handle_order(self, data: dict):
        otype = str(data.get("type", ""))
        oid = data.get("id", "")
        side = str(data.get("side", "")).upper()
        asset_id = data.get("asset_id", "")

        if otype == "CANCELLATION":
            if self.exec_layer.is_system_cancel(oid):
                logger.info("[CANCEL] 系统撤单确认 %s", oid[:20])
                if asset_id and side == "BUY":
                    actor = self.get_actor(asset_id)
                    if actor:
                        actor.post(ActorEvent(EventType.EXTERNAL_CANCEL, {"order_id": oid}))
                return

            logger.info("[MANUAL CANCEL] 人工撤单 %s asset=%s", oid[:20], asset_id[:20])
            if side != "BUY" or not asset_id:
                return
            threading.Thread(target=self._check_abandon, args=(oid, asset_id), daemon=True).start()

    def _check_abandon(self, oid: str, asset_id: str):
        time.sleep(self.cfg.cooldown_delay)
        if not self.running:
            return
        try:
            orders = self.open_orders()
            has_buy = any(o.token_id == asset_id and o.side.upper() == "BUY" for o in orders)
            if has_buy:
                actor = self.get_actor(asset_id)
                if actor:
                    actor.post(ActorEvent(EventType.EXTERNAL_CANCEL, {"order_id": oid}))
                return

            actor = self.remove_actor(asset_id)
            mi = self.market_info(asset_id)
            msg = f"asset_id={asset_id} | title={mi.get('title','未知')[:50]} | reason=手动撤销最后一张买单"
            abandon_logger.info(msg)
            logger.info("[ABANDON] 放弃市场 %s | %s", asset_id[:20], msg)
            if actor:
                actor.stop(cancel_active=False)
        except Exception as e:
            logger.error("[ABANDON CHECK] 失败: %s", e)

    # ── 持仓兜底 ──────────────────────────────────────────────────────────────
    def check_positions(self):
        stale_timeout = 1800
        now = time.time()
        with self._pending_sells_lock:
            stale = [
                tid for tid, p in self._pending_sells.items()
                if now - p.get("time", 0) > stale_timeout
            ]
        for tid in stale:
            with self._pending_sells_lock:
                self._pending_sells.pop(tid, None)
            with self._trade_lock:
                self._processed_trades.add(tid)
            logger.warning("[PENDING CLEANUP] 过期 trade %s 未收到 CONFIRMED", tid[:16])

        pos_list = self.positions()
        if not pos_list:
            return
        logger.info("[POSITION] 发现 %d 个持仓", len(pos_list))
        sold = 0
        for p in pos_list:
            tid = p.get("asset", "")
            with self._sell_lock:
                if tid in self._selling:
                    continue
            bal = self.onchain_balance(tid)
            if bal > self.cfg.position_threshold:
                logger.info("[POLL SELL] %s | %.4f", p.get("title", "未知")[:40], bal)
                self.sell_position(tid, {
                    "token_id": tid, "size": bal,
                    "price": safe_float(p.get("avgPrice", 0)),
                    "title": p.get("title", ""), "outcome": p.get("outcome", ""),
                    "source": "polling",
                })
                sold += 1
                time.sleep(0.3)
        if sold:
            logger.info("[POSITION] 兜底卖出 %d 个", sold)

    # ── 缓存清理（P2 修复） ────────────────────────────────────────────────────
    def _prune_caches(self):
        now = time.time()
        max_age = max(self.cfg.cache_ttl * 2, 60.0)

        stale = [k for k, (t, _) in self._ob_cache.items() if now - t > max_age]
        for k in stale:
            self._ob_cache.pop(k, None)
        if stale:
            logger.debug("[CACHE] 清理 _ob_cache %d 条", len(stale))

        if len(self._market_info) > self.cfg.cache_max_size:
            keys = list(self._market_info.keys())
            for k in keys[:len(keys) - self.cfg.cache_max_size // 2]:
                self._market_info.pop(k, None)
            logger.debug("[CACHE] 清理 _market_info")

        with self._trade_lock:
            if len(self._processed_trades) > self.cfg.trade_max_size:
                oldest = list(self._processed_trades)[:self.cfg.trade_max_size // 2]
                for o in oldest:
                    self._processed_trades.discard(o)
                logger.debug("[CACHE] 清理 _processed_trades %d 条", len(oldest))

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

        # 4. 市场频道
        if self.actor_count() > 0:
            self.ws_manager.start_market(
                on_open=lambda ws: ws.send(json.dumps({
                    "assets_ids": self.list_actor_ids(),
                    "type": "market",
                    "custom_feature_enabled": True,
                })),
                on_message=self.ws_router.on_market_message,
            )

        # 5. 主循环
        last_discover = last_audit = last_position = last_prune = time.time()
        while self.running:
            try:
                now = time.time()

                if now - last_discover >= self.cfg.discover_interval:
                    self.discover()
                    last_discover = now
                    if self.actor_count() > 0:
                        self.ws_manager.start_market(
                            on_open=lambda ws: ws.send(json.dumps({
                                "assets_ids": self.list_actor_ids(),
                                "type": "market",
                                "custom_feature_enabled": True,
                            })),
                            on_message=self.ws_router.on_market_message,
                        )

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
