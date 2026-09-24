"""手动做市：人工挂单/退出，机器人仅追价并处理买入后的卖出。"""

from __future__ import annotations

import json
import logging
import os
import queue
import signal
import threading
import time
from decimal import Decimal
from typing import Dict, List, Optional, Set

import requests
from polymarket import RelayerApiKey, SecureClient

from background import BackgroundDispatcher
from config import Config
from execution import ExecutionLayer
from heartbeat import HeartbeatManager
from models import ActorState, MarketState, OrderInfo
from scheduler import PeriodicScheduler
from utils import safe_decimal, safe_float
from ws_manager import WSManager
from ws_router import WSRouter
from wss import BidCache, MarketWS

logger = logging.getLogger("guardian.manual")


def _order_key(value) -> str:
    return str(value or "").lower().removeprefix("0x")


def _trade_logger(instance: str):
    directory = os.path.join(os.path.dirname(__file__), "data", instance)
    os.makedirs(directory, exist_ok=True)
    log = logging.getLogger(f"guardian.trades.{instance}")
    log.setLevel(logging.INFO)
    log.propagate = False
    if not log.handlers:
        handler = logging.FileHandler(os.path.join(directory, "trades.log"), encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
        log.addHandler(handler)
    return log


class ManualGuardian:
    def __init__(self, cfg: Config, client=None):
        self.cfg = cfg
        self.running = True
        if client is None:
            key = (RelayerApiKey(key=cfg.relayer_api_key, address=cfg.relayer_api_key_address)
                   if cfg.relayer_api_key and cfg.relayer_api_key_address else None)
            client = SecureClient.create(private_key=cfg.pk, wallet=cfg.wallet or cfg.proxy or None,
                                         api_key=key)
        self.client = client
        self.address = client.wallet
        self.heartbeat = HeartbeatManager(client, cfg, self.address)
        self.exec_layer = ExecutionLayer(client, cfg)
        self.ws_manager = WSManager(cfg)
        self.ws_router = WSRouter(self)
        self._bg = BackgroundDispatcher()

        self._markets: Dict[str, MarketState] = {}
        # 按 token 记住用户最初手动下单的 share 数；追价时保持原规模。
        self._sizes: Dict[str, Decimal] = {}
        # 退出后忽略旧订单；只有用户创建新的 order ID 才允许重新接管。
        self._ignored_order_ids: set[str] = set()
        # 系统发起的撤单 ID 保留一段时间，以覆盖异步 WS 事件与 REST 回执乱序。
        self._own_cancel_ids: Dict[str, float] = {}
        # 撤单消息可能早于系统挂单 Future/首次 discover 到达。
        self._external_cancel_ids: Dict[str, float] = {}
        self._pending_ops: list[tuple] = []
        self._pending_ops_inbox: queue.Queue = queue.Queue()
        self._user_events: queue.Queue = queue.Queue()
        self._ws_bids: queue.Queue = queue.Queue()
        self._ws_trades: queue.Queue = queue.Queue()
        self._bid_cache = BidCache()
        self._ws_enabled = cfg.ws_market_enabled
        self._market_ws = MarketWS(
            cache=self._bid_cache, url=cfg.ws_market_url,
            reconnect_delay=cfg.ws_reconnect_delay,
            ping_interval=cfg.market_ping_interval, ping_timeout=cfg.market_ping_timeout,
            proxy_url=cfg.proxy_url,
            on_bid_changed=lambda tid, _old, new: self._ws_bids.put((tid, new)),
            on_trade=lambda tid, price, side: self._ws_trades.put((tid, price, side)),
        )
        self._market_ws_was_connected = False

        self.trade_logger = _trade_logger(cfg.instance_name)
        self._sell_lock = threading.Lock()
        self._selling: set[str] = set()
        self._pending_sell_lock = threading.Lock()
        self._pending_sell_tokens: Dict[str, Decimal] = {}
        self._holding_since: Dict[str, float] = {}
        self._trade_lock = threading.Lock()
        self._processed_trades: Dict[str, float] = {}
        self._market_info: Dict[str, dict] = {}
        self._cache_lock = threading.Lock()

        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self._on_signal)
            signal.signal(signal.SIGTERM, self._on_signal)
        logger.info("手动做市启动 | 实例=%s 钱包=%s 档位=%d 冷却=%.0fs",
                    cfg.instance_name, self.address, cfg.maker_rank, cfg.maker_cooldown)

    def _on_signal(self, *_):
        self.running = False

    def open_orders(self) -> Optional[List[OrderInfo]]:
        try:
            result = []
            for page in self.client.list_open_orders():
                for order in page.items:
                    result.append(OrderInfo(
                        order_id=str(order.id), token_id=str(order.token_id),
                        side=str(order.side), price=safe_float(order.price),
                        size=safe_float(order.original_size),
                        market=str(order.market) if order.market else "",
                    ))
            return result
        except Exception as exc:
            logger.error("查询订单失败: %s", exc)
            return None

    def market_info(self, token_id: str) -> dict:
        with self._cache_lock:
            cached = self._market_info.get(token_id)
        if cached is not None:
            return cached
        info = {"title": "未知", "outcome": ""}
        try:
            response = requests.get(f"{self.cfg.host}/markets-by-token/{token_id}", timeout=10)
            response.raise_for_status()
            data = response.json()
            item = data[0] if isinstance(data, list) and data else data
            if isinstance(item, dict):
                info["title"] = item.get("question") or item.get("title") or "未知"
                info["outcome"] = item.get("outcome") or ""
        except Exception as exc:
            logger.warning("市场信息查询失败 %s: %s", token_id[:16], exc)
        with self._cache_lock:
            self._market_info[token_id] = info
        return info

    def _discover_fetch(self):
        orders = self.open_orders()
        if orders is None:
            raise RuntimeError("open_orders 失败")
        return orders

    @staticmethod
    def _chunk_list(items, size):
        return [items[i:i + size] for i in range(0, len(items), size)]

    def _apply_discover_result(self, orders):
        """仅接管新出现的 BUY order ID；消失的受管订单保守退出。"""
        grouped: Dict[str, List[OrderInfo]] = {}
        for order in orders:
            if (order.side.upper() == "BUY"
                    and _order_key(order.order_id) not in self._ignored_order_ids
                    and _order_key(order.order_id) not in self._external_cancel_ids):
                grouped.setdefault(order.token_id, []).append(order)
        for tid, ms in list(self._markets.items()):
            candidates = grouped.get(tid, [])
            if ms.state is ActorState.RESTING:
                current_ids = {_order_key(order.order_id) for order in candidates}
                if _order_key(ms.active_id) not in current_ids:
                    # WS 断线期间可能漏掉手动取消；无论是取消还是成交，都不擅自重挂。
                    self._stop_tracking(tid, "原买单已不在 open_orders")
                elif len(candidates) != 1:
                    self._stop_tracking(tid, "同 token 出现多个买单，暂停自动追价")
            elif ms.state is ActorState.COOLING and candidates:
                # 用户在冷却期重新手动挂单，新的订单接管原来的追价轮次。
                self._stop_tracking(tid, "用户在冷却期创建了新买单")
        for tid, candidates in grouped.items():
            if tid in self._markets:
                continue
            if len(candidates) != 1:
                logger.warning("[DISCOVER] %s 有 %d 个买单；单 token 只支持一个，暂不接管",
                               tid[:16], len(candidates))
                continue
            order = candidates[0]
            size = safe_decimal(order.size)
            if size is None or size <= 0:
                logger.warning("[DISCOVER] %s 数量无效，跳过", tid[:16])
                continue
            self._markets[tid] = MarketState(state=ActorState.RESTING,
                                              active_id=order.order_id,
                                              active_price=safe_decimal(order.price))
            self._sizes[tid] = size
            info = self.market_info(tid)
            logger.info("[DISCOVER] 接管 %s | %s | %s shares @ %s id=%s",
                        info["title"][:60], info["outcome"], size, order.price, order.order_id[:20])
        self._sync_ws_subscriptions()

    def _stop_tracking(self, tid: str, reason: str, *, cancel_remaining: bool = False):
        ms = self._markets.pop(tid, None)
        self._sizes.pop(tid, None)
        if ms is None:
            return
        ms.state = ActorState.STOPPED
        if ms.active_id:
            self._ignored_order_ids.add(_order_key(ms.active_id))
            if cancel_remaining:
                self._cancel_exited_order(tid, ms.active_id, "买入成交，停止做市")
        # 订单已退出时若还有挂单 Future，完成后按结果撤掉，绝不放任迟到的系统单。
        logger.info("[STOP] %s 停止跟踪: %s", tid[:16], reason)
        self._sync_ws_subscriptions()

    def _target_price(self, tid: str) -> Optional[Decimal]:
        try:
            book = self.client.get_order_book(token_id=tid)
            bids = sorted((safe_decimal(level.price) for level in book.bids
                           if safe_decimal(level.price) is not None and safe_float(level.size) > 0),
                          reverse=True)
            return bids[self.cfg.maker_rank - 1] if len(bids) >= self.cfg.maker_rank else None
        except Exception as exc:
            logger.error("查询买盘失败 %s: %s", tid[:16], exc)
            return None

    def _start_cooldown(self, ms: MarketState, seconds: float = None):
        ms.state = ActorState.COOLING
        ms.state_at = time.time()
        ms.cooldown_until = ms.state_at + (self.cfg.maker_cooldown if seconds is None else seconds)

    def _trigger_cancel(self, tid: str, reason: str):
        ms = self._markets.get(tid)
        if ms is None or ms.state is not ActorState.RESTING or not ms.active_id:
            return
        oid = ms.active_id
        ms.state = ActorState.CANCELING
        ms.state_at = time.time()
        self._own_cancel_ids[_order_key(oid)] = ms.state_at
        future = self.exec_layer.cancel(oid, reason)
        self._pending_ops.append((future, tid, "cancel", {"order_id": oid}))
        logger.info("[CHASE] 撤单 %s | %s", tid[:16], reason)

    def _trigger_place(self, tid: str):
        ms = self._markets.get(tid)
        if ms is None or ms.state is not ActorState.COOLING or tid not in self._sizes:
            return
        # 手动撤单事件依赖用户 WS；离线时不创建新单。
        if not self.ws_manager.is_user_connected():
            return
        if self._ws_enabled and not self._market_ws.is_connected():
            return
        ms.state = ActorState.PLACING
        ms.state_at = time.time()
        future = self.exec_layer.run_async(self._target_price, tid)
        self._pending_ops.append((future, tid, "target_price", {}))

    def _check_cooldowns(self, now: float):
        for tid, ms in list(self._markets.items()):
            if ms.state is ActorState.COOLING and now >= ms.cooldown_until:
                self._trigger_place(tid)

    def _enqueue_pending_op(self, item):
        self._pending_ops_inbox.put(item)

    def _cancel_exited_order(self, tid: str, oid: str, reason: str):
        self._own_cancel_ids[_order_key(oid)] = time.time()
        future = self.exec_layer.cancel(oid, reason)
        self._enqueue_pending_op((future, tid, "exit_cancel", {"order_id": oid, "attempt": 1}))

    def _check_pending_ops(self):
        while True:
            try:
                self._pending_ops.append(self._pending_ops_inbox.get_nowait())
            except queue.Empty:
                break
        remain = []
        for future, tid, op, meta in self._pending_ops:
            if not future.done():
                remain.append((future, tid, op, meta))
                continue
            try:
                result = future.result()
            except Exception as exc:
                logger.error("[%s] %s 异步任务失败: %s", op, tid[:16], exc)
                result = None
            if op in ("limit_sell", "limit_sell_triggered", "market_sell"):
                if not result:
                    logger.error("[%s] %s 卖单失败；定时持仓检查会重试", op, tid[:16])
                continue
            if op == "exit_cancel":
                if not result:
                    attempt = meta["attempt"]
                    if attempt < 3:
                        future = self.exec_layer.cancel(meta["order_id"], "退出重试撤单")
                        remain.append((future, tid, op, {"order_id": meta["order_id"],
                                                         "attempt": attempt + 1}))
                    else:
                        logger.error("[EXIT] %s 剩余买单 %s 撤单失败，请在官网检查",
                                     tid[:16], meta["order_id"][:20])
                continue
            ms = self._markets.get(tid)
            if ms is None:
                if op == "place" and result:
                    oid = str(result)
                    self._ignored_order_ids.add(_order_key(oid))
                    self._cancel_exited_order(tid, oid, "退出后迟到的系统挂单")
                continue
            if op == "cancel":
                oid = meta["order_id"]
                if _order_key(ms.active_id) != _order_key(oid) or ms.state is not ActorState.CANCELING:
                    continue
                if result:
                    self._ignored_order_ids.add(_order_key(oid))
                    ms.active_id = None
                    ms.active_price = None
                    self._start_cooldown(ms)
                else:
                    # 取消结果不明时绝不重挂，下一次 discover 对账确认。
                    ms.state = ActorState.RESTING
                    ms.state_at = time.time()
                    self._own_cancel_ids.pop(_order_key(oid), None)
                    logger.warning("[CANCEL] %s 结果不明，保持原单跟踪", tid[:16])
            elif op == "target_price":
                if ms.state is not ActorState.PLACING:
                    continue
                if result is None:
                    self._start_cooldown(ms, 10.0)
                else:
                    future = self.exec_layer.place(tid, result, self._sizes[tid], self.cfg.tick_size)
                    remain.append((future, tid, "place", {"price": result}))
            elif op == "place":
                self.exec_layer.clear_place(tid, meta["price"])
                if ms.state is not ActorState.PLACING:
                    if result:
                        oid = str(result)
                        self._ignored_order_ids.add(_order_key(oid))
                        self._cancel_exited_order(tid, oid, "过期挂单结果")
                    continue
                if result:
                    ms.active_id = str(result)
                    if _order_key(result) in self._external_cancel_ids:
                        self._stop_tracking(tid, "挂单确认前已被用户撤销")
                        continue
                    ms.active_price = meta["price"]
                    ms.state = ActorState.RESTING
                    ms.state_at = time.time()
                    logger.info("[CHASE] %s 重挂 %s shares @ %s id=%s",
                                tid[:16], self._sizes[tid], ms.active_price, ms.active_id[:20])
                else:
                    self._start_cooldown(ms)
        self._pending_ops = remain

    def enqueue_user_event(self, kind: str, data: dict):
        self._user_events.put((kind, data))

    def _process_user_events(self):
        while True:
            try:
                kind, data = self._user_events.get_nowait()
            except queue.Empty:
                break
            if kind == "trade":
                self.handle_trade(data)
            else:
                self.handle_order(data)

    def handle_order(self, data: dict):
        kind = str(data.get("type") or data.get("eventType") or "").upper()
        status = str(data.get("status") or "").upper()
        oid = str(data.get("id") or data.get("order_id") or "")
        if kind == "UPDATE" and safe_float(data.get("size_matched")) > 0:
            for tid, ms in list(self._markets.items()):
                if _order_key(ms.active_id) == _order_key(oid):
                    self._stop_tracking(tid, "买单部分成交，等待 CONFIRMED 卖出", cancel_remaining=True)
                    return
        if kind != "CANCELLATION" and status not in ("CANCELED", "CANCELLED"):
            return
        if not oid:
            return
        if _order_key(oid) in self._own_cancel_ids:
            logger.debug("[ORDER] 忽略系统撤单事件 %s", oid[:20])
            return
        for tid, ms in list(self._markets.items()):
            if _order_key(ms.active_id) == _order_key(oid):
                self._stop_tracking(tid, "用户手动撤单")
                return
        self._external_cancel_ids[_order_key(oid)] = time.time()

    def _apply_bid_change(self, tid: str, bid: Decimal, ask: Optional[Decimal] = None):
        ms = self._markets.get(tid)
        if ms is None:
            return
        if ms.best_bid is None:
            ms.best_bid = bid
            ms.best_ask = ask
            return
        if ask is not None:
            ms.best_ask = ask
        if bid == ms.best_bid:
            return
        ms.best_bid = bid
        if ms.state is ActorState.RESTING:
            self._trigger_cancel(tid, "best_bid 变化")

    def _process_ws_bids(self):
        latest = {}
        while True:
            try:
                tid, bid = self._ws_bids.get_nowait()
                latest[tid] = bid
            except queue.Empty:
                break
        for tid, bid in latest.items():
            self._apply_bid_change(tid, bid)

    def _process_ws_trades(self):
        while True:
            try:
                tid, price, side = self._ws_trades.get_nowait()
            except queue.Empty:
                break
            if self.cfg.cancel_on_trade:
                self._trigger_cancel(tid, f"市场成交 {side} @ {price}")

    def _poll_fetch(self, token_ids):
        items = []
        for index in range(0, len(token_ids), 500):
            try:
                response = requests.post(f"{self.cfg.host}/books",
                                         json=[{"token_id": tid} for tid in token_ids[index:index + 500]],
                                         timeout=10)
                response.raise_for_status()
                data = response.json()
                if isinstance(data, list):
                    items.extend(data)
            except Exception as exc:
                logger.error("[POLL] 订单簿查询失败: %s", exc)
        return items

    def _apply_poll_result(self, items):
        for item in items:
            bids = item.get("bids") or []
            asks = item.get("asks") or []
            if not bids:
                continue
            bid = safe_decimal(bids[-1].get("price"))
            ask = safe_decimal(asks[-1].get("price")) if asks else None
            if bid is not None:
                self._apply_bid_change(str(item.get("asset_id", "")), bid, ask)

    def _sync_ws_subscriptions(self):
        if not self._ws_enabled:
            return
        desired = set(self._markets)
        subscribed = self._market_ws.subscribed_ids()
        if desired - subscribed:
            self._market_ws.subscribe_more(list(desired - subscribed))
        if subscribed - desired:
            self._market_ws.unsubscribe(list(subscribed - desired))

    def _check_ws_connection(self):
        if not self._ws_enabled:
            return
        connected = self._market_ws.is_connected()
        if self._market_ws_was_connected and not connected:
            logger.warning("市场 WS 断开；撤当前买单并暂停重挂")
            for tid, ms in list(self._markets.items()):
                if ms.state is ActorState.RESTING:
                    self._trigger_cancel(tid, "市场 WS 断开")
        self._market_ws_was_connected = connected

    def positions(self) -> Optional[List[dict]]:
        try:
            response = requests.get(f"{self.cfg.data_api}/positions",
                                    params={"user": self.address,
                                            "sizeThreshold": self.cfg.position_threshold}, timeout=10)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                raise ValueError("持仓响应不是列表")
            return data
        except Exception as exc:
            logger.error("查询持仓失败: %s", exc)
            return None

    def onchain_balance(self, tid: str) -> Optional[float]:
        try:
            return self.client.get_balance_allowance(asset_type="CONDITIONAL", token_id=tid).balance / 1_000_000
        except Exception as exc:
            logger.error("余额查询失败 %s: %s", tid[:16], exc)
            return None

    def handle_trade(self, data: dict):
        if str(data.get("status", "")).upper() != "CONFIRMED":
            return
        trade_id = str(data.get("id") or "")
        if not trade_id:
            return
        with self._trade_lock:
            if trade_id in self._processed_trades:
                return
            self._processed_trades[trade_id] = time.time()
        makers = data.get("maker_orders") or []
        managed_ids = self._ignored_order_ids | {
            _order_key(ms.active_id) for ms in self._markets.values() if ms.active_id
        }
        managed_fill = any(_order_key(order.get("order_id")) in managed_ids for order in makers)
        ours = [order for order in makers if
                (order.get("maker_address") or "").lower() == self.address.lower()
                or _order_key(order.get("order_id")) in managed_ids]
        own = ours[0] if ours else data
        tid = str(own.get("asset_id") or data.get("asset_id") or data.get("token_id") or "")
        if not tid:
            logger.warning("[TRADE] 无 token ID: %s", trade_id)
            return
        side = "BUY" if managed_fill else str(own.get("side") or data.get("side") or "").upper()
        price = safe_decimal(own.get("price") or data.get("price"))
        amount = (sum(safe_float(order.get("matched_amount")) for order in ours)
                  if ours else safe_float(data.get("size")))
        info = self.market_info(tid)
        self.trade_logger.info(json.dumps({
            f"{side.lower()}_confirmed": {
                "trade_id": trade_id, "token_id": tid, "side": side,
                "size": amount, "price": str(price), "title": info["title"],
                "outcome": own.get("outcome") or info["outcome"],
            }
        }, ensure_ascii=False))
        logger.info("[%s CONFIRMED] %s | %s shares @ %s", side, info["title"][:40], amount, price)
        if side == "BUY":
            self._stop_tracking(tid, "BUY 成交", cancel_remaining=True)
            with self._pending_sell_lock:
                self._pending_sell_tokens[tid] = price or Decimal("0")

    def _prune_caches(self):
        cutoff = time.time() - 600
        self._own_cancel_ids = {oid: ts for oid, ts in self._own_cancel_ids.items() if ts > cutoff}
        self._external_cancel_ids = {oid: ts for oid, ts in self._external_cancel_ids.items()
                                     if ts > cutoff}
        with self._trade_lock:
            if len(self._processed_trades) > 50000:
                newest = sorted(self._processed_trades.items(), key=lambda item: item[1], reverse=True)
                self._processed_trades = dict(newest[:25000])

    def run(self):
        self.heartbeat.start()
        self.ws_manager.start()
        self.ws_manager.start_user(
            on_open=lambda ws: ws.send(json.dumps({
                "auth": {
                    "apiKey": self.client.credentials.key,
                    "secret": self.client.credentials.secret,
                    "passphrase": self.client.credentials.passphrase,
                },
                "type": "user",
            })),
            on_message=self.ws_router.on_user_message,
        )
        if self._ws_enabled:
            self._market_ws.start()
        try:
            self._apply_discover_result(self._discover_fetch())
        except Exception as exc:
            logger.error("启动 discover 失败: %s", exc)
        scheduler = PeriodicScheduler()
        scheduler.add("discover", self.cfg.discover_interval,
                      lambda: self._bg.submit("discover", self._discover_fetch))
        poll_interval = (self.cfg.ws_rest_reconcile_interval if self._ws_enabled
                         else self.cfg.best_bid_poll_interval)
        scheduler.add("poll", poll_interval,
                      lambda: self._bg.submit("poll", self._poll_fetch, list(self._markets)))
        scheduler.add("position", self.cfg.position_interval,
                      lambda: self._bg.submit("position", self.check_positions), run_immediately=True)
        scheduler.add("prune", self.cfg.cache_prune_interval, self._prune_caches)
        if self._ws_enabled:
            scheduler.add("sub_sync", self.cfg.ws_sub_sync_interval, self._sync_ws_subscriptions)
        try:
            while self.running:
                try:
                    # 用户事件先处理：同一 tick 中手动撤单优先于 bid 追价。
                    self._process_user_events()
                    self._check_pending_ops()
                    self._check_ws_connection()
                    self._process_ws_bids()
                    self._process_ws_trades()
                    self._check_cooldowns(time.time())
                    self._check_pending_sells()
                    for name, ok, payload in self._bg.drain():
                        if not ok:
                            continue
                        if name == "discover":
                            self._apply_discover_result(payload)
                        elif name == "poll":
                            self._apply_poll_result(payload)
                    scheduler.tick()
                except Exception as exc:
                    logger.error("主循环异常: %s", exc, exc_info=True)
                time.sleep(1)
        finally:
            self._shutdown()

    def _shutdown(self):
        self._bg.shutdown(wait=False)
        if self._ws_enabled:
            self._market_ws.stop()
            self._market_ws.join(timeout=3)
        self.ws_manager.stop()
        # 不主动 cancel_all；但停止 REST 心跳后交易所会按其规则取消该账户活跃单。
        self.exec_layer.shutdown(wait=True)
        self.heartbeat.stop()
        logger.info("手动做市已停止")

    # 以下持仓卖出逻辑沿用 guardian_v8，并修正查询失败语义。

    def check_positions(self):
        pos_list = self.positions()
        if pos_list is None:
            return

        # ── 持仓首见计时维护（V8.4 超时强平）──────────────────────────────────
        # 每轮同步 _holding_since：新持仓记当前时间，已消失（卖光）的清理。
        # 仅主线程 check_positions 读写，无锁。空持仓也要清理，故在 early-return 前做。
        now_ts = time.time()
        current_tids = {p.get("asset", "") for p in pos_list if p.get("asset")}
        for tid in current_tids:
            if tid not in self._holding_since:
                self._holding_since[tid] = now_ts
        for tid in list(self._holding_since.keys()):
            if tid not in current_tids:
                del self._holding_since[tid]

        if not pos_list:
            return

        # 超时集合：持有超过 max_hold_hours 的 token（到期无条件 FOK 市价全卖）
        overdue: Set[str] = set()
        if self.cfg.max_hold_enabled:
            max_age = self.cfg.max_hold_hours * 3600.0
            for tid in current_tids:
                if now_ts - self._holding_since.get(tid, now_ts) >= max_age:
                    overdue.add(tid)

        # 批量查询所有持仓的 best_ask（maker 卖单挂在 best_ask，省 taker 费 + 赚价差）
        # 注：/books 的 asks 数组降序排列，asks[-1] 为最低卖价 = best_ask
        best_ask_map: Dict[str, Decimal] = {}
        for chunk in self._chunk_list([p["asset"] for p in pos_list if p.get("asset")], 500):
            try:
                r = requests.post(
                    f"{self.cfg.host}/books",
                    json=[{"token_id": tid} for tid in chunk],
                    timeout=10,
                )
                if r.status_code == 200:
                    for item in r.json():
                        asks = item.get("asks", [])
                        if asks:
                            best_ask_map[item["asset_id"]] = Decimal(asks[-1].get("price", "0"))
            except Exception as e:
                logger.error("[POSITION] 批量查询 best_ask 失败: %s", e)

        orders = self.open_orders()
        if orders is None:
            logger.error("[POSITION] 订单查询失败，跳过卖出检查")
            return
        # {token_id: (order_id, price)} — 用于判断是否需要更新卖价
        sell_map = {o.token_id: (o.order_id, safe_decimal(o.price))
                    for o in orders if o.side.upper() == "SELL"}

        logger.debug("[POSITION] 发现 %d 个持仓", len(pos_list))
        placed = 0
        for p in pos_list:
            tid = p.get("asset", "")
            if not tid:
                continue

            # ── 超时强平：持有 >= max_hold_hours → FOK 市价全卖，忽略崩盘保护 ──
            # 放在 best_ask/崩盘保护逻辑之前：超时逃生优先于任何价格保护。
            # FOK 失败（流动性不足）不做特殊处理，下一轮 position 周期(120s)自然重试。
            if tid in overdue:
                with self._sell_lock:
                    if tid in self._selling:
                        continue
                    self._selling.add(tid)
                try:
                    bal = self.onchain_balance(tid)
                    if bal is None:
                        logger.warning(
                            "[MAX-HOLD] %s 余额查询失败，跳过本轮强平（下轮重试）",
                            tid[:16],
                        )
                        continue
                    if bal <= self.cfg.position_threshold:
                        continue
                    age_h = (now_ts - self._holding_since.get(tid, now_ts)) / 3600.0
                    # 已有卖单先撤，避免占用余额导致市价单余额不足
                    existing = sell_map.get(tid)
                    if existing:
                        self.exec_layer.cancel(existing[0], "超时强平撤旧卖单")
                    logger.warning(
                        "[MAX-HOLD] %s 持有 %.2fh >= %.1fh，FOK 市价全卖 %.4f shares",
                        p.get("title", "未知")[:40], age_h, self.cfg.max_hold_hours, bal)
                    fut = self.exec_layer.market_sell(tid, bal, self.cfg.tick_size)
                    self._enqueue_pending_op((fut, tid, "market_sell", {"shares": bal}))
                    placed += 1
                finally:
                    with self._sell_lock:
                        self._selling.discard(tid)
                continue

            ba = best_ask_map.get(tid)
            if ba is None:
                continue
            entry = safe_float(p.get("avgPrice", 0))

            # best_ask 低于成本-gap（市场崩了）→ 取消现有卖单，持有等回稳
            if entry > 0:
                min_ask = Decimal(str(entry)) - self.cfg.sell_position_gap
                if ba < min_ask:
                    existing = sell_map.get(tid)
                    if existing:
                        logger.warning(
                            "[POSITION] %s best_ask=%s < 成本%.4f-%.2f=%.4f，取消卖单等待",
                            tid[:16], ba, entry, self.cfg.sell_position_gap, min_ask)
                        self.exec_layer.cancel(existing[0], "best_ask过低取消卖单")
                    continue

            # 已有卖单且价格已贴在 best_ask → 保持不动（保住队列位置，不重复撤挂）
            existing = sell_map.get(tid)
            if existing and existing[1] == ba:
                continue

            with self._sell_lock:
                if tid in self._selling:
                    continue
                self._selling.add(tid)

            try:
                bal = self.onchain_balance(tid)
                if bal is None:
                    logger.warning(
                        "[POSITION] %s 余额查询失败，跳过本轮卖出（下轮重试）",
                        tid[:16],
                    )
                    continue
                if bal <= self.cfg.position_threshold:
                    continue

                # 挂单价 != best_ask（别人在更低价插了新卖单）→ 撤旧、重挂追到新 best_ask
                if existing:
                    logger.info("[LIMIT SELL] %s 追价 %s → %s",
                               tid[:16], existing[1], ba)
                    self.exec_layer.cancel(existing[0], "追price到best_ask")

                logger.info("[LIMIT SELL] %s | %.4f shares @ %s (maker)",
                           p.get("title", "未知")[:40], bal, ba)
                fut = self.exec_layer.limit_sell(tid, bal, ba, self.cfg.tick_size)
                # 【后台线程】不直接 append，改投收件箱由主线程收编（_pending_ops 主线程独占）
                self._enqueue_pending_op((fut, tid, "limit_sell", {"price": ba}))
                placed += 1
            finally:
                with self._sell_lock:
                    self._selling.discard(tid)

        if placed:
            logger.info("[POSITION] maker 卖单 %d 个持仓", placed)


    def _fetch_single_best_bid(self, tid: str):
        """单 token 查询 best_bid（POST /books 单条请求），失败返回 None。"""
        try:
            r = requests.post(
                f"{self.cfg.host}/books",
                json=[{"token_id": tid}],
                timeout=10,
            )
            if r.status_code == 200:
                for item in r.json():
                    bids = item.get("bids", [])
                    if bids:
                        return Decimal(bids[-1].get("price", "0"))
        except Exception as e:
            logger.error("[SELL-TRIGGER] 查询 best_bid 失败 %s: %s", tid[:16], e)
        return None


    def _sell_single_position(self, tid: str, fill_price: Decimal):
        """BUY 成交后针对单个 token 的即时卖单逻辑，在独立守护线程中执行。

        fill_price: 本次成交价，用作 sell_min_bid_gap 保护基准。
        若 best_bid < fill_price - sell_min_bid_gap，说明大单把订单簿打薄，
        暂不挂卖单，等 120s check_positions 兜底处理。
        """
        with self._sell_lock:
            if tid in self._selling:
                logger.info("[SELL-TRIGGER] %s 正在处理中，跳过重复触发", tid[:16])
                return
            self._selling.add(tid)
        logger.info("[SELL-TRIGGER] %s 开始执行，fill_price=%s", tid[:16], fill_price)

        try:
            # 1. 查 best_bid
            bb = self._fetch_single_best_bid(tid)
            if bb is None:
                logger.warning("[SELL-TRIGGER] %s 无法获取 best_bid，回退等定时轮询", tid[:16])
                return

            # 2. 价差保护：best_bid 太低说明大单打穿订单簿，等市场回稳
            if fill_price > 0:
                min_bid = fill_price - self.cfg.sell_min_bid_gap
                if bb < min_bid:
                    logger.warning(
                        "[SELL-TRIGGER] %s best_bid=%s < 成交价%.4f-%.2f=%.4f，跳过等定时轮询",
                        tid[:16], bb, fill_price, self.cfg.sell_min_bid_gap, min_bid,
                    )
                    return

            # 3. 查当前活跃卖单
            orders = self.open_orders()
            if orders is None:
                logger.warning("[SELL-TRIGGER] open_orders 失败，回退等 120s 定时轮询")
                return
            sell_map = {
                o.token_id: (o.order_id, safe_decimal(o.price))
                for o in orders if o.side.upper() == "SELL"
            }
            existing = sell_map.get(tid)

            # 已有完全相同价格的卖单 → 无需重复挂
            if existing and existing[1] == bb:
                logger.info("[SELL-TRIGGER] %s 已有相同价格卖单 %s，无需重挂", tid[:16], bb)
                return

            # 4. 查链上余额（BUY 刚成交时链上余额可能延迟到账，最多重试 5 次，间隔 2s）
            #    查询失败（None）≠ 没到账（0）：前者放弃交 120s 兜底，后者继续等。
            bal = 0.0
            for attempt in range(1, 6):
                bal = self.onchain_balance(tid)
                if bal is None:
                    logger.warning(
                        "[SELL-TRIGGER] %s 余额查询失败，放弃即时卖单（交 120s 兜底）",
                        tid[:16],
                    )
                    return
                if bal > self.cfg.position_threshold:
                    break
                if attempt < 5:
                    logger.info(
                        "[SELL-TRIGGER] %s 余额 %.4f ≤ 阈值，等待链上确认 (%d/5)…",
                        tid[:16], bal, attempt,
                    )
                    time.sleep(2)
            else:
                logger.warning(
                    "[SELL-TRIGGER] %s 余额 %.4f ≤ 阈值 %.4f，5次重试后放弃",
                    tid[:16], bal, self.cfg.position_threshold,
                )
                return

            # 5. 撤旧卖单（价格变了）
            if existing:
                logger.info("[SELL-TRIGGER] %s 更新卖价 %s → %s", tid[:16], existing[1], bb)
                self.exec_layer.cancel(existing[0], "BUY成交后更新卖价")

            logger.info("[SELL-TRIGGER] BUY成交触发挂单 %s | %.4f shares @ %s", tid[:16], bal, bb)
            fut = self.exec_layer.limit_sell(tid, bal, bb, self.cfg.tick_size)
            # 守护线程 → 收件箱，由主线程 _check_pending_ops 收编（不直接碰 _pending_ops）
            self._enqueue_pending_op((fut, tid, "limit_sell_triggered", {"price": bb}))

        except Exception as e:
            logger.error("[SELL-TRIGGER] %s 处理异常: %s", tid[:16], e, exc_info=True)
        finally:
            with self._sell_lock:
                self._selling.discard(tid)


    def _check_pending_sells(self):
        """消费 _pending_sell_tokens，为每个 token 启动独立守护线程执行卖单逻辑。

        调用方：主循环内层 1s tick。本方法本身不阻塞，REST 调用在线程里执行。
        """
        with self._pending_sell_lock:
            if not self._pending_sell_tokens:
                return
            tokens = dict(self._pending_sell_tokens)
            self._pending_sell_tokens.clear()

        for tid, fill_price in tokens.items():
            logger.info("[SELL-TRIGGER] BUY成交，启动即时卖单线程 %s fill_price=%s", tid[:16], fill_price)
            t = threading.Thread(
                target=self._sell_single_position,
                args=(tid, fill_price),
                daemon=True,
                name=f"sell-trigger-{tid[:8]}",
            )
            t.start()
