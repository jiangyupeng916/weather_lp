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
import queue
import random
import signal
import threading
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from polymarket import RelayerApiKey, SecureClient

from config import Config
from models import ActorState, MarketState, OrderInfo
from utils import safe_float, safe_decimal, round_to_tick, safe_float_from_decimal
from heartbeat import HeartbeatManager
from execution import ExecutionLayer
from ws_manager import WSManager
from ws_router import WSRouter
from scheduler import PeriodicScheduler
from background import BackgroundDispatcher
from wss import BidCache, MarketWS

logger = logging.getLogger("guardian")


def _file_logger(name: str, instance: str) -> logging.Logger:
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", instance)
    os.makedirs(data_dir, exist_ok=True)
    lg = logging.getLogger(f"{name}.{instance}")
    lg.setLevel(logging.INFO)
    lg.propagate = False
    h = logging.FileHandler(os.path.join(data_dir, f"{name}.log"), encoding="utf-8")
    h.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
    lg.addHandler(h)
    return lg


class Guardian:
    def __init__(self, cfg: Config = None):
        self.cfg = cfg or Config()
        self.running = True

        # ── 初始化 CLOB 客户端（V8.2：SecureClient.create，支持新旧两种账户） ──
        # 新账户（Deposit Wallet）: WALLET_ADDRESS + RELAYER_API_KEY/ADDRESS（gasless）
        # 旧账户（POLY_PROXY）    : PROXY_ADDRESS（无 relayer）
        # wallet 优先级：WALLET_ADDRESS > PROXY_ADDRESS > None（SDK 解析到 signer 的 Deposit Wallet）
        api_key = None
        if self.cfg.relayer_api_key and self.cfg.relayer_api_key_address:
            api_key = RelayerApiKey(
                key=self.cfg.relayer_api_key,
                address=self.cfg.relayer_api_key_address,
            )
        self.client = SecureClient.create(
            private_key=self.cfg.pk,
            wallet=self.cfg.wallet or self.cfg.proxy or None,
            api_key=api_key,
        )
        # wallet 属性：SDK 解析后的实际链上地址（Deposit Wallet / Proxy Wallet / EOA）
        self.address = self.client.wallet

        # ── 组件初始化（按依赖顺序） ───────────────────────────────────────────
        # V8：HeartbeatManager 不再接收 creds 参数，内部从 cfg 读取
        self.heartbeat = HeartbeatManager(self.client, self.cfg, self.address)
        self.exec_layer = ExecutionLayer(self.client, self.cfg)
        self.ws_manager = WSManager(self.cfg)
        self.ws_router = WSRouter(self)
        # 后台调度：重 REST（screener 等）「只算不写」，结果回主线程应用（H1 第二步）
        self._bg = BackgroundDispatcher()

        # ── 市场频道 WebSocket（Round 2：A2 直接集成，非子类） ──────────────────
        # WS 子线程只做一件事：把 bid 变化 put 进 _ws_bid_queue；所有 _markets
        # 写入仍只在主线程 tick 里发生（保住「主线程独占 _markets 无锁」不变量）。
        self._ws_enabled: bool = self.cfg.ws_market_enabled
        self._bid_cache = BidCache()
        self._market_ws = MarketWS(
            cache=self._bid_cache,
            url=self.cfg.ws_market_url,
            reconnect_delay=self.cfg.ws_reconnect_delay,
            ping_interval=self.cfg.market_ping_interval,
            ping_timeout=self.cfg.market_ping_timeout,
            proxy_url=self.cfg.proxy_url,
            on_bid_changed=self._enqueue_bid_change,
        )
        # 线程安全队列：WS 子线程写 → 主循环 _process_ws_bids 读
        self._ws_bid_queue: "queue.Queue[Tuple[str, Decimal]]" = queue.Queue()
        # 断线检测状态（主线程独占读写）
        self._ws_was_connected: bool = False
        # ── 连接稳定性统计（主线程独占，便于后期 grep 判断要否换 B2） ──────────
        self._ws_disconnect_count: int = 0
        self._ws_total_downtime: float = 0.0
        self._ws_last_down_at: float = 0.0
        self._ws_started_at: float = 0.0

        # ── 市场状态（集中式，主线程直读直写） ─────────────────────────────────
        self._markets: Dict[str, MarketState] = {}
        self._file_managed_ids: Set[str] = set()
        # pending: [(future, token_id, op_type, metadata), ...]
        # 只有主线程读写；后台/守护线程要追加时走 _pending_ops_inbox（线程安全队列），
        # 主线程每 tick 在 _check_pending_ops 开头统一 drain 进来 —— 恢复「只有主线程碰 _pending_ops」。
        self._pending_ops: List[Tuple[Any, str, str, Dict[str, Any]]] = []
        self._pending_ops_inbox: "queue.Queue[Tuple[Any, str, str, Dict[str, Any]]]" = queue.Queue()
        # 筛选器已移除、等待撤单完成的市场（替代字符串匹配，更可靠）
        self._removed_by_screener: Set[str] = set()

        # ── 交易处理 ──────────────────────────────────────────────────────────
        self.trade_logger = _file_logger("trades", self.cfg.instance_name)
        self._sell_lock = threading.Lock()
        self._selling: Set[str] = set()
        # 持仓首见时间戳 {token_id: 首次在 check_positions 看到的 time.time()}，
        # 用于 max_hold_hours 超时强平计时。主线程独占（仅 check_positions 读写）。
        self._holding_since: Dict[str, float] = {}
        self._pending_sell_tokens: Dict[str, Decimal] = {}  # BUY 成交后待触发卖单 {token_id: fill_price}
        self._pending_sell_lock = threading.Lock()
        self._processed_trades: Dict[str, float] = {}
        self._trade_lock = threading.Lock()

        # ── 缓存 ──────────────────────────────────────────────────────────────
        self._cache_lock = threading.RLock()
        self._market_info: Dict[str, dict] = {}

        # ── 信号处理 ──────────────────────────────────────────────────────────
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

        logger.info("=" * 60)
        logger.info("Maker-only Guardian V8.0 启动")
        logger.info("地址: %s | 档: %d | 量: %s | 冷却: %.0fs | 心跳: %.1fs",
                    self.address, self.cfg.maker_rank, self.cfg.maker_size,
                    self.cfg.maker_cooldown, self.cfg.heartbeat_interval)
        logger.info("筛选器: keyword=%s top1=%.0f top2=%.0f top3=%.0f existing=%.0f rewards>=%.0f",
                    self.cfg.screener_keyword or "(不过滤)",
                    self.cfg.screener_min_top1_bids, self.cfg.screener_min_top2_bids,
                    self.cfg.screener_min_top3_bids, self.cfg.screener_min_existing_size,
                    self.cfg.screener_min_daily_rewards)
        logger.info("实例: %s | midpoint=[%.2f, %.2f]",
                    self.cfg.instance_name,
                    self.cfg.screener_min_midpoint, self.cfg.screener_max_midpoint)
        logger.info("超时强平: %s | max_hold=%.1fh",
                    "启用" if self.cfg.max_hold_enabled else "关闭",
                    self.cfg.max_hold_hours)
        logger.info("=" * 60)

    # ── 信号处理 ──────────────────────────────────────────────────────────────
    def _on_signal(self, *_):
        logger.info("收到停止信号，优雅退出...")
        self.running = False

    # ── 查询 ──────────────────────────────────────────────────────────────────
    def open_orders(self) -> Optional[List[OrderInfo]]:
        """查询所有活跃订单。失败时返回 None，调用方不应认为是"无订单"。

        V8：list_open_orders() 返回 sync paginator，每页 page.items 是 OpenOrder 对象元组。
        OpenOrder 字段：id, token_id, side, price (Decimal), original_size (Decimal),
                        market（str，condition_id 的别名，即问题的 0x... 条件 ID）
        """
        try:
            pages = self.client.list_open_orders()
            result: List[OrderInfo] = []
            for page in pages:
                for o in page.items:
                    result.append(OrderInfo(
                        order_id=o.id,
                        price=safe_float(o.price),
                        size=safe_float(o.original_size),
                        side=str(o.side),
                        token_id=o.token_id,
                        market=str(o.market) if o.market else "",
                    ))
            return result
        except Exception as e:
            logger.error("查询订单失败: %s", e)
            return None

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
        """通过 REST API 获取买盘价格列表，从高到低排序。

        V8：fetch_order_book(token_id=...) 返回对象，ob.bids 是 OrderBookLevel 列表，
        每个 level 有 .price (Decimal) 和 .size (Decimal) 属性。
        """
        try:
            ob = self.client.get_order_book(token_id=token_id)
            bids_raw = ob.bids if ob else []
            prices: List[Decimal] = []
            for b in bids_raw:
                pr = safe_decimal(b.price)
                sz = safe_decimal(b.size)
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
        # V8.3: 直接挂订单簿实际档位价，不做 round_to_tick。
        # 订单簿价格本身就是该市场合法 tick 的倍数；硬编码 0.01 round 会把
        # 0.001-tick 市场的档位错误吸附（如 0.933 → 0.93），并冻结成非实际档位价。
        return bids[self.cfg.maker_rank - 1]

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
        logger.debug("[STATE] %s CANCELING %s... | %s", token_id[:16], oid[:20], reason)
        fut = self.exec_layer.cancel(oid, reason)
        self._pending_ops.append((fut, token_id, "cancel", {"order_id": oid, "reason": reason}))

    # ── 批量撤单 ──────────────────────────────────────────────────────────────
    def _batch_cancel(self, token_ids: List[str], reason: str = ""):
        """收集 market state 中的 active_id，批量发 DELETE /orders。"""
        order_ids = []
        # 保持 token_id 与 order_id 一一对应，供 _handle_batch_cancel_result 回写状态
        tid_oid_pairs: List[Tuple[str, str]] = []
        for tid in token_ids:
            ms = self._markets.get(tid)
            if ms and ms.active_id and ms.state is not ActorState.CANCELING:
                oid = ms.active_id
                order_ids.append(oid)
                tid_oid_pairs.append((tid, oid))
                ms.state = ActorState.CANCELING
                ms.state_at = time.time()
        if order_ids:
            logger.debug("[BATCH CANCEL] 批量撤单 %d 个 | %s", len(order_ids), reason)
            fut = self.exec_layer.cancel_batch(order_ids, reason)
            self._pending_ops.append((fut, "_batch_", "cancel_batch",
                                      {"tid_oid_pairs": tid_oid_pairs, "reason": reason}))

    def _handle_batch_cancel_result(self, tid_oid_pairs: List[Tuple[str, str]],
                                      reason: str, result: dict):
        """根据批量撤单结果更新状态。

        成功取消 → 清空 active_id，进入冷却，正常重挂流程。
        取消失败 → 回退 RESTING（保留 active_id），由 audit (120s) 确认真实状态：
          - 若订单仍活着：RESTING 是正确状态，不会引发双订单
          - 若订单已不在：audit 检测到 active_id 缺失后重置并重挂

        ID 归一化：对比前去掉可能的 "0x" 前缀并转小写，避免 SDK 格式不一致误判为失败。
        """
        def _norm(oid: str) -> str:
            return oid.lower().lstrip("0x")

        canceled_raw = set(result.get("canceled", []))
        canceled_norm = {_norm(c) for c in canceled_raw}

        for tid, oid in tid_oid_pairs:
            ms = self._markets.get(tid)
            if not ms or ms.state is not ActorState.CANCELING:
                continue

            success = oid in canceled_raw or _norm(oid) in canceled_norm

            if success:
                logger.debug("[BATCH CANCEL] %s 取消成功", tid[:16])
                ms.active_id = None
                ms.active_price = None
                # 筛选器已移除该市场 → STOPPED，不重新挂单
                if tid in self._removed_by_screener:
                    ms.state = ActorState.STOPPED
                    ms.state_at = time.time()
                    continue
                ms.state = ActorState.NO_ORDER
                ms.state_at = time.time()
                self._start_cooldown(ms, self.cfg.maker_cooldown)
            else:
                # 取消失败（网络错误或订单已被撤销但 id 归一化后仍不匹配）
                # 回退 RESTING，保留 active_id，等 audit 120s 后确认真实状态
                # 这样即便订单仍活着也不会触发重挂，彻底杜绝双订单
                logger.error(
                    "[BATCH CANCEL] %s 取消结果不明，回退 RESTING 等 audit 确认 | oid=%s",
                    tid[:16], oid[:20],
                )
                # 筛选器已移除 → 保持 RESTING（audit 会重新触发撤单）
                ms.state = ActorState.RESTING
                ms.state_at = time.time()

    # ── 挂单动作（异步两步） ──────────────────────────────────────────────────
    def _trigger_place(self, token_id: str):
        """Step 1: 异步获取订单簿，避免 GET /book 阻塞主循环。"""
        ms = self._markets.get(token_id)
        if not ms:
            return
        ms.state = ActorState.PLACING
        ms.state_at = time.time()
        fut = self.exec_layer.run_async(self._target_price, token_id)
        self._pending_ops.append((fut, token_id, "target_price", {}))

    # ── 异步结果处理 ──────────────────────────────────────────────────────────
    def _has_pending_op(self, token_id: str) -> bool:
        """检查该 token 是否有未完成的异步操作（含批量撤单中的订单）。

        注意：不能因为 fut.done() 就跳过 —— Future 完成但主循环尚未在
        _check_pending_ops 中回写状态时，audit 会误判该市场处于卡死状态
        并触发不必要的重置。只要 op 还在 _pending_ops 里就视为 pending。
        """
        ms = self._markets.get(token_id)
        active_id = ms.active_id if ms else None
        for fut, tid, op, meta in self._pending_ops:
            if tid == token_id:
                return True
            # 批量撤单 Future 用 "_batch_" 占位，需检查 tid_oid_pairs 中的 token_id
            if op == "cancel_batch":
                pairs = meta.get("tid_oid_pairs") or []
                for p_tid, p_oid in pairs:
                    if p_tid == token_id:
                        return True
                    if active_id and p_oid == active_id:
                        return True
        return False

    def _handle_cancel_result(self, ms: MarketState, token_id: str, ok: bool,
                               order_id: str, reason: str):
        if not ok:
            if ms.active_id is None:
                logger.warning("[CANCEL FAIL] %s 订单已不存在，忽略取消失败", order_id[:20])
                if token_id in self._removed_by_screener:
                    ms.state = ActorState.STOPPED
                    ms.state_at = time.time()
                return
            logger.error("[CANCEL FAIL] %s 订单仍存活在交易所！保持 RESTING", order_id[:20])
            ms.state = ActorState.RESTING
            ms.state_at = time.time()
            return

        logger.debug("[CANCEL OK] %s... | %s", order_id[:20], reason)
        ms.active_id = None
        ms.active_price = None
        if ms.state == ActorState.COOLING:
            return
        # 筛选器已移除该市场 → STOPPED，不重新挂单
        if token_id in self._removed_by_screener:
            ms.state = ActorState.STOPPED
            ms.state_at = time.time()
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
            logger.debug("[STATE] %s RESTING %s... price=%s", token_id[:16], order_id[:20], price)
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
        # B3 挂单门禁：WS 启用但当前断线 → 暂停挂新单（断线期不在场，防旧价被逆向吃）。
        # 冷却计时不冻结——重连后到期的市场会在后续 tick 正常挂出。
        ws_paused = self._ws_enabled and not self._market_ws.is_connected()
        for token_id, ms in list(self._markets.items()):
            if ms.state == ActorState.COOLING and now >= ms.cooldown_until:
                if token_id in self._removed_by_screener:
                    ms.state = ActorState.STOPPED
                    ms.state_at = now
                    continue
                if ws_paused:
                    continue
                self._trigger_place(token_id)

    def _enqueue_pending_op(self, op_tuple):
        """【任意线程】把 (future, token_id, op_type, meta) 放进收件箱。

        后台/守护线程不再直接 append self._pending_ops——改投 thread-safe 队列，
        由主线程 _check_pending_ops 开头统一收编。恢复「只有主线程碰 _pending_ops」。
        """
        self._pending_ops_inbox.put(op_tuple)

    def _check_pending_ops(self, now: float):
        # 先收编后台/守护线程投递的 pending op（主线程独占 _pending_ops 的唯一入口）
        while True:
            try:
                self._pending_ops.append(self._pending_ops_inbox.get_nowait())
            except queue.Empty:
                break

        completed = []
        for i, (fut, token_id, op, meta) in enumerate(self._pending_ops):
            if not fut.done():
                continue
            completed.append(i)

            try:
                result = fut.result(timeout=0)
            except Exception:
                result = False if op == "cancel" else None

            # 批量撤单特殊处理：token_id 是 "_batch_" 占位符，无法用 _markets.get 查 ms
            if op == "cancel_batch":
                pairs = meta.get("tid_oid_pairs") or []
                if isinstance(result, dict):
                    self._handle_batch_cancel_result(pairs,
                                                      meta.get("reason", ""), result)
                else:
                    # 批量撤单异常：SDK 调用失败，订单仍在交易所存活
                    # 回退 RESTING 等待下轮 poll/audit 重试，不清空 active_id
                    for tid, _oid in pairs:
                        ms_sub = self._markets.get(tid)
                        if ms_sub and ms_sub.state is ActorState.CANCELING:
                            ms_sub.state = ActorState.RESTING
                            ms_sub.state_at = time.time()
                continue

            # 重复订单清理（audit 发起，不对应状态机，只打日志）
            if op == "duplicate_cancel":
                if isinstance(result, dict):
                    canceled = result.get("canceled", [])
                    logger.info("[AUDIT] 重复买单撤销完成: %d 成功 / %d 请求",
                                len(canceled), len(meta.get("order_ids", [])))
                else:
                    logger.error("[AUDIT] 重复买单撤销异常")
                continue

            # 孤儿订单清理（audit 发起，token 已不在 _markets，只打日志）
            if op == "orphan_cancel":
                if isinstance(result, dict):
                    canceled = result.get("canceled", [])
                    logger.info("[AUDIT] 孤儿订单撤销完成: %d 成功 / %d 请求",
                                len(canceled), len(meta.get("order_ids", [])))
                else:
                    logger.error("[AUDIT] 孤儿订单撤销异常")
                continue

            # 卖单结果只打日志、不改状态机（卖出走 _selling 独立跟踪，与 _markets 状态机无关）。
            # 放在 ms 门禁之前：持仓 token 常不在 _markets 里，若门禁拦掉会静默丢失卖单确认日志。
            if op in ("limit_sell", "limit_sell_triggered"):
                tag = "LIMIT SELL" if op == "limit_sell" else "SELL-TRIGGER"
                if result:
                    logger.debug("[%s] %s 卖单已挂 id=%s price=%s",
                                 tag, token_id[:16], str(result)[:20], meta.get("price"))
                else:
                    logger.error("[%s] %s 卖单失败 price=%s",
                                 tag, token_id[:16], meta.get("price"))
                continue

            # 超时强平市价单结果（FOK）：成功即清仓；失败下一轮 position 周期重试。
            if op == "market_sell":
                if result:
                    logger.warning("[MAX-HOLD] %s FOK 市价卖成功 id=%s shares=%s",
                                   token_id[:16], str(result)[:20], meta.get("shares"))
                else:
                    logger.error("[MAX-HOLD] %s FOK 市价卖失败（流动性不足?），下轮重试 shares=%s",
                                 token_id[:16], meta.get("shares"))
                continue

            ms = self._markets.get(token_id)
            if not ms:
                continue

            if op == "cancel":
                self._handle_cancel_result(ms, token_id, bool(result),
                                            meta["order_id"], meta.get("reason", ""))
            elif op == "target_price":
                # Step 2: 拿到订单簿价格后，提交实际下单
                target = result
                if target is None:
                    logger.warning("[RETRY] %s bids不足%d档，10s后重试",
                                   token_id[:16], self.cfg.maker_rank)
                    self._start_cooldown(ms, 10.0)
                else:
                    logger.debug("[STATE] %s PLACING target=%s", token_id[:16], target)
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

    def _apply_market_targets(self, targets: List[Tuple[str, str]]):
        """将目标列表同步到 _markets：新增则创建状态，移除则停止监控并取消订单。

        targets: [(token_id, title), ...]
        此方法供 _sync_from_file (CSV 回退) 和 _apply_screener_result (内存直传) 共用。

        移除市场时，若有活跃订单则先触发异步撤单，但保留 MarketState 在
        _markets 中直到撤单完成。避免 discover() 在撤单完成前重新发现该订单
        并将其加回为"孤儿"市场（不在 _file_managed_ids 中，永不清理）。
        """
        target_ids = {t[0] for t in targets}

        # 移除：不再在目标列表中的 file-managed 市场
        removed = self._file_managed_ids - target_ids
        for token_id in list(removed):
            ms = self._markets.get(token_id)
            if not ms:
                self._file_managed_ids.discard(token_id)
                continue

            # 有未完成的异步操作 → 下轮再处理
            if ms.state is ActorState.CANCELING or self._has_pending_op(token_id):
                continue

            if ms.active_id:
                logger.debug("[SYNC] 市场已从筛选器移除，发起撤单 %s", token_id[:20])
                self._trigger_cancel(token_id, "从筛选器移除")
                self._removed_by_screener.add(token_id)
                self._file_managed_ids.discard(token_id)
                continue

            # 无活跃订单 → 安全移除
            self._markets.pop(token_id, None)
            self._file_managed_ids.discard(token_id)

        # 新增：新出现的市场
        for token_id, title in targets:
            if token_id not in self._markets:
                logger.debug("[SYNC] 新市场 %s | %s", token_id[:20], title[:50])
                ms = MarketState()
                self._markets[token_id] = ms
                stagger = random.uniform(self.cfg.cooldown_delay, 30.0)
                self._start_cooldown(ms, stagger)
            self._file_managed_ids.add(token_id)
            # 筛选器重新加入此市场 → 清除移除标记，允许正常挂单
            self._removed_by_screener.discard(token_id)

    def _sync_from_file(self):
        """从 CSV 文件同步市场（MARKET_FILE 回退路径，V7.7 起通常不使用）。"""
        csv_path = self.cfg.market_file
        if not csv_path:
            return
        targets = self._load_market_targets()
        if targets is None:
            return
        self._apply_market_targets(targets)

    def _screener_fetch(self):
        """【后台线程】筛选器纯计算：拉取+评分+筛选+构建目标。

        H1 第二小步：只读 self.cfg（冻结的 dataclass）、只做网络查询与计算，
        绝不触碰 self._markets —— 保住「主线程独占 _markets 无锁」不变量。
        返回一个 payload dict，由主线程 _apply_screener_result 应用。
        异常直接抛出，由 BackgroundDispatcher 统一捕获记录（不在此吞掉）。
        """
        from screener import fetch_and_filter, analyze_orderbooks, enrich_and_filter

        t0 = time.time()
        candidates = fetch_and_filter(self.cfg)
        # 成交量/流动性上限过滤（gamma 补查），放在 orderbook 之前：
        # 先滤掉已饱和市场，减少进入 /books 查询的候选数（尤其 Midterms 4755 候选）。
        candidates = enrich_and_filter(candidates, self.cfg)
        scored = analyze_orderbooks(candidates, self.cfg)
        # 过滤现有流动性不足的市场
        scored = [m for m in scored
                  if m.existing_total_size >= self.cfg.screener_min_existing_size]

        # 构建目标列表（与旧 CSV 输出逻辑一致：深度阈值检查每个方向）
        targets: List[Tuple[str, str]] = []
        yes_count = 0
        no_count = 0
        for m in scored:
            title = m.question
            yes_ok = (m.yes_top3_bids >= self.cfg.screener_min_top3_bids
                      and m.yes_top2_bids >= self.cfg.screener_min_top2_bids
                      and m.yes_top1_bids >= self.cfg.screener_min_top1_bids)
            no_ok = (m.no_top3_bids >= self.cfg.screener_min_top3_bids
                     and m.no_top2_bids >= self.cfg.screener_min_top2_bids
                     and m.no_top1_bids >= self.cfg.screener_min_top1_bids)
            if yes_ok:
                targets.append((m.yes_token_id, f"{title} [YES]" if title else ""))
                yes_count += 1
            if no_ok:
                targets.append((m.no_token_id, f"{title} [NO]" if title else ""))
                no_count += 1

        return {
            "scored": scored,
            "targets": targets,
            "yes_count": yes_count,
            "no_count": no_count,
            "elapsed": time.time() - t0,
        }

    def _apply_screener_result(self, payload):
        """【主线程】应用筛选结果：写 _markets + 输出 CSV + 日志。"""
        scored = payload["scored"]
        # 直接内存同步（唯一 _markets 写入点，主线程）
        self._apply_market_targets(payload["targets"])
        # CSV 输出（调试用）
        self._save_screener_csv(scored)
        logger.info("[SCREENER] %d markets in %.1fs | yes:%d no:%d | next in %ds",
                    len(scored), payload["elapsed"],
                    payload["yes_count"], payload["no_count"],
                    int(self.cfg.screener_interval))

    def _save_screener_csv(self, scored):
        """保存筛选结果 CSV（调试用，不影响逻辑）。"""
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "data", self.cfg.instance_name)
        os.makedirs(data_dir, exist_ok=True)
        csv_path = os.path.join(data_dir, "screener_latest.csv")
        csv_tmp = os.path.join(data_dir, "screener_latest.csv.tmp")

        sorted_m = sorted(scored, key=lambda m: m.reward_per_dollar, reverse=True)
        with open(csv_tmp, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Market", "minSz", "Reward/day", "Competition",
                             "Comp_YES", "Comp_NO", "Comp_YES_2", "Comp_NO_2",
                             "Comp_YES_1", "Comp_NO_1",
                             "Volume24h", "Liquidity", "VolumeTotal",
                             "yes_token_id", "no_token_id"])
            for m in sorted_m:
                yes_ok = (m.yes_top3_bids >= self.cfg.screener_min_top3_bids
                          and m.yes_top2_bids >= self.cfg.screener_min_top2_bids
                          and m.yes_top1_bids >= self.cfg.screener_min_top1_bids)
                no_ok = (m.no_top3_bids >= self.cfg.screener_min_top3_bids
                         and m.no_top2_bids >= self.cfg.screener_min_top2_bids
                         and m.no_top1_bids >= self.cfg.screener_min_top1_bids)
                writer.writerow([
                    m.question,
                    f"{m.min_size:.0f}",
                    f"{m.total_daily_rewards:.2f}",
                    f"{m.existing_total_size:.0f}",
                    f"{m.yes_top3_bids:.0f}",
                    f"{m.no_top3_bids:.0f}",
                    f"{m.yes_top2_bids:.0f}",
                    f"{m.no_top2_bids:.0f}",
                    f"{m.yes_top1_bids:.0f}",
                    f"{m.no_top1_bids:.0f}",
                    f"{m.volume24hr:.0f}",
                    f"{m.liquidity:.0f}",
                    f"{m.volume:.0f}",
                    m.yes_token_id if yes_ok else "",
                    m.no_token_id if no_ok else "",
                ])
        os.replace(csv_tmp, csv_path)

    # ── 订单发现 ──────────────────────────────────────────────────────────────
    def _discover_fetch(self):
        """【后台线程】查询 open_orders，纯 REST，零 _markets 访问。

        H1 第二小步：只发一次 list_open_orders 分页查询，返回 orders 列表。
        open_orders() 返回 None 时抛异常 → BackgroundDispatcher 统一记录 ERROR。
        结果由主线程 _apply_discover_result 应用。
        """
        orders = self.open_orders()
        if orders is None:
            raise RuntimeError("订单查询失败，本周期 discover 跳过")
        return orders

    def _apply_discover_result(self, orders):
        """【主线程】应用 discover 结果：清理 STOPPED + 发现新市场 + CSV 同步 + 日志。"""
        # 清理 STOPPED 状态市场（主线程独占写 _markets，无锁安全）
        stopped = [tid for tid, ms in self._markets.items() if ms.state == ActorState.STOPPED]
        for tid in stopped:
            self._markets.pop(tid, None)
            self._file_managed_ids.discard(tid)
            # 不清除 _removed_by_screener：防止 discover 重新加回孤儿订单
            logger.debug("[DISCOVER] 清理 STOPPED 市场 %s", tid[:20])

        buys = {o.token_id: o for o in orders if o.side.upper() == "BUY"}
        now = time.time()

        # 发现新市场（已有挂单）
        for tid in set(buys.keys()) - set(self._markets.keys()):
            # 跳过筛选器已移除的市场，避免重新监控孤儿订单
            if tid in self._removed_by_screener:
                o = buys[tid]
                logger.info(
                    "[DISCOVER] 跳过已移除市场 %s (order=%s)，等待 audit() 清理",
                    tid[:20], o.order_id[:20],
                )
                continue

            o = buys[tid]
            logger.debug("[DISCOVER] 新市场 %s price=%s", tid[:20], o.price)
            self._markets[tid] = MarketState(
                state=ActorState.RESTING,
                state_at=now,
                active_id=o.order_id,
                active_price=safe_decimal(o.price),
            )
            # 注册到 file-managed 集合，使其纳入筛选器移除循环的管理范围
            self._file_managed_ids.add(tid)

        # 从 CSV 文件同步
        self._sync_from_file()

        logger.debug("[DISCOVER] 守护 %d 个市场", len(self._markets))

    # ── 批量轮询最佳买价 ──────────────────────────────────────────────────────
    def _poll_fetch(self, token_ids):
        """【后台线程】批量查 /books，纯 REST，零 _markets 访问。

        token_ids 由主线程调度时快照传入（后台绝不读 self._markets）。
        返回扁平的 book item 列表，主线程 _apply_poll_result 应用。
        分块失败只跳过该块，成功块照常返回（与旧逐块 continue 语义一致）。
        """
        if not token_ids:
            return []
        items: List[dict] = []
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
            if isinstance(books, list):
                items.extend(books)
        return items

    def _apply_bid_change(self, ms: MarketState, token_id: str,
                          new_bid: Decimal, new_ask: Optional[Decimal] = None
                          ) -> Tuple[bool, bool]:
        """【主线程】把单个市场的 best_bid 变化应用到状态机。

        REST 轮询(_apply_poll_result)与 WS 推送(_process_ws_bids)共用此唯一逻辑，
        确保两条路径永不漂移（这正是旧 guardian_wss 子类翻车的教训）。
        仅主线程调用，写 _markets 无锁。调用方需在调用前完成
        `not ms / ms.state is STOPPED` 门禁。

        new_ask=None（WS 只推 bid, B1 bid-only）时不覆盖 ms.best_ask，
        保留上次 REST 对账写入的 ask 值。

        返回 (processed, needs_cancel)：
          processed    —— 是否为一次有效更新（init 或 bid 变化），供日志计数
          needs_cancel —— 该 token 是否需撤单（调用方收集后批量撤）
        """
        # 首次初始化 best_bid
        if ms.best_bid is None:
            ms.best_bid = new_bid
            if new_ask is not None:
                ms.best_ask = new_ask
            logger.debug("[BID INIT] %s best_bid=%s", token_id[:16], new_bid)
            return True, False

        # bid 未变 → 无操作
        if new_bid == ms.best_bid:
            return False, False

        # bid 变化 → 更新缓存并触发状态机
        ms.best_bid = new_bid
        if new_ask is not None:
            ms.best_ask = new_ask
        logger.debug("[BID] %s best_bid=%s state=%s",
                     token_id[:16], new_bid, ms.state.name)

        if ms.state is ActorState.RESTING:
            return True, True
        if ms.state is ActorState.NO_ORDER and ms.cooldown_until <= time.time():
            if token_id in self._removed_by_screener:
                ms.state = ActorState.STOPPED
                ms.state_at = time.time()
            else:
                self._start_cooldown(ms, self.cfg.maker_cooldown)
        return True, False

    def _apply_poll_result(self, items):
        """【主线程】应用 best_bid 变化：更新 ms、收集撤单、触发批量撤单。"""
        polled = 0
        cancels: List[str] = []
        for item in items:
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

            processed, needs_cancel = self._apply_bid_change(ms, aid, new_bid, new_ask)
            if processed:
                polled += 1
            if needs_cancel:
                cancels.append(aid)

        if cancels:
            self._batch_cancel(cancels, "best_bid变化")

        logger.debug("[POLL] 轮询 %d 个 Actor", polled)

    # ── 市场频道 WS（Round 2：毫秒级 bid 推送） ────────────────────────────────
    def _enqueue_bid_change(self, token_id: str,
                            old_bid: Optional[Decimal], new_bid: Decimal) -> None:
        """【WS 子线程】bid 变化事件入队，由主线程 _process_ws_bids 消费。

        WS 线程绝不碰 _markets——只投线程安全队列，保住主线程独占不变量。
        """
        self._ws_bid_queue.put((token_id, new_bid))

    def _process_ws_bids(self) -> None:
        """【主线程】drain WS bid 队列，应用到状态机（与 REST 共用 _apply_bid_change）。

        WS 只推 bid（B1 bid-only），new_ask 传 None → 不覆盖 ms.best_ask，
        由 30s REST 对账维持 ask 值。
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
            _processed, needs_cancel = self._apply_bid_change(ms, token_id, new_bid, None)
            if needs_cancel:
                cancels.append(token_id)
        if cancels:
            self._batch_cancel(cancels, "WS bid变化")

    def _sync_ws_subscriptions(self) -> None:
        """【主线程】对齐 _markets 集合 ↔ MarketWS 订阅列表。

        新市场（discover/screener/file 加入）→ 增订；移除的市场 → 退订。
        MarketWS.subscribe_more/unsubscribe 线程安全；未连接时仅更新待订阅集合，
        连接建立后由 on_open 自动全量重订。
        """
        if not self._ws_enabled:
            return
        current = set(self._markets.keys())
        subscribed = self._market_ws.subscribed_ids()
        to_sub = current - subscribed
        to_unsub = subscribed - current
        if to_sub:
            self._market_ws.subscribe_more(list(to_sub))
        if to_unsub:
            self._market_ws.unsubscribe(list(to_unsub))
        if to_sub or to_unsub:
            logger.debug("[WS SUB] 同步订阅: 当前%d个 +%d -%d",
                         len(current), len(to_sub), len(to_unsub))

    def _check_ws_connection(self, now: float) -> None:
        """【主线程】检测 WS 连接状态翻转，处理断线撤单 + 稳定性统计。

        B3 策略：连接 True→False 的那一刻立即撤全部 RESTING（防逆向成交），
        断线期由 _check_cooldowns 门禁暂停挂新单，重连后随冷却自然重挂。
        统计字段仅主线程读写，无锁安全。
        """
        if not self._ws_enabled:
            return
        connected = self._market_ws.is_connected()
        if self._ws_was_connected and not connected:
            # 断线沿：撤全部活跃挂单
            self._ws_disconnect_count += 1
            self._ws_last_down_at = now
            resting = [tid for tid, ms in self._markets.items()
                       if ms.state is ActorState.RESTING and ms.active_id]
            logger.warning("[WS断线] 第%d次断线，撤全部 %d 个挂单，暂停挂新单",
                           self._ws_disconnect_count, len(resting))
            if resting:
                self._batch_cancel(resting, "WS断线")
        elif not self._ws_was_connected and connected:
            # 重连沿：累加本次断线时长
            if self._ws_last_down_at > 0.0:
                self._ws_total_downtime += now - self._ws_last_down_at
                logger.info("[WS RECONNECT] 断线 %.1fs 后恢复，将随冷却重挂",
                            now - self._ws_last_down_at)
                self._ws_last_down_at = 0.0
        self._ws_was_connected = connected

    def _log_ws_stats(self) -> None:
        """【主线程】输出 WS 连接稳定性汇总（搭在 prune 任务里，5min 一条）。"""
        if not self._ws_enabled or self._ws_started_at <= 0.0:
            return
        uptime = time.time() - self._ws_started_at
        downtime = self._ws_total_downtime
        # 若当前正处于断线中，把进行中的这段也算进去
        if self._ws_last_down_at > 0.0:
            downtime += time.time() - self._ws_last_down_at
        avail = (uptime - downtime) / uptime * 100 if uptime > 0 else 0.0
        logger.info("[WS STATS] 断线 %d 次 | 累计断线 %.0fs | 运行 %.0fs | 可用率 %.1f%%",
                    self._ws_disconnect_count, downtime, uptime, avail)

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
        """查询条件代币链上余额。

        V8.1：改用 SDK get_balance_allowance（内部正确签 L2 头）。
        旧实现手搓 REST 只发 POLY_ADDRESS → 该端点需完整 L2 签名 → 恒 401 →
        静默返回 0.0，导致 check_positions / _sell_single_position 全部跳过，
        持仓永远挂不出卖单。BalanceAllowance.balance 为 base units（1e6）。
        """
        try:
            ba = self.client.get_balance_allowance(
                asset_type="CONDITIONAL",
                token_id=token_id,
            )
            return ba.balance / 1_000_000
        except Exception as e:
            logger.error("余额查询失败 %s...: %s", token_id[:20], e)
            return 0.0

    # ── 审计 ──────────────────────────────────────────────────────────────────
    def _audit_timeouts_only(self):
        """open_orders 失败时的降级审计：仅处理状态超时，不动 RESTING 检查。"""
        now = time.time()
        for token_id, ms in list(self._markets.items()):
            if self._has_pending_op(token_id):
                continue
            if ms.state == ActorState.CANCELING \
                    and (now - ms.state_at) > self.cfg.stale_timeout:
                logger.warning("[AUDIT] %s CANCELING 超时重置", token_id[:16])
                self.exec_layer.clear_place_by_asset(token_id)
                ms.active_id = None
                ms.active_price = None
                ms.state = ActorState.NO_ORDER
                ms.state_at = now
                self._start_cooldown(ms, self.cfg.maker_cooldown)
            elif ms.state == ActorState.PLACING \
                    and (now - ms.state_at) > self.cfg.stale_timeout:
                logger.warning("[AUDIT] %s PLACING 超时重置", token_id[:16])
                self.exec_layer.clear_place_by_asset(token_id)
                ms.active_id = None
                ms.active_price = None
                ms.state = ActorState.NO_ORDER
                ms.state_at = now
                self._start_cooldown(ms, self.cfg.maker_cooldown)
            elif ms.state is ActorState.NO_ORDER \
                    and (now - ms.state_at) > self.cfg.stale_timeout:
                logger.warning("[AUDIT] %s NO_ORDER 卡死强制重挂", token_id[:16])
                self._start_cooldown(ms, self.cfg.maker_cooldown)
        logger.debug("[AUDIT] 降级完成（订单查询失败，跳过订单校验）")

    def _audit_fetch(self):
        """【后台线程】审计的两次阻塞 REST：open_orders + 批量 best_bid。

        H1 第二小步：只做网络查询，零 _markets 访问。返回 (orders, best_bid_map)。
        open_orders 失败时返回 (None, {})，主线程据此走降级审计。
        """
        orders = self.open_orders()
        if orders is None:
            return (None, {})
        buys = [o for o in orders if o.side.upper() == "BUY"]
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
        return (orders, best_bid_map)

    def _apply_audit_result(self, payload):
        """【主线程】应用审计结果：状态校验 + 纠偏 + 重复订单清理（全部写 _markets）。"""
        orders, best_bid_map = payload
        if orders is None:
            # API 查询失败，仅处理状态超时，跳过依赖订单列表的检查
            self._audit_timeouts_only()
            return
        order_map = {o.order_id: o for o in orders}
        buys = [o for o in orders if o.side.upper() == "BUY"]
        now = time.time()

        # 逐个市场审计
        for token_id, ms in list(self._markets.items()):
            # 有 pending Future 的市场跳过——状态机正在变更中，不应干预
            if self._has_pending_op(token_id):
                logger.debug("[AUDIT] %s 有 pending 操作，跳过", token_id[:16])
                continue

            # CANCELING 兜底重置：仅当 Future 已完成但状态未被回写时（异常路径）
            if ms.state == ActorState.CANCELING \
                    and (now - ms.state_at) > self.cfg.stale_timeout:
                logger.warning("[AUDIT] %s CANCELING Future 完成但状态未回写，兜底重置",
                               token_id[:16])
                self.exec_layer.clear_place_by_asset(token_id)
                ms.active_id = None
                ms.active_price = None
                ms.state = ActorState.NO_ORDER
                ms.state_at = now
                self._start_cooldown(ms, self.cfg.maker_cooldown)
                continue

            # PLACING 卡死重置
            if ms.state == ActorState.PLACING \
                    and (now - ms.state_at) > self.cfg.stale_timeout:
                logger.warning("[AUDIT] %s PLACING 超时重置", token_id[:16])
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

        # ── 重复订单检测：同一 token 有多笔 BUY 单 → 只保留 active_id 对应的 ──
        # 正常情况每个 token 最多一笔，若出现多笔说明之前某次 cancel 失败后又重挂
        buys_by_token: Dict[str, List] = {}
        for o in buys:
            buys_by_token.setdefault(o.token_id, []).append(o)

        dup_oids: List[str] = []
        for token_id, token_orders in buys_by_token.items():
            if len(token_orders) <= 1:
                continue
            if self._has_pending_op(token_id):
                continue  # 有 pending 操作，下轮再检查
            ms = self._markets.get(token_id)
            keep_id = ms.active_id if (ms and ms.active_id) else None
            order_ids_in_response = {o.order_id for o in token_orders}
            # keep_id 不在本次 open_orders 响应里（API 延迟）→ 跳过，下轮再判断
            # 避免误撤状态机正在跟踪的订单
            if keep_id and keep_id not in order_ids_in_response:
                logger.debug(
                    "[AUDIT] %s active_id 不在 open_orders 响应中，跳过重复订单清理",
                    token_id[:16],
                )
                continue
            # keep_id 为 None 时保留最先出现的那笔（index 0），撤掉其余
            kept = False
            for o in token_orders:
                if not kept and (keep_id is None or o.order_id == keep_id):
                    kept = True
                    continue
                dup_oids.append(o.order_id)
            logger.warning(
                "[AUDIT] %s 检测到 %d 份重复买单，撤销 %d 份 (keep=%s)",
                token_id[:16], len(token_orders), len(token_orders) - 1,
                (keep_id or "first")[:20],
            )

        if dup_oids:
            fut = self.exec_layer.cancel_batch(dup_oids, "重复订单清理")
            # 这些是"野单"，不对应状态机 active_id，用专属 op 类型只做日志
            self._pending_ops.append((fut, "_dupes_", "duplicate_cancel",
                                      {"order_ids": dup_oids}))

        # ── 筛选器已移除但订单仍活着的市场 → 重试撤单 ──────────────────────
        # _handle_batch_cancel_result 失败路径回退 RESTING 后，这里负责补救
        for token_id, ms in list(self._markets.items()):
            if (token_id in self._removed_by_screener
                    and ms.state is ActorState.RESTING
                    and ms.active_id
                    and not self._has_pending_op(token_id)):
                logger.warning(
                    "[AUDIT] %s 筛选器已移除但订单仍活，重新触发撤单", token_id[:16]
                )
                self._trigger_cancel(token_id, "审计重试撤单(筛选器已移除)")

        # ── 孤儿订单清理：筛选器已移除且已从 _markets pop，但订单仍活在交易所 ──
        # discover 把这些订单甩给 audit（pop 后不在 _markets），而上面的 _markets
        # 循环够不着它们 → 直接扫 open_orders，凡 token 在 _removed_by_screener
        # 但不在 _markets 的一律撤。避免 discover/audit 互相等待的死循环孤儿单。
        inflight_orphan_oids: Set[str] = set()
        for _fut, _tid, _op, _meta in self._pending_ops:
            if _op == "orphan_cancel":
                inflight_orphan_oids.update(_meta.get("order_ids", []))
        orphan_oids = [
            o.order_id for o in buys
            if o.token_id in self._removed_by_screener
            and o.token_id not in self._markets
            and o.order_id not in inflight_orphan_oids
        ]
        if orphan_oids:
            logger.warning("[AUDIT] 检测到 %d 份孤儿订单（已移除市场仍挂单），撤销",
                           len(orphan_oids))
            fut = self.exec_layer.cancel_batch(orphan_oids, "孤儿订单清理")
            self._pending_ops.append((fut, "_orphans_", "orphan_cancel",
                                      {"order_ids": orphan_oids}))

        logger.debug("[AUDIT] 完成 %d 买单检查 | 纠偏 %d 个 | 重复 %d 个 | 孤儿 %d 个",
                     len(buys), len(overpriced), len(dup_oids), len(orphan_oids))

    # ── 交易处理 ──────────────────────────────────────────────────────────────
    def handle_trade(self, data: dict):
        tid = data.get("id", "")
        status = str(data.get("status", "")).upper()

        # 仅处理 CONFIRMED；MATCHED/MINED 时链上余额尚未到账，跳过。
        # 【必须先过滤 status，再去重】同一笔成交会依次推送
        # MATCHED→MINED→CONFIRMED，三者共用同一 trade id。若先去重，
        # 先到的 MATCHED/MINED 会占用去重槽位，导致同 id 的 CONFIRMED
        # 被误判为"已处理"而丢弃 → trades.log 恒空、即时卖单从不触发。
        if status != "CONFIRMED":
            return

        # ── 去重：同一 trade_id 的 CONFIRMED 只处理一次 ──────────────────────
        with self._trade_lock:
            if tid in self._processed_trades:
                return
            self._processed_trades[tid] = time.time()

        # ── 从 maker_orders 找我们自己的成交 ──────────────────────────────────
        maker_orders = data.get("maker_orders") or []
        # V8：改用 maker_address（钱包地址）匹配，比 api_key UUID 更可靠
        our_orders = [
            m for m in maker_orders
            if (m.get("maker_address") or "").lower() == self.address.lower()
        ]
        our_fill = sum(safe_float(m.get("matched_amount", 0)) for m in our_orders)

        # 优先用 maker_orders 里我们订单的 asset_id / price / outcome
        # 顶层字段是 taker 那侧的数据（互补 token），不能直接用
        if our_orders:
            asset_id = our_orders[0].get("asset_id") or data.get("asset_id") or data.get("token_id", "")
            price    = safe_float(our_orders[0].get("price") or data.get("price", 0))
            outcome  = our_orders[0].get("outcome") or data.get("outcome", "")
            side     = str(our_orders[0].get("side", data.get("side", ""))).upper()
        else:
            asset_id = data.get("asset_id") or data.get("token_id", "")
            price    = safe_float(data.get("price", 0))
            outcome  = data.get("outcome", "")
            side     = str(data.get("side", "")).upper()

        mi = self.market_info(asset_id)
        if side == "BUY":
            self.trade_logger.info(json.dumps({
                "buy_confirmed": {
                    "trade_id": tid, "token_id": asset_id, "side": "BUY",
                    "size": our_fill, "price": price,
                    "title": mi.get("title", "未知"), "outcome": outcome,
                }
            }, ensure_ascii=False))
            logger.info("[BUY CONFIRMED] %s | %s | size=%.4f price=%s",
                        mi.get("title", "未知")[:40], outcome, our_fill, price)
            # BUY 成交立即排队触发卖单，不等 position_interval(120s) 定时轮询
            with self._pending_sell_lock:
                self._pending_sell_tokens[asset_id] = Decimal(str(price))
        else:
            self.trade_logger.info(json.dumps({
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
                min_ask = Decimal(str(entry)) - self.cfg.sell_min_bid_gap
                if ba < min_ask:
                    existing = sell_map.get(tid)
                    if existing:
                        logger.warning(
                            "[POSITION] %s best_ask=%s < 成本%.4f-%.2f=%.4f，取消卖单等待",
                            tid[:16], ba, entry, self.cfg.sell_min_bid_gap, min_ask)
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

    # ── BUY 成交即时触发卖单 ───────────────────────────────────────────────────
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
            bal = Decimal("0")
            for attempt in range(1, 6):
                bal = self.onchain_balance(tid)
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
                    # V8：使用 SDK 派生的实时凭据（client.credentials），
                    # 而非 cfg.api_key 等 Optional 环境变量（可能为 None 或过期）
                    "apiKey": self.client.credentials.key,
                    "secret": self.client.credentials.secret,
                    "passphrase": self.client.credentials.passphrase,
                },
                "type": "user",
                "markets": [],
                "assets_ids": [],
                "initial_dump": True,
            })),
            on_message=self.ws_router.on_user_message,
        )

        # 2b. 启动市场频道 WS（Round 2：毫秒级 bid 推送；kill switch 关闭时跳过）
        if self._ws_enabled:
            self._ws_started_at = time.time()
            self._market_ws.start()

        # 3. 发现已有订单（启动阶段主线程同步执行一次，播种 _markets 后再进主循环）
        try:
            self._apply_discover_result(self._discover_fetch())
        except Exception as e:
            logger.error("[DISCOVER] 启动发现失败: %s", e)

        # 3b. 播种后立即同步一次订阅，把已发现的市场订上（不必等首个 sub_sync 周期）
        self._sync_ws_subscriptions()

        # 4. 主循环（H1 第一步：单一 1s tick + 独立计时器）
        #    调度粒度回到真正的 1s —— poll 不再被旧的内层 10×1s 循环拖成 ~10s。
        #    周期任务此时仍同步执行（阻塞问题由 H1 第二步「后台化」解决）。
        # 后台结果 → 主线程应用处理器（key = BackgroundDispatcher 任务名）
        bg_handlers = {
            "screener": self._apply_screener_result,
            "discover": self._apply_discover_result,
            "poll": self._apply_poll_result,
            "audit": self._apply_audit_result,
            # position：整个 check_positions 在后台自成闭环（零 _markets 访问，
            # 卖单互斥走 _selling、pending 走收件箱），无 apply 阶段 →
            # no-op 处理器，避免 _drain_background 报"无处理器"。
            "position": lambda _payload: None,
        }

        scheduler = PeriodicScheduler()
        # 各周期任务到期时提交到后台（只算不写），单飞——上一次没跑完就跳过本次。
        # _markets 写入一律在主线程 _drain_background / _apply_* 里发生。
        scheduler.add("screener", self.cfg.screener_interval,
                      lambda: self._bg.submit("screener", self._screener_fetch),
                      run_immediately=True)
        # discover：只有 open_orders() REST 在后台跑，_markets 写入在主线程。
        # 不 run_immediately——与迁移前一致，首次在 discover_interval 后触发。
        scheduler.add("discover", self.cfg.discover_interval,
                      lambda: self._bg.submit("discover", self._discover_fetch))
        # poll：token_ids 快照在主线程取（此 lambda 由 scheduler.tick 在主线程调用），
        # 传给后台 _poll_fetch 查 /books；结果由 _apply_poll_result 应用。
        # WS 启用时 poll 降到 30s 仅做对账兜底；关闭时保持 3s 作为唯一数据源。
        poll_interval = (self.cfg.ws_rest_reconcile_interval
                         if self._ws_enabled else self.cfg.best_bid_poll_interval)
        scheduler.add("poll", poll_interval,
                      lambda: self._bg.submit("poll", self._poll_fetch,
                                              list(self._markets.keys())))
        scheduler.add("audit", self.cfg.audit_interval,
                      lambda: self._bg.submit("audit", self._audit_fetch))
        scheduler.add("position", self.cfg.position_interval,
                      lambda: self._bg.submit("position", self.check_positions))
        # ws_sub_sync：定期对齐 _markets ↔ WS 订阅列表（新增/移除市场）
        if self._ws_enabled:
            scheduler.add("ws_sub_sync", self.cfg.ws_sub_sync_interval,
                          self._sync_ws_subscriptions)

        def _prune_and_ws_stats():
            self._prune_caches()
            self._log_ws_stats()
        scheduler.add("prune", self.cfg.cache_prune_interval, _prune_and_ws_stats)

        while self.running:
            try:
                # 周期任务：各自到期才触发（PeriodicScheduler 内部逐任务捕获异常）
                scheduler.tick()
                # 每 tick 必做的轻活（非阻塞，毫秒级）
                now = time.time()
                self._check_ws_connection(now)    # WS 断线检测 + 撤单 + 统计（在挂单门禁之前）
                self._process_ws_bids()           # 消费 WS 毫秒级 bid 推送
                self._check_cooldowns(now)
                self._check_pending_ops(now)      # 回写撤单/挂单结果
                self._check_pending_sells()       # 消费 BUY 成交即时卖单队列
                self._drain_background(bg_handlers)  # 应用后台任务结果（主线程写 _markets）
            except Exception as e:
                logger.error("主循环异常: %s", e, exc_info=True)
            time.sleep(1)

        # 6. 优雅关闭
        self._shutdown()

    def _drain_background(self, handlers):
        """【主线程】取出后台任务结果并应用。

        后台线程只产出「结果 payload」，所有 _markets 写入都在这里发生 ——
        保住主线程独占 _markets 无锁的不变量。失败结果已由 BackgroundDispatcher
        记录日志，这里只跳过（下个周期 scheduler 会重新提交）。
        """
        for name, ok, payload in self._bg.drain():
            if not ok:
                continue  # 异常已在 BackgroundDispatcher._run 里 log
            handler = handlers.get(name)
            if handler is None:
                logger.warning("[BG] 无处理器的后台结果: %s", name)
                continue
            try:
                handler(payload)
            except Exception as e:
                logger.error("[BG] 应用 %s 结果异常: %s", name, e, exc_info=True)

    def _shutdown(self):
        logger.info("开始优雅关闭...")

        # 停止后台调度器（纯计算任务，不必等待收尾）
        self._bg.shutdown(wait=False)

        # 停止市场频道 WS
        if self._ws_enabled:
            self._market_ws.stop()
            self._market_ws.join(timeout=3.0)

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
