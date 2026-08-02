#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""WssGuard — 市场 WS 守卫，管理订单监控与撤单逻辑。（V8：polymarket-client SDK）

职责：
1. 维护 _order_map {token_id: order_id}（每 30s 通过 list_open_orders 核对）
2. 当 MarketWS 推送 best_bid 变化时，立即异步撤销对应 BUY 挂单
3. 当 WS 断线时，立即调用 cancel_all 取消全部挂单，暂停监控
4. 当 WS 重连时，刷新订阅列表，恢复监控

V8 变更：
 - 用 SecureClient（polymarket-client）替代 ClobClient（py_clob_client_v2）
 - list_open_orders() 返回 sync paginator，字段用属性访问（o.token_id / o.id / o.side）
 - cancel_order(order_id=...) — 不再需要 OrderPayload 包装
 - cancel_all() 返回对象（.canceled / .not_canceled），不再是 dict
 - from_env() 用 SecureClient.create() 构造客户端
"""

from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Dict, Optional, Set

from polymarket import SecureClient

from .cache import BidCache
from .market_ws import MarketWS

logger = logging.getLogger("wss.guard")


class WssGuard:
    """市场 WebSocket 撤单守卫（V8 版）。

    使用方式：
        guard = WssGuard.from_env()
        guard.start()
        guard.join()   # 阻塞直到 Ctrl-C
    """

    def __init__(
        self,
        client: SecureClient,
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        reconnect_delay: float = 5.0,
        ping_interval: float = 10.0,
        reconcile_interval: float = 30.0,
        proxy_url: Optional[str] = None,
    ) -> None:
        self._client = client
        self._reconcile_interval = reconcile_interval
        self._running = False

        # 订单状态（token_id → order_id，仅跟踪 BUY 挂单）
        self._order_map: Dict[str, str] = {}
        self._order_lock = threading.Lock()

        # 正在撤单中的 token_id，防止重复触发
        self._canceling: Set[str] = set()
        self._cancel_lock = threading.Lock()

        # WS 断线时暂停撤单触发（避免 cancel_all 后又触发单笔撤单）
        self._paused = False

        # 核心组件
        self._cache = BidCache()
        self._market_ws = MarketWS(
            cache=self._cache,
            url=ws_url,
            reconnect_delay=reconnect_delay,
            ping_interval=ping_interval,
            proxy_url=proxy_url,
            on_bid_changed=self._on_bid_changed,
            on_disconnect=self._on_disconnect,
            on_reconnect=self._on_reconnect,
        )

        # 撤单线程池（4 个 worker 足够，cancel 为低延迟 REST 调用）
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="wss-cancel")

        # 后台线程
        self._reconcile_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    # ── 工厂方法 ─────────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> "WssGuard":
        """从当前环境变量（已通过 dotenv 加载）构造 WssGuard。

        V8：用 SecureClient.create() 代替 ClobClient；proxy_addr 对应 Proxy Wallet 旧账户。
        """
        pk = os.environ["PK"]
        proxy_addr = os.environ.get("PROXY_ADDRESS", "") or None

        # V8：SDK 自动派生 API 凭据，自动检测签名类型（Proxy/EOA）
        client = SecureClient.create(private_key=pk, wallet=proxy_addr)

        ws_url = os.environ.get(
            "WS_MARKET_URL",
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        )
        reconnect_delay = float(os.environ.get("WS_RECONNECT_DELAY", "5"))
        ping_interval = float(os.environ.get("MARKET_PING_INTERVAL", "10"))
        reconcile_interval = float(os.environ.get("RECONCILE_INTERVAL", "30"))
        proxy_url = (
            os.environ.get("HTTPS_PROXY")
            or os.environ.get("HTTP_PROXY")
            or os.environ.get("ALL_PROXY")
            or None
        )

        host = os.environ.get("CLOB_API_URL", "https://clob.polymarket.com")
        logger.info(
            "[WssGuard] 初始化 host=%s ws=%s reconcile=%.0fs",
            host, ws_url, reconcile_interval,
        )
        return cls(
            client=client,
            ws_url=ws_url,
            reconnect_delay=reconnect_delay,
            ping_interval=ping_interval,
            reconcile_interval=reconcile_interval,
            proxy_url=proxy_url,
        )

    # ── 生命周期 ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """初始化订单列表，启动 WS 连接和定时核对线程。"""
        if self._running:
            return
        self._running = True
        self._stop_event.clear()

        logger.info("[WssGuard] 初始拉取挂单...")
        self._refresh_subscriptions(is_initial=True)

        self._market_ws.start()

        self._reconcile_thread = threading.Thread(
            target=self._reconcile_loop,
            daemon=True,
            name="wss-reconcile",
        )
        self._reconcile_thread.start()
        logger.info("[WssGuard] 已启动，监控 %d 个市场", len(self._order_map))

    def stop(self) -> None:
        """优雅停止：取消全部挂单，关闭 WS 和线程池。"""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()

        logger.info("[WssGuard] 停止中，取消全部挂单...")
        self._do_cancel_all("WssGuard 手动停止")

        self._market_ws.stop()
        self._executor.shutdown(wait=True)
        logger.info("[WssGuard] 已停止")

    def join(self) -> None:
        """阻塞直到 stop() 被调用（用于主线程等待）。"""
        self._stop_event.wait()

    # ── MarketWS 回调 ─────────────────────────────────────────────────────────

    def _on_bid_changed(
        self, token_id: str, old_bid: Optional[Decimal], new_bid: Decimal
    ) -> None:
        """WS 推送 best_bid 变化时回调（运行在 WS 子线程）。"""
        if self._paused:
            return

        with self._order_lock:
            order_id = self._order_map.get(token_id)
        if not order_id:
            return

        with self._cancel_lock:
            if token_id in self._canceling:
                return
            self._canceling.add(token_id)

        logger.debug(
            "[WssGuard] bid变化 %s  %s → %s，触发撤单",
            token_id[:16], old_bid, new_bid,
        )
        self._executor.submit(self._do_cancel_one, token_id, order_id, new_bid)

    def _on_disconnect(self) -> None:
        """WS 断线回调（运行在 WS 子线程）。"""
        self._paused = True
        logger.warning("[WssGuard] 市场WS断线，立即取消全部挂单，暂停监控")
        self._executor.submit(self._do_cancel_all, "市场WS断线")

    def _on_reconnect(self) -> None:
        """WS 重连成功回调（运行在 WS 子线程）。"""
        logger.info("[WssGuard] 市场WS重连，刷新订阅列表")
        self._refresh_subscriptions(is_initial=False)
        self._paused = False
        logger.info("[WssGuard] 恢复监控，当前 %d 个市场", len(self._order_map))

    # ── 撤单执行 ─────────────────────────────────────────────────────────────

    def _do_cancel_one(self, token_id: str, order_id: str, trigger_bid: Decimal) -> None:
        """单笔撤单（在线程池中执行）。V8：cancel_order(order_id=...)"""
        try:
            self._client.cancel_order(order_id=order_id)
            logger.info(
                "[WssGuard] 撤单成功 %s  order=%s  bid=%s",
                token_id[:16], order_id[:20], trigger_bid,
            )
        except Exception as e:
            logger.error(
                "[WssGuard] 撤单失败 %s  order=%s: %s",
                token_id[:16], order_id[:20], e,
            )
        finally:
            with self._cancel_lock:
                self._canceling.discard(token_id)
            with self._order_lock:
                if self._order_map.get(token_id) == order_id:
                    self._order_map.pop(token_id, None)

    def _do_cancel_all(self, reason: str) -> None:
        """取消全部挂单。V8：cancel_all() 返回对象（.canceled / .not_canceled）"""
        try:
            result = self._client.cancel_all()
            # V8: 属性访问（不是 dict.get）
            canceled = list(result.canceled) if result.canceled else []
            not_canceled = dict(result.not_canceled) if result.not_canceled else {}
            logger.info("[WssGuard] cancel_all: %d 已取消 | %s", len(canceled), reason)
            if not_canceled:
                logger.error("[WssGuard] cancel_all: %d 失败: %s", len(not_canceled), not_canceled)
        except Exception as e:
            logger.error("[WssGuard] cancel_all 异常: %s | %s", e, reason)
        finally:
            with self._order_lock:
                self._order_map.clear()

    # ── 订阅核对 ─────────────────────────────────────────────────────────────

    def _refresh_subscriptions(self, *, is_initial: bool = False) -> None:
        """通过 list_open_orders 核对当前 BUY 挂单，更新 WS 订阅列表。

        V8：list_open_orders() 返回 sync paginator，字段用属性访问：
            o.token_id, o.id, o.side
        """
        try:
            pages = self._client.list_open_orders()
        except Exception as e:
            logger.error("[WssGuard] 获取挂单失败: %s", e)
            return

        new_map: Dict[str, str] = {}
        try:
            for page in pages:
                for o in page.items:
                    tid = str(o.token_id) if o.token_id else ""
                    oid = str(o.id) if o.id else ""
                    side = str(o.side).upper()
                    if tid and oid and side == "BUY":
                        new_map[tid] = oid
        except Exception as e:
            logger.error("[WssGuard] 遍历 open_orders 失败: %s", e)
            return

        with self._order_lock:
            old_tids = set(self._order_map.keys())
            new_tids = set(new_map.keys())
            to_unsub = old_tids - new_tids
            to_sub = new_tids - old_tids
            self._order_map = new_map

        if is_initial:
            self._market_ws.subscribe_initial(list(new_tids))
            logger.info("[WssGuard] 初始订阅: %d 个市场", len(new_tids))
        else:
            if to_unsub:
                self._market_ws.unsubscribe(list(to_unsub))
            if to_sub:
                self._market_ws.subscribe_more(list(to_sub))
            logger.debug(
                "[WssGuard] 核对完成: 共%d个 +%d -%d",
                len(new_map), len(to_sub), len(to_unsub),
            )

    def _reconcile_loop(self) -> None:
        """每 reconcile_interval 秒核对一次订单列表和 WS 订阅。"""
        while self._running:
            time.sleep(self._reconcile_interval)
            if not self._running:
                break
            if self._paused:
                continue
            self._refresh_subscriptions(is_initial=False)
