#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""配置模块 — 环境变量加载、数据类定义、校验"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    # ── 服务端点 ──────────────────────────────────────────────────────────────
    host: str = os.environ.get("CLOB_API_URL", "https://clob.polymarket.com")
    ws_user: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    data_api: str = "https://data-api.polymarket.com"

    # ── 账户与认证 ────────────────────────────────────────────────────────────
    chain_id: int = int(os.environ.get("CHAIN_ID", "137"))
    pk: str = field(default_factory=lambda: os.environ.get("PK", ""))
    proxy: str = field(default_factory=lambda: os.environ.get("PROXY_ADDRESS", ""))
    api_key: str = field(default_factory=lambda: os.environ.get("CLOB_API_KEY", ""))
    api_secret: str = field(default_factory=lambda: os.environ.get("CLOB_SECRET", ""))
    passphrase: str = field(default_factory=lambda: os.environ.get("CLOB_PASS_PHRASE", ""))

    # ── 签名类型 ──────────────────────────────────────────────────────────────
    signature_type: int = -1

    def __post_init__(self):
        if self.signature_type < 0:
            st = 1 if self.proxy else 0
            object.__setattr__(self, "signature_type", st)

    # ── Maker 挂单策略 ────────────────────────────────────────────────────────
    maker_size: Decimal = Decimal("50")
    maker_rank: int = 2
    maker_cooldown: float = 120.0
    tick_size: Decimal = Decimal("0.01")

    # ── 执行层限流与重试 ──────────────────────────────────────────────────────
    exec_interval: float = 0.2
    place_retries: int = 2
    place_retry_delay: float = 1.0
    max_workers: int = 10

    # ── 定时任务间隔 ──────────────────────────────────────────────────────────
    discover_interval: float = 30.0
    best_bid_poll_interval: float = 3.0
    audit_interval: float = 120.0
    position_interval: float = 120.0
    cache_prune_interval: float = 300.0
    stale_timeout: float = 60.0
    cooldown_delay: float = 2.0

    # ── 市场筛选文件 ──────────────────────────────────────────────────────────
    market_file: str = field(default_factory=lambda: os.environ.get("MARKET_FILE", ""))

    # ── 缓存与并发 ────────────────────────────────────────────────────────────
    cache_ttl: float = 5.0
    cancel_delay: float = 0.3
    cache_max_size: int = 500
    trade_max_size: int = 50000

    # ── 持仓卖出阈值 ──────────────────────────────────────────────────────────
    position_threshold: float = 1.0

    # ── WebSocket 连接 ────────────────────────────────────────────────────────
    ws_reconnect_delay: float = 5.0
    user_ping_interval: float = 50.0

    # ── 心跳 (Heartbeat) ─────────────────────────────────────────────────────
    heartbeat_interval: float = 7.0
    heartbeat_max_errors: int = 3

    # ── 超时 ────────────────────────────────────────────────────────────────
    cancel_timeout: float = 10.0
    place_timeout: float = 15.0

    # ── 代理地址（WS） ────────────────────────────────────────────────────────
    proxy_url: Optional[str] = field(
        default_factory=lambda: os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or os.environ.get("ALL_PROXY")
    )

    def validate(self) -> None:
        missing = [k for k in ("pk", "api_key", "api_secret", "passphrase") if not getattr(self, k)]
        if missing:
            raise EnvironmentError(f"缺少环境变量: {', '.join(missing)}")
