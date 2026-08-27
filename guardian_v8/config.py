#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""配置模块 — 环境变量加载、数据类定义、校验（V8：移除 signature_type，凭据可选）"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional


def load_config(env_file: str = ".env") -> None:
    """加载指定 .env 文件（在导入 Config 之前调用）。"""
    from dotenv import load_dotenv as _load
    if os.path.exists(env_file):
        _load(env_file)
    elif env_file != ".env" and os.path.exists(".env"):
        _load(".env")  # 回退到默认 .env


@dataclass(frozen=True)
class Config:
    # ── 实例标识 ──────────────────────────────────────────────────────────────
    instance_name: str = "default"

    # ── 服务端点 ──────────────────────────────────────────────────────────────
    host: str = os.environ.get("CLOB_API_URL", "https://clob.polymarket.com")
    ws_user: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    data_api: str = "https://data-api.polymarket.com"
    gamma_api: str = os.environ.get("GAMMA_API_URL", "https://gamma-api.polymarket.com")

    # ── 账户 ──────────────────────────────────────────────────────────────────
    # V8.2：支持两种账户格式 ——
    #  新账户（Deposit Wallet，2026-05-04 后标准账户）: PK + WALLET_ADDRESS + RELAYER_API_KEY + RELAYER_API_KEY_ADDRESS
    #  旧账户（POLY_PROXY，Magic/Google 登录旧账户）:      PK + PROXY_ADDRESS（无 relayer）
    # wallet 优先级：WALLET_ADDRESS > PROXY_ADDRESS > None（SDK 解析到 signer 的 Deposit Wallet）
    chain_id: int = int(os.environ.get("CHAIN_ID", "137"))
    # 私钥：官方命名 SIGNER_PRIVATE_KEY 优先，兼容旧短名 PK
    pk: str = field(default_factory=lambda: (
        os.environ.get("SIGNER_PRIVATE_KEY") or os.environ.get("PK", "")
    ))
    # 账户钱包地址（新账户必须）：POLYMARKET_WALLET_ADDRESS 优先，兼容 WALLET_ADDRESS
    wallet: str = field(default_factory=lambda: (
        os.environ.get("POLYMARKET_WALLET_ADDRESS") or os.environ.get("WALLET_ADDRESS", "")
    ))
    # 旧账户（POLY_PROXY）代理地址：仅旧格式使用
    proxy: str = field(default_factory=lambda: os.environ.get("PROXY_ADDRESS", ""))

    # ── Relayer API Key（新账户 gasless 钱包操作授权，两者都设才生效） ──────
    # 官方命名 POLYMARKET_RELAYER_API_KEY* 优先，兼容短名 RELAYER_API_KEY*
    relayer_api_key: Optional[str] = field(default_factory=lambda: (
        os.environ.get("POLYMARKET_RELAYER_API_KEY") or os.environ.get("RELAYER_API_KEY") or None
    ))
    relayer_api_key_address: Optional[str] = field(default_factory=lambda: (
        os.environ.get("POLYMARKET_RELAYER_API_KEY_ADDRESS")
        or os.environ.get("RELAYER_API_KEY_ADDRESS") or None
    ))

    # ── API 凭据（历史遗留，无代码使用；SDK 自动派生 L2 凭据，用 client.credentials） ──
    # 保留仅为向后兼容（旧 .env 仍可能带这些变量），实际心跳/下单都走 client.credentials。
    api_key: Optional[str] = field(
        default_factory=lambda: os.environ.get("CLOB_API_KEY") or None
    )
    api_secret: Optional[str] = field(
        default_factory=lambda: os.environ.get("CLOB_SECRET") or None
    )
    passphrase: Optional[str] = field(
        default_factory=lambda: os.environ.get("CLOB_PASS_PHRASE") or None
    )

    # ── Maker 挂单策略 ────────────────────────────────────────────────────────
    # maker_size 可在各 .env.botN 单独设置 MAKER_SIZE，不同账号挂不同份额；不设默认 50。
    maker_size: Decimal = field(
        default_factory=lambda: Decimal(os.environ.get("MAKER_SIZE", "50"))
    )
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

    # ── 市场筛选文件（已废弃，由内置筛选器替代） ─────────────────────────
    market_file: str = field(default_factory=lambda: os.environ.get("MARKET_FILE", ""))

    # ── 内置筛选器 ────────────────────────────────────────────────────────
    screener_interval: float = float(os.environ.get("SCREENER_INTERVAL", "30"))
    screener_keyword: str = os.environ.get("SCREENER_KEYWORD", "temp")
    # 标签筛选：逗号分隔的 label 列表，命中任一（OR）即保留；空=不过滤。
    # 大小写不敏感（sampling-markets 的 tags 与 gamma /tags 的 label 大小写不一致）。
    # 防御：过滤掉以 # 开头的项（python-dotenv 会把「SCREENER_TAGS=  # 注释」的行内注释解析进值）。
    screener_tags: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            t.strip().lower()
            for t in os.environ.get("SCREENER_TAGS", "").split(",")
            if t.strip() and not t.strip().startswith("#")
        )
    )
    screener_min_daily_rewards: float = float(os.environ.get("SCREENER_MIN_DAILY_REWARDS", "10.0"))
    screener_min_days_to_expiry: float = float(os.environ.get("SCREENER_MIN_DAYS_TO_EXPIRY", "0"))
    screener_min_midpoint: float = float(os.environ.get("SCREENER_MIN_MIDPOINT", "0.15"))
    screener_max_midpoint: float = float(os.environ.get("SCREENER_MAX_MIDPOINT", "0.85"))
    screener_min_size_lower: float = float(os.environ.get("SCREENER_MIN_SIZE_LOWER", "0.0"))
    screener_min_size_upper: float = float(os.environ.get("SCREENER_MIN_SIZE_UPPER", "60"))
    screener_min_existing_size: float = float(os.environ.get("SCREENER_MIN_EXISTING_SIZE", "2000.0"))
    screener_min_top1_bids: float = float(os.environ.get("SCREENER_MIN_TOP1_BIDS", "100.0"))
    screener_min_top2_bids: float = float(os.environ.get("SCREENER_MIN_TOP2_BIDS", "400.0"))
    screener_min_top3_bids: float = float(os.environ.get("SCREENER_MIN_TOP3_BIDS", "1200.0"))
    # 成交量 / 流动性上限（gamma 补查，双接口方案）：
    # sampling-markets 只给 rewards 不给 volume/liquidity，需用 condition_id 去
    # gamma /markets 补查后按上限过滤，排除成交量/流动性过高（已饱和、LP 收益被稀释）
    # 的市场。默认 inf = 不过滤（等同未加此功能）；≤0 会滤掉所有市场，慎设。
    screener_max_volume_24h: float = float(os.environ.get("SCREENER_MAX_VOLUME_24H", "inf"))
    screener_max_liquidity: float = float(os.environ.get("SCREENER_MAX_LIQUIDITY", "inf"))

    # ── 缓存与并发 ────────────────────────────────────────────────────────────
    cache_ttl: float = 5.0
    cancel_delay: float = 0.3
    cache_max_size: int = 500
    trade_max_size: int = 50000

    # ── 持仓卖出阈值 ──────────────────────────────────────────────────────────
    position_threshold: float = 1.0
    sell_min_bid_gap: Decimal = Decimal("0.02")  # best_bid 低于成本价-此值则跳过卖出

    # ── 持仓超时强平（V8.4）─────────────────────────────────────────────────
    # 持仓自 bot 首次看到起超过 max_hold_hours 仍未卖出 → 无条件 FOK 市价全卖，
    # 忽略 sell_min_bid_gap 崩盘保护（止损逃生优先）。kill switch：置 false 关闭。
    max_hold_enabled: bool = field(
        default_factory=lambda: os.environ.get("MAX_HOLD_ENABLED", "true").lower()
        not in ("false", "0", "no", "off")
    )
    max_hold_hours: float = field(
        default_factory=lambda: float(os.environ.get("MAX_HOLD_HOURS", "4"))
    )

    # ── WebSocket 连接 ────────────────────────────────────────────────────────
    ws_reconnect_delay: float = 5.0
    user_ping_interval: float = 50.0

    # ── 市场频道 WebSocket（Round 2：bid 毫秒级推送 + REST 30s 对账 + 断线撤单） ──
    # kill switch：置 false 立即回退到纯 REST 3s 轮询（今日行为），无需改代码。
    ws_market_enabled: bool = field(
        default_factory=lambda: os.environ.get("WS_MARKET_ENABLED", "true").lower()
        not in ("false", "0", "no", "off")
    )
    ws_market_url: str = os.environ.get(
        "WS_MARKET_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    )
    market_ping_interval: float = float(os.environ.get("MARKET_PING_INTERVAL", "10"))
    # 主线程每隔多久对齐一次 _markets ↔ WS 订阅列表
    ws_sub_sync_interval: float = 15.0
    # WS 启用时 REST 降频到此间隔只做兜底对账（禁用时用 best_bid_poll_interval=3s）
    ws_rest_reconcile_interval: float = 30.0

    # ── 心跳 (Heartbeat) ─────────────────────────────────────────────────────
    heartbeat_interval: float = 7.0
    heartbeat_max_errors: int = 3

    # ── 超时 ────────────────────────────────────────────────────────────────
    cancel_timeout: float = 10.0
    place_timeout: float = 15.0

    # ── 代理地址（WS） ────────────────────────────────────────────────────────
    proxy_url: Optional[str] = field(
        default_factory=lambda: (
            os.environ.get("HTTPS_PROXY")
            or os.environ.get("HTTP_PROXY")
            or os.environ.get("ALL_PROXY")
        )
    )

    def validate(self) -> None:
        """V8.2：只要求 pk；wallet/relayer/proxy 按账户格式可选。"""
        if not self.pk:
            raise EnvironmentError("缺少环境变量: SIGNER_PRIVATE_KEY / PK（私钥）")
        # 防串号：新旧命名同时存在且内容不同时，不静默选一个，直接报错。
        _sig = os.environ.get("SIGNER_PRIVATE_KEY")
        _old_pk = os.environ.get("PK")
        if _sig and _old_pk and _sig.strip() != _old_pk.strip():
            raise EnvironmentError(
                "检测到 SIGNER_PRIVATE_KEY 与 PK 同时设置且内容不同（疑似串号）："
                "请只保留一套命名"
            )
        # 新账户：显式给了账户钱包地址就必须 relayer 四件套齐全，
        # 否则 bot 能启动但首次 gasless 下单才失败（DEPLOY.md 已声明"四字段齐全"）。
        if self.wallet and not (self.relayer_api_key and self.relayer_api_key_address):
            raise EnvironmentError(
                "设置了账户钱包地址（POLYMARKET_WALLET_ADDRESS）但缺少 Relayer API Key 配对："
                "新账户需 RELAYER_API_KEY + RELAYER_API_KEY_ADDRESS 两个都要有"
            )
        if self.relayer_api_key and not self.relayer_api_key_address:
            raise EnvironmentError(
                "设置了 RELAYER_API_KEY（POLYMARKET_RELAYER_API_KEY）但缺少 "
                "RELAYER_API_KEY_ADDRESS（POLYMARKET_RELAYER_API_KEY_ADDRESS）：新账户需两者配对"
            )
