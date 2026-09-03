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
    # maker_rank 挂买盘第几档：默认 2（买二档）。可各 .env.botN 单独设 MAKER_RANK，
    # 不同账号挂不同档位（如 bot1 挂买一、bot2 挂买三）。填非数字会在启动时报错（fail-fast）。
    maker_rank: int = field(
        default_factory=lambda: int(os.environ.get("MAKER_RANK", "2"))
    )
    maker_cooldown: float = 120.0
    tick_size: Decimal = Decimal("0.01")

    # 「有成交就撤单」策略（MAKER_RANK=1 专用）：死水市场偶尔有成交说明市场活了，
    # 撤单重挂刷新流动性。只对 MAKER_RANK=1 生效（RANK=2+ 继续用 bid 变化撤单）。
    cancel_on_trade: bool = field(
        default_factory=lambda: os.environ.get("CANCEL_ON_TRADE", "false").lower() == "true"
    )

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
    # 到期天数上限（inf=不过滤）：剩余天数 > 该值则跳过，与 min 组成 [min, max] 范围。
    # 无 end_date 的长期市场 days_to_expiry=inf，若设有限上限会被排除（inf > 上限）。
    screener_max_days_to_expiry: float = float(os.environ.get("SCREENER_MAX_DAYS_TO_EXPIRY", "inf"))
    screener_min_midpoint: float = float(os.environ.get("SCREENER_MIN_MIDPOINT", "0.15"))
    screener_max_midpoint: float = float(os.environ.get("SCREENER_MAX_MIDPOINT", "0.85"))
    screener_min_size_lower: float = float(os.environ.get("SCREENER_MIN_SIZE_LOWER", "0.0"))
    screener_min_size_upper: float = float(os.environ.get("SCREENER_MIN_SIZE_UPPER", "60"))
    screener_min_existing_size: float = float(os.environ.get("SCREENER_MIN_EXISTING_SIZE", "2000.0"))
    screener_min_top1_bids: float = float(os.environ.get("SCREENER_MIN_TOP1_BIDS", "100.0"))
    screener_min_top2_bids: float = float(os.environ.get("SCREENER_MIN_TOP2_BIDS", "400.0"))
    screener_min_top3_bids: float = float(os.environ.get("SCREENER_MIN_TOP3_BIDS", "1200.0"))
    # 成交量 / 流动性过滤（gamma 补查，双接口方案）：
    # sampling-markets 只给 rewards 不给 volume/liquidity，需用 condition_id 去
    # gamma /markets 补查后按 [min,max] 范围过滤。
    # 上限排除已饱和市场，下限排除从未成交的冷门市场（想「市场至少有过成交再挂单」设 min）。
    # 默认：下限 0=不过滤，上限 inf=不过滤。所有字段都是 market 级合计（YES+NO 两边），单位美元：
    #   volume_total  = volumeNum（累计成交量，不限时间）
    #   volume_24h    = volume24hr（近 24h 成交量）
    #   liquidity     = liquidityNum（当前总流动性）
    screener_min_volume_total: float = float(os.environ.get("SCREENER_MIN_VOLUME_TOTAL", "0"))
    screener_max_volume_total: float = float(os.environ.get("SCREENER_MAX_VOLUME_TOTAL", "inf"))
    screener_max_volume_24h: float = float(os.environ.get("SCREENER_MAX_VOLUME_24H", "inf"))
    screener_max_liquidity: float = float(os.environ.get("SCREENER_MAX_LIQUIDITY", "inf"))

    # ── 缓存与并发 ────────────────────────────────────────────────────────────
    cache_ttl: float = 5.0
    cancel_delay: float = 0.3
    cache_max_size: int = 500
    trade_max_size: int = 50000

    # ── 持仓卖出阈值 ──────────────────────────────────────────────────────────
    position_threshold: float = 1.0
    # 即时卖单（BUY 成交后立即卖）：best_bid < 成交价 - 此值 → 跳过等定时轮询
    sell_min_bid_gap: Decimal = Decimal(os.environ.get("SELL_MIN_BID_GAP", "0.02"))
    # 定时兜底（check_positions 每 120s）：best_ask < 成本价 - 此值 → 取消卖单等待回稳
    # 默认 0.02 与历史一致；死水市场可设更大（如 0.05）放宽崩盘保护，持有更久等回稳
    sell_position_gap: Decimal = Decimal(os.environ.get("SELL_POSITION_GAP", "0.02"))

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
    # WS 心跳超时阈值（秒）：超过该时长未收到 PONG 判定断线，主动重连。
    # 默认 50s（原为 ping_interval×2=20s），为「无交易死水市场」场景放宽——
    # 这类市场 WS 上几乎无消息，偶发 PONG 延迟会误触 20s 阈值导致频繁断线撤单。
    market_ping_timeout: float = float(os.environ.get("MARKET_PING_TIMEOUT", "50"))
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
