"""手动做市配置；每个实例只管理一个钱包。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional


def _enabled(name: str, default: str = "true") -> bool:
    return os.environ.get(name, default).lower() not in ("false", "0", "no", "off")


@dataclass(frozen=True)
class Config:
    instance_name: str = "bot1"
    host: str = field(default_factory=lambda: os.environ.get("CLOB_API_URL", "https://clob.polymarket.com"))
    data_api: str = "https://data-api.polymarket.com"
    ws_user: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    chain_id: int = field(default_factory=lambda: int(os.environ.get("CHAIN_ID", "137")))
    pk: str = field(default_factory=lambda: os.environ.get("SIGNER_PRIVATE_KEY") or os.environ.get("PK", ""))
    wallet: str = field(default_factory=lambda: os.environ.get("POLYMARKET_WALLET_ADDRESS") or os.environ.get("WALLET_ADDRESS", ""))
    proxy: str = field(default_factory=lambda: os.environ.get("PROXY_ADDRESS", ""))
    relayer_api_key: Optional[str] = field(default_factory=lambda: os.environ.get("POLYMARKET_RELAYER_API_KEY") or os.environ.get("RELAYER_API_KEY"))
    relayer_api_key_address: Optional[str] = field(default_factory=lambda: os.environ.get("POLYMARKET_RELAYER_API_KEY_ADDRESS") or os.environ.get("RELAYER_API_KEY_ADDRESS"))

    maker_rank: int = field(default_factory=lambda: int(os.environ.get("MAKER_RANK", "2")))
    maker_cooldown: float = 120.0
    tick_size: Decimal = Decimal("0.01")
    cancel_on_trade: bool = field(default_factory=lambda: _enabled("CANCEL_ON_TRADE", "false"))
    exec_interval: float = 0.025
    max_workers: int = 10
    place_retries: int = 2
    place_retry_delay: float = 1.0

    discover_interval: float = 30.0
    position_interval: float = 120.0
    best_bid_poll_interval: float = 3.0
    ws_rest_reconcile_interval: float = 30.0
    ws_sub_sync_interval: float = 15.0
    cache_prune_interval: float = 300.0
    position_threshold: float = 1.0
    sell_min_bid_gap: Decimal = field(default_factory=lambda: Decimal(os.environ.get("SELL_MIN_BID_GAP", "0.02")))
    sell_position_gap: Decimal = field(default_factory=lambda: Decimal(os.environ.get("SELL_POSITION_GAP", "0.02")))
    max_hold_enabled: bool = field(default_factory=lambda: _enabled("MAX_HOLD_ENABLED"))
    max_hold_hours: float = field(default_factory=lambda: float(os.environ.get("MAX_HOLD_HOURS", "4")))

    ws_market_enabled: bool = field(default_factory=lambda: _enabled("WS_MARKET_ENABLED"))
    ws_market_url: str = field(default_factory=lambda: os.environ.get("WS_MARKET_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"))
    market_ping_interval: float = field(default_factory=lambda: float(os.environ.get("MARKET_PING_INTERVAL", "10")))
    market_ping_timeout: float = field(default_factory=lambda: float(os.environ.get("MARKET_PING_TIMEOUT", "50")))
    ws_reconnect_delay: float = 5.0
    user_ping_interval: float = 10.0
    heartbeat_interval: float = 7.0
    heartbeat_max_errors: int = 3
    proxy_url: Optional[str] = field(default_factory=lambda: os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or os.environ.get("ALL_PROXY"))

    def validate(self) -> None:
        if not self.pk:
            raise EnvironmentError("缺少 SIGNER_PRIVATE_KEY / PK")
        if os.environ.get("SIGNER_PRIVATE_KEY") and os.environ.get("PK") and os.environ["SIGNER_PRIVATE_KEY"].strip() != os.environ["PK"].strip():
            raise EnvironmentError("SIGNER_PRIVATE_KEY 与 PK 不同，可能用错账户")
        if bool(self.relayer_api_key) != bool(self.relayer_api_key_address):
            raise EnvironmentError("Relayer API Key 与 Key Address 必须配对")
        if self.wallet and not self.relayer_api_key:
            raise EnvironmentError("新账户钱包地址需要 Relayer API Key 与 Key Address")
        if self.maker_rank < 1:
            raise EnvironmentError("MAKER_RANK 必须 >= 1")
        if self.max_hold_enabled and self.max_hold_hours <= 0:
            raise EnvironmentError("MAX_HOLD_HOURS 必须 > 0")
