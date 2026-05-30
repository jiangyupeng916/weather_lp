#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_config.py — 配置模块单元测试"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from decimal import Decimal
from config import Config


class TestConfigDefaults:
    def test_default_host(self):
        cfg = Config()
        assert cfg.host == "https://clob.polymarket.com"

    def test_default_maker(self):
        cfg = Config()
        assert cfg.maker_size == Decimal("50")
        assert cfg.maker_rank == 3
        assert cfg.maker_cooldown == 120.0  # V7 默认 120s（V6 是 360s）

    def test_default_heartbeat(self):
        cfg = Config()
        assert cfg.heartbeat_interval == 7.0
        assert cfg.heartbeat_max_errors == 3

    def test_default_sell_fallback(self):
        cfg = Config()
        assert len(cfg.sell_fallback_order_types) == 2
        assert "FOK" in cfg.sell_fallback_order_types
        assert "FAK" in cfg.sell_fallback_order_types

    def test_tick_size_is_decimal(self):
        cfg = Config()
        assert isinstance(cfg.tick_size, Decimal)
        assert cfg.tick_size == Decimal("0.01")

    def test_cache_defaults(self):
        cfg = Config()
        assert cfg.cache_max_size == 500
        assert cfg.trade_max_size == 50000

    def test_balance_retries(self):
        cfg = Config()
        assert cfg.balance_retries == 3


class TestConfigSignatureType:
    def test_eoa_without_proxy(self):
        cfg = Config()
        cfg = Config(
            pk="0x" + "a" * 64,
            api_key="key",
            api_secret="secret",
            passphrase="pass",
            proxy="",
        )
        assert cfg.signature_type == 0

    def test_proxy_with_address(self):
        cfg = Config(
            pk="0x" + "a" * 64,
            api_key="key",
            api_secret="secret",
            passphrase="pass",
            proxy="0x304bf40e9e50c265f6dd86417707d5b41d8ea619",
        )
        assert cfg.signature_type == 1


class TestConfigValidation:
    def test_missing_required(self):
        cfg = Config(pk="", api_key="", api_secret="", passphrase="")
        try:
            cfg.validate()
            assert False, "应当抛出 EnvironmentError"
        except EnvironmentError as e:
            assert "pk" in str(e)
            assert "api_key" in str(e)

    def test_valid_config(self):
        cfg = Config(
            pk="0x" + "a" * 64,
            api_key="test-key",
            api_secret="test-secret",
            passphrase="test-pass",
        )
        cfg.validate()  # 不应抛出异常
