#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gamma 补查的「403 快速失败 + 批次级降级」单元测试 —— 纯逻辑，无网络。"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from screener import gamma  # noqa: E402


class _Cfg:
    """fetch_volume_liquidity 只用到 gamma_api 一个字段。"""

    gamma_api = "https://gamma.example"


def _resp(status, payload=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload if payload is not None else []
    if status >= 400:
        r.raise_for_status.side_effect = requests.exceptions.HTTPError(str(status))
    else:
        r.raise_for_status.side_effect = None
    return r


def _ids(n):
    return [f"0x{i:064x}" for i in range(n)]


# ── 403 快速失败 ──────────────────────────────────────────────────────────

def test_403_fails_fast_without_retry():
    """403 是确定性拒绝：只发 1 次请求、不 sleep（原实现会重试 5 次白等 30s）。"""
    with patch.object(gamma.requests, "get", return_value=_resp(403)) as m, \
            patch.object(gamma.time, "sleep") as slp:
        with pytest.raises(gamma._GammaForbidden):
            gamma._fetch_gamma_batch(["0xabc"], _Cfg.gamma_api)
    assert m.call_count == 1, "403 不应重试"
    slp.assert_not_called()


def test_429_still_retried():
    """429 是限流（等一下可能就好），必须保持重试 —— 回归保护。"""
    with patch.object(gamma.requests, "get", return_value=_resp(429)) as m, \
            patch.object(gamma.time, "sleep"):
        with pytest.raises(requests.exceptions.HTTPError):
            gamma._fetch_gamma_batch(["0xabc"], _Cfg.gamma_api)
    assert m.call_count == 6, "429 应重试满 retries+1 次"


def test_200_returns_parsed_fields():
    """正常路径未被破坏。"""
    payload = [{"conditionId": "0xabc", "volumeNum": 12.5, "volume24hr": 3.0,
                "liquidityNum": 99.0, "createdAt": "2026-09-10T17:38:27Z"}]
    with patch.object(gamma.requests, "get", return_value=_resp(200, payload)):
        out = gamma._fetch_gamma_batch(["0xabc"], _Cfg.gamma_api)
    assert out["0xabc"]["volume"] == 12.5
    assert out["0xabc"]["liquidity"] == 99.0
    assert out["0xabc"]["age_hours"] > 0


# ── 批次级降级 ────────────────────────────────────────────────────────────

def test_partial_failure_degrades():
    """5 批里 2 批失败 → 返回其余 3 批共 300 条，不抛异常。"""
    ids = _ids(500)
    fail_first = {ids[100], ids[200]}          # 第 2、3 批失败

    def fake(batch, gamma_api, retries=5):
        if batch[0] in fail_first:
            raise RuntimeError("403 Forbidden")
        return {cid: {"volume": 1.0} for cid in batch}

    with patch.object(gamma, "_fetch_gamma_batch", side_effect=fake):
        out = gamma.fetch_volume_liquidity(ids, _Cfg())
    assert len(out) == 300, "应保留 3 批成功结果"


def test_all_fail_raises_guard():
    """全部批次失败 → 触发护栏抛异常，而不是返回空 dict。

    返回空 dict 会让所有候选项按 volume=0/age=inf 被过滤掉，
    targets 变空 → 全量撤单，比崩溃更糟。
    """
    ids = _ids(500)                            # 5 批
    with patch.object(gamma, "_fetch_gamma_batch", side_effect=RuntimeError("403")):
        with pytest.raises(RuntimeError, match="失败面过大"):
            gamma.fetch_volume_liquidity(ids, _Cfg())


def test_two_failures_tolerated():
    """少量失败（<= max(2, 20%)）应降级放行，不触发护栏。"""
    ids = _ids(500)                            # 5 批，20% = 1 → 阈值取 max(2,1)=2
    fail_first = {ids[0], ids[100]}

    def fake(batch, gamma_api, retries=5):
        if batch[0] in fail_first:
            raise RuntimeError("403 Forbidden")
        return {cid: {"volume": 1.0} for cid in batch}

    with patch.object(gamma, "_fetch_gamma_batch", side_effect=fake):
        out = gamma.fetch_volume_liquidity(ids, _Cfg())
    assert len(out) == 300


def test_abort_threshold_is_20pct_on_bot6_scale():
    """bot6 量级（148 批）：29 批失败仍降级，30 批则放弃本轮。"""
    ids = _ids(14800)                          # 148 批
    assert len(ids) // gamma.BATCH_SIZE == 148

    for fail_n, should_abort in ((29, False), (30, True)):
        fail_first = set(ids[: fail_n * gamma.BATCH_SIZE: gamma.BATCH_SIZE])

        def fake(batch, gamma_api, retries=5, _ff=fail_first):
            if batch[0] in _ff:
                raise RuntimeError("403 Forbidden")
            return {cid: {"volume": 1.0} for cid in batch}

        with patch.object(gamma, "_fetch_gamma_batch", side_effect=fake):
            if should_abort:
                with pytest.raises(RuntimeError, match="失败面过大"):
                    gamma.fetch_volume_liquidity(ids, _Cfg())
            else:
                out = gamma.fetch_volume_liquidity(ids, _Cfg())
                assert len(out) == (148 - fail_n) * gamma.BATCH_SIZE
