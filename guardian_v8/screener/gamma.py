#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gamma API 补查成交量 / 流动性（双接口方案）。

CLOB /sampling-markets 只给 rewards、不给 volume/liquidity；gamma /markets 有
volumeNum/volume24hr/liquidityNum 但市场列表不全。故以 sampling-markets 的候选
为准，用 condition_id 批量去 gamma /markets 补查，漏掉的市场按 0 处理。

实测坑（务必注意）：
1. condition_ids 必须传 list（重复参数），requests 会编码成
   condition_ids=a&condition_ids=b；逗号拼接会返回空 []。
2. 默认 limit=20，必须显式传 limit=BATCH_SIZE，否则整批被静默截断。
3. 缺失的 condition_id 被 gamma 静默跳过（2 valid + 1 fake → 只返回 2 条），
   正好实现「漏市场 = 0」语义，无需额外处理。
4. 响应可能是裸 JSON 数组，也可能是 {items: [...]} / {markets: [...]} 对象包装
   （端点版本差异），两种都兼容。
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# gamma /markets 限流 300 req/10s（官方 rate-limits）。批大小 50 是折中：
# condition_id 长 66 字符，50 个拼 GET URL ≈ 4KB，避开常见 URL 长度截断上限。
BATCH_SIZE = 50
# 并发线程数。候选多时（如 4755 候选 = 96 批）串行很慢；96 批 < 300 限流，10 并发安全。
MAX_WORKERS = 10


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _fetch_gamma_batch(condition_ids, gamma_api: str, retries: int = 5) -> dict[str, dict]:
    url = f"{gamma_api}/markets"
    # condition_ids 必须传 list（重复参数），逗号拼接会返回空
    params = {"condition_ids": list(condition_ids), "limit": BATCH_SIZE}
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429 and attempt < retries:
                time.sleep(2 * (attempt + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            # 兼容裸数组 / {items:[...]} / {markets:[...]} 三种响应形状
            if isinstance(data, dict):
                data = data.get("items") or data.get("markets") or []
            if not isinstance(data, list):
                return {}
            result: dict[str, dict] = {}
            for item in data:
                cid = item.get("conditionId", "")
                if not cid:
                    continue
                result[cid] = {
                    "volume": _safe_float(item.get("volumeNum")),
                    "volume24hr": _safe_float(item.get("volume24hr")),
                    "liquidity": _safe_float(item.get("liquidityNum")),
                }
            return result
        except Exception:
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
            else:
                raise


def fetch_volume_liquidity(condition_ids: list[str], cfg) -> dict[str, dict]:
    """批量补查，返回 {condition_id: {volume, volume24hr, liquidity}}。"""
    ids = list(dict.fromkeys(condition_ids))  # 去重保序
    if not ids:
        return {}
    batches = [ids[i:i + BATCH_SIZE] for i in range(0, len(ids), BATCH_SIZE)]
    out: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_gamma_batch, b, cfg.gamma_api): b for b in batches
        }
        for fut in as_completed(futures):
            out.update(fut.result())
    return out


def enrich_and_filter(candidates, cfg) -> list:
    """补查成交量/流动性，并按上限过滤（放在 orderbook 查询之前）。

    - 三个上限都是 inf 时不发起任何 gamma 请求（默认不过滤，等同未加此功能）。
    - 过滤用 volume（累计成交量 volumeNum）、volume24hr（近 24h 成交量）、
      liquidity（当前总流动性）。
    - 漏掉的市场按 0 处理，0 <= 上限恒成立 → 放行。
    """
    max_vol_total = cfg.screener_max_volume_total
    max_vol = cfg.screener_max_volume_24h
    max_liq = cfg.screener_max_liquidity
    if not candidates or (
        max_vol_total == float("inf")
        and max_vol == float("inf")
        and max_liq == float("inf")
    ):
        return candidates

    gamma_data = fetch_volume_liquidity([m.condition_id for m in candidates], cfg)
    for m in candidates:
        info = gamma_data.get(m.condition_id, {})
        m.volume = info.get("volume", 0.0)
        m.volume24hr = info.get("volume24hr", 0.0)
        m.liquidity = info.get("liquidity", 0.0)

    return [
        m for m in candidates
        if m.volume <= max_vol_total
        and m.volume24hr <= max_vol
        and m.liquidity <= max_liq
    ]
