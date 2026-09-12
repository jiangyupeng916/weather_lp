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

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

logger = logging.getLogger("guardian.screener.gamma")

# gamma /markets 限流 300 req/10s（官方 rate-limits），且**按出口 IP 计算** ——
# 同机多 bot 共用 IP，配额是叠加的，故必须压低单 bot 的请求量。
# 批大小取服务端硬上限 100（实测 101 个即报
# {"error":"expected array length <= 100"}；官方文档未记载此限制）。
# 100 相比 50 把批次数直接减半：bot6 295 批 → 148 批。
BATCH_SIZE = 100
# 并发线程数。服务器实测单批延迟约 0.15~0.18s，10 并发时单轮爆发仅约 4.5s ——
# 爆发短于 10s 限流窗口，意味着整轮请求会挤进同一窗口（窗口内请求数 ≈ 批次数）。
# 降到 5 后爆发约 5.4s，单 bot 窗口请求数由 295 降到 148，避免多 bot 叠加冲破 300。
MAX_WORKERS = 5
# 批次级降级护栏：失败批次占比超过该值（且绝对值 > 2 批）→ 判定为系统性故障，
# 主动放弃本轮筛选。宁可这一轮不更新 targets，也不能让 targets 变空 ——
# 漏查市场按 volume=0 / age=inf 语义会被全部过滤掉，进而触发全量撤单。
FAILURE_ABORT_RATIO = 0.2


class _GammaForbidden(Exception):
    """gamma 返回 403：确定性拒绝（IP 被限流/封锁），重试不可能成功。"""


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _created_age_hours(created_at) -> float:
    """gamma 的 createdAt（ISO 字符串）→ 市场年龄（小时）。

    缺失或解析失败返回 inf（视为「创建很久」）：
    设 max_age 有限时这些市场会被上限排除，min 方向放行。
    """
    if not created_at:
        return float("inf")
    try:
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return float("inf")
    age = (datetime.now(timezone.utc) - created).total_seconds() / 3600.0
    return age if age > 0 else 0.0


def _fetch_gamma_batch(condition_ids, gamma_api: str, retries: int = 5) -> dict[str, dict]:
    url = f"{gamma_api}/markets"
    # condition_ids 必须传 list（重复参数），逗号拼接会返回空
    params = {"condition_ids": list(condition_ids), "limit": BATCH_SIZE}
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            # 403 是确定性拒绝（IP 被限流/封锁）：走下面的通用 except 会白等
            # 2+4+6+8+10=30s 且必然再次失败，故单独立即抛出，交给上层降级处理。
            if resp.status_code == 403:
                raise _GammaForbidden(f"403 Forbidden ({len(condition_ids)} ids)")
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
                    "age_hours": _created_age_hours(item.get("createdAt")),
                }
            return result
        except _GammaForbidden:
            raise  # 确定性拒绝：不重试，立即上抛（由批次级降级兜住）
        except Exception:
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
            else:
                raise


def fetch_volume_liquidity(condition_ids: list[str], cfg) -> dict[str, dict]:
    """批量补查，返回 {condition_id: {volume, volume24hr, liquidity, age_hours}}。

    批次级降级：个别批次失败只丢那一批市场，不再一票否决整轮 ——
    原实现 `out.update(fut.result())` 会让 148 批里 1 批 403 就废掉整轮，
    进而使 _apply_market_targets 不执行、不达标市场永不撤单。
    护栏：失败面过大时主动抛异常放弃本轮（见 FAILURE_ABORT_RATIO 注释）。
    """
    ids = list(dict.fromkeys(condition_ids))  # 去重保序
    if not ids:
        return {}
    batches = [ids[i:i + BATCH_SIZE] for i in range(0, len(ids), BATCH_SIZE)]
    out: dict[str, dict] = {}
    failed = 0
    first_err: Exception | None = None
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_gamma_batch, b, cfg.gamma_api): b for b in batches
        }
        for fut in as_completed(futures):
            try:
                out.update(fut.result())
            except Exception as e:  # noqa: BLE001 — 单批失败降级，不中断整轮
                failed += 1
                if first_err is None:
                    first_err = e

    if failed:
        logger.warning(
            "[GAMMA] %d/%d 批失败，本轮缺约 %d 个市场的数据 | 首个错误: %s",
            failed, len(batches), failed * BATCH_SIZE, first_err,
        )
        # 护栏：失败面过大 → 系统性故障，放弃本轮（保持原 targets，避免全量撤单）
        if failed > max(2, int(len(batches) * FAILURE_ABORT_RATIO)):
            raise RuntimeError(
                f"gamma 补查失败面过大（{failed}/{len(batches)} 批），放弃本轮筛选"
            )
    return out


def enrich_and_filter(candidates, cfg) -> list:
    """补查成交量/流动性/创建时间，并按 [min,max] 范围过滤（放在 orderbook 查询之前）。

    - 下限（min_volume_total，默认 0）和所有上限（默认 inf）都不过滤时，
      不发起任何 gamma 请求（等同未加此功能，向后兼容）。
    - 过滤用 volume（累计成交量 volumeNum）、volume24hr（近 24h 成交量）、
      liquidity（当前总流动性）、age_hours（市场年龄，createdAt 至今小时数）。
    - volume/liquidity 漏查按 0 处理：0 <= 上限恒成立 → 上限方向放行；
      设 min_volume_total > 0 时漏查市场（0）会被下限排除。
    - age_hours 漏查按 inf 处理（视为创建很久）：设 max_age 有限时被上限排除，
      min 方向放行。
    """
    min_vol_total = cfg.screener_min_volume_total
    max_vol_total = cfg.screener_max_volume_total
    max_vol = cfg.screener_max_volume_24h
    max_liq = cfg.screener_max_liquidity
    min_age = cfg.screener_min_age_hours
    max_age = cfg.screener_max_age_hours
    # 短路：下限为 0 且所有上限都是 inf → 无任何过滤，跳过 gamma 查询
    if not candidates or (
        min_vol_total <= 0
        and max_vol_total == float("inf")
        and max_vol == float("inf")
        and max_liq == float("inf")
        and min_age <= 0
        and max_age == float("inf")
    ):
        return candidates

    gamma_data = fetch_volume_liquidity([m.condition_id for m in candidates], cfg)
    for m in candidates:
        info = gamma_data.get(m.condition_id, {})
        m.volume = info.get("volume", 0.0)
        m.volume24hr = info.get("volume24hr", 0.0)
        m.liquidity = info.get("liquidity", 0.0)
        m.age_hours = info.get("age_hours", float("inf"))

    return [
        m for m in candidates
        if m.volume >= min_vol_total
        and m.volume <= max_vol_total
        and m.volume24hr <= max_vol
        and m.liquidity <= max_liq
        and m.age_hours >= min_age
        and m.age_hours <= max_age
    ]
