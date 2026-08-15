import time
from datetime import datetime, timezone

import requests

from screener.types import CandidateMarket

END_CURSOR = "LTE="


def fetch_and_filter(cfg) -> list[CandidateMarket]:
    """拉取 CLOB /sampling-markets 并过滤，返回候选市场列表。cfg 是 guardian Config 对象。"""
    clob_api = cfg.host
    keyword = cfg.screener_keyword.lower() if cfg.screener_keyword else ""

    all_markets: list[dict] = []
    cursor = ""
    page = 0

    while True:
        page += 1
        url = f"{clob_api}/sampling-markets"
        params = {}
        if cursor:
            params["next_cursor"] = cursor

        for attempt in range(5):
            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                break
            except Exception:
                if attempt < 4:
                    time.sleep(2 * (attempt + 1))
                else:
                    raise
        body = resp.json()

        all_markets.extend(body.get("data", []))

        next_cursor = body.get("next_cursor", "")
        if not next_cursor or next_cursor == END_CURSOR:
            break
        cursor = next_cursor

    now = time.time() * 1000
    candidates: list[CandidateMarket] = []

    for m in all_markets:
        if not all([
            m.get("active"),
            not m.get("closed"),
            not m.get("archived"),
            m.get("accepting_orders"),
            m.get("enable_order_book"),
        ]):
            continue

        rewards = m.get("rewards") or {}
        rates = rewards.get("rates") or []
        if not rates:
            continue

        total_daily_rewards = sum(r.get("rewards_daily_rate", 0) for r in rates)
        if total_daily_rewards < cfg.screener_min_daily_rewards:
            continue

        end_date_str = m.get("end_date_iso", "")
        if not end_date_str:
            continue

        end_date = datetime.fromisoformat(
            end_date_str.replace("Z", "+00:00")
        )
        days_to_expiry = (end_date.timestamp() * 1000 - now) / (
            1000 * 60 * 60 * 24
        )
        if days_to_expiry < cfg.screener_min_days_to_expiry:
            continue

        min_size = rewards.get("min_size", 0)
        if min_size < cfg.screener_min_size_lower or min_size > cfg.screener_min_size_upper:
            continue

        max_spread = rewards.get("max_spread", 0) / 100
        tokens = m.get("tokens") or []
        if len(tokens) < 2:
            continue

        if keyword and keyword not in m["question"].lower():
            continue

        # 标签筛选（OR 语义，大小写不敏感）：命中任一配置标签即保留
        if cfg.screener_tags:
            market_tags = {t.lower() for t in (m.get("tags") or [])}
            if not any(t in market_tags for t in cfg.screener_tags):
                continue

        candidates.append(
            CandidateMarket(
                condition_id=m["condition_id"],
                question=m["question"],
                end_date=end_date,
                days_to_expiry=days_to_expiry,
                yes_token_id=tokens[0]["token_id"],
                no_token_id=tokens[1]["token_id"],
                total_daily_rewards=total_daily_rewards,
                min_size=min_size,
                max_spread=max_spread,
            )
        )

    return candidates
