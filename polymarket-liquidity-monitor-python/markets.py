import time
import requests
from config import CONFIG
from market_types import CandidateMarket


END_CURSOR = "LTE="


def fetch_and_filter() -> list[CandidateMarket]:
    all_markets: list[dict] = []
    cursor = ""
    page = 0

    while True:
        page += 1
        print(
            f"\rFetching reward markets... page {page} ({len(all_markets)} so far)",
            end="",
            flush=True,
        )

        url = f"{CONFIG.CLOB_API}/sampling-markets"
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

    print(f"\rFetched {len(all_markets)} reward-distributing markets from CLOB")

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
        if total_daily_rewards < CONFIG.MIN_DAILY_REWARDS:
            continue

        end_date_str = m.get("end_date_iso", "")
        if not end_date_str:
            continue
        from datetime import datetime, timezone

        end_date = datetime.fromisoformat(
            end_date_str.replace("Z", "+00:00")
        )
        days_to_expiry = (end_date.timestamp() * 1000 - now) / (
            1000 * 60 * 60 * 24
        )
        if days_to_expiry < CONFIG.MIN_DAYS_TO_EXPIRY:
            continue

        min_size = rewards.get("min_size", 0)
        if min_size < CONFIG.MIN_SIZE_LOWER or min_size > CONFIG.MIN_SIZE_UPPER:
            continue

        max_spread = rewards.get("max_spread", 0) / 100
        tokens = m.get("tokens") or []
        if len(tokens) < 2:
            continue

        if CONFIG.SEARCH_KEYWORD:
            if CONFIG.SEARCH_KEYWORD.lower() not in m["question"].lower():
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

    print(f"{len(candidates)} candidates after filtering")
    return candidates
