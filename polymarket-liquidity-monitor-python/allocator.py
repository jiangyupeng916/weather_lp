from dataclasses import fields

from config import CONFIG
from market_types import ScoredMarket, AllocatedMarket


def _to_allocated(market: ScoredMarket, allocated_size: float) -> AllocatedMarket:
    kwargs = {f.name: getattr(market, f.name) for f in fields(market)}
    kwargs["allocated_size"] = allocated_size
    kwargs["estimated_daily_rewards"] = 0.0
    return AllocatedMarket(**kwargs)


def allocate(sorted_markets: list[ScoredMarket]) -> list[AllocatedMarket]:
    remaining = CONFIG.BUDGET_USD
    selected: dict[str, AllocatedMarket] = {}
    unselected = list(sorted_markets)

    # Pass 1 — select top markets by reward_per_dollar
    for market in sorted_markets:
        if len(selected) >= CONFIG.MAX_MARKETS:
            break
        if remaining < market.min_size:
            continue

        remaining -= market.min_size
        selected[market.condition_id] = _to_allocated(market, market.min_size)
        unselected = [m for m in unselected if m.condition_id != market.condition_id]

    # Pass 2 — redistribute remaining budget
    while remaining > 0:
        new_candidates: list[dict] = []
        if len(selected) < CONFIG.MAX_MARKETS:
            for m in unselected:
                if m.min_size <= remaining:
                    new_candidates.append({
                        "market": m,
                        "score": m.reward_per_dollar,
                        "is_new": True,
                        "id": m.condition_id,
                    })

        existing_candidates: list[dict] = []
        for m in selected.values():
            score = 0.0
            ets = m.existing_total_size
            if ets > 0:
                score = (m.total_daily_rewards * ets) / ((ets + m.allocated_size) ** 2)
            existing_candidates.append({
                "market": m,
                "score": score,
                "is_new": False,
                "id": m.condition_id,
            })

        candidates = [c for c in new_candidates + existing_candidates if c["score"] > 0]
        if not candidates:
            break

        best = max(candidates, key=lambda c: c["score"])

        if best["is_new"]:
            mkt: ScoredMarket = best["market"]
            remaining -= mkt.min_size
            selected[best["id"]] = _to_allocated(mkt, mkt.min_size)
            unselected = [m for m in unselected if m.condition_id != best["id"]]
        else:
            increment = min(5.0, remaining)
            mkt = selected[best["id"]]
            mkt.allocated_size += increment
            remaining -= increment

    # Compute estimated daily rewards
    for m in selected.values():
        m.estimated_daily_rewards = (
            m.allocated_size / (m.existing_total_size + m.allocated_size)
        ) * m.total_daily_rewards

    return list(selected.values())
