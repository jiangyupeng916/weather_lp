import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from screener.types import CandidateMarket, ScoredMarket

BATCH_SIZE = 500


def _fetch_orderbooks_batch(token_ids: list[str], cfg, retries: int = 5) -> dict[str, dict]:
    url = f"{cfg.host}/books"
    body = [{"token_id": tid} for tid in token_ids]
    for attempt in range(retries + 1):
        try:
            resp = requests.post(url, json=body, timeout=30)
            if resp.status_code == 429 and attempt < retries:
                time.sleep(2 * (attempt + 1))
                continue
            resp.raise_for_status()
            books: dict[str, dict] = {}
            for item in resp.json():
                tid = item.get("asset_id", "")
                books[tid] = {
                    "bids": [
                        {"price": float(b["price"]), "size": float(b["size"])}
                        for b in item.get("bids", [])
                    ],
                    "asks": [
                        {"price": float(a["price"]), "size": float(a["size"])}
                        for a in item.get("asks", [])
                    ],
                }
            return books
        except Exception:
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
            else:
                raise


def _fetch_all_orderbooks(candidates: list[CandidateMarket], cfg) -> dict[str, dict]:
    token_ids: list[str] = []
    for m in candidates:
        token_ids.append(m.yes_token_id)
        token_ids.append(m.no_token_id)

    batches = [token_ids[i:i + BATCH_SIZE] for i in range(0, len(token_ids), BATCH_SIZE)]
    all_books: dict[str, dict] = {}

    # /books 限流 500 req/10s（官方文档），19 批并发远低于上限，可安全提高并发。
    # 候选多时（如 Midterms 标签 4755 候选 = 19 批）串行排队是耗时主因，提并发加速。
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(_fetch_orderbooks_batch, b, cfg): b for b in batches}
        for future in as_completed(futures):
            batch_books = future.result()
            all_books.update(batch_books)

    return all_books


def _calc_midpoint(book: dict) -> float:
    bids = book["bids"]
    asks = book["asks"]
    best_bid = max(b["price"] for b in bids) if bids else None
    best_ask = min(a["price"] for a in asks) if asks else None
    if best_bid is not None and best_ask is not None:
        return (best_bid + best_ask) / 2
    if best_bid is not None:
        return best_bid
    if best_ask is not None:
        return best_ask
    return 0.5


def _calc_total_size(book: dict, midpoint: float, max_spread: float) -> float:
    lower = midpoint - max_spread
    upper = midpoint + max_spread
    bid_size = sum(b["size"] for b in book["bids"] if b["price"] >= lower)
    ask_size = sum(a["size"] for a in book["asks"] if a["price"] <= upper)
    return bid_size + ask_size


def _score_market(market: CandidateMarket, books: dict[str, dict], cfg) -> ScoredMarket | None:
    yes_book = books.get(market.yes_token_id)
    no_book = books.get(market.no_token_id)
    if yes_book is None or no_book is None:
        return None

    midpoint = _calc_midpoint(yes_book)
    if midpoint < cfg.screener_min_midpoint or midpoint > cfg.screener_max_midpoint:
        return None

    yes_lower = midpoint - market.max_spread
    yes_in_range = [b for b in yes_book["bids"] if b["price"] >= yes_lower]
    yes_top3 = sum(b["size"] for b in yes_in_range[-3:])
    yes_top2 = sum(b["size"] for b in yes_in_range[-2:])
    yes_top1 = sum(b["size"] for b in yes_in_range[-1:])

    no_midpoint = 1 - midpoint
    no_lower = no_midpoint - market.max_spread
    no_in_range = [b for b in no_book["bids"] if b["price"] >= no_lower]
    no_top3 = sum(b["size"] for b in no_in_range[-3:])
    no_top2 = sum(b["size"] for b in no_in_range[-2:])
    no_top1 = sum(b["size"] for b in no_in_range[-1:])

    yes_total = _calc_total_size(yes_book, midpoint, market.max_spread)
    no_total = _calc_total_size(no_book, no_midpoint, market.max_spread)
    existing = yes_total + no_total

    reward_per_dollar = market.total_daily_rewards / (existing + market.min_size)

    return ScoredMarket(
        condition_id=market.condition_id,
        question=market.question,
        end_date=market.end_date,
        days_to_expiry=market.days_to_expiry,
        yes_token_id=market.yes_token_id,
        no_token_id=market.no_token_id,
        total_daily_rewards=market.total_daily_rewards,
        min_size=market.min_size,
        max_spread=market.max_spread,
        midpoint=midpoint,
        yes_total_size=yes_total,
        no_total_size=no_total,
        existing_total_size=existing,
        reward_per_dollar=reward_per_dollar,
        yes_top3_bids=yes_top3,
        no_top3_bids=no_top3,
        yes_top2_bids=yes_top2,
        no_top2_bids=no_top2,
        yes_top1_bids=yes_top1,
        no_top1_bids=no_top1,
        volume=market.volume,
        volume24hr=market.volume24hr,
        liquidity=market.liquidity,
    )


def analyze_orderbooks(candidates: list[CandidateMarket], cfg) -> list[ScoredMarket]:
    books = _fetch_all_orderbooks(candidates, cfg)

    results: list[ScoredMarket | None] = []
    for market in candidates:
        try:
            results.append(_score_market(market, books, cfg))
        except Exception:
            results.append(None)

    scored = [r for r in results if r is not None]
    return sorted(
        scored,
        key=lambda m: (-m.reward_per_dollar, m.min_size),
    )
