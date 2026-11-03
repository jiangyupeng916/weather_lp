"""Read current positions through Data API v2, independently of the trading SDK.

v1 retired on 2026-11-02. Keep this public, read-only contract separate from
the audited SDK order signatures, CLOB trade reconciliation and merge routing.
"""
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterator

import requests


class DataApiError(ValueError):
    """An incomplete/invalid positions response must never mean an empty wallet."""


def _number(row, key):
    value = row.get(key)
    if value is None or isinstance(value, bool):
        raise DataApiError(f"Data API v2 position missing/invalid {key}")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise DataApiError(f"Data API v2 position invalid {key}") from exc
    if not number.is_finite():
        raise DataApiError(f"Data API v2 position non-finite {key}")
    return number


@dataclass(frozen=True)
class Position:
    condition_id: str
    asset_id: str
    size: Decimal
    avg_price: Decimal
    title: str = ""

    @classmethod
    def from_v2(cls, row, wallet):
        if not isinstance(row, dict):
            raise DataApiError("Data API v2 position must be an object")
        condition, token = row.get("condition_id"), row.get("token_id")
        if not all(isinstance(v, str) and v.strip() for v in (condition, token)):
            raise DataApiError("Data API v2 position missing condition_id/token_id")
        owner = row.get("proxy_wallet")
        if owner is not None and (not isinstance(owner, str) or owner.lower() != wallet.lower()):
            raise DataApiError("Data API v2 position belongs to another wallet")
        if row.get("status", "OPEN") not in ("OPEN", "REDEEMABLE"):
            raise DataApiError("Data API v2 OPEN query returned a non-held position")
        # total_size is LIFETIME bought shares, never the remaining holding.
        quantity, price = _number(row, "current_size"), _number(row, "avg_price")
        if quantity < 0 or not 0 <= price <= 1:
            raise DataApiError("Data API v2 position has invalid shares/entry price")
        return cls(condition, token, quantity, price, str(row.get("title") or ""))

    def legacy_fields(self):
        """Preserve the existing sell strategy's input, without changing prices."""
        return {"conditionId": self.condition_id, "asset": self.asset_id,
                "size": str(self.size), "avgPrice": str(self.avg_price), "title": self.title}


@dataclass(frozen=True)
class PositionPage:
    items: tuple[Position, ...]


class DataApiV2:
    def __init__(self, wallet, base_url="https://data-api.polymarket.com", *, get=None, timeout=10):
        self.wallet = str(wallet)
        if not self.wallet:
            raise ValueError("Positions require a wallet address")
        # Accept either the host or its documented /v2 base.
        base = base_url.rstrip("/")
        self.url = base + ("/positions" if base.endswith("/v2") else "/v2/positions")
        self.get = get if get is not None else requests.get
        self.timeout = timeout

    def list_positions(self, *, size_threshold=0, page_size=100) -> Iterator[PositionPage]:
        minimum = Decimal(str(size_threshold))
        if not minimum.is_finite() or minimum < 0:
            raise ValueError("Invalid positions share threshold")
        if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size <= 0:
            raise ValueError("Invalid positions page size")
        params = {"user": self.wallet, "status": "OPEN", "filter_type": "TOKENS",
                  "filter_amount": str(minimum), "sort_by": "TOKENS", "sort_direction": "DESC",
                  "limit": page_size}
        cursor, cursors, seen = None, set(), set()
        while True:
            query = dict(params)
            if cursor is not None:
                query["cursor"] = cursor
            response = self.get(self.url, params=query, timeout=self.timeout)
            response.raise_for_status()
            try:
                payload = response.json(parse_float=Decimal)
            except ValueError as exc:
                raise DataApiError("Data API v2 positions returned invalid JSON") from exc
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                raise DataApiError("Data API v2 positions missing data array")
            paging = payload.get("pagination")
            if not isinstance(paging, dict) or not isinstance(paging.get("has_more"), bool):
                raise DataApiError("Data API v2 positions missing pagination/has_more")
            following = paging.get("next_cursor")
            if paging["has_more"]:
                if not isinstance(following, str) or not following or following in cursors:
                    raise DataApiError("Data API v2 positions missing/repeated continuation cursor")
                cursors.add(following)
            elif following is not None:
                raise DataApiError("Data API v2 positions pagination contradicts has_more")
            items = []
            for row in payload["data"]:
                position = Position.from_v2(row, self.wallet)
                key = (position.condition_id, position.asset_id)
                if key in seen:
                    raise DataApiError("Data API v2 positions repeated a position; retry a stable snapshot")
                seen.add(key)
                if position.size >= minimum:
                    items.append(position)
            yield PositionPage(tuple(items))
            if not paging["has_more"]:
                return
            cursor = following
