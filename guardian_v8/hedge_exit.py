"""Durable, single-writer BUY-opposite / MERGE exit state machine.

Only CONFIRMED trade legs change inventory. Optional MATCHED triggering uses
separate provisional source legs; they can never be merged or sold as inventory.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import queue
import threading
import time
from decimal import Decimal, ROUND_DOWN

from hedge_sdk import HedgeSDK

log = logging.getLogger("guardian.hedge")
ZERO = Decimal(0)
ONE = Decimal(1)
UNIT = Decimal("0.000001")
REPEATED_ERROR_LOG_LIMIT = 3


def value(obj, name, *aliases, default=None):
    for key in (name, *aliases):
        result = obj.get(key) if isinstance(obj, dict) else getattr(obj, key, None)
        if result is not None:
            return result
    return default


def dec(number):
    result = Decimal(str(number))
    if not result.is_finite():
        raise ValueError("Non-finite hedge value")
    return result


def status_of(event):
    return str(value(event, "status", default="")).upper().removeprefix("TRADE_STATUS_")


def own_legs(event, address, owner):
    """Preserve each maker's token, side, price and shares; never sum unlike legs."""
    def ours(obj):
        return ((value(obj, "maker_address", default="").lower() == address.lower())
                or bool(owner and value(obj, "owner") == owner))

    for maker in value(event, "maker_orders", default=()) or ():
        if ours(maker):
            yield (str(value(maker, "order_id", default="")),
                   str(value(maker, "token_id", "asset_id", default="")),
                   str(value(maker, "side", default="")).upper(),
                   dec(value(maker, "matched_amount", default=0)),
                   dec(value(maker, "price", default=0)))
    if str(value(event, "trader_side", default="")).upper() == "TAKER" and ours(event):
        yield (str(value(event, "taker_order_id", default="")),
               str(value(event, "token_id", "asset_id", default="")),
               str(value(event, "side", default="")).upper(),
               dec(value(event, "size", default=0)), dec(value(event, "price", default=0)))


def hedge_quote(book, opposite_book, entry, gap, passive=False, forced=False, shares=ZERO):
    """Flip the original exit quote and round DOWN on the opposite token's grid."""
    tick = dec(opposite_book.tick_size)
    if tick <= 0 or tick >= 1:
        raise ValueError("Invalid opposite tick")
    if forced:
        asks = sorted((dec(x.price), dec(x.size)) for x in opposite_book.asks if dec(x.size) > 0)
        if not asks:
            return None
        depth = ZERO
        for price, size in asks:
            depth += size
            if depth >= shares:
                break
    else:
        levels = book.asks if passive else book.bids
        prices = [dec(x.price) for x in levels if dec(x.size) > 0]
        if not prices:
            return None
        original = min(prices) if passive else max(prices)
        if not ZERO < original < ONE or original < entry - gap:
            return None
        price = min(ONE - original, ONE - entry + gap)
    price = (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick
    return price if tick <= price <= ONE - tick else None


def has_exposure(state):
    return bool(state.get("pending_trades") or state.get("pending_legs") or state.get("startup_orders")) or any(
        m.get("order") or m.get("tx") or m.get("blocked") or m.get("draining_orders")
        or m.get("recover_sell") or m.get("merge_scan")
        or any(dec(p["qty"]) != 0 for p in m["inventory"].values())
        for m in state.get("markets", {}).values())


class Journal:
    """Atomic replace + fsync; OS lock prevents two hedge engines for one wallet."""
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = open(str(self.path) + ".lock", "a+b")
        try:
            self.lock.seek(0)
            if not self.lock.read(1):
                self.lock.write(b"0")
                self.lock.flush()
            self.lock.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise RuntimeError("This wallet already has a hedge engine running") from None

    def read(self):
        if not self.path.exists():
            return None
        state = json.loads(self.path.read_text(encoding="utf-8"))
        if state.get("version") not in (1, 2, 3):
            raise RuntimeError("Unsupported hedge journal; do not delete the journal")
        return state

    def write(self, state):
        temp = self.path.with_suffix(".tmp")
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(state, stream, ensure_ascii=False, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, self.path)
        if os.name != "nt":
            fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def archive(self):
        """Keep the previous bytes for audit, without reading them as state."""
        if not self.path.exists():
            return None
        folder = self.path.parent / "archive"
        folder.mkdir(mode=0o700, exist_ok=True)
        destination = folder / f"{self.path.stem}.{time.time_ns()}.json"
        os.replace(self.path, destination)
        return destination

    def close(self):
        self.lock.close()


def ensure_sell_safe(path):
    """Do not sell away shares reserved for a pending hedge/merge after a mode flip."""
    if not Path(path).exists():
        return
    journal = Journal(path)
    try:
        state = journal.read()
        if has_exposure(state):
            raise RuntimeError("Unfinished hedge exits: keep EXIT_MODE=hedge_merge and reconcile first")
        state["enabled"] = False
        journal.write(state)
    finally:
        journal.close()


class HedgeExit:
    def __init__(self, client, cfg, path, rate_wait, trade_logger=None, sdk=None, maker_busy=None):
        self.client, self.cfg = client, cfg
        self.sdk = sdk or HedgeSDK(client, rate_wait)
        self.maker_busy = maker_busy or (lambda tokens: False)
        self.trade_logger = trade_logger or log
        self.journal = Journal(path)
        self.address = str(client.wallet).lower()
        self.owner = str(client.credentials.key)
        self.startup_mode = getattr(cfg, "hedge_startup_mode", "positions")
        if self.startup_mode not in ("positions", "resume"):
            self.journal.close()
            raise ValueError("Invalid hedge startup mode")
        try:
            if self.startup_mode == "positions":
                archived = self.journal.archive()
                previous = None
                if archived:
                    log.info("[HEDGE STARTUP] Previous journal archived: %s", archived)
            else:
                previous = self.journal.read()
        except Exception:
            self.journal.close()
            raise
        if previous and previous["wallet"] != self.address:
            self.journal.close()
            raise RuntimeError("Hedge journal belongs to another wallet")
        self.state = previous if previous and previous.get("enabled") else {
            "version": 3, "enabled": True, "wallet": self.address,
            "initialized": False, "since": int(time.time()), "cursor": 0,
            "markets": {}, "seen": {}, "fills": {}, "owned_orders": {},
            "pending_trades": {}, "terminal_trades": {},
            "startup_mode": self.startup_mode, "startup_orders": {},
        }
        self.state.setdefault("startup_mode", "resume")
        self.state.setdefault("startup_orders", {})
        self.trigger = getattr(cfg, "hedge_trigger", "confirmed")
        self.state.setdefault("pending_legs", {})
        if (self.state.get("trigger", "confirmed") != self.trigger
                and any(p.get("early") for p in self.state["pending_legs"].values())):
            self.journal.close()
            raise RuntimeError("Unsettled early hedges: keep HEDGE_TRIGGER=matched until reconciled")
        # Preserve older inventories, signatures and dedupe keys. Older code
        # must reject v3 rather than ignore a refreshed inventory's time fence.
        self.state.update(version=3, trigger=self.trigger)
        for market in self.state["markets"].values():
            market.setdefault("recover_sell", False)
            market.setdefault("sell_immediate", True)
        self.inbox = queue.Queue()
        self.wake = threading.Event()
        self.stopping = threading.Event()
        self.thread = None
        self.ready = False
        self.fatal = False
        self._snapshot_lock = threading.Lock()
        self._blocked_tokens = set()
        self._owned_orders = set(self.state["owned_orders"])
        self._missing_book_logs = {}
        self._small_buy_logs = {}
        self.last_poll = 0.0
        self.last_merge_scan = time.monotonic()
        self._persist()

    def _persist(self):
        try:
            self.journal.write(self.state)
        except Exception:
            self.fatal = True
            self.ready = False
            raise
        blocked = set()
        for market in self.state["markets"].values():
            if market["order"] or market["tx"] or market["blocked"] or market["draining_orders"] or market.get("recover_sell") or market.get("merge_scan") or any(
                    dec(p["qty"]) > 0 for p in market["inventory"].values()):
                blocked.update(market["tokens"])
        for leg in self.state["pending_legs"].values():
            market = self.state["markets"].get(leg["condition"])
            if market:
                blocked.update(market["tokens"])
        for condition in self.state["pending_trades"].values():
            market = self.state["markets"].get(condition)
            if market:
                blocked.update(market["tokens"])
        with self._snapshot_lock:
            self._blocked_tokens = blocked
            self._owned_orders = set(self.state["owned_orders"])

    def owns(self, order_id):
        with self._snapshot_lock:
            return order_id in self._owned_orders

    def blocks(self, token):
        with self._snapshot_lock:
            return not self.ready or self.fatal or token in self._blocked_tokens

    def notify(self, event):
        self.inbox.put(event)
        self.wake.set()

    def start(self):
        self.thread = threading.Thread(target=self._run, name="hedge-exit", daemon=True)
        self.thread.start()

    def stop(self):
        self.stopping.set()
        self.wake.set()
        if self.thread:
            # Caller waits BEFORE cancel_all. No hedge write can appear afterwards.
            self.thread.join()

    def close(self):
        self.journal.close()

    def _run(self):
        while not self.stopping.is_set():
            try:
                self.step()
            except Exception as exc:
                self.ready = False
                log.error("[HEDGE PAUSED] %s", exc)
            if self.fatal:
                log.critical("[HEDGE JOURNAL] Cannot persist state; restart only after repairing storage")
                return
            delay = self.cfg.hedge_poll_interval
            if self.state["initialized"] and not self.ready:
                remaining = self.state["since"] - time.time()
                if remaining > 0:
                    delay = min(delay, remaining)
            self.wake.wait(delay)
            self.wake.clear()

    def _bootstrap(self):
        if self.state["startup_mode"] == "positions":
            self._bootstrap_positions()
            return
        # Migrating an account with old positions using a delayed Data API
        # snapshot would double-count trades. Require a clean activation once.
        start = int(time.time())
        markets = set()
        for page in self.client.list_positions(size_threshold=0, page_size=100):
            for pos in page.items:
                if pos.size is not None and dec(pos.size) > 0:
                    markets.add(str(pos.condition_id))
        for page in self.client.list_open_orders():
            if any(str(order.side).upper() == "SELL" for order in page.items):
                raise RuntimeError("First hedge activation requires no legacy SELL orders; finish exits in sell mode")
        for page in self.client.list_account_trades(after=str(start - 600)):
            for trade in page.items:
                if status_of(trade) not in ("CONFIRMED", "FAILED"):
                    raise RuntimeError("Unsettled trades at first activation; retry after settlement")
                markets.add(str(value(trade, "condition_id", "market")))
        for condition in markets:
            if any(q > 0 for q in self.sdk.balances(condition)):
                raise RuntimeError("First hedge activation requires empty outcome positions; finish legacy exits first")
        self.state["since"] = start
        self.state["cursor"] = start
        self.state["initialized"] = True
        self._persist()

    def _bootstrap_positions(self):
        """Use current Data API shares and entry prices as the sole baseline."""
        positions = {}
        conditions = set()
        for page in self.client.list_positions(size_threshold=0, page_size=100):
            for pos in page.items:
                if value(pos, "size") is None:
                    raise ValueError("Missing startup position size")
                quantity = dec(value(pos, "size"))
                if quantity < 0:
                    raise ValueError("Negative startup position")
                if quantity == 0:
                    continue
                condition = str(value(pos, "condition_id", "conditionId", default=""))
                token = str(value(pos, "asset_id", "token_id", "asset", default=""))
                raw_price = value(pos, "avg_price", "avgPrice")
                if raw_price is None:
                    raise ValueError("Missing startup position average price")
                price = dec(raw_price)
                if not condition or not token or not ZERO <= price <= ONE:
                    raise ValueError("Incomplete startup position or average price")
                key = (condition, token)
                if key in positions:
                    raise ValueError("Duplicate startup position; retry a stable snapshot")
                positions[key] = (quantity.quantize(UNIT, rounding=ROUND_DOWN), price)
                conditions.add(condition)
        snapshot = {}
        for condition in sorted(conditions):
            tokens = tuple(str(t) for t in self.sdk.tokens(condition))
            if len(tokens) != 2 or tokens[0] == tokens[1]:
                raise ValueError("Startup requires two distinct outcomes of one condition")
            if any(cid == condition and token not in tokens for cid, token in positions):
                raise ValueError("Startup position token does not belong to condition")
            snapshot[condition] = {t: positions.get((condition, t), (ZERO, ZERO)) for t in tokens}
        # CLOB match times have second precision. Keep reward placement paused
        # until the next second, separating the snapshot from this run's fills.
        start = int(time.time()) + 1
        for condition, holdings in snapshot.items():
            market = self._market(condition)
            market["inventory"] = {t: {"qty": str(qty), "cost": str(qty * price),
                                        "first": start if qty else 0}
                                   for t, (qty, price) in holdings.items()}
        self.state.update(initialized=True, since=start, cursor=start,
                          snapshot_only=True, startup_orders={})
        self.last_poll = time.time()
        self._persist()
        log.info("[HEDGE STARTUP POSITIONS] Rebuilt %s markets from current positions", len(snapshot))

    def _market(self, condition):
        markets = self.state["markets"]
        if condition not in markets:
            tokens = list(self.sdk.tokens(condition))
            if len(tokens) != 2 or tokens[0] == tokens[1]:
                raise ValueError("Hedge requires two distinct outcomes of the SAME condition")
            markets[condition] = {
                "tokens": tokens, "inventory": {
                    token: {"qty": "0", "cost": "0", "first": 0} for token in tokens},
                "order": None, "tx": None, "blocked": "", "immediate": True,
                "next_quote": 0, "draining_orders": [],
                "recover_sell": False, "sell_immediate": True,
            }
        return markets[condition]

    def _positions(self, condition, market):
        """Effective BUY target only; confirmed inventory remains untouched."""
        positions = {t: dict(p) for t, p in market["inventory"].items()}
        for leg in self.state["pending_legs"].values():
            if leg["condition"] != condition or not leg["early"]:
                continue
            inv = positions[leg["token"]]
            qty = dec(leg["qty"])
            inv["qty"] = str(dec(inv["qty"]) + qty)
            inv["cost"] = str(dec(inv["cost"]) + qty * dec(leg["price"]))
            inv["first"] = min(inv["first"] or leg["first"], leg["first"])
        return positions

    def _waiting(self, condition, allow_early=False):
        for tid, cid in self.state["pending_trades"].items():
            if cid != condition:
                continue
            legs = [p for p in self.state["pending_legs"].values() if p["trade"] == tid]
            if not allow_early or not legs or any(not p["early"] for p in legs):
                return True
        return False

    def ingest(self, event):
        trade_id = str(value(event, "id", default=""))
        condition = str(value(event, "condition_id", "market", default=""))
        status = status_of(event)
        legs = list(own_legs(event, self.address, self.owner))
        if not trade_id or not condition or not legs:
            return
        matched = value(event, "matched_at", "match_time", "matchtime")
        inventory_since = self.state["markets"].get(condition, {}).get("inventory_since", 0)
        if matched is None and (self.state.get("snapshot_only") or inventory_since):
            # An undated WS event might already be in the baseline. Let REST
            # provide its original match time rather than adding it speculatively.
            self.last_poll = 0
            return
        if matched is not None:
            if hasattr(matched, "timestamp"):
                matched = matched.timestamp()
            else:
                try:
                    matched = float(matched)
                except (ValueError, TypeError):
                    from datetime import datetime
                    matched = datetime.fromisoformat(str(matched).replace("Z", "+00:00")).timestamp()
            if matched < max(self.state["since"], inventory_since):
                return
        if status not in ("CONFIRMED", "FAILED"):
            if trade_id in self.state["terminal_trades"]:
                return  # delayed MATCHED must not resurrect a confirmed trade
        market = self._market(condition)
        if self.trigger == "matched" and value(event, "bucket_index") is None:
            # WS bucket_index is optional; REST supplies it. Do not guess 0:
            # a later REST bucket could otherwise look like a second source
            # fill. Leave this trade to the existing REST reconciliation path.
            if self.state["terminal_trades"].get(trade_id) == status:
                return
            self.state["pending_trades"][trade_id] = condition
            if status == "FAILED" and any(p["trade"] == trade_id and p["early"]
                                          for p in self.state["pending_legs"].values()):
                # Stop a known early hedge even before REST supplies the bucket
                # needed to finalize its accounting. Do not keep buying meanwhile.
                market["recover_sell"], market["sell_immediate"], market["next_quote"] = True, True, 0
            self._persist()
            return
        keyed = []
        for order_id, token, side, quantity, price in legs:
            if not order_id or token not in market["tokens"] or side not in ("BUY", "SELL"):
                raise ValueError("Incomplete or inconsistent owned trade leg")
            if quantity <= 0 or not ZERO < price < ONE:
                raise ValueError("Invalid fill shares/price")
            key = f"{trade_id}:{value(event, 'bucket_index', default=0)}:{order_id}:{token}:{side}"
            old = self.state["pending_legs"].get(key)
            if not old and any(p["trade"] == trade_id and p["order"] == order_id
                               and p["token"] == token and p["side"] == side
                               for p in self.state["pending_legs"].values()):
                market["blocked"] = "Pending trade bucket changed; reconcile account"
                self._persist()
                return
            if old and (old["condition"] != condition or dec(old["qty"]) != quantity
                        or dec(old["price"]) != price):
                market["blocked"] = "Pending trade leg changed quantity/price; reconcile account"
                self._persist()
                return
            keyed.append((key, order_id, token, side, quantity, price))
        if status not in ("CONFIRMED", "FAILED"):
            self.state["pending_trades"][trade_id] = condition
            for key, order_id, token, side, quantity, price in keyed:
                if key in self.state["seen"]:
                    continue
                old = self.state["pending_legs"].get(key)
                early = (self.trigger == "matched" and side == "BUY"
                         and order_id not in self.state["owned_orders"] and status in (
                             "MATCHED", "MATCHED_NOT_BROADCASTED", "MINED", "RETRYING"))
                self.state["pending_legs"][key] = {
                    "trade": trade_id, "condition": condition, "order": order_id,
                    "token": token, "side": side, "qty": str(quantity), "price": str(price),
                    "first": old["first"] if old else time.time(),
                    "early": early or bool(old and old["early"]),
                }
                if early and not old:
                    market["scan_only"] = False
                    market["immediate"], market["next_quote"] = True, 0
                    log.info("[HEDGE EARLY] trade=%s token=%s shares=%s", trade_id, token, quantity)
            self._persist()
            return
        self.state["terminal_trades"][trade_id] = status
        for key, order_id, token, side, quantity, price in keyed:
            if key in self.state["seen"]:
                if self.state["seen"][key] != status:
                    market["blocked"] = "Conflicting terminal trade statuses"
                continue
            pending = self.state["pending_legs"].pop(key, None)
            self.state["seen"][key] = status
            totals = self.state["fills"].setdefault(order_id, {"CONFIRMED": "0", "FAILED": "0"})
            totals[status] = str(dec(totals[status]) + quantity)
            if status == "FAILED":
                if pending and pending["early"]:
                    market["recover_sell"], market["sell_immediate"], market["next_quote"] = True, True, 0
                    log.warning("[HEDGE SOURCE FAILED] condition=%s trade=%s; cancel BUY then sell residual",
                                condition, trade_id)
                continue
            owned = self.state["owned_orders"].get(order_id)
            if owned and (token != owned["token"] or side != owned.get("side", "BUY")
                          or dec(totals["CONFIRMED"]) > dec(owned["shares"])):
                market["blocked"] = "Hedge execution exceeds reserved shares or wrong token; reconcile account"
            inv = market["inventory"][token]
            old_qty, old_cost = dec(inv["qty"]), dec(inv["cost"])
            if side == "BUY":
                inv["qty"] = str(old_qty + quantity)
                inv["cost"] = str(old_cost + price * quantity)
                if old_qty == 0:
                    inv["first"] = pending["first"] if pending and pending["early"] else time.time()
                if order_id not in self.state["owned_orders"] and not (pending and pending["early"]):
                    market["scan_only"] = False
                    market["immediate"] = True
                    market["next_quote"] = 0
            elif quantity <= old_qty:
                inv["qty"] = str(old_qty - quantity)
                inv["cost"] = str(old_cost * (old_qty - quantity) / old_qty)
            else:
                market["blocked"] = "SELL exceeds journal inventory; external activity needs reconciliation"
            self.trade_logger.info("[HEDGE FILL] trade=%s order=%s token=%s side=%s shares=%s price=%s",
                                   trade_id, order_id, token, side, quantity, price)
        if any(p["trade"] == trade_id for p in self.state["pending_legs"].values()):
            self.state["pending_trades"][trade_id] = condition
        else:
            self.state["pending_trades"].pop(trade_id, None)
        self._persist()

    def _poll_trades(self):
        now = time.time()
        after = max(self.state["since"] - 1, self.state["cursor"] - 120)
        for page in self.client.list_account_trades(after=str(int(after))):
            for trade in page.items:
                self.ingest(trade)
        # RETRYING can outlive the overlap; poll those IDs until terminal.
        for trade_id in list(self.state["pending_trades"]):
            for page in self.client.list_account_trades(id=trade_id):
                for trade in page.items:
                    self.ingest(trade)
        self.state["cursor"] = int(now)
        self.last_poll = now
        self._persist()

    def _poll_merge_positions(self):
        """Discover pairs outside the journal; never overwrite live reservations."""
        positions = {}
        for page in self.client.list_positions(size_threshold=0, page_size=100):
            for pos in page.items:
                quantity = dec(value(pos, "size"))
                if quantity < 0:
                    raise ValueError("Negative merge-scan position")
                if quantity < UNIT:
                    continue
                condition = str(value(pos, "condition_id", "conditionId", default=""))
                token = str(value(pos, "asset_id", "token_id", "asset", default=""))
                if not condition or not token:
                    raise ValueError("Incomplete merge-scan position")
                holdings = positions.setdefault(condition, {})
                if token in holdings:
                    raise ValueError("Duplicate merge-scan position; retry a stable snapshot")
                holdings[token] = pos
        candidates = sum(len(p) >= 2 for p in positions.values())
        log.info("[HEDGE POSITIONS SCAN] markets=%s paired_candidates=%s", len(positions), candidates)
        for condition, holdings in positions.items():
            if self.stopping.is_set():
                return
            if len(holdings) < 2:
                continue
            try:
                market = self._market(condition)
                if set(holdings) != set(market["tokens"]):
                    raise ValueError("Merge-scan tokens do not match this condition")
                # A historical replay also repairs fills missed beyond the
                # ordinary 120-second overlap, without touching signed orders.
                after = max(self.state["since"], market.get("inventory_since", 0)) - 1
                for page in self.client.list_account_trades(market=condition, after=str(after)):
                    for event in page.items:
                        self.ingest(event)
                if (market["order"] or market["tx"] or market["blocked"]
                        or market.get("recover_sell") or self._waiting(condition)
                        or market.get("merge_scan")):
                    log.info("[HEDGE SCAN DEFERRED] condition=%s active exit, transaction or unsettled trades", condition)
                    continue
                if min(dec(p["qty"]) for p in market["inventory"].values()) >= UNIT:
                    continue  # the normal partial-merge path already owns these pairs
                if min(self._chain_quantities(condition)) < UNIT:
                    continue  # do not cancel healthy reward orders on an old API snapshot
                prices = {t: dec(value(p, "avg_price", "avgPrice")) for t, p in holdings.items()}
                if any(not ZERO <= p <= ONE for p in prices.values()):
                    raise ValueError("Invalid merge-scan entry price")
                market["merge_scan"] = {"prices": {t: str(p) for t, p in prices.items()}}
                self._persist()  # block new reward orders before draining old ones
            except Exception as exc:
                log.error("[HEDGE SCAN RETRY] condition=%s | %s", condition, exc)
                if self.fatal:
                    raise

    def _refresh_merge_positions(self, condition, market):
        """Rebase only an idle, drained condition to actual chain holdings."""
        if market["order"] or market["tx"] or market["blocked"] or market.get("recover_sell"):
            market.pop("merge_scan", None)
            self._persist()
            return False
        if not self._quiesce(condition, market):
            return True
        after = max(self.state["since"], market.get("inventory_since", 0)) - 1
        for page in self.client.list_account_trades(market=condition, after=str(after)):
            for event in page.items:
                self.ingest(event)
        if self._waiting(condition) or market["blocked"] or market.get("recover_sell"):
            market.pop("merge_scan", None)
            self._persist()
            return True
        quantities = self._chain_quantities(condition)
        previous = [dec(market["inventory"][t]["qty"]) for t in market["tokens"]]
        prices = market.pop("merge_scan")["prices"]
        # A stale positions API response after a merge cannot resurrect shares.
        # An unchanged ledger needs no new baseline or time fence at all.
        if min(quantities) < UNIT or tuple(previous) == quantities:
            self._persist()
            return False
        start = int(time.time()) + 1
        if not any(previous):
            market["scan_only"] = True  # discovery alone must never create a BUY
        for token, quantity in zip(market["tokens"], quantities):
            old = market["inventory"][token]
            old_qty = dec(old["qty"])
            # Keep the known cost basis and holding clock. An indexer's older
            # average must not move an existing exit's price protection.
            retained_cost = dec(old["cost"]) * min(old_qty, quantity) / old_qty if old_qty else ZERO
            added_cost = max(ZERO, quantity - old_qty) * dec(prices[token])
            market["inventory"][token] = {
                "qty": str(quantity), "cost": str(retained_cost + added_cost),
                "first": old["first"] if old_qty > 0 else start,
            }
        market["inventory_since"] = start
        self._persist()
        log.info("[HEDGE SCAN RECONCILED] condition=%s shares=%s merge_only=%s",
                 condition, "/".join(str(q) for q in quantities), market.get("scan_only", False))
        return True  # wait for the per-condition timestamp boundary

    def _chain_quantities(self, condition):
        quantities = tuple(dec(q) for q in self.sdk.balances(condition))
        if len(quantities) != 2 or any(q < 0 for q in quantities):
            raise ValueError("Invalid merge-scan chain balances")
        return tuple(q.quantize(UNIT, rounding=ROUND_DOWN) for q in quantities)

    def _order_book(self, condition, token):
        """Limit missing-book logs, while keeping recovery checks running."""
        key = (condition, token)
        try:
            book = self.client.get_order_book(token_id=token)
        except Exception as exc:
            if "no orderbook exists for the requested token id" not in str(exc).lower():
                raise
            count = self._missing_book_logs.get(key, 0)
            if count < REPEATED_ERROR_LOG_LIMIT:
                count += 1
                self._missing_book_logs[key] = count
                suffix = ("; further identical errors suppressed; background checks continue"
                          if count == REPEATED_ERROR_LOG_LIMIT else "")
                log.error("[HEDGE RETRY] condition=%s token=%s missing_orderbook=%s/%s%s | %s",
                          condition, token, count, REPEATED_ERROR_LOG_LIMIT, suffix, exc)
            return None
        if self._missing_book_logs.pop(key, None):
            log.info("[HEDGE BOOK RESTORED] condition=%s token=%s", condition, token)
        return book

    def step(self):
        if self.fatal or self.stopping.is_set():
            return
        if not self.state["initialized"]:
            self._bootstrap()
            if not self.state["initialized"]:
                return
        if not self.ready and time.time() < self.state["since"]:
            return
        while not self.inbox.empty():
            # Do not lose a failed event: REST replay is an additional backstop.
            event = self.inbox.get_nowait()
            try:
                self.ingest(event)
            except Exception:
                self.inbox.put(event)
                raise
        if time.time() - self.last_poll >= self.cfg.hedge_poll_interval:
            self._poll_trades()
        interval = getattr(self.cfg, "hedge_merge_scan_interval", 60)
        if interval and time.monotonic() - self.last_merge_scan >= interval:
            self.last_merge_scan = time.monotonic()
            try:
                self._poll_merge_positions()
            except Exception as exc:
                log.error("[HEDGE SCAN RETRY] positions query failed | %s", exc)
                if self.fatal:
                    raise
        self.ready = True
        for condition, market in list(self.state["markets"].items()):
            if self.stopping.is_set():
                return
            try:
                self._step_market(condition, market)
            except Exception as exc:
                log.error("[HEDGE RETRY] condition=%s | %s", condition, exc)
                if self.fatal:
                    raise

    def _settled(self, remote, allow_early=False):
        """Reconcile every associated trade before treating cancellation as final."""
        matched = dec(remote.size_matched)
        def accounted():
            totals = self.state["fills"].get(str(remote.id), {"CONFIRMED": "0", "FAILED": "0"})
            pending = sum((dec(p["qty"]) for p in self.state["pending_legs"].values()
                           if allow_early and p["early"] and p["order"] == str(remote.id)), ZERO)
            return dec(totals["CONFIRMED"]) + dec(totals["FAILED"]) + pending
        if accounted() >= matched:
            return True
        for trade_id in remote.associate_trades:
            for page in self.client.list_account_trades(id=trade_id):
                for trade in page.items:
                    self.ingest(trade)
        return accounted() >= matched

    def _quiesce(self, condition, market):
        """Stop reward orders on BOTH tokens before a hedge can cross our own book."""
        if self.maker_busy(market["tokens"]):
            return False
        for page in self.client.list_open_orders(market=condition):
            for remote in page.items:
                oid = str(remote.id)
                if not self.owns(oid) and oid not in market["draining_orders"]:
                    market["draining_orders"].append(oid)
        self._persist()
        pending = []
        for oid in market["draining_orders"]:
            remote = self.client.get_order(order_id=oid)
            terminal = str(remote.status).upper() in ("CANCELED", "CANCELLED", "EXPIRED", "UNMATCHED")
            terminal = terminal or dec(remote.size_matched) >= dec(remote.original_size)
            settled = self._settled(remote, allow_early=self.trigger == "matched")
            if not terminal:
                self.sdk.cancel(oid)
            if not terminal or not settled:
                pending.append(oid)
        market["draining_orders"] = pending
        self._persist()
        return not pending

    def _reservation(self, market):
        order = market["order"]
        if not order:
            return False
        if order["state"] == "prepared":
            if market.get("recover_sell") and order.get("side", "BUY") == "BUY":
                market["order"] = None  # prepared was durably saved but never POSTed
                self._persist()
                return False
            self._post(market)
            return market["order"] is not None
        try:
            remote = self.client.get_order(order_id=order["id"])
        except Exception:
            # Submission may have succeeded. Replay EXACT signed bytes only;
            # never create a fresh salt/order while the previous one is unknown.
            if order["state"] == "unknown" and not (
                    market.get("recover_sell") and order.get("side", "BUY") == "BUY"):
                self._post(market, retry=True)
            return True
        if str(remote.token_id) != order["token"] or str(remote.side).upper() != order.get("side", "BUY"):
            market["blocked"] = "Order ID resolves to unexpected token/side"
            self._persist()
            return True
        order["state"] = "live"
        matched = dec(remote.size_matched)
        settled = self._settled(remote)
        terminal = str(remote.status).upper() in ("CANCELED", "CANCELLED", "EXPIRED", "UNMATCHED")
        terminal = terminal or matched >= dec(remote.original_size)
        if terminal and settled:
            market["order"] = None
            if order.get("side", "BUY") == "BUY":
                market["next_quote"] = 0
            self._persist()
            return False
        order["unsettled"] = not settled
        self._persist()
        return True

    def _cancel(self, market):
        order = market["order"]
        self.sdk.cancel(order["id"])
        # A cancel response is not settlement. Re-read order and all matched
        # trades on following cycles before releasing the reserved quantity.
        order["canceling"] = True
        self._persist()

    def _post(self, market, retry=False):
        order = market["order"]
        order["state"] = "unknown"
        self._persist()  # write-ahead of every network mutation
        try:
            response = self.sdk.post(order["signed"])
        except Exception as exc:
            message = str(exc).lower()
            if "invalid amount for a marketable buy order" in message and "min size" in message:
                count = self._small_buy_logs.get(order["id"], 0)
                if count < REPEATED_ERROR_LOG_LIMIT:
                    count += 1
                    self._small_buy_logs[order["id"]] = count
                    suffix = ("; further identical errors suppressed; reconciliation continues"
                              if count == REPEATED_ERROR_LOG_LIMIT else "")
                    log.error("[HEDGE ORDER UNKNOWN] id=%s minimum_amount=%s/%s%s | %s",
                              order["id"], count, REPEATED_ERROR_LOG_LIMIT, suffix, exc)
            else:
                log.error("[HEDGE ORDER UNKNOWN] id=%s | %s", order["id"], exc)
            return
        self._small_buy_logs.pop(order["id"], None)
        if response.ok:
            if str(response.order_id).lower() != order["id"].lower():
                self.state["owned_orders"][str(response.order_id)] = self.state["owned_orders"][order["id"]]
                market["blocked"] = "SDK order hash differs from exchange; reconcile before continuing"
            else:
                order["state"] = "live"
                log.info("[HEDGE %s] %s shares=%s price=%s id=%s", order.get("side", "BUY"),
                         order["token"], order["shares"], order["price"], order["id"])
        elif not retry and (response.code in (
                "market_not_ready", "not_enough_balance", "invalid_expiration", "post_only_would_cross")
                or (order.get("side") == "SELL" and order["forced"] and response.code == "fok_not_filled")):
            # Only a definite refusal of the FIRST submission releases shares.
            log.warning("[HEDGE REJECTED] %s", response.code)
            market["order"] = None
        else:
            log.warning("[HEDGE ORDER UNKNOWN] id=%s code=%s; reservation retained",
                        order["id"], response.code)
        self._persist()

    def _transaction(self, condition, market):
        tx = market["tx"]
        if not tx:
            return False
        if not tx.get("id") and not tx.get("hash"):
            # Submission lost its response / process died between submit and
            # receipt persistence. Do not merge a second batch speculatively.
            market["blocked"] = "Transaction submission unknown; reconcile relayer before resuming"
            self._persist()
            return True
        state = self.sdk.transaction_state(tx)
        if state == "STATE_CONFIRMED":
            if tx["kind"] == "merge":
                shares = dec(tx["shares"])
                if any(dec(inv["qty"]) < shares for inv in market["inventory"].values()):
                    market["blocked"] = "Confirmed merge conflicts with inventory; reconcile external activity"
                    self._persist()
                    return True
                for inv in market["inventory"].values():
                    qty = dec(inv["qty"])
                    inv["cost"] = str(dec(inv["cost"]) * (qty - shares) / qty)
                    inv["qty"] = str(qty - shares)
                self.trade_logger.info("[HEDGE MERGED] condition=%s shares=%s tx=%s",
                                       condition, shares, tx.get("hash") or tx.get("id"))
            market["tx"] = None
        elif state in ("STATE_FAILED", "STATE_INVALID"):
            log.error("[HEDGE TX FAILED] condition=%s kind=%s; retry after terminal failure", condition, tx["kind"])
            market["tx"] = None
        self._persist()
        return market["tx"] is not None

    def _submit_transaction(self, condition, market, shares, approved, sell_token=None):
        # Relayer wallet nonces are shared across conditions. Serialize through
        # confirmation, not merely until the HTTP submission returns.
        if any(m["tx"] for cid, m in self.state["markets"].items() if cid != condition):
            return
        market["tx"] = {"kind": "sell_approve" if sell_token else "merge" if approved else "approve",
                        "shares": str(shares)}
        self._persist()
        try:
            if sell_token:
                handle = self.sdk.approve(condition, sell=True, token=sell_token)
            else:
                handle = (self.sdk.merge(condition, shares) if approved else self.sdk.approve(condition))
        except Exception:
            market["blocked"] = "Transaction submission unknown; reconcile relayer before resuming"
            self._persist()
            raise
        market["tx"].update(id=handle.transaction_id, hash=handle.transaction_hash)
        self._persist()

    def _merge_pairs(self, condition, market):
        """Merge confirmed available pairs, preserving a live BUY's reservation."""
        if market["tx"]:
            return True
        order = market["order"]
        if order and (order.get("side", "BUY") != "BUY" or order["state"] != "live"
                      or order.get("canceling") or order.get("unsettled")):
            return False
        if self._waiting(condition) or market["draining_orders"]:
            return False
        # Provisional MATCHED source legs must never become mergeable inventory.
        quantities = [dec(market["inventory"][t]["qty"]) for t in market["tokens"]]
        paired = min(quantities)
        if paired < UNIT:
            return False
        if order:
            gap = quantities[0] - quantities[1]
            target = market["tokens"][1 if gap > 0 else 0]
            confirmed = dec(self.state["fills"].get(order["id"], {}).get("CONFIRMED", "0"))
            if dec(order["shares"]) - confirmed != abs(gap) or (gap and order["token"] != target):
                return False  # let normal order handling reconcile the changed target first
        try:
            shares = min(paired, *self.sdk.balances(condition)).quantize(UNIT, rounding=ROUND_DOWN)
            if shares < UNIT:
                log.debug("[HEDGE WAIT_BALANCE] condition=%s paired shares=%s", condition, paired)
                return False
            approved = self.sdk.approved(condition)
        except Exception as exc:
            # A read-only merge check must not disable normal BUY maintenance.
            # Submission/persistence failures below still keep their reservations.
            log.error("[HEDGE MERGE RETRY] condition=%s | %s", condition, exc)
            return False
        # A BUY reserves collateral, not the held outcome tokens. Burning equal
        # YES/NO amounts does not change its remaining gap or its signed shares.
        self._submit_transaction(condition, market, shares, approved)
        if market["tx"] and market["tx"]["kind"] == "merge":
            log.info("[HEDGE MERGE SUBMITTED] condition=%s shares=%s retained_buy=%s",
                     condition, shares, order["id"] if order else "none")
        return market["tx"] is not None

    def _recover_market(self, condition, market):
        """A source FAILED: settle/cancel buys, merge confirmed pairs, sell surplus.

        This path never creates a BUY. Recovery orders use the same durable
        reservation/settlement checks as hedge orders and survive restarts.
        """
        order = market["order"]
        if order and order.get("side", "BUY") == "BUY":
            if order["state"] != "unknown" and not order.get("canceling"):
                self._cancel(market)
            return
        if self._waiting(condition):
            return  # never sell provisional tokens or race a late BUY settlement
        if order and (order.get("unsettled") or order["state"] == "unknown" or order.get("canceling")):
            return
        quantities = [dec(market["inventory"][t]["qty"]) for t in market["tokens"]]
        if min(quantities) >= UNIT:
            if order:
                self._cancel(market)
            else:
                self._merge_pairs(condition, market)
            return
        if max(quantities) == 0 and not order:
            market["recover_sell"], market["sell_immediate"], market["next_quote"] = False, True, 0
            self._persist()
            return
        index = 0 if quantities[0] >= quantities[1] else 1
        token = market["tokens"][index]
        if order and order["token"] != token:
            self._cancel(market)
            return
        if time.time() < market["next_quote"]:
            return
        inv = market["inventory"][token]
        forced = bool(self.cfg.max_hold_enabled and
                      time.time() - inv["first"] >= self.cfg.max_hold_hours * 3600)
        book = self._order_book(condition, token)
        if book is None:
            return
        immediate = market["sell_immediate"]
        levels = book.bids if immediate else book.asks
        prices = [dec(p.price) for p in levels if dec(p.size) > 0]
        price = (max(prices) if immediate else min(prices)) if prices else None
        entry = dec(inv["cost"]) / dec(inv["qty"])
        gap = self.cfg.sell_min_bid_gap if immediate else self.cfg.sell_position_gap
        market["sell_immediate"] = False
        market["next_quote"] = time.time() + self.cfg.position_interval
        if not forced and (price is None or not ZERO < price < ONE or price < entry - gap):
            if order:
                self._cancel(market)
            else:
                self._persist()
            return
        if order:
            confirmed = dec(self.state["fills"].get(order["id"], {}).get("CONFIRMED", "0"))
            if (forced != bool(order["forced"]) or price != dec(order["price"])
                    or quantities[index] != dec(order["shares"]) - confirmed):
                self._cancel(market)
                market["next_quote"] = 0
                self._persist()
            return
        # The old seller must wait for spendable conditional-token balance.
        # Cap by BOTH confirmed ledger and actual chain/CLOB balances.
        available = min(quantities[index], self.sdk.balances(condition)[index],
                        self.sdk.sell_balance(condition, token))
        shares = available.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        if shares <= dec(getattr(self.cfg, "position_threshold", 1)) or shares < dec(book.min_order_size):
            log.warning("[HEDGE RECOVERY WAIT] condition=%s token=%s available=%s", condition, token, available)
            self._persist()
            return
        if not self.sdk.approved(condition, sell=True, token=token):
            market["sell_immediate"], market["next_quote"] = immediate, 0
            self._submit_transaction(condition, market, shares, False, sell_token=token)
            return
        signed, order_id = self.sdk.prepare_sell(condition, token, price, shares, forced)
        # A market SELL may estimate its own limit price. Persist the actual
        # signed order, never re-create it after an ambiguous POST result.
        market["order"] = {
            "id": order_id, "token": token, "side": "SELL", "shares": str(shares),
            "price": str(price or ZERO), "signed": signed, "state": "prepared",
            "forced": forced, "canceling": False,
        }
        self.state["owned_orders"][order_id] = {
            "condition": condition, "token": token, "side": "SELL", "shares": str(shares)}
        self._persist()
        if not self.stopping.is_set():
            self._post(market)

    def _step_market(self, condition, market):
        if time.time() < market.get("inventory_since", 0):
            return
        if market["tx"] and market.get("recover_sell"):
            order = market["order"]
            if (order and order.get("side", "BUY") == "BUY" and order["state"] == "live"
                    and not order.get("canceling")):
                # A source FAILED while a partial merge was in flight. Stop the
                # still-resting BUY now, rather than waiting for the merge.
                self._cancel(market)
        if self._transaction(condition, market) or market["blocked"]:
            return
        self._reservation(market)
        if market["blocked"]:
            return
        if market.get("merge_scan"):
            try:
                if self._refresh_merge_positions(condition, market):
                    return
            except Exception as exc:
                if self.fatal:
                    raise
                market.pop("merge_scan", None)
                self._persist()
                log.error("[HEDGE SCAN RETRY] condition=%s refresh failed | %s", condition, exc)
        positions = self._positions(condition, market)
        if (not any(dec(p["qty"]) > 0 for p in positions.values())
                and not market["order"] and not market["draining_orders"] and not market.get("recover_sell")):
            return
        if not self._quiesce(condition, market):
            return
        if market.get("recover_sell"):
            self._recover_market(condition, market)
            return
        # Recycle filled pairs before the next quote or funding check, even
        # while the unfilled remainder of a verified BUY is still resting.
        if self._merge_pairs(condition, market):
            return
        if market.get("scan_only"):
            return
        tokens = market["tokens"]
        positions = self._positions(condition, market)  # quiesce may ingest a late source fill
        quantities = [dec(positions[t]["qty"]) for t in tokens]
        # Net both outcomes first, including independent reward fills. Buying
        # opposite for each fill separately would create redundant round trips.
        index = 0 if quantities[0] >= quantities[1] else 1
        source, opposite = tokens[index], tokens[1 - index]
        need = abs(quantities[0] - quantities[1])
        inv = positions[source]
        order = market["order"]
        if order and (order.get("unsettled") or order["state"] == "unknown"):
            return
        if self._waiting(condition, allow_early=self.trigger == "matched"):
            # An unresolved source match could reduce or reverse the imbalance.
            if order and not order.get("canceling"):
                self._cancel(market)
            return
        if order and (need == 0 or order["token"] != opposite or order.get("canceling")):
            self._cancel(market)
            return
        if need < Decimal("0.01"):
            if need > 0 and time.time() >= market["next_quote"]:
                log.warning("[HEDGE DUST] condition=%s residual=%s below share precision", condition, need)
                market["next_quote"] = time.time() + self.cfg.position_interval
                self._persist()
            return
        if time.time() < market["next_quote"]:
            return
        forced = bool(self.cfg.max_hold_enabled and
                      time.time() - inv["first"] >= self.cfg.max_hold_hours * 3600)
        passive = not market["immediate"] and not forced
        original_book = self._order_book(condition, source)
        if original_book is None:
            return
        opposite_book = self._order_book(condition, opposite)
        if opposite_book is None:
            return
        entry = dec(inv["cost"]) / dec(inv["qty"])
        gap = self.cfg.sell_position_gap if passive else self.cfg.sell_min_bid_gap
        shares = need.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        price = hedge_quote(original_book, opposite_book, entry, gap, passive, forced, shares)
        market["immediate"] = False
        market["next_quote"] = time.time() + self.cfg.position_interval
        minimum = dec(opposite_book.min_order_size)
        if price is None or shares < minimum:
            if order:
                self._cancel(market)
            else:
                self._persist()
            if shares < minimum:
                log.warning("[HEDGE DUST] condition=%s residual=%s minimum=%s; never round shares up",
                            condition, need, minimum)
            return
        if order:
            confirmed = dec(self.state["fills"].get(order["id"], {}).get("CONFIRMED", "0"))
            remaining = dec(order["shares"]) - confirmed
            if (price != dec(order["price"]) or shares != remaining
                    or bool(order["forced"]) != forced):
                self._cancel(market)
                market["next_quote"] = 0
                self._persist()
            return
        # Do not wait for the source balance. The selected trigger supplies
        # confirmed/provisional exact shares; only collateral gates this BUY.
        collateral = self.client.get_balance_allowance(asset_type="COLLATERAL")
        if dec(collateral.balance) / 1_000_000 < price * shares:
            log.error("[HEDGE FUNDS] condition=%s insufficient collateral for %s shares", condition, shares)
            self._persist()
            return
        signed, order_id = self.sdk.prepare(condition, opposite, price, shares, passive, forced)
        market["order"] = {
            "id": order_id, "token": opposite, "shares": str(shares), "price": str(price),
            "signed": signed, "state": "prepared", "forced": forced, "canceling": False,
        }
        self.state["owned_orders"][order_id] = {
            "condition": condition, "token": opposite, "shares": str(shares)}
        self._persist()  # publish ownership BEFORE any order can be observed by discover/audit
        if not self.stopping.is_set():
            self._post(market)
