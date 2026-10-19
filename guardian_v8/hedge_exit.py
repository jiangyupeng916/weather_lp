"""Durable, single-writer BUY-opposite / MERGE exit state machine.

Only CONFIRMED trade legs change inventory. An accepted/matched order is a
reservation, not inventory. Network reads failing never mean zero holdings.
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
    return bool(state.get("pending_trades")) or any(
        m.get("order") or m.get("tx") or m.get("blocked") or m.get("draining_orders")
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
        if state.get("version") != 1:
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
        previous = self.journal.read()
        if previous and previous["wallet"] != self.address:
            self.journal.close()
            raise RuntimeError("Hedge journal belongs to another wallet")
        self.state = previous if previous and previous.get("enabled") else {
            "version": 1, "enabled": True, "wallet": self.address,
            "initialized": False, "since": int(time.time()), "cursor": 0,
            "markets": {}, "seen": {}, "fills": {}, "owned_orders": {},
            "pending_trades": {}, "terminal_trades": {},
        }
        self.inbox = queue.Queue()
        self.wake = threading.Event()
        self.stopping = threading.Event()
        self.thread = None
        self.ready = False
        self.fatal = False
        self._snapshot_lock = threading.Lock()
        self._blocked_tokens = set()
        self._owned_orders = set(self.state["owned_orders"])
        self.last_poll = 0.0
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
            if market["order"] or market["tx"] or market["blocked"] or market["draining_orders"] or any(
                    dec(p["qty"]) > 0 for p in market["inventory"].values()):
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
            self.wake.wait(self.cfg.hedge_poll_interval)
            self.wake.clear()

    def _bootstrap(self):
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
            }
        return markets[condition]

    def ingest(self, event):
        trade_id = str(value(event, "id", default=""))
        condition = str(value(event, "condition_id", "market", default=""))
        status = status_of(event)
        legs = list(own_legs(event, self.address, self.owner))
        if not trade_id or not condition or not legs:
            return
        matched = value(event, "matched_at", "match_time", "matchtime")
        if matched is not None:
            if hasattr(matched, "timestamp"):
                matched = matched.timestamp()
            else:
                try:
                    matched = float(matched)
                except (ValueError, TypeError):
                    from datetime import datetime
                    matched = datetime.fromisoformat(str(matched).replace("Z", "+00:00")).timestamp()
            if matched < self.state["since"]:
                return
        if status not in ("CONFIRMED", "FAILED"):
            if trade_id in self.state["terminal_trades"]:
                return  # delayed MATCHED must not resurrect a confirmed trade
            self.state["pending_trades"][trade_id] = condition
            self._persist()
            return
        self.state["pending_trades"].pop(trade_id, None)
        self.state["terminal_trades"][trade_id] = status
        market = self._market(condition)
        for order_id, token, side, quantity, price in legs:
            if not order_id or token not in market["tokens"] or side not in ("BUY", "SELL"):
                raise ValueError("Incomplete or inconsistent owned trade leg")
            if quantity <= 0 or not ZERO < price < ONE:
                raise ValueError("Invalid fill shares/price")
            key = f"{trade_id}:{value(event, 'bucket_index', default=0)}:{order_id}:{token}:{side}"
            if key in self.state["seen"]:
                if self.state["seen"][key] != status:
                    market["blocked"] = "Conflicting terminal trade statuses"
                continue
            self.state["seen"][key] = status
            totals = self.state["fills"].setdefault(order_id, {"CONFIRMED": "0", "FAILED": "0"})
            totals[status] = str(dec(totals[status]) + quantity)
            if status == "FAILED":
                continue
            owned = self.state["owned_orders"].get(order_id)
            if owned and (token != owned["token"] or dec(totals["CONFIRMED"]) > dec(owned["shares"])):
                market["blocked"] = "Hedge execution exceeds reserved shares or wrong token; reconcile account"
            inv = market["inventory"][token]
            old_qty, old_cost = dec(inv["qty"]), dec(inv["cost"])
            if side == "BUY":
                inv["qty"] = str(old_qty + quantity)
                inv["cost"] = str(old_cost + price * quantity)
                if old_qty == 0:
                    inv["first"] = time.time()
                if order_id not in self.state["owned_orders"]:
                    market["immediate"] = True
                    market["next_quote"] = 0
            elif quantity <= old_qty:
                inv["qty"] = str(old_qty - quantity)
                inv["cost"] = str(old_cost * (old_qty - quantity) / old_qty)
            else:
                market["blocked"] = "SELL exceeds journal inventory; external activity needs reconciliation"
            self.trade_logger.info("[HEDGE FILL] trade=%s order=%s token=%s side=%s shares=%s price=%s",
                                   trade_id, order_id, token, side, quantity, price)
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

    def step(self):
        if self.fatal or self.stopping.is_set():
            return
        if not self.state["initialized"]:
            self._bootstrap()
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

    def _settled(self, remote):
        """Reconcile every associated trade before treating cancellation as final."""
        matched = dec(remote.size_matched)
        totals = self.state["fills"].get(str(remote.id), {"CONFIRMED": "0", "FAILED": "0"})
        if dec(totals["CONFIRMED"]) + dec(totals["FAILED"]) >= matched:
            return True
        total = ZERO
        for trade_id in remote.associate_trades:
            for page in self.client.list_account_trades(id=trade_id):
                for trade in page.items:
                    self.ingest(trade)
                    if status_of(trade) in ("CONFIRMED", "FAILED"):
                        total += sum((qty for oid, _, _, qty, _ in own_legs(
                            trade, self.address, self.owner) if oid == str(remote.id)), ZERO)
        return total >= matched

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
            settled = self._settled(remote)
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
            self._post(market)
            return market["order"] is not None
        try:
            remote = self.client.get_order(order_id=order["id"])
        except Exception:
            # Submission may have succeeded. Replay EXACT signed bytes only;
            # never create a fresh salt/order while the previous one is unknown.
            if order["state"] == "unknown":
                self._post(market, retry=True)
            return True
        if str(remote.token_id) != order["token"] or str(remote.side).upper() != "BUY":
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
            log.error("[HEDGE ORDER UNKNOWN] id=%s | %s", order["id"], exc)
            return
        if response.ok:
            if str(response.order_id).lower() != order["id"].lower():
                self.state["owned_orders"][str(response.order_id)] = self.state["owned_orders"][order["id"]]
                market["blocked"] = "SDK order hash differs from exchange; reconcile before continuing"
            else:
                order["state"] = "live"
                log.info("[HEDGE BUY] %s shares=%s price=%s id=%s",
                         order["token"], order["shares"], order["price"], order["id"])
        elif not retry and response.code in (
                "market_not_ready", "not_enough_balance", "invalid_expiration", "post_only_would_cross"):
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

    def _submit_transaction(self, condition, market, shares, approved):
        # Relayer wallet nonces are shared across conditions. Serialize through
        # confirmation, not merely until the HTTP submission returns.
        if any(m["tx"] for cid, m in self.state["markets"].items() if cid != condition):
            return
        market["tx"] = {"kind": "merge" if approved else "approve", "shares": str(shares)}
        self._persist()
        try:
            handle = (self.sdk.merge(condition, shares) if approved else self.sdk.approve(condition))
        except Exception:
            market["blocked"] = "Transaction submission unknown; reconcile relayer before resuming"
            self._persist()
            raise
        market["tx"].update(id=handle.transaction_id, hash=handle.transaction_hash)
        self._persist()

    def _merge_pairs(self, condition, market, quantities):
        if market["order"] or min(quantities) < UNIT:
            return
        shares = min(quantities).quantize(UNIT, rounding=ROUND_DOWN)
        # Wait for actual onchain tokens only at merge time.
        if min(self.sdk.balances(condition)) >= shares:
            self._submit_transaction(condition, market, shares, self.sdk.approved(condition))
        else:
            log.debug("[HEDGE WAIT_BALANCE] condition=%s paired shares=%s", condition, shares)

    def _step_market(self, condition, market):
        if self._transaction(condition, market) or market["blocked"]:
            return
        self._reservation(market)
        if market["blocked"]:
            return
        if (not any(dec(p["qty"]) > 0 for p in market["inventory"].values())
                and not market["order"] and not market["draining_orders"]):
            return
        if not self._quiesce(condition, market):
            return
        tokens = market["tokens"]
        quantities = [dec(market["inventory"][t]["qty"]) for t in tokens]
        # Net both outcomes first, including independent reward fills. Buying
        # opposite for each fill separately would create redundant round trips.
        index = 0 if quantities[0] >= quantities[1] else 1
        source, opposite = tokens[index], tokens[1 - index]
        need = abs(quantities[0] - quantities[1])
        inv = market["inventory"][source]
        order = market["order"]
        if order and (order.get("unsettled") or order["state"] == "unknown"):
            return
        if condition in self.state["pending_trades"].values():
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
            self._merge_pairs(condition, market, quantities)
            return
        if time.time() < market["next_quote"]:
            return
        forced = bool(self.cfg.max_hold_enabled and
                      time.time() - inv["first"] >= self.cfg.max_hold_hours * 3600)
        passive = not market["immediate"] and not forced
        original_book = self.client.get_order_book(token_id=source)
        opposite_book = self.client.get_order_book(token_id=opposite)
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
                self._merge_pairs(condition, market, quantities)
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
        # Do not wait for the source balance: exact CONFIRMED shares already
        # determine need. Only collateral is required for the opposite BUY.
        collateral = self.client.get_balance_allowance(asset_type="COLLATERAL")
        if dec(collateral.balance) / 1_000_000 < price * shares:
            log.error("[HEDGE FUNDS] condition=%s insufficient collateral for %s shares", condition, shares)
            self._persist()
            self._merge_pairs(condition, market, quantities)
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
