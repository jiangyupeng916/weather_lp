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
    return bool(state.get("pending_trades") or state.get("pending_legs")) or any(
        m.get("order") or m.get("tx") or m.get("blocked") or m.get("draining_orders")
        or m.get("recover_sell")
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
        if state.get("version") not in (1, 2):
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
            "version": 2, "enabled": True, "wallet": self.address,
            "initialized": False, "since": int(time.time()), "cursor": 0,
            "markets": {}, "seen": {}, "fills": {}, "owned_orders": {},
            "pending_trades": {}, "terminal_trades": {},
        }
        self.trigger = getattr(cfg, "hedge_trigger", "confirmed")
        self.state.setdefault("pending_legs", {})
        if (self.state.get("trigger", "confirmed") != self.trigger
                and any(p.get("early") for p in self.state["pending_legs"].values())):
            self.journal.close()
            raise RuntimeError("Unsettled early hedges: keep HEDGE_TRIGGER=matched until reconciled")
        # Version 1 inventories, order signatures and dedupe keys are preserved.
        # Old code rejects version 2 instead of ignoring provisional fills.
        self.state.update(version=2, trigger=self.trigger)
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
            if market["order"] or market["tx"] or market["blocked"] or market["draining_orders"] or market.get("recover_sell") or any(
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
            log.error("[HEDGE ORDER UNKNOWN] id=%s | %s", order["id"], exc)
            return
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

    def _merge_pairs(self, condition, market, quantities):
        if market["order"] or self._waiting(condition) or min(quantities) < UNIT:
            return
        shares = min(quantities).quantize(UNIT, rounding=ROUND_DOWN)
        # Wait for actual onchain tokens only at merge time.
        if min(self.sdk.balances(condition)) >= shares:
            self._submit_transaction(condition, market, shares, self.sdk.approved(condition))
        else:
            log.debug("[HEDGE WAIT_BALANCE] condition=%s paired shares=%s", condition, shares)

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
                self._merge_pairs(condition, market, quantities)
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
        book = self.client.get_order_book(token_id=token)
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
        if self._transaction(condition, market) or market["blocked"]:
            return
        self._reservation(market)
        if market["blocked"]:
            return
        positions = self._positions(condition, market)
        if (not any(dec(p["qty"]) > 0 for p in positions.values())
                and not market["order"] and not market["draining_orders"] and not market.get("recover_sell")):
            return
        if not self._quiesce(condition, market):
            return
        if market.get("recover_sell"):
            self._recover_market(condition, market)
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
        # Do not wait for the source balance. The selected trigger supplies
        # confirmed/provisional exact shares; only collateral gates this BUY.
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
