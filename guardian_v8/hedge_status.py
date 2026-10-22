"""Read local exit progress without loading credentials or displaying signatures."""
import argparse
import json
from decimal import Decimal
from pathlib import Path


def summarize(state):
    markets = []
    for condition, market in state["markets"].items():
        provisional = {t: str(sum((Decimal(p["qty"]) for p in state.get("pending_legs", {}).values()
                                   if p["condition"] == condition and p["token"] == t and p["early"]), Decimal(0)))
                       for t in market["tokens"]}
        if not (market["order"] or market["tx"] or market["blocked"]
                or market["draining_orders"] or market.get("recover_sell")
                or any(Decimal(p) for p in provisional.values())
                or any(Decimal(p["qty"]) for p in market["inventory"].values())):
            continue
        order = market["order"] or {}
        markets.append({
            "condition": condition,
            "inventory": {t: p["qty"] for t, p in market["inventory"].items()},
            "order": {k: order[k] for k in ("id", "token", "shares", "price", "state", "canceling") if k in order},
            "transaction": market["tx"], "blocked": market["blocked"],
            "draining_orders": market["draining_orders"],
            "recover_sell": market.get("recover_sell", False), "provisional": provisional,
        })
    return {"wallet": state["wallet"], "enabled": state["enabled"],
            "trigger": state.get("trigger", "confirmed"),
            "startup_mode": state.get("startup_mode", "resume"),
            "startup_orders": state.get("startup_orders", {}),
            "initialized": state["initialized"],
            "pending_trades": state["pending_trades"], "markets": markets}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, help="Read one wallet's journal")
    args = parser.parse_args()
    paths = [args.state] if args.state else sorted(
        (Path(__file__).resolve().parent / "data" / "hedge_exit").glob("*.json"))
    if not paths:
        print("No hedge journal found. EXIT_MODE defaults to sell.")
    for path in paths:
        with path.open(encoding="utf-8") as stream:
            print(json.dumps(summarize(json.load(stream)), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
