import csv
import os
import time
from datetime import datetime, timezone

from config import CONFIG
from markets import fetch_and_filter
from clob import analyze_orderbooks

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
CSV_PATH = os.path.join(DATA_DIR, "screener_latest.csv")
CSV_TMP = os.path.join(DATA_DIR, "screener_latest.csv.tmp")


def save_csv(scored) -> tuple[int, int]:
    os.makedirs(DATA_DIR, exist_ok=True)
    sorted_m = sorted(scored, key=lambda m: m.reward_per_dollar, reverse=True)
    yes_count = 0
    no_count = 0
    with open(CSV_TMP, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Market", "minSz", "Reward/day", "Competition",
                         "Comp_YES", "Comp_NO", "Comp_YES_2", "Comp_NO_2",
                         "Comp_YES_1", "Comp_NO_1",
                         "yes_token_id", "no_token_id"])
        for m in sorted_m:
            yes_ok = (m.yes_top3_bids >= CONFIG.MIN_TOP3_BIDS
                      and m.yes_top2_bids >= CONFIG.MIN_TOP2_BIDS
                      and m.yes_top1_bids >= CONFIG.MIN_TOP1_BIDS)
            no_ok = (m.no_top3_bids >= CONFIG.MIN_TOP3_BIDS
                     and m.no_top2_bids >= CONFIG.MIN_TOP2_BIDS
                     and m.no_top1_bids >= CONFIG.MIN_TOP1_BIDS)
            if yes_ok:
                yes_count += 1
            if no_ok:
                no_count += 1
            writer.writerow([
                m.question,
                f"{m.min_size:.0f}",
                f"{m.total_daily_rewards:.2f}",
                f"{m.existing_total_size:.0f}",
                f"{m.yes_top3_bids:.0f}",
                f"{m.no_top3_bids:.0f}",
                f"{m.yes_top2_bids:.0f}",
                f"{m.no_top2_bids:.0f}",
                f"{m.yes_top1_bids:.0f}",
                f"{m.no_top1_bids:.0f}",
                m.yes_token_id if yes_ok else "",
                m.no_token_id if no_ok else "",
            ])
    os.replace(CSV_TMP, CSV_PATH)
    return yes_count, no_count


def run() -> None:
    t0 = time.time()
    candidates = fetch_and_filter()
    scored = analyze_orderbooks(candidates)
    scored = [m for m in scored if m.existing_total_size >= CONFIG.MIN_EXISTING_SIZE]
    yes_count, no_count = save_csv(scored)

    elapsed = time.time() - t0
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}]  {len(scored)} markets in {elapsed:.1f}s  |  yes:{yes_count} no:{no_count}  |  next run in {CONFIG.INTERVAL_MINUTES}min")


def main() -> None:
    print(f"Interval: {CONFIG.INTERVAL_MINUTES} min  |  Ctrl+C to stop")
    while True:
        try:
            run()
        except Exception as e:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{now}]  Error: {e}  |  retrying in {CONFIG.INTERVAL_MINUTES}min")
        time.sleep(CONFIG.INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    main()
