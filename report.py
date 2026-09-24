"""
report.py — latest-snapshot LIP reward-share estimate for every watched market.

Prints, for each market on the watchlist, the gross reward/day a resting bid would earn
on each side at the reference price, and ranks by gross reward per $100 of capital
(the handoff's ranking), showing absolute dollars too so 1-cent contracts don't win on
ratio alone.

THIS IS AN UPPER BOUND, NOT A FORECAST. It ignores: fills and queue position, adverse
selection (getting hit just before news), competitors who re-quote, maker fees, the
$1 minimum payout, and the unconfirmed 50/50 YES/NO split of the pool (--side-split).
The Phase 2 simulator is what turns this into a real number.

    python report.py [--qty 100] [--top 30] [--side-split 0.5] [--min-gross 0.5]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import lip_scoring as lip

DB_PATH = Path(__file__).parent / "data" / "lip.db"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--qty", type=float, default=100)
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--side-split", type=float, default=0.5)
    ap.add_argument("--min-gross", type=float, default=0.5, help="hide rows earning less than this $/day")
    ap.add_argument("--thin-book-ref", choices=("best", "worst"), default="best")
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """
        SELECT w.ticker, bp.ts_ms, bs.yes_json, bs.no_json, bs.truncated,
               p.period_reward_raw, p.start_date, p.end_date, p.target_size_raw, p.discount_bps
        FROM watchlist w
        JOIN book_polls bp ON bp.ticker = w.ticker
             AND bp.ts_ms = (SELECT MAX(ts_ms) FROM book_polls WHERE ticker = w.ticker)
        JOIN book_states bs ON bs.hash = bp.hash
        JOIN programs p ON p.market_ticker = w.ticker
             AND p.last_seen_ms = (SELECT MAX(last_seen_ms) FROM programs WHERE market_ticker = w.ticker)
        WHERE w.removed_ms IS NULL
        """
    ).fetchall()
    if not rows:
        print("No data yet. Run collector.py first.")
        return

    from datetime import datetime

    def days(s, e):
        f = lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")).timestamp()
        return (f(e) - f(s)) / 86400

    out = []
    for tk, ts, yes_j, no_j, trunc, reward, start, end, target, disc in rows:
        pool = reward / 10000 / days(start, end)
        for side, raw in (("YES", yes_j), ("NO", no_j)):
            levels = [(float(p), float(q)) for p, q in json.loads(raw)]
            est = lip.estimate_reward(levels, pool, float(target), disc / 10000, args.qty,
                                      args.side_split, thin_book_ref=args.thin_book_ref)
            if est["price"] is None or est["gross_per_day"] < args.min_gross:
                continue
            out.append((est["per_100_capital"], tk, side, pool, est, trunc))

    out.sort(key=lambda r: -r[0])
    print(f"{len(rows)} watched markets with a book; showing top {args.top} rows "
          f"(bid of {args.qty:g} contracts at the reference price, side split {args.side_split}).")
    print("UPPER BOUND ONLY: ignores fills, adverse selection, competitors, fees, $1 min payout.\n")
    print(f"{'ticker':44s} {'side':4s} {'pool/d':>7s} {'ref':>5s} {'share':>6s} {'gross/d':>8s} {'capital':>8s} {'per$100':>8s}")
    for per100, tk, side, pool, est, trunc in out[: args.top]:
        flag = "*" if trunc else " "
        print(f"{tk[:44]:44s} {side:4s} {pool:7.0f} {est['price']:5.2f} {est['share']:6.1%} "
              f"{est['gross_per_day']:8.2f} {est['capital']:8.1f} {per100:8.1f}{flag}")
    print("\n* book had more levels than stored (share slightly overstated).")


if __name__ == "__main__":
    main()
