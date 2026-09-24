"""
adverse_selection.py — how much do passive fills lose (or win) at settlement?

Uses ONLY public historical data (past incentive programs, trades, settlement results,
1-minute bid/ask candles), so it can be run today without waiting for the collector.

WHAT IT MEASURES: the settlement payoff of every maker fill in non-sports markets that
had a liquidity program, restricted to trades inside the program's own window. This is
the adverse-selection half of the LIP question. It is NOT net LIP P&L:
  - it says nothing about how often YOUR order would fill (that needs book depth and
    queue position, i.e. the live collector), and nothing about rewards;
  - it averages over every maker who was hit, including ones resting far from the touch
    and ones who never cancel. A `near best bid` cut (using bid/ask candles) approximates
    fills from orders that would actually earn LIP rewards;
  - it is pre-fee (most series appear to charge no maker fee; unconfirmed).

Sign convention (verified on a real trade: taker bought NO at 0.99 == hit a YES bid at 0.01):
  taker_side == "no"  -> maker bought YES at yes_price
  taker_side == "yes" -> maker bought NO  at no_price
  payoff per contract = (1 if the maker's side won else 0) - price paid
  return on capital   = payoff / price

Sanity benchmark (Burgi, Deng & Whelan, "Makers and Takers", cited in HANDOFF.md): makers
averaged about -9.6% pre-fee across all prices, about +2.6% on contracts priced >= 50c, and
5c contracts won about 2% of the time. Different period and markets, so only a rough
neighbourhood test for a sign or denominator bug.

    python adverse_selection.py fetch  [--n 500]     # sample + download trades/results
    python adverse_selection.py candles              # 1-min bid/ask candles for the sample
    python adverse_selection.py analyze              # tables (re-runnable, no network)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import sqlite3
import statistics
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import aiohttp

from collector import Api, SPORTS_CATEGORIES, SPORTS_PREFIXES, parse_iso, series_of

ROOT = Path(__file__).parent
DB_PATH = ROOT / "data" / "adverse.db"
LIVE_DB = ROOT / "data" / "lip.db"
log = logging.getLogger("adverse")

SCHEMA = """
CREATE TABLE IF NOT EXISTS sample_markets (
    ticker TEXT PRIMARY KEY, series TEXT, category TEXT, result TEXT, close_time TEXT,
    status TEXT, win_start INTEGER, win_end INTEGER, trades_truncated INTEGER);
CREATE TABLE IF NOT EXISTS windows (ticker TEXT, start_ts INTEGER, end_ts INTEGER);
CREATE TABLE IF NOT EXISTS hist_trades (
    trade_id TEXT PRIMARY KEY, ticker TEXT, ts INTEGER, yes_raw TEXT, no_raw TEXT,
    count_raw TEXT, taker_side TEXT);
CREATE INDEX IF NOT EXISTS ix_ht ON hist_trades(ticker, ts);
CREATE TABLE IF NOT EXISTS candles (
    ticker TEXT, end_ts INTEGER, bid_high TEXT, bid_low TEXT, bid_close TEXT,
    ask_high TEXT, ask_low TEXT, ask_close TEXT, PRIMARY KEY (ticker, end_ts));
"""


def db() -> sqlite3.Connection:
    (ROOT / "data").mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    return conn


# --------------------------------------------------------------------------- fetch

async def cmd_fetch(args) -> None:
    conn = db()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=40)) as session:
        api = Api(session, args.req_per_sec)

        # 1. past programs -> per-ticker windows
        progs, cursor = [], None
        for _ in range(args.program_pages):
            params = {"type": "liquidity", "status": "paid_out", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            data, _ = await api.get("/incentive_programs", params)
            rows = (data or {}).get("incentive_programs", [])
            progs += rows
            cursor = (data or {}).get("next_cursor")
            if not cursor or not rows:
                break
        by_ticker: dict[str, list] = defaultdict(list)
        for p in progs:
            by_ticker[p["market_ticker"]].append(
                (int(parse_iso(p["start_date"])), int(parse_iso(p["end_date"]))))
        log.info("past programs: %d rows, %d tickers", len(progs), len(by_ticker))

        # 2. series category, fail closed (same rule as the collector)
        cats: dict[str, str | None] = {}
        if LIVE_DB.exists():
            for sr, cat, cj in sqlite3.connect(LIVE_DB).execute(
                    "SELECT series_ticker, category, categories_json FROM series"):
                cats[sr] = "Sports" if (cat in SPORTS_CATEGORIES or any(
                    c in SPORTS_CATEGORIES for c in json.loads(cj or "[]"))) else cat
        for sr in sorted({series_of(t) for t in by_ticker} - set(cats)):
            data, _ = await api.get(f"/series/{sr}")
            s = (data or {}).get("series")
            if s:
                allc = set(s.get("categories") or []) | {s.get("category")}
                cats[sr] = "Sports" if allc & SPORTS_CATEGORIES else s.get("category")
        eligible = [t for t in by_ticker
                    if cats.get(series_of(t)) and cats[series_of(t)] != "Sports"
                    and not series_of(t).startswith(SPORTS_PREFIXES)]
        log.info("non-sports tickers with resolved category: %d of %d", len(eligible), len(by_ticker))
        # Mirror the collector's watchlist rule (program period >= min days). Without this the
        # sample is dominated by 15-minute / hourly price-ladder programs the handoff says to avoid.
        min_s = args.min_period_days * 86400
        eligible = [t for t in eligible if max(b - a for a, b in by_ticker[t]) >= min_s]
        log.info("after requiring a program period >= %.1f days: %d tickers", args.min_period_days, len(eligible))
        random.Random(7).shuffle(eligible)
        sample = eligible[: args.n]

        # 3. per-market: result, then trades inside the union of its program windows
        sem = asyncio.Semaphore(8)
        stats = defaultdict(int)

        async def one(t: str):
            async with sem:
                m, _ = await api.get(f"/markets/{t}")
                m = (m or {}).get("market") or {}
                if m.get("result") not in ("yes", "no"):
                    stats["no_result"] += 1
                    return
                wins = by_ticker[t]
                lo, hi = min(w[0] for w in wins), max(w[1] for w in wins)
                trades, cursor, truncated = [], None, 0
                for page in range(args.max_pages):
                    params = {"ticker": t, "limit": 1000, "min_ts": lo, "max_ts": hi}
                    if cursor:
                        params["cursor"] = cursor
                    d, _ = await api.get("/markets/trades", params)
                    rows = (d or {}).get("trades", [])
                    trades += rows
                    cursor = (d or {}).get("cursor")
                    if not cursor or len(rows) < 1000:
                        break
                else:
                    truncated = 1
                conn.execute("INSERT OR REPLACE INTO sample_markets VALUES (?,?,?,?,?,?,?,?,?)",
                             (t, series_of(t), cats[series_of(t)], m["result"], m.get("close_time"),
                              m.get("status"), lo, hi, truncated))
                conn.execute("DELETE FROM windows WHERE ticker=?", (t,))
                conn.executemany("INSERT INTO windows VALUES (?,?,?)", [(t, a, b) for a, b in wins])
                for tr in trades:
                    conn.execute("INSERT OR IGNORE INTO hist_trades VALUES (?,?,?,?,?,?,?)",
                                 (tr["trade_id"], t, int(parse_iso(tr["created_time"])),
                                  tr.get("yes_price_dollars"), tr.get("no_price_dollars"),
                                  tr.get("count_fp"), tr.get("taker_side")))
                stats["markets"] += 1
                stats["trades"] += len(trades)
                stats["truncated"] += truncated
                if stats["markets"] % 50 == 0:
                    conn.commit()
                    log.info("progress: %s", dict(stats))

        await asyncio.gather(*[one(t) for t in sample])
        conn.commit()
        log.info("fetch done: %s;  429s=%d errors=%d", dict(stats), api.n429, api.errors)


async def cmd_candles(args) -> None:
    conn = db()
    tickers = conn.execute("SELECT ticker FROM sample_markets").fetchall()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=40)) as session:
        api = Api(session, args.req_per_sec)
        sem = asyncio.Semaphore(6)
        done = 0

        async def one(t: str):
            nonlocal done
            async with sem:
                lo, hi = conn.execute("SELECT MIN(ts), MAX(ts) FROM hist_trades WHERE ticker=?", (t,)).fetchone()
                if lo is None:
                    return
                start = lo - lo % 60
                while start <= hi:
                    end = min(start + 2 * 86400, hi + 60)
                    d, _ = await api.get("/markets/candlesticks", {
                        "market_tickers": t, "start_ts": start, "end_ts": end, "period_interval": 1})
                    mk = ((d or {}).get("markets") or [{}])[0]
                    for c in mk.get("candlesticks") or []:
                        b, a = c.get("yes_bid") or {}, c.get("yes_ask") or {}
                        conn.execute("INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?,?,?,?)",
                                     (t, c["end_period_ts"], b.get("high_dollars"), b.get("low_dollars"),
                                      b.get("close_dollars"), a.get("high_dollars"), a.get("low_dollars"),
                                      a.get("close_dollars")))
                    start = end
                done += 1
                if done % 50 == 0:
                    conn.commit()
                    log.info("candles: %d/%d markets", done, len(tickers))

        await asyncio.gather(*[one(t[0]) for t in tickers])
        conn.commit()
        log.info("candles done: %d markets, %d candle rows", done, conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0])


# --------------------------------------------------------------------------- analysis

PRICE_BUCKETS = [(0.0, 0.05), (0.05, 0.10), (0.10, 0.30), (0.30, 0.50), (0.50, 0.70), (0.70, 0.90), (0.90, 1.01)]
TTC_BUCKETS = [(0, 3600, "<1h to close"), (3600, 6 * 3600, "1-6h"), (6 * 3600, 24 * 3600, "6-24h"),
               (24 * 3600, 1e12, ">24h")]


def load_fills(conn, near_best_only: bool):
    """Yield per-fill records for trades inside a program window."""
    res = {t: (r, c, cat) for t, r, c, cat in conn.execute(
        "SELECT ticker, result, close_time, category FROM sample_markets")}
    wins = defaultdict(list)
    for t, a, b in conn.execute("SELECT ticker, start_ts, end_ts FROM windows"):
        wins[t].append((a, b))
    candles = {}
    if near_best_only:
        for t, e, bh, al in conn.execute("SELECT ticker, end_ts, bid_high, ask_low FROM candles"):
            candles[(t, e)] = (bh, al)
    stats = defaultdict(int)
    for tid, t, ts, yr, nr, cr, side in conn.execute(
            "SELECT trade_id, ticker, ts, yes_raw, no_raw, count_raw, taker_side FROM hist_trades"):
        if t not in res or not any(a <= ts <= b for a, b in wins[t]):
            stats["outside_window"] += 1
            continue
        result, close, cat = res[t]
        yes_px, no_px, n = float(yr), float(nr), float(cr)
        if side == "no":
            maker, px = "yes", yes_px
        elif side == "yes":
            maker, px = "no", no_px
        else:
            stats["bad_side"] += 1
            continue
        if not (0 < px < 1) or n <= 0:
            stats["bad_px"] += 1
            continue
        if near_best_only:
            c = candles.get((t, ts - ts % 60 + 60))
            if not c or c[0] in (None, "") or c[1] in (None, ""):
                stats["no_candle"] += 1
                continue
            bid_high, ask_low = float(c[0]), float(c[1])
            near = (px >= bid_high - 0.02) if maker == "yes" else (px >= (1 - ask_low) - 0.02)
            if not near:
                stats["not_near_best"] += 1
                continue
        payoff = (1.0 if result == maker else 0.0) - px
        ttc = (parse_iso(close) - ts) if close else 1e12
        yield {"ticker": t, "cat": cat, "px": px, "n": n, "payoff": payoff, "won": result == maker,
               "ttc": ttc, "ts": ts}
    load_fills.stats = dict(stats)


def summarize(rows):
    cap = sum(r["px"] * r["n"] for r in rows)
    pay = sum(r["payoff"] * r["n"] for r in rows)
    contracts = sum(r["n"] for r in rows)
    wins = sum(r["n"] for r in rows if r["won"])
    return cap, pay, contracts, wins


def cluster_ci(rows, key_fn=None, reps=400, seed=11):
    """95% CI of return-on-capital, bootstrapping MARKETS (trades in one market share an outcome)."""
    by_t = defaultdict(lambda: [0.0, 0.0])
    for r in rows:
        by_t[r["ticker"]][0] += r["px"] * r["n"]
        by_t[r["ticker"]][1] += r["payoff"] * r["n"]
    vals = list(by_t.values())
    if len(vals) < 5:
        return None
    rng, out = random.Random(seed), []
    for _ in range(reps):
        s = [vals[rng.randrange(len(vals))] for _ in vals]
        c = sum(x[0] for x in s)
        if c:
            out.append(sum(x[1] for x in s) / c)
    out.sort()
    return out[int(0.025 * len(out))], out[int(0.975 * len(out))]


def table(title, groups):
    print(f"\n{title}")
    print(f"  {'group':22s} {'fills':>7s} {'contracts':>10s} {'avg px':>7s} {'win%':>6s} {'c/contract':>10s} "
          f"{'return on capital':>18s}  95% CI (by market)")
    for name, rows in groups:
        if not rows:
            continue
        cap, pay, k, w = summarize(rows)
        ci = cluster_ci(rows)
        ci_s = f"[{ci[0]:+.1%}, {ci[1]:+.1%}]" if ci else "n/a"
        print(f"  {name:22s} {len(rows):7d} {k:10.0f} {cap / k:7.3f} {w / k:6.1%} {pay / k * 100:+10.2f} "
              f"{pay / cap:+18.1%}  {ci_s}")


def cmd_analyze(args) -> None:
    conn = db()
    n_mk = conn.execute("SELECT COUNT(*) FROM sample_markets").fetchone()[0]
    trunc = conn.execute("SELECT SUM(trades_truncated) FROM sample_markets").fetchone()[0] or 0
    print(f"{n_mk} settled non-sports markets with a past liquidity program ({trunc} had trades truncated by the page cap).")
    cats = defaultdict(int)
    for (c,) in conn.execute("SELECT category FROM sample_markets"):
        cats[c] += 1
    print("category mix:", dict(sorted(cats.items(), key=lambda x: -x[1])))

    for near in (False, True):
        if near and not conn.execute("SELECT 1 FROM candles LIMIT 1").fetchone():
            print("\n(no candles downloaded; run `candles` for the near-best-bid cut)")
            continue
        rows = list(load_fills(conn, near))
        label = "FILLS NEAR THE BEST BID (candle-filtered)" if near else "ALL MAKER FILLS INSIDE PROGRAM WINDOWS"
        print(f"\n{'=' * 100}\n{label}   skipped: {getattr(load_fills, 'stats', {})}")
        if not rows:
            continue
        table("Overall", [("all", rows)])
        table("By price the maker paid (the side the maker holds)",
              [(f"{lo:.2f}-{min(hi, 1):.2f}", [r for r in rows if lo <= r["px"] < hi]) for lo, hi in PRICE_BUCKETS])
        table("By time from fill to close", [(nm, [r for r in rows if lo <= r["ttc"] < hi]) for lo, hi, nm in TTC_BUCKETS])
        table("By category", [(c, [r for r in rows if r["cat"] == c])
                              for c in sorted({r["cat"] for r in rows}, key=lambda c: -sum(1 for r in rows if r["cat"] == c))])
        if not near:
            per_day = defaultdict(float)
            for r in rows:
                per_day[(r["ticker"], r["ts"] // 86400)] += r["n"]
            v = sorted(per_day.values())
            print(f"\n  Contracts traded per market-day inside programs: median {statistics.median(v):.0f}, "
                  f"p90 {v[int(0.9 * len(v))]:.0f}, max {v[-1]:.0f}  (caps how many contracts a quote can be filled for per day)")

    print("\nBenchmark (Burgi/Deng/Whelan, different period): makers ~-9.6% overall pre-fee, ~+2.6% at >=50c, "
          "5c contracts win ~2%.")
    print("REMINDER: loss per FILLED contract only. Fill rate and rewards are not modelled here.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["fetch", "candles", "analyze"])
    ap.add_argument("--db", default="adverse.db", help="database file under data/")
    ap.add_argument("--min-period-days", type=float, default=0.0,
                    help="only sample markets whose program period is at least this long (watchlist uses 1.0)")
    ap.add_argument("--n", type=int, default=500, help="markets to sample")
    ap.add_argument("--program-pages", type=int, default=12)
    ap.add_argument("--max-pages", type=int, default=30, help="trade pages per market")
    ap.add_argument("--req-per-sec", type=float, default=12.0)
    args = ap.parse_args()
    global DB_PATH
    DB_PATH = ROOT / "data" / args.db
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.cmd == "fetch":
        asyncio.run(cmd_fetch(args))
    elif args.cmd == "candles":
        asyncio.run(cmd_candles(args))
    else:
        cmd_analyze(args)


if __name__ == "__main__":
    main()
