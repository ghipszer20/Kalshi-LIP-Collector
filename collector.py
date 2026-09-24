"""
collector.py — Phase 1 of the Kalshi Liquidity Incentive Program (LIP) project.

Records, for a prioritised watchlist of incentivised NON-SPORTS markets, everything the
Phase 2 simulator needs to replay hypothetical quotes: program parameters, order-book
snapshots, trades, market status and settlement results. READ-ONLY: only public,
unauthenticated GET endpoints; nothing is ever placed.

Why a watchlist: Kalshi has thousands of live programs (6,276 on 2026-09-24, not the
~480 the handoff assumed). The public API tolerated a sustained 30 req/s but returned
429s (lingering for seconds) on a ~270 req/s burst, so requests are paced by a limiter
with exponential back-off. Default budget is 12 req/s.

Watchlist selection (re-run every few hours):
  eligible = live program, period >= 1 day, series category resolved and not Sports,
             ticker prefix not on the sports/combo blocklist (unknown category => excluded)
  rank     = estimated gross reward/day of a 100-contract cheap-side bid at the reference
             price, from the live book (lip_scoring.py) — an upper bound used only to rank.

Storage (SQLite, WAL; data/lip.db). Raw API strings are kept next to anything parsed so a
rounding bug in the simulator can always be traced back to the feed:
  series, programs (versioned), watchlist, book_states + book_polls (identical books stored
  once, every poll logged), market_polls, trades, cycle_log, meta.

Run:  python collector.py            # loop forever (one instance; heartbeat-guarded)
      python collector.py --once     # single cycle, for testing
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aiohttp

import lip_scoring as lip

BASE = "https://api.elections.kalshi.com/trade-api/v2"
ROOT = Path(__file__).parent
DATA = ROOT / "data"
DB_PATH = DATA / "lip.db"

SPORTS_CATEGORIES = {"Sports"}
# Second layer behind the series category (defence in depth for the Maryland restriction).
SPORTS_PREFIXES = ("KXNFL", "KXNBA", "KXMLB", "KXNHL", "KXNCAA", "KXMVE", "KXTTELITE",
                   "KXUFC", "KXSOCCER", "KXMLS", "KXEPL", "KXATP", "KXWTA", "KXPGA", "KXF1")
SETTLED_STATUSES = {"settled", "finalized"}

log = logging.getLogger("collector")


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_iso(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


# --------------------------------------------------------------------------- database

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS series (
    series_ticker TEXT PRIMARY KEY, category TEXT, categories_json TEXT, fee_type TEXT,
    fee_multiplier REAL, frequency TEXT, fetched_ms INTEGER);
CREATE TABLE IF NOT EXISTS programs (
    id TEXT, market_ticker TEXT, start_date TEXT, end_date TEXT, period_reward_raw INTEGER,
    target_size_raw TEXT, discount_bps INTEGER, paid_out INTEGER,
    first_seen_ms INTEGER, last_seen_ms INTEGER,
    PRIMARY KEY (id, period_reward_raw, target_size_raw, discount_bps, end_date));
CREATE INDEX IF NOT EXISTS ix_programs_ticker ON programs(market_ticker, last_seen_ms);
CREATE TABLE IF NOT EXISTS watchlist (
    ticker TEXT PRIMARY KEY, added_ms INTEGER, removed_ms INTEGER,
    daily_pool REAL, est_gross_100 REAL);
CREATE TABLE IF NOT EXISTS book_states (
    hash TEXT PRIMARY KEY, yes_json TEXT, no_json TEXT, yes_levels_total INTEGER,
    no_levels_total INTEGER, truncated INTEGER);
CREATE TABLE IF NOT EXISTS book_polls (
    ts_ms INTEGER, ticker TEXT, hash TEXT, req_ms INTEGER, latency_ms INTEGER);
CREATE INDEX IF NOT EXISTS ix_book_polls ON book_polls(ticker, ts_ms);
CREATE TABLE IF NOT EXISTS market_polls (
    ts_ms INTEGER, ticker TEXT, status TEXT, result TEXT, volume_raw TEXT, volume_24h_raw TEXT,
    open_interest_raw TEXT, yes_bid_raw TEXT, yes_ask_raw TEXT, no_bid_raw TEXT, no_ask_raw TEXT,
    close_time TEXT, price_level_structure TEXT);
CREATE INDEX IF NOT EXISTS ix_market_polls ON market_polls(ticker, ts_ms);
CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT PRIMARY KEY, ticker TEXT, created_raw TEXT, ts_ms INTEGER, yes_price_raw TEXT,
    no_price_raw TEXT, count_raw TEXT, taker_side TEXT, is_block INTEGER, recv_ms INTEGER);
CREATE INDEX IF NOT EXISTS ix_trades ON trades(ticker, ts_ms);
CREATE TABLE IF NOT EXISTS cycle_log (
    ts_ms INTEGER, duration_ms INTEGER, watchlist_n INTEGER, books INTEGER, trade_reqs INTEGER,
    new_trades INTEGER, n429 INTEGER, errors INTEGER);
"""


def open_db() -> sqlite3.Connection:
    DATA.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    return conn


def meta_get(conn, key, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def meta_set(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, str(value)))


# --------------------------------------------------------------------------- HTTP

class Api:
    """Paced, back-off-aware GET client. One shared limiter for every request."""

    def __init__(self, session: aiohttp.ClientSession, rate: float):
        self.session = session
        self.gap = 1.0 / rate
        self.next_slot = 0.0
        self.backoff_until = 0.0
        self.lock = asyncio.Lock()
        self.n429 = 0
        self.errors = 0

    async def _slot(self):
        async with self.lock:
            now = time.time()
            wait = max(self.next_slot - now, self.backoff_until - now, 0.0)
            self.next_slot = max(now, self.next_slot, self.backoff_until) + self.gap
        if wait:
            await asyncio.sleep(wait)

    async def get(self, path: str, params: dict | None = None):
        for attempt in range(6):
            await self._slot()
            try:
                t0 = time.time()
                async with self.session.get(BASE + path, params=params) as r:
                    if r.status == 200:
                        return await r.json(), int((time.time() - t0) * 1000)
                    if r.status == 429:
                        self.n429 += 1
                        self.backoff_until = time.time() + min(2 ** attempt, 60)
                        log.warning("429 on %s (attempt %d), backing off", path, attempt + 1)
                        continue
                    self.errors += 1
                    log.warning("HTTP %s on %s", r.status, path)
                    return None, 0
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                self.errors += 1
                log.warning("request error on %s: %s", path, e)
                await asyncio.sleep(min(2 ** attempt, 30))
        return None, 0


# --------------------------------------------------------------------------- programs / series

def series_of(ticker: str) -> str:
    return ticker.split("-")[0]


async def refresh_programs(api: Api, conn: sqlite3.Connection) -> int:
    seen, cursor, ts = 0, None, now_ms()
    while True:
        params = {"status": "active", "type": "liquidity", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        data, _ = await api.get("/incentive_programs", params)
        if not data:
            break
        rows = data.get("incentive_programs", [])
        for p in rows:
            key = (p["id"], p["period_reward"], p["target_size_fp"], p["discount_factor_bps"], p["end_date"])
            cur = conn.execute(
                "UPDATE programs SET last_seen_ms=?, paid_out=? WHERE id=? AND period_reward_raw=? "
                "AND target_size_raw=? AND discount_bps=? AND end_date=?",
                (ts, int(bool(p.get("paid_out"))), *key))
            if cur.rowcount == 0:
                conn.execute(
                    "INSERT INTO programs VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (p["id"], p["market_ticker"], p["start_date"], p["end_date"], p["period_reward"],
                     p["target_size_fp"], p["discount_factor_bps"], int(bool(p.get("paid_out"))), ts, ts))
        seen += len(rows)
        cursor = data.get("next_cursor")
        if not cursor or not rows:
            break
    conn.commit()
    return seen


async def refresh_series(api: Api, conn: sqlite3.Connection, tickers: set[str]) -> None:
    stale_before = now_ms() - 24 * 3600 * 1000
    have = {r[0] for r in conn.execute("SELECT series_ticker FROM series WHERE fetched_ms>?", (stale_before,))}
    for sr in sorted({series_of(t) for t in tickers} - have):
        data, _ = await api.get(f"/series/{sr}")
        if not data or "series" not in data:
            continue  # leave unresolved: unknown category is treated as excluded
        s = data["series"]
        conn.execute(
            "INSERT OR REPLACE INTO series VALUES (?,?,?,?,?,?,?)",
            (sr, s.get("category"), json.dumps(s.get("categories")), s.get("fee_type"),
             s.get("fee_multiplier"), s.get("frequency"), now_ms()))
    conn.commit()


def is_eligible_series(conn, ticker: str) -> bool:
    sr = series_of(ticker)
    if sr.startswith(SPORTS_PREFIXES):
        return False
    row = conn.execute("SELECT category, categories_json FROM series WHERE series_ticker=?", (sr,)).fetchone()
    if not row or not row[0]:
        return False  # unresolved category: fail closed
    cats = set(json.loads(row[1] or "[]")) | {row[0]}
    return not (cats & SPORTS_CATEGORIES)


def live_candidates(conn, min_period_days: float) -> list[dict]:
    now = time.time()
    out = {}
    for tk, start, end, reward, target, disc in conn.execute(
            "SELECT market_ticker,start_date,end_date,period_reward_raw,target_size_raw,discount_bps "
            "FROM programs WHERE last_seen_ms > ?", (now_ms() - 3 * 3600 * 1000,)):
        s, e = parse_iso(start), parse_iso(end)
        days = (e - s) / 86400
        if not (s <= now <= e) or days < min_period_days or not is_eligible_series(conn, tk):
            continue
        cand = {"ticker": tk, "daily_pool": reward / 10000 / days, "target": float(target), "discount": disc / 10000}
        if tk not in out or cand["daily_pool"] > out[tk]["daily_pool"]:
            out[tk] = cand
    return sorted(out.values(), key=lambda c: -c["daily_pool"])


# --------------------------------------------------------------------------- books

def parse_book(payload: dict, k: int):
    ob = payload.get("orderbook_fp") or payload.get("orderbook") or {}
    sides = {}
    for name in ("yes_dollars", "no_dollars"):
        raw = ob.get(name) or []
        raw = sorted(raw, key=lambda lv: -float(lv[0]))  # best bid first
        sides[name] = (raw[:k], len(raw))
    return sides


def book_hash(yes, no) -> str:
    return hashlib.sha1((json.dumps(yes) + "|" + json.dumps(no)).encode()).hexdigest()[:16]


async def fetch_book(api: Api, ticker: str, k: int):
    data, latency = await api.get(f"/markets/{ticker}/orderbook")
    if not data:
        return None
    return parse_book(data, k), latency, now_ms()


def cheap_side_estimate(book_sides, cand) -> float:
    best = 0.0
    for name in ("yes_dollars", "no_dollars"):
        levels = [(float(p), float(q)) for p, q in book_sides[name][0]]
        est = lip.estimate_reward(levels, cand["daily_pool"], cand["target"], cand["discount"], qty=100)
        best = max(best, est["gross_per_day"])
    return best


async def select_watchlist(api: Api, conn: sqlite3.Connection, args) -> list[dict]:
    cands = live_candidates(conn, args.min_period_days)[: args.candidate_pool]
    log.info("selection: %d eligible candidates considered (top by daily pool)", len(cands))
    scored = []

    async def one(c):
        r = await fetch_book(api, c["ticker"], args.book_levels)
        if r:
            c["est_gross_100"] = cheap_side_estimate(r[0], c)
            scored.append(c)

    await asyncio.gather(*[one(c) for c in cands])
    scored.sort(key=lambda c: -c["est_gross_100"])
    chosen = scored[: args.watchlist_size]
    ts = now_ms()
    chosen_set = {c["ticker"] for c in chosen}
    conn.execute("UPDATE watchlist SET removed_ms=? WHERE removed_ms IS NULL AND ticker NOT IN (%s)"
                 % ",".join("?" * len(chosen_set)), (ts, *chosen_set))
    for c in chosen:
        conn.execute("INSERT INTO watchlist VALUES (?,?,NULL,?,?) ON CONFLICT(ticker) DO UPDATE SET "
                     "removed_ms=NULL, daily_pool=excluded.daily_pool, est_gross_100=excluded.est_gross_100",
                     (c["ticker"], ts, c["daily_pool"], c["est_gross_100"]))
    meta_set(conn, "last_reselect_ms", ts)
    conn.commit()
    log.info("watchlist: %d markets, total pool $%.0f/day, median est gross/day @100 contracts $%.2f",
             len(chosen), sum(c["daily_pool"] for c in chosen),
             sorted(c["est_gross_100"] for c in chosen)[len(chosen) // 2] if chosen else 0)
    return chosen


# --------------------------------------------------------------------------- polling cycle

def tracked_tickers(conn) -> tuple[list[str], list[str]]:
    active = [r[0] for r in conn.execute("SELECT ticker FROM watchlist WHERE removed_ms IS NULL")]
    # Removed markets stay tracked (status only) until a result is recorded, so the
    # simulator always has the settlement value.
    pending = [r[0] for r in conn.execute(
        "SELECT w.ticker FROM watchlist w WHERE w.removed_ms IS NOT NULL AND COALESCE("
        "(SELECT result FROM market_polls m WHERE m.ticker=w.ticker ORDER BY ts_ms DESC LIMIT 1),'')=''")]
    return active, pending


async def poll_cycle(api: Api, conn: sqlite3.Connection, args, vol_state: dict) -> dict:
    t0, ts = time.time(), now_ms()
    active, pending = tracked_tickers(conn)
    all_tickers = active + pending
    counts = {"books": 0, "trade_reqs": 0, "new_trades": 0}

    # 1. batch market status / volume (cheap: ~200 tickers per request)
    status = {}
    for i in range(0, len(all_tickers), 200):
        chunk = all_tickers[i:i + 200]
        data, _ = await api.get("/markets", {"tickers": ",".join(chunk), "limit": 1000})
        for m in (data or {}).get("markets", []):
            status[m["ticker"]] = m
            conn.execute(
                "INSERT INTO market_polls VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ts, m["ticker"], m.get("status"), m.get("result"), m.get("volume_fp"), m.get("volume_24h_fp"),
                 m.get("open_interest_fp"), m.get("yes_bid_dollars"), m.get("yes_ask_dollars"),
                 m.get("no_bid_dollars"), m.get("no_ask_dollars"), m.get("close_time"),
                 m.get("price_level_structure")))

    # 2. order books for active, still-open watchlist markets
    open_tickers = [t for t in active if status.get(t, {}).get("status", "active") not in SETTLED_STATUSES
                    and status.get(t, {}).get("status", "active") != "closed"]

    async def book(t):
        r = await fetch_book(api, t, args.book_levels)
        if not r:
            return
        sides, latency, req_ms = r
        yes, no = sides["yes_dollars"][0], sides["no_dollars"][0]
        h = book_hash(yes, no)
        trunc = int(sides["yes_dollars"][1] > len(yes) or sides["no_dollars"][1] > len(no))
        conn.execute("INSERT OR IGNORE INTO book_states VALUES (?,?,?,?,?,?)",
                     (h, json.dumps(yes), json.dumps(no), sides["yes_dollars"][1], sides["no_dollars"][1], trunc))
        conn.execute("INSERT INTO book_polls VALUES (?,?,?,?,?)", (ts, t, h, req_ms, latency))
        counts["books"] += 1

    await asyncio.gather(*[book(t) for t in open_tickers])

    # 3. trades, only where cumulative volume moved since the last poll
    async def trades(t):
        last = conn.execute("SELECT MAX(ts_ms) FROM trades WHERE ticker=?", (t,)).fetchone()[0]
        params = {"ticker": t, "limit": 1000}
        if last:
            params["min_ts"] = int(last / 1000) - 2
        cursor = None
        for _ in range(5):
            if cursor:
                params["cursor"] = cursor
            data, _ = await api.get("/markets/trades", params)
            counts["trade_reqs"] += 1
            rows = (data or {}).get("trades", [])
            for tr in rows:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO trades VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (tr["trade_id"], tr["ticker"], tr["created_time"], int(parse_iso(tr["created_time"]) * 1000),
                     tr.get("yes_price_dollars"), tr.get("no_price_dollars"), tr.get("count_fp"),
                     tr.get("taker_side"), int(bool(tr.get("is_block_trade"))), ts))
                counts["new_trades"] += cur.rowcount
            cursor = (data or {}).get("cursor")
            if not cursor or len(rows) < 1000:
                break
        else:
            log.warning("trades for %s exceeded 5 pages in one poll (possible gap)", t)

    to_fetch = []
    for t in all_tickers:
        vol = (status.get(t) or {}).get("volume_fp")
        if vol is not None and vol != vol_state.get(t):
            to_fetch.append(t)
        if vol is not None:
            vol_state[t] = vol
    await asyncio.gather(*[trades(t) for t in to_fetch])

    conn.execute("INSERT INTO cycle_log VALUES (?,?,?,?,?,?,?,?)",
                 (ts, int((time.time() - t0) * 1000), len(active), counts["books"], counts["trade_reqs"],
                  counts["new_trades"], api.n429, api.errors))
    meta_set(conn, "heartbeat_ms", now_ms())
    conn.commit()
    log.info("cycle: %d active (+%d pending settle), %d books, %d trade reqs, %d new trades, %.1fs, 429s=%d",
             len(active), len(pending), counts["books"], counts["trade_reqs"], counts["new_trades"],
             time.time() - t0, api.n429)
    return counts


# --------------------------------------------------------------------------- main

async def run(args) -> None:
    conn = open_db()
    hb = meta_get(conn, "heartbeat_ms")
    if hb and now_ms() - int(hb) < 3 * args.poll_seconds * 1000 and not args.force:
        log.error("another collector appears to be running (heartbeat %.0fs ago); exiting",
                  (now_ms() - int(hb)) / 1000)
        return
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        api = Api(session, args.req_per_sec)
        # Seed last-seen volumes from the DB so a restart doesn't re-request trades for
        # every market when nothing has traded since the previous run.
        vol_state: dict = dict(conn.execute(
            "SELECT ticker, volume_raw FROM market_polls WHERE (ticker, ts_ms) IN "
            "(SELECT ticker, MAX(ts_ms) FROM market_polls GROUP BY ticker)"))
        last_programs = 0.0
        while True:
            cycle_start = time.time()
            try:
                meta_set(conn, "heartbeat_ms", now_ms())
                if cycle_start - last_programs > args.program_refresh_s:
                    n = await refresh_programs(api, conn)
                    # Series must be resolved for ALL recently seen tickers, not just eligible
                    # ones: eligibility itself depends on the series category.
                    recent = {r[0] for r in conn.execute(
                        "SELECT DISTINCT market_ticker FROM programs WHERE last_seen_ms > ?",
                        (now_ms() - 3 * 3600 * 1000,))}
                    await refresh_series(api, conn, recent)
                    log.info("programs refreshed: %d rows", n)
                    last_programs = time.time()
                last_sel = int(meta_get(conn, "last_reselect_ms", 0))
                if now_ms() - last_sel > args.reselect_s * 1000 or not tracked_tickers(conn)[0]:
                    await select_watchlist(api, conn, args)
                await poll_cycle(api, conn, args, vol_state)
            except Exception:
                log.exception("cycle failed; continuing")
            if args.once:
                return
            await asyncio.sleep(max(0.0, args.poll_seconds - (time.time() - cycle_start)))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--force", action="store_true", help="ignore the single-instance heartbeat guard")
    ap.add_argument("--watchlist-size", type=int, default=300)
    ap.add_argument("--candidate-pool", type=int, default=800)
    ap.add_argument("--poll-seconds", type=int, default=60)
    ap.add_argument("--req-per-sec", type=float, default=12.0)
    ap.add_argument("--program-refresh-s", type=int, default=1800)
    ap.add_argument("--reselect-s", type=int, default=6 * 3600)
    ap.add_argument("--book-levels", type=int, default=40)
    ap.add_argument("--min-period-days", type=float, default=1.0)
    args = ap.parse_args()

    DATA.mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = RotatingFileHandler(DATA / "collector.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    handlers = [fh]
    if sys.stderr is not None:  # under pythonw.exe there is no console stream
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        handlers.append(sh)
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    try:
        asyncio.run(run(args))
    except BaseException:
        # With no console attached a crash would otherwise vanish without a trace.
        log.exception("collector exiting on unhandled exception")
        raise


if __name__ == "__main__":
    main()
