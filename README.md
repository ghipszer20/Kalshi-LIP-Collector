# Kalshi LIP collector

Phase 1 of a study of Kalshi's Liquidity Incentive Program (LIP): can resting limit orders
in quiet **non-sports** markets earn more in rewards than they lose to adverse selection
and fees? This repo currently holds only the **measurement tool**. It is read-only, uses
public unauthenticated endpoints, and places nothing.

Phases: 1 collector (this repo) -> 2 simulator -> 3 live at tiny size -> 4 scale. The gate
out of phase 1 is at least 7 days of data covering at least 100 markets. Do not skip to
phase 3.

## Run

```
python -m venv .venv && .venv\Scripts\pip install -r requirements.txt
python collector.py            # loops forever, one poll per minute
python collector.py --once     # single cycle (testing)
python report.py               # reward-share estimate per market, latest snapshot
powershell -File schedule_collector.ps1   # keep it running via Task Scheduler
```

## Status (2026-09-24)

Phase 1 is running: the collector was started on 2026-09-24 and is kept alive by a Windows
scheduled task. Watching 300 non-sports markets (about $25k/day of reward pool between them).
A steady-state cycle takes about 26 seconds and issues roughly 10 trade requests (volume gating
skips markets where nothing traded); no rate-limit responses so far. The Phase 1 gate (at least
7 days and 100 markets) has not been reached, and Phase 2 (the simulator) has not been started.
`report.py` output is an upper bound, not a profit estimate.

## Historical data: what exists (checked 2026-09-24)

- **Kalshi API:** `/historical/markets` and `/historical/trades` work unauthenticated (verified
  live; `/historical/cutoff` reports the live/historical boundary at 2026-07-25). Candlesticks
  carry bid/ask OHLC without sizes. Third-party guides say there is no historical order-book
  depth endpoint; I did not find one, but did not confirm that against Kalshi's own docs.
- **Past incentive programs:** `GET /incentive_programs?status=paid_out` returns ended programs
  with their parameters (40,000+ back to at least 2026-09-11 in a capped pull). Unknown status
  values are silently ignored and behave like `all`.
- **Third party:** the PMXT Archive (archive.pmxt.dev) advertises free tick-level historical
  order books for Kalshi as Parquet files, and pmxt's docs say its API serves historical Kalshi
  books too. The archive host refused connections from this machine on 2026-09-24, so coverage,
  dates and whether it includes the LIP-era markets are **unverified**. Commercial vendors also
  exist (Lychee, Convex Lake, Predexon, DepthFeed); not evaluated.
- **GitHub:** only small collectors like this one; no ready-made archive found.

Implication: historical trades plus past programs can already support the adverse-selection half
of a simulation. The reward-share half needs book depth, which for now comes only from this
collector (or a verified archive).

## First result: adverse selection from historical trades (2026-09-24)

`adverse_selection.py` measures the settlement payoff of every maker fill, using only public
historical data (past programs, trades, results, 1-minute bid/ask candles). Sample: 325 settled
non-sports markets whose program lasted at least a day (programs from 2026-09-11 to 2026-09-24),
128,781 maker fills inside program windows. **Loss per FILLED contract only, pre-fee.** It does not
model how often a quote fills or what it earns in rewards.

Fills near the best bid (candle-filtered, closest to what earns LIP rewards):

| Price the maker paid | Contracts | Return on capital (95% CI, resampling markets) | Cents per contract |
|---|---|---|---|
| under 5c | 888k | -51% (-97% to +22%) | -0.83 |
| 5-10c | 149k | -55% (-99% to +11%) | -3.52 |
| 10-30c | 208k | -26% (-73% to +25%) | -4.58 |
| 30-50c | 178k | -2% (-37% to +33%) | -0.85 |
| 50-70c | 196k | -1.5% (-23% to +18%) | -0.90 |
| 70-90c | 272k | +7.7% (-1% to +16%) | +6.26 |
| 90c and over | 1.47M | +1.8% (+0.2% to +2.8%) | +1.74 |
| **All** | 3.37M | **+0.9% (-0.6% to +2.3%)** | +0.51 |

- On average, being filled as a maker in these markets is roughly break-even, not a big loser.
  The loss sits in cheap contracts (under 30c); favourites earn a little. That is the
  favourite-longshot bias, and it points the same way as the published maker results (makers
  positive at 50c and above; longshots winning less often than their price).
- By category: Climate and Weather (88% of fills) +2.9%; Politics -6.1%; Economics -8.4%;
  Science and Technology (few, very cheap markets) -78%.
- Contracts traded per market-day inside programs: median 1,241, p90 18,830.
- The cheap side is also where the reward estimate per dollar is highest, so the two effects
  offset; whether the reward wins depends on fill rate, which needs book depth (the collector).
- Confidence is low for the cheap buckets: the intervals are very wide because a handful of markets
  carry most of the contracts. The sample covers about two weeks and is dominated by weather markets.
- An earlier run that forgot the "program at least one day" filter (65% of fills within an hour of
  close, half the markets Crypto or Commodities) showed the same pattern.

```
python adverse_selection.py fetch   --db adverse_long.db --min-period-days 1 --n 500 --req-per-sec 6
python adverse_selection.py candles --db adverse_long.db --req-per-sec 6
python adverse_selection.py analyze --db adverse_long.db
```

## What was verified against the live API (2026-09-24)

- **6,276** live liquidity programs across 539 series, not the ~480 the original handoff
  assumed. Pools sum to roughly $250k/day; median $23/day per market.
- The API tolerated a sustained **30 req/s** but returned 429s that lingered for seconds after
  a ~270 req/s burst, hence the paced limiter (default 12 req/s) with exponential back-off.
- Sports exclusion resolves through the **series** endpoint (`category`), where the only sports
  string is exactly `"Sports"` (26 series). Unresolved category is treated as excluded (fail
  closed), plus a ticker-prefix blocklist as a second layer.
- Volume / open interest / bid / ask populate on the batch `GET /markets?tickers=` endpoint
  (300 tickers per call worked when paced); field names end `_fp` / `_dollars`.
- All 539 series have `fee_multiplier` 1. Only 4 have `fee_type: quadratic_with_maker_fees`; the
  other 535 are plain `quadratic`. This *suggests* most series charge no maker fee, but that is
  inferred from a field name and not yet confirmed against the fee schedule.

## Not verified

- Whether the reward pool splits 50/50 between YES and NO (report/simulator take a parameter).
- The reference price when total depth never reaches `target/5` (`best` vs `worst`; both are
  supported in `lip_scoring.py`).
- Maker-fee applicability (see above) and the fee rounding rule.
- `min_ts` on `/markets/trades` is assumed to be unix seconds; duplicates are harmless because
  `trade_id` is the primary key.

## Watchlist

Not every program can be polled. Candidates: live program, period >= 1 day, series resolved and
not Sports. Ranked by the estimated gross reward of a 100-contract cheap-side bid at the
reference price (an upper bound, used only for ranking); top 300 are polled. Re-selected every
6 hours. Markets that leave the watchlist keep being polled for status until a result is
recorded, so settlement values are never lost.

## Schema (data/lip.db) and why

Chosen from what the Phase 2 simulator needs:

| Table | Purpose |
|---|---|
| `programs` | versioned program parameters (reward, target size, discount, dates) |
| `series` | category, fee type and multiplier per series |
| `watchlist` | what was tracked and when, with the estimate used to select it |
| `book_states` + `book_polls` | top 40 levels per side, raw price/qty strings; identical books stored once, every poll logged so the book at any time can be replayed |
| `market_polls` | status, result, volume, open interest, best bid/ask over time (markout, settlement value) |
| `trades` | every trade with `trade_id`, raw timestamp, prices and count strings, taker side (queue-position and fill simulation) |
| `cycle_log` | per-cycle duration, request counts, 429s, errors (health) |

Raw strings are stored beside anything parsed, so a rounding problem can always be traced to the
feed. `book_states.truncated` marks books with more than 40 levels on a side.

Known limits: cancellations are not observable, so queue position in the simulator can only be
reduced by trades (conservative). Snapshots are one per minute, so reference-price dynamics
between polls are invisible.

## Legal / scope

Non-sports markets only. Kalshi LIP is for US members and the program is scheduled to end
2027-01-01 unless extended.
