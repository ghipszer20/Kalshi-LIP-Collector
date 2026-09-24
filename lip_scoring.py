"""
lip_scoring.py — Kalshi Liquidity Incentive Program reward-share scoring.

Implements the scoring rules from Kalshi's help article as summarised in HANDOFF.md
(section 3). Kept separate from the collector so the Phase 2 simulator reuses exactly
the same code.

Rules (per side of the book, per snapshot):
  - Each resting bid scores  size * multiplier(price).
  - Reference price = first price level, walking from the best bid, where cumulative
    resting size >= target_size / 5.
  - multiplier = 1.0 at or better than the reference price, otherwise
    discount ** ticks_away  (discount = discount_factor_bps / 10000).
  - Your share of a side = your score / total score on that side (your own order
    is included in the book when the reference price is computed).

Assumptions that are NOT confirmed by Kalshi and are exposed as parameters:
  - thin_book_ref: what the reference price is when total depth never reaches
    target/5. 'best' follows the handoff's sketch; 'worst' treats every level as
    qualifying. Unknown which is right.
  - side_split: fraction of the period reward attributed to each side (0.5 assumed).
"""

from __future__ import annotations

Level = tuple[float, float]  # (price in dollars, quantity in contracts)


def sort_bids(levels: list[Level]) -> list[Level]:
    return sorted(levels, key=lambda x: -x[0])


def reference_price(bids: list[Level], target: float, thin_book_ref: str = "best") -> float | None:
    bids = sort_bids(bids)
    if not bids:
        return None
    cum = 0.0
    for price, qty in bids:
        cum += qty
        if cum >= target / 5:
            return price
    return bids[0][0] if thin_book_ref == "best" else bids[-1][0]


def multiplier(price: float, ref: float, discount: float, tick: float = 0.01) -> float:
    ticks_away = max(0, round((ref - price) / tick))
    return discount ** ticks_away


def side_share(
    bids: list[Level],
    target: float,
    discount: float,
    my_price: float | None = None,
    my_qty: float = 0.0,
    tick: float = 0.01,
    thin_book_ref: str = "best",
) -> dict:
    """Score one side. If my_price is given, my order is added to the book first
    (it can move the reference price). Returns ref, total score, my score, share."""
    book = list(bids)
    if my_price is not None and my_qty > 0:
        book.append((my_price, my_qty))
    ref = reference_price(book, target, thin_book_ref)
    if ref is None:
        return {"ref": None, "total": 0.0, "mine": 0.0, "share": 0.0}
    total = sum(q * multiplier(p, ref, discount, tick) for p, q in book)
    mine = my_qty * multiplier(my_price, ref, discount, tick) if my_price is not None else 0.0
    return {"ref": ref, "total": total, "mine": mine, "share": (mine / total) if total else 0.0}


def estimate_reward(
    bids: list[Level],
    daily_pool: float,
    target: float,
    discount: float,
    qty: float,
    side_split: float = 0.5,
    tick: float = 0.01,
    thin_book_ref: str = "best",
) -> dict:
    """Gross daily reward for resting `qty` contracts at the reference price of this
    side (the price a quote must reach for full score, computed WITHOUT our order),
    plus capital tied up. Upper bound: ignores fills, adverse selection, competitors
    who re-quote, and fees."""
    base_ref = reference_price(bids, target, thin_book_ref)
    if base_ref is None:
        return {"price": None, "share": 0.0, "gross_per_day": 0.0, "capital": 0.0, "per_100_capital": 0.0}
    s = side_share(bids, target, discount, base_ref, qty, tick, thin_book_ref)
    gross = daily_pool * side_split * s["share"]
    capital = base_ref * qty
    return {
        "price": base_ref,
        "share": s["share"],
        "gross_per_day": gross,
        "capital": capital,
        "per_100_capital": (gross / capital * 100) if capital else 0.0,
    }
