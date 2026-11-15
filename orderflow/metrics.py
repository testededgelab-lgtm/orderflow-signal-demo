"""Rolling order-flow metrics over a stream of aggressive trades.

Every trade is (ts_ms, price, qty, side) where side is "buy" when the taker
bought (lifted the ask) and "sell" when the taker sold (hit the bid).
Volumes are measured in quote currency (price * qty, e.g. USDT).
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class Trade:
    ts_ms: int
    price: float
    qty: float
    side: str  # "buy" or "sell" (taker side)

    @property
    def notional(self) -> float:
        return self.price * self.qty


@dataclass(frozen=True)
class Snapshot:
    """Metrics for one symbol at one moment."""
    ts_ms: int
    price: float
    buy_vol: float          # aggressive buy volume in the short window
    sell_vol: float         # aggressive sell volume in the short window
    delta: float            # buy_vol - sell_vol
    imbalance: float        # delta / (buy_vol + sell_vol), from -1 to +1
    activity: float         # short-window volume rate / baseline volume rate
    price_change: float     # last price / first price in the short window - 1
    trades: int             # trades in the short window
    warm: bool              # True once the baseline window is full


class _Window:
    """Trades inside the last `seconds`, with running buy/sell sums."""

    def __init__(self, seconds: float):
        self.span_ms = int(seconds * 1000)
        self.items: deque[Trade] = deque()
        self.buy = 0.0
        self.sell = 0.0

    def add(self, t: Trade) -> None:
        self.items.append(t)
        if t.side == "buy":
            self.buy += t.notional
        else:
            self.sell += t.notional

    def evict(self, now_ms: int) -> None:
        cutoff = now_ms - self.span_ms
        while self.items and self.items[0].ts_ms <= cutoff:
            old = self.items.popleft()
            if old.side == "buy":
                self.buy -= old.notional
            else:
                self.sell -= old.notional
        if not self.items:          # clear float drift when empty
            self.buy = self.sell = 0.0

    @property
    def volume(self) -> float:
        return max(self.buy, 0.0) + max(self.sell, 0.0)


class OrderFlow:
    """Short window (the impulse) measured against a longer baseline window."""

    def __init__(self, short_seconds: float = 30, baseline_seconds: float = 900):
        if baseline_seconds <= short_seconds:
            raise ValueError("baseline_seconds must be longer than short_seconds")
        self.short = _Window(short_seconds)
        self.base = _Window(baseline_seconds)
        self.short_s = short_seconds
        self.base_s = baseline_seconds
        self.first_ts: int | None = None

    def update(self, t: Trade) -> Snapshot:
        if self.first_ts is None:
            self.first_ts = t.ts_ms
        self.short.add(t)
        self.base.add(t)
        self.short.evict(t.ts_ms)
        self.base.evict(t.ts_ms)
        return self.snapshot(t.ts_ms, t.price)

    def snapshot(self, now_ms: int, price: float) -> Snapshot:
        buy, sell = max(self.short.buy, 0.0), max(self.short.sell, 0.0)
        total = buy + sell
        base_rate = self.base.volume / self.base_s
        short_rate = total / self.short_s
        first = self.short.items[0].price if self.short.items else price
        return Snapshot(
            ts_ms=now_ms,
            price=price,
            buy_vol=buy,
            sell_vol=sell,
            delta=buy - sell,
            imbalance=(buy - sell) / total if total else 0.0,
            activity=short_rate / base_rate if base_rate else 0.0,
            price_change=price / first - 1 if first else 0.0,
            trades=len(self.short.items),
            warm=self.first_ts is not None and now_ms - self.first_ts >= self.base.span_ms,
        )
