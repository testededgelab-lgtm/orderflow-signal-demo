"""Turn order-flow snapshots into LONG / SHORT signals with configurable filters."""
from __future__ import annotations

from dataclasses import dataclass

from .metrics import Snapshot


@dataclass(frozen=True)
class Filters:
    min_imbalance: float = 0.35       # |delta| / volume in the short window
    min_activity: float = 3.0         # short-window volume rate vs baseline rate
    min_price_change: float = 0.0015  # 0.15% move in the short window, in the signal's direction
    min_trades: int = 50              # ignore thin windows
    min_volume: float = 250_000       # short-window volume in quote currency (USDT)
    cooldown_seconds: float = 120     # one impulse -> one signal


@dataclass(frozen=True)
class Signal:
    symbol: str
    side: str            # "LONG" or "SHORT"
    snap: Snapshot

    def text(self) -> str:
        s = self.snap
        arrow = "🟢 LONG" if self.side == "LONG" else "🔴 SHORT"
        return (f"{arrow} {self.symbol} @ {s.price:,.4g}\n"
                f"imbalance {s.imbalance:+.0%} · activity {s.activity:.1f}x · "
                f"move {s.price_change:+.2%} · delta {s.delta/1e3:+,.0f}k · {s.trades} trades")


class SignalEngine:
    def __init__(self, filters: Filters):
        self.f = filters
        self.last_signal_ms: dict[str, int] = {}

    def check(self, symbol: str, s: Snapshot) -> Signal | None:
        f = self.f
        if not s.warm or s.trades < f.min_trades or (s.buy_vol + s.sell_vol) < f.min_volume:
            return None
        if s.activity < f.min_activity:
            return None
        last = self.last_signal_ms.get(symbol)
        if last is not None and s.ts_ms - last < f.cooldown_seconds * 1000:
            return None
        side = None
        if s.imbalance >= f.min_imbalance and s.price_change >= f.min_price_change:
            side = "LONG"
        elif s.imbalance <= -f.min_imbalance and s.price_change <= -f.min_price_change:
            side = "SHORT"
        if side is None:
            return None
        self.last_signal_ms[symbol] = s.ts_ms
        return Signal(symbol, side, s)
