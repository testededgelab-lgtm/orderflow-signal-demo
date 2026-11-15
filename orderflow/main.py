"""Command line entry point.

  python -m orderflow.main live   --config config.yaml           # live signals (console + Telegram)
  python -m orderflow.main record --config config.yaml --out trades.csv --minutes 60
  python -m orderflow.main replay --config config.yaml --file trades.csv
"""
from __future__ import annotations

import argparse
import asyncio
import bisect
import csv
import logging
import time
from dataclasses import fields
from datetime import datetime, timezone
from pathlib import Path

import yaml

from . import streams
from .metrics import OrderFlow, Trade
from .notify import Telegram
from .signals import Filters, Signal, SignalEngine

log = logging.getLogger("orderflow")


def load_config(path: str) -> dict:
    cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    cfg.setdefault("exchanges", {"bybit": ["BTCUSDT", "ETHUSDT"]})
    cfg.setdefault("windows", {"short_seconds": 30, "baseline_seconds": 900})
    cfg.setdefault("filters", {})
    known = {f.name for f in fields(Filters)}
    unknown = set(cfg["filters"]) - known
    if unknown:
        raise SystemExit(f"Unknown filter setting(s) in {path}: {', '.join(sorted(unknown))}")
    return cfg


def build(cfg: dict):
    w = cfg["windows"]
    flows: dict[str, OrderFlow] = {}

    def flow(key: str) -> OrderFlow:
        if key not in flows:
            flows[key] = OrderFlow(w["short_seconds"], w["baseline_seconds"])
        return flows[key]

    return flow, SignalEngine(Filters(**cfg["filters"]))


def open_streams(cfg: dict):
    makers = {"binance": streams.binance, "bybit": streams.bybit}
    its = []
    for ex, syms in cfg["exchanges"].items():
        if ex not in makers:
            raise SystemExit(f"Unsupported exchange '{ex}'. Use: {', '.join(makers)}")
        its.append(tag(ex, makers[ex](syms)))
    return streams.merge(*its)


async def tag(ex, it):
    async for sym, t in it:
        yield ex, sym, t


def stamp(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------- live ----------
async def run_live(cfg: dict) -> None:
    flow, engine = build(cfg)
    tg = Telegram()
    log.info("Telegram %s", "on" if tg.enabled else "off (set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)")
    log.info("Warming up: signals start after the %ss baseline window fills.", cfg["windows"]["baseline_seconds"])
    async for ex, sym, t in open_streams(cfg):
        key = f"{ex}:{sym}"
        sig = engine.check(key, flow(key).update(t))
        if sig:
            msg = f"{stamp(t.ts_ms)}\n{sig.text()}"
            print(msg, flush=True)
            await tg.send(msg)


# ---------- record ----------
async def run_record(cfg: dict, out: str, minutes: float) -> None:
    end = time.monotonic() + minutes * 60
    n = 0
    with open(out, "w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(["ts_ms", "exchange", "symbol", "price", "qty", "side"])
        async for ex, sym, t in open_streams(cfg):
            wr.writerow([t.ts_ms, ex, sym, t.price, t.qty, t.side])
            n += 1
            if n % 5000 == 0:
                fh.flush()
                log.info("recorded %s trades", f"{n:,}")
            if time.monotonic() >= end:
                break
    log.info("saved %s trades to %s", f"{n:,}", out)


# ---------- replay ----------
def run_replay(cfg: dict, path: str, horizons=(60, 300)) -> None:
    flow, engine = build(cfg)
    prices: dict[str, tuple[list[int], list[float]]] = {}
    signals: list[Signal] = []
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    rows.sort(key=lambda r: int(r["ts_ms"]))
    for r in rows:
        key = f'{r["exchange"]}:{r["symbol"]}'
        t = Trade(int(r["ts_ms"]), float(r["price"]), float(r["qty"]), r["side"])
        ts, px = prices.setdefault(key, ([], []))
        ts.append(t.ts_ms); px.append(t.price)
        sig = engine.check(key, flow(key).update(t))
        if sig:
            signals.append(sig)

    if not rows:
        print("No trades in file."); return
    span_min = (int(rows[-1]["ts_ms"]) - int(rows[0]["ts_ms"])) / 60000
    print(f"Replayed {len(rows):,} trades over {span_min:.0f} minutes -> {len(signals)} signals\n")

    def fwd(sig: Signal, h: int):
        ts, px = prices[sig.symbol]
        i = bisect.bisect_left(ts, sig.snap.ts_ms + h * 1000)
        if i >= len(ts):
            return None
        r = px[i] / sig.snap.price - 1
        return r if sig.side == "LONG" else -r

    for s in signals:
        moves = "  ".join(f"{h}s: {fwd(s, h):+.2%}" if fwd(s, h) is not None else f"{h}s: n/a" for h in horizons)
        print(f"{stamp(s.snap.ts_ms)}  {s.symbol:<16} {s.side:<5} imb {s.snap.imbalance:+.0%}  "
              f"act {s.snap.activity:4.1f}x  move {s.snap.price_change:+.2%}  | after {moves}")
    if signals:
        print("\nAverage move in the signal's direction (before fees):")
        for h in horizons:
            vals = [v for v in (fwd(s, h) for s in signals) if v is not None]
            if vals:
                hit = sum(v > 0 for v in vals) / len(vals)
                print(f"  {h:>4}s: {sum(vals)/len(vals):+.3%} avg, {hit:.0%} in the right direction ({len(vals)} signals)")
        print("\nA short recording proves the pipeline works, not that the signals have an edge.\n"
              "Judge thresholds on weeks of data, after fees.")


def main() -> None:
    ap = argparse.ArgumentParser(description="Crypto order-flow impulse signals")
    ap.add_argument("mode", choices=["live", "record", "replay"])
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--out", default="trades.csv", help="record: output CSV")
    ap.add_argument("--minutes", type=float, default=60, help="record: how long to record")
    ap.add_argument("--file", default="trades.csv", help="replay: input CSV")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = load_config(a.config)
    try:
        if a.mode == "live":
            asyncio.run(run_live(cfg))
        elif a.mode == "record":
            asyncio.run(run_record(cfg, a.out, a.minutes))
        else:
            run_replay(cfg, a.file)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
