"""Live trade streams from Binance USD-M futures and Bybit linear perpetuals.

Each stream yields (symbol, Trade) and reconnects with backoff on any error.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import AsyncIterator

import websockets

from .metrics import Trade

log = logging.getLogger("streams")


async def _with_reconnect(name: str, connect_and_read):
    delay = 1.0
    while True:
        try:
            async for item in connect_and_read():
                delay = 1.0
                yield item
        except asyncio.CancelledError:
            raise
        except Exception as e:  # network errors, server closes, bad frames
            log.warning("%s stream dropped (%s: %s); reconnecting in %.0fs", name, type(e).__name__, e, delay)
        await asyncio.sleep(delay + random.random())
        delay = min(delay * 2, 60)


def binance(symbols: list[str]) -> AsyncIterator[tuple[str, Trade]]:
    streams = "/".join(f"{s.lower()}@aggTrade" for s in symbols)
    url = f"wss://fstream.binance.com/stream?streams={streams}"

    async def read():
        async with websockets.connect(url, open_timeout=15, ping_interval=20) as ws:
            log.info("binance connected: %s", ", ".join(symbols))
            async for raw in ws:
                d = json.loads(raw).get("data", {})
                if d.get("e") != "aggTrade":
                    continue
                # m = buyer is the maker -> the taker SOLD
                yield d["s"], Trade(int(d["T"]), float(d["p"]), float(d["q"]), "sell" if d["m"] else "buy")

    return _with_reconnect("binance", read)


def bybit(symbols: list[str]) -> AsyncIterator[tuple[str, Trade]]:
    url = "wss://stream.bybit.com/v5/public/linear"

    async def read():
        async with websockets.connect(url, open_timeout=15, ping_interval=None) as ws:
            await ws.send(json.dumps({"op": "subscribe", "args": [f"publicTrade.{s.upper()}" for s in symbols]}))
            log.info("bybit connected: %s", ", ".join(symbols))

            async def keepalive():
                while True:
                    await asyncio.sleep(20)
                    await ws.send(json.dumps({"op": "ping"}))

            pinger = asyncio.create_task(keepalive())
            try:
                async for raw in ws:
                    msg = json.loads(raw)
                    if not str(msg.get("topic", "")).startswith("publicTrade."):
                        continue
                    for d in msg.get("data", []):
                        # S = taker side
                        yield d["s"], Trade(int(d["T"]), float(d["p"]), float(d["v"]), "buy" if d["S"] == "Buy" else "sell")
            finally:
                pinger.cancel()

    return _with_reconnect("bybit", read)


async def merge(*iters: AsyncIterator) -> AsyncIterator:
    """Merge several async iterators into one."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)

    async def pump(it):
        async for item in it:
            await queue.put(item)

    tasks = [asyncio.create_task(pump(it)) for it in iters]
    try:
        while True:
            yield await queue.get()
    finally:
        for t in tasks:
            t.cancel()
