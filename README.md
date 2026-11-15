# Order-Flow Impulse Signals (crypto futures)

Real-time Python module that reads the **aggressive trade stream** from Binance USD-M futures and Bybit
linear perpetuals, measures short-term **buy/sell imbalance** against normal activity, and sends
**LONG / SHORT alerts** to the console and Telegram. Alerts are for manual trading: it never places orders.

```
trade stream (WebSocket) ──► rolling windows ──► filters ──► signal ──► console + Telegram
  taker buy / taker sell       30 s impulse       imbalance              🟢 LONG BTCUSDT @ 64,710
  price, size, time            15 min baseline    activity x             imbalance +62% · activity 4.8x
                                                  price move             move +0.21% · delta +1,240k
                                                  cooldown
```

## What it measures

For each symbol, over the **short window** (default 30 s):

| Metric | Meaning |
|---|---|
| Buy / sell volume | Notional (USDT) traded by takers who bought (lifted the ask) or sold (hit the bid) |
| Delta | Buy volume − sell volume |
| Imbalance | Delta ÷ total volume, from −100% (all selling) to +100% (all buying) |
| Activity | Volume per second now ÷ volume per second over the **baseline window** (default 15 min) |
| Price move | Price change inside the short window |

A **LONG** fires when imbalance, activity and price move all pass their thresholds in the same direction;
**SHORT** is the mirror. A per-symbol cooldown makes one impulse produce one alert, and no signal fires
until the baseline window has filled.

## Quick start

```bash
pip install -r requirements.txt
cp config.example.yaml config.yaml          # edit symbols, windows, thresholds

# optional Telegram alerts
export TELEGRAM_BOT_TOKEN=123456:ABC...
export TELEGRAM_CHAT_ID=123456789

python -m orderflow.main live   --config config.yaml
```

## Record and replay (tune without waiting for live markets)

```bash
python -m orderflow.main record --config config.yaml --out trades.csv --minutes 60
python -m orderflow.main replay --config config.yaml --file trades.csv
```

Replay runs the same engine over the recorded trades and shows how price moved 60 s and 300 s after
each signal, so thresholds can be judged on data instead of guesses.

### Sample output

Real replay of a 20-minute Bybit recording (BTC, ETH, SOL perpetuals, 55,894 trades, quiet Sunday
session) with the looser `config.demo.yaml`:

```
Replayed 55,894 trades over 20 minutes -> 3 signals

2026-11-15 17:49:32 UTC  bybit:BTCUSDT    SHORT imb -84%  act  4.7x  move -0.14%  | after 60s: -0.09%  300s: -0.18%
2026-11-15 17:50:24 UTC  bybit:ETHUSDT    LONG  imb +63%  act  2.5x  move +0.14%  | after 60s: +0.08%  300s: +0.25%
2026-11-15 17:50:46 UTC  bybit:SOLUSDT    LONG  imb +69%  act  2.5x  move +0.21%  | after 60s: +0.17%  300s: +0.20%

Average move in the signal's direction (before fees):
    60s: +0.053% avg, 67% in the right direction (3 signals)
   300s: +0.089% avg, 67% in the right direction (3 signals)
```

Three signals prove the pipeline works end to end. They say nothing yet about an edge: that needs
weeks of recordings and fees included.

## Configuration

Everything lives in `config.yaml`. No code changes needed:

```yaml
exchanges:
  bybit: [BTCUSDT, ETHUSDT, SOLUSDT]
  binance: [BTCUSDT, ETHUSDT]
windows:
  short_seconds: 30
  baseline_seconds: 900
filters:
  min_imbalance: 0.35
  min_activity: 3.0
  min_price_change: 0.0015
  min_trades: 50
  min_volume: 250000
  cooldown_seconds: 120
```

## Design notes

- **asyncio + websockets**, one connection per exchange, merged into one event stream.
- **Auto-reconnect** with exponential backoff and jitter; Bybit keep-alive pings every 20 s.
- **O(1) rolling windows**: running buy/sell sums with deque eviction, so hundreds of trades per second
  per symbol are cheap.
- Taker side is taken straight from the exchange (`m` flag on Binance, `S` on Bybit), not guessed from
  price ticks.
- Unit tests for the metrics, eviction, warm-up and cooldown: `python -m pytest tests`.

## Project layout

```
orderflow/
  streams.py   Binance + Bybit WebSocket clients, reconnect, merge
  metrics.py   rolling order-flow windows and snapshots
  signals.py   filters, cooldown, LONG/SHORT signal
  notify.py    Telegram sender
  main.py      CLI: live / record / replay
tests/         unit tests
```

## Disclaimer

Educational software. Signals are not trade recommendations. Order-flow impulses are noisy:
test any thresholds on weeks of recorded data, after fees, before using them.

---
Built by **TestedEdge Lab**: trading tools with honest testing.
