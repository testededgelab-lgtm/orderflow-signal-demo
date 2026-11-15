from orderflow.metrics import OrderFlow, Trade
from orderflow.signals import Filters, SignalEngine


def feed(flow, trades):
    snap = None
    for t in trades:
        snap = flow.update(t)
    return snap


def test_imbalance_and_delta():
    f = OrderFlow(short_seconds=10, baseline_seconds=60)
    s = feed(f, [Trade(1_000, 100, 3, "buy"), Trade(2_000, 100, 1, "sell")])
    assert s.buy_vol == 300 and s.sell_vol == 100
    assert s.delta == 200
    assert abs(s.imbalance - 0.5) < 1e-12


def test_short_window_evicts_old_trades():
    f = OrderFlow(short_seconds=10, baseline_seconds=60)
    s = feed(f, [Trade(0, 100, 5, "buy"), Trade(11_000, 100, 1, "sell")])
    assert s.buy_vol == 0 and s.sell_vol == 100      # first trade left the 10 s window
    assert s.trades == 1


def test_activity_vs_baseline():
    f = OrderFlow(short_seconds=10, baseline_seconds=100)
    trades = [Trade(i * 1000, 100, 1, "buy") for i in range(100)]   # 1 trade / second for 100 s
    trades += [Trade(100_000 + i * 100, 100, 1, "buy") for i in range(50)]  # 10 trades / second burst
    s = feed(f, trades)
    assert s.activity > 2.5


def test_long_signal_and_cooldown():
    flt = Filters(min_imbalance=0.3, min_activity=2, min_price_change=0.001, min_trades=5,
                  min_volume=0, cooldown_seconds=60)
    f, eng = OrderFlow(short_seconds=10, baseline_seconds=100), SignalEngine(flt)
    calm = [Trade(i * 1000, 100, 1, "buy" if i % 2 else "sell") for i in range(100)]
    burst = [Trade(100_000 + i * 100, 100 + i * 0.01, 2, "buy") for i in range(40)]
    sigs = [eng.check("X", f.update(t)) for t in calm + burst]
    fired = [s for s in sigs if s]
    assert len(fired) == 1 and fired[0].side == "LONG"   # cooldown stops repeats


def test_no_signal_before_warmup():
    flt = Filters(min_imbalance=0.1, min_activity=0, min_price_change=0, min_trades=1, min_volume=0)
    f, eng = OrderFlow(short_seconds=10, baseline_seconds=100), SignalEngine(flt)
    assert eng.check("X", f.update(Trade(0, 100, 1, "buy"))) is None
