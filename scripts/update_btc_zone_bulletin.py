#!/usr/bin/env python3
"""Generate the public BTC-zone bulletin used by the MYPYBITE staging adapter."""

from __future__ import annotations

import datetime as dt
import json
import statistics
import urllib.request
from pathlib import Path


STEP = 14_400
LOOKBACK = 14 * 6
OUTPUT = Path(__file__).with_name("btc_zone_bulletin.json")


def main() -> None:
    url = (
        "https://www.bitstamp.net/api/v2/ohlc/btcusd/"
        "?step=14400&limit=200&exclude_current_candle=true"
    )
    with urllib.request.urlopen(url, timeout=30) as response:
        raw = json.load(response)["data"]["ohlc"]
    candles = [
        {
            "timestamp": int(item["timestamp"]),
            "open": float(item["open"]),
            "high": float(item["high"]),
            "low": float(item["low"]),
            "close": float(item["close"]),
        }
        for item in raw
    ]
    if len(candles) < LOOKBACK + 15:
        raise RuntimeError("Insufficient confirmed Bitstamp candles")

    current = candles[-1]
    history = candles[-(LOOKBACK + 1) : -1]
    low = min(item["low"] for item in history)
    high = max(item["high"] for item in history)
    width = high - low
    tr = []
    for pos in range(len(candles) - 14, len(candles)):
        bar = candles[pos]
        previous = candles[pos - 1]
        tr.append(
            max(
                bar["high"] - bar["low"],
                abs(bar["high"] - previous["close"]),
                abs(bar["low"] - previous["close"]),
            )
        )
    spot = current["close"]
    prior_24h = candles[-7]["close"]
    change_24h = (spot / prior_24h - 1) * 100
    generated = dt.datetime.now(dt.timezone.utc)
    payload = {
        "schema": "mypybite.btc-zones.v1",
        "status": "forward_test",
        "source": "BITSTAMP:BTCUSD",
        "timeframe": "4h",
        "model": "range14_f382_f705_v1",
        "bar_time": current["timestamp"],
        "generated_at": generated.isoformat(),
        "expires_at": (generated + dt.timedelta(hours=8)).isoformat(),
        "spot": round(spot, 2),
        "change_24h_percent": round(change_24h, 4),
        "atr": round(statistics.fmean(tr), 2),
        "r2": round(high, 2),
        "r1": round(low + width * 0.705, 2),
        "s1": round(low + width * 0.382, 2),
        "s2": round(low, 2),
    }
    OUTPUT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

