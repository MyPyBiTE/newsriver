#!/usr/bin/env python3
"""Build a lawful T+1 U.S. reference-price input from free IEX HIST TOPS data.

This adapter streams the compressed capture and keeps only the final IEX Trade
Report for each symbol. It does not create a consolidated closing price and it
does not cover Canadian exchanges. Its output is deliberately labelled as an
IEX-only T+1 reference trade for the educational MYPYBITE calculator.

The packet-decoding approach is adapted from BlackMonday's ``iex-pcap-decoder``
(Copyright 2026 BlackMonday, MIT License). The project-specific catalog,
validation, filtering and JSON-output code is original to MYPYBITE.
"""

from __future__ import annotations

import argparse
import gzip
import json
import struct
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import BinaryIO, Iterator

from fetch_sec_intake import atomic_write_text


ROOT = Path(__file__).resolve().parents[1]
INPUT_SCHEMA = "mypybite.approved-price-input.v1"
HIST_CATALOG = "https://iextrading.com/api/1.0/hist"
IEX_TERMS = "https://www.iex.io/legal/hist-data-terms"
ATTRIBUTION = (
    "Data provided for free by IEX. By accessing or using IEX Historical Data, "
    "you agree to the IEX Historical Data Terms of Use."
)
USER_AGENT = "MYPYBITE educational research https://www.mypybite.com"
TRADE_REPORT = 0x54


def _read(stream: BinaryIO, size: int) -> bytes | None:
    value = stream.read(size)
    return value if len(value) == size else None


def _strip_to_iextp(data: bytes, linktype: int) -> bytes | None:
    """Strip Ethernet/VLAN/IPv4/UDP headers and return an IEX-TP segment."""
    offset = 0
    if linktype == 1:
        if len(data) < 14:
            return None
        ethernet_type = struct.unpack(">H", data[12:14])[0]
        offset = 14
        while ethernet_type in (0x8100, 0x88A8):
            if len(data) < offset + 4:
                return None
            ethernet_type = struct.unpack(">H", data[offset + 2 : offset + 4])[0]
            offset += 4
        if ethernet_type != 0x0800:
            return None
    elif linktype not in (101, 12, 14):
        offset = 14
    if len(data) < offset + 20:
        return None
    version_and_length = data[offset]
    if version_and_length >> 4 != 4:
        return None
    internet_header_length = (version_and_length & 0x0F) * 4
    if data[offset + 9] != 17:
        return None
    udp_offset = offset + internet_header_length
    if len(data) < udp_offset + 8:
        return None
    return data[udp_offset + 8 :]


def iter_segment_trades(segment: bytes) -> Iterator[tuple[int, str, int, float]]:
    """Yield timestamp, symbol, size and price from IEX Trade Reports."""
    if len(segment) < 40 or segment[0] != 0x01:
        return
    message_count = struct.unpack_from("<H", segment, 14)[0]
    offset = 40
    for _ in range(message_count):
        if len(segment) < offset + 2:
            break
        message_length = struct.unpack_from("<H", segment, offset)[0]
        offset += 2
        body = segment[offset : offset + message_length]
        offset += message_length
        if len(body) < 38 or body[0] != TRADE_REPORT:
            continue
        timestamp_ns = struct.unpack_from("<q", body, 2)[0]
        symbol = body[10:18].rstrip(b" \x00").decode("ascii", "ignore").upper()
        size = struct.unpack_from("<I", body, 18)[0]
        price = struct.unpack_from("<q", body, 22)[0] / 10_000.0
        if symbol and price > 0:
            yield timestamp_ns, symbol, size, price


def stream_trades(stream: BinaryIO) -> Iterator[tuple[int, str, int, float]]:
    """Stream Trade Reports from a classic PCAP or PCAPNG capture."""
    magic = _read(stream, 4)
    if magic is None:
        return
    if magic == b"\x0a\x0d\x0d\x0a":
        raw_length = _read(stream, 4)
        byte_order_magic = _read(stream, 4)
        if raw_length is None or byte_order_magic is None:
            return
        endian = "<" if byte_order_magic == b"\x4d\x3c\x2b\x1a" else ">"
        total_length = struct.unpack(endian + "I", raw_length)[0]
        _read(stream, total_length - 12)
        linktype = 1
        block_header = struct.Struct(endian + "II")
        while True:
            header = _read(stream, 8)
            if header is None:
                break
            block_type, block_length = block_header.unpack(header)
            if block_length < 12:
                break
            body = _read(stream, block_length - 8)
            if body is None:
                break
            if block_type == 0x00000001:
                linktype = struct.unpack(endian + "H", body[0:2])[0]
            elif block_type == 0x00000006 and len(body) >= 20:
                captured_length = struct.unpack(endian + "I", body[12:16])[0]
                segment = _strip_to_iextp(body[20 : 20 + captured_length], linktype)
                if segment:
                    yield from iter_segment_trades(segment)
            elif block_type == 0x00000003 and len(body) >= 4:
                original_length = struct.unpack(endian + "I", body[0:4])[0]
                segment = _strip_to_iextp(body[4 : 4 + original_length], linktype)
                if segment:
                    yield from iter_segment_trades(segment)
        return

    classic_magics = {
        b"\xd4\xc3\xb2\xa1": "<",
        b"\x4d\x3c\xb2\xa1": "<",
        b"\xa1\xb2\xc3\xd4": ">",
        b"\xa1\xb2\x3c\x4d": ">",
    }
    if magic not in classic_magics:
        raise ValueError(f"Unrecognized capture magic {magic.hex()}")
    endian = classic_magics[magic]
    remainder = _read(stream, 20)
    if remainder is None:
        return
    linktype = struct.unpack(endian + "I", remainder[16:20])[0]
    packet_header = struct.Struct(endian + "IIII")
    while True:
        header = _read(stream, 16)
        if header is None:
            break
        _seconds, _fraction, included_length, _original_length = packet_header.unpack(
            header
        )
        packet = _read(stream, included_length)
        if packet is None:
            break
        segment = _strip_to_iextp(packet, linktype)
        if segment:
            yield from iter_segment_trades(segment)


def fetch_json(url: str) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def find_tops_capture(date: str | None, lookback_days: int = 7) -> dict:
    requested = (
        datetime.strptime(date, "%Y%m%d").date()
        if date
        else (datetime.now(timezone.utc) - timedelta(days=1)).date()
    )
    for offset in range(lookback_days + 1):
        candidate = (requested - timedelta(days=offset)).strftime("%Y%m%d")
        payload = fetch_json(f"{HIST_CATALOG}?date={candidate}")
        if not isinstance(payload, list):
            continue
        rows = [
            row
            for row in payload
            if isinstance(row, dict)
            and str(row.get("feed") or "").upper() == "TOPS"
            and row.get("link")
        ]
        if rows:
            return max(rows, key=lambda row: tuple(int(x) for x in str(row.get("version") or "0").split(".")))
    raise ValueError("No IEX TOPS HIST capture was found in the requested window")


def load_symbol_filter(path: Path | None) -> set[str] | None:
    if path is None:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    fields = payload.get("fields")
    data = payload.get("data")
    if isinstance(fields, list) and isinstance(data, list) and "symbol" in fields:
        position = fields.index("symbol")
        symbols = {
            str(row[position] or "").strip().upper()
            for row in data
            if isinstance(row, list) and len(row) > position
        }
        return {symbol for symbol in symbols if symbol}
    candidates = payload.get("companies") or payload.get("securities") or payload.get("rows")
    if not isinstance(candidates, list):
        raise ValueError("Ticker directory has no supported company list")
    symbols = {
        str(row.get("symbol") or row.get("ticker") or "").strip().upper()
        for row in candidates
        if isinstance(row, dict)
    }
    return {symbol for symbol in symbols if symbol}


def build_input(
    stream: BinaryIO,
    *,
    capture_date: str,
    capture_url: str,
    keep: set[str] | None = None,
) -> dict:
    last: dict[str, tuple[int, int, float]] = {}
    trade_count = 0
    for timestamp_ns, symbol, size, price in stream_trades(stream):
        if keep is not None and symbol not in keep:
            continue
        trade_count += 1
        if trade_count % 1_000_000 == 0:
            print(
                f"Scanned {trade_count:,} usable IEX trades; "
                f"{len(last):,} requested symbols seen so far...",
                flush=True,
            )
        prior = last.get(symbol)
        if prior is None or timestamp_ns > prior[0]:
            last[symbol] = (timestamp_ns, size, price)
    if not last:
        raise ValueError("The capture contained no usable confirmed IEX trades")
    quotes = []
    for symbol in sorted(last):
        timestamp_ns, size, price = last[symbol]
        as_of = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, timezone.utc)
        quotes.append(
            {
                "symbol": symbol,
                "price": round(price, 4),
                "currency": "USD",
                "asOf": as_of.isoformat().replace("+00:00", "Z"),
                "status": "iex-last-trade-t1",
                "venue": "IEX",
                "tradeSize": size,
            }
        )
    return {
        "schema": INPUT_SCHEMA,
        "source": {
            "name": "IEX HIST",
            "permission": "Public T+1 distribution permitted with required attribution",
            "attribution": ATTRIBUTION,
            "url": IEX_TERMS,
            "scope": "IEX-only U.S. trades; not a consolidated market close",
            "captureDate": capture_date,
            "captureUrl": capture_url,
        },
        "quotes": quotes,
        "scan": {"usableTradeCount": trade_count, "symbolCount": len(quotes)},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="IEX HIST date as YYYYMMDD; defaults to yesterday")
    parser.add_argument("--url", help="Direct official TOPS .pcap.gz URL for a bounded test")
    parser.add_argument("--capture-date", help="Date attached to --url, as YYYYMMDD")
    parser.add_argument("--symbols", type=Path, help="Optional ticker-directory JSON filter")
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "outputs" / "iex-approved-price-input.json",
    )
    args = parser.parse_args()
    try:
        capture = (
            {"link": args.url, "date": args.capture_date or args.date or "unknown"}
            if args.url
            else find_tops_capture(args.date)
        )
        keep = load_symbol_filter(args.symbols)
        request = urllib.request.Request(
            str(capture["link"]), headers={"User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            with gzip.GzipFile(fileobj=response) as uncompressed:
                payload = build_input(
                    uncompressed,
                    capture_date=str(capture.get("date") or "unknown"),
                    capture_url=str(capture["link"]),
                    keep=keep,
                )
        atomic_write_text(args.output, json.dumps(payload, indent=2) + "\n")
    except (OSError, ValueError, json.JSONDecodeError, urllib.error.URLError) as exc:
        raise SystemExit(f"IEX HIST input was not changed: {exc}") from exc
    print(
        f"Scanned {payload['scan']['usableTradeCount']:,} usable trades and wrote "
        f"{payload['scan']['symbolCount']:,} IEX-only T+1 reference prices."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
