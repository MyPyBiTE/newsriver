from __future__ import annotations

import importlib.util
import io
import json
import struct
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location(
    "build_iex_hist_price_input", SCRIPTS / "build_iex_hist_price_input.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def classic_pcap_with_trade(
    *, symbol: str = "AAPL", price: float = 211.25, timestamp_ns: int = 1_540_000_000_000_000_000
) -> bytes:
    body = bytearray(38)
    body[0] = 0x54
    struct.pack_into("<q", body, 2, timestamp_ns)
    body[10:18] = symbol.encode("ascii").ljust(8, b" ")
    struct.pack_into("<I", body, 18, 100)
    struct.pack_into("<q", body, 22, round(price * 10_000))
    segment = bytearray(40)
    segment[0] = 0x01
    struct.pack_into("<H", segment, 14, 1)
    segment.extend(struct.pack("<H", len(body)))
    segment.extend(body)

    ipv4 = bytearray(20)
    ipv4[0] = 0x45
    ipv4[9] = 17
    udp = bytes(8)
    packet = bytes(ipv4) + udp + bytes(segment)
    global_header = b"\xd4\xc3\xb2\xa1" + struct.pack(
        "<HHIIII", 2, 4, 0, 0, 65_535, 101
    )
    packet_header = struct.pack("<IIII", 0, 0, len(packet), len(packet))
    return global_header + packet_header + packet


class IexHistPriceInputTests(unittest.TestCase):
    def test_streams_a_trade_from_classic_pcap(self):
        trades = list(MODULE.stream_trades(io.BytesIO(classic_pcap_with_trade())))
        self.assertEqual(len(trades), 1)
        _timestamp, symbol, size, price = trades[0]
        self.assertEqual(symbol, "AAPL")
        self.assertEqual(size, 100)
        self.assertEqual(price, 211.25)

    def test_builds_an_attributed_iex_only_input(self):
        payload = MODULE.build_input(
            io.BytesIO(classic_pcap_with_trade()),
            capture_date="20181019",
            capture_url="https://example.invalid/official-sample.pcap.gz",
        )
        self.assertEqual(payload["schema"], "mypybite.approved-price-input.v1")
        self.assertEqual(payload["source"]["name"], "IEX HIST")
        self.assertIn("IEX Historical Data Terms of Use", payload["source"]["attribution"])
        self.assertIn("not a consolidated", payload["source"]["scope"])
        self.assertEqual(payload["quotes"][0]["status"], "iex-last-trade-t1")
        self.assertEqual(payload["quotes"][0]["venue"], "IEX")

    def test_symbol_filter_excludes_unrequested_trades(self):
        with self.assertRaisesRegex(ValueError, "no usable confirmed IEX trades"):
            MODULE.build_input(
                io.BytesIO(classic_pcap_with_trade()),
                capture_date="20181019",
                capture_url="https://example.invalid/official-sample.pcap.gz",
                keep={"MSFT"},
            )

    def test_loads_the_compact_ticker_directory_filter(self):
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", dir=ROOT / "outputs", delete=False, encoding="utf-8"
        ) as handle:
            json.dump(
                {
                    "schema": "mypybite.ticker-directory.v1",
                    "fields": ["symbol", "name"],
                    "data": [["AAPL", "Apple Inc."], ["MSFT", "Microsoft"]],
                },
                handle,
            )
            path = Path(handle.name)
        self.addCleanup(path.unlink, missing_ok=True)
        self.assertEqual(MODULE.load_symbol_filter(path), {"AAPL", "MSFT"})


if __name__ == "__main__":
    unittest.main()
