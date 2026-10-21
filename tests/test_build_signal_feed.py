from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"

sys.path.insert(0, str(SCRIPTS))

SCRIPT = SCRIPTS / "build_signal_feed.py"
SPEC = importlib.util.spec_from_file_location(
    "build_signal_feed",
    SCRIPT,
)

assert SPEC and SPEC.loader

MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class SignalFeedTests(unittest.TestCase):
    def test_feed_exposes_freshness_without_mutating_evidence(self):
        now = datetime(
            2026,
            10,
            21,
            14,
            30,
            tzinfo=timezone.utc,
        )

        feed = MODULE.build_feed(
            ROOT,
            now=now,
        )

        self.assertEqual(
            feed["schema"],
            "mypybite.signal-feed.v1",
        )

        self.assertEqual(
            feed["generatedAt"],
            "2026-10-21T14:30:00Z",
        )

        self.assertEqual(
            feed["freshness"]["feedGeneratedAt"],
            feed["generatedAt"],
        )

        self.assertEqual(
            feed["freshness"]["recommendedRefreshSeconds"],
            300,
        )

        self.assertEqual(
            feed["freshness"]["warningAfterMinutes"],
            4320,
        )

        self.assertTrue(
            feed["freshness"]["latestFiledDate"]
        )

        self.assertGreater(
            len(feed["filings"]),
            0,
        )

        self.assertEqual(
            len(feed["issuers"]),
            75,
        )

    def test_feed_remains_json_serializable(self):
        feed = MODULE.build_feed(
            ROOT,
            now=datetime(
                2026,
                10,
                21,
                tzinfo=timezone.utc,
            ),
        )

        encoded = json.dumps(feed)

        self.assertIn(
            '"mypybite.signal-feed.v1"',
            encoded,
        )

        for filing in feed["filings"]:
            self.assertTrue(
                filing["url"].startswith(
                    "https://www.sec.gov/"
                )
            )


if __name__ == "__main__":
    unittest.main()
