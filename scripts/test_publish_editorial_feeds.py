#!/usr/bin/env python3
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from publish_editorial_feeds import HEADERS, PublishFailure, make_feeds


def row(**changes):
    base = {header: "" for header in HEADERS}
    base.update(
        story_id="story-1",
        source="Example Source",
        source_url="https://example.com/story-1",
        canonical_url="https://example.com/story-1",
        published_at="2026-07-21 12:00:00",
        scraped_headline="Example headline",
        editor_synopsis="A concise verified synopsis containing fewer than twenty-five words.",
        status="Approved",
        ticker_eligible=True,
        newsriver_eligible=True,
        access_type="FREE",
        validation_state="VALID",
        selection_mode="MANUAL",
    )
    base.update(changes)
    return [base[header] for header in HEADERS]


NOW = datetime(2026, 7, 21, 16, 0, tzinfo=timezone.utc)
TZ = ZoneInfo("America/Toronto")


class PublishingRulesTest(unittest.TestCase):
    def test_approved_publishes(self):
        breaking, newsriver, _ = make_feeds([row()], NOW, TZ)
        self.assertEqual([item["id"] for item in breaking["items"]], ["story-1"])
        self.assertEqual([item["id"] for item in newsriver["items"]], ["story-1"])

    def test_hold_and_rejected_never_publish(self):
        rows = [row(status="Hold"), row(story_id="story-2", status="Rejected")]
        breaking, newsriver, _ = make_feeds(rows, NOW, TZ)
        self.assertEqual(breaking["items"], [])
        self.assertEqual(newsriver["items"], [])

    def test_schedule_waits_then_publishes(self):
        future = row(status="Scheduled", go_live_at="2026-07-21 13:00:00")
        live = row(story_id="story-2", status="Scheduled", go_live_at="2026-07-21 11:00:00")
        breaking, _, _ = make_feeds([future, live], NOW, TZ)
        self.assertEqual([item["id"] for item in breaking["items"]], ["story-2"])

    def test_expiry_and_keep_until(self):
        expired = row(expires_at="2026-07-21 11:00:00")
        retained = row(
            story_id="story-2",
            expires_at="2026-07-21 11:00:00",
            keep_until="2026-07-21 14:00:00",
        )
        breaking, _, _ = make_feeds([expired, retained], NOW, TZ)
        self.assertEqual([item["id"] for item in breaking["items"]], ["story-2"])

    def test_correction_replaces_original(self):
        original = row()
        correction = row(story_id="story-2", correction_of="story-1", scraped_headline="Corrected headline")
        breaking, _, _ = make_feeds([original, correction], NOW, TZ)
        self.assertEqual([item["id"] for item in breaking["items"]], ["story-2"])

    def test_retraction_removes_story(self):
        breaking, _, _ = make_feeds([row(status="Retracted")], NOW, TZ)
        self.assertEqual(breaking["items"], [])

    def test_over_25_word_synopsis_fails_closed(self):
        synopsis = " ".join(["word"] * 26)
        with self.assertRaises(PublishFailure):
            make_feeds([row(editor_synopsis=synopsis)], NOW, TZ)


if __name__ == "__main__":
    unittest.main()
