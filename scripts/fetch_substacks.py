#!/usr/bin/env python3

import json
import urllib.request
import xml.etree.ElementTree as ET

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path


FEEDS = [
    ("MYPYBITE", "https://mypybite.substack.com/feed"),
    ("Nora Loreto", "https://noraloreto.substack.com/feed"),
    ("Francis Bacon Conspiracy", "https://francisbaconconspiracy.substack.com/feed"),
    ("Guard the Leaf", "https://www.guardtheleaf.com/feed"),
    ("Lisa Young", "https://lisayoung.substack.com/feed"),
    ("Pascal Lottaz", "https://pascallottaz.substack.com/feed"),
    ("i-D", "https://substack.i-d.co/feed"),
    ("Stella Monique Vandal", "https://stellamoniquevandal.substack.com/feed"),
]


def parse_date(value):
    if not value:
        return None

    value = value.strip()

    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def text(element, names):
    for name in names:
        found = element.find(name)
        if found is not None and found.text:
            return found.text.strip()
    return ""


def fetch_feed(source, url):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "MYPYBITE-Substack-Reader/1.0"},
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read()

    root = ET.fromstring(raw)

    posts = []

    # RSS
    for item in root.findall(".//item"):
        title = text(item, ["title"])
        link = text(item, ["link"])
        published_raw = text(
            item,
            [
                "pubDate",
                "{http://purl.org/dc/elements/1.1/}date",
            ],
        )

        published = parse_date(published_raw)

        if title and link and published:
            posts.append(
                {
                    "source": source,
                    "headline": title,
                    "url": link,
                    "publishedAt": published.isoformat().replace("+00:00", "Z"),
                    "_sort": published.timestamp(),
                }
            )

    # Atom fallback
    namespace = "{http://www.w3.org/2005/Atom}"

    for entry in root.findall(f".//{namespace}entry"):
        title = text(entry, [f"{namespace}title"])

        link = ""
        for link_element in entry.findall(f"{namespace}link"):
            href = link_element.attrib.get("href", "")
            rel = link_element.attrib.get("rel", "alternate")
            if href and rel == "alternate":
                link = href
                break

        published_raw = text(
            entry,
            [
                f"{namespace}published",
                f"{namespace}updated",
            ],
        )

        published = parse_date(published_raw)

        if title and link and published:
            posts.append(
                {
                    "source": source,
                    "headline": title,
                    "url": link,
                    "publishedAt": published.isoformat().replace("+00:00", "Z"),
                    "_sort": published.timestamp(),
                }
            )

    return posts


def main():
    posts = []

    for source, url in FEEDS:
        try:
            posts.extend(fetch_feed(source, url))
            print(f"OK: {source}")
        except Exception as exc:
            print(f"SKIP: {source}: {exc}")

    if not posts:
        raise SystemExit("No Substack posts were found.")

    posts.sort(key=lambda item: item["_sort"], reverse=True)

    newest = posts[0]
    newest.pop("_sort", None)

    payload = {
        "generatedAt": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "latest": newest,
    }

    Path("substack.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(
        f"NEWEST: {newest['source']} | "
        f"{newest['headline']}"
    )


if __name__ == "__main__":
    main()
