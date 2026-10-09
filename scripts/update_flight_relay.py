#!/usr/bin/env python3
from __future__ import annotations

import json
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

API_ROOT = "https://api.adsb.lol/v2/icao"
WATCHLIST_PATH = Path("flight_watchlist.json")
OUTPUT_PATH = Path("flight.json")


def clean(value: Any) -> str:
    return " ".join(str(value or "").split())


def altitude(value: Any) -> int:
    if clean(value).lower() == "ground":
        return 0
    try:
        return max(0, round(float(value)))
    except (TypeError, ValueError):
        return 0


def load_watchlist() -> list[dict[str, str]]:
    payload = json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
    entries = payload.get("aircraft", []) if isinstance(payload, dict) else []
    return [entry for entry in entries if isinstance(entry, dict) and clean(entry.get("hex"))]


def newest_aircraft(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    aircraft = payload.get("ac", [])
    if not isinstance(aircraft, list) or not aircraft:
        return None
    valid = [item for item in aircraft if isinstance(item, dict)]
    return min(valid, key=lambda item: float(item.get("seen", 999999))) if valid else None


def normalize(raw: dict[str, Any], tracked: dict[str, str]) -> dict[str, Any]:
    hex_code = clean(raw.get("hex") or tracked.get("hex")).lower()
    registration = clean(raw.get("r") or tracked.get("registration"))
    callsign = clean(raw.get("flight") or registration or hex_code.upper())
    height = altitude(raw.get("alt_baro") if raw.get("alt_baro") is not None else raw.get("alt_geom"))
    status = "Airborne" if height > 0 else "Not currently airborne"
    label = clean(tracked.get("label") or registration or callsign)
    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "ownerName": label,
        "callsign": callsign,
        "altitudeFt": height,
        "route": status,
        "updatedAt": updated_at,
        "url": clean(tracked.get("trackerUrl")) or f"https://www.adsb.lol/?icao={hex_code}",
        "sourceName": "ADSB.lol",
    }


def unavailable(tracked: dict[str, str], status: str = "Not currently airborne") -> dict[str, Any]:
    hex_code = clean(tracked.get("hex")).lower()
    registration = clean(tracked.get("registration"))
    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "ownerName": clean(tracked.get("label") or registration or hex_code.upper()),
        "callsign": registration or hex_code.upper(),
        "altitudeFt": 0,
        "route": status,
        "updatedAt": updated_at,
        "url": clean(tracked.get("trackerUrl")) or f"https://www.adsb.lol/?icao={hex_code}",
        "sourceName": "ADSB.lol",
    }


def fetch_aircraft(session: requests.Session, tracked: dict[str, str]) -> dict[str, Any] | None:
    hex_code = clean(tracked.get("hex")).lower()
    response = session.get(f"{API_ROOT}/{hex_code}", timeout=25)
    response.raise_for_status()
    raw = newest_aircraft(response.json())
    return normalize(raw, tracked) if raw else unavailable(tracked)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(rendered)
        temporary = Path(handle.name)
    temporary.replace(path)


def main() -> int:
    items: list[dict[str, Any]] = []
    with requests.Session() as session:
        session.headers.update({"User-Agent": "MyPyBiTE flight summary/1.0 (public website)"})
        watchlist = load_watchlist()
        for index, tracked in enumerate(watchlist):
            try:
                item = fetch_aircraft(session, tracked)
                if item:
                    items.append(item)
            except requests.RequestException as exc:
                print(f"WARN: {tracked.get('hex')}: {exc}")
                items.append(unavailable(tracked, "Live status temporarily unavailable"))
            if index < len(watchlist) - 1:
                time.sleep(1.25)

    items.sort(key=lambda row: (row["altitudeFt"] > 0, row["altitudeFt"]), reverse=True)
    payload = {
        "schemaVersion": 1,
        "generatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "attribution": "Flight data: ADSB.lol (ODbL 1.0). Presence does not identify passengers.",
        "items": items[:16],
    }
    write_json(OUTPUT_PATH, payload)
    print(f"Published {len(items)} public flight summaries; {sum(row['altitudeFt'] > 0 for row in items)} airborne.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
