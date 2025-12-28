#!/usr/bin/env python3
# scripts/fetch_nhl.py
#
# Purpose: Build a clean NHL relay JSON for MyPyBITE flipboard.
# - Gets today's schedule in ET. If before 09:30 ET, also includes yesterday.
# - NEVER drops live games.
# - Finals are kept for ~5 hours after a rough "game end" estimate (start + 3h15m),
#   even if that means we must include yesterday after the morning window.
# - Tries multiple NHL endpoints with retries and backoff
#   (api-web first for freshness, statsapi as fallback).
# - Normalizes to: {"generated_utc": "...Z", "source": [...], "dates":[{"date":"YYYY-MM-DD","games":[...]}]}
# - Writes to OUTFILE and mirrors to a second path so you don't end up with a 404
#   depending on whether the site is reading /nhl.json or /newsriver/nhl.json.
#
# Env (optional):
#   SCHEDULE_DATE=YYYY-MM-DD
#   OUTFILE=nhl.json
#   OUTFILE_EXTRA=newsriver/nhl.json   # if set, also write here
#
# Stdlib only.

from __future__ import annotations
import os
import sys
import json
import time
import socket
import datetime
import urllib.request
import urllib.error
from urllib.parse import urlparse
from typing import Tuple, List, Dict, Any, Optional

OUTFILE = os.environ.get("OUTFILE", "nhl.json")
OUTFILE_EXTRA = os.environ.get("OUTFILE_EXTRA")
SCHEDULE_DATE = os.environ.get("SCHEDULE_DATE")  # YYYY-MM-DD or None

# We keep yesterday's games while it's still "morning" in ET.
# Window: from midnight until 09:30 ET.
MORNING_CUTOFF_HOUR_ET = 9
MORNING_CUTOFF_MINUTE_ET = 30

# Finals retention: keep finals for ~5 hours after estimated end time.
FINAL_KEEP_HOURS = 5.0

# Rough NHL game duration estimate: start + 3h15m.
EST_GAME_DURATION_MIN = 195

USER_AGENT = "MyPyBITE/nhl-relay (newsriver)"

CANADIAN_ABBRS = {"TOR", "MTL", "OTT", "WPG", "EDM", "CGY", "VAN"}


# ----- time helpers -----
def _now_et() -> datetime.datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.datetime.now(ZoneInfo("America/Toronto"))
    except Exception:
        # Fallback: UTC, but we treat it as "ET-ish" only for window checks.
        return datetime.datetime.utcnow()


def _now_utc() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)


def today_eastern_date() -> datetime.date:
    return _now_et().date()


def include_yesterday_window_et() -> bool:
    """
    Return True if we should include yesterday's games:
    Before 09:30 ET on the current day.
    """
    now = _now_et()
    if now.hour < MORNING_CUTOFF_HOUR_ET:
        return True
    if now.hour == MORNING_CUTOFF_HOUR_ET and now.minute < MORNING_CUTOFF_MINUTE_ET:
        return True
    return False


def fmt_date(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")


def parse_iso_dt(s: str) -> Optional[datetime.datetime]:
    """
    Parse common NHL-ish ISO strings into an aware UTC datetime when possible.
    Accepts:
      - 2025-12-28T00:00:00Z
      - 2025-12-28T00:00:00+00:00
      - 2025-12-28T00:00:00.000Z
    """
    if not s or not isinstance(s, str):
        return None
    t = s.strip()
    try:
        if t.endswith("Z"):
            t2 = t[:-1]
            # strip fractional seconds if present
            if "." in t2:
                t2 = t2.split(".", 1)[0]
            dt = datetime.datetime.fromisoformat(t2)
            return dt.replace(tzinfo=datetime.timezone.utc)
        # fromisoformat handles +00:00
        dt = datetime.datetime.fromisoformat(t)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        return None


# ----- candidate endpoints (api-web preferred, statsapi fallback) -----
CANDIDATES = [
    {
        "name": "api-web",
        "url": lambda d: f"https://api-web.nhle.com/v1/schedule/{d}",
        "kind": "apiweb",
    },
    {
        "name": "statsapi",
        "url": lambda d: f"https://statsapi.web.nhl.com/api/v1/schedule?date={d}&hydrate=linescore,team",
        "kind": "statsapi",
    },
]


def _req(url: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )


def fetch_with_retries(url: str, attempts: int = 6, first_delay: float = 0.9):
    delay = first_delay
    last_err = None
    for i in range(1, attempts + 1):
        try:
            # Best-effort DNS precheck; ignore failures here.
            try:
                host = urlparse(url).hostname
                if host:
                    socket.gethostbyname(host)
            except Exception:
                pass

            with urllib.request.urlopen(_req(url), timeout=22) as r:
                if r.status >= 500:
                    raise urllib.error.HTTPError(url, r.status, "Server error", r.headers, None)
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, socket.gaierror) as e:
            last_err = e
        except Exception as e:
            last_err = e

        if i < attempts:
            time.sleep(delay)
            delay = min(delay * 1.8, 10.0)
        else:
            raise last_err or RuntimeError("Unknown fetch error")


# ----- state helpers -----
def map_state_generic(val: str) -> str:
    t = (val or "").upper()
    if ("IN_PROGRESS" in t) or ("LIVE" in t) or ("STARTED" in t):
        return "Live"
    if ("FINAL" in t) or (t == "FINAL") or ("OFF" in t):
        return "Final"
    if ("PRE" in t) or ("FUT" in t) or ("SCHEDULED" in t) or ("UPCOMING" in t) or (t == "PREVIEW"):
        return "Preview"
    return "Unknown"


# ----- normalizers -----
def _abbr_from_team(team: Dict[str, Any]) -> str:
    return (
        team.get("abbreviation")
        or team.get("triCode")
        or (team.get("shortName") if isinstance(team.get("shortName"), str) else None)
        or (team.get("name", "")[:3].upper() if team.get("name") else "")
        or ""
    ).upper()


def _score_or_none(x: Any):
    return x if isinstance(x, int) else None


def _final_winner_abbr(away_abbr: str, home_abbr: str, away_score, home_score, state: str):
    if state != "Final":
        return None
    if isinstance(away_score, int) and isinstance(home_score, int) and away_score != home_score:
        return away_abbr if away_score > home_score else home_abbr
    return None


def norm_from_statsapi(data: Dict[str, Any], date_str: str) -> List[Dict[str, Any]]:
    games_out: List[Dict[str, Any]] = []
    dates = (data or {}).get("dates") or []
    if dates and dates[0].get("games"):
        for g in dates[0]["games"]:
            status = g.get("status") or {}
            ls = g.get("linescore") or {}
            teams = g.get("teams") or {}
            aw = teams.get("away") or {}
            hm = teams.get("home") or {}
            awt = aw.get("team") or {}
            hmt = hm.get("team") or {}

            aw_abbr = _abbr_from_team(awt) or "AWY"
            hm_abbr = _abbr_from_team(hmt) or "HOM"

            abstract = status.get("abstractGameState", "") or ""
            detailed = status.get("detailedState", "") or abstract
            mapped = map_state_generic(detailed or abstract)

            away_score = _score_or_none(aw.get("score"))
            home_score = _score_or_none(hm.get("score"))

            games_out.append(
                {
                    "gamePk": g.get("gamePk"),
                    "gameDate": g.get("gameDate"),
                    "status": {
                        "abstractGameState": mapped,
                        "detailedState": (detailed or "").upper() or mapped,
                    },
                    "teams": {
                        "away": {"team": {"abbreviation": aw_abbr, "triCode": aw_abbr}, "score": away_score},
                        "home": {"team": {"abbreviation": hm_abbr, "triCode": hm_abbr}, "score": home_score},
                    },
                    "linescore": {
                        "currentPeriod": ls.get("currentPeriod") or 0,
                        "currentPeriodOrdinal": ls.get("currentPeriodOrdinal") or "",
                        "currentPeriodTimeRemaining": ls.get("currentPeriodTimeRemaining") or "",
                    },
                    "finalWinner": _final_winner_abbr(aw_abbr, hm_abbr, away_score, home_score, mapped),
                }
            )
    return games_out


def norm_from_apiweb(data: Dict[str, Any], date_str: str) -> List[Dict[str, Any]]:
    games_raw: List[Dict[str, Any]] = []

    if isinstance(data, dict):
        if "gameWeek" in data and isinstance(data["gameWeek"], list):
            for day in data["gameWeek"]:
                if (day or {}).get("date") == date_str and day.get("games"):
                    games_raw.extend(day["games"])
        if "dates" in data and isinstance(data["dates"], list):
            for day in data["dates"]:
                if (day or {}).get("date") == date_str and day.get("games"):
                    games_raw.extend(day["games"])
        if not games_raw and "games" in data and isinstance(data["games"], list):
            if (data.get("date") in (None, "", date_str)) or (data.get("date") == date_str):
                games_raw.extend(data["games"])

    def abbr(node: Dict[str, Any]) -> str:
        return (
            node.get("abbrev")
            or node.get("abbreviation")
            or (node.get("triCode") if isinstance(node.get("triCode"), str) else None)
            or (node.get("name", "")[:3].upper() if node.get("name") else "")
            or ""
        ).upper()

    games_out: List[Dict[str, Any]] = []
    for g in games_raw:
        game_date = g.get("startTimeUTC") or g.get("gameDate") or ""

        state_raw = (g.get("gameState") or "")
        if not state_raw and isinstance(g.get("status"), dict):
            st = g["status"]
            state_raw = st.get("detailedState") or st.get("abstractGameState") or ""
        mapped = map_state_generic(state_raw)

        aw = g.get("awayTeam") or {}
        hm = g.get("homeTeam") or {}

        aw_abbr = abbr(aw) or "AWY"
        hm_abbr = abbr(hm) or "HOM"

        away_score = _score_or_none(
            aw.get("score") if isinstance(aw.get("score"), int) else g.get("awayTeamScore")
        )
        home_score = _score_or_none(
            hm.get("score") if isinstance(hm.get("score"), int) else g.get("homeTeamScore")
        )

        games_out.append(
            {
                "gamePk": g.get("id") or g.get("gamePk"),
                "gameDate": game_date,
                "status": {
                    "abstractGameState": mapped,
                    "detailedState": (state_raw or "").upper() or mapped,
                },
                "teams": {
                    "away": {"team": {"abbreviation": aw_abbr, "triCode": aw_abbr}, "score": away_score},
                    "home": {"team": {"abbreviation": hm_abbr, "triCode": hm_abbr}, "score": home_score},
                },
                "linescore": {
                    "currentPeriod": 0,
                    "currentPeriodOrdinal": "",
                    "currentPeriodTimeRemaining": "",
                },
                "finalWinner": _final_winner_abbr(aw_abbr, hm_abbr, away_score, home_score, mapped),
            }
        )

    return games_out


# ----- per-date fetch using candidates -----
def fetch_games_for_date(date_str: str) -> Tuple[List[Dict[str, Any]], str]:
    errors = []
    for cand in CANDIDATES:
        url = cand["url"](date_str)
        try:
            data = fetch_with_retries(url)
            if cand["kind"] == "statsapi":
                return norm_from_statsapi(data, date_str), cand["name"]
            else:
                return norm_from_apiweb(data, date_str), cand["name"]
        except Exception as e:
            errors.append((cand["name"], str(e)))
            continue
    msgs = "; ".join([f"{name}: {msg}" for name, msg in errors])
    raise RuntimeError(f"All NHL endpoints failed for {date_str}: {msgs}")


def estimate_final_keep(game: Dict[str, Any], now_utc: datetime.datetime) -> bool:
    """
    Keep finals for FINAL_KEEP_HOURS after estimated end time.
    If we can't parse a start time, we keep it rather than risk dropping.
    """
    try:
        st = (game.get("status") or {}).get("abstractGameState") or ""
        if st != "Final":
            return False
        start = parse_iso_dt(game.get("gameDate") or "")
        if start is None:
            return True  # can't time it -> do NOT drop
        est_end = start + datetime.timedelta(minutes=EST_GAME_DURATION_MIN)
        keep_until = est_end + datetime.timedelta(hours=FINAL_KEEP_HOURS)
        return now_utc <= keep_until
    except Exception:
        return True  # safest: do NOT drop


def build_payload(primary_date: str, include_yesterday: bool) -> Dict[str, Any]:
    """
    We always include ALL games for each included date.
    Never drop Live.
    Optionally include yesterday (morning window, or finals retention).
    """
    sources_used: List[str] = []
    now_utc = _now_utc()

    # Fetch today first
    games_today, src_today = fetch_games_for_date(primary_date)
    sources_used.append(src_today)

    dates_out: List[Dict[str, Any]] = [{"date": primary_date, "games": games_today}]

    # Determine yesterday
    dt = datetime.datetime.strptime(primary_date, "%Y-%m-%d").date()
    y_str = fmt_date(dt - datetime.timedelta(days=1))

    # We may need yesterday beyond the morning window if we want to retain finals
    # that ended recently (within ~5h window).
    need_y_for_finals = False
    if not include_yesterday:
        # If any "Final" exists in today's payload with a gameDate near midnight edge,
        # it's not the case we need yesterday; the real reason is finals that finished
        # just after midnight ET but are on yesterday's schedule date.
        # So we decide "need yesterday" based on time-of-day: after midnight ET,
        # we may still want yesterday finals in the keep window.
        # We'll do the real check after fetching yesterday (cheap enough).
        pass

    # If include_yesterday is True (morning window) fetch it.
    # If it's False, we still fetch it if we're potentially in a finals-keep window
    # (i.e., early hours ET), because those finals might live under yesterday.
    maybe_need_y = include_yesterday
    if not maybe_need_y:
        now_et = _now_et()
        # from midnight until (FINAL_KEEP_HOURS + EST duration) it's plausible yesterday finals matter
        # We keep this wide but sensible: before 08:30 ET, fetch yesterday.
        if now_et.hour < 8 or (now_et.hour == 8 and now_et.minute <= 30):
            maybe_need_y = True

    if maybe_need_y:
        try:
            games_y, src_y = fetch_games_for_date(y_str)
            sources_used.append(src_y)

            # Apply finals retention filter to yesterday only:
            # - keep all Live/Preview from yesterday (rare but safe)
            # - keep Finals only if within keep window (again: safest if we can't parse)
            kept_y: List[Dict[str, Any]] = []
            for g in games_y:
                st = (g.get("status") or {}).get("abstractGameState") or "Unknown"
                if st != "Final":
                    kept_y.append(g)  # do not drop
                else:
                    if estimate_final_keep(g, now_utc):
                        kept_y.append(g)

            dates_out.append({"date": y_str, "games": kept_y})
        except Exception as e:
            print(f"[warn] yesterday fetch failed: {e}", file=sys.stderr)

    payload: Dict[str, Any] = {
        "generated_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "source": list(dict.fromkeys(sources_used)),
        "meta": {
            "canadian_abbrs": sorted(CANADIAN_ABBRS),
            "final_keep_hours": FINAL_KEEP_HOURS,
            "est_game_duration_min": EST_GAME_DURATION_MIN,
        },
        "dates": dates_out,
    }
    return payload


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _mirror_path(outfile: str) -> Optional[str]:
    """
    Mirror to the "other" common path automatically:
      - If outfile is bare "nhl.json" -> mirror to "newsriver/nhl.json"
      - If outfile is "newsriver/nhl.json" -> mirror to "nhl.json"
      - Otherwise None (unless OUTFILE_EXTRA is set).
    """
    base = os.path.basename(outfile)
    if base != outfile:
        # has dirs
        norm = outfile.replace("\\", "/")
        if norm.startswith("newsriver/") and norm.endswith("/" + base):
            return base
        return None
    # bare filename
    return os.path.join("newsriver", base)


def main() -> int:
    if SCHEDULE_DATE:
        primary = SCHEDULE_DATE
        include_y = False
    else:
        today = today_eastern_date()
        primary = fmt_date(today)
        include_y = include_yesterday_window_et()

    try:
        payload = build_payload(primary, include_y)
    except Exception as e:
        print(f"Error building NHL payload: {e}", file=sys.stderr)
        return 1

    # Write primary OUTFILE
    _ensure_dir(OUTFILE)
    with open(OUTFILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    # Write secondary path if requested or auto-mirror to prevent 404 confusion
    extra_path = OUTFILE_EXTRA or _mirror_path(OUTFILE)
    if extra_path and extra_path != OUTFILE:
        try:
            _ensure_dir(extra_path)
            with open(extra_path, "w", encoding="utf-8") as f2:
                json.dump(payload, f2, ensure_ascii=False, separators=(",", ":"))
        except Exception as e:
            print(f"[warn] failed to write extra output {extra_path}: {e}", file=sys.stderr)

    # Logging
    total = 0
    by_date = []
    for d in payload.get("dates") or []:
        n = len(d.get("games") or [])
        total += n
        by_date.append(f"{d.get('date')}={n}")
    sources = payload.get("source")
    print(f"Wrote {OUTFILE} primary={primary} total_games={total} ({', '.join(by_date)}). sources={sources}")
    if extra_path and extra_path != OUTFILE:
        print(f"Mirrored to {extra_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
