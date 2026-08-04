#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]

TLE_DIR = ROOT / "docs/data/tle"
TLE_HISTORY_DIR = ROOT / "docs/data/tle_history"

KEEP_DAYS = 7

USERNAME = os.environ.get("SPACETRACK_USERNAME")
PASSWORD = os.environ.get("SPACETRACK_PASSWORD")

BASE_URL = "https://www.space-track.org"
LOGIN_URL = f"{BASE_URL}/ajaxauth/login"

QUERY_URL = (
    f"{BASE_URL}/basicspacedata/query/"
    "class/gp/"
    "decay_date/null-val/"
    "epoch/>now-10/"
    "orderby/NORAD_CAT_ID asc,EPOCH desc/"
    "format/tle"
)

ALPHA5_ALPHABET = "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ"

TLE_DIR.mkdir(parents=True, exist_ok=True)
TLE_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def to_iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[WARN] JSON load failed {path}: {exc}")
        return default


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(path)


def is_valid_tle_pair(line1: Any, line2: Any) -> bool:
    return (
        isinstance(line1, str)
        and isinstance(line2, str)
        and line1.startswith("1 ")
        and line2.startswith("2 ")
        and len(line1) >= 32
        and len(line2) >= 7
    )


def decode_alpha5_catalog_number(field: str) -> int | None:
    value = str(field or "").strip().upper()

    if not value:
        return None

    if value.isdigit():
        return int(value)

    if len(value) != 5:
        return None

    first = value[0]
    suffix = value[1:]

    if first not in ALPHA5_ALPHABET or not suffix.isdigit():
        return None

    high = ALPHA5_ALPHABET.index(first)

    if high < 10:
        return None

    return high * 10000 + int(suffix)


def norad_from_tle1(line1: str) -> int | None:
    return decode_alpha5_catalog_number(line1[2:7])


def parse_tle_epoch(line1: str) -> str | None:
    try:
        yy = int(line1[18:20])
        doy = float(line1[20:32])
        year = 2000 + yy if yy < 57 else 1900 + yy
        dt = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=doy - 1)
        return to_iso(dt)
    except Exception:
        return None


def parse_space_track_tle(text: str) -> list[dict[str, Any]]:
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    items: list[dict[str, Any]] = []

    i = 0
    while i < len(lines):
        name = None

        if (
            i + 2 < len(lines)
            and not lines[i].startswith("1 ")
            and lines[i + 1].startswith("1 ")
            and lines[i + 2].startswith("2 ")
        ):
            name = lines[i].strip()
            line1 = lines[i + 1].strip()
            line2 = lines[i + 2].strip()
            i += 3

        elif (
            i + 1 < len(lines)
            and lines[i].startswith("1 ")
            and lines[i + 1].startswith("2 ")
        ):
            line1 = lines[i].strip()
            line2 = lines[i + 1].strip()
            i += 2

        else:
            i += 1
            continue

        if not is_valid_tle_pair(line1, line2):
            continue

        norad = norad_from_tle1(line1)

        if norad is None:
            raw_field = line1[2:7] if len(line1) >= 7 else "?"
            print(f"[WARN] Cannot decode NORAD field raw={raw_field!r}")
            continue

        items.append({
            "norad_id": norad,
            "name": name or str(norad),
            "tle_epoch": parse_tle_epoch(line1),
            "tle1": line1,
            "tle2": line2,
        })

    return items


def dedupe_latest_by_norad(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[int, dict[str, Any]] = {}

    for item in items:
        norad = int(item["norad_id"])
        epoch = item.get("tle_epoch") or ""

        current = latest.get(norad)
        if current is None:
            latest[norad] = item
            continue

        old_epoch = current.get("tle_epoch") or ""
        if epoch > old_epoch:
            latest[norad] = item

    return list(latest.values())


def prune_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cutoff = now_utc() - timedelta(days=KEEP_DAYS)
    kept: list[dict[str, Any]] = []

    for item in history:
        t = item.get("fetched_at") or item.get("tle_epoch")
        if not t:
            kept.append(item)
            continue

        try:
            dt = datetime.fromisoformat(str(t).replace("Z", "+00:00"))
            if dt >= cutoff:
                kept.append(item)
        except Exception:
            kept.append(item)

    return kept


def should_append(history: list[dict[str, Any]], tle1: str, tle2: str) -> bool:
    if not history:
        return True

    last = history[-1]
    return last.get("tle1") != tle1 or last.get("tle2") != tle2


def update_one(item: dict[str, Any], fetched_at: str) -> int:
    norad = int(item["norad_id"])
    name = item.get("name") or str(norad)

    latest_obj = {
        "norad_id": norad,
        "name": name,
        "fetched_at": fetched_at,
        "tle_epoch": item.get("tle_epoch"),
        "tle1": item["tle1"],
        "tle2": item["tle2"],
    }

    save_json(TLE_DIR / f"{norad}.json", latest_obj)

    hist_path = TLE_HISTORY_DIR / f"{norad}.json"

    hist_obj = load_json(
        hist_path,
        {
            "norad_id": norad,
            "name": name,
            "tle_history": [],
        },
    )

    history = hist_obj.get("tle_history", [])
    if not isinstance(history, list):
        history = []

    history = prune_history(history)

    entry = {
        "fetched_at": fetched_at,
        "tle_epoch": item.get("tle_epoch"),
        "tle1": item["tle1"],
        "tle2": item["tle2"],
    }

    if should_append(history, item["tle1"], item["tle2"]):
        history.append(entry)

    save_json(
        hist_path,
        {
            "norad_id": norad,
            "name": name,
            "tle_history": history,
        },
    )

    return len(history)


def validate_alpha5_decoder() -> None:
    tests = {
        "00005": 5,
        "25544": 25544,
        "99999": 99999,
        "A0000": 100000,
        "A0001": 100001,
        "B0000": 110000,
        "H9999": 179999,
        "J0000": 180000,
        "Z9999": 339999,
    }

    for raw, expected in tests.items():
        actual = decode_alpha5_catalog_number(raw)
        if actual != expected:
            raise RuntimeError(
                f"Alpha-5 decoder test failed: {raw} -> {actual}, expected {expected}"
            )

    print("Alpha-5 decoder validation: OK")


def main() -> None:
    validate_alpha5_decoder()

    if not USERNAME or not PASSWORD:
        raise RuntimeError(
            "SPACETRACK_USERNAME / SPACETRACK_PASSWORD ãGitHub Secretsã«ããã¾ããã"
        )

    session = requests.Session()
    session.headers.update({
        "User-Agent": "space-weather-drag/2.0",
        "Accept": "text/plain,*/*",
    })

    print("Login Space-Track...")

    login_res = session.post(
        LOGIN_URL,
        data={
            "identity": USERNAME,
            "password": PASSWORD,
        },
        timeout=60,
    )

    if login_res.status_code != 200:
        raise RuntimeError(
            f"Space-Track login failed HTTP {login_res.status_code}: "
            f"{login_res.text[:300]}"
        )

    print("Fetch active GP TLE...")

    res = session.get(QUERY_URL, timeout=300)

    if res.status_code != 200:
        raise RuntimeError(
            f"Space-Track query failed HTTP {res.status_code}: "
            f"{res.text[:300]}"
        )

    items = dedupe_latest_by_norad(parse_space_track_tle(res.text))

    if not items:
        raise RuntimeError("No TLE items parsed from Space-Track response.")

    fetched_at = to_iso(now_utc())

    alpha5_count = sum(
        1 for item in items if int(item["norad_id"]) >= 100000
    )

    print(f"Parsed latest active TLE count: {len(items)}")
    print(f"Alpha-5 / NORAD >= 100000 count: {alpha5_count}")
    print(f"Fetched at: {fetched_at}")
    print(f"Keep days: {KEEP_DAYS}")

    ok = 0

    for item in items:
        hist_len = update_one(item, fetched_at)
        ok += 1

        if ok % 1000 == 0:
            print(
                f"Progress: {ok}/{len(items)} "
                f"last_norad={item['norad_id']} "
                f"history={hist_len}"
            )

    manifest = {
        "updated_at": fetched_at,
        "source": "Space-Track class/gp format/tle",
        "filter": {
            "decay_date": "null-val",
            "epoch": ">now-10",
            "latest_per_norad": True,
        },
        "alpha5_supported": True,
        "alpha5_count": alpha5_count,
        "keep_days": KEEP_DAYS,
        "count": ok,
    }

    save_json(TLE_HISTORY_DIR / "index.json", manifest)
    save_json(TLE_DIR / "index.json", manifest)

    print(f"Done. saved={ok}")


if __name__ == "__main__":
    main()