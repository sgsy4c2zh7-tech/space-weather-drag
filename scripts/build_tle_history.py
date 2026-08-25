#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]

TLE_DIR = ROOT / "docs/data/tle"
TLE_HISTORY_DIR = ROOT / "docs/data/tle_history"

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


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def to_iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")

    tmp.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ),
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

        dt = (
            datetime(year, 1, 1, tzinfo=timezone.utc)
            + timedelta(days=doy - 1)
        )

        return to_iso(dt)

    except Exception:
        return None


def parse_space_track_tle(text: str) -> list[dict[str, Any]]:
    lines = [
        line.rstrip()
        for line in text.splitlines()
        if line.strip()
    ]

    items: list[dict[str, Any]] = []

    i = 0

    while i < len(lines):
        name = None

        # 3LE形式
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

        # 通常の2LE
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

            print(
                f"[WARN] Cannot decode NORAD field "
                f"raw={raw_field!r}"
            )

            continue

        items.append(
            {
                "norad_id": norad,
                "name": name or str(norad),
                "tle_epoch": parse_tle_epoch(line1),
                "tle1": line1,
                "tle2": line2,
            }
        )

    return items


def dedupe_latest_by_norad(
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:

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


def cleanup_old_tle_files(active_ids: set[int]) -> int:
    """
    今回のSpace-Track取得結果に存在しない衛星の
    古いTLE JSONを削除する。
    """

    removed = 0

    for path in TLE_DIR.glob("*.json"):

        if path.name == "index.json":
            continue

        try:
            norad = int(path.stem)

        except ValueError:
            print(f"[WARN] Unknown JSON file: {path}")
            continue

        if norad not in active_ids:
            path.unlink()

            removed += 1

    return removed


def remove_old_history_directory() -> None:
    """
    旧tle_historyを完全削除。
    今後TLE履歴は保存しない。
    """

    if TLE_HISTORY_DIR.exists():
        print(
            f"Removing obsolete TLE history directory: "
            f"{TLE_HISTORY_DIR}"
        )

        shutil.rmtree(TLE_HISTORY_DIR)


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
                f"Alpha-5 decoder test failed: "
                f"{raw} -> {actual}, expected {expected}"
            )

    print("Alpha-5 decoder validation: OK")


def main() -> None:

    validate_alpha5_decoder()

    if not USERNAME or not PASSWORD:
        raise RuntimeError(
            "SPACETRACK_USERNAME / "
            "SPACETRACK_PASSWORD が "
            "GitHub Secrets にありません。"
        )

    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": "space-weather-drag/3.0",
            "Accept": "text/plain,*/*",
        }
    )

    # -------------------------
    # Login
    # -------------------------

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
            f"Space-Track login failed "
            f"HTTP {login_res.status_code}: "
            f"{login_res.text[:300]}"
        )

    # -------------------------
    # Fetch
    # -------------------------

    print("Fetch active GP TLE...")

    res = session.get(
        QUERY_URL,
        timeout=300,
    )

    if res.status_code != 200:
        raise RuntimeError(
            f"Space-Track query failed "
            f"HTTP {res.status_code}: "
            f"{res.text[:300]}"
        )

    parsed = parse_space_track_tle(res.text)

    items = dedupe_latest_by_norad(parsed)

    if not items:
        raise RuntimeError(
            "No TLE items parsed from Space-Track response."
        )

    fetched_at = to_iso(now_utc())

    alpha5_count = sum(
        1
        for item in items
        if int(item["norad_id"]) >= 100000
    )

    print(
        f"Parsed latest active TLE count: "
        f"{len(items)}"
    )

    print(
        f"Alpha-5 / NORAD >= 100000 count: "
        f"{alpha5_count}"
    )

    print(
        f"Fetched at: "
        f"{fetched_at}"
    )

    # -------------------------
    # Save latest TLE only
    # -------------------------

    active_ids: set[int] = set()

    for index, item in enumerate(items, start=1):

        norad = int(item["norad_id"])

        active_ids.add(norad)

        obj = {
            "norad_id": norad,
            "name": item.get("name") or str(norad),
            "fetched_at": fetched_at,
            "tle_epoch": item.get("tle_epoch"),
            "tle1": item["tle1"],
            "tle2": item["tle2"],
        }

        save_json(
            TLE_DIR / f"{norad}.json",
            obj,
        )

        if index % 1000 == 0:
            print(
                f"Progress: "
                f"{index}/{len(items)} "
                f"last_norad={norad}"
            )

    # -------------------------
    # Remove stale satellites
    # -------------------------

    removed = cleanup_old_tle_files(
        active_ids
    )

    # -------------------------
    # Remove old history
    # -------------------------

    remove_old_history_directory()

    # -------------------------
    # Manifest
    # -------------------------

    manifest = {
        "updated_at": fetched_at,
        "source": "Space-Track class/gp format/tle",
        "filter": {
            "decay_date": "null-val",
            "epoch": ">now-10",
            "latest_per_norad": True,
        },
        "storage_mode": "latest_only",
        "history_enabled": False,
        "alpha5_supported": True,
        "alpha5_count": alpha5_count,
        "count": len(items),
        "stale_files_removed": removed,
    }

    save_json(
        TLE_DIR / "index.json",
        manifest,
    )

    print("")
    print("================================")
    print("TLE UPDATE COMPLETE")
    print("================================")

    print(
        f"Latest TLE files saved: "
        f"{len(items)}"
    )

    print(
        f"Old satellite files removed: "
        f"{removed}"
    )

    print(
        "TLE history: DISABLED"
    )

    print(
        "Storage mode: "
        "1 satellite = 1 latest TLE"
    )


if __name__ == "__main__":
    main()
