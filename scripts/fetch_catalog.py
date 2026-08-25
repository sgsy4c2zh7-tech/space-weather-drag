#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# ============================================================
# Paths
# ============================================================

ROOT = Path(__file__).resolve().parents[1]

CATALOG_DIR = ROOT / "docs/data/catalog"
LATEST_INDEX = ROOT / "docs/data/satellites.json"

CATALOG_DIR.mkdir(parents=True, exist_ok=True)
LATEST_INDEX.parent.mkdir(parents=True, exist_ok=True)


# ============================================================
# Settings
# ============================================================

KEEP_DAYS = 30

CELESTRAK_ACTIVE_JSON = (
    "https://celestrak.org/NORAD/elements/"
    "gp.php?GROUP=active&FORMAT=json"
)

SATCAT_CSV_URLS = [
    "https://celestrak.org/pub/satcat.csv",
    "https://www.celestrak.org/pub/satcat.csv",
]

USER_AGENT = (
    "space-weather-drag/3.0 "
    "(GitHub Actions; catalog updater)"
)


# ============================================================
# Filename patterns
# ============================================================

ACTIVE_GP_PATTERN = re.compile(
    r"^active_gp_(\d{4}-\d{2}-\d{2}T\d{4}Z)\.json$"
)

LEGACY_SATCAT_PATTERN = re.compile(
    r"^satcat_(\d{4}-\d{2}-\d{2}T\d{4}Z)\.json$"
)


# ============================================================
# Basic helpers
# ============================================================

def now_utc() -> datetime:
    return datetime.now(
        timezone.utc
    ).replace(
        microsecond=0
    )


def fetch_text(
    url: str,
    timeout: int = 180,
) -> str:

    req = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": (
                "application/json,"
                "text/csv,"
                "text/plain,"
                "*/*"
            ),
        },
    )

    with urlopen(
        req,
        timeout=timeout,
    ) as resp:

        return resp.read().decode(
            "utf-8",
            errors="replace",
        )


def fetch_json(
    url: str,
) -> Any:

    text = fetch_text(
        url
    )

    return json.loads(
        text
    )


def load_json(
    path: Path,
    default: Any = None,
) -> Any:

    if not path.exists():
        return default

    try:
        with path.open(
            "r",
            encoding="utf-8",
        ) as f:

            return json.load(f)

    except Exception as exc:

        print(
            f"[WARN] Failed to load JSON "
            f"{path}: {exc}"
        )

        return default


def save_json(
    path: Path,
    data: Any,
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    tmp = path.with_suffix(
        path.suffix + ".tmp"
    )

    with tmp.open(
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2,
        )

    tmp.replace(
        path
    )


def safe_int(
    value: Any,
) -> int | None:

    try:
        return int(
            str(value).strip()
        )

    except Exception:
        return None


def pick_first(
    row: dict[str, Any],
    keys: list[str],
    default: Any = None,
) -> Any:

    for key in keys:

        if (
            key in row
            and row[key] not in [
                None,
                "",
            ]
        ):
            return row[key]

    return default


# ============================================================
# Active GP snapshot handling
# ============================================================

def parse_active_gp_datetime(
    filename: str,
) -> datetime | None:

    match = ACTIVE_GP_PATTERN.match(
        filename
    )

    if not match:
        return None

    try:

        return datetime.strptime(
            match.group(1),
            "%Y-%m-%dT%H%MZ",
        ).replace(
            tzinfo=timezone.utc
        )

    except Exception:
        return None


def list_active_gp_snapshots() -> list[
    tuple[datetime, Path]
]:

    snapshots = []

    for path in CATALOG_DIR.iterdir():

        if not path.is_file():
            continue

        dt = parse_active_gp_datetime(
            path.name
        )

        if dt is None:
            continue

        snapshots.append(
            (
                dt,
                path,
            )
        )

    snapshots.sort(
        key=lambda x: x[0]
    )

    return snapshots


def latest_active_gp_cache() -> Path | None:

    snapshots = (
        list_active_gp_snapshots()
    )

    if not snapshots:
        return None

    return snapshots[-1][1]


def load_active_gp_cache(
    path: Path,
) -> list[dict[str, Any]]:

    rows = load_json(
        path,
        default=None,
    )

    if (
        not isinstance(rows, list)
        or len(rows) == 0
    ):
        raise RuntimeError(
            "Cached active GP snapshot "
            f"is invalid: {path}"
        )

    print(
        f"Loaded cached GP snapshot: "
        f"{path}"
    )

    print(
        f"Cached GP rows: "
        f"{len(rows)}"
    )

    return rows


def fetch_active_gp() -> tuple[
    list[dict[str, Any]],
    bool,
    Path | None,
]:
    """
    Returns:
        rows
        downloaded_new
        cache_used
    """

    print("")
    print(
        "================================"
    )
    print(
        "FETCH CELESTRAK ACTIVE GP"
    )
    print(
        "================================"
    )

    try:

        rows = fetch_json(
            CELESTRAK_ACTIVE_JSON
        )

        if (
            not isinstance(rows, list)
            or len(rows) == 0
        ):
            raise RuntimeError(
                "CelesTrak active GP "
                "returned no rows."
            )

        print(
            f"CelesTrak active GP "
            f"downloaded successfully."
        )

        print(
            f"GP rows: "
            f"{len(rows)}"
        )

        return (
            rows,
            True,
            None,
        )

    except HTTPError as exc:

        print(
            f"[WARN] CelesTrak GP HTTP "
            f"{exc.code}: {exc.reason}"
        )

        try:

            response_body = (
                exc.read()
                .decode(
                    "utf-8",
                    errors="replace",
                )
            )

            if response_body:
                print(
                    "CelesTrak response:"
                )

                print(
                    response_body[:1000]
                )

        except Exception:
            pass

        cache = (
            latest_active_gp_cache()
        )

        if cache is None:

            raise RuntimeError(
                "CelesTrak GP request failed "
                f"with HTTP {exc.code}, and "
                "no cached active_gp snapshot "
                "exists."
            ) from exc

        print(
            "[WARN] Falling back to "
            "latest cached active GP."
        )

        rows = (
            load_active_gp_cache(
                cache
            )
        )

        return (
            rows,
            False,
            cache,
        )

    except (
        URLError,
        TimeoutError,
        OSError,
        json.JSONDecodeError,
    ) as exc:

        print(
            "[WARN] CelesTrak GP "
            f"download failed: {exc}"
        )

        cache = (
            latest_active_gp_cache()
        )

        if cache is None:

            raise RuntimeError(
                "CelesTrak GP download "
                "failed and no cached "
                "active_gp snapshot exists."
            ) from exc

        print(
            "[WARN] Falling back to "
            "latest cached active GP."
        )

        rows = (
            load_active_gp_cache(
                cache
            )
        )

        return (
            rows,
            False,
            cache,
        )


# ============================================================
# SATCAT
# ============================================================

def load_cached_satcat() -> list[
    dict[str, Any]
]:

    path = (
        CATALOG_DIR
        / "satcat_latest.json"
    )

    rows = load_json(
        path,
        default=[],
    )

    if isinstance(
        rows,
        list,
    ):
        return rows

    return []


def fetch_satcat_csv() -> tuple[
    list[dict[str, Any]],
    bool,
]:

    print("")
    print(
        "================================"
    )

    print(
        "FETCH CELESTRAK SATCAT"
    )

    print(
        "================================"
    )

    last_error = None

    for url in SATCAT_CSV_URLS:

        try:

            print(
                f"Trying SATCAT: {url}"
            )

            text = fetch_text(
                url
            )

            rows = list(
                csv.DictReader(
                    text.splitlines()
                )
            )

            if rows:

                print(
                    "SATCAT downloaded "
                    f"successfully: {url}"
                )

                print(
                    f"SATCAT rows: "
                    f"{len(rows)}"
                )

                return (
                    rows,
                    True,
                )

        except Exception as exc:

            last_error = exc

            print(
                "[WARN] SATCAT fetch "
                f"failed: {url}"
            )

            print(
                f"[WARN] {exc}"
            )

    cached = (
        load_cached_satcat()
    )

    if cached:

        print(
            "[WARN] All SATCAT "
            "downloads failed."
        )

        print(
            "Using cached "
            "satcat_latest.json"
        )

        print(
            f"Cached SATCAT rows: "
            f"{len(cached)}"
        )

        return (
            cached,
            False,
        )

    print(
        "[WARN] SATCAT unavailable "
        "and no cache exists."
    )

    print(
        f"[WARN] Last error: "
        f"{last_error}"
    )

    return (
        [],
        False,
    )


# ============================================================
# SATCAT map
# ============================================================

def build_satcat_map(
    satcat_rows: list[
        dict[str, Any]
    ],
) -> dict[
    int,
    dict[str, Any],
]:

    out = {}

    for row in satcat_rows:

        norad = safe_int(
            pick_first(
                row,
                [
                    "NORAD_CAT_ID",
                    "NORAD",
                    "CATNR",
                    "OBJECT_NUMBER",
                ],
            )
        )

        if norad is None:
            continue

        country = pick_first(
            row,
            [
                "COUNTRY",
                "OWNER",
                "COUNTRY_CODE",
                "LAUNCHING_STATE",
            ],
            "UNK",
        )

        out[norad] = {

            "country_code": (
                str(country)
                .strip()
                .upper()
                if country
                else "UNK"
            ),

            "object_type": (
                pick_first(
                    row,
                    [
                        "OBJECT_TYPE",
                        "TYPE",
                    ],
                )
            ),

            "launch_site": (
                pick_first(
                    row,
                    [
                        "LAUNCH_SITE",
                        "SITE",
                    ],
                )
            ),

            "launch_date": (
                pick_first(
                    row,
                    [
                        "LAUNCH_DATE",
                    ],
                )
            ),

            "decay_date": (
                pick_first(
                    row,
                    [
                        "DECAY_DATE",
                    ],
                )
            ),
        }

    return out


# ============================================================
# Satellite index
# ============================================================

def build_satellite_index(
    gp_rows: list[
        dict[str, Any]
    ],
    satcat_map: dict[
        int,
        dict[str, Any],
    ],
) -> list[
    dict[str, Any]
]:

    out = []

    for row in gp_rows:

        norad = safe_int(
            row.get(
                "NORAD_CAT_ID"
            )
        )

        if norad is None:
            continue

        name = row.get(
            "OBJECT_NAME"
        )

        satcat = satcat_map.get(
            norad,
            {},
        )

        out.append(
            {
                "norad_id": norad,

                "name": (
                    name
                    or f"NORAD-{norad}"
                ),

                "country_code": (
                    satcat.get(
                        "country_code",
                        "UNK",
                    )
                ),

                "object_type": (
                    satcat.get(
                        "object_type"
                    )
                ),

                "launch_site": (
                    satcat.get(
                        "launch_site"
                    )
                ),

                "launch_date": (
                    satcat.get(
                        "launch_date"
                    )
                ),

                "decay_date": (
                    satcat.get(
                        "decay_date"
                    )
                ),
            }
        )

    out.sort(
        key=lambda x: x[
            "norad_id"
        ]
    )

    return out


# ============================================================
# Cleanup GP snapshots
# ============================================================

def cleanup_old_active_gp_files(
    days: int = KEEP_DAYS,
) -> int:
    """
    ファイルmtimeではなく、
    active_gp_YYYY-MM-DDTHHMMZ.json
    の日時で判断する。
    """

    cutoff = (
        now_utc()
        - timedelta(
            days=days
        )
    )

    snapshots = (
        list_active_gp_snapshots()
    )

    removed = 0

    # 最低でも最新1件は残す
    newest_path = (
        snapshots[-1][1]
        if snapshots
        else None
    )

    for (
        snapshot_time,
        path,
    ) in snapshots:

        if (
            newest_path is not None
            and path == newest_path
        ):
            continue

        if snapshot_time >= cutoff:
            continue

        try:

            path.unlink()

            removed += 1

            print(
                "Deleted old GP "
                f"snapshot: {path}"
            )

        except Exception as exc:

            print(
                "[WARN] Failed to "
                f"delete {path}: "
                f"{exc}"
            )

    return removed


# ============================================================
# Cleanup old SATCAT snapshots
# ============================================================

def cleanup_legacy_satcat_snapshots() -> int:
    """
    satcat_2026-01-01T0000Z.json
    のような旧スナップショットを削除。

    satcat_latest.json は削除しない。
    """

    removed = 0

    for path in CATALOG_DIR.iterdir():

        if not path.is_file():
            continue

        if not LEGACY_SATCAT_PATTERN.match(
            path.name
        ):
            continue

        try:

            path.unlink()

            removed += 1

            print(
                "Deleted obsolete "
                f"SATCAT snapshot: "
                f"{path}"
            )

        except Exception as exc:

            print(
                "[WARN] Failed to "
                f"delete {path}: "
                f"{exc}"
            )

    return removed


# ============================================================
# Stats
# ============================================================

def directory_size_bytes(
    folder: Path,
) -> int:

    total = 0

    for path in folder.rglob(
        "*"
    ):

        if not path.is_file():
            continue

        try:
            total += (
                path.stat().st_size
            )

        except OSError:
            pass

    return total


def human_size(
    size: int,
) -> str:

    value = float(size)

    for unit in [
        "B",
        "KB",
        "MB",
        "GB",
        "TB",
    ]:

        if value < 1024.0:
            return (
                f"{value:.2f} {unit}"
            )

        value /= 1024.0

    return (
        f"{value:.2f} PB"
    )


# ============================================================
# Main
# ============================================================

def main() -> None:

    started_at = now_utc()

    print("")
    print(
        "================================"
    )

    print(
        "CATALOG UPDATE START"
    )

    print(
        "================================"
    )

    print(
        f"Started at: "
        f"{started_at.isoformat()}"
    )

    print(
        f"GP retention: "
        f"{KEEP_DAYS} days"
    )

    # --------------------------------------------------------
    # Active GP
    # --------------------------------------------------------

    (
        gp_rows,
        gp_downloaded,
        gp_cache_used,
    ) = fetch_active_gp()

    # --------------------------------------------------------
    # SATCAT
    # --------------------------------------------------------

    (
        satcat_rows,
        satcat_downloaded,
    ) = fetch_satcat_csv()

    satcat_map = (
        build_satcat_map(
            satcat_rows
        )
    )

    # --------------------------------------------------------
    # Timestamp
    # --------------------------------------------------------

    now = now_utc()

    stamp = now.strftime(
        "%Y-%m-%dT%H%MZ"
    )

    # --------------------------------------------------------
    # Write new GP snapshot only when actually downloaded
    # --------------------------------------------------------

    gp_snap_path = None

    if gp_downloaded:

        gp_snap_path = (
            CATALOG_DIR
            / f"active_gp_{stamp}.json"
        )

        save_json(
            gp_snap_path,
            gp_rows,
        )

        print("")
        print(
            "New GP snapshot saved:"
        )

        print(
            gp_snap_path
        )

    else:

        print("")
        print(
            "No new GP snapshot created."
        )

        print(
            "Cached GP data is being used."
        )

        if gp_cache_used:

            print(
                f"Cache: "
                f"{gp_cache_used}"
            )

    # --------------------------------------------------------
    # SATCAT latest only
    # --------------------------------------------------------

    satcat_latest_path = (
        CATALOG_DIR
        / "satcat_latest.json"
    )

    if satcat_rows:

        save_json(
            satcat_latest_path,
            satcat_rows,
        )

        print(
            f"SATCAT latest saved: "
            f"{satcat_latest_path}"
        )

    # --------------------------------------------------------
    # Satellite search/basic index
    # --------------------------------------------------------

    sat_index = (
        build_satellite_index(
            gp_rows,
            satcat_map,
        )
    )

    if not sat_index:

        raise RuntimeError(
            "Satellite index is empty."
        )

    save_json(
        LATEST_INDEX,
        sat_index,
    )

    # --------------------------------------------------------
    # Cleanup
    # --------------------------------------------------------

    print("")
    print(
        "================================"
    )

    print(
        "CLEANUP"
    )

    print(
        "================================"
    )

    removed_gp = (
        cleanup_old_active_gp_files(
            days=KEEP_DAYS
        )
    )

    removed_satcat = (
        cleanup_legacy_satcat_snapshots()
    )

    # --------------------------------------------------------
    # Stats
    # --------------------------------------------------------

    snapshots = (
        list_active_gp_snapshots()
    )

    with_country = sum(
        1
        for item in sat_index
        if item.get(
            "country_code"
        )
        != "UNK"
    )

    catalog_size = (
        directory_size_bytes(
            CATALOG_DIR
        )
    )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    print("")
    print(
        "================================"
    )

    print(
        "CATALOG UPDATE COMPLETE"
    )

    print(
        "================================"
    )

    print(
        f"GP source: "
        f"{'downloaded' if gp_downloaded else 'cache'}"
    )

    print(
        f"GP rows: "
        f"{len(gp_rows)}"
    )

    print(
        f"GP snapshots retained: "
        f"{len(snapshots)}"
    )

    print(
        f"GP retention: "
        f"{KEEP_DAYS} days"
    )

    print(
        f"Old GP snapshots removed: "
        f"{removed_gp}"
    )

    print("")
    print(
        f"SATCAT source: "
        f"{'downloaded' if satcat_downloaded else 'cache'}"
    )

    print(
        f"SATCAT rows: "
        f"{len(satcat_rows)}"
    )

    print(
        "SATCAT storage: "
        "latest only"
    )

    print(
        "Legacy SATCAT snapshots "
        f"removed: {removed_satcat}"
    )

    print("")
    print(
        f"Satellite index rows: "
        f"{len(sat_index)}"
    )

    print(
        f"With country_code: "
        f"{with_country}"
    )

    print(
        f"Without country_code: "
        f"{len(sat_index) - with_country}"
    )

    print("")
    print(
        f"Catalog directory size: "
        f"{human_size(catalog_size)}"
    )

    print(
        f"Satellite index: "
        f"{LATEST_INDEX}"
    )

    print("")
    print(
        "Storage policy:"
    )

    print(
        f"  active_gp = "
        f"{KEEP_DAYS} days"
    )

    print(
        "  satcat = latest only"
    )


if __name__ == "__main__":
    main()
