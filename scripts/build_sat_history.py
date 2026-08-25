#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from skyfield.api import EarthSatellite, load, wgs84


# ============================================================
# Paths
# ============================================================

ROOT = Path(__file__).resolve().parents[1]

CATALOG_DIR = ROOT / "docs/data/catalog"
HISTORY_DIR = ROOT / "docs/data/sat_history"

HISTORY_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


# ============================================================
# Settings
# ============================================================

KEEP_DAYS = 30

MU = 398600.4418
EARTH_RADIUS_KM = 6378.137

ACTIVE_GP_PATTERN = re.compile(
    r"^active_gp_(\d{4}-\d{2}-\d{2}T\d{4}Z)\.json$"
)

TS = load.timescale()


# ============================================================
# Time helpers
# ============================================================

def now_utc() -> datetime:
    return datetime.now(
        timezone.utc
    ).replace(
        microsecond=0
    )


def parse_iso_datetime(
    value: Any,
) -> datetime | None:

    if not value:
        return None

    try:
        dt = datetime.fromisoformat(
            str(value).replace(
                "Z",
                "+00:00",
            )
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.astimezone(
            timezone.utc
        )

    except Exception:
        return None


# ============================================================
# Numeric helpers
# ============================================================

def safe_float(
    value: Any,
    default: float | None = None,
) -> float | None:

    try:
        if value is None or value == "":
            return default

        return float(value)

    except Exception:
        return default


def safe_int(
    value: Any,
) -> int | None:

    try:
        return int(
            str(value).strip()
        )

    except Exception:
        return None


# ============================================================
# Orbit calculations
# ============================================================

def calc_sma_from_mean_motion(
    mean_motion_rev_day: float,
) -> float:

    n = (
        mean_motion_rev_day
        * 2.0
        * math.pi
        / 86400.0
    )

    return (
        MU / (n * n)
    ) ** (1.0 / 3.0)


# ============================================================
# Snapshot handling
# ============================================================

def parse_snapshot_datetime(
    filename: str,
) -> datetime | None:
    """
    active_gp_2026-08-25T0517Z.json
    ↓
    datetime
    """

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


def snapshot_files_in_range() -> list[Path]:
    """
    docs/data/catalog の中から

        active_gp_YYYY-MM-DDTHHMMZ.json

    のみを取得。

    直近 KEEP_DAYS 日だけ使用する。

    GitHub checkoutによってmtimeが変化する可能性があるため、
    ファイル更新時刻ではなく、
    ファイル名に含まれる日時を使用する。
    """

    if not CATALOG_DIR.exists():
        return []

    cutoff = (
        now_utc()
        - timedelta(
            days=KEEP_DAYS
        )
    )

    snapshots: list[
        tuple[datetime, Path]
    ] = []

    for path in CATALOG_DIR.iterdir():

        if not path.is_file():
            continue

        snapshot_time = (
            parse_snapshot_datetime(
                path.name
            )
        )

        if snapshot_time is None:
            continue

        if snapshot_time < cutoff:
            continue

        snapshots.append(
            (
                snapshot_time,
                path,
            )
        )

    snapshots.sort(
        key=lambda item: item[0]
    )

    return [
        path
        for _dt, path
        in snapshots
    ]


# ============================================================
# Load snapshot
# ============================================================

def load_snapshot(
    path: Path,
) -> list[dict[str, Any]]:

    try:

        with path.open(
            "r",
            encoding="utf-8",
        ) as f:

            data = json.load(
                f
            )

    except Exception as exc:

        print(
            f"[WARN] Could not load "
            f"{path.name}: {exc}"
        )

        return []

    if not isinstance(
        data,
        list,
    ):

        print(
            f"[WARN] Snapshot is not "
            f"a JSON array: {path.name}"
        )

        return []

    return data


# ============================================================
# Skyfield
# ============================================================

def skyfield_true_lat_lon_height(
    row: dict[str, Any],
    epoch_dt: datetime,
) -> tuple[
    float,
    float,
    float,
]:
    """
    CelesTrak OMM JSON
      ↓
    Skyfield EarthSatellite.from_omm
      ↓
    latitude / longitude / height
    """

    satellite = (
        EarthSatellite.from_omm(
            TS,
            row,
        )
    )

    t = TS.from_datetime(
        epoch_dt
    )

    geocentric = satellite.at(
        t
    )

    geographic = (
        wgs84.geographic_position_of(
            geocentric
        )
    )

    return (
        geographic.latitude.degrees,
        geographic.longitude.degrees,
        geographic.elevation.km,
    )


# ============================================================
# Build one orbital history entry
# ============================================================

def build_entry(
    row: dict[str, Any],
) -> dict[str, Any] | None:

    norad_id = safe_int(
        row.get(
            "NORAD_CAT_ID"
        )
    )

    if norad_id is None:
        return None

    name = (
        row.get("OBJECT_NAME")
        or row.get("OBJECT_ID")
        or f"NORAD-{norad_id}"
    )

    epoch_dt = parse_iso_datetime(
        row.get(
            "EPOCH"
        )
    )

    if epoch_dt is None:
        return None

    mean_motion = safe_float(
        row.get(
            "MEAN_MOTION"
        )
    )

    if (
        mean_motion is None
        or mean_motion <= 0
    ):
        return None

    eccentricity = (
        safe_float(
            row.get(
                "ECCENTRICITY"
            ),
            0.0,
        )
        or 0.0
    )

    inclination = (
        safe_float(
            row.get(
                "INCLINATION"
            ),
            0.0,
        )
        or 0.0
    )

    raan = (
        safe_float(
            row.get(
                "RA_OF_ASC_NODE"
            ),
            0.0,
        )
        or 0.0
    )

    arg_perigee = (
        safe_float(
            row.get(
                "ARG_OF_PERICENTER"
            ),
            0.0,
        )
        or 0.0
    )

    mean_anomaly = (
        safe_float(
            row.get(
                "MEAN_ANOMALY"
            ),
            0.0,
        )
        or 0.0
    )

    semi_major_axis = (
        calc_sma_from_mean_motion(
            mean_motion
        )
    )

    rp = (
        semi_major_axis
        * (1.0 - eccentricity)
    )

    ra = (
        semi_major_axis
        * (1.0 + eccentricity)
    )

    apogee_km = (
        ra
        - EARTH_RADIUS_KM
    )

    perigee_km = (
        rp
        - EARTH_RADIUS_KM
    )

    try:

        (
            latitude_deg,
            longitude_deg,
            height_km,
        ) = (
            skyfield_true_lat_lon_height(
                row,
                epoch_dt,
            )
        )

    except Exception as exc:

        print(
            "[WARN] Skyfield "
            f"NORAD={norad_id}: "
            f"{exc}"
        )

        return None

    epoch_iso = (
        epoch_dt
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )

    return {
        "norad_id": norad_id,

        "name": str(
            name
        ),

        "entry": {
            "epoch": epoch_iso,

            "apogee_km": (
                apogee_km
            ),

            "perigee_km": (
                perigee_km
            ),

            "latitude_deg": (
                latitude_deg
            ),

            "longitude_deg": (
                longitude_deg
            ),

            "height_km": (
                height_km
            ),

            "semi_major_axis_km": (
                semi_major_axis
            ),

            "eccentricity": (
                eccentricity
            ),

            "inclination_deg": (
                inclination
            ),

            "raan_deg": (
                raan
            ),

            "arg_perigee_deg": (
                arg_perigee
            ),

            "mean_anomaly_deg": (
                mean_anomaly
            ),

            "mean_motion_rev_day": (
                mean_motion
            ),
        },
    }


# ============================================================
# Build histories
# ============================================================

def rebuild_histories_from_snapshots(
    files: list[Path],
):

    by_sat = defaultdict(
        lambda: {
            "name": None,
            "history_by_epoch": {},
        }
    )

    total_rows = 0
    valid_rows = 0
    failed_rows = 0

    for snapshot_number, path in enumerate(
        files,
        start=1,
    ):

        print("")
        print(
            f"Snapshot "
            f"{snapshot_number}/{len(files)}"
        )

        print(
            f"Reading: {path.name}"
        )

        rows = load_snapshot(
            path
        )

        if not rows:
            print(
                "[WARN] No usable rows."
            )

            continue

        print(
            f"Rows: {len(rows)}"
        )

        total_rows += len(
            rows
        )

        for row in rows:

            if not isinstance(
                row,
                dict,
            ):
                failed_rows += 1
                continue

            built = build_entry(
                row
            )

            if built is None:
                failed_rows += 1
                continue

            valid_rows += 1

            norad_id = (
                built[
                    "norad_id"
                ]
            )

            name = (
                built[
                    "name"
                ]
            )

            entry = (
                built[
                    "entry"
                ]
            )

            by_sat[
                norad_id
            ][
                "name"
            ] = name

            # 同じTLE epochが複数snapshotにあった場合、
            # 同じ履歴点を重複保存しない。
            by_sat[
                norad_id
            ][
                "history_by_epoch"
            ][
                entry["epoch"]
            ] = entry

    return (
        by_sat,
        total_rows,
        valid_rows,
        failed_rows,
    )


# ============================================================
# Delete old generated history
# ============================================================

def clear_history_directory() -> int:
    """
    sat_historyは毎回30日分から完全再生成する。
    """

    removed = 0

    for path in HISTORY_DIR.glob(
        "*.json"
    ):

        try:
            path.unlink()

            removed += 1

        except Exception as exc:

            print(
                "[WARN] Could not "
                f"delete {path}: "
                f"{exc}"
            )

    return removed


# ============================================================
# Write histories
# ============================================================

def write_sat_history_files(
    by_sat,
) -> int:

    removed = (
        clear_history_directory()
    )

    print("")
    print(
        f"Old sat_history files "
        f"removed: {removed}"
    )

    written = 0

    for (
        norad_id,
        payload,
    ) in by_sat.items():

        history = list(
            payload[
                "history_by_epoch"
            ].values()
        )

        history.sort(
            key=lambda item: item[
                "epoch"
            ]
        )

        if not history:
            continue

        data = {
            "norad_id": (
                norad_id
            ),

            "name": (
                payload["name"]
                or f"NORAD-{norad_id}"
            ),

            "history": (
                history
            ),
        }

        out_path = (
            HISTORY_DIR
            / f"{norad_id}.json"
        )

        try:

            with out_path.open(
                "w",
                encoding="utf-8",
            ) as f:

                # indentを付けないことで
                # GitHub Pages容量を削減
                json.dump(
                    data,
                    f,
                    ensure_ascii=False,
                    separators=(
                        ",",
                        ":",
                    ),
                )

            written += 1

        except Exception as exc:

            print(
                "[WARN] Could not "
                f"write {out_path}: "
                f"{exc}"
            )

    return written


# ============================================================
# Directory size
# ============================================================

def directory_size_bytes(
    folder: Path,
) -> int:

    total = 0

    if not folder.exists():
        return 0

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

    value = float(
        size
    )

    for unit in (
        "B",
        "KB",
        "MB",
        "GB",
        "TB",
    ):

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

    print("")
    print(
        "========================================"
    )

    print(
        "SATELLITE HISTORY BUILD"
    )

    print(
        "========================================"
    )

    print(
        "Network access: DISABLED"
    )

    print(
        "CelesTrak direct requests: ZERO"
    )

    print(
        "Source: docs/data/catalog/"
        "active_gp_*.json"
    )

    print(
        f"Retention: {KEEP_DAYS} days"
    )

    # --------------------------------------------------------
    # Catalog existence
    # --------------------------------------------------------

    if not CATALOG_DIR.exists():

        raise SystemExit(
            "ERROR: "
            "docs/data/catalog "
            "does not exist."
        )

    # --------------------------------------------------------
    # Find snapshots
    # --------------------------------------------------------

    files = (
        snapshot_files_in_range()
    )

    if not files:

        print("")
        print(
            "Files currently in catalog:"
        )

        for path in sorted(
            CATALOG_DIR.iterdir()
        ):

            if path.is_file():
                print(
                    f"  {path.name}"
                )

        raise SystemExit(
            "ERROR: No active_gp "
            "snapshots found within "
            f"the last {KEEP_DAYS} days."
        )

    print("")
    print(
        f"Snapshots found: "
        f"{len(files)}"
    )

    print(
        f"Oldest: "
        f"{files[0].name}"
    )

    print(
        f"Newest: "
        f"{files[-1].name}"
    )

    # --------------------------------------------------------
    # Build
    # --------------------------------------------------------

    (
        by_sat,
        total_rows,
        valid_rows,
        failed_rows,
    ) = (
        rebuild_histories_from_snapshots(
            files
        )
    )

    if not by_sat:

        raise SystemExit(
            "ERROR: No satellite "
            "history could be generated."
        )

    # --------------------------------------------------------
    # Write
    # --------------------------------------------------------

    written = (
        write_sat_history_files(
            by_sat
        )
    )

    # --------------------------------------------------------
    # Validate
    # --------------------------------------------------------

    if written < 100:

        raise SystemExit(
            "ERROR: Too few "
            "sat_history files "
            f"written: {written}"
        )

    # --------------------------------------------------------
    # Size
    # --------------------------------------------------------

    history_size = (
        directory_size_bytes(
            HISTORY_DIR
        )
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
        "========================================"
    )

    print(
        "SATELLITE HISTORY COMPLETE"
    )

    print(
        "========================================"
    )

    print(
        f"Snapshots used: "
        f"{len(files)}"
    )

    print(
        f"Total input rows: "
        f"{total_rows}"
    )

    print(
        f"Valid rows: "
        f"{valid_rows}"
    )

    print(
        f"Skipped/failed rows: "
        f"{failed_rows}"
    )

    print(
        f"Satellites written: "
        f"{written}"
    )

    print("")
    print(
        f"Catalog size: "
        f"{human_size(catalog_size)}"
    )

    print(
        f"sat_history size: "
        f"{human_size(history_size)}"
    )

    print("")
    print(
        "Network access: DISABLED"
    )

    print(
        "CelesTrak calls from "
        "build_sat_history.py: 0"
    )


if __name__ == "__main__":
    main()
