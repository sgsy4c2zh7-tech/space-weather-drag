#!/usr/bin/env python3
"""
Hybrid Satellite Viewer 用・検索インデックス生成スクリプト。

生成ファイル
------------
docs/data/satellite_index_full.json
    全アクティブ物体

docs/data/satellite_index_geo_payload.json
    GEO判定されたペイロード

docs/data/satellite_index_payload.json
    GEO以外のペイロード

docs/data/satellite_index_non_geo_payload.json
    satellite_index_payload.json と同内容の互換ファイル

docs/data/satellite_index_other.json
    OBJECT A/B/C、Rocket Body、Debris、Unknown 等

docs/data/satellite_index_manifest.json
    各データセットの件数・ファイル名

入力
----
docs/data/sat_history/*.json
docs/data/catalog/satcat_full.json
docs/data/catalog/*.json

基本方針
--------
・sat_history がある物体は、最新の履歴値を優先する。
・SATCAT metadata をマージして OBJECT_NAME / OBJECT_ID / OBJECT_TYPE 等を付加する。
・sat_history に存在しなくても、未減衰のSATCAT物体は検索用最小レコードとして追加する。
・GEO Payload / Non-GEO Payload / Other Objects / ALL に分割して、HTMLの初期負荷を減らす。
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


# ============================================================
# Paths / settings
# ============================================================

SAT_HISTORY_DIR = Path("docs/data/sat_history")
CATALOG_DIR = Path("docs/data/catalog")
SATCAT_FULL_FILE = CATALOG_DIR / "satcat_full.json"

OUT_DIR = Path("docs/data")
OUT_FULL = OUT_DIR / "satellite_index_full.json"
OUT_GEO_PAYLOAD = OUT_DIR / "satellite_index_geo_payload.json"
OUT_NON_GEO_PAYLOAD = OUT_DIR / "satellite_index_payload.json"
OUT_NON_GEO_PAYLOAD_ALIAS = OUT_DIR / "satellite_index_non_geo_payload.json"
OUT_OTHER = OUT_DIR / "satellite_index_other.json"
OUT_MANIFEST = OUT_DIR / "satellite_index_manifest.json"

# False: 減衰済みSATCAT物体は、sat_historyに存在する場合だけ残す。
# True : 過去に減衰した物体も全件インデックスへ含める。
INCLUDE_DECAYED_OBJECTS = False

# JSONをインデントするとファイルが大きくなるため、通常はFalse推奨。
PRETTY_JSON = False

# GEO判定。現在のViewerの閾値と互換性を持たせつつ、少し余裕を持たせる。
GEO_PERIGEE_MIN_KM = 34000.0
GEO_APOGEE_MAX_KM = 38000.0
GEO_MEAN_ALT_MIN_KM = 34000.0
GEO_MEAN_ALT_MAX_KM = 38000.0
GEO_MEAN_MOTION_MIN = 0.85
GEO_MEAN_MOTION_MAX = 1.15
GEO_SMA_MIN_KM = 41000.0
GEO_SMA_MAX_KM = 44000.0


# ============================================================
# Generic helpers
# ============================================================

def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        result = float(str(value).strip())
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def safe_date(value: Any) -> str | None:
    text = safe_text(value)
    if not text:
        return None
    return text[:10] if len(text) >= 10 else text


def read_json(path: Path, default: Any = None) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(f"skip/read error {path}: {exc}")
        return default


def atomic_write_json(path: Path, data: Any, pretty: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")

    with tmp.open("w", encoding="utf-8") as f:
        if pretty:
            json.dump(data, f, ensure_ascii=False, indent=2)
        else:
            json.dump(
                data,
                f,
                ensure_ascii=False,
                separators=(",", ":"),
            )

    tmp.replace(path)


def normalize_country(value: Any) -> str:
    country = str(value or "").strip().upper()

    aliases = {
        "USA": "US",
        "U.S.": "US",
        "UNITED STATES": "US",
        "UNITED STATES OF AMERICA": "US",

        "PRC": "CHN",
        "CN": "CHN",
        "CHINA": "CHN",
        "PEOPLE'S REPUBLIC OF CHINA": "CHN",
        "PEOPLES REPUBLIC OF CHINA": "CHN",

        "CIS": "RUS",
        "RU": "RUS",
        "RUSSIA": "RUS",
        "USSR": "RUS",

        "JP": "JPN",
        "JAPAN": "JPN",
    }

    return aliases.get(country, country or "UNK")


def normalize_object_type(value: Any, object_name: Any = None) -> str:
    raw = str(value or "").strip().upper()
    name = str(object_name or "").strip().upper()

    if raw in {"PAYLOAD", "PL"}:
        return "PAYLOAD"

    if raw in {"ROCKET BODY", "R/B", "ROCKETBODY", "RB"}:
        return "ROCKET BODY"

    if raw in {"DEBRIS", "DEB", "FRAGMENT", "FRAGMENTATION DEBRIS"}:
        return "DEBRIS"

    # object_type欠落時の名称フォールバック
    if re.search(r"(?:^|\s)R/B(?:\s|$)", name):
        return "ROCKET BODY"

    if re.search(r"(?:^|\s)DEB(?:\s|$)", name):
        return "DEBRIS"

    return raw or "UNKNOWN"


GENERIC_OBJECT_NAME_RE = re.compile(
    r"^(?:"
    r"OBJECT(?:\s|[-_])?[A-Z0-9]+|"
    r"TBA(?:\s|[-_])?[A-Z0-9]*|"
    r"UNKNOWN(?:\s|[-_])?[A-Z0-9]*|"
    r"UNIDENTIFIED(?:\s|[-_])?[A-Z0-9]*|"
    r"OBJ(?:ECT)?(?:\s|[-_])?[A-Z0-9]+"
    r")$",
    re.IGNORECASE,
)


def is_generic_object_name(name: Any) -> bool:
    text = str(name or "").strip()
    return bool(text and GENERIC_OBJECT_NAME_RE.match(text))


def latest_history_row(history: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not history:
        return None

    valid = [row for row in history if isinstance(row, dict)]
    if not valid:
        return None

    with_epoch = [row for row in valid if row.get("epoch")]
    if with_epoch:
        return max(with_epoch, key=lambda row: str(row.get("epoch")))

    return valid[-1]


def merge_missing(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    """baseに無い値だけincomingから補完する。"""
    for key, value in incoming.items():
        if value is None or value == "":
            continue
        if base.get(key) is None or base.get(key) == "":
            base[key] = value
    return base


# ============================================================
# Catalog metadata
# ============================================================

def extract_catalog_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]

    if isinstance(data, dict):
        for key in ("objects", "rows", "data", "results", "satellites"):
            value = data.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]

    return []


def catalog_row_to_metadata(row: dict[str, Any]) -> tuple[int, dict[str, Any]] | None:
    norad = safe_int(
        first_non_empty(
            row.get("norad_id"),
            row.get("NORAD_CAT_ID"),
            row.get("NORAD"),
            row.get("CATNR"),
            row.get("catalog_number"),
        )
    )
    if norad is None:
        return None

    object_name = safe_text(
        first_non_empty(
            row.get("object_name"),
            row.get("OBJECT_NAME"),
            row.get("name"),
            row.get("OBJECT"),
            row.get("SATNAME"),
        )
    )

    raw_country = safe_text(
        first_non_empty(
            row.get("raw_country"),
            row.get("COUNTRY"),
            row.get("Country"),
            row.get("country"),
            row.get("COUNTRY_CODE"),
            row.get("country_code"),
            row.get("OWNER"),
            row.get("Owner"),
            row.get("owner"),
            row.get("OBJECT_COUNTRY"),
            row.get("OBJECT_OWNER"),
            row.get("LAUNCHING_STATE"),
        )
    )

    object_type = normalize_object_type(
        first_non_empty(
            row.get("object_type"),
            row.get("OBJECT_TYPE"),
            row.get("type"),
            row.get("TYPE"),
        ),
        object_name,
    )

    metadata = {
        "norad_id": norad,
        "object_name": object_name,
        "object_id": safe_text(
            first_non_empty(
                row.get("object_id"),
                row.get("OBJECT_ID"),
                row.get("INTLDES"),
                row.get("international_designator"),
                row.get("COSPAR_ID"),
            )
        ),
        "object_type": object_type,
        "owner": normalize_country(raw_country),
        "country_code": normalize_country(raw_country),
        "country": normalize_country(raw_country),
        "raw_country": raw_country,
        "launch_date": safe_date(
            first_non_empty(
                row.get("launch_date"),
                row.get("LAUNCH"),
                row.get("LAUNCH_DATE"),
            )
        ),
        "decay_date": safe_date(
            first_non_empty(
                row.get("decay_date"),
                row.get("DECAY"),
                row.get("DECAY_DATE"),
            )
        ),
        "site": safe_text(
            first_non_empty(
                row.get("site"),
                row.get("SITE"),
                row.get("LAUNCH_SITE"),
            )
        ),
        "rcs": first_non_empty(
            row.get("rcs"),
            row.get("RCS"),
            row.get("RCS_SIZE"),
        ),
        "comment": safe_text(
            first_non_empty(
                row.get("comment"),
                row.get("COMMENT"),
            )
        ),
        "epoch": safe_text(
            first_non_empty(
                row.get("epoch"),
                row.get("EPOCH"),
            )
        ),
        "apogee_km": safe_float(
            first_non_empty(
                row.get("apogee_km"),
                row.get("APOGEE"),
            )
        ),
        "perigee_km": safe_float(
            first_non_empty(
                row.get("perigee_km"),
                row.get("PERIGEE"),
            )
        ),
        "height_km": safe_float(
            first_non_empty(
                row.get("height_km"),
                row.get("HEIGHT"),
            )
        ),
        "inclination_deg": safe_float(
            first_non_empty(
                row.get("inclination_deg"),
                row.get("INCLINATION"),
            )
        ),
        "latitude_deg": safe_float(
            first_non_empty(
                row.get("latitude_deg"),
                row.get("LATITUDE"),
            )
        ),
        "longitude_deg": safe_float(
            first_non_empty(
                row.get("longitude_deg"),
                row.get("LONGITUDE"),
            )
        ),
        "semi_major_axis_km": safe_float(
            first_non_empty(
                row.get("semi_major_axis_km"),
                row.get("SEMIMAJOR_AXIS"),
                row.get("SEMIMAJOR_AXIS_KM"),
            )
        ),
        "eccentricity": safe_float(
            first_non_empty(
                row.get("eccentricity"),
                row.get("ECCENTRICITY"),
            )
        ),
        "raan_deg": safe_float(
            first_non_empty(
                row.get("raan_deg"),
                row.get("RA_OF_ASC_NODE"),
                row.get("RAAN"),
            )
        ),
        "arg_perigee_deg": safe_float(
            first_non_empty(
                row.get("arg_perigee_deg"),
                row.get("ARG_OF_PERICENTER"),
                row.get("ARG_PERIGEE"),
            )
        ),
        "mean_anomaly_deg": safe_float(
            first_non_empty(
                row.get("mean_anomaly_deg"),
                row.get("MEAN_ANOMALY"),
            )
        ),
        "mean_motion_rev_day": safe_float(
            first_non_empty(
                row.get("mean_motion_rev_day"),
                row.get("MEAN_MOTION"),
            )
        ),
        "period_min": safe_float(
            first_non_empty(
                row.get("period_min"),
                row.get("PERIOD"),
            )
        ),
    }

    return norad, metadata


def load_catalog_metadata() -> dict[int, dict[str, Any]]:
    metadata_map: dict[int, dict[str, Any]] = {}

    files: list[Path] = []
    if SATCAT_FULL_FILE.exists():
        files.append(SATCAT_FULL_FILE)

    # satcat_fullを最優先。その後、他のカタログで欠損項目のみ補完。
    other_files = sorted(
        (
            path
            for path in CATALOG_DIR.glob("*.json")
            if path != SATCAT_FULL_FILE
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    files.extend(other_files)

    print("===== catalog files =====")
    for path in files[:30]:
        print(path)

    for path in files:
        data = read_json(path, default=None)
        rows = extract_catalog_rows(data)

        if not rows:
            print(f"skip non-row catalog: {path}")
            continue

        found = 0
        for row in rows:
            parsed = catalog_row_to_metadata(row)
            if not parsed:
                continue

            norad, metadata = parsed
            if norad not in metadata_map:
                metadata_map[norad] = metadata
            else:
                merge_missing(metadata_map[norad], metadata)
            found += 1

        print(f"catalog metadata: {path} -> {found}")

    print(f"TOTAL catalog metadata: {len(metadata_map)}")
    return metadata_map


# ============================================================
# sat_history
# ============================================================

def history_object_to_record(
    obj: dict[str, Any],
    source_path: Path,
    metadata: dict[str, Any] | None,
) -> tuple[int, dict[str, Any]] | None:
    history = obj.get("history")
    if not isinstance(history, list) or not history:
        return None

    latest = latest_history_row(history)
    if not latest:
        return None

    norad = safe_int(
        first_non_empty(
            obj.get("norad_id"),
            obj.get("NORAD_CAT_ID"),
            source_path.stem,
        )
    )
    if norad is None:
        return None

    meta = dict(metadata or {})
    name = safe_text(
        first_non_empty(
            obj.get("name"),
            obj.get("object_name"),
            meta.get("object_name"),
            f"NORAD-{norad}",
        )
    )

    object_name = safe_text(first_non_empty(meta.get("object_name"), name))
    object_type = normalize_object_type(meta.get("object_type"), object_name)
    owner = normalize_country(
        first_non_empty(
            meta.get("owner"),
            meta.get("country_code"),
            meta.get("country"),
            obj.get("owner"),
            obj.get("country_code"),
        )
    )

    record = {
        "norad_id": norad,
        "name": name,
        "object_name": object_name,
        "object_id": meta.get("object_id"),
        "object_type": object_type,

        "owner": owner,
        "country_code": owner,
        "country": owner,
        "raw_country": meta.get("raw_country"),

        "launch_date": meta.get("launch_date"),
        "decay_date": meta.get("decay_date"),
        "site": meta.get("site"),
        "rcs": meta.get("rcs"),
        "comment": meta.get("comment"),

        # sat_historyの最新値を最優先
        "epoch": first_non_empty(latest.get("epoch"), meta.get("epoch")),
        "apogee_km": safe_float(
            first_non_empty(latest.get("apogee_km"), meta.get("apogee_km"))
        ),
        "perigee_km": safe_float(
            first_non_empty(latest.get("perigee_km"), meta.get("perigee_km"))
        ),
        "height_km": safe_float(
            first_non_empty(latest.get("height_km"), meta.get("height_km"))
        ),
        "inclination_deg": safe_float(
            first_non_empty(
                latest.get("inclination_deg"),
                meta.get("inclination_deg"),
            )
        ),
        "latitude_deg": safe_float(
            first_non_empty(
                latest.get("latitude_deg"),
                meta.get("latitude_deg"),
            )
        ),
        "longitude_deg": safe_float(
            first_non_empty(
                latest.get("longitude_deg"),
                meta.get("longitude_deg"),
            )
        ),
        "semi_major_axis_km": safe_float(
            first_non_empty(
                latest.get("semi_major_axis_km"),
                meta.get("semi_major_axis_km"),
            )
        ),
        "eccentricity": safe_float(
            first_non_empty(
                latest.get("eccentricity"),
                meta.get("eccentricity"),
            )
        ),
        "raan_deg": safe_float(
            first_non_empty(
                latest.get("raan_deg"),
                meta.get("raan_deg"),
            )
        ),
        "arg_perigee_deg": safe_float(
            first_non_empty(
                latest.get("arg_perigee_deg"),
                meta.get("arg_perigee_deg"),
            )
        ),
        "mean_anomaly_deg": safe_float(
            first_non_empty(
                latest.get("mean_anomaly_deg"),
                meta.get("mean_anomaly_deg"),
            )
        ),
        "mean_motion_rev_day": safe_float(
            first_non_empty(
                latest.get("mean_motion_rev_day"),
                meta.get("mean_motion_rev_day"),
            )
        ),
        "period_min": safe_float(
            first_non_empty(
                latest.get("period_min"),
                meta.get("period_min"),
            )
        ),
        "history_count": len(history),
        "has_history": True,
    }

    return norad, record


def load_history_records(
    metadata_map: dict[int, dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    if not SAT_HISTORY_DIR.exists():
        raise SystemExit("docs/data/sat_history does not exist")

    records: dict[int, dict[str, Any]] = {}

    paths = sorted(SAT_HISTORY_DIR.glob("*.json"))
    for index, path in enumerate(paths, start=1):
        obj = read_json(path, default=None)
        if not isinstance(obj, dict):
            continue

        norad_hint = safe_int(
            first_non_empty(
                obj.get("norad_id"),
                obj.get("NORAD_CAT_ID"),
                path.stem,
            )
        )
        metadata = metadata_map.get(norad_hint) if norad_hint is not None else None

        parsed = history_object_to_record(obj, path, metadata)
        if not parsed:
            continue

        norad, record = parsed
        records[norad] = record

        if index % 5000 == 0:
            print(f"sat_history processed: {index}/{len(paths)}")

    print(f"sat_history records: {len(records)}")
    return records


def metadata_to_minimal_record(
    norad: int,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    object_name = safe_text(
        first_non_empty(
            metadata.get("object_name"),
            f"NORAD-{norad}",
        )
    )
    object_type = normalize_object_type(
        metadata.get("object_type"),
        object_name,
    )
    owner = normalize_country(
        first_non_empty(
            metadata.get("owner"),
            metadata.get("country_code"),
            metadata.get("country"),
            metadata.get("raw_country"),
        )
    )

    return {
        "norad_id": norad,
        "name": object_name,
        "object_name": object_name,
        "object_id": metadata.get("object_id"),
        "object_type": object_type,

        "owner": owner,
        "country_code": owner,
        "country": owner,
        "raw_country": metadata.get("raw_country"),

        "launch_date": metadata.get("launch_date"),
        "decay_date": metadata.get("decay_date"),
        "site": metadata.get("site"),
        "rcs": metadata.get("rcs"),
        "comment": metadata.get("comment"),

        "epoch": metadata.get("epoch"),
        "apogee_km": metadata.get("apogee_km"),
        "perigee_km": metadata.get("perigee_km"),
        "height_km": metadata.get("height_km"),
        "inclination_deg": metadata.get("inclination_deg"),
        "latitude_deg": metadata.get("latitude_deg"),
        "longitude_deg": metadata.get("longitude_deg"),
        "semi_major_axis_km": metadata.get("semi_major_axis_km"),
        "eccentricity": metadata.get("eccentricity"),
        "raan_deg": metadata.get("raan_deg"),
        "arg_perigee_deg": metadata.get("arg_perigee_deg"),
        "mean_anomaly_deg": metadata.get("mean_anomaly_deg"),
        "mean_motion_rev_day": metadata.get("mean_motion_rev_day"),
        "period_min": metadata.get("period_min"),

        "history_count": 0,
        "has_history": False,
    }


# ============================================================
# Classification
# ============================================================

def mean_altitude_km(record: dict[str, Any]) -> float | None:
    height = safe_float(record.get("height_km"))
    if height is not None:
        return height

    apogee = safe_float(record.get("apogee_km"))
    perigee = safe_float(record.get("perigee_km"))
    if apogee is not None and perigee is not None:
        return (apogee + perigee) / 2.0

    sma = safe_float(record.get("semi_major_axis_km"))
    if sma is not None:
        # 平均地球半径を概算で差し引く。
        return sma - 6378.137

    return None


def is_geo_orbit(record: dict[str, Any]) -> bool:
    apogee = safe_float(record.get("apogee_km"))
    perigee = safe_float(record.get("perigee_km"))
    height = mean_altitude_km(record)
    mean_motion = safe_float(record.get("mean_motion_rev_day"))
    sma = safe_float(record.get("semi_major_axis_km"))

    if apogee is not None and perigee is not None:
        if (
            perigee >= GEO_PERIGEE_MIN_KM
            and apogee <= GEO_APOGEE_MAX_KM
        ):
            return True

    if height is not None:
        if GEO_MEAN_ALT_MIN_KM <= height <= GEO_MEAN_ALT_MAX_KM:
            return True

    if mean_motion is not None:
        if GEO_MEAN_MOTION_MIN <= mean_motion <= GEO_MEAN_MOTION_MAX:
            return True

    if sma is not None:
        if GEO_SMA_MIN_KM <= sma <= GEO_SMA_MAX_KM:
            return True

    return False


def infer_orbit_class(record: dict[str, Any]) -> str:
    if is_geo_orbit(record):
        return "GEO"

    height = mean_altitude_km(record)
    apogee = safe_float(record.get("apogee_km"))
    perigee = safe_float(record.get("perigee_km"))

    if height is None:
        return "UNKNOWN"

    if height < 2000:
        return "LEO"

    if height < 30000:
        return "MEO"

    # 遠地点だけがGEO帯で近地点が低い場合はGTO寄り。
    if (
        apogee is not None
        and perigee is not None
        and apogee >= 30000
        and perigee < 10000
    ):
        return "GTO"

    return "HEO"


def classify_dataset(record: dict[str, Any]) -> str:
    name = first_non_empty(
        record.get("object_name"),
        record.get("name"),
    )
    object_type = normalize_object_type(record.get("object_type"), name)

    # OBJECT A/B/C等は、Space-Track上でPAYLOAD扱いでも
    # ユーザー指定に合わせてOtherへ送る。
    if is_generic_object_name(name):
        return "OTHER"

    if object_type != "PAYLOAD":
        return "OTHER"

    if is_geo_orbit(record):
        return "GEO_PAYLOAD"

    return "NON_GEO_PAYLOAD"


def add_derived_fields(record: dict[str, Any]) -> dict[str, Any]:
    record = dict(record)

    record["object_type"] = normalize_object_type(
        record.get("object_type"),
        first_non_empty(record.get("object_name"), record.get("name")),
    )
    record["orbit_class"] = infer_orbit_class(record)
    record["dataset"] = classify_dataset(record)

    return record


# ============================================================
# Build / write
# ============================================================

def sort_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        records,
        key=lambda row: (
            str(
                first_non_empty(
                    row.get("name"),
                    row.get("object_name"),
                    "",
                )
            ).upper(),
            safe_int(row.get("norad_id")) or 0,
        ),
    )


def main() -> None:
    print("===== build satellite index v2 =====")

    metadata_map = load_catalog_metadata()
    record_map = load_history_records(metadata_map)

    # sat_historyに無い未減衰SATCAT物体も検索可能にする。
    metadata_only_added = 0
    decayed_skipped = 0

    for norad, metadata in metadata_map.items():
        if norad in record_map:
            continue

        decay_date = safe_date(metadata.get("decay_date"))
        if decay_date and not INCLUDE_DECAYED_OBJECTS:
            decayed_skipped += 1
            continue

        record_map[norad] = metadata_to_minimal_record(norad, metadata)
        metadata_only_added += 1

    all_records = sort_records(
        add_derived_fields(record)
        for record in record_map.values()
    )

    geo_payload = [
        row for row in all_records
        if row.get("dataset") == "GEO_PAYLOAD"
    ]
    non_geo_payload = [
        row for row in all_records
        if row.get("dataset") == "NON_GEO_PAYLOAD"
    ]
    other_objects = [
        row for row in all_records
        if row.get("dataset") == "OTHER"
    ]

    atomic_write_json(OUT_FULL, all_records, pretty=PRETTY_JSON)
    atomic_write_json(OUT_GEO_PAYLOAD, geo_payload, pretty=PRETTY_JSON)
    atomic_write_json(
        OUT_NON_GEO_PAYLOAD,
        non_geo_payload,
        pretty=PRETTY_JSON,
    )
    atomic_write_json(
        OUT_NON_GEO_PAYLOAD_ALIAS,
        non_geo_payload,
        pretty=PRETTY_JSON,
    )
    atomic_write_json(OUT_OTHER, other_objects, pretty=PRETTY_JSON)

    counts_by_type: dict[str, int] = {}
    counts_by_orbit: dict[str, int] = {}
    counts_by_country: dict[str, int] = {}

    for row in all_records:
        object_type = str(row.get("object_type") or "UNKNOWN")
        orbit_class = str(row.get("orbit_class") or "UNKNOWN")
        country = str(row.get("country_code") or "UNK")

        counts_by_type[object_type] = counts_by_type.get(object_type, 0) + 1
        counts_by_orbit[orbit_class] = counts_by_orbit.get(orbit_class, 0) + 1
        counts_by_country[country] = counts_by_country.get(country, 0) + 1

    manifest = {
        "updated_at": utc_now_iso(),
        "include_decayed_objects": INCLUDE_DECAYED_OBJECTS,
        "pretty_json": PRETTY_JSON,
        "datasets": {
            "geo_payload": {
                "label": "GEO Payload",
                "file": OUT_GEO_PAYLOAD.name,
                "count": len(geo_payload),
            },
            "payload": {
                "label": "Non-GEO Payload",
                "file": OUT_NON_GEO_PAYLOAD.name,
                "alias_file": OUT_NON_GEO_PAYLOAD_ALIAS.name,
                "count": len(non_geo_payload),
            },
            "other": {
                "label": "Other Objects",
                "file": OUT_OTHER.name,
                "count": len(other_objects),
            },
            "all": {
                "label": "ALL",
                "file": OUT_FULL.name,
                "count": len(all_records),
            },
        },
        "counts_by_object_type": dict(sorted(counts_by_type.items())),
        "counts_by_orbit_class": dict(sorted(counts_by_orbit.items())),
        "counts_by_country": dict(sorted(counts_by_country.items())),
        "source_counts": {
            "catalog_metadata": len(metadata_map),
            "metadata_only_added": metadata_only_added,
            "decayed_metadata_skipped": decayed_skipped,
            "with_history": sum(
                1 for row in all_records if row.get("has_history")
            ),
            "without_history": sum(
                1 for row in all_records if not row.get("has_history")
            ),
        },
    }

    atomic_write_json(OUT_MANIFEST, manifest, pretty=True)

    print("")
    print("===== generated files =====")
    print(f"{OUT_GEO_PAYLOAD}: {len(geo_payload)}")
    print(f"{OUT_NON_GEO_PAYLOAD}: {len(non_geo_payload)}")
    print(f"{OUT_NON_GEO_PAYLOAD_ALIAS}: {len(non_geo_payload)}")
    print(f"{OUT_OTHER}: {len(other_objects)}")
    print(f"{OUT_FULL}: {len(all_records)}")
    print(f"{OUT_MANIFEST}")

    print("")
    print("===== source summary =====")
    print(f"catalog metadata: {len(metadata_map)}")
    print(f"metadata-only active objects added: {metadata_only_added}")
    print(f"decayed metadata skipped: {decayed_skipped}")

    print("")
    print("===== sample GEO payload =====")
    for row in geo_payload[:5]:
        print(
            row.get("norad_id"),
            row.get("name"),
            row.get("object_type"),
            row.get("orbit_class"),
            row.get("owner"),
        )

    print("")
    print("===== sample non-GEO payload =====")
    for row in non_geo_payload[:5]:
        print(
            row.get("norad_id"),
            row.get("name"),
            row.get("object_type"),
            row.get("orbit_class"),
            row.get("owner"),
        )

    print("")
    print("===== sample other objects =====")
    for row in other_objects[:5]:
        print(
            row.get("norad_id"),
            row.get("name"),
            row.get("object_type"),
            row.get("orbit_class"),
            row.get("owner"),
        )


if __name__ == "__main__":
    main()
