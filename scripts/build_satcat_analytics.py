#!/usr/bin/env python3
"""
Hybrid Satellite Viewer 用 SATCAT Analytics 生成スクリプト。

入力:
    docs/data/catalog/satcat_full.json

出力:
    docs/data/catalog/satcat_analytics.json

HTML が期待する構造:
{
  "updated_at": "...",
  "satellites": [
    {
      "norad": 25544,
      "name": "ISS (ZARYA)",
      "launch_date": "1998-11-20",
      "decay_date": null,
      "country": "US",
      "orbit": "LEO",
      "orbit_current": "LEO",
      "series": "OTHER",
      "gp_epoch": null,
      "apogee_km": 420.0,
      "perigee_km": 415.0,
      "inclination_deg": 51.64
    }
  ]
}

重要:
・HTML は data.satellites を読み込むため、必ず satellites キーを出力する。
・JSONはコンパクト形式で保存し、GitHub Pagesでの読込み量を抑える。
・打上げ日が無い物体はグラフ化できないため、satellites から除外する。
・集計済みデータも残し、将来のHTML高速化に利用できるようにする。
"""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]

SATCAT_FILE = ROOT / "docs/data/catalog/satcat_full.json"
OUT_FILE = ROOT / "docs/data/catalog/satcat_analytics.json"

# True にすると読みやすいJSONになるが、ファイルサイズが大きくなる。
PRETTY_JSON = False


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


def safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


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
        number = float(str(value).strip())
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def safe_date(value: Any) -> str | None:
    text = safe_text(value)
    if not text:
        return None

    # Space-Trackの日時や日付を YYYY-MM-DD に統一。
    match = re.match(r"^(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return match.group(0)

    # 不正な値をグラフへ渡さない。
    return None


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


# ============================================================
# Input
# ============================================================

def load_satcat() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not SATCAT_FILE.exists():
        raise FileNotFoundError(f"SATCAT file not found: {SATCAT_FILE}")

    with SATCAT_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        metadata = {
            "updated_at": utc_now_iso(),
            "source": "SATCAT list",
        }
        objects = data

    elif isinstance(data, dict):
        metadata = data
        objects = data.get("objects")

        if not isinstance(objects, list):
            # 形式変更へのフォールバック
            for key in ("satellites", "rows", "data", "results"):
                candidate = data.get(key)
                if isinstance(candidate, list):
                    objects = candidate
                    break

    else:
        raise ValueError("satcat_full.json root must be an object or array")

    if not isinstance(objects, list):
        raise ValueError("satcat_full.json does not contain an objects array")

    valid_objects = [obj for obj in objects if isinstance(obj, dict)]
    return metadata, valid_objects


# ============================================================
# Series classification
# ============================================================

SERIES_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("STARLINK", ("STARLINK",)),
    ("ONEWEB", ("ONEWEB",)),
    ("GPS", ("GPS", "NAVSTAR")),
    ("GLONASS", ("GLONASS",)),
    ("BEIDOU", ("BEIDOU", "BD-")),
    ("GALILEO", ("GALILEO",)),
    ("QZSS", ("QZS", "QZSS", "MICHIBIKI")),
    ("IRNSS", ("IRNSS", "NAVIC")),
    ("YAOGAN", ("YAOGAN",)),
    ("GAOFEN", ("GAOFEN",)),
    ("TIANLIAN", ("TIANLIAN",)),
    ("TJS", ("TJS", "TONGXIN JISHU SHIYAN")),
    ("SHIJIAN", ("SHIJIAN", "SJ-")),
    ("CHINASAT", ("CHINASAT", "ZX-", "ZHONGXING")),
    ("WGS", ("WGS ", "WGS-")),
    ("AEHF", ("AEHF",)),
    ("SBIRS", ("SBIRS",)),
    ("INTELSAT", ("INTELSAT",)),
    ("EUTELSAT", ("EUTELSAT",)),
    ("INMARSAT", ("INMARSAT",)),
    ("SES", ("SES-", "SES ")),
    ("COSMOS", ("COSMOS", "KOSMOS")),
    ("NROL", ("NROL",)),
    ("USA", ("USA ", "USA-")),
    ("OBJECT", ("OBJECT ", "OBJECT-", "TBA ")),
    ("DEBRIS", (" DEB", "DEBRIS")),
    ("ROCKET BODY", (" R/B", "ROCKET BODY")),
]


def series_name(name: Any, object_type: Any = None) -> str:
    object_name = str(name or "").strip().upper()
    object_type_text = str(object_type or "").strip().upper()

    if object_type_text in {"ROCKET BODY", "R/B", "ROCKETBODY"}:
        return "ROCKET BODY"

    if object_type_text in {"DEBRIS", "DEB"}:
        return "DEBRIS"

    if not object_name:
        return "OTHER"

    for label, patterns in SERIES_RULES:
        if any(pattern in object_name for pattern in patterns):
            return label

    return "OTHER"


# ============================================================
# Orbit classification
# ============================================================

def orbit_type(
    apogee: Any,
    perigee: Any,
    inclination: Any = None,
    period_min: Any = None,
) -> str:
    ap = safe_float(apogee)
    pe = safe_float(perigee)
    inc = safe_float(inclination)
    period = safe_float(period_min)

    # 高度が欠落していても周期がほぼ恒星日ならGEOと判定できる。
    if ap is None or pe is None:
        if period is not None and 1300 <= period <= 1500:
            return "GEO"
        return "UNKNOWN"

    mean_alt = (ap + pe) / 2.0

    # GEO遷移軌道。平均高度だけでMEO扱いしないため先に判定。
    if ap >= 30000 and pe < 10000:
        return "GTO"

    # ほぼ円軌道の静止・準静止軌道。
    if pe >= 30000 and ap <= 45000:
        return "GEO"

    # 高遠地点を持つ楕円軌道。
    if ap >= 30000:
        return "HEO/GEO"

    # LEOは平均高度で判定。
    if mean_alt < 2000:
        return "LEO"

    # 高傾斜の楕円軌道をHEO/SSO系へまとめる。
    if (
        inc is not None
        and 80 <= inc <= 110
        and ap >= 2000
        and (ap - pe) >= 1000
    ):
        return "HEO/SSO"

    if mean_alt < 30000:
        return "MEO"

    # HTMLの選択肢に一般的なHEOが無いため、
    # GEO方向の高軌道としてまとめる。
    return "HEO/GEO"


# ============================================================
# Analytics records
# ============================================================

def make_satellite_record(obj: dict[str, Any]) -> dict[str, Any] | None:
    norad = safe_int(
        first_non_empty(
            obj.get("norad"),
            obj.get("norad_id"),
            obj.get("NORAD_CAT_ID"),
            obj.get("NORAD"),
            obj.get("CATNR"),
        )
    )
    if norad is None:
        return None

    name = safe_text(
        first_non_empty(
            obj.get("name"),
            obj.get("object_name"),
            obj.get("OBJECT_NAME"),
            f"NORAD-{norad}",
        )
    )

    launch_date = safe_date(
        first_non_empty(
            obj.get("launch_date"),
            obj.get("LAUNCH_DATE"),
            obj.get("LAUNCH"),
        )
    )

    # 打上げグラフには打上げ日が必須。
    if launch_date is None:
        return None

    decay_date = safe_date(
        first_non_empty(
            obj.get("decay_date"),
            obj.get("DECAY_DATE"),
            obj.get("DECAY"),
        )
    )

    apogee = safe_float(
        first_non_empty(
            obj.get("apogee_km"),
            obj.get("APOGEE"),
        )
    )
    perigee = safe_float(
        first_non_empty(
            obj.get("perigee_km"),
            obj.get("PERIGEE"),
        )
    )
    inclination = safe_float(
        first_non_empty(
            obj.get("inclination_deg"),
            obj.get("INCLINATION"),
        )
    )
    period_min = safe_float(
        first_non_empty(
            obj.get("period_min"),
            obj.get("PERIOD"),
        )
    )

    orbit = orbit_type(
        apogee,
        perigee,
        inclination,
        period_min,
    )

    object_type = safe_text(
        first_non_empty(
            obj.get("object_type"),
            obj.get("OBJECT_TYPE"),
        )
    )

    country = normalize_country(
        first_non_empty(
            obj.get("country"),
            obj.get("COUNTRY"),
            obj.get("owner"),
            obj.get("OWNER"),
        )
    )

    gp_epoch = safe_text(
        first_non_empty(
            obj.get("gp_epoch"),
            obj.get("epoch"),
            obj.get("EPOCH"),
        )
    )

    series = series_name(name, object_type)

    # HTMLとCSV出力で使うフィールド名に合わせる。
    return {
        "norad": norad,
        "norad_id": norad,

        "name": name,
        "object_name": name,
        "object_id": safe_text(
            first_non_empty(
                obj.get("object_id"),
                obj.get("OBJECT_ID"),
            )
        ),
        "object_type": object_type,

        "country": country,
        "raw_country": safe_text(
            first_non_empty(
                obj.get("raw_country"),
                obj.get("COUNTRY"),
            )
        ),

        "launch_date": launch_date,
        "decay_date": decay_date,

        "series": series,

        # 現在のSATCAT軌道値から分類するため両方同じ。
        "orbit": orbit,
        "orbit_current": orbit,

        "gp_epoch": gp_epoch,
        "apogee_km": apogee,
        "perigee_km": perigee,
        "inclination_deg": inclination,
        "period_min": period_min,
    }


def month_key(date: Any) -> str | None:
    value = safe_date(date)
    return value[:7] if value else None


def aggregate_monthly(
    satellites: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    monthly: dict[str, int] = defaultdict(int)
    country_monthly: dict[tuple[str, str], int] = defaultdict(int)
    series_monthly: dict[tuple[str, str], int] = defaultdict(int)
    orbit_monthly: dict[tuple[str, str], int] = defaultdict(int)

    for sat in satellites:
        month = month_key(sat.get("launch_date"))
        if month is None:
            continue

        country = str(sat.get("country") or "UNK")
        series = str(sat.get("series") or "OTHER")
        orbit = str(sat.get("orbit") or "UNKNOWN")

        monthly[month] += 1
        country_monthly[(country, month)] += 1
        series_monthly[(series, month)] += 1
        orbit_monthly[(orbit, month)] += 1

    def grouped_rows(
        source: dict[tuple[str, str], int],
    ) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for (group, month), count in sorted(source.items()):
            grouped[group].append({
                "month": month,
                "count": count,
            })

        return dict(grouped)

    return {
        "monthly_total": [
            {
                "month": month,
                "count": count,
            }
            for month, count in sorted(monthly.items())
        ],
        "country": grouped_rows(country_monthly),
        "series": grouped_rows(series_monthly),
        "orbit": grouped_rows(orbit_monthly),
    }


# ============================================================
# Main
# ============================================================

def main() -> None:
    print("===== build SATCAT analytics v2 =====")
    print(f"Input: {SATCAT_FILE}")

    metadata, objects = load_satcat()

    satellites: list[dict[str, Any]] = []

    skipped_no_norad = 0
    skipped_no_launch = 0

    for obj in objects:
        record = make_satellite_record(obj)

        if record is None:
            norad = safe_int(
                first_non_empty(
                    obj.get("norad"),
                    obj.get("norad_id"),
                    obj.get("NORAD_CAT_ID"),
                )
            )

            if norad is None:
                skipped_no_norad += 1
            else:
                skipped_no_launch += 1
            continue

        satellites.append(record)

    satellites.sort(
        key=lambda sat: (
            str(sat.get("launch_date") or ""),
            str(sat.get("name") or "").upper(),
            int(sat.get("norad") or 0),
        )
    )

    aggregates = aggregate_monthly(satellites)

    counts_by_series: dict[str, int] = defaultdict(int)
    counts_by_orbit: dict[str, int] = defaultdict(int)
    counts_by_country: dict[str, int] = defaultdict(int)
    counts_by_object_type: dict[str, int] = defaultdict(int)

    for sat in satellites:
        counts_by_series[str(sat.get("series") or "OTHER")] += 1
        counts_by_orbit[str(sat.get("orbit") or "UNKNOWN")] += 1
        counts_by_country[str(sat.get("country") or "UNK")] += 1
        counts_by_object_type[str(sat.get("object_type") or "UNKNOWN")] += 1

    output = {
        "schema_version": 2,
        "updated_at": metadata.get("updated_at") or utc_now_iso(),
        "generated_at": utc_now_iso(),
        "source": metadata.get("source") or "Space-Track SATCAT full",

        "source_object_count": len(objects),
        "count": len(satellites),
        "skipped": {
            "missing_norad": skipped_no_norad,
            "missing_launch_date": skipped_no_launch,
        },

        # HTMLが直接読み込む本体。
        "satellites": satellites,

        # 将来、HTML側の集計を軽量化するための集計済みデータ。
        **aggregates,

        "counts": {
            "series": dict(sorted(counts_by_series.items())),
            "orbit": dict(sorted(counts_by_orbit.items())),
            "country": dict(sorted(counts_by_country.items())),
            "object_type": dict(sorted(counts_by_object_type.items())),
        },
    }

    atomic_write_json(OUT_FILE, output, pretty=PRETTY_JSON)

    size_mb = OUT_FILE.stat().st_size / (1024 * 1024)

    print(f"Output: {OUT_FILE}")
    print(f"Source objects: {len(objects)}")
    print(f"Analytics satellites: {len(satellites)}")
    print(f"Skipped missing NORAD: {skipped_no_norad}")
    print(f"Skipped missing launch date: {skipped_no_launch}")
    print(f"Output size: {size_mb:.2f} MB")
    print(f"Orbit counts: {dict(sorted(counts_by_orbit.items()))}")
    print(f"Country counts: {dict(sorted(counts_by_country.items()))}")

    # 最低限の出力検証
    with OUT_FILE.open("r", encoding="utf-8") as f:
        check = json.load(f)

    if not isinstance(check.get("satellites"), list):
        raise RuntimeError("Generated JSON does not contain satellites array")

    required_fields = {
        "norad",
        "name",
        "launch_date",
        "decay_date",
        "country",
        "orbit",
        "orbit_current",
        "series",
        "gp_epoch",
        "apogee_km",
        "perigee_km",
        "inclination_deg",
    }

    if check["satellites"]:
        missing = required_fields - set(check["satellites"][0].keys())
        if missing:
            raise RuntimeError(
                f"Generated satellite record is missing fields: {sorted(missing)}"
            )

    print("Validation: OK")


if __name__ == "__main__":
    main()
