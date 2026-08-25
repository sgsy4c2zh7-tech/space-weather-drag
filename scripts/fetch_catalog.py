import csv
import json
import os
import re
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen

CATALOG_DIR = "docs/data/catalog"
LATEST_INDEX = "docs/data/satellites.json"

CELESTRAK_ACTIVE_JSON = (
    "https://celestrak.org/NORAD/elements/"
    "gp.php?GROUP=active&FORMAT=json"
)

SATCAT_CSV_URLS = [
    "https://celestrak.org/pub/satcat.csv",
    "https://www.celestrak.org/pub/satcat.csv",
]

KEEP_DAYS = 30

ACTIVE_GP_PATTERN = re.compile(
    r"^active_gp_(\d{4}-\d{2}-\d{2}T\d{4}Z)\.json$"
)

LEGACY_SATCAT_PATTERN = re.compile(
    r"^satcat_(\d{4}-\d{2}-\d{2}T\d{4}Z)\.json$"
)

os.makedirs(CATALOG_DIR, exist_ok=True)


def fetch_text(url: str):
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
        },
    )

    with urlopen(req, timeout=180) as resp:
        return resp.read().decode(
            "utf-8",
            errors="replace",
        )


def fetch_json(url: str):
    return json.loads(fetch_text(url))


def fetch_satcat_csv():
    last_error = None

    for url in SATCAT_CSV_URLS:
        try:
            text = fetch_text(url)

            rows = list(
                csv.DictReader(
                    text.splitlines()
                )
            )

            if rows:
                print(
                    f"Fetched SATCAT CSV: {url}"
                )

                return rows

        except Exception as e:
            last_error = e

            print(
                f"SATCAT CSV failed: "
                f"{url} / {e}"
            )

    print(
        "warning: SATCAT CSV unavailable. "
        f"last_error={last_error}"
    )

    return []


def parse_snapshot_datetime(
    filename: str,
    pattern: re.Pattern,
):
    match = pattern.match(filename)

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


def cleanup_old_active_gp_files(
    folder: str,
    days: int = KEEP_DAYS,
):
    """
    active_gp_YYYY-MM-DDTHHMMZ.json の
    ファイル名の日付を使って削除する。

    GitHub Actions checkout後のmtimeには依存しない。
    """

    cutoff = (
        datetime.now(timezone.utc)
        - timedelta(days=days)
    )

    removed = 0

    for name in os.listdir(folder):
        snapshot_time = parse_snapshot_datetime(
            name,
            ACTIVE_GP_PATTERN,
        )

        if snapshot_time is None:
            continue

        if snapshot_time >= cutoff:
            continue

        path = os.path.join(
            folder,
            name,
        )

        try:
            os.remove(path)

            removed += 1

            print(
                f"Deleted old GP snapshot: "
                f"{path}"
            )

        except Exception as e:
            print(
                f"Failed to delete "
                f"{path}: {e}"
            )

    return removed


def cleanup_legacy_satcat_snapshots(
    folder: str,
):
    """
    旧形式:
      satcat_YYYY-MM-DDTHHMMZ.json

    はすべて削除する。

    今後はsatcat_latest.jsonだけ保存する。
    """

    removed = 0

    for name in os.listdir(folder):
        if not LEGACY_SATCAT_PATTERN.match(name):
            continue

        path = os.path.join(
            folder,
            name,
        )

        try:
            os.remove(path)

            removed += 1

            print(
                "Deleted obsolete SATCAT "
                f"snapshot: {path}"
            )

        except Exception as e:
            print(
                f"Failed to delete "
                f"{path}: {e}"
            )

    return removed


def safe_int(v):
    try:
        return int(str(v).strip())

    except Exception:
        return None


def pick_first(
    row,
    keys,
    default=None,
):
    for k in keys:
        if (
            k in row
            and row[k] not in [None, ""]
        ):
            return row[k]

    return default


def build_satcat_map(
    satcat_rows,
):
    out = {}

    for r in satcat_rows:
        norad = safe_int(
            pick_first(
                r,
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
            r,
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
                str(country).strip().upper()
                if country
                else "UNK"
            ),
            "object_type": pick_first(
                r,
                [
                    "OBJECT_TYPE",
                    "TYPE",
                ],
            ),
            "launch_site": pick_first(
                r,
                [
                    "LAUNCH_SITE",
                    "SITE",
                ],
            ),
            "launch_date": pick_first(
                r,
                ["LAUNCH_DATE"],
            ),
            "decay_date": pick_first(
                r,
                ["DECAY_DATE"],
            ),
        }

    return out


def build_satellite_index(
    gp_rows,
    satcat_map,
):
    out = []

    for r in gp_rows:
        norad = safe_int(
            r.get("NORAD_CAT_ID")
        )

        name = r.get(
            "OBJECT_NAME"
        )

        if norad is None:
            continue

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
        key=lambda x: x["norad_id"]
    )

    return out


def main():

    # -------------------------
    # GP
    # -------------------------

    gp_rows = fetch_json(
        CELESTRAK_ACTIVE_JSON
    )

    if (
        not isinstance(gp_rows, list)
        or len(gp_rows) == 0
    ):
        raise SystemExit(
            "CelesTrak GP catalog "
            "fetch returned no rows."
        )

    # -------------------------
    # SATCAT
    # -------------------------

    satcat_rows = fetch_satcat_csv()

    satcat_map = build_satcat_map(
        satcat_rows
    )

    # -------------------------
    # Timestamp
    # -------------------------

    now = datetime.now(
        timezone.utc
    )

    stamp = now.strftime(
        "%Y-%m-%dT%H%MZ"
    )

    # -------------------------
    # Save GP history snapshot
    # -------------------------

    gp_snap_path = os.path.join(
        CATALOG_DIR,
        f"active_gp_{stamp}.json",
    )

    with open(
        gp_snap_path,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            gp_rows,
            f,
            ensure_ascii=False,
            indent=2,
        )

    # -------------------------
    # SATCAT latest only
    # -------------------------

    satcat_latest_path = os.path.join(
        CATALOG_DIR,
        "satcat_latest.json",
    )

    with open(
        satcat_latest_path,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            satcat_rows,
            f,
            ensure_ascii=False,
            indent=2,
        )

    # -------------------------
    # Satellite index
    # -------------------------

    sat_index = build_satellite_index(
        gp_rows,
        satcat_map,
    )

    with open(
        LATEST_INDEX,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            sat_index,
            f,
            ensure_ascii=False,
            indent=2,
        )

    # -------------------------
    # Cleanup
    # -------------------------

    removed_gp = (
        cleanup_old_active_gp_files(
            CATALOG_DIR,
            days=KEEP_DAYS,
        )
    )

    removed_satcat = (
        cleanup_legacy_satcat_snapshots(
            CATALOG_DIR
        )
    )

    # -------------------------
    # Stats
    # -------------------------

    with_country = sum(
        1
        for x in sat_index
        if x.get("country_code")
        != "UNK"
    )

    active_gp_count = sum(
        1
        for name in os.listdir(
            CATALOG_DIR
        )
        if ACTIVE_GP_PATTERN.match(
            name
        )
    )

    print("")
    print(
        "=============================="
    )

    print(
        "CATALOG UPDATE COMPLETE"
    )

    print(
        "=============================="
    )

    print(
        f"GP rows: "
        f"{len(gp_rows)}"
    )

    print(
        f"SATCAT rows: "
        f"{len(satcat_rows)}"
    )

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

    print(
        f"GP snapshots retained: "
        f"{active_gp_count}"
    )

    print(
        f"GP retention: "
        f"{KEEP_DAYS} days"
    )

    print(
        f"Old GP snapshots removed: "
        f"{removed_gp}"
    )

    print(
        "SATCAT storage: "
        "latest only"
    )

    print(
        f"Legacy SATCAT snapshots "
        f"removed: {removed_satcat}"
    )

    print(
        f"Wrote GP snapshot: "
        f"{gp_snap_path}"
    )

    print(
        f"Wrote SATCAT latest: "
        f"{satcat_latest_path}"
    )

    print(
        f"Wrote satellite index: "
        f"{LATEST_INDEX}"
    )


if __name__ == "__main__":
    main()
