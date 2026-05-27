import csv
import json
import os
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen

CATALOG_DIR = "docs/data/catalog"
LATEST_INDEX = "docs/data/satellites.json"

CELESTRAK_ACTIVE_JSON = "https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=json"

SATCAT_CSV_URLS = [
    "https://celestrak.org/pub/satcat.csv",
    "https://www.celestrak.org/pub/satcat.csv",
]

os.makedirs(CATALOG_DIR, exist_ok=True)


def fetch_text(url: str):
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=180) as resp:
        return resp.read().decode("utf-8", errors="replace")


def fetch_json(url: str):
    return json.loads(fetch_text(url))


def fetch_satcat_csv():
    last_error = None
    for url in SATCAT_CSV_URLS:
        try:
            text = fetch_text(url)
            rows = list(csv.DictReader(text.splitlines()))
            if rows:
                print(f"Fetched SATCAT CSV: {url}")
                return rows
        except Exception as e:
            last_error = e
            print(f"SATCAT CSV failed: {url} / {e}")

    print(f"warning: SATCAT CSV unavailable. last_error={last_error}")
    return []


def cleanup_old_files(folder: str, days: int = 30):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    for name in os.listdir(folder):
        if not name.endswith(".json"):
            continue
        path = os.path.join(folder, name)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)
            if mtime < cutoff:
                os.remove(path)
                print(f"Deleted old snapshot: {path}")
        except Exception as e:
            print(f"Skip cleanup for {path}: {e}")


def safe_int(v):
    try:
        return int(str(v).strip())
    except Exception:
        return None


def pick_first(row, keys, default=None):
    for k in keys:
        if k in row and row[k] not in [None, ""]:
            return row[k]
    return default


def build_satcat_map(satcat_rows):
    out = {}

    for r in satcat_rows:
        norad = safe_int(pick_first(r, ["NORAD_CAT_ID", "NORAD", "CATNR", "OBJECT_NUMBER"]))
        if norad is None:
            continue

        country = pick_first(
            r,
            ["COUNTRY", "OWNER", "COUNTRY_CODE", "LAUNCHING_STATE"],
            "UNK"
        )

        out[norad] = {
            "country_code": str(country).strip().upper() if country else "UNK",
            "object_type": pick_first(r, ["OBJECT_TYPE", "TYPE"]),
            "launch_site": pick_first(r, ["LAUNCH_SITE", "SITE"]),
            "launch_date": pick_first(r, ["LAUNCH_DATE"]),
            "decay_date": pick_first(r, ["DECAY_DATE"]),
        }

    return out


def build_satellite_index(gp_rows, satcat_map):
    out = []

    for r in gp_rows:
        norad = safe_int(r.get("NORAD_CAT_ID"))
        name = r.get("OBJECT_NAME")

        if norad is None:
            continue

        satcat = satcat_map.get(norad, {})

        out.append({
            "norad_id": norad,
            "name": name or f"NORAD-{norad}",
            "country_code": satcat.get("country_code", "UNK"),
            "object_type": satcat.get("object_type"),
            "launch_site": satcat.get("launch_site"),
            "launch_date": satcat.get("launch_date"),
            "decay_date": satcat.get("decay_date"),
        })

    out.sort(key=lambda x: x["norad_id"])
    return out


def main():
    gp_rows = fetch_json(CELESTRAK_ACTIVE_JSON)
    if not isinstance(gp_rows, list) or len(gp_rows) == 0:
        raise SystemExit("CelesTrak GP catalog fetch returned no rows.")

    satcat_rows = fetch_satcat_csv()
    satcat_map = build_satcat_map(satcat_rows)

    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%dT%H%MZ")

    gp_snap_path = os.path.join(CATALOG_DIR, f"active_gp_{stamp}.json")
    satcat_snap_path = os.path.join(CATALOG_DIR, f"satcat_{stamp}.json")

    with open(gp_snap_path, "w", encoding="utf-8") as f:
        json.dump(gp_rows, f, ensure_ascii=False, indent=2)

    with open(satcat_snap_path, "w", encoding="utf-8") as f:
        json.dump(satcat_rows, f, ensure_ascii=False, indent=2)

    sat_index = build_satellite_index(gp_rows, satcat_map)

    with open(LATEST_INDEX, "w", encoding="utf-8") as f:
        json.dump(sat_index, f, ensure_ascii=False, indent=2)

    cleanup_old_files(CATALOG_DIR, days=30)

    with_country = sum(1 for x in sat_index if x.get("country_code") != "UNK")

    print(f"GP rows: {len(gp_rows)}")
    print(f"SATCAT rows: {len(satcat_rows)}")
    print(f"Satellite index rows: {len(sat_index)}")
    print(f"With country_code: {with_country}")
    print(f"Without country_code: {len(sat_index) - with_country}")
    print(f"Wrote GP snapshot: {gp_snap_path}")
    print(f"Wrote SATCAT snapshot: {satcat_snap_path}")
    print(f"Wrote satellite index: {LATEST_INDEX}")


if __name__ == "__main__":
    main()
