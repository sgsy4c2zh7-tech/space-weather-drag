import json
import os
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen

CATALOG_DIR = "docs/data/catalog"
LATEST_INDEX = "docs/data/satellites.json"

CELESTRAK_ACTIVE_JSON = "https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=json"

os.makedirs(CATALOG_DIR, exist_ok=True)


def fetch_json(url: str):
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=180) as resp:
        return json.loads(resp.read().decode("utf-8"))


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


def build_satellite_index(rows):
    out = []
    for r in rows:
        norad = r.get("NORAD_CAT_ID")
        name = r.get("OBJECT_NAME")
        if norad is None:
            continue
        out.append({
            "norad_id": int(norad),
            "name": name or f"NORAD-{norad}"
        })
    out.sort(key=lambda x: x["norad_id"])
    return out


def main():
    rows = fetch_json(CELESTRAK_ACTIVE_JSON)
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%dT%H%MZ")
    snap_path = os.path.join(CATALOG_DIR, f"active_{stamp}.json")

    with open(snap_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    sat_index = build_satellite_index(rows)
    with open(LATEST_INDEX, "w", encoding="utf-8") as f:
        json.dump(sat_index, f, ensure_ascii=False, indent=2)

    cleanup_old_files(CATALOG_DIR, days=30)

    print(f"Snapshot rows: {len(rows)}")
    print(f"Wrote snapshot: {snap_path}")
    print(f"Wrote satellite index: {LATEST_INDEX}")


if __name__ == "__main__":
    main()
