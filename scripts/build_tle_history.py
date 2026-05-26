import json
from pathlib import Path
from datetime import datetime, timezone

CATALOG_DIR = Path("docs/data/catalog")
OUT_DIR = Path("docs/data/tle_history")
RETENTION_DAYS = 370

OUT_DIR.mkdir(parents=True, exist_ok=True)


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_tle_epoch(line1: str) -> str | None:
    try:
        yy = int(line1[18:20])
        doy = float(line1[20:32])
        year = 2000 + yy if yy < 57 else 1900 + yy

        jan1 = datetime(year, 1, 1, tzinfo=timezone.utc)
        dt = jan1.timestamp() + (doy - 1) * 86400
        return datetime.fromtimestamp(dt, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def normalize_catalog_items(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ["data", "satellites", "items", "catalog"]:
            if isinstance(data.get(key), list):
                return data[key]
    return []


def find_latest_catalog_file():
    candidates = list(CATALOG_DIR.glob("*.json"))
    if not candidates:
        raise FileNotFoundError("No catalog JSON found in docs/data/catalog")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def get_field(item, names):
    for name in names:
        if name in item and item[name] not in [None, ""]:
            return item[name]
    return None


def is_valid_tle(line1, line2):
    return (
        isinstance(line1, str)
        and isinstance(line2, str)
        and line1.startswith("1 ")
        and line2.startswith("2 ")
    )


def prune_old_entries(entries):
    now = datetime.now(timezone.utc)
    kept = []

    for e in entries:
        t = e.get("fetched_at") or e.get("tle_epoch")
        if not t:
            kept.append(e)
            continue

        try:
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
            age_days = (now - dt).days
            if age_days <= RETENTION_DAYS:
                kept.append(e)
        except Exception:
            kept.append(e)

    return kept


def main():
    fetched_at = now_iso()
    catalog_file = find_latest_catalog_file()
    data = load_json(catalog_file)
    items = normalize_catalog_items(data)

    updated = 0
    skipped = 0

    for item in items:
        norad = get_field(item, ["norad_id", "NORAD_CAT_ID", "norad", "id"])
        name = get_field(item, ["name", "OBJECT_NAME", "satellite_name", "object_name"])
        tle1 = get_field(item, ["tle1", "TLE_LINE1", "line1"])
        tle2 = get_field(item, ["tle2", "TLE_LINE2", "line2"])

        if norad is None or not is_valid_tle(tle1, tle2):
            skipped += 1
            continue

        try:
            norad = int(norad)
        except Exception:
            skipped += 1
            continue

        out_path = OUT_DIR / f"{norad}.json"

        if out_path.exists():
            obj = load_json(out_path)
        else:
            obj = {
                "norad_id": norad,
                "name": name or str(norad),
                "tle_history": []
            }

        obj["norad_id"] = norad
        obj["name"] = name or obj.get("name") or str(norad)

        tle_epoch = parse_tle_epoch(tle1)

        new_entry = {
            "fetched_at": fetched_at,
            "tle_epoch": tle_epoch,
            "tle1": tle1.strip(),
            "tle2": tle2.strip()
        }

        hist = obj.get("tle_history", [])

        duplicate = any(
            h.get("tle1") == new_entry["tle1"] and h.get("tle2") == new_entry["tle2"]
            for h in hist
        )

        if not duplicate:
            hist.append(new_entry)
            updated += 1

        hist = prune_old_entries(hist)
        hist.sort(key=lambda x: x.get("tle_epoch") or x.get("fetched_at") or "")

        obj["tle_history"] = hist
        obj["updated_at"] = fetched_at

        save_json(out_path, obj)

    print(f"catalog_file={catalog_file}")
    print(f"updated={updated}")
    print(f"skipped={skipped}")
    print(f"tle_history_dir={OUT_DIR}")


if __name__ == "__main__":
    main()