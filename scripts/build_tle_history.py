import json
import time
import requests
from pathlib import Path
from datetime import datetime, timezone

CATALOG_DIR = Path("docs/data/catalog")
OUT_DIR = Path("docs/data/tle_history")

RETENTION_DAYS = 370
CELESTRAK_SLEEP_SEC = 0.25
REQUEST_TIMEOUT = 20

OUT_DIR.mkdir(parents=True, exist_ok=True)


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_tle_epoch(line1: str):
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
        and line1.strip().startswith("1 ")
        and line2.strip().startswith("2 ")
    )


def parse_celestrak_tle_text(text: str):
    lines = [x.strip() for x in text.splitlines() if x.strip()]

    if len(lines) < 2:
        return None

    name = None
    tle1 = None
    tle2 = None

    for i in range(len(lines)):
        if lines[i].startswith("1 ") and i + 1 < len(lines) and lines[i + 1].startswith("2 "):
            tle1 = lines[i]
            tle2 = lines[i + 1]
            if i - 1 >= 0 and not lines[i - 1].startswith(("1 ", "2 ")):
                name = lines[i - 1]
            break

    if is_valid_tle(tle1, tle2):
        return {
            "name": name,
            "tle1": tle1,
            "tle2": tle2,
        }

    return None


def fetch_celestrak_by_catnr(norad_id: int):
    url = f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad_id}&FORMAT=TLE"

    try:
        res = requests.get(url, timeout=REQUEST_TIMEOUT)
        if res.status_code != 200:
            return None

        text = res.text.strip()

        if not text or "No GP data found" in text or "Invalid query" in text:
            return None

        return parse_celestrak_tle_text(text)

    except Exception:
        return None


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


def load_existing_history(norad: int, name: str):
    out_path = OUT_DIR / f"{norad}.json"

    if out_path.exists():
        try:
            obj = load_json(out_path)
        except Exception:
            obj = {}
    else:
        obj = {}

    obj.setdefault("norad_id", norad)
    obj.setdefault("name", name or str(norad))
    obj.setdefault("tle_history", [])

    obj["norad_id"] = norad
    obj["name"] = name or obj.get("name") or str(norad)

    return obj


def append_tle_entry(obj, tle1, tle2, fetched_at, source):
    tle1 = tle1.strip()
    tle2 = tle2.strip()

    tle_epoch = parse_tle_epoch(tle1)

    new_entry = {
        "fetched_at": fetched_at,
        "tle_epoch": tle_epoch,
        "source": source,
        "tle1": tle1,
        "tle2": tle2,
    }

    hist = obj.get("tle_history", [])

    duplicate = any(
        h.get("tle1") == new_entry["tle1"] and h.get("tle2") == new_entry["tle2"]
        for h in hist
    )

    if not duplicate:
        hist.append(new_entry)

    hist = prune_old_entries(hist)
    hist.sort(key=lambda x: x.get("tle_epoch") or x.get("fetched_at") or "")

    obj["tle_history"] = hist
    obj["latest_tle_epoch"] = tle_epoch
    obj["updated_at"] = fetched_at

    return not duplicate


def main():
    fetched_at = now_iso()
    catalog_file = find_latest_catalog_file()
    data = load_json(catalog_file)
    items = normalize_catalog_items(data)

    updated = 0
    skipped = 0
    from_catalog = 0
    from_celestrak = 0
    celestrak_attempts = 0

    for item in items:
        norad = get_field(item, ["norad_id", "NORAD_CAT_ID", "norad", "id", "OBJECT_ID"])
        name = get_field(item, ["name", "OBJECT_NAME", "satellite_name", "object_name", "SATNAME"])
        tle1 = get_field(item, ["tle1", "TLE_LINE1", "line1", "TLE1"])
        tle2 = get_field(item, ["tle2", "TLE_LINE2", "line2", "TLE2"])

        try:
            norad = int(norad)
        except Exception:
            skipped += 1
            continue

        source = "catalog"

        if not is_valid_tle(tle1, tle2):
            celestrak_attempts += 1
            time.sleep(CELESTRAK_SLEEP_SEC)

            ct = fetch_celestrak_by_catnr(norad)
            if not ct:
                skipped += 1
                continue

            tle1 = ct["tle1"]
            tle2 = ct["tle2"]

            if ct.get("name"):
                name = name or ct["name"]

            source = "celestrak_gp"
            from_celestrak += 1
        else:
            from_catalog += 1

        obj = load_existing_history(norad, name)

        changed = append_tle_entry(
            obj=obj,
            tle1=tle1,
            tle2=tle2,
            fetched_at=fetched_at,
            source=source,
        )

        save_json(OUT_DIR / f"{norad}.json", obj)

        if changed:
            updated += 1

    print(f"catalog_file={catalog_file}")
    print(f"items={len(items)}")
    print(f"updated={updated}")
    print(f"from_catalog={from_catalog}")
    print(f"celestrak_attempts={celestrak_attempts}")
    print(f"from_celestrak={from_celestrak}")
    print(f"skipped={skipped}")
    print(f"out_dir={OUT_DIR}")


if __name__ == "__main__":
    main()