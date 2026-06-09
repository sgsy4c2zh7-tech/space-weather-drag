import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]

CATALOG_PATH = ROOT / "docs" / "data" / "satellite_index_full.json"
LATEST_DIR = ROOT / "docs" / "data" / "tle"
HISTORY_DIR = ROOT / "docs" / "data" / "tle_history"

KEEP_DAYS = 30
REQUEST_SLEEP_SEC = 0.35
MAX_SATS = 1200

LATEST_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_tle_epoch(line1):
    try:
        yy = int(line1[18:20])
        day = float(line1[20:32])
        year = 2000 + yy if yy < 57 else 1900 + yy
        jan1 = datetime(year, 1, 1, tzinfo=timezone.utc)
        epoch = jan1 + timedelta(days=day - 1)
        return epoch.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def valid_tle(tle1, tle2):
    return isinstance(tle1, str) and isinstance(tle2, str) and tle1.startswith("1 ") and tle2.startswith("2 ")


def parse_tle_text(text):
    lines = [x.strip() for x in text.splitlines() if x.strip()]

    if not lines:
        return None

    if lines[0].startswith("<") or "No GP data found" in text or "Invalid query" in text:
        return None

    for i in range(len(lines) - 1):
        if lines[i].startswith("1 ") and lines[i + 1].startswith("2 "):
            tle1 = lines[i]
            tle2 = lines[i + 1]
            name = None

            if i > 0 and not lines[i - 1].startswith("1 ") and not lines[i - 1].startswith("2 "):
                name = lines[i - 1]

            if not valid_tle(tle1, tle2):
                return None

            return {
                "name": name,
                "tle1": tle1,
                "tle2": tle2,
                "tle_epoch": parse_tle_epoch(tle1),
            }

    return None


def fetch_tle(norad_id):
    formats = ["TLE", "3LE", "2LE"]

    for fmt in formats:
        url = f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad_id}&FORMAT={fmt}"

        try:
            r = requests.get(url, timeout=20)
            if r.status_code != 200:
                continue

            parsed = parse_tle_text(r.text)
            if parsed:
                return parsed

        except Exception as e:
            print(f"[WARN] fetch failed NORAD={norad_id} FORMAT={fmt}: {e}")

    return None


def load_json(path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def save_json(path, obj):
    path.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def load_catalog_targets():
    catalog = load_json(CATALOG_PATH, [])

    targets = []
    seen = set()

    for sat in catalog:
        norad = sat.get("norad_id") or sat.get("NORAD_CAT_ID") or sat.get("catalog_number")
        if norad is None:
            continue

        try:
            norad = int(norad)
        except Exception:
            continue

        if norad in seen:
            continue

        seen.add(norad)

        targets.append({
            "norad_id": norad,
            "name": sat.get("name") or sat.get("OBJECT_NAME") or str(norad),
            "apogee_km": sat.get("apogee_km"),
            "perigee_km": sat.get("perigee_km"),
            "height_km": sat.get("height_km"),
            "selected": sat.get("selected"),
        })

    # まず検索インデックスに載っている主要衛星を優先
    # MAX_SATSでGitHub Actionsの時間超過を防止
    targets = sorted(targets, key=lambda x: x["norad_id"])

    return targets[:MAX_SATS]


def should_append(history, tle1, tle2):
    if not history:
        return True

    latest = history[-1]
    return latest.get("tle1") != tle1 or latest.get("tle2") != tle2


def prune_history(history):
    cutoff = datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)
    kept = []

    for item in history:
        t = item.get("fetched_at") or item.get("tle_epoch")
        if not t:
            kept.append(item)
            continue

        try:
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
            if dt >= cutoff:
                kept.append(item)
        except Exception:
            kept.append(item)

    return kept


def update_one_sat(target):
    norad = target["norad_id"]
    catalog_name = target.get("name") or str(norad)

    tle = fetch_tle(norad)
    if not tle:
        print(f"[MISS] NORAD {norad}: no TLE")
        return False

    fetched_at = utc_now_iso()
    name = tle.get("name") or catalog_name

    latest_obj = {
        "norad_id": norad,
        "name": name,
        "fetched_at": fetched_at,
        "tle_epoch": tle.get("tle_epoch"),
        "tle1": tle["tle1"],
        "tle2": tle["tle2"],
    }

    save_json(LATEST_DIR / f"{norad}.json", latest_obj)

    hist_path = HISTORY_DIR / f"{norad}.json"
    hist_obj = load_json(hist_path, {
        "norad_id": norad,
        "name": name,
        "tle_history": []
    })

    hist_obj["norad_id"] = norad
    hist_obj["name"] = name

    history = hist_obj.get("tle_history", [])
    history = prune_history(history)

    entry = {
        "fetched_at": fetched_at,
        "tle_epoch": tle.get("tle_epoch"),
        "tle1": tle["tle1"],
        "tle2": tle["tle2"],
    }

    if should_append(history, tle["tle1"], tle["tle2"]):
        history.append(entry)

    hist_obj["tle_history"] = history

    save_json(hist_path, hist_obj)

    print(f"[OK] NORAD {norad}: {name} epoch={tle.get('tle_epoch')} history={len(history)}")
    return True


def main():
    targets = load_catalog_targets()

    if not targets:
        raise RuntimeError(f"No targets found from {CATALOG_PATH}")

    print(f"Targets: {len(targets)}")
    print(f"Keep days: {KEEP_DAYS}")

    ok = 0
    miss = 0

    for i, target in enumerate(targets, start=1):
        try:
            if update_one_sat(target):
                ok += 1
            else:
                miss += 1
        except Exception as e:
            miss += 1
            print(f"[ERROR] NORAD {target.get('norad_id')}: {e}")

        if i % 50 == 0:
            print(f"Progress: {i}/{len(targets)} ok={ok} miss={miss}")

        time.sleep(REQUEST_SLEEP_SEC)

    print(f"Done. ok={ok} miss={miss}")


if __name__ == "__main__":
    main()
