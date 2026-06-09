import json
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests


ROOT = Path(__file__).resolve().parents[1]

CATALOG_PATH = ROOT / "docs/data/satellite_index_full.json"
TLE_DIR = ROOT / "docs/data/tle"
TLE_HISTORY_DIR = ROOT / "docs/data/tle_history"

KEEP_DAYS = 30
SLEEP_SEC = 0.35
MAX_TARGETS = 1500

TLE_DIR.mkdir(parents=True, exist_ok=True)
TLE_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


def load_json(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path, data):
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def valid_tle(tle1, tle2):
    return (
        isinstance(tle1, str)
        and isinstance(tle2, str)
        and tle1.strip().startswith("1 ")
        and tle2.strip().startswith("2 ")
    )


def parse_tle_epoch(line1):
    try:
        yy = int(line1[18:20])
        doy = float(line1[20:32])
        year = 2000 + yy if yy < 57 else 1900 + yy
        dt = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=doy - 1)
        return iso(dt.replace(microsecond=0))
    except Exception:
        return None


def parse_tle_text(text):
    if not text:
        return None

    if (
        "No GP data found" in text
        or "Invalid query" in text
        or text.strip().startswith("<")
    ):
        return None

    lines = [x.strip() for x in text.splitlines() if x.strip()]

    for i in range(len(lines) - 1):
        if lines[i].startswith("1 ") and lines[i + 1].startswith("2 "):
            tle1 = lines[i]
            tle2 = lines[i + 1]

            if not valid_tle(tle1, tle2):
                return None

            name = None
            if i > 0 and not lines[i - 1].startswith(("1 ", "2 ")):
                name = lines[i - 1]

            return {
                "name": name,
                "tle1": tle1,
                "tle2": tle2,
                "tle_epoch": parse_tle_epoch(tle1),
            }

    return None


def fetch_tle_from_celestrak(norad):
    for fmt in ["TLE", "3LE", "2LE"]:
        url = f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad}&FORMAT={fmt}"
        try:
            r = requests.get(url, timeout=20)
            if r.status_code != 200:
                continue

            parsed = parse_tle_text(r.text)
            if parsed:
                return parsed

        except Exception as e:
            print(f"[WARN] NORAD {norad} {fmt} failed: {e}")

    return None


def load_targets():
    catalog = load_json(CATALOG_PATH, [])
    targets = []
    seen = set()

    for s in catalog:
        norad = (
            s.get("norad_id")
            or s.get("NORAD_CAT_ID")
            or s.get("catalog_number")
            or s.get("id")
        )

        if norad is None:
            continue

        try:
            norad = int(norad)
        except Exception:
            continue

        if norad in seen:
            continue

        seen.add(norad)

        name = (
            s.get("name")
            or s.get("OBJECT_NAME")
            or s.get("object_name")
            or str(norad)
        )

        targets.append({
            "norad_id": norad,
            "name": name,
        })

    targets.sort(key=lambda x: x["norad_id"])
    return targets[:MAX_TARGETS]


def prune_history(history):
    cutoff = now_utc() - timedelta(days=KEEP_DAYS)
    out = []

    for item in history:
        t = item.get("fetched_at") or item.get("tle_epoch")
        if not t:
            out.append(item)
            continue

        try:
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
            if dt >= cutoff:
                out.append(item)
        except Exception:
            out.append(item)

    return out


def should_append(history, tle1, tle2):
    if not history:
        return True

    last = history[-1]
    return last.get("tle1") != tle1 or last.get("tle2") != tle2


def update_satellite(target):
    norad = target["norad_id"]
    fallback_name = target["name"]

    tle = fetch_tle_from_celestrak(norad)

    if not tle:
        print(f"[MISS] {norad} no TLE")
        return False

    fetched_at = iso(now_utc())
    name = tle.get("name") or fallback_name or str(norad)

    latest = {
        "norad_id": norad,
        "name": name,
        "fetched_at": fetched_at,
        "tle_epoch": tle.get("tle_epoch"),
        "tle1": tle["tle1"],
        "tle2": tle["tle2"],
    }

    save_json(TLE_DIR / f"{norad}.json", latest)

    hist_path = TLE_HISTORY_DIR / f"{norad}.json"
    hist_obj = load_json(hist_path, {
        "norad_id": norad,
        "name": name,
        "tle_history": [],
    })

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

    hist_obj = {
        "norad_id": norad,
        "name": name,
        "tle_history": history,
    }

    save_json(hist_path, hist_obj)

    print(f"[OK] {norad} {name} epoch={tle.get('tle_epoch')} hist={len(history)}")
    return True


def main():
    targets = load_targets()

    if not targets:
        raise RuntimeError(f"No targets from {CATALOG_PATH}")

    print(f"targets={len(targets)} keep_days={KEEP_DAYS}")

    ok = 0
    miss = 0

    for i, target in enumerate(targets, start=1):
        try:
            if update_satellite(target):
                ok += 1
            else:
                miss += 1
        except Exception as e:
            miss += 1
            print(f"[ERROR] {target.get('norad_id')}: {e}")

        if i % 50 == 0:
            print(f"progress {i}/{len(targets)} ok={ok} miss={miss}")

        time.sleep(SLEEP_SEC)

    print(f"done ok={ok} miss={miss}")


if __name__ == "__main__":
    main()