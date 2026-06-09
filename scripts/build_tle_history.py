import os
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests


ROOT = Path(__file__).resolve().parents[1]

TLE_DIR = ROOT / "docs/data/tle"
TLE_HISTORY_DIR = ROOT / "docs/data/tle_history"

KEEP_DAYS = 7

USERNAME = os.environ.get("SPACETRACK_USERNAME")
PASSWORD = os.environ.get("SPACETRACK_PASSWORD")

BASE_URL = "https://www.space-track.org"
LOGIN_URL = f"{BASE_URL}/ajaxauth/login"

# アクティブっぽい衛星の最新GPをTLE形式で取得
QUERY_URL = (
    f"{BASE_URL}/basicspacedata/query/"
    "class/gp/"
    "decay_date/null-val/"
    "epoch/>now-10/"
    "orderby/NORAD_CAT_ID asc,EPOCH desc/"
    "format/tle"
)


TLE_DIR.mkdir(parents=True, exist_ok=True)
TLE_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)


def to_iso(dt):
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


def is_valid_tle_pair(line1, line2):
    return (
        isinstance(line1, str)
        and isinstance(line2, str)
        and line1.startswith("1 ")
        and line2.startswith("2 ")
    )


def norad_from_tle1(line1):
    try:
        return int(line1[2:7])
    except Exception:
        return None


def parse_tle_epoch(line1):
    try:
        yy = int(line1[18:20])
        doy = float(line1[20:32])
        year = 2000 + yy if yy < 57 else 1900 + yy
        dt = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=doy - 1)
        return to_iso(dt)
    except Exception:
        return None


def parse_space_track_tle(text):
    lines = [x.rstrip() for x in text.splitlines() if x.strip()]
    items = []

    i = 0
    while i < len(lines):
        name = None

        if (
            i + 2 < len(lines)
            and not lines[i].startswith("1 ")
            and lines[i + 1].startswith("1 ")
            and lines[i + 2].startswith("2 ")
        ):
            name = lines[i].strip()
            line1 = lines[i + 1].strip()
            line2 = lines[i + 2].strip()
            i += 3

        elif (
            i + 1 < len(lines)
            and lines[i].startswith("1 ")
            and lines[i + 1].startswith("2 ")
        ):
            line1 = lines[i].strip()
            line2 = lines[i + 1].strip()
            i += 2

        else:
            i += 1
            continue

        if not is_valid_tle_pair(line1, line2):
            continue

        norad = norad_from_tle1(line1)
        if norad is None:
            continue

        items.append({
            "norad_id": norad,
            "name": name or str(norad),
            "tle_epoch": parse_tle_epoch(line1),
            "tle1": line1,
            "tle2": line2,
        })

    return items


def dedupe_latest_by_norad(items):
    latest = {}

    for item in items:
        norad = item["norad_id"]
        epoch = item.get("tle_epoch") or ""

        if norad not in latest:
            latest[norad] = item
            continue

        old_epoch = latest[norad].get("tle_epoch") or ""

        if epoch > old_epoch:
            latest[norad] = item

    return list(latest.values())


def prune_history(history):
    cutoff = now_utc() - timedelta(days=KEEP_DAYS)
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


def should_append(history, tle1, tle2):
    if not history:
        return True

    last = history[-1]
    return last.get("tle1") != tle1 or last.get("tle2") != tle2


def update_one(item, fetched_at):
    norad = item["norad_id"]
    name = item.get("name") or str(norad)

    latest_obj = {
        "norad_id": norad,
        "name": name,
        "fetched_at": fetched_at,
        "tle_epoch": item.get("tle_epoch"),
        "tle1": item["tle1"],
        "tle2": item["tle2"],
    }

    save_json(TLE_DIR / f"{norad}.json", latest_obj)

    hist_path = TLE_HISTORY_DIR / f"{norad}.json"
    hist_obj = load_json(hist_path, {
        "norad_id": norad,
        "name": name,
        "tle_history": []
    })

    history = hist_obj.get("tle_history", [])
    history = prune_history(history)

    entry = {
        "fetched_at": fetched_at,
        "tle_epoch": item.get("tle_epoch"),
        "tle1": item["tle1"],
        "tle2": item["tle2"],
    }

    if should_append(history, item["tle1"], item["tle2"]):
        history.append(entry)

    hist_obj = {
        "norad_id": norad,
        "name": name,
        "tle_history": history
    }

    save_json(hist_path, hist_obj)

    return len(history)


def main():
    if not USERNAME or not PASSWORD:
        raise RuntimeError(
            "SPACETRACK_USERNAME / SPACETRACK_PASSWORD がGitHub Secretsにありません。"
        )

    session = requests.Session()

    print("Login Space-Track...")
    login_res = session.post(
        LOGIN_URL,
        data={
            "identity": USERNAME,
            "password": PASSWORD,
        },
        timeout=30,
    )

    if login_res.status_code != 200:
        raise RuntimeError(f"Space-Track login failed HTTP {login_res.status_code}")

    print("Fetch active GP TLE...")
    res = session.get(QUERY_URL, timeout=120)

    if res.status_code != 200:
        raise RuntimeError(f"Space-Track query failed HTTP {res.status_code}: {res.text[:300]}")

    items = parse_space_track_tle(res.text)
    items = dedupe_latest_by_norad(items)

    if not items:
        raise RuntimeError("No TLE items parsed from Space-Track response.")

    fetched_at = to_iso(now_utc())

    print(f"Parsed latest active TLE count: {len(items)}")
    print(f"Fetched at: {fetched_at}")
    print(f"Keep days: {KEEP_DAYS}")

    ok = 0

    for item in items:
        hist_len = update_one(item, fetched_at)
        ok += 1

        if ok % 1000 == 0:
            print(f"Progress: {ok}/{len(items)}")

    manifest = {
        "updated_at": fetched_at,
        "source": "Space-Track class/gp format/tle",
        "filter": {
            "decay_date": "null-val",
            "epoch": ">now-10",
            "latest_per_norad": True,
        },
        "keep_days": KEEP_DAYS,
        "count": ok,
    }

    save_json(TLE_HISTORY_DIR / "index.json", manifest)
    save_json(TLE_DIR / "index.json", manifest)

    print(f"Done. saved={ok}")


if __name__ == "__main__":
    main()