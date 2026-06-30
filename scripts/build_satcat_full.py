import os
import json
from pathlib import Path
from datetime import datetime, timezone

import requests


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs/data/catalog"
OUT_FILE = OUT_DIR / "satcat_full.json"

USERNAME = os.environ.get("SPACETRACK_USERNAME")
PASSWORD = os.environ.get("SPACETRACK_PASSWORD")

BASE_URL = "https://www.space-track.org"
LOGIN_URL = f"{BASE_URL}/ajaxauth/login"

QUERY_URL = (
    f"{BASE_URL}/basicspacedata/query/"
    "class/satcat/"
    "orderby/NORAD_CAT_ID asc/"
    "format/json"
)

OUT_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_date(x):
    if not x:
        return None
    s = str(x).strip()
    return s[:10] if len(s) >= 10 else s


def safe_float(x):
    try:
        if x in [None, ""]:
            return None
        return float(x)
    except Exception:
        return None


def safe_int(x):
    try:
        if x in [None, ""]:
            return None
        return int(x)
    except Exception:
        return None


def norm_country(c):
    c = str(c or "").upper().strip()

    if c in ["US", "USA", "UNITED STATES"]:
        return "US"
    if c in ["PRC", "CHN", "CN", "CHINA"]:
        return "CHN"
    if c in ["CIS", "RUS", "RU", "RUSSIA"]:
        return "RUS"
    if c in ["JPN", "JP", "JAPAN"]:
        return "JPN"

    return c or "UNK"


def main():
    if not USERNAME or not PASSWORD:
        raise RuntimeError("SPACETRACK_USERNAME / SPACETRACK_PASSWORD がありません。")

    session = requests.Session()

    print("Login Space-Track...")
    r = session.post(
        LOGIN_URL,
        data={
            "identity": USERNAME,
            "password": PASSWORD,
        },
        timeout=30,
    )

    if r.status_code != 200:
        raise RuntimeError(f"Space-Track login failed HTTP {r.status_code}")

    print("Fetch full SATCAT...")
    r = session.get(QUERY_URL, timeout=240)

    if r.status_code != 200:
        raise RuntimeError(f"SATCAT query failed HTTP {r.status_code}: {r.text[:300]}")

    raw = r.json()
    objects = []

    for x in raw:
        norad = safe_int(x.get("NORAD_CAT_ID"))

        if norad is None:
            continue

        objects.append({
            "norad_id": norad,
            "object_name": x.get("OBJECT_NAME"),
            "object_id": x.get("OBJECT_ID"),
            "object_type": x.get("OBJECT_TYPE"),
            "country": norm_country(x.get("COUNTRY")),
            "raw_country": x.get("COUNTRY"),
            "launch_date": safe_date(x.get("LAUNCH")),
            "decay_date": safe_date(x.get("DECAY")),
            "period_min": safe_float(x.get("PERIOD")),
            "inclination_deg": safe_float(x.get("INCLINATION")),
            "apogee_km": safe_float(x.get("APOGEE")),
            "perigee_km": safe_float(x.get("PERIGEE")),
            "rcs": x.get("RCS"),
            "site": x.get("SITE"),
            "comment": x.get("COMMENT"),
        })

    out = {
        "updated_at": utc_now_iso(),
        "source": "Space-Track SATCAT full",
        "count": len(objects),
        "objects": objects,
    }

    OUT_FILE.write_text(
        json.dumps(out, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"Wrote {OUT_FILE}")
    print(f"Objects: {len(objects)}")


if __name__ == "__main__":
    main()