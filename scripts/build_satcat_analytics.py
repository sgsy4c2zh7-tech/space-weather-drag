import os
import json
from pathlib import Path
from datetime import datetime, timezone

import requests


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs/data/catalog"
OUT_FILE = OUT_DIR / "satcat_analytics.json"

USERNAME = os.environ.get("SPACETRACK_USERNAME")
PASSWORD = os.environ.get("SPACETRACK_PASSWORD")

BASE_URL = "https://www.space-track.org"
LOGIN_URL = f"{BASE_URL}/ajaxauth/login"

QUERY_URL = (
    f"{BASE_URL}/basicspacedata/query/"
    "class/satcat/"
    "object_type/PAYLOAD/"
    "orderby/LAUNCH asc/"
    "format/json"
)

OUT_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def detect_series(name):
    n = str(name or "").upper()

    rules = [
        ("STARLINK", "STARLINK"),
        ("ONEWEB", "ONEWEB"),
        ("KUIPER", "KUIPER"),
        ("YAOGAN", "YAOGAN"),
        ("BEIDOU", "BEIDOU"),
        ("GPS", "GPS"),
        ("GLONASS", "GLONASS"),
        ("GALILEO", "GALILEO"),
        ("QZS", "QZSS"),
        ("QZSS", "QZSS"),
        ("IRNSS", "IRNSS"),
        ("NAVIC", "NAVIC"),
        ("COSMOS", "COSMOS"),
        ("FLOCK", "FLOCK"),
        ("LEMUR", "LEMUR"),
        ("PLANET", "PLANET"),
        ("GAOFEN", "GAOFEN"),
        ("TIANLIAN", "TIANLIAN"),
        ("TJS", "TJS"),
        ("SKYNET", "SKYNET"),
        ("AEHF", "AEHF"),
        ("WGS", "WGS"),
    ]

    for key, label in rules:
        if key in n:
            return label

    return "OTHER"


def classify_orbit(period_min, apogee_km, perigee_km, inclination_deg):
    try:
        period = float(period_min) if period_min not in [None, ""] else None
    except Exception:
        period = None

    try:
        ap = float(apogee_km) if apogee_km not in [None, ""] else None
        pe = float(perigee_km) if perigee_km not in [None, ""] else None
    except Exception:
        ap = pe = None

    try:
        inc = float(inclination_deg) if inclination_deg not in [None, ""] else None
    except Exception:
        inc = None

    if ap is not None and pe is not None:
        mean_alt = (ap + pe) / 2

        if pe >= 35000 and ap <= 37000:
            return "GEO"
        if mean_alt < 2000:
            return "LEO"
        if mean_alt < 30000:
            return "MEO"
        if mean_alt >= 30000:
            return "HEO/GEO"

    if period is not None:
        if period < 128:
            return "LEO"
        if period < 1000:
            return "MEO"
        if 1300 <= period <= 1500:
            return "GEO"
        if period > 1000:
            return "HEO/GEO"

    if inc is not None and inc > 63:
        return "HEO/SSO"

    return "UNKNOWN"


def safe_date(x):
    if not x:
        return None
    s = str(x).strip()
    if len(s) >= 10:
        return s[:10]
    return s or None


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

    print("Fetch SATCAT...")
    r = session.get(QUERY_URL, timeout=180)

    if r.status_code != 200:
        raise RuntimeError(f"SATCAT query failed HTTP {r.status_code}: {r.text[:300]}")

    raw = r.json()
    sats = []

    for x in raw:
        norad = x.get("NORAD_CAT_ID")
        name = x.get("OBJECT_NAME")

        launch_date = safe_date(x.get("LAUNCH"))
        if not launch_date:
            continue

        decay_date = safe_date(x.get("DECAY"))

        country = norm_country(x.get("COUNTRY"))

        period = x.get("PERIOD")
        apogee = x.get("APOGEE")
        perigee = x.get("PERIGEE")
        inclination = x.get("INCLINATION")

        sats.append({
            "norad": int(norad) if str(norad).isdigit() else norad,
            "name": name,
            "country": country,
            "launch_date": launch_date,
            "decay_date": decay_date,
            "orbit": classify_orbit(period, apogee, perigee, inclination),
            "series": detect_series(name),
            "period_min": float(period) if period not in [None, ""] else None,
            "apogee_km": float(apogee) if apogee not in [None, ""] else None,
            "perigee_km": float(perigee) if perigee not in [None, ""] else None,
            "inclination_deg": float(inclination) if inclination not in [None, ""] else None,
        })

    out = {
        "updated_at": utc_now_iso(),
        "source": "Space-Track SATCAT",
        "count": len(sats),
        "satellites": sats,
    }

    OUT_FILE.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote {OUT_FILE}")
    print(f"Satellites: {len(sats)}")


if __name__ == "__main__":
    main()
