import os
import json
from pathlib import Path
from datetime import datetime, timezone

import requests


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs/data/catalog"
OUT_FILE = OUT_DIR / "satcat_analytics.json"
DEBUG_CHINA_FILE = OUT_DIR / "satcat_analytics_debug_china_recent.json"

USERNAME = os.environ.get("SPACETRACK_USERNAME")
PASSWORD = os.environ.get("SPACETRACK_PASSWORD")

BASE_URL = "https://www.space-track.org"
LOGIN_URL = f"{BASE_URL}/ajaxauth/login"

SATCAT_QUERY_URL = (
    f"{BASE_URL}/basicspacedata/query/"
    "class/satcat/"
    "OBJECT_TYPE/PAYLOAD/"
    "orderby/LAUNCH asc/"
    "format/json"
)

GP_QUERY_URL = (
    f"{BASE_URL}/basicspacedata/query/"
    "class/gp/"
    "decay_date/null-val/"
    "orderby/NORAD_CAT_ID asc,EPOCH desc/"
    "format/json"
)

OUT_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def safe_date(x):
    if not x:
        return None
    s = str(x).strip()
    if len(s) >= 10:
        return s[:10]
    return s or None


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
    n = str(name or "").upper().replace("_", "-")

    rules = [
        ("STARLINK", "STARLINK"),
        ("ONEWEB", "ONEWEB"),
        ("KUIPER", "KUIPER"),

        ("TJSW", "TJS"),
        ("TJS", "TJS"),
        ("TONGXIN JISHU SHIYAN", "TJS"),

        ("ZHONGXING", "CHINASAT"),
        ("CHINASAT", "CHINASAT"),
        ("CHINA SAT", "CHINASAT"),
        ("ZX-", "CHINASAT"),
        ("ZX ", "CHINASAT"),

        ("TIANTONG", "TIANTONG"),
        ("APSTAR", "APSTAR"),

        ("YAOGAN", "YAOGAN"),
        ("BEIDOU", "BEIDOU"),
        ("GPS", "GPS"),
        ("NAVSTAR", "GPS"),
        ("GLONASS", "GLONASS"),
        ("GALILEO", "GALILEO"),
        ("QZS", "QZSS"),
        ("QZSS", "QZSS"),
        ("MICHIBIKI", "QZSS"),

        ("COSMOS", "COSMOS"),
        ("FLOCK", "FLOCK"),
        ("LEMUR", "LEMUR"),
        ("GAOFEN", "GAOFEN"),
        ("TIANLIAN", "TIANLIAN"),
        ("AEHF", "AEHF"),
        ("WGS", "WGS"),
    ]

    for key, label in rules:
        if key in n:
            return label

    return "OTHER"


def is_geo_family(name, series, country):
    n = str(name or "").upper()

    geo_series = {
        "TJS",
        "CHINASAT",
        "TIANTONG",
        "APSTAR",
        "TIANLIAN",
        "AEHF",
        "WGS",
    }

    if series in geo_series:
        return True

    geo_words = [
        "TJS",
        "TJSW",
        "ZHONGXING",
        "CHINASAT",
        "CHINA SAT",
        "TIANTONG",
        "APSTAR",
        "TIANLIAN",
        "FENGYUN-4",
        "FY-4",
    ]

    if any(w in n for w in geo_words):
        return True

    return False


def classify_current_orbit(period_min, apogee_km, perigee_km, inclination_deg):
    period = safe_float(period_min)
    ap = safe_float(apogee_km)
    pe = safe_float(perigee_km)
    inc = safe_float(inclination_deg)

    if ap is not None and pe is not None:
        mean_alt = (ap + pe) / 2

        # 静止軌道付近
        if pe >= 34000 and ap <= 38000:
            return "GEO"

        # GTO / GEO transfer
        if ap >= 30000 and pe < 10000:
            return "GTO"

        if mean_alt < 2000:
            return "LEO"

        if mean_alt < 30000:
            return "MEO"

        if mean_alt >= 30000:
            return "HEO/GEO"

    if period is not None:
        if 1300 <= period <= 1500:
            return "GEO"
        if period < 128:
            return "LEO"
        if period < 1000:
            return "MEO"
        if period > 1000:
            return "HEO/GEO"

    if inc is not None and inc > 63:
        return "HEO/SSO"

    return "UNKNOWN"


def classify_stats_orbit(name, series, country, current_orbit):
    # 打上げ統計では、TJS/中星/天通などはGTO段階でもGEO系として数える
    if is_geo_family(name, series, country):
        return "GEO"

    if current_orbit == "GTO":
        return "GEO"

    return current_orbit


def login_space_track():
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
        raise RuntimeError(f"Space-Track login failed HTTP {r.status_code}: {r.text[:200]}")

    return session


def fetch_json(session, url, label):
    print(f"Fetch {label}...")
    r = session.get(url, timeout=240)

    if r.status_code != 200:
        raise RuntimeError(f"{label} query failed HTTP {r.status_code}: {r.text[:300]}")

    try:
        return r.json()
    except Exception:
        raise RuntimeError(f"{label} JSON parse failed: {r.text[:300]}")


def build_gp_latest_map(gp_rows):
    gp_map = {}

    for x in gp_rows:
        norad = safe_int(x.get("NORAD_CAT_ID"))
        if norad is None:
            continue

        epoch = str(x.get("EPOCH") or "")

        if norad not in gp_map:
            gp_map[norad] = x
            continue

        old_epoch = str(gp_map[norad].get("EPOCH") or "")
        if epoch > old_epoch:
            gp_map[norad] = x

    return gp_map


def main():
    session = login_space_track()

    satcat_rows = fetch_json(session, SATCAT_QUERY_URL, "SATCAT")
    gp_rows = fetch_json(session, GP_QUERY_URL, "GP latest")

    gp_map = build_gp_latest_map(gp_rows)

    sats = []
    skipped_no_launch = 0
    missing_gp = 0

    for x in satcat_rows:
        norad = safe_int(x.get("NORAD_CAT_ID"))
        name = x.get("OBJECT_NAME") or x.get("SATNAME") or ""

        if norad is None:
            continue

        launch_date = safe_date(x.get("LAUNCH"))
        if not launch_date:
            skipped_no_launch += 1
            continue

        decay_date = safe_date(x.get("DECAY"))
        country = norm_country(x.get("COUNTRY"))
        series = detect_series(name)

        gp = gp_map.get(norad)
        if gp is None:
            missing_gp += 1
            gp = {}

        period = gp.get("PERIOD")
        apogee = gp.get("APOGEE")
        perigee = gp.get("PERIGEE")
        inclination = gp.get("INCLINATION")

        current_orbit = classify_current_orbit(period, apogee, perigee, inclination)
        stats_orbit = classify_stats_orbit(name, series, country, current_orbit)

        sats.append({
            "norad": norad,
            "name": name,
            "country": country,
            "launch_date": launch_date,
            "decay_date": decay_date,

            # HTML側の軌道フィルタはこれを見る
            "orbit": stats_orbit,

            # デバッグ用：実際の最新軌道分類
            "orbit_current": current_orbit,

            "series": series,
            "gp_epoch": gp.get("EPOCH"),

            "period_min": safe_float(period),
            "apogee_km": safe_float(apogee),
            "perigee_km": safe_float(perigee),
            "inclination_deg": safe_float(inclination),

            "intldes": x.get("INTLDES"),
            "object_type": x.get("OBJECT_TYPE"),
        })

    out = {
        "updated_at": utc_now_iso(),
        "source": "Space-Track SATCAT + GP",
        "count": len(sats),
        "skipped_no_launch": skipped_no_launch,
        "missing_gp": missing_gp,
        "notes": [
            "orbit is for statistics/filtering.",
            "orbit_current is classified from latest GP elements.",
            "GEO-family satellites such as TJS/TJSW/CHINASAT/TIANTONG are counted as GEO even if current GP still looks like GTO."
        ],
        "satellites": sats,
    }

    OUT_FILE.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    # 中国の最近のGEO系/TJS確認用デバッグ
    china_recent = [
        s for s in sats
        if s["country"] == "CHN"
        and s["launch_date"] >= "2025-01-01"
        and (
            s["orbit"] == "GEO"
            or s["series"] in ["TJS", "CHINASAT", "TIANTONG", "APSTAR", "TIANLIAN"]
        )
    ]

    DEBUG_CHINA_FILE.write_text(
        json.dumps({
            "updated_at": utc_now_iso(),
            "count": len(china_recent),
            "satellites": china_recent,
        }, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"Wrote {OUT_FILE}")
    print(f"Satellites: {len(sats)}")
    print(f"Skipped no launch: {skipped_no_launch}")
    print(f"Missing GP: {missing_gp}")
    print(f"Wrote debug: {DEBUG_CHINA_FILE}")


if __name__ == "__main__":
    main()