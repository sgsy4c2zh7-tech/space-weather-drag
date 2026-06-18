import os
import json
from pathlib import Path
from datetime import datetime, timezone

import requests


ROOT = Path(__file__).resolve().parents[1]

OUT_DIR = ROOT / "docs/data/catalog"
OUT_FILE = OUT_DIR / "satcat_analytics.json"
DEBUG_CHINA_FILE = OUT_DIR / "satcat_analytics_debug_china_recent.json"
DEBUG_GEO_FILE = OUT_DIR / "satcat_analytics_debug_geo_recent.json"

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
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


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

    if not s:
        return None

    if len(s) >= 10:
        return s[:10]

    return s


def month_key(date_str):
    d = safe_date(date_str)
    if not d or len(d) < 7:
        return None
    return d[:7]


def year_key(date_str):
    d = safe_date(date_str)
    if not d or len(d) < 4:
        return None
    return d[:4]


def quarter_key(date_str):
    d = safe_date(date_str)
    if not d or len(d) < 7:
        return None

    try:
        y = int(d[:4])
        m = int(d[5:7])
        q = ((m - 1) // 3) + 1
        return f"{y}-Q{q}"
    except Exception:
        return None


def norm_country(c):
    c = str(c or "").upper().strip()

    if c in ["US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"]:
        return "US"

    if c in ["PRC", "CHN", "CN", "CHINA", "PEOPLE'S REPUBLIC OF CHINA"]:
        return "CHN"

    if c in ["CIS", "RUS", "RU", "RUSSIA", "USSR"]:
        return "RUS"

    if c in ["JPN", "JP", "JAPAN"]:
        return "JPN"

    if c in ["ESA", "EU", "EUME", "EUTE"]:
        return "EU/ESA"

    return c or "UNK"


def normalize_name(name):
    return str(name or "").upper().replace("_", "-").replace("  ", " ").strip()


def detect_series(name):
    n = normalize_name(name)

    rules = [
        ("STARLINK", "STARLINK"),
        ("ONEWEB", "ONEWEB"),
        ("KUIPER", "KUIPER"),
        ("GUOWANG", "GUOWANG"),
        ("GW-", "GUOWANG"),

        ("TJSW", "TJS"),
        ("TJS-", "TJS"),
        ("TJS ", "TJS"),
        ("TJS", "TJS"),
        ("TONGXIN JISHU SHIYAN", "TJS"),

        ("ZHONGXING", "CHINASAT"),
        ("CHINASAT", "CHINASAT"),
        ("CHINA SAT", "CHINASAT"),
        ("ZX-", "CHINASAT"),
        ("ZX ", "CHINASAT"),

        ("TIANTONG", "TIANTONG"),
        ("APSTAR", "APSTAR"),
        ("ASIASAT", "ASIASAT"),
        ("EXPRESS", "EXPRESS"),
        ("YAMAL", "YAMAL"),

        ("YAOGAN", "YAOGAN"),
        ("GAOFEN", "GAOFEN"),
        ("TIANLIAN", "TIANLIAN"),
        ("FENGYUN", "FENGYUN"),
        ("FY-", "FENGYUN"),

        ("BEIDOU", "BEIDOU"),
        ("GPS", "GPS"),
        ("NAVSTAR", "GPS"),
        ("GLONASS", "GLONASS"),
        ("GALILEO", "GALILEO"),
        ("QZS", "QZSS"),
        ("QZSS", "QZSS"),
        ("MICHIBIKI", "QZSS"),
        ("IRNSS", "NAVIC"),
        ("NAVIC", "NAVIC"),

        ("COSMOS", "COSMOS"),
        ("FLOCK", "FLOCK"),
        ("LEMUR", "LEMUR"),
        ("PLANET", "PLANET"),
        ("SKYSAT", "SKYSAT"),

        ("AEHF", "AEHF"),
        ("WGS", "WGS"),
        ("MUOS", "MUOS"),
        ("SBIRS", "SBIRS"),
        ("DSP", "DSP"),
    ]

    for key, label in rules:
        if key in n:
            return label

    return "OTHER"


def is_geo_family(name, series, country):
    n = normalize_name(name)

    geo_series = {
        "TJS",
        "CHINASAT",
        "TIANTONG",
        "APSTAR",
        "ASIASAT",
        "TIANLIAN",
        "FENGYUN",
        "AEHF",
        "WGS",
        "MUOS",
        "SBIRS",
        "DSP",
        "EXPRESS",
        "YAMAL",
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
        "ASIASAT",
        "TIANLIAN",
        "FENGYUN-4",
        "FY-4",
        "AEHF",
        "WGS",
        "MUOS",
        "SBIRS",
        "DSP",
        "EXPRESS",
        "YAMAL",
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
        mean_alt = (ap + pe) / 2.0

        # GEO / GSO周辺。少し広めに取る。
        if pe >= 33000 and ap <= 39000:
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
        if 1250 <= period <= 1550:
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
    # 打上げ統計では、GEO系衛星はGTO段階でもGEO系として数える。
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
        raise RuntimeError(
            f"Space-Track login failed HTTP {r.status_code}: {r.text[:200]}"
        )

    return session


def fetch_json(session, url, label):
    print(f"Fetch {label}...")
    r = session.get(url, timeout=300)

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


def build_satellite_record(satcat_row, gp_map):
    norad = safe_int(satcat_row.get("NORAD_CAT_ID"))
    name = satcat_row.get("OBJECT_NAME") or satcat_row.get("SATNAME") or ""

    if norad is None:
        return None, "missing_norad"

    launch_date = safe_date(satcat_row.get("LAUNCH"))

    if not launch_date:
        return None, "missing_launch"

    decay_date = safe_date(satcat_row.get("DECAY"))
    country = norm_country(satcat_row.get("COUNTRY"))
    series = detect_series(name)

    gp = gp_map.get(norad) or {}

    period = gp.get("PERIOD")
    apogee = gp.get("APOGEE")
    perigee = gp.get("PERIGEE")
    inclination = gp.get("INCLINATION")

    current_orbit = classify_current_orbit(period, apogee, perigee, inclination)
    stats_orbit = classify_stats_orbit(name, series, country, current_orbit)

    mk = month_key(launch_date)
    qk = quarter_key(launch_date)
    yk = year_key(launch_date)

    record = {
        "norad": norad,
        "name": name,
        "country": country,

        "launch_date": launch_date,
        "launch_month": mk,
        "launch_quarter": qk,
        "launch_year": yk,

        "decay_date": decay_date,

        # HTML側の軌道フィルタはこれを見る。
        "orbit": stats_orbit,

        # デバッグ用：最新GPから見た実際の現在軌道分類。
        "orbit_current": current_orbit,

        "series": series,
        "gp_epoch": gp.get("EPOCH"),

        "period_min": safe_float(period),
        "apogee_km": safe_float(apogee),
        "perigee_km": safe_float(perigee),
        "inclination_deg": safe_float(inclination),

        "intldes": satcat_row.get("INTLDES"),
        "object_type": satcat_row.get("OBJECT_TYPE"),

        # HTML tooltip / CSV / Excel用
        "label": f"{name} / {norad}",
        "export_name": name,
        "export_norad": norad,
        "export_launch_date": launch_date,
        "export_country": country,
        "export_orbit": stats_orbit,
        "export_series": series,
    }

    return record, None


def build_summary(sats):
    by_country = {}
    by_series = {}
    by_orbit = {}
    by_month = {}

    for s in sats:
        by_country[s["country"]] = by_country.get(s["country"], 0) + 1
        by_series[s["series"]] = by_series.get(s["series"], 0) + 1
        by_orbit[s["orbit"]] = by_orbit.get(s["orbit"], 0) + 1

        mk = s.get("launch_month") or "UNKNOWN"
        by_month[mk] = by_month.get(mk, 0) + 1

    return {
        "by_country": dict(sorted(by_country.items())),
        "by_series": dict(sorted(by_series.items())),
        "by_orbit": dict(sorted(by_orbit.items())),
        "by_month": dict(sorted(by_month.items())),
    }


def write_debug_files(sats):
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
        json.dumps(
            {
                "updated_at": utc_now_iso(),
                "count": len(china_recent),
                "satellites": china_recent,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    geo_recent = [
        s for s in sats
        if s["launch_date"] >= "2025-01-01"
        and s["orbit"] == "GEO"
    ]

    DEBUG_GEO_FILE.write_text(
        json.dumps(
            {
                "updated_at": utc_now_iso(),
                "count": len(geo_recent),
                "satellites": geo_recent,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def main():
    session = login_space_track()

    satcat_rows = fetch_json(session, SATCAT_QUERY_URL, "SATCAT")
    gp_rows = fetch_json(session, GP_QUERY_URL, "GP latest")

    gp_map = build_gp_latest_map(gp_rows)

    sats = []
    skipped_no_launch = 0
    skipped_missing_norad = 0
    missing_gp = 0

    for row in satcat_rows:
        sat, reason = build_satellite_record(row, gp_map)

        if reason == "missing_launch":
            skipped_no_launch += 1
            continue

        if reason == "missing_norad":
            skipped_missing_norad += 1
            continue

        if sat is None:
            continue

        if not sat.get("gp_epoch"):
            missing_gp += 1

        sats.append(sat)

    sats.sort(key=lambda x: (x.get("launch_date") or "", x.get("norad") or 0))

    summary = build_summary(sats)

    out = {
        "updated_at": utc_now_iso(),
        "source": "Space-Track SATCAT + GP",
        "count": len(sats),
        "skipped_no_launch": skipped_no_launch,
        "skipped_missing_norad": skipped_missing_norad,
        "missing_gp": missing_gp,
        "notes": [
            "orbit is for statistics/filtering.",
            "orbit_current is classified from latest GP elements.",
            "GEO-family satellites such as TJS/TJSW/CHINASAT/TIANTONG are counted as GEO even if current GP still looks like GTO.",
            "launch_month, launch_quarter, and launch_year are included for chart grouping.",
            "The satellites array is intentionally kept detailed for HTML tooltip, CSV, and Excel export."
        ],
        "summary": summary,
        "satellites": sats,
    }

    OUT_FILE.write_text(
        json.dumps(out, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    write_debug_files(sats)

    print(f"Wrote {OUT_FILE}")
    print(f"Satellites: {len(sats)}")
    print(f"Skipped no launch: {skipped_no_launch}")
    print(f"Skipped missing NORAD: {skipped_missing_norad}")
    print(f"Missing GP: {missing_gp}")
    print(f"Wrote debug: {DEBUG_CHINA_FILE}")
    print(f"Wrote debug: {DEBUG_GEO_FILE}")


if __name__ == "__main__":
    main()
