import json
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]

SATCAT_FILE = ROOT / "docs/data/catalog/satcat_full.json"
OUT_FILE = ROOT / "docs/data/catalog/satcat_analytics.json"


def orbit_type(apogee, perigee):
    if apogee is None or perigee is None:
        return "Unknown"

    mean_alt = (apogee + perigee) / 2

    if mean_alt < 2000:
        return "LEO"

    if mean_alt < 30000:
        return "MEO"

    if mean_alt < 38000:
        return "GEO"

    return "HEO"


def month_key(date):
    if not date:
        return None

    if len(date) < 7:
        return None

    return date[:7]


def series_name(name):

    if not name:
        return "Unknown"

    n = name.upper()

    rules = [
        "STARLINK",
        "ONEWEB",
        "GPS",
        "GLONASS",
        "BEIDOU",
        "GALILEO",
        "YAOGAN",
        "SJ",
        "SHIJIAN",
        "TJS",
        "TIANLIAN",
        "QZS",
        "IRNSS",
        "COSMOS",
        "NROL",
        "USA",
        "OBJECT",
        "DEB",
        "R/B"
    ]

    for r in rules:
        if r in n:
            return r

    return n.split()[0]


with open(SATCAT_FILE, encoding="utf-8") as f:
    satcat = json.load(f)

objects = satcat["objects"]

monthly = defaultdict(int)
country_monthly = defaultdict(int)
series_monthly = defaultdict(int)
orbit_monthly = defaultdict(int)

details = []

for obj in objects:

    launch = obj.get("launch_date")

    month = month_key(launch)

    if month is None:
        continue

    country = obj.get("country") or "UNK"

    orbit = orbit_type(
        obj.get("apogee_km"),
        obj.get("perigee_km")
    )

    series = series_name(obj.get("object_name"))

    monthly[month] += 1

    country_monthly[(country, month)] += 1

    series_monthly[(series, month)] += 1

    orbit_monthly[(orbit, month)] += 1

    details.append({
        "norad_id": obj["norad_id"],
        "object_name": obj.get("object_name"),
        "object_type": obj.get("object_type"),
        "country": country,
        "launch_date": launch,
        "series": series,
        "orbit": orbit,
    })


country_data = defaultdict(list)

for (country, month), count in sorted(country_monthly.items()):
    country_data[country].append({
        "month": month,
        "count": count
    })

series_data = defaultdict(list)

for (series, month), count in sorted(series_monthly.items()):
    series_data[series].append({
        "month": month,
        "count": count
    })

orbit_data = defaultdict(list)

for (orbit, month), count in sorted(orbit_monthly.items()):
    orbit_data[orbit].append({
        "month": month,
        "count": count
    })


output = {

    "updated_at": satcat["updated_at"],

    "monthly_total": [
        {
            "month": m,
            "count": c
        }
        for m, c in sorted(monthly.items())
    ],

    "country": dict(country_data),

    "series": dict(series_data),

    "orbit": dict(orbit_data),

    "details": details

}

OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

print("Analytics written:", OUT_FILE)
print("Objects:", len(details))
