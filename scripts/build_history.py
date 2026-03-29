import json
import math
import os
from datetime import datetime, timezone, timedelta

FILEPATH = "docs/data/25544.json"
EARTH_RADIUS_KM = 6378.137
MU = 398600.4418  # km^3/s^2

def calc_sma_from_mean_motion(mean_motion_rev_day: float) -> float:
    n = mean_motion_rev_day * 2.0 * math.pi / 86400.0
    return (MU / (n * n)) ** (1.0 / 3.0)

def clamp_longitude(lon):
    while lon > 180:
        lon -= 360
    while lon < -180:
        lon += 360
    return lon

# まずは表示のための簡易ダミー。
# あとで TLE パーサ + SGP4 に差し替える。
now = datetime.now(timezone.utc)
day_offset = 0

mean_motion = 15.50
ecc = 0.00039
inc = 51.64
raan = 246.31
argp = 111.20
mean_anomaly = 320.50

a = calc_sma_from_mean_motion(mean_motion)
rp = a * (1.0 - ecc)
ra = a * (1.0 + ecc)

new_entry = {
    "epoch": now.isoformat(),
    "apogee_km": ra - EARTH_RADIUS_KM,
    "perigee_km": rp - EARTH_RADIUS_KM,
    "latitude_deg": 12.0 + math.sin(now.timestamp() / 50000.0) * 35.0,
    "longitude_deg": clamp_longitude((now.timestamp() / 2000.0) % 360.0 - 180.0),
    "semi_major_axis_km": a,
    "eccentricity": ecc,
    "inclination_deg": inc,
    "raan_deg": raan,
    "arg_perigee_deg": argp,
    "mean_anomaly_deg": mean_anomaly
}

data = {
    "norad_id": 25544,
    "name": "ISS (ZARYA)",
    "history": []
}

if os.path.exists(FILEPATH):
    with open(FILEPATH, "r", encoding="utf-8") as f:
        data = json.load(f)

history = data.get("history", [])

# 同じ epoch は追加しない
if not any(item.get("epoch") == new_entry["epoch"] for item in history):
    history.append(new_entry)

history.sort(key=lambda x: x["epoch"])

cutoff = now - timedelta(days=30)
filtered = []
for item in history:
    try:
        t = datetime.fromisoformat(item["epoch"])
        if t >= cutoff:
            filtered.append(item)
    except Exception:
        pass

data["history"] = filtered

with open(FILEPATH, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"Updated {FILEPATH} with {len(filtered)} records")
