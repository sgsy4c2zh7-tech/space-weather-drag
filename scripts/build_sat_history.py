import json
import math
import os
from datetime import datetime, timezone, timedelta
from sgp4.api import Satrec, jday

CATALOG_DIR = "docs/data/catalog"
HISTORY_DIR = "docs/data/sat_history"
os.makedirs(HISTORY_DIR, exist_ok=True)

MU = 398600.4418  # km^3/s^2
EARTH_RADIUS_KM = 6378.137

def clamp_longitude(lon_deg: float) -> float:
    while lon_deg > 180:
        lon_deg -= 360
    while lon_deg < -180:
        lon_deg += 360
    return lon_deg

def gmst_rad(dt: datetime) -> float:
    jd, fr = jday(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second + dt.microsecond / 1e6)
    t = ((jd + fr) - 2451545.0) / 36525.0
    gmst_deg = (
        280.46061837
        + 360.98564736629 * ((jd + fr) - 2451545.0)
        + 0.000387933 * t * t
        - (t * t * t) / 38710000.0
    )
    return math.radians(gmst_deg % 360.0)

def eci_to_latlon_km(r_eci, dt: datetime):
    x, y, z = r_eci
    theta = gmst_rad(dt)

    x_ecef = x * math.cos(theta) + y * math.sin(theta)
    y_ecef = -x * math.sin(theta) + y * math.cos(theta)
    z_ecef = z

    lon = math.degrees(math.atan2(y_ecef, x_ecef))
    r = math.sqrt(x_ecef * x_ecef + y_ecef * y_ecef)
    lat = math.degrees(math.atan2(z_ecef, r))

    return lat, clamp_longitude(lon)

def calc_sma_from_mean_motion(mean_motion_rev_day: float) -> float:
    n = mean_motion_rev_day * 2.0 * math.pi / 86400.0
    return (MU / (n * n)) ** (1.0 / 3.0)

def latest_snapshot_files():
    files = [
        os.path.join(CATALOG_DIR, f)
        for f in os.listdir(CATALOG_DIR)
        if f.endswith(".json")
    ]
    return sorted(files)

def append_history(filepath: str, meta: dict, entry: dict):
    data = {
        "norad_id": meta["norad_id"],
        "name": meta["name"],
        "history": []
    }

    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

    history = data.get("history", [])

    # epoch 重複防止
    if not any(item.get("epoch") == entry["epoch"] for item in history):
        history.append(entry)

    history.sort(key=lambda x: x["epoch"])

    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    filtered = []
    for item in history:
        try:
            t = datetime.fromisoformat(item["epoch"].replace("Z", "+00:00"))
            if t >= cutoff:
                filtered.append(item)
        except Exception:
            pass

    data["history"] = filtered

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def build_entry(row: dict):
    norad_id = int(row["NORAD_CAT_ID"])
    name = row.get("OBJECT_NAME") or f"NORAD-{norad_id}"

    epoch_str = row.get("EPOCH")
    if not epoch_str:
        return None

    epoch_dt = datetime.fromisoformat(epoch_str.replace("Z", "+00:00"))

    line1 = row.get("TLE_LINE1")
    line2 = row.get("TLE_LINE2")

    if not line1 or not line2:
        return None

    sat = Satrec.twoline2rv(line1, line2)
    jd, fr = jday(epoch_dt.year, epoch_dt.month, epoch_dt.day, epoch_dt.hour, epoch_dt.minute, epoch_dt.second + epoch_dt.microsecond / 1e6)
    e, r, v = sat.sgp4(jd, fr)
    if e != 0:
        return None

    mean_motion = float(row.get("MEAN_MOTION", 0.0))
    ecc = float(row.get("ECCENTRICITY", 0.0))
    inc = float(row.get("INCLINATION", 0.0))
    raan = float(row.get("RA_OF_ASC_NODE", 0.0))
    argp = float(row.get("ARG_OF_PERICENTER", 0.0))
    mean_anomaly = float(row.get("MEAN_ANOMALY", 0.0))

    a = calc_sma_from_mean_motion(mean_motion)
    rp = a * (1.0 - ecc)
    ra = a * (1.0 + ecc)
    lat, lon = eci_to_latlon_km(r, epoch_dt)

    return {
        "meta": {
            "norad_id": norad_id,
            "name": name
        },
        "entry": {
            "epoch": epoch_dt.isoformat().replace("+00:00", "Z"),
            "apogee_km": ra - EARTH_RADIUS_KM,
            "perigee_km": rp - EARTH_RADIUS_KM,
            "latitude_deg": lat,
            "longitude_deg": lon,
            "semi_major_axis_km": a,
            "eccentricity": ecc,
            "inclination_deg": inc,
            "raan_deg": raan,
            "arg_perigee_deg": argp,
            "mean_anomaly_deg": mean_anomaly,
            "mean_motion_rev_day": mean_motion
        }
    }

def main():
    files = latest_snapshot_files()
    if not files:
        print("No catalog snapshots found.")
        return

    latest = files[-1]
    with open(latest, "r", encoding="utf-8") as f:
        rows = json.load(f)

    ok = 0
    for row in rows:
        try:
            built = build_entry(row)
            if not built:
                continue
            norad_id = built["meta"]["norad_id"]
            out_path = os.path.join(HISTORY_DIR, f"{norad_id}.json")
            append_history(out_path, built["meta"], built["entry"])
            ok += 1
        except Exception as e:
            # 個別失敗は止めずに継続
            print(f"Skip row due to error: {e}")

    print(f"Updated histories: {ok}")

if __name__ == "__main__":
    main()
