import json
import math
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta

CATALOG_DIR = "docs/data/catalog"
HISTORY_DIR = "docs/data/sat_history"
SATELLITES_INDEX = "docs/data/satellites.json"

MU = 398600.4418
EARTH_RADIUS_KM = 6378.137
KEEP_DAYS = 30

os.makedirs(HISTORY_DIR, exist_ok=True)


def clamp_longitude(lon_deg: float) -> float:
    while lon_deg > 180:
        lon_deg -= 360
    while lon_deg < -180:
        lon_deg += 360
    return lon_deg


def calc_sma_from_mean_motion(mean_motion_rev_day: float) -> float:
    n = mean_motion_rev_day * 2.0 * math.pi / 86400.0
    return (MU / (n * n)) ** (1.0 / 3.0)


def parse_epoch(epoch_str: str):
    if not epoch_str:
        return None
    try:
        return datetime.fromisoformat(str(epoch_str).replace("Z", "+00:00"))
    except Exception:
        return None


def safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def estimate_lat_lon(epoch_dt: datetime, norad_id: int, inclination_deg: float):
    phase = (epoch_dt.timestamp() / 5400.0) + (norad_id % 997) * 0.01
    lat = math.sin(phase) * min(max(inclination_deg, 0.0), 85.0)

    lon_phase = (epoch_dt.timestamp() / 900.0) + norad_id * 0.1
    lon = clamp_longitude((lon_phase % 360.0) - 180.0)
    return lat, lon


def snapshot_files_in_range():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=KEEP_DAYS)
    files = []
    for name in os.listdir(CATALOG_DIR):
        if not name.endswith(".json"):
            continue
        path = os.path.join(CATALOG_DIR, name)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)
            if mtime >= cutoff:
                files.append(path)
        except Exception:
            continue
    files.sort()
    return files


def build_entry(row: dict):
    norad_id = row.get("NORAD_CAT_ID")
    name = row.get("OBJECT_NAME") or row.get("OBJECT_ID") or f"NORAD-{norad_id}"
    epoch_str = row.get("EPOCH")

    if norad_id is None or epoch_str is None:
        return None

    epoch_dt = parse_epoch(epoch_str)
    if epoch_dt is None:
        return None

    mean_motion = safe_float(row.get("MEAN_MOTION"))
    eccentricity = safe_float(row.get("ECCENTRICITY"), 0.0)
    inclination = safe_float(row.get("INCLINATION"), 0.0)
    raan = safe_float(row.get("RA_OF_ASC_NODE"), 0.0)
    arg_perigee = safe_float(row.get("ARG_OF_PERICENTER"), 0.0)
    mean_anomaly = safe_float(row.get("MEAN_ANOMALY"), 0.0)

    if mean_motion is None or mean_motion <= 0:
        return None

    semi_major_axis = calc_sma_from_mean_motion(mean_motion)
    rp = semi_major_axis * (1.0 - eccentricity)
    ra = semi_major_axis * (1.0 + eccentricity)

    apogee_km = ra - EARTH_RADIUS_KM
    perigee_km = rp - EARTH_RADIUS_KM

    latitude_deg, longitude_deg = estimate_lat_lon(epoch_dt, int(norad_id), float(inclination))

    return {
        "norad_id": int(norad_id),
        "name": str(name),
        "entry": {
            "epoch": epoch_dt.isoformat().replace("+00:00", "Z"),
            "apogee_km": apogee_km,
            "perigee_km": perigee_km,
            "latitude_deg": latitude_deg,
            "longitude_deg": longitude_deg,
            "semi_major_axis_km": semi_major_axis,
            "eccentricity": eccentricity,
            "inclination_deg": inclination,
            "raan_deg": raan,
            "arg_perigee_deg": arg_perigee,
            "mean_anomaly_deg": mean_anomaly,
            "mean_motion_rev_day": mean_motion,
        },
    }


def rebuild_histories_from_snapshots(files):
    by_sat = defaultdict(lambda: {"name": None, "history_by_epoch": {}})
    total_rows = 0
    valid_rows = 0

    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            rows = json.load(f)

        if not isinstance(rows, list):
            continue

        total_rows += len(rows)

        for row in rows:
            built = build_entry(row)
            if not built:
                continue

            valid_rows += 1
            norad_id = built["norad_id"]
            name = built["name"]
            entry = built["entry"]

            by_sat[norad_id]["name"] = name
            by_sat[norad_id]["history_by_epoch"][entry["epoch"]] = entry

    return by_sat, total_rows, valid_rows


def write_sat_history_files(by_sat):
    for name in os.listdir(HISTORY_DIR):
        if name.endswith(".json"):
            os.remove(os.path.join(HISTORY_DIR, name))

    written = 0

    for norad_id, payload in by_sat.items():
        history = list(payload["history_by_epoch"].values())
        history.sort(key=lambda x: x["epoch"])

        if not history:
            continue

        data = {
            "norad_id": norad_id,
            "name": payload["name"] or f"NORAD-{norad_id}",
            "history": history,
        }

        out_path = os.path.join(HISTORY_DIR, f"{norad_id}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        written += 1

    return written


def write_satellites_index(by_sat):
    sats = []
    for norad_id, payload in by_sat.items():
        sats.append({
            "norad_id": int(norad_id),
            "name": payload["name"] or f"NORAD-{norad_id}",
        })

    sats.sort(key=lambda x: x["norad_id"])

    with open(SATELLITES_INDEX, "w", encoding="utf-8") as f:
        json.dump(sats, f, ensure_ascii=False, indent=2)


def main():
    if not os.path.isdir(CATALOG_DIR):
        raise SystemExit("docs/data/catalog does not exist")

    files = snapshot_files_in_range()
    if not files:
        raise SystemExit("No catalog snapshots found in the last 30 days.")

    by_sat, total_rows, valid_rows = rebuild_histories_from_snapshots(files)
    written = write_sat_history_files(by_sat)
    write_satellites_index(by_sat)

    print(f"Catalog snapshots used: {len(files)}")
    print(f"Total input rows: {total_rows}")
    print(f"Valid rows converted: {valid_rows}")
    print(f"sat_history files written: {written}")

    if written < 100:
        raise SystemExit(f"Too few sat_history files written: {written}")


if __name__ == "__main__":
    main()
