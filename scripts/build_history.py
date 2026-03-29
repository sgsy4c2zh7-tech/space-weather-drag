import json
import math
import os
from datetime import datetime, timezone, timedelta

CATALOG_DIR = "docs/data/catalog"
HISTORY_DIR = "docs/data/sat_history"
SATELLITES_INDEX = "docs/data/satellites.json"

MU = 398600.4418          # km^3/s^2
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


def latest_snapshot_file():
    files = [
        os.path.join(CATALOG_DIR, f)
        for f in os.listdir(CATALOG_DIR)
        if f.endswith(".json")
    ]
    if not files:
        return None
    files.sort()
    return files[-1]


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
    """
    簡易表示用の緯度経度。
    本格版は TLE/SGP4 に差し替え。
    まずは全衛星履歴生成を優先する。
    """
    phase = (epoch_dt.timestamp() / 5400.0) + (norad_id % 997) * 0.01
    lat = math.sin(phase) * min(max(inclination_deg, 0.0), 85.0)

    lon_phase = (epoch_dt.timestamp() / 900.0) + norad_id * 0.1
    lon = clamp_longitude((lon_phase % 360.0) - 180.0)

    return lat, lon


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

    latitude_deg, longitude_deg = estimate_lat_lon(
        epoch_dt, int(norad_id), float(inclination)
    )

    return {
        "meta": {
            "norad_id": int(norad_id),
            "name": str(name)
        },
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
            "mean_motion_rev_day": mean_motion
        }
    }


def load_existing_history(filepath: str, norad_id: int, name: str):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "history" in data and isinstance(data["history"], list):
                return data
        except Exception:
            pass

    return {
        "norad_id": norad_id,
        "name": name,
        "history": []
    }


def trim_history(history: list, now_utc: datetime):
    cutoff = now_utc - timedelta(days=KEEP_DAYS)
    out = []
    for item in history:
        try:
            t = datetime.fromisoformat(item["epoch"].replace("Z", "+00:00"))
            if t >= cutoff:
                out.append(item)
        except Exception:
            continue
    out.sort(key=lambda x: x["epoch"])
    return out


def append_history(filepath: str, meta: dict, entry: dict):
    data = load_existing_history(filepath, meta["norad_id"], meta["name"])
    history = data.get("history", [])

    # 同じ epoch は重複追加しない
    if not any(h.get("epoch") == entry["epoch"] for h in history):
        history.append(entry)

    history = trim_history(history, datetime.now(timezone.utc))

    data["norad_id"] = meta["norad_id"]
    data["name"] = meta["name"]
    data["history"] = history

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_satellites_index(rows: list):
    sats = []
    for row in rows:
        norad_id = row.get("NORAD_CAT_ID")
        name = row.get("OBJECT_NAME") or row.get("OBJECT_ID") or f"NORAD-{norad_id}"
        if norad_id is None:
            continue
        sats.append({
            "norad_id": int(norad_id),
            "name": str(name)
        })

    sats.sort(key=lambda x: x["norad_id"])

    with open(SATELLITES_INDEX, "w", encoding="utf-8") as f:
        json.dump(sats, f, ensure_ascii=False, indent=2)


def main():
    snap = latest_snapshot_file()
    if not snap:
        print("No catalog snapshot found in docs/data/catalog/")
        return

    with open(snap, "r", encoding="utf-8") as f:
        rows = json.load(f)

    if not isinstance(rows, list):
        print("Catalog snapshot is not a list.")
        return

    updated = 0
    skipped = 0

    for row in rows:
        built = build_entry(row)
        if not built:
            skipped += 1
            continue

        out_path = os.path.join(
            HISTORY_DIR,
            f"{built['meta']['norad_id']}.json"
        )

        append_history(out_path, built["meta"], built["entry"])
        updated += 1

    write_satellites_index(rows)

    print(f"Snapshot file: {snap}")
    print(f"Updated histories: {updated}")
    print(f"Skipped rows: {skipped}")
    print(f"Wrote index: {SATELLITES_INDEX}")


if __name__ == "__main__":
    main()
