import json
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
from skyfield.api import EarthSatellite, load, wgs84

# ========= Config =========
KEEP_DAYS = 30
BATCH_SIZE = 100          # comma-delimited NORAD list size
SLEEP_BETWEEN_BATCHES = 3 # seconds
LOGIN_URL = "https://www.space-track.org/ajaxauth/login"
QUERY_BASE = "https://www.space-track.org/basicspacedata/query"
DATA_DIR = Path("docs/data")
SAT_INDEX_PATH = DATA_DIR / "satellites.json"
SAT_HISTORY_DIR = DATA_DIR / "sat_history"
BACKFILL_MARKER = DATA_DIR / "spacetrack_backfill_done.json"

MU = 398600.4418
EARTH_RADIUS_KM = 6378.137

SAT_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
TS = load.timescale()
# ==========================


def get_env_or_fail(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def load_satellite_ids() -> List[int]:
    if not SAT_INDEX_PATH.exists():
        raise SystemExit(f"Missing {SAT_INDEX_PATH}. Run your normal catalog build first.")
    with open(SAT_INDEX_PATH, "r", encoding="utf-8") as f:
        sats = json.load(f)
    ids = []
    for row in sats:
        norad = row.get("norad_id")
        if norad is not None:
            ids.append(int(norad))
    if not ids:
        raise SystemExit("No satellite IDs found in satellites.json")
    return ids


def chunks(seq: List[int], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def login_session(identity: str, password: str) -> requests.Session:
    """
    Best-effort session login.
    If your account/org uses a different auth flow, only this function may need adjustment.
    """
    sess = requests.Session()
    resp = sess.post(
        LOGIN_URL,
        data={"identity": identity, "password": password},
        timeout=60,
    )
    resp.raise_for_status()
    # Space-Track often returns the site home page on success.
    if "You are logged in" not in resp.text and "Space-Track.org" not in resp.text:
        # Keep this soft: many successful logins still won't contain a stable phrase.
        print("Warning: login response did not contain a success phrase; continuing.")
    return sess


def parse_epoch(epoch_str: str) -> Optional[datetime]:
    if not epoch_str:
        return None
    try:
        dt = datetime.fromisoformat(str(epoch_str).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def calc_sma_from_mean_motion(mean_motion_rev_day: float) -> float:
    n = mean_motion_rev_day * 2.0 * math.pi / 86400.0
    return (MU / (n * n)) ** (1.0 / 3.0)


def skyfield_true_lat_lon_height(omm_row: dict, epoch_dt: datetime):
    sat = EarthSatellite.from_omm(TS, omm_row)
    t = TS.from_datetime(epoch_dt)
    geocentric = sat.at(t)
    gp = wgs84.geographic_position_of(geocentric)
    return gp.latitude.degrees, gp.longitude.degrees, gp.elevation.km


def build_entry(row: dict) -> Optional[dict]:
    norad_id = row.get("NORAD_CAT_ID") or row.get("NORAD_CAT_ID".lower())
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

    try:
        latitude_deg, longitude_deg, height_km = skyfield_true_lat_lon_height(row, epoch_dt)
    except Exception as e:
        print(f"Skyfield propagation failed for {norad_id}: {e}")
        return None

    return {
        "norad_id": int(norad_id),
        "name": str(name),
        "entry": {
            "epoch": epoch_dt.isoformat().replace("+00:00", "Z"),
            "apogee_km": ra - EARTH_RADIUS_KM,
            "perigee_km": rp - EARTH_RADIUS_KM,
            "latitude_deg": latitude_deg,
            "longitude_deg": longitude_deg,
            "height_km": height_km,
            "semi_major_axis_km": semi_major_axis,
            "eccentricity": eccentricity,
            "inclination_deg": inclination,
            "raan_deg": raan,
            "arg_perigee_deg": arg_perigee,
            "mean_anomaly_deg": mean_anomaly,
            "mean_motion_rev_day": mean_motion,
        },
    }


def query_gp_history_batch(sess: requests.Session, ids: List[int]) -> List[dict]:
    id_str = ",".join(str(x) for x in ids)
    # Space-Track docs give the GP_History 30-day example with CREATION_DATE/>now-30
    # and recommend GP_History for historical ephemerides, storing results locally.
    url = (
        f"{QUERY_BASE}/class/gp_history/"
        f"norad_cat_id/{id_str}/"
        f"CREATION_DATE/%3Enow-30/"
        f"orderby/NORAD_CAT_ID asc,CREATION_DATE asc/"
        f"format/json"
    )
    resp = sess.get(url, timeout=180)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError("Unexpected GP_History response type")
    return data


def rebuild_histories(rows: List[dict]) -> Dict[int, dict]:
    by_sat = defaultdict(lambda: {"name": None, "history_by_epoch": {}})

    for row in rows:
        built = build_entry(row)
        if not built:
            continue
        norad_id = built["norad_id"]
        by_sat[norad_id]["name"] = built["name"]
        by_sat[norad_id]["history_by_epoch"][built["entry"]["epoch"]] = built["entry"]

    out = {}
    for norad_id, payload in by_sat.items():
        history = list(payload["history_by_epoch"].values())
        history.sort(key=lambda x: x["epoch"])
        if history:
            out[norad_id] = {
                "norad_id": norad_id,
                "name": payload["name"] or f"NORAD-{norad_id}",
                "history": history,
            }
    return out


def write_histories(histories: Dict[int, dict]) -> int:
    written = 0
    for norad_id, payload in histories.items():
        out_path = SAT_HISTORY_DIR / f"{norad_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        written += 1
    return written


def write_marker(total_ids: int, written: int):
    payload = {
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "source": "Space-Track GP_History initial backfill",
        "keep_days": KEEP_DAYS,
        "total_ids_requested": total_ids,
        "sat_history_files_written": written,
    }
    with open(BACKFILL_MARKER, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main():
    identity = get_env_or_fail("SPACETRACK_ID")
    password = get_env_or_fail("SPACETRACK_PASSWORD")

    norad_ids = load_satellite_ids()
    print(f"Loaded {len(norad_ids)} NORAD IDs from satellites.json")

    sess = login_session(identity, password)

    all_rows: List[dict] = []
    for i, batch in enumerate(chunks(norad_ids, BATCH_SIZE), start=1):
        print(f"Fetching batch {i}: {len(batch)} satellites")
        rows = query_gp_history_batch(sess, batch)
        print(f"  returned rows: {len(rows)}")
        all_rows.extend(rows)
        time.sleep(SLEEP_BETWEEN_BATCHES)

    print(f"Total GP_History rows fetched: {len(all_rows)}")
    histories = rebuild_histories(all_rows)
    written = write_histories(histories)
    write_marker(len(norad_ids), written)

    print(f"sat_history files written: {written}")
    print(f"marker written: {BACKFILL_MARKER}")


if __name__ == "__main__":
    main()
