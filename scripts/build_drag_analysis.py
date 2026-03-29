import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import pymsis
except Exception:
    pymsis = None

DATA_DIR = Path("docs/data")
SAT_HISTORY_DIR = DATA_DIR / "sat_history"
NOAA_DIR = DATA_DIR / "noaa"
ANALYSIS_DIR = DATA_DIR / "analysis"
ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

WATCHLIST = [25544, 44713, 20580, 33591, 39084]


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_dt(s: str):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def nearest_kp_value(dt, kp_series):
    best = None
    best_abs = None
    for item in kp_series:
        t = parse_dt(item["time_tag"])
        diff = abs((t - dt).total_seconds())
        if best_abs is None or diff < best_abs:
            best_abs = diff
            best = float(item["kp_index"])
    return best


def estimate_density(lat_deg, lon_deg, alt_km, dt, kp_value):
    if pymsis is None:
        return None
    try:
        f107a = 150.0
        f107 = 150.0
        ap = max(0.0, kp_value * 8.0)
        out = pymsis.calculate(
            dates=np.array([np.datetime64(dt)]),
            lons=np.array([lon_deg]),
            lats=np.array([lat_deg]),
            alts=np.array([alt_km]),
            f107s=np.array([f107]),
            f107as=np.array([f107a]),
            aps=np.array([[ap] * 7]),
        )
        density = float(np.ravel(out)[0])
        return density
    except Exception:
        return None


def moving_delta_height(history):
    deltas = []
    for i in range(1, len(history)):
        prev = history[i - 1]
        cur = history[i]
        dt_prev = parse_dt(prev["epoch"])
        dt_cur = parse_dt(cur["epoch"])
        hours = (dt_cur - dt_prev).total_seconds() / 3600.0
        if hours <= 0:
            continue
        prev_h = (float(prev["apogee_km"]) + float(prev["perigee_km"])) / 2.0
        cur_h = (float(cur["apogee_km"]) + float(cur["perigee_km"])) / 2.0
        deltas.append({
            "time": cur["epoch"],
            "delta_h_km": cur_h - prev_h,
            "hours": hours,
            "rate_km_per_day": (cur_h - prev_h) / hours * 24.0,
        })
    return deltas


def storm_windows(history, kp_series, threshold=5.0):
    events = []
    deltas = moving_delta_height(history)
    for d in deltas:
        dt = parse_dt(d["time"])
        kp = nearest_kp_value(dt, kp_series)
        if kp is None or kp < threshold:
            continue
        events.append({
            "time": d["time"],
            "kp": kp,
            "delta_h_km": d["delta_h_km"],
            "rate_km_per_day": d["rate_km_per_day"],
        })
    return events


def correlation_points(history, kp_series):
    points = []
    deltas = moving_delta_height(history)
    for d in deltas:
        dt = parse_dt(d["time"])
        kp = nearest_kp_value(dt, kp_series)
        if kp is None:
            continue
        points.append({
            "time": d["time"],
            "kp": kp,
            "delta_h_km": d["delta_h_km"],
            "rate_km_per_day": d["rate_km_per_day"],
        })
    return points


def enrich_with_density(history, kp_series):
    enriched = []
    for row in history:
        dt = parse_dt(row["epoch"])
        alt = (float(row["apogee_km"]) + float(row["perigee_km"])) / 2.0
        lat = float(row.get("latitude_deg", 0.0))
        lon = float(row.get("longitude_deg", 0.0))
        kp = nearest_kp_value(dt, kp_series)
        density = estimate_density(lat, lon, alt, dt, kp if kp is not None else 2.0)
        enriched.append({
            **row,
            "avg_altitude_km": alt,
            "kp_nearest": kp,
            "nrlmsis_density": density,
        })
    return enriched


def pearson_xy(points, key_x, key_y):
    xs = []
    ys = []
    for p in points:
        x = p.get(key_x)
        y = p.get(key_y)
        if x is None or y is None:
            continue
        xs.append(float(x))
        ys.append(float(y))
    if len(xs) < 2:
        return None
    x = np.array(xs)
    y = np.array(ys)
    if np.std(x) == 0 or np.std(y) == 0:
        return None
    return float(np.corrcoef(x, y)[0, 1])


def main():
    kp_obs = load_json(NOAA_DIR / "kp_observed.json").get("data", [])
    kp_fcst = load_json(NOAA_DIR / "kp_forecast.json").get("data", [])
    kp_all = sorted(kp_obs + kp_fcst, key=lambda x: x["time_tag"])

    for norad in WATCHLIST:
        path = SAT_HISTORY_DIR / f"{norad}.json"
        if not path.exists():
            continue
        sat = load_json(path)
        history = sat.get("history", [])
        if len(history) < 2:
            continue

        enriched = enrich_with_density(history, kp_all)
        storms = storm_windows(enriched, kp_all, threshold=5.0)
        corr = correlation_points(enriched, kp_all)

        payload = {
            "norad_id": sat.get("norad_id"),
            "name": sat.get("name"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "history_enriched": enriched,
            "storm_events": storms,
            "correlation_points": corr,
            "pearson": {
                "kp_vs_delta_h": pearson_xy(corr, "kp", "delta_h_km"),
                "kp_vs_rate": pearson_xy(corr, "kp", "rate_km_per_day"),
            },
        }

        with open(ANALYSIS_DIR / f"{norad}.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        print(f"Wrote analysis for {norad}")


if __name__ == "__main__":
    main()
