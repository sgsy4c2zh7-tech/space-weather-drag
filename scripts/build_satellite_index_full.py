import json
from pathlib import Path

SAT_HISTORY_DIR = Path("docs/data/sat_history")
OUT_FILE = Path("docs/data/satellite_index_full.json")


def main():
    rows = []

    if not SAT_HISTORY_DIR.exists():
        raise SystemExit("docs/data/sat_history does not exist")

    for path in SAT_HISTORY_DIR.glob("*.json"):
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)

            history = obj.get("history", [])
            if not history:
                continue

            latest = history[-1]

            rows.append({
                "norad_id": obj.get("norad_id"),
                "name": obj.get("name", f"NORAD-{obj.get('norad_id')}"),
                "epoch": latest.get("epoch"),
                "apogee_km": latest.get("apogee_km"),
                "perigee_km": latest.get("perigee_km"),
                "height_km": latest.get("height_km"),
                "inclination_deg": latest.get("inclination_deg"),
                "latitude_deg": latest.get("latitude_deg"),
                "longitude_deg": latest.get("longitude_deg"),
                "semi_major_axis_km": latest.get("semi_major_axis_km"),
                "eccentricity": latest.get("eccentricity"),
                "raan_deg": latest.get("raan_deg"),
                "arg_perigee_deg": latest.get("arg_perigee_deg"),
                "mean_anomaly_deg": latest.get("mean_anomaly_deg"),
                "mean_motion_rev_day": latest.get("mean_motion_rev_day"),
                "history_count": len(history)
            })
        except Exception as e:
            print(f"skip {path.name}: {e}")

    rows.sort(key=lambda x: (str(x.get("name", "")), x.get("norad_id", 0)))

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUT_FILE} with {len(rows)} satellites")


if __name__ == "__main__":
    main()
