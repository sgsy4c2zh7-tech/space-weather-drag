import json
from pathlib import Path

SAT_HISTORY_DIR = Path("docs/data/sat_history")
SATELLITES_FILE = Path("docs/data/satellites.json")
OUT_FILE = Path("docs/data/satellite_index_full.json")


def load_satellite_meta():
    """
    docs/data/satellites.json から
    NORAD ID -> country_code 等のメタ情報を作る
    """
    if not SATELLITES_FILE.exists():
        print(f"warning: {SATELLITES_FILE} does not exist. country_code will be UNK.")
        return {}

    try:
        with open(SATELLITES_FILE, "r", encoding="utf-8") as f:
            rows = json.load(f)

        meta = {}
        for r in rows:
            norad = r.get("norad_id")
            if norad is None:
                continue

            try:
                norad = int(norad)
            except Exception:
                continue

            meta[norad] = {
                "country_code": r.get("country_code", "UNK"),
                "object_type": r.get("object_type"),
                "launch_site": r.get("launch_site"),
                "launch_date": r.get("launch_date"),
                "decay_date": r.get("decay_date"),
            }

        return meta

    except Exception as e:
        print(f"warning: failed to load {SATELLITES_FILE}: {e}")
        return {}


def main():
    rows = []
    meta_map = load_satellite_meta()

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
            norad = obj.get("norad_id")

            try:
                norad_int = int(norad)
            except Exception:
                norad_int = norad

            meta = meta_map.get(norad_int, {})

            rows.append({
                "norad_id": norad_int,
                "name": obj.get("name", f"NORAD-{norad}"),

                "country_code": meta.get("country_code", "UNK"),
                "object_type": meta.get("object_type"),
                "launch_site": meta.get("launch_site"),
                "launch_date": meta.get("launch_date"),
                "decay_date": meta.get("decay_date"),

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

    with_country = sum(1 for x in rows if x.get("country_code") != "UNK")

    print(f"Wrote {OUT_FILE} with {len(rows)} satellites")
    print(f"With country_code: {with_country}")
    print(f"Without country_code: {len(rows) - with_country}")


if __name__ == "__main__":
    main()
