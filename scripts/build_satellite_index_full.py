import json
from pathlib import Path

SAT_HISTORY_DIR = Path("docs/data/sat_history")
CATALOG_DIR = Path("docs/data/catalog")
OUT_FILE = Path("docs/data/satellite_index_full.json")


def load_catalog_owner_map():
    files = sorted(CATALOG_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)

    print("===== catalog files =====")
    for f in files[:20]:
        print(f)

    owner_map = {}

    for catalog_file in files:
        try:
            with open(catalog_file, "r", encoding="utf-8") as f:
                rows = json.load(f)

            if not isinstance(rows, list) or not rows:
                print(f"skip non-list/empty: {catalog_file}")
                continue

            print(f"\nChecking catalog: {catalog_file}")
            print(f"rows: {len(rows)}")
            print(f"sample keys: {list(rows[0].keys())}")

            local_map = {}

            for r in rows:
                norad_raw = (
                    r.get("NORAD_CAT_ID")
                    or r.get("norad_id")
                    or r.get("NORAD")
                    or r.get("CATNR")
                )

                try:
                    norad = int(norad_raw)
                except Exception:
                    continue

                owner = (
                    r.get("OWNER")
                    or r.get("Owner")
                    or r.get("owner")
                    or r.get("COUNTRY")
                    or r.get("Country")
                    or r.get("country")
                    or r.get("COUNTRY_CODE")
                    or r.get("country_code")
                    or r.get("OBJECT_COUNTRY")
                    or r.get("OBJECT_OWNER")
                    or r.get("LAUNCHING_STATE")
                )

                if owner:
                    local_map[norad] = str(owner).strip().upper()

            print(f"owner found in this file: {len(local_map)}")

            if local_map:
                owner_map.update(local_map)
                print(f"using owner data from: {catalog_file}")
                break

        except Exception as e:
            print(f"skip catalog {catalog_file}: {e}")

    print(f"TOTAL owner_map: {len(owner_map)}")
    return owner_map


def main():
    rows = []
    owner_map = load_catalog_owner_map()

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

            owner = owner_map.get(norad_int, "UNK")

            rows.append({
                "norad_id": norad_int,
                "name": obj.get("name", f"NORAD-{norad}"),

                "owner": owner,
                "country_code": owner,

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

    with_owner = sum(1 for x in rows if x.get("owner") != "UNK")

    print(f"Wrote {OUT_FILE} with {len(rows)} satellites")
    print(f"With owner: {with_owner}")
    print(f"Without owner: {len(rows) - with_owner}")

    print("===== sample output =====")
    for r in rows[:10]:
        print(r.get("norad_id"), r.get("name"), r.get("owner"))


if __name__ == "__main__":
    main()
