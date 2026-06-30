import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

INDEX_FILE = ROOT / "docs/data/satellite_index_full.json"
SATCAT_FILE = ROOT / "docs/data/catalog/satcat_full.json"


def load_json(path, default):
    if not path.exists():
        return default

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path, data):
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def main():
    index = load_json(INDEX_FILE, [])
    satcat = load_json(SATCAT_FILE, {})

    objects = satcat.get("objects", [])
    satcat_map = {
        int(x["norad_id"]): x
        for x in objects
        if x.get("norad_id") is not None
    }

    merged = []

    for s in index:
        norad = s.get("norad_id") or s.get("NORAD_CAT_ID")

        try:
          norad_int = int(norad)
        except Exception:
          merged.append(s)
          continue

        meta = satcat_map.get(norad_int)

        if meta:
            s["object_name"] = meta.get("object_name")
            s["object_id"] = meta.get("object_id")
            s["object_type"] = meta.get("object_type")
            s["country"] = meta.get("country")
            s["raw_country"] = meta.get("raw_country")
            s["launch_date"] = meta.get("launch_date")
            s["decay_date"] = meta.get("decay_date")
            s["site"] = meta.get("site")
            s["rcs"] = meta.get("rcs")

            if not s.get("name") and meta.get("object_name"):
                s["name"] = meta.get("object_name")

        merged.append(s)

    save_json(INDEX_FILE, merged)

    print(f"Merged SATCAT into {INDEX_FILE}")
    print(f"Index rows: {len(merged)}")
    print(f"SATCAT rows: {len(objects)}")


if __name__ == "__main__":
    main()