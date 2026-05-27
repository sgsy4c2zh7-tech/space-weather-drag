def load_catalog_owner_map():
    files = sorted(CATALOG_DIR.glob("active*.json"), key=lambda p: p.stat().st_mtime, reverse=True)

    if not files:
        print("warning: no catalog active*.json found")
        return {}

    for catalog_file in files:
        try:
            with open(catalog_file, "r", encoding="utf-8") as f:
                rows = json.load(f)

            owner_map = {}

            for r in rows:
                try:
                    norad = int(r.get("NORAD_CAT_ID"))
                except Exception:
                    continue

                owner = (
                    r.get("OWNER")
                    or r.get("COUNTRY")
                    or r.get("COUNTRY_CODE")
                    or r.get("OBJECT_COUNTRY")
                )

                if owner:
                    owner_map[norad] = str(owner).strip().upper()

            if owner_map:
                print(f"Using OWNER catalog: {catalog_file}")
                print(f"Loaded OWNER map: {len(owner_map)} satellites")
                return owner_map

            print(f"skip catalog without OWNER: {catalog_file}")

        except Exception as e:
            print(f"skip catalog {catalog_file}: {e}")

    print("warning: no catalog with OWNER found")
    return {}
