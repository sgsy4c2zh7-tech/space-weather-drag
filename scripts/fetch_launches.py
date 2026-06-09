import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
from urllib.request import Request, urlopen

OUT_DIR = Path("docs/data/launches")
OUT_FILE = OUT_DIR / "weekly_launches.json"

BASE_URL = "https://ll.thespacedevs.com/2.2.0/launch/upcoming/"

LOOKAHEAD_DAYS = 14

COUNTRY_GROUPS = {
    "JPN": ["JPN", "JAPAN", "JAXA", "TANEGASHIMA", "UCHINOURA", "MHI"],
    "US": ["USA", "US", "UNITED STATES", "SPACEX", "ULA", "NASA", "VANDENBERG", "CAPE CANAVERAL", "KENNEDY"],
    "CHN": ["CHN", "CHINA", "PRC", "CASC", "JIUQUAN", "XICHANG", "WENCHANG", "TAIYUAN"],
    "RUS": ["RUS", "RUSSIA", "ROSCOSMOS", "BAIKONUR", "PLESETSK", "VOSTOCHNY"],
}


def fetch_json(url):
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def detect_country_group(item):
    parts = [
        item.get("name"),
        item.get("mission", {}).get("name") if item.get("mission") else None,
        item.get("launch_service_provider", {}).get("name") if item.get("launch_service_provider") else None,
        item.get("pad", {}).get("name") if item.get("pad") else None,
        item.get("pad", {}).get("location", {}).get("name") if item.get("pad") and item.get("pad", {}).get("location") else None,
        item.get("pad", {}).get("location", {}).get("country_code") if item.get("pad") and item.get("pad", {}).get("location") else None,
    ]

    text = " ".join(str(x) for x in parts if x).upper()

    for group, keys in COUNTRY_GROUPS.items():
        if any(k in text for k in keys):
            return group

    return None


def simplify_launch(item):
    pad = item.get("pad") or {}
    loc = pad.get("location") or {}
    provider = item.get("launch_service_provider") or {}
    mission = item.get("mission") or {}

    group = detect_country_group(item)

    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "net": item.get("net"),
        "status": (item.get("status") or {}).get("name"),
        "country_group": group,
        "provider": provider.get("name"),
        "mission": mission.get("name"),
        "mission_type": mission.get("type"),
        "pad": pad.get("name"),
        "location": loc.get("name"),
        "location_country_code": loc.get("country_code"),
        "url": item.get("url"),
        "image": item.get("image"),
    }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    window_end = now + timedelta(days=LOOKAHEAD_DAYS)

    params = {
        "limit": 100,
        "net__gte": now.isoformat().replace("+00:00", "Z"),
        "net__lte": window_end.isoformat().replace("+00:00", "Z"),
        "ordering": "net",
    }

    url = BASE_URL + "?" + urlencode(params)
    data = fetch_json(url)

    results = data.get("results", [])
    simplified = []

    for item in results:
        row = simplify_launch(item)
        if row["country_group"] in ["JPN", "US", "CHN", "RUS"]:
            simplified.append(row)

    out = {
        "updated_at": now.isoformat().replace("+00:00", "Z"),
        "source": "Launch Library 2 / The Space Devs",
        "source_url": url,
        "window_days": LOOKAHEAD_DAYS,
        "window_start": now.isoformat().replace("+00:00", "Z"),
        "window_end": window_end.isoformat().replace("+00:00", "Z"),
        "count": len(simplified),
        "launches": simplified,
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUT_FILE}")
    print(f"Window days: {LOOKAHEAD_DAYS}")
    print(f"Launches: {len(simplified)}")


if __name__ == "__main__":
    main()
