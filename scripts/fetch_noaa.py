import json
import os
from datetime import datetime, timezone
from urllib.request import Request, urlopen

OUT_DIR = "docs/data/noaa"
os.makedirs(OUT_DIR, exist_ok=True)

SWPC_KP_OBS = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
SWPC_KP_FCST = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index-forecast.json"

def fetch_json(url: str):
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))

def write_json(path: str, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def normalize_kp(raw):
    out = []
    if isinstance(raw, list) and raw and isinstance(raw[0], list):
        for row in raw[1:]:
            if len(row) >= 2:
                t = str(row[0]).replace(" ", "T")
                if not t.endswith("Z") and "+" not in t:
                    t += "Z"
                out.append({"time_tag": t, "kp_index": float(row[1])})
    elif isinstance(raw, list) and raw and isinstance(raw[0], dict):
        for item in raw:
            t = item.get("time_tag") or item.get("timeTag")
            v = item.get("kp_index") or item.get("kp")
            if t is not None and v is not None:
                t = str(t).replace(" ", "T")
                if not t.endswith("Z") and "+" not in t:
                    t += "Z"
                out.append({"time_tag": t, "kp_index": float(v)})
    return out

def main():
    kp_obs = normalize_kp(fetch_json(SWPC_KP_OBS))
    kp_fcst = normalize_kp(fetch_json(SWPC_KP_FCST))

    write_json(os.path.join(OUT_DIR, "kp_observed.json"), {
        "source": "NOAA/SWPC",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data": kp_obs
    })

    write_json(os.path.join(OUT_DIR, "kp_forecast.json"), {
        "source": "NOAA/SWPC",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data": kp_fcst
    })

    # AE はまず空で置く。後で NOAA/NCEI 実取得に差し替え
    write_json(os.path.join(OUT_DIR, "ae_observed.json"), {
        "source": "NOAA/NCEI",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data": []
    })

    print(f"kp_observed: {len(kp_obs)}")
    print(f"kp_forecast: {len(kp_fcst)}")

if __name__ == "__main__":
    main()
