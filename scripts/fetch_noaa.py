import json
import os
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request

OUT_DIR = "docs/data/noaa"
os.makedirs(OUT_DIR, exist_ok=True)

SWPC_KP_OBS = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
SWPC_KP_FCST = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index-forecast.json"

# AE は NOAA/NCEI の CSV/テキストを中継変換する想定。
# ここではまずアプリが壊れないように、取得失敗時も空JSONを書く。
# 後で NCEI の使いたい AE 取得元に差し替えやすい形にしてある。
AE_FALLBACK_URL = None

def fetch_json(url: str):
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

def write_json(path: str, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def normalize_kp_observed(raw):
    # よくある形式:
    # [
    #   ["time_tag","kp_index"],
    #   ["2026-03-29 00:00:00.000","2.67"],
    #   ...
    # ]
    if isinstance(raw, list) and raw and isinstance(raw[0], list):
      header = raw[0]
      rows = raw[1:]
      out = []
      for row in rows:
          if len(row) >= 2:
              out.append({
                  "time_tag": str(row[0]).replace(" ", "T") + "Z" if "T" not in str(row[0]) else str(row[0]),
                  "kp_index": float(row[1])
              })
      return out

    if isinstance(raw, list) and raw and isinstance(raw[0], dict):
        out = []
        for item in raw:
            t = item.get("time_tag") or item.get("timeTag")
            v = item.get("kp_index") or item.get("kp") or item.get("k_index")
            if t is not None and v is not None:
                t = str(t).replace(" ", "T")
                if not t.endswith("Z") and "+" not in t:
                    t += "Z"
                out.append({"time_tag": t, "kp_index": float(v)})
        return out

    return []

def normalize_kp_forecast(raw):
    # 形式変化に備えて observed と同じように吸う
    return normalize_kp_observed(raw)

def write_empty_ae():
    write_json(os.path.join(OUT_DIR, "ae_observed.json"), {
        "source": "NOAA/NCEI",
        "note": "AE fetch placeholder. Replace source parser when AE endpoint is finalized.",
        "data": []
    })

def main():
    kp_obs_raw = fetch_json(SWPC_KP_OBS)
    kp_fcst_raw = fetch_json(SWPC_KP_FCST)

    kp_obs = normalize_kp_observed(kp_obs_raw)
    kp_fcst = normalize_kp_forecast(kp_fcst_raw)

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

    # AE はまず壊れない最低限版
    write_empty_ae()

    print(f"kp_observed: {len(kp_obs)} rows")
    print(f"kp_forecast: {len(kp_fcst)} rows")
    print("ae_observed: placeholder written")

if __name__ == "__main__":
    main()
