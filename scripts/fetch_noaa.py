import json
import os
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen

OUT_DIR = "docs/data/noaa"
os.makedirs(OUT_DIR, exist_ok=True)

SWPC_KP_OBS = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
SWPC_KP_FCST = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index-forecast.json"
SWPC_DST = "https://services.swpc.noaa.gov/products/kyoto-dst.json"
SWPC_EST_KP_1M = "https://services.swpc.noaa.gov/products/noaa-estimated-planetary-k-index-1-minute.json"

KP_HISTORY_FILE = os.path.join(OUT_DIR, "kp_history.json")
KEEP_DAYS = 30


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
                try:
                    v = float(row[1])
                except Exception:
                    continue
                out.append({"time_tag": t, "kp_index": v})
    elif isinstance(raw, list) and raw and isinstance(raw[0], dict):
        for item in raw:
            t = item.get("time_tag") or item.get("timeTag")
            v = item.get("kp_index") or item.get("kp")
            if t is not None and v is not None:
                t = str(t).replace(" ", "T")
                if not t.endswith("Z") and "+" not in t:
                    t += "Z"
                try:
                    v = float(v)
                except Exception:
                    continue
                out.append({"time_tag": t, "kp_index": v})
    return out


def normalize_dst(raw):
    out = []
    if isinstance(raw, list) and raw and isinstance(raw[0], list):
        for row in raw[1:]:
            if len(row) >= 2:
                t = str(row[0]).replace(" ", "T")
                if not t.endswith("Z") and "+" not in t:
                    t += "Z"
                try:
                    v = float(row[1])
                except Exception:
                    continue
                out.append({"time_tag": t, "dst": v})
    elif isinstance(raw, list) and raw and isinstance(raw[0], dict):
        for item in raw:
            t = item.get("time_tag") or item.get("timeTag")
            v = item.get("dst")
            if t is None or v is None:
                continue
            t = str(t).replace(" ", "T")
            if not t.endswith("Z") and "+" not in t:
                t += "Z"
            try:
                v = float(v)
            except Exception:
                continue
            out.append({"time_tag": t, "dst": v})
    return out


def parse_dt(s: str):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def load_existing_history():
    if not os.path.exists(KP_HISTORY_FILE):
        return {"updated_at": None, "data": []}
    try:
        with open(KP_HISTORY_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, dict) and isinstance(obj.get("data"), list):
            return obj
    except Exception:
        pass
    return {"updated_at": None, "data": []}


def merge_kp_history(existing_rows, new_rows):
    by_time = {}

    for row in existing_rows:
        t = row.get("time_tag")
        v = row.get("kp_index")
        if t is None or v is None:
            continue
        by_time[t] = {"time_tag": t, "kp_index": float(v)}

    for row in new_rows:
        t = row.get("time_tag")
        v = row.get("kp_index")
        if t is None or v is None:
            continue
        by_time[t] = {"time_tag": t, "kp_index": float(v)}

    merged = list(by_time.values())
    merged.sort(key=lambda x: x["time_tag"])

    cutoff = datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)
    trimmed = []
    for row in merged:
        try:
            if parse_dt(row["time_tag"]) >= cutoff:
                trimmed.append(row)
        except Exception:
            continue

    return trimmed


def main():
    kp_obs = normalize_kp(fetch_json(SWPC_KP_OBS))
    kp_fcst = normalize_kp(fetch_json(SWPC_KP_FCST))
    dst = normalize_dst(fetch_json(SWPC_DST))
    est_kp = normalize_kp(fetch_json(SWPC_EST_KP_1M))

    write_json(os.path.join(OUT_DIR, "kp_observed.json"), {
        "source": "NOAA/SWPC",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data": kp_obs,
    })

    write_json(os.path.join(OUT_DIR, "kp_forecast.json"), {
        "source": "NOAA/SWPC",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data": kp_fcst,
    })

    write_json(os.path.join(OUT_DIR, "dst.json"), {
        "source": "NOAA/SWPC",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data": dst,
    })

    write_json(os.path.join(OUT_DIR, "kp_estimated_1m.json"), {
        "source": "NOAA/SWPC",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data": est_kp,
    })

    write_json(os.path.join(OUT_DIR, "ae_observed.json"), {
        "source": "NOAA/NCEI",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data": [],
    })

    existing = load_existing_history()
    merged_history = merge_kp_history(existing.get("data", []), kp_obs)

    write_json(KP_HISTORY_FILE, {
        "source": "NOAA/SWPC",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data": merged_history,
    })

    print(f"kp_observed: {len(kp_obs)}")
    print(f"kp_forecast: {len(kp_fcst)}")
    print(f"dst: {len(dst)}")
    print(f"kp_estimated_1m: {len(est_kp)}")
    print(f"kp_history: {len(merged_history)}")


if __name__ == "__main__":
    main()
