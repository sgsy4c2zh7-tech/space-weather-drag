import json
from datetime import datetime

data = {
  "norad_id": 25544,
  "name": "ISS",
  "history": [
    {
      "epoch": datetime.utcnow().isoformat(),
      "apogee_km": 420,
      "perigee_km": 415,
      "inclination_deg": 51.6
    }
  ]
}

with open("docs/data/25544.json", "w") as f:
    json.dump(data, f, indent=2)
