#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${ROOT_DIR}/src/main/resources/reference-data"
TMP_DIR="${DATA_DIR}/.tmp-download"

mkdir -p "${TMP_DIR}"
export DATA_DIR TMP_DIR

echo "Downloading ISO 3166 country codes..."
curl -sSL https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.json \
  -o "${TMP_DIR}/iso-3166-raw.json"

python3 - <<'PYCODE'
import json
from pathlib import Path
import datetime
import os

tmp_dir = Path(os.environ["TMP_DIR"])
out_path = Path(os.environ["DATA_DIR"]) / "iso-3166.json"

with open(tmp_dir / "iso-3166-raw.json", "r", encoding="utf-8") as fh:
    countries = json.load(fh)

trimmed = []
for row in countries:
    trimmed.append({
        "alpha2": row.get("alpha-2"),
        "alpha3": row.get("alpha-3"),
        "numeric": row.get("country-code"),
        "name": row.get("name"),
        "region": row.get("region"),
        "subRegion": row.get("sub-region"),
    })

payload = {
    "metadata": {
        "sourceUrl": "https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes",
        "retrieved": datetime.datetime.utcnow().isoformat() + "Z"
    },
    "countries": trimmed
}

out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
PYCODE

echo "Downloading IANA time zones..."
curl -sSL https://raw.githubusercontent.com/dmfilipenko/timezones.json/master/timezones.json \
  -o "${TMP_DIR}/timezones-raw.json"

python3 - <<'PYCODE'
import json
from pathlib import Path
import datetime
import os

tmp_dir = Path(os.environ["TMP_DIR"])
out_path = Path(os.environ["DATA_DIR"]) / "iana-time-zones.json"

zones = json.loads((tmp_dir / "timezones-raw.json").read_text(encoding="utf-8"))
records = []
for z in zones:
    ids = z.get("utc")
    if isinstance(ids, str):
        ids = [ids]
    elif ids is None:
        ids = []
    records.append({
        "ids": ids,
        "description": z.get("value"),
        "abbr": z.get("abbr"),
        "offset": z.get("offset"),
    })

payload = {
    "metadata": {
        "sourceUrl": "https://github.com/dmfilipenko/timezones.json",
        "retrieved": datetime.datetime.utcnow().isoformat() + "Z"
    },
    "zones": records
}

out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
PYCODE

echo "Downloading Natural Earth admin0 boundaries (GeoJSON)..."
curl -sSL https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson \
  -o "${DATA_DIR}/natural-earth-admin0.geojson"

echo "Downloading GBIF vocabularies..."
curl -sSL "https://api.gbif.org/v1/vocabularies/basisOfRecord/terms?limit=300" \
  -o "${TMP_DIR}/gbif-basisOfRecord.json"
curl -sSL "https://api.gbif.org/v1/vocabularies/establishmentMeans/terms?limit=300" \
  -o "${TMP_DIR}/gbif-establishmentMeans.json"
curl -sSL "https://api.gbif.org/v1/vocabularies/degreeOfEstablishment/terms?limit=300" \
  -o "${TMP_DIR}/gbif-degreeOfEstablishment.json"

python3 - <<'PYCODE'
import json
from pathlib import Path
import datetime
import os

tmp_dir = Path(os.environ["TMP_DIR"])
out_path = Path(os.environ["DATA_DIR"]) / "gbif-vocabularies.json"

def extract_terms(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    items = []
    for term in data.get("results", []):
        items.append({
            "name": term.get("name"),
            "description": term.get("definition"),
            "deprecated": term.get("deprecated", False)
        })
    return items

payload = {
    "metadata": {
        "sourceUrl": "https://api.gbif.org/v1/vocabularies",
        "retrieved": datetime.datetime.utcnow().isoformat() + "Z"
    },
    "vocabularies": {
        "basisOfRecord": extract_terms(tmp_dir / "gbif-basisOfRecord.json"),
        "establishmentMeans": extract_terms(tmp_dir / "gbif-establishmentMeans.json"),
        "degreeOfEstablishment": extract_terms(tmp_dir / "gbif-degreeOfEstablishment.json")
    }
}

out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
PYCODE

echo "Cleaning up temporary files..."
rm -rf "${TMP_DIR}"

echo "Recomputing SHA-256 checksums:"
find "${DATA_DIR}" -maxdepth 1 -type f \( -name '*.json' -o -name '*.geojson' \) -print0 | while IFS= read -r -d '' file; do
  printf "%s %s\n" "$(shasum -a 256 "$file" | awk '{print $1}')" "${file}"
done
