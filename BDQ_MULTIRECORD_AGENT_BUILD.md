# Build Plan for Multirecord BDQ Measures (for the agent)

Goal: add **multirecord Measure** support around `multi_measures.py` with a clean CLI, tests, a container, and a Pub/Sub‑driven Cloud Run worker that writes `measures.jsonl` and `measures_summary.json`. Keep Java BDQ API unchanged.

---

## 0) Inputs available
- `multi_measures.py` (the skeleton you were given)
- `TG2_multirecord_measure_tests.csv` (registry; optional first pass)

---

## 1) Project layout
Create a small Python package + CLI.

```
bdq-multi/
  pyproject.toml
  README.md
  bdq_multi/
    __init__.py
    multi_measures.py        # move the provided skeleton here (module-safe)
    registry.py               # optional: load TG2_multirecord_measure_tests.csv
    runner.py                 # CLI entry that calls into multi_measures
    worker.py                 # Pub/Sub HTTP handler (FastAPI) -> runs measures
    io_utils.py               # gzip/jsonl helpers, GCS helpers
    gbif_io.py                # dwca/csv fetch + extract (minimal)
    types.py                  # dataclasses / pydantic for BdqResponse etc. (optional)
  tests/
    test_occurrence_duplicates.py
    test_coordinate_duplicates.py
    data/
      tiny.csv                # small fixture with crafted edge cases
      single_results.csv      # tiny single-record outputs fixture
  Dockerfile
  .dockerignore
  .gitignore
```

---

## 2) Packaging: `pyproject.toml`
Create a modern, dependency‑light build with a CLI entry point.

```toml
[build-system]
requires = ["setuptools>=69", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "bdq-multi"
version = "0.1.0"
description = "Multirecord BDQ Measures (worker + CLI)"
requires-python = ">=3.10"
dependencies = []  # keep stdlib-only first; add fastapi, uvicorn, google-cloud-* later as needed

[project.scripts]
bdq-multi = "bdq_multi.runner:main"
```

---

## 3) Move the skeleton into the package
- Place the provided code in `bdq_multi/multi_measures.py`.
- Make sure it is **import-safe** (no top-level CLI execution on import).
- Export `BdqResponse`, `MultiRecordMeasure`, `build_measures`, `stream_dataset_csv`, `stream_single_results_csv`, `write_outputs` from `bdq_multi/__init__.py`.

`bdq_multi/__init__.py`:
```python
from .multi_measures import (
    BdqResponse,
    MultiRecordMeasure,
    build_measures,
    stream_dataset_csv,
    stream_single_results_csv,
    write_outputs,
)
__all__ = [name for name in dir() if not name.startswith("_")]
```

---

## 4) CLI runner
Create `bdq_multi/runner.py` as a thin wrapper over the skeleton’s `main()` so the CLI works via `bdq-multi`.

```python
from .multi_measures import main
if __name__ == "__main__":
    main()
```

Usage (local):
```
pip install -e .
bdq-multi --dataset-csv tests/data/tiny.csv --out-dir out
bdq-multi --test-results-csv tests/data/single_results.csv --out-dir out \
  --measures '[{"name":"AggregateFromSingleLabel","params":{"target_label":"VALIDATION_COUNTRYCODE_STANDARD","count_result":"NOT_COMPLIANT"}}]'
```

---

## 5) Tests
Add very small, targeted tests.

`tests/test_occurrence_duplicates.py`:
```python
from bdq_multi.multi_measures import OccurrenceIdDuplicatesScan

def test_occurrence_id_duplicates():
    m = OccurrenceIdDuplicatesScan()
    m.prepare({})
    rows = [
        {"occurrenceID": "A"},
        {"occurrenceID": "B"},
        {"occurrenceID": "A"},   # dup
        {"occurrenceID": ""},    # empty ignored
    ]
    for r in rows:
        m.consume_row(r)
    out = m.finish()
    assert out.result == 1
    assert out.status == "RUN_HAS_RESULT"
```

`tests/test_coordinate_duplicates.py`:
```python
from bdq_multi.multi_measures import CoordinateDuplicatesScan

def test_coord_dups_rounding():
    m = CoordinateDuplicatesScan()
    m.prepare({"round_decimals": 2})
    rows = [
        {"decimalLatitude":"-33.9249", "decimalLongitude":"18.4241"},
        {"decimalLatitude":"-33.9250", "decimalLongitude":"18.4242"},
        {"decimalLatitude":"-33.9249", "decimalLongitude":"18.4241"},  # dup
    ]
    for r in rows:
        m.consume_row(r)
    out = m.finish()
    assert out.result >= 1
```

Run:
```
python -m pytest -q
```

---

## 6) Optional: Registry loader
If you want to auto-list Measures from `TG2_multirecord_measure_tests.csv`, add `bdq_multi/registry.py` with a function that loads CSV to a dict keyed by label/GUID, then map rows to your class names via a small config file (yaml/json). Keep this simple; do not block core workflow if the file is missing.

---

## 7) Container
Slim Python image; non-root; use `bdq-multi` CLI as entrypoint by default for local batch, and `worker:app` for service mode.

`Dockerfile`:
```dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
COPY bdq_multi bdq_multi
COPY tests tests

RUN pip install -e . && pytest -q || true  # run tests but do not fail the image build in early stages

# Default to CLI; override in Cloud Run to run the worker
ENTRYPOINT ["bdq-multi"]
```

Build:
```
docker build -t bdq-multi:local .
```

---

## 8) Worker (Pub/Sub push → Cloud Run)
Add FastAPI app to receive Pub/Sub messages and run measures.

`bdq_multi/worker.py`:
```python
import base64, json, os, tempfile, gzip
from fastapi import FastAPI, Request
from .multi_measures import build_measures, stream_dataset_csv, stream_single_results_csv, write_outputs

app = FastAPI()

@app.post("/pubsub")
async def pubsub(request: Request):
    body = await request.json()
    msg = body.get("message", {})
    data_b64 = msg.get("data", "")
    data = json.loads(base64.b64decode(data_b64) or b"{}")

    # expected fields: {"job_id": "...", "dataset_csv_gcs": "gs://..."} or {"test_results_csv_gcs": "gs://..."}
    out_dir = os.path.join("/tmp", data.get("job_id","job"))
    os.makedirs(out_dir, exist_ok=True)

    measures_cfg = data.get("measures")
    measures = build_measures(measures_cfg)

    # download files from GCS if provided (left as TODO; implement io_utils.gcs_download)
    dataset_csv = data.get("dataset_csv_local")  # or path after download
    single_csv = data.get("test_results_csv_local")

    if dataset_csv:
        stream_dataset_csv(dataset_csv, measures)
    if single_csv:
        stream_single_results_csv(single_csv, measures)

    write_outputs(out_dir, [m.finish() for m in measures])
    # upload outputs back to GCS; return 200
    return {"ok": True, "job_id": data.get("job_id")}
```

Add minimal server entry for Cloud Run:
```
uvicorn bdq_multi.worker:app --host 0.0.0.0 --port 8080
```

**Note:** implement `io_utils.py` with `gcs_download(uri)->local_path` and `gcs_upload(local_path, uri)`. Use `google-cloud-storage` if you choose; otherwise use signed URLs and `requests`. Keep it simple.

---

## 9) Deploy worker to Cloud Run
Commands (adjust PROJECT/REGION/IMAGE):
```
gcloud builds submit --tag gcr.io/$PROJECT/bdq-multi:latest
gcloud run deploy bdq-multi-worker \
  --image gcr.io/$PROJECT/bdq-multi:latest \
  --region $REGION \
  --platform managed \
  --allow-unauthenticated \
  --port 8080
```

Create Pub/Sub topic + push subscription to `https://<run-url>/pubsub`.

---

## 10) Data contracts
Outputs:
- `measures.jsonl` — one JSON per line with:
  - `label`, `status`, `result` (number or "COMPLETE"/"NOT_COMPLETE"), `comment`, optional `qualifier`, optional `guid`
- `measures_summary.json` — simple map of label → result and a timestamp

Respect the “one number” rule for Measure `result`. Put extra breakdown into `qualifier`.

---

## 11) Wiring from the email pipeline
- After single-record run completes and `test_results.csv` is written, publish a Pub/Sub message with:
  - `job_id`, `test_results_csv_gcs` or `dataset_csv_gcs`
  - optional `measures` JSON config
- Wait for the worker to upload `measures.jsonl` and `measures_summary.json`, then include both files and a short summary in the reply email.

---

## 12) Lint, format, CI (optional)
- Add `ruff` and `pytest` to dev dependencies.
- GitHub Actions: python setup, run tests, build image (optional).

---

## 13) Acceptance checklist
- [ ] `pip install -e .` works and `bdq-multi --help` shows usage
- [ ] Running against `tests/data/tiny.csv` produces `out/measures.jsonl` and `out/measures_summary.json`
- [ ] Two unit tests pass
- [ ] Docker image builds and runs locally
- [ ] Worker deploys to Cloud Run and handles a test Pub/Sub push
- [ ] GCS upload of outputs works
- [ ] Email pipeline can consume the outputs

---

## 14) Next steps (later)
- Add more reducers (variants per ID, time gaps, etc.)
- Implement registry reading from `TG2_multirecord_measure_tests.csv`
- Add memory caps and row caps with clear flags in `qualifier`
- Emit metrics (counts, durations) as logs
