BDQ Multirecord Agent (Docker-first)

- Package: `bdq_multi` with CLI and worker.
- Registry: auto-maps all 144 TG2 multirecord measures from `TG2_multirecord_measure_tests.csv`.
- GCS: downloads inputs and uploads outputs using `google-cloud-storage`.

Quick Start
- Build: `docker compose build`
- CLI basic: `docker compose run --rm cli --dataset-csv tests/data/tiny.csv --out-dir out`
- CLI with registry (all 144):
  - Single results: `docker compose run --rm cli --use-registry --test-results-csv tests/data/single_results.csv --out-dir out_reg`
  - Dataset + registry: `docker compose run --rm cli --use-registry --dataset-csv tests/data/tiny.csv --out-dir out_reg_ds`
- Worker: `docker compose up worker` (listens on `http://localhost:8080/pubsub`)

Worker Message (Pub/Sub push body)
- Content-Type: application/json
- Body shape:
  - `{ "message": { "data": base64(json) } }`
- Inner JSON fields:
  - `job_id`: unique string for output folder in `/tmp`
  - Inputs: one or both of
    - `dataset_csv_gcs` or `dataset_csv_local`
    - `test_results_csv_gcs` or `test_results_csv_local`
  - Output: `out_gcs_prefix` (e.g., `gs://bucket/path/job-123`)
  - Measures:
    - `use_registry: true` to derive all measures from the TG2 CSV (copied into the image)
    - or `measures`: explicit JSON list of `{name, params}`
  - Optional:
    - `registry_csv_gcs` or `registry_csv_local` to override the registry source

Outputs
- Writes to `/tmp/<job_id>/`:
  - `measures.jsonl` and `.gz`: one JSON per measure with fields: `label`, `status`, `result`, optional `qualifier`, optional `guid`.
  - `measures_summary.json` and `.gz`: map of label → result and a timestamp.

Registry Auto‑Mapping
- File: `TG2_multirecord_measure_tests.csv` (copied into the image root).
- Loader: `bdq_multi.registry.measures_from_registry()`
- Strategy per row:
  - Derive target single‑record label from `InformationElement:ActedUpon` (e.g., `bdq:VALIDATION_COUNTRYCODE_STANDARD.Response` → `VALIDATION_COUNTRYCODE_STANDARD`).
  - If label contains `COUNT` or text indicates counting compliance → `AggregateFromSingleLabel` with `count_result=COMPLIANT`.
  - If label contains `QA` or text indicates COMPLETE/NOT_COMPLETE QA → `QaAllCompliantOrPrereq`.
  - Each registry row yields one measure config with `label` override set to the registry `Label` and `guid` from `term_localName` (or `term_iri`).
- Result: 144 measure configs returned for the current registry.

GCS Auth
- Local: provide credentials via `GOOGLE_APPLICATION_CREDENTIALS=/path/key.json` or `gcloud auth application-default login` and mount ADC into the container if needed.
- Cloud Run: prefer Workload Identity; no secrets in the image.

Develop/Extend
- Add more multi‑record measures in `bdq_multi/multi_measures.py` and map them automatically by adjusting `registry.py` if needed.
- Unit tests: `pytest -q` inside the image (compose already installs pytest).
- CLI entrypoint: `bdq-multi`. Worker: `bdq_multi.worker:app`.

