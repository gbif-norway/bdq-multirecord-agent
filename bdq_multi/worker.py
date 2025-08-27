import base64
import json
import os
from typing import List

from fastapi import FastAPI, Request

from .multi_measures import (
    build_measures,
    stream_dataset_csv,
    stream_single_results_csv,
    write_outputs,
)
from .io_utils import gcs_download, gcs_upload
from .registry import measures_from_registry

app = FastAPI()


@app.post("/pubsub")
async def pubsub(request: Request):
    body = await request.json()
    msg = body.get("message", {})
    data_b64 = msg.get("data", "")
    data = json.loads(base64.b64decode(data_b64) or b"{}")

    # expected fields: {"job_id": "...", "dataset_csv_gcs": "gs://..."} or {"test_results_csv_gcs": "gs://..."}
    # optional: {"out_gcs_prefix": "gs://bucket/path/prefix"}
    out_dir = os.path.join("/tmp", data.get("job_id", "job"))
    os.makedirs(out_dir, exist_ok=True)

    measures_cfg = data.get("measures")

    # Resolve local input paths from either local or GCS
    dataset_csv = data.get("dataset_csv_local")
    single_csv = data.get("test_results_csv_local")
    if not dataset_csv and data.get("dataset_csv_gcs"):
        dataset_csv = gcs_download(data["dataset_csv_gcs"])  # download to temp
    if not single_csv and data.get("test_results_csv_gcs"):
        single_csv = gcs_download(data["test_results_csv_gcs"])  # download to temp

    # Optionally build measures from registry
    if data.get("use_registry") and measures_cfg is None:
        registry_csv = data.get("registry_csv_local")
        if not registry_csv and data.get("registry_csv_gcs"):
            registry_csv = gcs_download(data["registry_csv_gcs"])
        measures_cfg = measures_from_registry(registry_csv or "TG2_multirecord_measure_tests.csv")

    measures = build_measures(measures_cfg)

    if dataset_csv:
        stream_dataset_csv(dataset_csv, measures)
    if single_csv:
        stream_single_results_csv(single_csv, measures)

    write_outputs(out_dir, [m.finish() for m in measures])

    # Upload outputs if requested
    out_prefix = data.get("out_gcs_prefix")
    if out_prefix:
        files: List[str] = [
            "measures.jsonl",
            "measures.jsonl.gz",
            "measures_summary.json",
            "measures_summary.json.gz",
        ]
        for fname in files:
            local_path = os.path.join(out_dir, fname)
            gcs_uri = out_prefix.rstrip("/") + "/" + fname
            gcs_upload(local_path, gcs_uri)
    return {"ok": True, "job_id": data.get("job_id")}
