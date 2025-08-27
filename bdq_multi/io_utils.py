import os
import tempfile
from typing import Optional, Tuple

from google.cloud import storage  # type: ignore


def _parse_gs_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError("GCS URI must start with gs://")
    path = uri[len("gs://") :]
    parts = path.split("/", 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def gcs_download(uri: str, dest: Optional[str] = None) -> str:
    bucket_name, blob_name = _parse_gs_uri(uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if dest is None:
        base = os.path.basename(blob_name) or "download"
        fd, dest = tempfile.mkstemp(prefix="bdq_", suffix="_" + base)
        os.close(fd)
    blob.download_to_filename(dest)
    return dest


def gcs_upload(local_path: str, uri: str) -> None:
    bucket_name, blob_name = _parse_gs_uri(uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)

