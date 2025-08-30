
# BDQ Email → Report Service — Implementation Spec (Cloud Run, Gmail Polling)

A lightweight service that lets users **email a Darwin Core core file** (e.g. `occurrence.txt`, `taxon.txt`, or CSV/TSV) and get back:
- a **natural-language summary**,
- a **per-field issue breakdown**,
- the **raw BDQ test results**, and
- an **amended dataset** with proposed fixes applied.

Runs on **Google Cloud Run (free tier)**, polls a **Gmail inbox**, calls the **BDQ API** (hosted on Cloud Run), and replies by email with attachments. All state is in Gmail (labels), so no database.

---

## 0) Key choices / constraints
- **Gmail polling** (no webhooks/push): Cloud Scheduler triggers polling every N minutes.
- **In-memory CSV** processing: load the whole core file once.
- **Unique‐value dedup** by *actedUpon* combos **per test** to minimize API calls.
- **No multi-record tests**; instead, **aggregate single-record validation failures** across the dataset and report counts.
- **Discord webhook** for error logging.
- Include **amended dataset** in v1.

---

## 1) High-level flow

1. **Scheduler → Cloud Run**: periodic HTTP call to `/poll`.
2. **Poll Gmail** for new messages (e.g. label `DwC-Incoming` or just unread). For each:
   - Download the **attachment** (plain core `.txt`/`.csv`/`.tsv` or **DwC-A zip** → extract `occurrence.txt`/`taxon.txt`).
3. **Load the core file into memory** (detect delimiter, header). Determine **core type** by header presence:
   - Occurrence core if header contains `occurrenceID`.
   - Taxon core if header contains `taxonID`.
4. **Discover tests** from `GET {BDQ_API_BASE}/api/v1/tests`. Build a plan:
   - For each test, if **all `actedUpon` columns** exist in the CSV header, include it.
   - Split into **Validations** and **Amendments** by the `type` field.
5. **Unique-value dedup per test**:
   - For each test, create a set of **unique tuples** = values of its `actedUpon` columns across **all rows**.
   - For each unique tuple, **call `/tests/run`** once. Cache the result by `(test_id, tuple)`.
   - **Map cached results back** to every row that has that same tuple.
6. **Build artifacts**:
   - **Raw results** (CSV or JSON): per row × per applicable test → `status`, `result`, `comment`, `amendment` (if any).
   - **Amended dataset** (CSV): apply proposed changes from **Amendment** results.
   - **Summary** (email body):
     - Totals (records, tests run)
     - **Per-field validation failure counts** across all rows
     - Examples/samples of common issues
     - Note that the amended dataset applies **proposed** changes.
7. **Reply by email** to the sender with:
   - **Summary** in the body.
   - Attach **raw results** and **amended dataset**.
8. Label the original Gmail thread as **processed** and/or archive it.

On any error, **post to Discord** with context, keep the email unprocessed so the next poll retries.

---

## 2) Inputs & outputs

### Accepted inputs (attachments)
- **Plain core**: `.txt` (tab), `.tsv`, `.csv` with Darwin Core header.
- **DwC-A zip**: unzip and use `occurrence.txt` or `taxon.txt`. If both exist, prefer the one referenced by `meta.xml` (if present); else pick the obvious one.

### Outputs (attachments in reply)
- `bdq_raw_results.csv` (or `.json`)
- `amended_dataset.csv`
- Body: **human summary** + **per-field breakdown**

---

## 3) Config & secrets

Use env vars in Cloud Run; store secrets in **Secret Manager**.

```
BDQ_API_BASE=https://bdq-api-638241344017.europe-west1.run.app
GMAIL_POLL_LABEL=DwC-Incoming        # optional; empty → use INBOX unread
GMAIL_FROM_FILTER=                   # optional
MAX_WORKERS=8                        # parallelism for /tests/run calls
HTTP_TIMEOUT_S=30                    # per BDQ request
DISCORD_WEBHOOK_URL=...              # for error logs
REPLY_FROM=bdq-reports@yourdomain    # or the Gmail address
MAX_ATTACHMENT_MB=25
```

Gmail auth options (pick one):
- **Gmail API OAuth** (service account w/ delegated domain-wide auth or installed app flow).
- **IMAP/SMTP** with an **app password** for a dedicated Gmail account (simple; store in Secret Manager).

---

## 4) BDQ API usage

### 4.1 Discover tests
`GET {BDQ_API_BASE}/api/v1/tests` → array like:

```json
{
  "id": "AMENDMENT_BASISOFRECORD_STANDARDIZED",
  "guid": "07c28ace-...",
  "type": "Amendment",
  "actedUpon": ["dwc:basisOfRecord"],
  "consulted": [],
  "parameters": []
}
```

### 4.2 Run a test (per unique tuple)
`POST {BDQ_API_BASE}/api/v1/tests/run`

**Request** (recommendation; adjust to your server):
```json
{
  "id": "VALIDATION_TAXONRANK_STANDARD",
  "inputs": {
    "dwc:taxonRank": "species"
  },
  "parameters": {}
}
```

**Response** (normalized form):
```json
{
  "status": "RUN_HAS_RESULT",
  "result": "COMPLIANT",                // or NOT_COMPLIANT, POTENTIAL_ISSUE, etc.
  "comment": "dwc:taxonRank is valid",
  "amendment": null                     // or {"dwc:...": "newValue"} for Amendment tests
}
```

> If your server returns amendments via `result` or another field, normalize into `amendment` during ingestion.

---

## 5) Algorithm details

### 5.1 Header & core detection
- Read first line → header list `hdr`.
- Core = `"occurrence"` if `"occurrenceID" in hdr`; else `"taxon"` if `"taxonID" in hdr`.
- If neither, return an error mail.

### 5.2 Build test plan
- `tests = GET /tests`
- Keep `tests_ok = [t for t in tests if all(strip_prefix(c) in hdr for c in t.actedUpon)]`
  - `strip_prefix("dwc:decimalLatitude") → "decimalLatitude"`
- Partition:
  - `validations = [t for t in tests_ok if t.type == "Validation"]`
  - `amendments  = [t for t in tests_ok if t.type == "Amendment"]`

### 5.3 Unique-tuple cache keys
For each test `t`:
- `cols = [strip_prefix(c) for c in t.actedUpon]`
- For every row `r`, build `key = tuple(r[col] or "")`
- Maintain `seen[t.id] = set(keys)`

### 5.4 Execute tests with de-dup
- For each `t` in `validations + amendments`:
  - For each `key` in `seen[t.id]` (parallel up to `MAX_WORKERS`):
    - Build `inputs = {"dwc:"+c: v for c,v in zip(cols,key)}`
    - `resp = POST /tests/run`
    - Cache `cache[(t.id, key)] = norm(resp)`

### 5.5 Map results back to rows
- For each row index `i`:
  - For each `t` applicable:
    - `cols = [strip_prefix(c) for c in t.actedUpon]`
    - `key = tuple((row.get(c) or "").strip() for c in cols)`
    - `assertion = cache[(t["id"], key)]`
    - Append to `raw_results`:
      - `row_index=i, test_id=t.id, type=t.type, status, result, comment, actedUpon=cols, values=key, amendment=json/amendedPairs`

### 5.6 Build amended dataset
- Make a deep copy `rows_amended = rows_original`
- For each `raw_result` where `type=="Amendment"` and `amendment` not empty:
  - For each `(dwc:term -> newVal)` in amendment:
    - `col = strip_prefix(term)`
    - `rows_amended[i][col] = newVal`

> Keep it simple: **apply all proposed amendments**. If you want a smarter policy later, add a filter or confidence rules.

### 5.7 Aggregate failures (replace “multi-record”)
- For **validations** only, count failures across all rows:
  - `fail_by_field[col] += 1` when a validation on `col` returns `NOT_COMPLIANT` (or equivalent fail states).
- Also compute per-test counts if helpful.

### 5.8 Summary text (email body)
Include:
- Records processed, tests run (uniq tuples evaluated counts can be shown too)
- **Top issue buckets by field** (descending count) with short explanations
- Note that **amendments were auto-applied** in the attached dataset
- If any rows/keys errored, list counts and advise re-try

---

## 6) File formats

### 6.1 `bdq_raw_results.csv`
Columns (suggested):
```
row_index,test_id,test_type,status,result,comment,acted_upon,values,amendment_json
```

### 6.2 `amended_dataset.csv`
- Same header as input
- All amendments applied

---

## 7) Email handling

- **Polling**: Cloud Scheduler → `GET https://SERVICE_URL/poll`
- **Gmail search**: unread or label `GMAIL_POLL_LABEL`
- **MIME parse**: pick the 1st valid attachment (or follow `meta.xml` for DwC-A)
- **Reply**:
  - Subject: `BDQ Report: {filename} — {nRecords} records`
  - Body: summary text
  - Attach: `bdq_raw_results.csv`, `amended_dataset.csv`
- **Mark processed**: add label `DwC-Processed`, remove `DwC-Incoming`, mark as read, or move to archive

---

## 8) Error handling & Discord logging

- Wrap each stage with try/except
- On exception:
  - Log to stdout (Cloud Logging)
  - POST to `DISCORD_WEBHOOK_URL` with JSON:
    ```json
    {
      "stage": "poll|parse|run_tests|email_send",
      "message": "...",
      "email_id": "...",
      "filename": "...",
      "trace": "stacktrace"
    }
    ```
  - **Do not** mark the email as processed; allow retry on next poll

---

## 9) Performance notes

- Unique-value dedup keeps `/tests/run` calls low, even for large files with repeated values.
- Use a **ThreadPool** up to `MAX_WORKERS` for BDQ calls.
- Consider simple **LRU cache** across tests within one run, keyed by `(test_id, key)`.
- Respect timeouts; if BDQ API is down, fail fast and retry on next poll.

---

## 10) Project layout (monolith, simple)

```
/app
  main.py            # all logic here (monolithic, readable)
  requirements.txt
  Dockerfile
```

### `requirements.txt` (suggested)
```
Flask
requests
google-api-python-client
google-auth
google-auth-oauthlib
google-auth-httplib2
python-dotenv
pytz
```

*(If using IMAP/SMTP instead of Gmail API, add `imapclient` and use `smtplib` from stdlib.)*

### `Dockerfile` (python-slim)
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["python", "main.py"]
```

---

## 11) Core pseudocode (minimal, clear)

```python
# main.py
import os, csv, io, zipfile, json, base64, traceback
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request

BDQ_API = os.environ["BDQ_API_BASE"]
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
TIMEOUT = int(os.getenv("HTTP_TIMEOUT_S", "30"))

app = Flask(__name__)

@app.get("/poll")
def poll():
    try:
        msgs = gmail_list_unprocessed()            # unread or with specific label
        for msg in msgs:
            try:
                process_message(msg)
                gmail_mark_processed(msg)
            except Exception as e:
                post_discord("process_message", e, msg=msg)
        return "ok", 200
    except Exception as e:
        post_discord("poll", e)
        return "err", 500

def process_message(msg):
    att = gmail_download_first_attachment(msg)
    core_bytes, fname = extract_core(att)          # handles zip or plain
    rows, header, delim = load_core(core_bytes)
    core_type = detect_core(header)                # occurrence|taxon
    tests = fetch_tests()
    plan = plan_tests(tests, header)               # validations, amendments

    cache = {}
    raw_rows = []

    # Build unique tuples per test
    uniq = build_unique_sets(plan, header, rows)

    # Run deduped test calls
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        futs = []
        for t in plan["validations"] + plan["amendments"]:
            for key in uniq[t["id"]]:
                futs.append(ex.submit(run_test_once, t, key))
        for f in futs:
            tid, key, assertion = f.result()
            cache[(tid, key)] = assertion

    # Map back to rows, collect raw results
    for i, row in enumerate(rows):
        for t in plan["validations"] + plan["amendments"]:
            cols = [strip(c) for c in t["actedUpon"]]
            key = tuple((row.get(c) or "").strip() for c in cols)
            assertion = cache[(t["id"], key)]
            raw_rows.append(format_raw(i, t, cols, key, assertion))

    # Build amended dataset
    amended = apply_amendments(rows, raw_rows)

    # Aggregate failures
    by_field = count_validation_failures(raw_rows)

    # Write artifacts (in-memory or tmp files)
    raw_csv = to_csv(raw_rows)
    amended_csv = to_csv_rows(amended, header)

    # Compose summary (plain text or simple markdown)
    summary = render_summary(len(rows), plan, by_field)

    # Reply email
    gmail_reply_with_attachments(msg, summary, [
        ("bdq_raw_results.csv", raw_csv),
        ("amended_dataset.csv", amended_csv),
    ])

# ... Implement helpers: gmail_*, extract_core, load_core, detect_core,
# fetch_tests, plan_tests, build_unique_sets, run_test_once, apply_amendments,
# count_validation_failures, render_summary, post_discord, etc.
```

---

## 12) Helper logic notes

- **Delimiter detection**: if header line contains tabs → TSV; else comma → CSV.
- **strip_prefix**: `dwc:decimalLatitude` → `decimalLatitude`.
- **run_test_once(t, key)**:
  - `inputs = {"dwc:"+c: v for c,v in zip(cols,key)}`
  - `POST /tests/run` with `{id, inputs, parameters:{}}`
  - Normalize to `{status, result, comment, amendment}` and return
- **apply_amendments**:
  - For each raw row where `test_type=="Amendment"` and `amendment` not empty → set `rows[i][col]=newVal`
- **count_validation_failures**:
  - For every `raw_row` with `test_type=="Validation"` and failed result → `by_field[col]+=1` for each actedUpon col
- **render_summary**: short bullets sorted by failure count desc.
- **Discord**: simple `requests.post(DISCORD_WEBHOOK_URL`, json=payload)

---

## 13) Deployment (CLI)

```bash
# build & deploy
gcloud builds submit --tag gcr.io/$PROJECT_ID/bdq-mailer
gcloud run deploy bdq-mailer   --image gcr.io/$PROJECT_ID/bdq-mailer   --region europe-west1   --platform managed   --allow-unauthenticated   --memory 512Mi

# scheduler (every 10 minutes)
gcloud scheduler jobs create http bdq-mailer-poll   --schedule "*/10 * * * *"   --uri "https://<SERVICE_URL>/poll"   --http-method GET
```

Give the Cloud Run service access to Secret Manager / Gmail credentials as needed.

---

## 14) Test plan

- **Happy path**: small occurrence CSV (100 rows), expect raw CSV + amended CSV, summary lists issues for countryCode, taxonRank, etc.
- **DwC-A zip**: includes `meta.xml`, both `occurrence.txt` and `taxon.txt` → pick core by meta or header.
- **Missing columns**: test selection should naturally skip tests whose `actedUpon` columns are absent.
- **Amendments**: verify amended CSV diff shows expected standardized values.
- **BDQ API errors**: induce a timeout → Discord log, email stays unprocessed, retries later.
- **Large duplicate values**: ensure dedup shrinks API calls (log counts).

---

## 15) Nice-to-have (later)

- Batch `/tests/run` endpoint that accepts **arrays of keys** per test to cut round-trips.
- Post-amendment **validation re-run** and show before/after deltas in the summary.
- Optional **HTML** summary with small tables.
- Rate limiting / backoff for BDQ calls.

---

## 16) Notes

- Amendments are **proposals**. This service applies them to produce a convenience CSV; users should still review.
- For transparency, consider adding a small **“CHANGELOG”** CSV that lists (row, column, old → new).

---

**Owner-implemented BDQ API**: acknowledged (the BDQ API is hosted on Cloud Run by you). This spec keeps the external interface simple and builds dedup + mapping on the client side.
