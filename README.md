# BDQ Email Report Service

A lightweight email-based service that runs Biodiversity Data Quality (BDQ) tests on incoming datasets and replies with results.

This service receives dataset submissions via email, processes CSV files, runs BDQ tests, and replies with detailed results including validation failures and proposed amendments. Deployed on Google Cloud Run using CI/CD on push to `main` branch. Google Cloud Build builds from `Dockerfile`.

All local development should be done in Docker containers.

## Service Flow

1. **Email Reception**: FastAPI receives JSON payload to from Google Apps Script
2. **Immediate Response**: Returns 200 status immediately for Apps Script acknowledgment
3. **CSV Extraction**: Extracts and validates CSV attachments
4. **Core Detection**: Identifies occurrence vs taxon core based on column presence
5. **Test Discovery**: Finds applicable BDQ tests from TG2_tests.csv
   - If multiple Java methods share the same annotation label, the service prefers the method whose name does not end with `String` (for stability across libraries)
6. **Test candidate deduplication**: Extract unique tuples for running on each test
7. **Test Execution**: Runs tests on unique tuples via Py4J integration with Java BDQ libraries
   - Local JVM process bdq-py4j-gateway (in the same Docker container) with resident FilteredPush BDQ libraries git submodules (geo_ref_qc, event_date_qc, sci_name_qc, rec_occur_qc)
   - Py4J executes BDQ tests via direct Java method calls
8. **Result Processing**: Expands test results to all matching rows
9. **Summary Generation**: Creates intelligent summaries using LLM
10. **Email Reply**: Sends results with summary and attachments via Google Apps Script
   - CSV of Raw results: per row × per applicable test → `occurrenceID or taxonID`, `status`, `result`, `comment`, `amendment` (if any)
   - CSV of Amended dataset: applies proposed changes from Amendment results

## Google Apps Script

   - Copy of code deployed is in `google-apps-scripts/`. 
   - A Gmail account is set up solely for this service.

### Email Forwarding

   - An Apps Script polls the inbox once per minute using the Gmail Advanced Service and forwards them to this service's `/email/incoming` endpoint
   - Messages that have been successfully forwarded are labeled `bdq/processed` to avoid duplicates.
   - One script handles forwarding to /email/incoming (no auth or HMAC for this), another (deployed as a Web app) is used to send replies. 
   - Expected email volume is very low (~3 per week), so quotas are not a concern.

## Email Sending

- Another Apps Script deployed as a Web app acts as a "send mail" webhook, this avoids oauth and periodic reauth. Need to add GMAIL_SEND env var to Google Cloud Run with endpoint.
- Call example: 
   ```
   curl -X POST "{GMAIL_SEND}" \
   -H "Content-Type: application/json" \
   -d '{
      "threadId": "1873e0a1f1c8fabc",        // use this to reply in-thread
      "bodyText": "Here are your BDQ results.\nSee attachments.",
      "bodyHtml": "<p>Here are your BDQ results.</p>",
      "attachments": [
         {
         "filename": "bdq_raw_results.csv",
         "mimeType": "text/csv",
         "contentBase64": "<base64>"
         },
         {
         "filename": "amended_dataset.csv",
         "mimeType": "text/csv",
         "contentBase64": "<base64>"
         }
      ]
   }'
   ```
### Running Tests

All tests must be run in Docker containers as per development rules:

```bash
# Run all tests
docker compose --profile test run --rm test-runner
```

Test data files in `tests/data/`:
- `simple_occurrence_dwc.csv` - Basic occurrence core data
- `simple_taxon_dwc.csv` - Basic taxon core data  
- `prefixed_occurrence_dwc.csv` - Occurrence data with dwc: prefixes
- `occurrence.txt` - Large occurrence dataset for performance testing

## Background Processing Plan

- Recommendation: Use Cloud Tasks push to a dedicated Cloud Run worker service. It offers deterministic delivery, per‑task timeouts, automatic retries, and simple idempotency without managing consumer loops.

- Rationale: Returning 200 and doing work in an in‑process background task can stall on Cloud Run when CPU throttles after the response. With Cloud Tasks, the worker request keeps CPU allocated for the lifetime of the task.

- Architecture:
  - Ingress (`bdq-app`): `/email/incoming` validates and enqueues a Cloud Task with the original JSON, then returns 200 immediately.
  - Worker (`bdq-worker`): Cloud Run service (same image) exposes `POST /tasks/process-email` and runs the BDQ pipeline synchronously per task. Returns 2xx to ack.
  - Auth: Cloud Tasks OIDC token with audience `WORKER_URL`; worker validates token. No HMAC needed for internal tasks.
  - Idempotency: Use Gmail `messageId` as the task name (`email-<messageId>`). If it exists, treat as duplicate and return 200.
  - Concurrency/limits: Worker concurrency 1 (or low), timeout up to 900s, memory ≥ 1GiB.
  - Retries/DLQ: Configure retry policy and optional dead‑letter queue.

- Implementation Steps:
  - Add endpoint: Implement `POST /tasks/process-email` that executes the existing email processing flow synchronously (no `asyncio.create_task`).
  - Add enqueuer: In `/email/incoming`, create a Cloud Task using queue `CLOUD_TASKS_QUEUE` in `CLOUD_TASKS_LOCATION`, target `WORKER_URL`, OIDC `SERVICE_ACCOUNT_EMAIL`, name `email-<messageId>`, body as JSON.
  - Config: Add env vars `CLOUD_TASKS_QUEUE`, `CLOUD_TASKS_LOCATION`, `WORKER_URL`, `SERVICE_ACCOUNT_EMAIL`.
  - Infra: In `cloudbuild.yaml`, add a step to create/update the Cloud Tasks queue via `gcloud tasks queues create|update` (or manage via Terraform).
  - Deploy: Deploy two Cloud Run services from the same image: `bdq-app` (ingress) and `bdq-worker` (tasks). Set worker `--timeout`, `--concurrency`, `--memory` appropriately.
  - Tests: Unit test the enqueuer (validate task payload/target). Integration test calls `/tasks/process-email` directly with a small CSV and asserts non‑ERROR statuses and two CSV attachments.

- Pub/Sub Alternative:
  - Use topic `bdq-incoming-email` with a push subscription to `bdq-worker`. Publish from `/email/incoming`.
  - Worker validates Pub/Sub push token, processes synchronously, and handles duplicates via `messageId` lock.
  - Tradeoffs: Suited for fan‑out; requires managing ack deadlines and at‑least‑once delivery. Not needed for single‑consumer, HTTP‑push workflow.

- Decision: Start with Cloud Tasks for simplicity and deterministic push semantics. Revisit Pub/Sub if we need multi‑consumer patterns or higher throughput.
