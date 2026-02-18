# BDQ Email Report Service

A lightweight email-based service that runs Biodiversity Data Quality (BDQ) tests on incoming datasets and replies with results.

This service receives dataset submissions via email, processes CSV files, runs BDQ tests, and replies with detailed results including validation failures and proposed amendments. Deployed on Google Cloud Run using CI/CD on push to `main` branch. Google Cloud Build builds from `Dockerfile`.

All local development should be done in Docker containers.

## Service Flow

1. Email Reception: FastAPI receives JSON payload from Google Apps Script
2. Immediate Response: Returns 200 status immediately for Apps Script acknowledgment
3. Background Processing: Email processing runs asynchronously in the background using asyncio.create_task(). Errors are logged but response to Apps Script is always 200.
4. CSV Extraction: Extracts and validates CSV attachments
5. Core Detection: Identifies occurrence vs taxon core based on column presence
6. Test Discovery: Finds applicable BDQ tests from integrated BDQ API (runs locally in same container)
   - Queries `/api/v1/tests` endpoint on localhost to get available tests
   - Filters tests based on CSV column availability
7. Test Execution: Runs tests on unique data combinations via integrated BDQ API
   - Deduplicates test candidates to avoid redundant API calls
   - Calls `/api/v1/tests/run/batch` endpoint on localhost with unique parameter combinations
   - Integrated Java service handles all BDQ test execution logic
8. Result Processing: Keeps results at the unique-combination level with a `count` of affected rows (no per-row expansion)
9. Summary Generation: Creates intelligent summaries using LLM
10. Email Reply: Sends results with summary and attachments via Google Apps Script
   - CSV of Unique results: one row per unique combination tested including actedUpon/consulted values, `status`, `result`, and `count`
   - CSV of Amended dataset: applies proposed changes from Amendment/FILLED_IN results to original data using value-based matching (no row IDs)

## BDQ API 

The BDQ API is now integrated into this service and runs as a Java Spring Boot application in the same container. It's a REST API wrapper for [FilteredPush](https://github.com/FilteredPush) biodiversity data quality validation libraries. FilteredPush provides implementations of BDQ (Biodiversity Data Quality) Tests via the FFDQ API. The BDQ API uses those libraries directly, mapping their responses into a simple JSON shape. The BDQ standard defines a library of Tests documented in the TDWG BDQ repository in the `bdq-api/TG2_tests.csv` file.

The BDQ API runs on port 8081 (configurable via `BDQ_API_PORT` environment variable) and communicates with the Python FastAPI service via localhost HTTP calls, eliminating network timeouts and improving performance.

### Tests List Endpoint 

The `/api/v1/tests/` endpoint is simple and extremely fast to load. Does not change very often. Included in `tests/data/bdq_api_tests_list.json` for testing purposes.

### Batch Endpoint 

The `/api/v1/tests/run/batch` endpoint is slower as it does heavy processing, such as checking long lists of coordinates to see if they fall within a country boundary. 

Accepts an array of `{ id, params }` objects, with the test name as the id:
```json
[
   { "id": "VALIDATION_COUNTRYCODE_VALID", "params": { "dwc:countryCode": "US" } },
   { "id": "AMENDMENT_EVENTDATE_STANDARDIZED", "params": { "dwc:eventDate": "8 May 1880" } }
]
```

It returns a list of results in the same order as the input tests:
```json
[
   { "status": "RUN_HAS_RESULT", "result": "COMPLIANT", "comment": "..." },
   { "status": "AMENDED", "result": "dwc:eventDate=1880-05-08", "comment": "..." },
   { "status": "NOT_AMENDED", "result": "", "comment": "..." },
   { "status": "AMENDED", "result": "dwc:decimalLatitude=\"-25.46\"|dwc:decimalLongitude=\"135.87\"", "comment": "..." }
]
```

Result Types:
- Single-field amendment: `result: dwc:eventDate=1880-05-08`
- Multi-field amendment: `result: dwc:minimumDepthInMeters=3.048 | dwc:maximumDepthInMeters=3.048`
- Validation item: `result: COMPLIANT` (unchanged; still the label from the value)
- Amendment test that didn't make changes: `status: NOT_AMENDED`, `result: ""`, `comment: explanation of why no amendment was needed`
- Failed item: `status: INTERNAL_PREREQUISITES_NOT_MET`, `result: ""`, `comment: error message (e.g., "Unknown test id or guid: …")`

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

All tests must be run in Docker containers as per development rules [[memory:7740249]]:

```bash
# Run all tests
docker compose --profile test run --rm test-runner
```

To run the live OpenAI Responses API test (requires OPENAI_API_KEY in .env.test):
```bash
docker compose --profile test run --rm -e OPENAI_LIVE=1 test-runner python -m pytest tests/test_openai_live.py -v -s
```

### Architecture

The service is a **single unified service** deployed as one container:
- **External-facing**: Python FastAPI (port 8080) - handles all HTTP requests
- **Internal**: Java BDQ API (port 8081, localhost only) - handles BDQ test execution using FilteredPush libraries

The Java BDQ API runs internally and is only accessed via localhost HTTP calls from Python. This eliminates network timeouts while keeping the services decoupled. Both processes start automatically when the container starts, with Python waiting for Java to be ready before accepting requests.

## Cloud Tasks Integration

The service uses **Cloud Tasks** for reliable email processing that survives container shutdowns:

- **Guaranteed delivery**: Tasks are persisted and survive container shutdowns
- **Automatic retries**: Cloud Tasks retries failed tasks up to 3 times
- **Cost effective**: No need for `min-instances=1` - containers can scale to zero
- **Fallback mode**: If Cloud Tasks is not configured, falls back to async tasks

See [CLOUD_TASKS_SETUP.md](CLOUD_TASKS_SETUP.md) for setup instructions.

### How It Works

1. Apps Script sends email → FastAPI returns 200 immediately
2. FastAPI creates Cloud Task with email data
3. Cloud Tasks calls `/tasks/process-email` worker endpoint
4. Worker processes email and sends reply
5. If container dies, Cloud Tasks automatically retries
