# BDQ Email Report Service

A lightweight email-based service that runs Biodiversity Data Quality (BDQ) tests on incoming datasets and replies with results.

This service receives dataset submissions via email, processes CSV files, runs BDQ tests, and replies with detailed results including validation failures and proposed amendments. Deployed on Google Cloud Run using CI/CD on push to `main` branch. Google Cloud Build builds from `Dockerfile`.

All local development should be done in Docker containers.

## Service Flow

1. Email Reception: FastAPI receives JSON payload from Google Apps Script
2. Immediate Response: Returns 200 status immediately for Apps Script acknowledgment
3. Background Processing: Email processing runs asynchronously in the background
4. CSV Extraction: Extracts and validates CSV attachments
5. Core Detection: Identifies occurrence vs taxon core based on column presence
6. Test Discovery: Finds applicable BDQ tests from external BDQ API
   - Queries `/api/v1/tests` endpoint to get available tests
   - Filters tests based on CSV column availability
7. Test Execution: Runs tests on unique data combinations via external BDQ API
   - Deduplicates test candidates to avoid redundant API calls
   - Calls `/api/v1/tests/run/batch` endpoint with unique parameter combinations
   - External API handles all BDQ test execution logic
   
   Batch Endpoint Format: The `/api/v1/tests/run/batch` endpoint accepts an array of `{ id, params }` objects, with the test name as the id:
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
8. Result Processing: Expands test results to all matching rows
9. Summary Generation: Creates intelligent summaries using LLM
10. Email Reply: Sends results with summary and attachments via Google Apps Script
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

All tests must be run in Docker containers as per development rules [[memory:7740249]]:

```bash
# Run all tests
docker compose --profile test run --rm test-runner
```

### BDQ API Integration Tests

The project includes comprehensive integration tests that verify the actual BDQ API is working correctly:

```bash
# Run the full integration test suite
docker compose --profile test run --rm test-runner python -m pytest tests/test_bdq_api_integration.py -v -s
```

These tests use the live BDQ API at `https://bdq-api-638241344017.europe-west1.run.app` to ensure the service is working as expected.

Test data files in `tests/data/`:
- `simple_occurrence_dwc.csv` - Basic occurrence core data
- `simple_taxon_dwc.csv` - Basic taxon core data  
- `prefixed_occurrence_dwc.csv` - Occurrence data with dwc: prefixes
- `occurrence.txt` - Large occurrence dataset for performance testing

## Background Processing

The service currently uses in-process background tasks for email processing:

- Current Implementation: The `/email/incoming` endpoint returns 200 immediately and processes emails asynchronously using `asyncio.create_task()`
- Processing Flow: Email processing runs in the background without blocking the HTTP response
- Error Handling: Errors during background processing are logged but don't affect the immediate HTTP response

Future Considerations: For production at scale, consider migrating to Cloud Tasks or Pub/Sub for more robust background processing with guaranteed delivery and retry mechanisms.
