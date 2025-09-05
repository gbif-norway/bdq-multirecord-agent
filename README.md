# BDQ Email Report Service

A lightweight email-based service that runs Biodiversity Data Quality (BDQ) tests on incoming datasets and replies with results.

This service receives dataset submissions via email, processes CSV files, runs BDQ tests, and replies with detailed results including validation failures and proposed amendments. To be deployed on Google Cloud Run.

All local development should be done in Docker containers.

## Service Flow

1. **Email Reception**: FastAPI receives JSON payload to  from Google Apps Script
2. **Immediate Response**: Returns 200 status immediately for Apps Script acknowledgment
3. **CSV Extraction**: Extracts and validates CSV attachments
4. **Core Detection**: Identifies occurrence vs taxon core based on column presence
5. **Test Discovery**: Finds applicable BDQ tests from TG2_tests.csv
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
