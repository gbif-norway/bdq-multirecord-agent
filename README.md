# BDQ Email Report Service

A lightweight email-based service that runs Biodiversity Data Quality (BDQ) tests on incoming datasets and replies with results.

This service receives dataset submissions via email, processes CSV files, runs BDQ tests, and replies with detailed results including validation failures and proposed amendments. To be deployed on Google Cloud Run.

### Service Flow

1. **Email Ingestion**
   - A Gmail account is set up solely for this service.
   - An Apps Script polls the inbox once per minute using the Gmail Advanced Service.
   - For each new message, it builds a JSON payload with headers, text/HTML body, and attachments, then posts it to this app's Cloud Run endpoint via `UrlFetchApp.fetch()`.
   - Messages that have been successfully forwarded are labeled `bdq/processed` to avoid duplicates.

2. **Dataset Processing**
   - This Cloud Run service receives the JSON payload and extracts the dataset file (CSV), saving it locally via `/email/incoming` endpoint
   - Immediately returns 200 so the Apps Script stops processing, Apps Script will label the message with `bdq/replied` in Gmail
   - If the file is not a CSV or there is no attachment, replies to the sender with a warning message and stops processing
   - Loads the core file into memory (detects delimiter, header). Determines core type by header presence:
     - Occurrence core if header contains `occurrenceID`
     - Taxon core if header contains `taxonID`
     - If there is no occurrenceID or taxonID header, replies to the sender notifying them that a CSV with a known ID header is required and stops processing

3. **Test Discovery**
   - Discovers tests from local TG2_tests.csv parsing, makes a list of tests to be applied
   - For each test, if all `actedUpon` columns exist in the CSV header, includes it
   - Splits into Validations and Amendments by the `type` field
   - Tests are loaded from the local TG2_tests.csv file and parsed into structured objects

4. **Test Execution**
   - Unique-value dedup per test: For each test, creates a set of **unique tuples** = values of its `actedUpon` columns across **all rows**
   - For each unique tuple, **executes the BDQ test locally** using Py4J integration. Caches the result by `(test_id, tuple)`
   - Results include status values: `RUN_HAS_RESULT`, `AMENDED`, `NOT_AMENDED`, `FILLED_IN`, `EXTERNAL_PREREQUISITES_NOT_MET`, `INTERNAL_PREREQUISITES_NOT_MET`, `AMBIGUOUS`
   - Result values: `COMPLIANT`, `NOT_COMPLIANT`, `POTENTIAL_ISSUE` / `NOT_ISSUE`
   - Maps cached results back to every row that has that same tuple. If there are no results, emails the user notifying them that they do not have any fields which can have BDQ tests run

5. **Result Generation**
   - CSV of Raw results: per row × per applicable test → `occurrenceID or taxonID`, `status`, `result`, `comment`, `amendment` (if any)
   - CSV of Amended dataset: applies proposed changes from Amendment results

6. **LLM Summary Generation**
   - Analyzes test results, user email, and dataset information
   - Generates intelligent, contextual email summaries using Google Gemini API
   - The LLM receives comprehensive context including:
     - Dataset type (Occurrence/Taxon core)
     - Test results with validation failures and amendments
     - User's original email content
     - Calculated data quality score
     - Field-specific issue categorization

7. **Email Reply**
   - Replies by email to the sender with:
     - LLM-generated intelligent summary (email body): Totals (records, tests run), per-field validation failure counts across all rows, examples/samples of common issues, note that the amended dataset applies proposed changes
     - Attaches Raw results csv and Amended dataset csv

### Email Reply Mechanism

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

## Architecture

All local development should be done in Docker containers.

### Architecture & Technology Stack

- **Google Apps Script**: Polls Gmail inbox and forwards emails to this service, and provides an endpoint for sending email replies. Code is in `google-apps-scripts/`. One script handles forwarding to /email/incoming (no auth or HMAC for this), another (deployed as a Web app) is used to send replies. Expected email volume is very low (~3 per week), so quotas are not a concern.
- **Google Cloud Run**: This app. Runs FastAPI service as a stateless HTTP app. Handles dataset processing and BDQ test execution for the entire dataset.
- **Py4J Gateway**: Local JVM process bdq-py4j-gateway (in the same Docker container) with resident FilteredPush BDQ libraries git submodules (geo_ref_qc, event_date_qc, sci_name_qc, rec_occur_qc), providing direct Java-Python integration. Py4J executes BDQ tests via direct Java method calls.
- **bdqtestrunner**: Official FilteredPush testing framework

## API Endpoints

- `GET /` - Health check
- `GET /health` - Detailed health check
- `POST /email/incoming` - Process incoming email with CSV attachment

