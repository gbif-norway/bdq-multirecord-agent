# BDQ Email Report Service

A lightweight email-based service that runs Biodiversity Data Quality (BDQ) tests on incoming datasets and replies with results.

## Overview

This service receives dataset submissions via email, processes CSV files, runs BDQ tests, and replies with detailed results including validation failures and proposed amendments. It is currently deployed on Google Cloud Run at https://bdq-multirecord-agent-638241344017.europe-west1.run.app/

A recent refactor replaced the CLI subprocess approach with fast Py4J integration for direct Java-Python communication. This is a major refactor and has left bugs and problems in the code. We did this because using the CLI was slow, although possibly this was due to poor design in the CLI. 

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

6. **Email Reply**
   - Replies by email to the sender with:
     - Summary (email body): Totals (records, tests run), per-field validation failure counts across all rows, examples/samples of common issues, note that the amended dataset applies proposed changes
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

- **Google Apps Script**: Polls Gmail inbox and forwards emails to this service, as well as providing an "endpoint" for email replies. Code for this is in this repo, in google-apps-scripts/
- **Google Cloud Run**: This FastAPI service, which processes datasets and runs BDQ tests for the entire dataset
- **Py4J Gateway**: Local JVM process with resident FilteredPush BDQ libraries, providing direct Java-Python integration
  - Py4J gateway executes BDQ tests locally using direct method calls
  - Test mappings from official TDWG BDQ specification (via git submodule) for comprehensive BDQ test coverage
  - Significantly faster than HTTP-based external API calls or subprocess approaches

### Technology Stack

- **Apps Script**: Polls Gmail, forwards new mail to Cloud Run. Separate Apps script deployed as a Web app sends email replies.
- **Google Cloud Run**: (this app) Stateless HTTP service with Py4J gateway for inline BDQ test execution.
- **Inline BDQ Libraries**: FilteredPush BDQ libraries (geo_ref_qc, event_date_qc, sci_name_qc, rec_occur_qc) run locally in the same container.
- **bdqtestrunner**: Official FilteredPush testing framework integrated for standards compliance.

### Apps Script Notes

- The Apps Script must complete within 6 minutes. It only forwards messages, so it typically finishes in seconds.
- Heavy processing (BDQ tests, file handling, reply composition) happens in Cloud Run, not Apps Script.
- No HMAC or authentication is used in this sandbox test. 
- Expected email volume is very low (~3 per week), so quotas are not a concern.

### Debugging

- Send debugging messages to {DISCORD_WEBHOOK}
  - Service sends Discord notifications for lifecycle events (startup/shutdown), unexpected GET probes to `/email/incoming`, uncaught exceptions, and persistent BDQ API failures after retries.

### Reliability

- All unhandled exceptions are captured by FastAPI exception handlers, logged with stack traces, and notified to Discord.

## Setup

## API Endpoints

- `GET /` - Health check
- `GET /health` - Detailed health check
- `POST /email/incoming` - Process incoming email with CSV attachment

## Email Processing Flow

1. **Email Ingestion**: Apps Script forwards emails to `/email/incoming`
2. **CSV Processing**: Extract and parse CSV attachment
3. **Core Detection**: Identify occurrence or taxon core type
4. **Test Discovery**: Load applicable BDQ tests from TDWG BDQ specification (bdq-spec submodule)
5. **CLI Execution**: Execute BDQ tests via Java CLI with JSON input/output files
6. **Test Execution**: Run tests locally via CLI with proper error handling and result processing
7. **Result Generation**: Create raw results and amended dataset CSVs
8. **LLM Summary**: Generate intelligent, contextual email summaries using Google Gemini
9. **Email Reply**: Send results back to sender with attachments (using HMAC authentication)


## Inline BDQ Implementation

This service now includes the FilteredPush BDQ libraries directly, running in a local JVM CLI instead of calling external APIs.

### Architecture
- **Local JVM CLI**: Java command-line application with BDQ libraries loaded
- **File-based Communication**: JSON input/output files for simple, reliable execution
- **Test Mapping**: Official TDWG BDQ specification drives the mapping from test IDs to Java class/method implementations
- **Stateless Execution**: Each request spawns a new CLI process for isolation
- **Subprocess Management**: Python manages CLI execution with proper error handling


### How LLM Summaries Work

1. **Email received** → CSV processed → BDQ tests run
2. **LLM Context Preparation** → Analyzes test results, user email, dataset info
3. **Gemini API Call** → Generates intelligent summary
4. **Email Reply** → Sends LLM-generated summary + attachments

The LLM receives comprehensive context including:
- Dataset type (Occurrence/Taxon core)
- Test results with validation failures and amendments
- User's original email content
- Calculated data quality score
- Field-specific issue categorization

### Fallback Behavior

If the LLM service is unavailable:
- Automatically falls back to basic summary generation
- No interruption to email processing
- Logs warnings for monitoring
- Service continues to work normally
