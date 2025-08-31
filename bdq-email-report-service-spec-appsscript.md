# BDQ Email Report Service

This document describes the design for a lightweight email-based service that runs Biodiversity Data Quality (BDQ) tests on incoming datasets and replies with results.

## Overview

A dedicated Gmail account receives dataset submissions by email. A Google Apps Script polls this mailbox every minute and forwards all new emails (including attachments) to the BDQ processing endpoint (running on Google Cloud Run). The service then processes the dataset, runs BDQ tests, and replies with the results.

## Flow

1. **Email ingestion**
   - A Gmail account is set up solely for this service.
   - An Apps Script polls the inbox once per minute using the Gmail Advanced Service.
   - For each new message, it builds a JSON payload with headers, text/HTML body, and attachments, then posts it to this app's Cloud Run endpoint via `UrlFetchApp.fetch()`.
   - Messages that have been successfully forwarded are labeled `bdq/processed` to avoid duplicates.

2. **Run BDQ tests on data**
   a) This Cloud Run service should receive the JSON payload and extracts the dataset file (CSV), saving it locally. Make an /email/incoming endpoint
   b) Immediately return 200 so the Apps Script stops processing, Apps Script will label the message with `bdq/replied` in Gmail. If the file is not a CSV or there is no attachment it should reply to the sender with a warning message and stop processing.
   c) Load the core file into memory** (detect delimiter, header). Determine core type by header presence:
      - Occurrence core if header contains `occurrenceID`.
      - Taxon core if header contains `taxonID`.
      If there is no occurrenceID or taxonID header, reply to the sender notifying them that a CSV with a known ID header is required and stop processing.
   c) Discover tests from `GET https://bdq-api-638241344017.europe-west1.run.app/api/v1/tests` (just hardcode this for the moment), make a list of tests to be applied
      - For each test, if all `actedUpon` columns exist in the CSV header, include it.
      - Split into Validations and Amendments by the `type` field
      - e.g. of 2 results:
      ```
         {
         id: "AMENDMENT_BASISOFRECORD_STANDARDIZED",
         guid: "07c28ace-561a-476e-a9b9-3d5ad6e35933",
         type: "Amendment",
         className: "org.filteredpush.qc.metadata.DwCMetadataDQDefaults",
         methodName: "amendmentBasisofrecordStandardized",
         actedUpon: [
         "dwc:basisOfRecord"
         ],
         consulted: [ ],
         parameters: [ ]
         },
         {
         id: "AMENDMENT_COORDINATES_CONVERTED",
         guid: "620749b9-7d9c-4890-97d2-be3d1cde6da8",
         type: "Amendment",
         className: "org.filteredpush.qc.georeference.DwCGeoRefDQ",
         methodName: "amendmentCoordinatesConverted",
         actedUpon: [
         "dwc:decimalLatitude",
         "dwc:decimalLongitude",
         "dwc:coordinateUncertaintyInMeters",
         "dwc:geodeticDatum",
         "dwc:coordinatePrecision"
         ],
         consulted: [ ],
         parameters: [ ]
         }, ...
      ```
   d) Unique-value dedup per test:
      - For each test, create a set of **unique tuples** = values of its `actedUpon` columns across **all rows**.
      - For each unique tuple, **call `https://bdq-api-638241344017.europe-west1.run.app/api/v1/tests/run`** once. Cache the result by `(test_id, tuple)`.
         - e.g. result: 
         ```
         rukayasj@Mac:~/Projects/bdq-api% curl -s -X POST "https://bdq-api-638241344017.europe-west1.run.app/api/v1/tests/run" \
         -H "Content-Type: application/json" \
         -d '{
            "id": "VALIDATION_COORDINATESCOUNTRYCODE_CONSISTENT",
            "params": {
               "dwc:decimalLatitude": "51.5074",
               "dwc:decimalLongitude": "-0.1278",
               "dwc:countryCode": "GB"
            }
         }' | jq .
         {
         "status": "RUN_HAS_RESULT",
         "result": "COMPLIANT",
         "comment": "Provided coordinate lies within the bounds of the country specified by the country code."
         }
         <,\n      "dwc:countryCode": "US"\n    }\n  }' | jq .
         {
         "status": "RUN_HAS_RESULT",
         "result": "COMPLIANT",
         "comment": "Provided coordinate lies within the bounds of the country specified by the country code."
         }
         ```
         Possible Status values:
         - `RUN_HAS_RESULT` - Completed run with a result
         - `AMENDED` - Proposed standardized/corrected value
         - `NOT_AMENDED` - No unambiguous amendment proposed
         - `FILLED_IN` - Populated a missing value
         - `EXTERNAL_PREREQUISITES_NOT_MET` - External service unavailable
         - `INTERNAL_PREREQUISITES_NOT_MET` - Input missing/invalid for the test
         - `AMBIGUOUS` - Inputs produce ambiguous outcome (no amendment)

         Possible Result values:
         - `COMPLIANT` - Passes validation
         - `NOT_COMPLIANT` - Fails validation
         - `POTENTIAL_ISSUE` / `NOT_ISSUE` - Issue signal (for Issue-type tests)
   e) Map cached results back to every row that has that same tuple. If there are no results, email the user notifying them that they do not have any fields which can have BDQ tests run, something like that, then stop processing.

3. **Build result files**:
   - CSV of Raw results: per row × per applicable test → `occurrenceID or taxonID`, `status`, `result`, `comment`, `amendment` (if any).
   - CSV of Amended dataset : apply proposed changes from Amendment results.

4. **Reply by email** to the sender with:
   - Summary (email body):
     - Totals (records, tests run)
     - Per-field validation failure counts across all rows (a failure is NOT_COMPLIANT)
     - Examples/samples of common issues
     - Note that the amended dataset applies proposed changes.
   - Attach Raw results csv and Amended dataset csv
   - Uses the Gmail API (`messages.send`) to reply in-thread to the sender (Query - is this actually possible?)
   - The reply email includes:
     - A summary report in the email body
     - Raw BDQ results attached
     - Amended dataset attached

## Email reply mechanism

- Another Apps Script deployed as a Web app acts as a "send mail" webhook, this avoids oauth and periodic reauth
- Call example: 
   ```
   curl -X POST "https://script.google.com/macros/s/AKfycb.../exec" \
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

## Stack

- **Apps Script**: Polls Gmail, forwards new mail to Cloud Run. Separate Apps script deployed as a Web app sends email replies.
- **Google Cloud Run**: (this app) Stateless HTTP service to run BDQ tests and do processing.
- **BDQ API**: Existing REST API wrapper for FilteredPush BDQ libraries.

## Apps Script Notes

- The Apps Script must complete within 6 minutes. It only forwards messages, so it typically finishes in seconds.
- Heavy processing (BDQ tests, file handling, reply composition) happens in Cloud Run, not Apps Script.
- No HMAC or authentication is used in this sandbox test. 
- Expected email volume is very low (~3 per week), so quotas are not a concern.
