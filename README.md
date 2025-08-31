# BDQ Email Report Service

A lightweight email-based service that runs Biodiversity Data Quality (BDQ) tests on incoming datasets and replies with results.

## Overview

This service receives dataset submissions via email, processes CSV files, runs BDQ tests, and replies with detailed results including validation failures and proposed amendments.

## Architecture

- **Google Apps Script**: Polls Gmail inbox and forwards emails to this service
- **Google Cloud Run**: This FastAPI service processes datasets and runs BDQ tests
- **BDQ API**: External REST API for running biodiversity data quality tests

## Setup

### Environment Variables

```bash
# Gmail Send Webhook (Apps Script deployed as web app)
GMAIL_SEND=https://script.google.com/macros/s/YOUR_SCRIPT_ID/exec

# HMAC Secret for authenticating requests to Apps Script
HMAC_SECRET=your_long_random_secret

# Discord Webhook for debugging notifications
DISCORD_WEBHOOK=https://discord.com/api/webhooks/YOUR_WEBHOOK_URL

# Logging level (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL=INFO

# Port for the service (default: 8080)
PORT=8080
```

## API Endpoints

- `GET /` - Health check
- `GET /health` - Detailed health check
- `POST /email/incoming` - Process incoming email with CSV attachment

## Email Processing Flow

1. **Email Ingestion**: Apps Script forwards emails to `/email/incoming`
2. **CSV Processing**: Extract and parse CSV attachment
3. **Core Detection**: Identify occurrence or taxon core type
4. **Test Discovery**: Fetch applicable BDQ tests from API
5. **Test Execution**: Run tests with unique value deduplication
6. **Result Generation**: Create raw results and amended dataset CSVs
7. **Email Reply**: Send results back to sender with attachments (using HMAC authentication)

## CSV Requirements

- Must contain either `occurrenceID` or `taxonID` column
- CSV format with proper headers
- Supported delimiters: comma, semicolon, tab, pipe

## Output Files

- **bdq_raw_results.csv**: Detailed test results for each record
- **amended_dataset.csv**: Original dataset with proposed amendments applied

## Error Handling

- Invalid CSV format
- Missing required columns
- No applicable tests found
- BDQ API errors
- Email sending failures

All errors are logged and error replies are sent to the original sender.
