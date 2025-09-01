# BDQ Email Report Service

A lightweight email-based service that runs Biodiversity Data Quality (BDQ) tests on incoming datasets and replies with results.

## Overview

This service receives dataset submissions via email, processes CSV files, runs BDQ tests, and replies with detailed results including validation failures and proposed amendments. It is currently deployed on google cloud run at https://bdq-multirecord-agent-638241344017.europe-west1.run.app/

## Architecture

- **Google Apps Script**: Polls Gmail inbox and forwards emails to this service, as well as providing an "endpoint" for email replies. Code for this is in this repo, in google-apps-scripts/
- **Google Cloud Run**: This FastAPI service, which processes datasets and runs BDQ tests for the entire dataset
- **BDQ API**: External REST API for running single BDQ tests

## Setup

### Environment Variables

```bash
# Gmail Send Webhook (Apps Script deployed as web app)
GMAIL_SEND=https://script.google.com/macros/s/YOUR_SCRIPT_ID/exec

# HMAC Secret for authenticating requests to Apps Script
HMAC_SECRET=your_long_random_secret

# Google API Key for Gemini LLM summaries
GOOGLE_API_KEY=your_google_api_key_here

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
7. **LLM Summary**: Generate intelligent, contextual email summaries using Google Gemini
8. **Email Reply**: Send results back to sender with attachments (using HMAC authentication)

More information about each of these is in the buildspec.md.

## CSV Requirements

- Must contain either `occurrenceID` or `taxonID` column
- CSV format with proper headers
- Supported delimiters: comma, semicolon, tab, pipe

## Output Files

- **bdq_raw_results.csv**: Detailed test results for each record
- **amended_dataset.csv**: Original dataset with proposed amendments applied

## LLM Integration

The service now uses Google Gemini to generate intelligent, contextual email summaries that:
- Analyze the user's original email context and subject
- Provide data quality scores and insights
- Explain issues in plain language for biologists
- Offer actionable advice for improving data quality
- Maintain a helpful, encouraging tone
- Fall back to basic summaries if the LLM is unavailable

## Error Handling

- Invalid CSV format
- Missing required columns
- No applicable tests found
- BDQ API errors
- Email sending failures

All errors are logged and error replies are sent to the original sender.
