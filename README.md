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

### LLM Setup

#### 1. Get a Google API Key
1. Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Sign in with your Google account
3. Click "Create API Key"
4. Copy the generated key

#### 2. Set Environment Variable
Add to your Google Cloud Run service:
```bash
GOOGLE_API_KEY=your_google_api_key_here
```

**In Google Cloud Console:**
1. Go to Cloud Run → Select `bdq-multirecord-agent`
2. Click "EDIT & DEPLOY NEW REVISION"
3. In "Variables & Secrets" section, add:
   - **Variable name**: `GOOGLE_API_KEY`
   - **Value**: Your Google API key

#### 3. API Quotas and Limits
- **Free tier**: 15 requests per minute, 1500 requests per day
- **Paid tier**: $0.50 per 1M characters input, $1.50 per 1M characters output
- **Rate limits**: 60 requests per minute for paid accounts

For typical usage (3 emails per week), the free tier should be sufficient.

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

### Monitoring and Debugging

#### Logs to Watch
```bash
# Check Cloud Run logs
gcloud logs read --service=bdq-multirecord-agent --limit=50

# Look for LLM-related messages:
# - "LLM service disabled" → API key missing
# - "Error generating LLM summary" → API issues
# - "Falling back to basic summary" → LLM unavailable
# General operational messages to note:
# - "Instance starting/shutting down" → lifecycle events (also sent to Discord)
# - "GET /email/incoming ... returning 405" → unsolicited probes blocked (Discord alert)
# - "BDQ API call failed ... after N attempts" → upstream BDQ issues (Discord alert)
```

#### Health Check
The `/health` endpoint shows service status and environment variable configuration.

### Troubleshooting

**Common Issues:**
1. **"LLM service disabled"** → Check `GOOGLE_API_KEY` environment variable
2. **"Error generating LLM summary"** → Check API quotas, network connectivity, API key permissions
3. **Fallback to basic summary** → LLM service unavailable, check logs for specific errors
4. **Container restarted during processing** → Now captured by global exception handlers. Check logs for "Unhandled exception" and Discord alerts. BDQ API calls have retries with exponential backoff.

**Support:**
- **Google AI Studio**: [makersuite.google.com](https://makersuite.google.com)
- **Cloud Run Logs**: Use `gcloud logs` command
- **Discord**: Check webhook notifications

### Security Considerations

1. **API Key Security**
   - Never commit API keys to version control
   - Use environment variables in Cloud Run
   - Rotate keys periodically

2. **Data Privacy**
   - LLM prompts include user email content
   - No data is stored by Google beyond the API call
   - Consider data sensitivity in prompts

3. **Rate Limiting**
   - Monitor API usage
   - Implement backoff for failed requests
   - Graceful degradation to basic summaries

## Error Handling

- Invalid CSV format
- Missing required columns
- No applicable tests found
- BDQ API errors
- Email sending failures

All errors are logged and error replies are sent to the original sender.
