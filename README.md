# BDQ Email Report Service

A lightweight email-based service that runs Biodiversity Data Quality (BDQ) tests on incoming datasets and replies with results.

## Overview

This service receives dataset submissions via email, processes CSV files, runs BDQ tests, and replies with detailed results including validation failures and proposed amendments. It is currently deployed on Google Cloud Run at https://bdq-multirecord-agent-638241344017.europe-west1.run.app/

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
   - Note: `GET /email/incoming` explicitly returns 405 and sends a Discord alert to surface unsolicited probes
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

## Recent Major Refactor (2024-2025)

**Py4J Architecture Migration**: Replaced the slow CLI subprocess approach with fast Py4J integration for direct Java-Python communication. This eliminates the 30-40 second startup times and timeout issues that were plaguing the Cloud Run deployment.

### Key Improvements
- **Fast Startup**: Py4J gateway starts in ~2 seconds vs 30-40 seconds for CLI
- **No Timeouts**: Direct Java-Python communication eliminates subprocess overhead
- **Cloud Run Optimized**: Single process architecture perfect for serverless environments
- **Reliable Execution**: No file I/O bottlenecks or process management issues
- **Better Error Handling**: Direct exception propagation from Java to Python
- **Modernized FastAPI**: Upgraded to modern `lifespan` context manager
- **Comprehensive Testing**: Full integration test coverage with Py4J architecture

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
- BDQ API calls use exponential backoff (1s → 2s → 4s → 8s) up to 4 attempts per parameter set; persistent failures raise a single Discord alert per test.

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
4. **Test Discovery**: Load applicable BDQ tests from TDWG BDQ specification (bdq-spec submodule)
5. **CLI Execution**: Execute BDQ tests via Java CLI with JSON input/output files
6. **Test Execution**: Run tests locally via CLI with proper error handling and result processing
7. **Result Generation**: Create raw results and amended dataset CSVs
8. **LLM Summary**: Generate intelligent, contextual email summaries using Google Gemini
9. **Email Reply**: Send results back to sender with attachments (using HMAC authentication)

More information about each of these is in the buildspec.md.

## Inline BDQ Implementation

This service now includes the FilteredPush BDQ libraries directly, running in a local JVM CLI instead of calling external APIs:

### Architecture
- **Local JVM CLI**: Java command-line application with BDQ libraries loaded
- **File-based Communication**: JSON input/output files for simple, reliable execution
- **Test Mapping**: Official TDWG BDQ specification drives the mapping from test IDs to Java class/method implementations
- **Stateless Execution**: Each request spawns a new CLI process for isolation
- **Subprocess Management**: Python manages CLI execution with proper error handling

### Benefits
- **Simplicity**: Eliminates socket server complexity, easier to debug and maintain
- **Reliability**: No persistent process management, each execution is isolated
- **Consistency**: Uses official FilteredPush implementations directly
- **Deployability**: Much simpler Docker build that actually works in Cloud Run

### Supported Libraries
- **geo_ref_qc**: Georeference quality control (from rukayaj/geo_ref_qc fork)
- **event_date_qc**: Event date quality control (v3.1.1-SNAPSHOT)
- **sci_name_qc**: Scientific name quality control (v1.2.1-SNAPSHOT)
- **rec_occur_qc**: Record/occurrence metadata quality control (v1.1.1-SNAPSHOT)

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

## Build Process

### Py4J Gateway Build Strategy

This project uses a **Docker-based build strategy** for the Py4J Gateway JAR. The JAR is built during the Docker image build process, not committed to the repository.

#### Why Docker Build?

✅ **Reproducible builds** - Same Maven/Java version every time  
✅ **Platform independent** - Works on any development machine  
✅ **Security** - No pre-built binaries in repository  
✅ **Clean repository** - No large binary files tracked by git  
✅ **CI/CD ready** - Build process is fully containerized  

#### JAR Size (~100MB)

The Py4J Gateway JAR is large because it's a "fat JAR" containing all dependencies:
- FilteredPush BDQ libraries (geo_ref_qc, sci_name_qc, event_date_qc, rec_occur_qc)
- Py4J library for Python-Java integration
- Transitive dependencies (Jackson, SLF4J, etc.)
- This is **expected and normal** for a self-contained gateway

#### Build Process

**Docker Build (Production)**
```bash
# Build Docker image - JAR is built automatically
docker build -t bdq-service .

# JAR location in container: /opt/bdq/bdq-py4j-gateway.jar
```

**Local Development Build (Optional)**
```bash
# Build JAR locally for testing
cd java
mvn clean package -DskipTests

# JAR output: bdq-py4j-gateway/target/bdq-py4j-gateway-1.0.0.jar
```

#### Important Rules

❌ **NEVER commit JAR files** - They are ignored by .gitignore  
❌ **NEVER commit target/ directories** - Maven build artifacts are ignored  
✅ **Always build through Docker for production**  
✅ **Local builds are for development/testing only**  

#### Docker Multi-Stage Build

The Dockerfile uses a multi-stage build:

1. **Build Stage** (`maven:3.9-eclipse-temurin-17`)
   - Copies Java source code
   - Builds BDQ libraries first
   - Builds Py4J Gateway JAR with all dependencies

2. **Runtime Stage** (`python:3.11-slim`)  
   - Installs OpenJDK 21 JRE
   - Copies only the Py4J Gateway JAR from build stage
   - Copies Python application code
   - Sets up runtime environment

#### Development Workflow

1. **Make changes** to Java or Python code
2. **Test locally** with `mvn clean package` if needed
3. **Build Docker image** to test full integration
4. **Commit source changes** only (JAR files are automatically ignored)
5. **Deploy via Docker** - JAR is built fresh each time

## Development

### Local Development Setup
All development should be done in Docker containers for consistency.

#### First-time Setup
```bash
# Initialize git submodules to get BDQ test specifications
git submodule update --init --recursive

# Build and run tests
docker compose -f docker-compose.test.yml run --rm test-runner

# Run specific test categories
docker compose -f docker-compose.test.yml run --rm test-runner python -m pytest tests/ -k "integration"

# Run with coverage
docker compose -f docker-compose.test.yml run --rm test-runner-coverage
```

#### BDQ Test Specification Updates
The project uses the official TDWG BDQ test specification via git submodule:
```bash
# Update to latest BDQ tests (when TDWG releases updates)
git submodule update --remote bdq-spec

# Pin to specific BDQ specification version
cd bdq-spec && git checkout <commit-hash> && cd ..
git add bdq-spec && git commit -m "Pin BDQ spec to version X"
```

### Test Suite

The project maintains a comprehensive test suite with **99.2% pass rate (121/122 tests)**:

- **Unit Tests**: Individual service and component testing
- **Integration Tests**: End-to-end CLI workflow testing
- **Edge Case Coverage**: CSV parsing variations, error scenarios, async patterns
- **Mock Testing**: External service dependencies with proper async mocking

#### Test Categories
- `tests/test_*_service.py` - Service layer unit tests
- `tests/test_main.py` - FastAPI application and endpoint tests  
- `tests/test_cli_integration.py` - Full CLI workflow integration tests
- `tests/test_tg2_parser.py` - TG2 CSV parsing and test mapping tests

#### Running Tests
```bash
# All tests
docker compose -f docker-compose.test.yml run --rm test-runner

# Specific test file
docker compose -f docker-compose.test.yml run --rm test-runner python -m pytest tests/test_main.py -v

# Integration tests only
docker compose -f docker-compose.test.yml run --rm test-runner python -m pytest tests/test_cli_integration.py -v
```

### Code Quality

The codebase follows modern Python and FastAPI patterns:

- **Type Hints**: Comprehensive type annotations throughout
- **Async/Await**: Proper async patterns for I/O operations
- **Dependency Injection**: Clean service architecture
- **Error Handling**: Comprehensive exception handling with graceful degradation
- **Logging**: Structured logging with Discord notifications
- **Testing**: High test coverage with realistic mocking

### Architecture Patterns

- **Service Layer**: Separate services for email, CSV, BDQ CLI, and LLM operations
- **Model Layer**: Pydantic models for type safety and validation
- **Background Processing**: Async email processing to avoid request timeouts
- **Graceful Degradation**: LLM failures don't break the core workflow
- **Stateless Design**: Each request is independent, no persistent state

## Error Handling

The service implements comprehensive error handling:

- **Invalid CSV format** - Detailed error messages with suggested fixes
- **Missing required columns** - Clear indication of required fields
- **No applicable tests found** - Explanation of supported data types
- **CLI execution failures** - Timeout handling and retry logic
- **Email sending failures** - Fallback error notifications
- **LLM API errors** - Graceful degradation to basic summaries

All errors are logged with structured information and error replies are sent to the original sender. Critical errors trigger Discord notifications for monitoring.

### Error Response Format
```json
{
  "status": "error",
  "message": "Detailed error description",
  "suggestions": ["Actionable fix suggestions"]
}
```
