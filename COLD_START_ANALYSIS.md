# Cold Start Analysis for Cloud Run

## Current Flow

1. **Apps Script** (`bdq-email-forwarder.js`) polls Gmail every minute
2. When email found → POSTs to `/email/incoming` endpoint
3. **FastAPI** immediately returns 200 (non-blocking)
4. Background task (`_handle_email_processing`) runs asynchronously:
   - Extracts CSV
   - Calls BDQ API (localhost:8081) to run tests
   - Processes results
   - Sends email reply via Apps Script (`bdq-email-sender.js`)

## Problems Identified

### 1. **Cold Start Race Condition** ⚠️ CRITICAL
- **Issue**: When Cloud Run spins up a new instance:
  - Startup script starts Java BDQ API (10-30+ seconds)
  - Waits up to 60 seconds for BDQ API health check
  - Starts Python FastAPI
  - **Problem**: If Apps Script sends request while Python FastAPI is still starting, request fails
  
- **Impact**: First request after cold start will fail with 502/503

### 2. **Background Task Failure** ⚠️ HIGH
- **Issue**: Background task calls BDQ API. If BDQ API isn't ready yet or fails:
  - No retry mechanism
  - Email won't be sent
  - User gets no response
  
- **Impact**: Silent failures - user never receives email reply

### 3. **No Health Check for BDQ API** ⚠️ MEDIUM
- **Issue**: `/` endpoint doesn't verify BDQ API is ready
- **Impact**: Cloud Run might route traffic before BDQ API is ready

### 4. **Container Shutdown During Processing** ⚠️ MEDIUM
- **Issue**: If Cloud Run shuts down container before background processing completes:
  - Email won't be sent
  - No retry mechanism
  
- **Impact**: Lost emails if processing takes longer than container lifetime

## Solutions Implemented ✅

### ✅ Solution 1: Health Check Endpoint
- Added `/health` endpoint that verifies BDQ API is ready
- Returns 503 if BDQ API is not ready
- Cloud Run can use this for startup probe

### ✅ Solution 2: Retry Logic in BDQAPIService
- Added exponential backoff retry (3 attempts: 1s, 2s, 4s wait)
- Handles `ConnectionError` specifically (BDQ API not ready)
- Logs warnings during retries

### ✅ Solution 3: Error Handling in Background Task
- Wrapped BDQ API calls in try/except
- Sends error email to user if BDQ API fails
- Wrapped entire processing in try/except to catch unexpected errors

### ✅ Solution 4: Improved Startup Script
- Better logging during startup
- Proper cleanup if BDQ API fails to start

## Cloud Run Configuration Recommendations

### Option A: Use Startup Probe (RECOMMENDED)
Configure Cloud Run to use `/health` endpoint as startup probe:

```bash
gcloud run services update SERVICE_NAME \
  --startup-probe-type=http \
  --startup-probe-path=/health \
  --startup-probe-initial-delay-seconds=0 \
  --startup-probe-timeout-seconds=1 \
  --startup-probe-period-seconds=2 \
  --startup-probe-failure-threshold=30
```

This ensures Cloud Run doesn't route traffic until both services are ready.

### Option B: Keep Minimum Instances (Costs More)
Set `min-instances=1` to avoid cold starts entirely:

```bash
gcloud run services update SERVICE_NAME \
  --min-instances=1
```

### Option C: Increase Timeout
Ensure background processing has enough time:

```bash
gcloud run services update SERVICE_NAME \
  --timeout=3600
```

## Testing Recommendations

1. **Test cold start**: Deploy new version, wait for container to scale to zero, then send test email
2. **Monitor logs**: Check for retry warnings and error emails
3. **Verify health endpoint**: `curl https://your-service.run.app/health`

## Future Improvements

- **Cloud Tasks**: For production scale, migrate to Cloud Tasks/Pub/Sub for guaranteed delivery and retry
- **Warmup endpoint**: Add a `/warmup` endpoint that Cloud Scheduler can call periodically to keep instance warm
