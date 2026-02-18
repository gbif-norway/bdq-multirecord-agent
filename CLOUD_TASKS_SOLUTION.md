# Cloud Tasks/Pub/Sub Solution for Container Shutdown Problem

## Current Problem

**Issue**: When Cloud Run scales container to zero while background task is processing:
- Background task (`asyncio.create_task()`) is killed mid-execution
- Email reply is never sent
- User gets no response (silent failure)

**Why it happens**: 
- Cloud Run scales to zero after idle timeout (default: 15 minutes)
- Background tasks don't prevent container shutdown
- No retry mechanism if container dies

## Solution: Cloud Tasks

Cloud Tasks provides **guaranteed delivery** and **automatic retries** for long-running tasks.

### Architecture Overview

```
Apps Script → FastAPI → Cloud Tasks → Worker Service → Email Reply
              (200 OK)   (queued)     (processes)      (sent)
```

### How It Works

1. **Apps Script** sends email data to `/email/incoming`
2. **FastAPI** immediately returns 200 (non-blocking)
3. **FastAPI** creates a Cloud Task with the email data
4. **Cloud Tasks** queues the task (guaranteed delivery)
5. **Worker Service** (same container or separate) processes tasks
6. **Cloud Tasks** automatically retries if worker fails or container dies

### Benefits

✅ **Guaranteed delivery**: Tasks survive container shutdowns
✅ **Automatic retries**: Cloud Tasks retries failed tasks
✅ **No min-instances needed**: Containers can scale to zero
✅ **Cost effective**: Only pay when processing tasks
✅ **Reliable**: Tasks are persisted in Cloud Tasks service

## Implementation Options

### Option A: Cloud Tasks (RECOMMENDED)

**Best for**: Long-running tasks that need retries

**How it works**:
- FastAPI creates Cloud Task with email data payload
- Cloud Tasks calls worker endpoint (HTTP POST)
- Worker processes email and sends reply
- Cloud Tasks retries automatically on failure

**Pros**:
- Built-in retry logic
- HTTP-based (easy to integrate)
- Can set retry policies (max attempts, backoff)
- Task deduplication support

**Cons**:
- Requires Cloud Tasks API setup
- Slight complexity increase

### Option B: Pub/Sub

**Best for**: Event-driven architecture, multiple consumers

**How it works**:
- FastAPI publishes message to Pub/Sub topic
- Pub/Sub delivers to subscriber (worker service)
- Worker processes and sends reply
- Pub/Sub retries on failure

**Pros**:
- Decoupled architecture
- Can have multiple subscribers
- Good for event-driven workflows

**Cons**:
- More complex setup
- Overkill for single consumer
- Requires subscription management

## Recommended: Cloud Tasks Implementation

### Step 1: Create Cloud Tasks Queue

```bash
# Create queue in same region as Cloud Run
gcloud tasks queues create email-processing-queue \
  --location=europe-west1 \
  --max-attempts=3 \
  --max-retry-duration=3600s \
  --min-backoff=10s \
  --max-backoff=300s
```

### Step 2: Update FastAPI to Create Tasks

Instead of `asyncio.create_task()`, create Cloud Task:

```python
from google.cloud import tasks_v2

@app.post("/email/incoming")
async def process_incoming_email(request: Request):
    # ... validate request ...
    
    # Create Cloud Task instead of background task
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(PROJECT_ID, LOCATION, QUEUE_NAME)
    
    task = {
        'http_request': {
            'http_method': tasks_v2.HttpMethod.POST,
            'url': f'https://{SERVICE_URL}/tasks/process-email',
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(email_data).encode(),
        }
    }
    
    client.create_task(request={'parent': parent, 'task': task})
    
    return JSONResponse(status_code=200, content={"status": "queued"})
```

### Step 3: Create Worker Endpoint

```python
@app.post("/tasks/process-email")
async def process_email_task(request: Request):
    """Worker endpoint called by Cloud Tasks"""
    # Verify Cloud Tasks authentication (optional but recommended)
    # ... verify headers ...
    
    # Process email (same logic as before)
    email_data = await request.json()
    await _handle_email_processing(email_data)
    
    return JSONResponse(status_code=200, content={"status": "processed"})
```

### Step 4: Grant Cloud Tasks Permission

```bash
# Grant Cloud Tasks permission to invoke Cloud Run service
gcloud run services add-iam-policy-binding SERVICE_NAME \
  --member=serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com \
  --role=roles/run.invoker \
  --region=europe-west1
```

## Cost Comparison

### Current (min-instances=1)
- **Cost**: ~$15-30/month (always running)
- **Reliability**: High (no cold starts)
- **Efficiency**: Low (idle most of the time)

### Cloud Tasks (scale-to-zero)
- **Cost**: ~$0.50/month (Cloud Tasks) + processing time only
- **Reliability**: High (guaranteed delivery + retries)
- **Efficiency**: High (only pay when processing)

**Savings**: ~$15-30/month

## Migration Path

1. **Phase 1**: Add Cloud Tasks alongside current background tasks (dual mode)
2. **Phase 2**: Switch Apps Script to use Cloud Tasks endpoint
3. **Phase 3**: Remove `asyncio.create_task()` code
4. **Phase 4**: Remove `min-instances=1` setting

## Alternative: Simpler Solution (If Cloud Tasks is too complex)

If Cloud Tasks setup is too complex, consider:

1. **Increase timeout**: Set Cloud Run timeout to max (3600s)
2. **Keep-alive ping**: Background task sends periodic HTTP requests to keep container alive
3. **Accept risk**: For 1-2 emails/month, risk of lost email is low

But Cloud Tasks is the **proper solution** for production reliability.
