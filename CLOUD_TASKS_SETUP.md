# Cloud Tasks Setup Guide

This guide walks you through setting up Cloud Tasks for reliable email processing that survives container shutdowns.

## Prerequisites

- Google Cloud Project with Cloud Tasks API enabled
- Cloud Run service deployed
- `gcloud` CLI installed and authenticated

## Step 1: Enable Cloud Tasks API

```bash
gcloud services enable cloudtasks.googleapis.com
```

## Step 2: Create Cloud Tasks Queue

Create a queue in the same region as your Cloud Run service:

```bash
# Replace with your project ID and region
PROJECT_ID="gen-lang-client-0895950745"
LOCATION="europe-west1"
QUEUE_NAME="email-processing-queue"

gcloud tasks queues create $QUEUE_NAME \
  --location=$LOCATION \
  --project=$PROJECT_ID \
  --max-attempts=3 \
  --max-retry-duration=3600s \
  --min-backoff=10s \
  --max-backoff=300s
```

**Queue Configuration Explained**:
- `max-attempts=3`: Retry up to 3 times if processing fails
- `max-retry-duration=3600s`: Maximum time to retry (1 hour)
- `min-backoff=10s`: Wait at least 10 seconds between retries
- `max-backoff=300s`: Wait at most 5 minutes between retries

## Step 3: Get Cloud Run Service URL

Get your Cloud Run service URL:

```bash
SERVICE_NAME="bdq-multirecord-agent"
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
  --region=$LOCATION \
  --format="value(status.url)")

echo "Service URL: $SERVICE_URL"
```

## Step 4: Grant Cloud Tasks Permission

Grant Cloud Tasks permission to invoke your Cloud Run service:

```bash
# Get the default compute service account
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
SERVICE_ACCOUNT="$PROJECT_NUMBER-compute@developer.gserviceaccount.com"

# Grant Cloud Tasks permission to invoke Cloud Run
gcloud run services add-iam-policy-binding $SERVICE_NAME \
  --member=serviceAccount:$SERVICE_ACCOUNT \
  --role=roles/run.invoker \
  --region=$LOCATION \
  --project=$PROJECT_ID
```

## Step 5: Set Environment Variables

Set the following environment variables in your Cloud Run service:

```bash
gcloud run services update $SERVICE_NAME \
  --region=$LOCATION \
  --update-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,CLOUD_TASKS_LOCATION=$LOCATION,CLOUD_TASKS_QUEUE_NAME=$QUEUE_NAME,CLOUD_RUN_SERVICE_URL=$SERVICE_URL"
```

Or set them individually:

```bash
gcloud run services update $SERVICE_NAME \
  --region=$LOCATION \
  --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID"

gcloud run services update $SERVICE_NAME \
  --region=$LOCATION \
  --set-env-vars="CLOUD_TASKS_LOCATION=$LOCATION"

gcloud run services update $SERVICE_NAME \
  --region=$LOCATION \
  --set-env-vars="CLOUD_TASKS_QUEUE_NAME=$QUEUE_NAME"

gcloud run services update $SERVICE_NAME \
  --region=$LOCATION \
  --set-env-vars="CLOUD_RUN_SERVICE_URL=$SERVICE_URL"
```

## Step 6: Deploy Updated Code

Deploy the updated code with Cloud Tasks support:

```bash
# Build and deploy (your CI/CD should handle this)
# Or manually:
gcloud run deploy $SERVICE_NAME \
  --source . \
  --region=$LOCATION \
  --project=$PROJECT_ID
```

## Step 7: Remove min-instances (Optional)

Once Cloud Tasks is working, you can remove `min-instances=1` to save costs:

```bash
gcloud run services update $SERVICE_NAME \
  --region=$LOCATION \
  --min-instances=0
```

## Step 8: Verify Setup

Test that Cloud Tasks is working:

1. Send a test email to your service
2. Check Cloud Run logs for "Created Cloud Task" message
3. Check Cloud Tasks queue in Console: https://console.cloud.google.com/cloudtasks
4. Verify email reply is sent

## Monitoring

### View Cloud Tasks Queue

```bash
gcloud tasks queues describe $QUEUE_NAME \
  --location=$LOCATION \
  --project=$PROJECT_ID
```

### View Tasks in Queue

```bash
gcloud tasks list \
  --queue=$QUEUE_NAME \
  --location=$LOCATION \
  --project=$PROJECT_ID
```

### View Cloud Run Logs

```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=$SERVICE_NAME" \
  --limit=50 \
  --format=json
```

## Troubleshooting

### Cloud Tasks not creating tasks

1. Check environment variables are set correctly
2. Verify Cloud Tasks API is enabled
3. Check Cloud Run logs for errors
4. Verify service account has `roles/run.invoker` permission

### Tasks failing

1. Check Cloud Run logs for processing errors
2. Verify `/tasks/process-email` endpoint is accessible
3. Check Cloud Tasks queue for failed tasks
4. Verify Cloud Run service URL is correct

### Fallback to async tasks

If Cloud Tasks is not configured, the service automatically falls back to `asyncio.create_task()`. Check logs for "falling back to async task" message.

## Cost Estimate

- **Cloud Tasks**: ~$0.50/month (first 1 million operations free, then $0.40 per million)
- **Cloud Run**: Pay only for processing time (scale-to-zero)
- **Total**: ~$0.50-2/month vs $15-30/month with min-instances=1

## Rollback

If you need to rollback to async tasks:

1. Remove Cloud Tasks environment variables:
```bash
gcloud run services update $SERVICE_NAME \
  --region=$LOCATION \
  --remove-env-vars="GOOGLE_CLOUD_PROJECT,CLOUD_TASKS_LOCATION,CLOUD_TASKS_QUEUE_NAME,CLOUD_RUN_SERVICE_URL"
```

2. Service will automatically fall back to `asyncio.create_task()`
