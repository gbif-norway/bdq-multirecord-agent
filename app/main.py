import os
import logging
from typing import Dict, Any
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi import BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn
from dotenv import load_dotenv
import base64
import asyncio

from app.services.email_service import EmailService
from app.services.bdq_cli_service import BDQCLIService
from app.services.csv_service import CSVService
from app.models.email_models import EmailPayload
from app.utils.logger import setup_logging, send_discord_notification

# Load environment variables
load_dotenv()

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

# Initialize services
email_service = EmailService()
bdq_service = BDQCLIService(skip_validation=True)  # Skip validation in test mode
csv_service = CSVService()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Service instance starting")
    try:
        send_discord_notification("Instance starting")
    except Exception:
        logger.warning("Failed to send Discord startup notification")
    # Test CLI connection at service start
    try:
        if bdq_service.test_connection():
            logger.info("BDQ CLI connection test successful")
        else:
            logger.warning("BDQ CLI connection test failed")
    except Exception as e:
        logger.error(f"Failed to test BDQ CLI connection: {e}")
    
    yield
    
    # Shutdown
    logger.info("Service instance shutting down")
    try:
        send_discord_notification("Instance shutting down")
    except Exception:
        logger.warning("Failed to send Discord shutdown notification")

app = FastAPI(
    title="BDQ Email Report Service",
    description="Service to process biodiversity data quality tests via email",
    version="1.0.0",
    lifespan=lifespan
)

# Backward compatibility functions for tests
async def on_startup():
    logger.info("Service instance starting")
    try:
        send_discord_notification("Instance starting")
    except Exception:
        logger.warning("Failed to send Discord startup notification")
    # Test CLI connection at service start
    try:
        if bdq_service.test_connection():
            logger.info("BDQ CLI connection test successful")
        else:
            logger.warning("BDQ CLI connection test failed")
    except Exception as e:
        logger.error(f"Failed to test BDQ CLI connection: {e}")


async def on_shutdown():
    logger.info("Service instance shutting down")
    try:
        send_discord_notification("Instance shutting down")
    except Exception:
        logger.warning("Failed to send Discord shutdown notification")

def _normalize_apps_script_payload(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    headers = raw_data.get('headers') or {}
    body = raw_data.get('body') or {}

    norm_atts = []
    for a in (raw_data.get('attachments') or []):
        content = a.get('contentBase64') or a.get('content_base64')
        if isinstance(content, list):
            try:
                b = bytes(int(x) for x in content)
                content_b64 = base64.b64encode(b).decode('utf-8')
            except Exception:
                content_b64 = ''
        elif isinstance(content, str):
            content_b64 = content
        else:
            content_b64 = ''

        norm_atts.append({
            'filename': a.get('filename', ''),
            'mime_type': a.get('mimeType') or a.get('mime_type') or '',
            'content_base64': content_b64,
            'size': a.get('size')
        })

    return {
        'message_id': raw_data.get('messageId') or raw_data.get('message_id') or '',
        'thread_id': raw_data.get('threadId') or raw_data.get('thread_id') or '',
        'from_email': headers.get('from', ''),
        'to_email': headers.get('to', ''),
        'subject': headers.get('subject', ''),
        'body_text': body.get('text') if isinstance(body, dict) else None,
        'body_html': body.get('html') if isinstance(body, dict) else None,
        'attachments': norm_atts,
        'headers': headers if isinstance(headers, dict) else {}
    }

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "BDQ Email Report Service is running"}

@app.get("/health")
async def health_check():
    """Detailed health check"""
    logger.info("Health check endpoint called")
    send_discord_notification("Testing - health check")
    
    # Check service health status
    services_status = {
        "email_service": "healthy",
        "bdq_service": "healthy", 
        "csv_service": "healthy"
    }
    
    # Probe CLI status if possible
    cli_ready = False
    try:
        cli_ready = bdq_service.test_connection()
    except Exception:
        cli_ready = False
    services_status.update({
        "bdq_cli_ready": cli_ready
    })

    return {
        "status": "healthy",
        "service": "BDQ Email Report Service",
        "version": "1.0.0",
        "services": services_status,
        "environment": {
            "gmail_send_configured": bool(os.getenv("GMAIL_SEND")),
            "hmac_secret_configured": bool(os.getenv("HMAC_SECRET")),
            "discord_webhook_configured": bool(os.getenv("DISCORD_WEBHOOK")),
            "google_api_key_configured": bool(os.getenv("GOOGLE_API_KEY"))
        }
    }

async def _handle_email_processing(email_data: EmailPayload):
    try:
        # Extract and validate CSV attachment
        csv_data = email_service.extract_csv_attachment(email_data)
        if not csv_data:
            await email_service.send_error_reply(
                email_data,
                "No CSV attachment found. Please attach a CSV file with biodiversity data."
            )
            return

        # Parse CSV and detect core type
        df, core_type = csv_service.parse_csv_and_detect_core(csv_data)
        if not core_type:
            await email_service.send_error_reply(
                email_data,
                "CSV must contain either 'occurrenceID' or 'taxonID' column to identify the core type."
            )
            return

        # Get available BDQ tests
        tests = bdq_service.get_available_tests()
        applicable_tests = bdq_service.filter_applicable_tests(tests, df.columns.tolist())

        if not applicable_tests:
            await email_service.send_error_reply(
                email_data,
                "No applicable BDQ tests found for the provided CSV columns."
            )
            return

        # Run BDQ tests
        test_results, skipped_tests = await bdq_service.run_tests_on_dataset(df, applicable_tests, core_type)

        # Generate result files
        raw_results_csv = csv_service.generate_raw_results_csv(test_results, core_type)
        amended_dataset_csv = csv_service.generate_amended_dataset(df, test_results, core_type)

        # Generate summary (include skipped tests)
        summary = bdq_service.generate_summary(test_results, len(df), skipped_tests)

        # Send reply email
        await email_service.send_results_reply(
            email_data,
            summary,
            raw_results_csv,
            amended_dataset_csv,
            test_results,
            core_type
        )

        logger.info(f"Successfully processed email from {email_data.from_email}")
    except Exception as e:
        logger.error(f"Error processing email in background: {str(e)}", exc_info=True)
        send_discord_notification(f"Processing error (background): {str(e)}")
        try:
            await email_service.send_error_reply(
                email_data,
                f"An error occurred while processing your request: {str(e)}"
            )
        except Exception as reply_error:
            logger.error(f"Failed to send error reply: {str(reply_error)}")


@app.post("/email/incoming")
async def process_incoming_email(request: Request, background_tasks: BackgroundTasks):
    """
    Accept incoming email and immediately return 200. Heavy processing runs in background.
    """
    # Log the raw request for debugging
    body = await request.body()
    logger.info(f"Received request with {len(body)} bytes")
    # Send Discord notification immediately for live updates
    send_discord_notification(f"Received email request: {len(body)} bytes")

    # Try to parse as JSON
    try:
        import json
        raw_data = json.loads(body.decode('utf-8'))
        logger.info(f"Parsed JSON data keys: {list(raw_data.keys())}")
    except Exception as parse_error:
        logger.error(f"Failed to parse JSON: {parse_error}")
        logger.error(f"Raw body (first 500 chars): {body[:500]}")
        # Send Discord notification immediately for live updates
        send_discord_notification(f"JSON parse error: {str(parse_error)}")
        # Still return 200 to avoid blocking the forwarder, but report error body
        return JSONResponse(status_code=200, content={"status": "error", "message": "Invalid JSON payload"})

    # Convert to EmailPayload model
    try:
        normalized = _normalize_apps_script_payload(raw_data)
        email_data = EmailPayload(**normalized)
        logger.info(f"Successfully parsed email from {email_data.from_email}")
    except Exception as model_error:
        logger.error(f"Failed to create EmailPayload model: {model_error}")
        logger.error(f"Raw data: {raw_data}")
        # Send Discord notification immediately for live updates
        send_discord_notification(f"Model validation error: {str(model_error)}")
        # Still return 200 to avoid blocking the forwarder
        return JSONResponse(status_code=200, content={"status": "error", "message": "Invalid email payload structure"})

    # Schedule background processing and return immediately
    # Use the running loop to schedule the async handler without blocking the response
    asyncio.create_task(_handle_email_processing(email_data))
    
    # Send Discord notification for successful queuing
    send_discord_notification(f"Email from {email_data.from_email} queued for processing")
    
    return JSONResponse(status_code=200, content={"status": "accepted", "message": "Email queued for processing"})


@app.get("/email/incoming")
async def reject_incoming_email_get(request: Request):
    """Explicitly reject GET requests to /email/incoming with 405 and alert."""
    client_ip = getattr(request.client, "host", "unknown")
    logger.warning(f"GET /email/incoming from {client_ip} - returning 405")
    try:
        send_discord_notification(f"Suspicious GET /email/incoming from {client_ip}")
    except Exception:
        logger.warning("Failed to send Discord notification for GET /email/incoming")
    return JSONResponse(status_code=405, content={"detail": "Method Not Allowed"})


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.error(f"HTTPException {exc.status_code}: {exc.detail}", exc_info=True)
    try:
        send_discord_notification(f"HTTPException {exc.status_code}: {exc.detail}")
    except Exception:
        logger.warning("Failed to send Discord notification for HTTPException")
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.error("Request validation error", exc_info=True)
    try:
        send_discord_notification("Request validation error on incoming request")
    except Exception:
        logger.warning("Failed to send Discord notification for validation error")
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    try:
        send_discord_notification(f"Unhandled exception: {exc}")
    except Exception:
        logger.warning("Failed to send Discord notification for unhandled exception")
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
