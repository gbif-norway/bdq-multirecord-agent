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
from app.services.bdq_py4j_service import BDQPy4JService
from app.services.csv_service import CSVService
from app.models.email_models import EmailPayload
from app.utils.logger import setup_logging, send_discord_notification

# Load environment variables
load_dotenv()

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

email_service = EmailService()
bdq_service = BDQPy4JService()
csv_service = CSVService()
test_mapper = TG2TestMapper(bdq_service)


app = FastAPI(
    title="BDQ Email Report Service",
    description="Service to process biodiversity data quality tests via email",
    version="1.0.0"
)

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

    return {
        "status": "healthy",
        "service": "BDQ Email Report Service",
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

        # Get applicable BDQ tests
        applicable_tests = test_mapper.get_applicable_tests_for_dataset(df.columns.tolist())

        if not applicable_tests:
            send_discord_notification(f"❗ No applicable BDQ tests found for provided CSV columns: {df.columns.tolist()}")
            await email_service.send_error_reply(
                email_data,
                "No applicable BDQ tests found for the provided CSV columns."
            )
            return

        # TODO For test in applicable_tests:
        # Get all unique value combinations in acted_upon + consulted (use helper.py _get_unique_tuples)
        # Make a list of test results
        # For each unique value combinations:
        #   Run test using bdq_service.executeSingleTuple (but change name to executeSingleTest)
        #   Run helper _expand_single_test_results_to_all_rows, and add the returned row results to the list of test results
        # After this, use pandas to get some summaries of the status, result, comment, label, acted_upon and consulteds - 
        # what we want is actually the unique value combinations and just add counts of how common they were
        # Send this + the original email address, subject and body on to llm_service.py to generate an email summary reply (just email body).
        
        send_discord_notification(f"📊 Generating result files with {len(test_results)} test results...")
        raw_results_csv = csv_service.generate_raw_results_csv(test_results, core_type)
        amended_dataset_csv = csv_service.generate_amended_dataset(df, test_results, core_type)

        send_discord_notification(f"Sending reply email...")
        await email_service.send_results_reply(
            email_data,
            summary,
            raw_results_csv,
            amended_dataset_csv,
            test_results,
            core_type
        )
        send_discord_notification(f"✅ Email sent successfully to {email_data.from_email}!")

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
