import os
import logging
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import uvicorn
from dotenv import load_dotenv

from services.email_service import EmailService
from services.bdq_service import BDQService
from services.csv_service import CSVService
from models.email_models import EmailPayload, EmailReply
from utils.logger import setup_logging, send_discord_notification

# Load environment variables
load_dotenv()

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="BDQ Email Report Service",
    description="Service to process biodiversity data quality tests via email",
    version="1.0.0"
)

# Initialize services (optional - allow app to start even if services fail)
logger.info("Initializing services...")
print("STDOUT: Initializing services...")
email_service = None
bdq_service = None
csv_service = None

# Initialize services with better error handling
try:
    from services.email_service import EmailService
    email_service = EmailService()
    logger.info("Email service initialized")
    print("STDOUT: Email service initialized")
except Exception as e:
    logger.warning(f"Failed to initialize email service: {e}")
    print(f"STDOUT: Failed to initialize email service: {e}")

try:
    from services.bdq_service import BDQService
    bdq_service = BDQService()
    logger.info("BDQ service initialized")
    print("STDOUT: BDQ service initialized")
except Exception as e:
    logger.warning(f"Failed to initialize BDQ service: {e}")
    print(f"STDOUT: Failed to initialize BDQ service: {e}")

try:
    from services.csv_service import CSVService
    csv_service = CSVService()
    logger.info("CSV service initialized")
    print("STDOUT: CSV service initialized")
except Exception as e:
    logger.warning(f"Failed to initialize CSV service: {e}")
    print(f"STDOUT: Failed to initialize CSV service: {e}")

logger.info("Service initialization complete")
print("STDOUT: Service initialization complete")

@app.get("/")
async def root():
    """Root endpoint - basic health check"""
    logger.info("Root endpoint called")
    print("STDOUT: Root endpoint called")
    return {
        "message": "BDQ Email Report Service is running",
        "status": "ok",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    """Detailed health check"""
    logger.info("Health check endpoint called")
    print("STDOUT: Health check endpoint called")
    
    # Check service status
    services_status = {
        "email_service": email_service is not None,
        "bdq_service": bdq_service is not None,
        "csv_service": csv_service is not None
    }
    
    # Determine overall health
    all_services_healthy = all(services_status.values())
    overall_status = "healthy" if all_services_healthy else "degraded"
    
    health_info = {
        "status": overall_status,
        "service": "BDQ Email Report Service",
        "version": "1.0.0",
        "services": services_status,
        "environment": {
            "gmail_send_configured": bool(os.getenv("GMAIL_SEND")),
            "hmac_secret_configured": bool(os.getenv("HMAC_SECRET")),
            "discord_webhook_configured": bool(os.getenv("DISCORD_WEBHOOK"))
        }
    }
    
    # Send Discord notification for debugging
    try:
        send_discord_notification(f"Health check: {overall_status}")
    except Exception as e:
        logger.warning(f"Failed to send Discord notification: {e}")
    
    return health_info

@app.get("/ready")
async def readiness_check():
    """Readiness probe for Cloud Run"""
    logger.info("Readiness check called")
    print("STDOUT: Readiness check called")
    return {"status": "ready"}

@app.get("/test")
async def test_endpoint():
    """Simple test endpoint that doesn't require services"""
    logger.info("Test endpoint called")
    print("STDOUT: Test endpoint called")
    return {"message": "Test endpoint working", "timestamp": "2025-08-31T18:57:57Z"}

@app.on_event("startup")
async def startup_event():
    """Log startup event"""
    logger.info("BDQ Email Report Service is starting up")
    print("STDOUT: BDQ Email Report Service is starting up")
    
    # Log environment info
    port = os.getenv("PORT", "8080")
    logger.info(f"Service starting on port {port}")
    print(f"STDOUT: Service starting on port {port}")
    
    # Test that the app is working
    logger.info("FastAPI app is ready to serve requests")
    print("STDOUT: FastAPI app is ready to serve requests")

@app.post("/email/incoming")
async def process_incoming_email(request: Request):
    """
    Process incoming email with CSV attachment for BDQ testing
    """
    print("STDOUT: /email/incoming endpoint called")  # Explicit stdout
    
    # Check if services are available
    if not all([email_service, bdq_service, csv_service]):
        logger.error("Services not initialized - cannot process email")
        return JSONResponse(
            status_code=503, 
            content={"error": "Service unavailable - services not initialized"}
        )
    
    try:
        # Log the raw request for debugging
        body = await request.body()
        logger.info(f"Received request with {len(body)} bytes")
        print(f"STDOUT: Received request with {len(body)} bytes")  # Explicit stdout
        send_discord_notification(f"Received email request: {len(body)} bytes")
        
        # Try to parse as JSON
        try:
            import json
            raw_data = json.loads(body.decode('utf-8'))
            logger.info(f"Parsed JSON data keys: {list(raw_data.keys())}")
            print(f"STDOUT: Parsed JSON data keys: {list(raw_data.keys())}")  # Explicit stdout
        except Exception as parse_error:
            logger.error(f"Failed to parse JSON: {parse_error}")
            logger.error(f"Raw body (first 500 chars): {body[:500]}")
            send_discord_notification(f"JSON parse error: {str(parse_error)}")
            return JSONResponse(status_code=400, content={"error": "Invalid JSON payload"})
        
        # Convert to EmailPayload model
        try:
            email_data = EmailPayload(**raw_data)
            logger.info(f"Successfully parsed email from {email_data.from_email}")
            print(f"STDOUT: Successfully parsed email from {email_data.from_email}")  # Explicit stdout
        except Exception as model_error:
            logger.error(f"Failed to create EmailPayload model: {model_error}")
            logger.error(f"Raw data: {raw_data}")
            send_discord_notification(f"Model validation error: {str(model_error)}")
            return JSONResponse(status_code=400, content={"error": "Invalid email payload structure"})
        
        # Extract and validate CSV attachment
        csv_data = email_service.extract_csv_attachment(email_data)
        if not csv_data:
            await email_service.send_error_reply(
                email_data, 
                "No CSV attachment found. Please attach a CSV file with biodiversity data."
            )
            return JSONResponse(status_code=200, content={"status": "error", "message": "No CSV attachment"})
        
        # Parse CSV and detect core type
        df, core_type = csv_service.parse_csv_and_detect_core(csv_data)
        if not core_type:
            await email_service.send_error_reply(
                email_data,
                "CSV must contain either 'occurrenceID' or 'taxonID' column to identify the core type."
            )
            return JSONResponse(status_code=200, content={"status": "error", "message": "Invalid core type"})
        
        # Get available BDQ tests
        tests = await bdq_service.get_available_tests()
        applicable_tests = bdq_service.filter_applicable_tests(tests, df.columns.tolist())
        
        if not applicable_tests:
            await email_service.send_error_reply(
                email_data,
                "No applicable BDQ tests found for the provided CSV columns."
            )
            return JSONResponse(status_code=200, content={"status": "error", "message": "No applicable tests"})
        
        # Run BDQ tests
        test_results = await bdq_service.run_tests_on_dataset(df, applicable_tests, core_type)
        
        # Generate result files
        raw_results_csv = csv_service.generate_raw_results_csv(test_results, core_type)
        amended_dataset_csv = csv_service.generate_amended_dataset(df, test_results, core_type)
        
        # Generate summary
        summary = bdq_service.generate_summary(test_results, len(df))
        
        # Send reply email
        await email_service.send_results_reply(
            email_data,
            summary,
            raw_results_csv,
            amended_dataset_csv
        )
        
        logger.info(f"Successfully processed email from {email_data.from_email}")
        return JSONResponse(status_code=200, content={"status": "success", "message": "Email processed successfully"})
        
    except Exception as e:
        logger.error(f"Error processing email: {str(e)}", exc_info=True)
        send_discord_notification(f"Processing error: {str(e)}")
        
        # Try to send error reply if we have email data
        try:
            if 'email_data' in locals():
                await email_service.send_error_reply(
                    email_data,
                    f"An error occurred while processing your request: {str(e)}"
                )
        except Exception as reply_error:
            logger.error(f"Failed to send error reply: {str(reply_error)}")
        
        return JSONResponse(status_code=200, content={"status": "error", "message": str(e)})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    logger.info(f"Starting BDQ Email Report Service on port {port}")
    print(f"STDOUT: Starting BDQ Email Report Service on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
