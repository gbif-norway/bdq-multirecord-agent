import os
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
import json

from app.services.email_service import EmailService
from app.services.bdq_api_service import BDQAPIService
from app.services.csv_service import CSVService
from app.services.llm_service import LLMService
from app.utils.helper import get_unique_tuples, expand_single_test_results_to_all_rows, generate_summary_statistics, log
import pandas as pd

# Load environment variables
load_dotenv()

email_service = EmailService()
bdq_service = BDQAPIService()
csv_service = CSVService()
llm_service = LLMService()


app = FastAPI(
    title="BDQ Email Report Service",
    description="Service to process biodiversity data quality tests via email",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    log("BDQ Email Report service initialized")

@app.on_event("shutdown")
async def shutdown_event():
    log("Shutting down BDQ Email Report Service...")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "BDQ Email Report Service is running"}

async def _handle_email_processing(email_data: Dict[str, Any]):
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
            return)

        # Run BDQ tests
        test_results, skipped_tests = await bdq_service.run_tests(df, applicable_tests, core_type)

        # Generate result files
        raw_results_csv = csv_service.generate_raw_results_csv(test_results, core_type)
        amended_dataset_csv = csv_service.generate_amended_dataset(df, test_results, core_type)

        # Generate summary (include skipped tests)
        body = llm_service.generate_intelligent_summary(test_results, email_data, core_type, generate_summary_statistics(test_results, df, core_type))

        # Send reply email
        await email_service.send_results_reply(
            email_data,
            body,
            raw_results_csv,
            amended_dataset_csv
        )
        
    except Exception as e:
        log(f"Error processing email in background: {str(e)}", "ERROR")
        try:
            await email_service.send_error_reply(
                email_data,
                f"An error occurred while processing your request: {str(e)}"
            )
        except Exception as reply_error:
            log(f"Failed to send error reply: {str(reply_error)}", "ERROR")


@app.post("/email/incoming")
async def process_incoming_email(request: Request, background_tasks: BackgroundTasks):
    """
    Accept incoming email and immediately return 200. Heavy processing runs in background.
    """
    # Log the raw request for debugging
    body = await request.body()
    log(f"Received request with {len(body)} bytes")
    
    try:
        raw_data = json.loads(body.decode('utf-8'))
    except json.JSONDecodeError as e:
        log(f"Invalid JSON in request: {e}", "ERROR")
        return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid JSON in request"})
    
    # Schedule background processing and return immediately
    # Use the running loop to schedule the async handler without blocking the response
    asyncio.create_task(_handle_email_processing(raw_data))
    
    return JSONResponse(status_code=200, content={"status": "accepted", "message": "Email queued for processing"})


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log(f"Unhandled exception: {exc}", "ERROR")
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
