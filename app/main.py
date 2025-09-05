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
from app.services.bdq_py4j_service import BDQPy4JService
from app.services.csv_service import CSVService
from app.services.llm_service import LLMService
from app.utils.helper import get_unique_tuples, expand_single_test_results_to_all_rows, generate_summary_statistics, log
import pandas as pd

# Load environment variables
load_dotenv()

email_service = EmailService()
bdq_service = BDQPy4JService()
csv_service = CSVService()
llm_service = LLMService()


app = FastAPI(
    title="BDQ Email Report Service",
    description="Service to process biodiversity data quality tests via email",
    version="1.0.0"
)



@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "BDQ Email Report Service is running"}

@app.get("/health")
async def health_check():
    """Detailed health check"""
    log("Health check endpoint called")

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
            return

        # Get applicable BDQ tests
        applicable_tests = bdq_service.get_applicable_tests_for_dataset(df.columns.tolist())

        if not applicable_tests:
            log(f"No applicable BDQ tests found for provided CSV columns: {df.columns.tolist()}", "WARNING")
            await email_service.send_error_reply(
                email_data,
                "No applicable BDQ tests found for the provided CSV columns."
            )
            return

        # Execute BDQ tests and collect results
        test_results = []
        
        for test in applicable_tests:
            log(f"Processing test: {test.label}")
            
            # Get all unique value combinations in acted_upon + consulted
            unique_tuples = get_unique_tuples(df, test.acted_upon, test.consulted)
            
            if not unique_tuples:
                log(f"No unique tuples found for test {test.label}", "WARNING")
                continue
            
            # Execute test for each unique tuple
            for tuple_values in unique_tuples:
                try:
                    # Run test using bdq_service
                    tuple_result = bdq_service.execute_single_test(
                        test.java_class,
                        test.java_method,
                        test.acted_upon,
                        test.consulted,
                        tuple_values
                    )
                    
                    # Expand results to all rows that match this tuple
                    row_results = expand_single_test_results_to_all_rows(
                        df, test, tuple_result, tuple_values, core_type
                    )
                    test_results.extend(row_results)
                    
                except Exception as e:
                    log(f"Error executing test {test.label} for tuple {tuple_values}: {e}", "ERROR")
                    continue
        
        if not test_results:
            await email_service.send_error_reply(
                email_data,
                "No test results were generated. Please check your data format."
            )
            return
        
        # Generate LLM summary
        body = llm_service.generate_intelligent_summary(test_results, email_data, core_type, generate_summary_statistics(test_results, df, core_type))
        
        log(f"Generating result files with {len(test_results)} test results...")
        raw_results_csv = csv_service.generate_raw_results_csv(test_results, core_type)
        amended_dataset_csv = csv_service.generate_amended_dataset(df, test_results, core_type)

        log("Sending reply email...")
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
