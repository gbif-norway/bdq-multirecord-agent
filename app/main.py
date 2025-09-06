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
    email_service = EmailService()

    csv_data = email_service.extract_csv_attachment(email_data)
    if not csv_data:
        await email_service.send_error_reply(email_data, "No CSV attachment found. Please attach a CSV file with biodiversity data.")
        return

    csv_service = CSVService()
    df, core_type = csv_service.parse_csv_and_detect_core(csv_data)
    if not core_type:
        await email_service.send_error_reply(email_data, "CSV must contain either 'occurrenceID' or 'taxonID' column to identify the core type.")
        return

    bdq_api_service = BDQAPIService()
    test_results = await bdq_api_service.run_tests_on_dataset(df, core_type)
    summary_stats = _get_summary_stats(test_results)

    # Get email body
    llm_service = LLMService()
    body = llm_service.generate_intelligent_summary(test_results, email_data)

    # Send reply email
    raw_results_csv = csv_service.generate_raw_results_csv(test_results, core_type)
    amended_dataset_csv = csv_service.generate_amended_dataset(df, test_results, core_type)
    await email_service.send_results_reply(email_data, body, df)
    

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

def _generate_summary_stats(df):
    # Generate some summary stats from the results file, should be a dict
    # Previously i had something like this, but there may be missing info now, just take and use what you can and make some stats you think are useful
    # version 1:
    #  """Generate summary statistics from test results DataFrame"""
    # if test_results_df is None or test_results_df.empty:
    #     return {}
    
    # # Calculate basic stats
    # total_records = len(df)
    # total_tests_run = len(test_results_df)
    
    # # Count validation failures by field
    # validation_failures = test_results_df[
    #     (test_results_df['result'] == 'NOT_COMPLIANT') | 
    #     (test_results_df['result'] == 'POTENTIAL_ISSUE')
    # ]
    
    # # Count amendments applied
    # amendments_applied = len(test_results_df[
    #     test_results_df['status'].isin(['AMENDED', 'FILLED_IN'])
    # ])
    
    # # Get common issues
    # common_issues = validation_failures['comment'].value_counts().head(5).to_dict()
    
    # return {
    #     'total_records': total_records,
    #     'total_tests_run': total_tests_run,
    #     'validation_failures': len(validation_failures),
    #     'amendments_applied': amendments_applied,
    #     'common_issues': common_issues
    # }
    # version 2
    #     """Generate processing summary from test results"""
    #     try:
    #         # Count validation failures by field
    #         validation_failures = {}
    #         amendments_applied = 0
    #         common_issues = []
            
    #         # Track unique issues for common issues list
    #         issue_counts = {}
            
    #         for result in test_results:
    #             if result.result == "NOT_COMPLIANT":
    #                 # This is a validation failure
    #                 if result.test_id not in validation_failures:
    #                     validation_failures[result.test_id] = 0
    #                 validation_failures[result.test_id] += 1
                    
    #                 # Track common issues
    #                 if result.comment:
    #                     issue_key = f"{result.test_id}: {result.comment}"
    #                     issue_counts[issue_key] = issue_counts.get(issue_key, 0) + 1
                
    #             elif result.status == "AMENDED":
    #                 amendments_applied += 1
            
    #         # Get top 5 most common issues
    #         sorted_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)
    #         common_issues = [issue[0] for issue in sorted_issues[:5]]
            
    #         summary = {
    #             "total_records": total_records,
    #             "total_tests_run": len(test_results),
    #             "validation_failures": validation_failures,
    #             "common_issues": common_issues,
    #             "amendments_applied": amendments_applied,
    #         }
            
    #         log(
    #             f"Generated summary: {total_records} records, {len(test_results)} tests, {len(validation_failures)} failure types
    #         )
    #         return summary
            
    #     except Exception as e:
    #         logger.error(f"Error generating summary: {e}")
    #         raise
    pass

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log(f"Unhandled exception: {exc}", "ERROR")
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
