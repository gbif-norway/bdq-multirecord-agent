import os
import io
import traceback
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
from app.services.minio_service import MinIOService
from app.utils.helper import log, str_snapshot, get_relevant_test_contexts
import pandas as pd

# Initialize services at module level for dependency injection
email_service = EmailService()
bdq_api_service = BDQAPIService()
csv_service = CSVService()
llm_service = LLMService()
minio_service = MinIOService()

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
    log(str(email_data))
    csv_data, original_filename = email_service.extract_csv_attachment(email_data)
    if not csv_data:
        await email_service.send_error_reply(email_data, "No CSV attachment found. Please attach a CSV file with biodiversity data.")
        return

    df, core_type = csv_service.parse_csv_and_detect_core(csv_data)
    if not core_type:
        await email_service.send_error_reply(email_data, "CSV must contain either 'occurrenceID' or 'taxonID' column to identify the core type.")
        return

    # Upload original processed DataFrame to MinIO
    original_csv = minio_service.upload_dataframe(df, original_filename, "original")

    # Run tests on dataset - note that at this point test_results will never be empty because the CSV has either occurrenceID or taxonID and the API will run tests on these at least
    test_results = await bdq_api_service.run_tests_on_dataset(df, core_type)
    summary_stats = _get_summary_stats(test_results, core_type)

    # Upload test results and amended dataset to MinIO
    test_results_csv = minio_service.upload_dataframe(test_results, original_filename, "test_results")
    amended_dataset = csv_service.generate_amended_dataset(df, test_results, core_type)
    amended_csv = minio_service.upload_csv_string(amended_dataset, original_filename, "amended")
    
    # Get LLM analysis
    prompt = llm_service.create_prompt(email_data, core_type, summary_stats, str_snapshot(test_results), str_snapshot(df), get_relevant_test_contexts(test_results['test_id'].unique().tolist()))
    llm_analysis = llm_service.generate_intelligent_summary(prompt, test_results_csv, original_csv)
    
    # Generate dashboard URL
    dashboard_url = minio_service.generate_dashboard_url(test_results_csv, original_csv)
    
    # Combine summary stats + LLM analysis + breakdown button
    body = _format_summary_stats_html(summary_stats, core_type, len(df)) + llm_analysis
    if dashboard_url:
        body += _format_breakdown_button_html(dashboard_url)
    
    # Send reply email
    await email_service.send_results_reply(email_data, body, test_results_csv, amended_csv)
    

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

def _get_summary_stats(test_results_df, coreID):
    """Generate summary statistics from test results DataFrame"""
    # Extract unique field names from actedUpon and consulted columns
    # These columns contain formatted strings like "field1=value1|field2=value2"
    acted_upon_fields = set()
    consulted_fields = set()
    
    for acted_upon_str in test_results_df['actedUpon'].dropna():
        if acted_upon_str:  # Skip empty strings
            # Split by | and extract field names (before =)
            for pair in acted_upon_str.split('|'):
                if '=' in pair:
                    field_name = pair.split('=')[0]
                    acted_upon_fields.add(field_name)
    
    for consulted_str in test_results_df['consulted'].dropna():
        if consulted_str:  # Skip empty strings
            # Split by | and extract field names (before =)
            for pair in consulted_str.split('|'):
                if '=' in pair:
                    field_name = pair.split('=')[0]
                    consulted_fields.add(field_name)
    
    all_cols_tested = list(acted_upon_fields.union(consulted_fields))
    amendments = test_results_df[test_results_df['status'] == 'AMENDED']
    filled_in = test_results_df[test_results_df['status'] == 'FILLED_IN']
    issues = test_results_df[test_results_df['result'] == 'POTENTIAL_ISSUE']
    non_compliant_validations = test_results_df[test_results_df['result'] == 'NOT_COMPLIANT']

    def _get_top_grouped(df, group_cols, n=15):
        """Helper to get top n grouped counts sorted descending."""
        return (df.groupby(group_cols)
                .size()
                .reset_index(name='count')
                .sort_values('count', ascending=False)
                .head(n))

    summary = {
        'list_of_all_columns_tested': all_cols_tested,
        'no_of_tests_results': len(test_results_df),
        'no_of_tests_run': test_results_df['test_id'].nunique(),
        'no_of_non_compliant_validations': len(non_compliant_validations),
        'no_of_amendments': len(amendments),
        'no_of_filled_in': len(filled_in),
        'no_of_issues': len(issues),
        'top_issues': _get_top_grouped(issues, ['actedUpon', 'consulted', 'test_id']),
        'top_filled_in': _get_top_grouped(filled_in, ['actedUpon', 'consulted', 'test_id']),
        'top_amendments': _get_top_grouped(amendments, ['actedUpon', 'consulted', 'test_id']),
        'top_non_compliant_validations': _get_top_grouped(non_compliant_validations, ['actedUpon', 'consulted', 'test_id']),
    }

    log(f"Generated summary: {summary}")
    return summary

def _format_summary_stats_html(summary_stats, core_type, no_of_records):
    """Format summary statistics as a nice HTML list for the top of the email"""
    return f"""
    <div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #007bff; margin-bottom: 20px;">
        <h3 style="margin-top: 0; color: #007bff;">&#x1F4CA; BDQ Test Results Summary</h3>
        <ul style="margin: 0; padding-left: 20px;">
            <li><strong>Dataset:</strong> {core_type.title()} core with {no_of_records} records</li>
            <li><strong>Tests Run:</strong> {summary_stats['no_of_tests_results']} tests across {summary_stats['no_of_tests_run']} types of BDQ Tests</li>
            <li><strong>Non-Compliant Validations:</strong> {summary_stats['no_of_non_compliant_validations']}, most commonly: {summary_stats['top_non_compliant_validations']}</li>
            <li><strong>Amendments:</strong> {summary_stats['no_of_amendments']}, most commonly: {summary_stats['top_amendments']}</li>
            <li><strong>Filled In:</strong> {summary_stats['no_of_filled_in']}, most commonly: {summary_stats['top_filled_in']}</li>
            <li><strong>Issues:</strong> {summary_stats['no_of_issues']}, most commonly: {summary_stats['top_issues']}</li>
        </ul>
    </div>
    """

def _format_breakdown_button_html(dashboard_url: str) -> str:
    """Format breakdown button HTML for the email"""
    return f"""
    <div style="text-align: center; margin: 20px 0;">
        <a href="{dashboard_url}" 
           style="display: inline-block; 
                  background-color: #007bff; 
                  color: white; 
                  padding: 12px 24px; 
                  text-decoration: none; 
                  border-radius: 6px; 
                  font-weight: bold; 
                  font-size: 16px;
                  box-shadow: 0 2px 4px rgba(0,123,255,0.3);">
            📊 View a Breakdown
        </a>
    </div>
    """

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log(f"Unhandled exception: {exc}", "ERROR")
    log(f"Traceback: {traceback.format_exc()}", "ERROR")
    # For background email processing, we don't need to return an error response
    # since Gmail already got a 200. Just log for debugging.
    return JSONResponse(status_code=200, content={"status": "error_logged"})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
