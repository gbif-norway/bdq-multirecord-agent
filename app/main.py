import os
import io
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
from app.utils.helper import log
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
    minio_service.upload_dataframe(df, original_filename or "unknown_file", "original")

    test_results = await bdq_api_service.run_tests_on_dataset(df, core_type)
    summary_stats = _get_summary_stats(test_results)

    # Extract email content (prefer HTML, fallback to text)
    email_body = email_data['body']['html'] if email_data['body']['html'] else email_data['body']['text']
    email_content = f"FROM: {email_data['headers']['from']}\nSUBJECT: {email_data['headers']['subject']}\n{email_body}"

    # Get LLM analysis (without stats)
    llm_analysis = llm_service.generate_intelligent_summary(test_results, email_content, core_type, summary_stats)
    
    # Combine summary stats + LLM analysis
    body = _format_summary_stats_html(summary_stats, core_type) + llm_analysis

    # Generate result files and upload to MinIO
    raw_results_csv = csv_service.generate_raw_results_csv(test_results)
    amended_dataset_csv = csv_service.generate_amended_dataset(df, test_results, core_type)
    
    # Convert CSV strings back to DataFrames for upload
    import pandas as pd
    raw_results_df = pd.read_csv(io.StringIO(raw_results_csv), dtype=str)
    amended_df = pd.read_csv(io.StringIO(amended_dataset_csv), dtype=str)
    
    # Upload result DataFrames to MinIO
    minio_service.upload_dataframe(raw_results_df, original_filename or "unknown_file", "raw_results")
    minio_service.upload_dataframe(amended_df, original_filename or "unknown_file", "amended")
    
    # Send reply email
    await email_service.send_results_reply(email_data, body, raw_results_csv, amended_dataset_csv)
    

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

def _get_summary_stats(test_results_df):
    """Generate summary statistics from test results DataFrame"""
    if test_results_df is None or test_results_df.empty:
        return {}
    
    # Calculate basic stats
    total_records = len(test_results_df)
    unique_tests = test_results_df['test_id'].nunique()
    
    # Count validation failures
    validation_failures = test_results_df[
        (test_results_df['result'] == 'NOT_COMPLIANT') | 
        (test_results_df['result'] == 'POTENTIAL_ISSUE')
    ]
    
    # Count amendments applied
    amendments_applied = len(test_results_df[
        test_results_df['status'].isin(['AMENDED', 'FILLED_IN'])
    ])
    
    # Count compliant results
    compliant_results = len(test_results_df[
        test_results_df['result'] == 'COMPLIANT'
    ])
    
    # Get failure counts by test type
    failure_counts_by_test = {}
    for test_id in test_results_df['test_id'].unique():
        test_failures = test_results_df[
            (test_results_df['test_id'] == test_id) & 
            ((test_results_df['result'] == 'NOT_COMPLIANT') | 
                (test_results_df['result'] == 'POTENTIAL_ISSUE'))
        ]
        if len(test_failures) > 0:
            failure_counts_by_test[test_id] = len(test_failures)
    
    # Get common issues (most frequent comments)
    common_issues = []
    if not validation_failures.empty and 'comment' in validation_failures.columns:
        # Filter out empty comments and get top 5 most common
        non_empty_comments = validation_failures[validation_failures['comment'].notna() & 
                                                (validation_failures['comment'] != '')]
        if not non_empty_comments.empty:
            common_issues = non_empty_comments['comment'].value_counts().head(5).to_dict()
    
    # Calculate success rate
    total_tests_run = len(test_results_df)
    success_rate = (compliant_results / total_tests_run * 100) if total_tests_run > 0 else 0
    
    summary = {
        'total_records': total_records,
        'total_tests_run': total_tests_run,
        'unique_tests': unique_tests,
        'validation_failures': len(validation_failures),
        'amendments_applied': amendments_applied,
        'compliant_results': compliant_results,
        'success_rate_percent': round(success_rate, 1),
        'failure_counts_by_test': failure_counts_by_test,
        'common_issues': common_issues
    }
    
    log(f"Generated summary: {total_records} records, {total_tests_run} tests, {len(validation_failures)} failures, {amendments_applied} amendments")
    return summary

def _format_summary_stats_html(summary_stats, core_type):
    """Format summary statistics as a nice HTML list for the top of the email"""
    if not summary_stats:
        return "<p><strong>No test results available</strong></p>"
    
    total_records = summary_stats.get('total_records', 0)
    total_tests = summary_stats.get('total_tests_run', 0)
    success_rate = summary_stats.get('success_rate_percent', 0)
    validation_failures = summary_stats.get('validation_failures', 0)
    amendments_applied = summary_stats.get('amendments_applied', 0)
    failure_counts = summary_stats.get('failure_counts_by_test', {})
    common_issues = summary_stats.get('common_issues', {})
    
    html = f"""
    <div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #007bff; margin-bottom: 20px;">
        <h3 style="margin-top: 0; color: #007bff;">&#x1F4CA; BDQ Test Results Summary</h3>
        <ul style="margin: 0; padding-left: 20px;">
            <li><strong>Dataset:</strong> {core_type.title()} core with {total_records:,} records</li>
            <li><strong>Tests Run:</strong> {total_tests:,} tests across {summary_stats.get('unique_tests', 0)} categories</li>
            <li><strong>Success Rate:</strong> {success_rate}% compliant</li>
    """
    
    if validation_failures > 0:
        html += f"<li><strong>Issues Found:</strong> {validation_failures:,} validation problems</li>"
        
        # Add top 3 failure categories (truncated)
        if failure_counts:
            top_failures = list(failure_counts.items())[:3]
            failure_list = ", ".join([f"{test_id} ({count})" for test_id, count in top_failures])
            if len(failure_counts) > 3:
                failure_list += f" and {len(failure_counts) - 3} more"
            html += f"<li><strong>Main Issues:</strong> {failure_list}</li>"
    else:
        html += "<li><strong>Status:</strong> &#x2705; No validation issues found!</li>"
    
    if amendments_applied > 0:
        html += f"<li><strong>Auto-Improvements:</strong> {amendments_applied:,} records enhanced</li>"
    
    # Add top 2 most common issues (truncated)
    if common_issues:
        top_issues = list(common_issues.items())[:2]
        issues_list = ", ".join([f"{issue[:50]}{'...' if len(issue) > 50 else ''} ({count})" for issue, count in top_issues])
        if len(common_issues) > 2:
            issues_list += f" and {len(common_issues) - 2} more"
        html += f"<li><strong>Common Issues:</strong> {issues_list}</li>"
    
    html += """
        </ul>
    </div>
    """
    
    return html
        

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log(f"Unhandled exception: {exc}", "ERROR")
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
