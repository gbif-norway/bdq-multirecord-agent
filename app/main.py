import os
import io
import traceback
from typing import Dict, Any
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Query
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

    # Run tests on dataset; returns unique results with counts
    unique_test_results = await bdq_api_service.run_tests_on_dataset(df, core_type)

    # Upload unique results (for the dashboard) and amended dataset to MinIO
    unique_test_results_csv = minio_service.upload_dataframe(unique_test_results, original_filename, "test_results_unique")
    
    # Generate summary stats using unique results (more efficient and accurate)
    summary_stats = _get_summary_stats_from_unique_results(unique_test_results, core_type, len(df))
    amended_dataset = csv_service.generate_amended_dataset(df, unique_test_results, core_type)
    amended_csv = minio_service.upload_dataframe(amended_dataset, original_filename, "amended")
    
    # Get LLM analysis using unique results (more efficient and focused)
    prompt = llm_service.create_prompt(email_data, core_type, summary_stats, str_snapshot(unique_test_results), str_snapshot(df), get_relevant_test_contexts(unique_test_results['test_id'].unique().tolist()))
    
    # Convert DataFrames to CSV strings for LLM
    unique_results_csv_content = csv_service.dataframe_to_csv_string(unique_test_results)
    original_csv_content = csv_service.dataframe_to_csv_string(df)
    llm_analysis = llm_service.generate_openai_intelligent_summary(prompt, unique_results_csv_content, original_csv_content)
    
    # Generate dashboard URL (unique + amended only)
    dashboard_url = minio_service.generate_dashboard_url(unique_test_results_csv, amended_csv)
    
    # Combine summary stats + LLM analysis + breakdown button
    body = _format_summary_stats_html(summary_stats, core_type, len(df)) + llm_analysis
    if dashboard_url:
        body += _format_breakdown_button_html(dashboard_url)
    
    # Send reply email
    await email_service.send_results_reply(email_data, body)
    

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

@app.get("/debug/llm-analysis")
async def debug_llm_analysis(
    unique_results: str = Query(..., description="Filename of CSV file with unique test results (e.g., 'test_results_unique_occurrencetxt_20250910_114343.csv')"),
    original_email: str = Query(..., description="Filename of CSV file with original dataset (e.g., 'simple_occurrence_dwc.csv')"),
    prompt_override: str = Query(None, description="Optional custom prompt to override default")
):
    """
    Debug endpoint to test LLM analysis with CSV files from S3.
    Automatically constructs S3 URLs from filenames.
    Sends analysis results to rukayasj@uio.no for debugging purposes.
    """
    try:
        # Construct full S3 URLs from filenames
        s3_base_url = "https://storage.gbif-no.sigma2.no/misc/bdqreport/results"
        unique_results_url = f"{s3_base_url}/{unique_results}"
        original_email_url = f"{s3_base_url}/{original_email}"
        
        # Download CSV files from S3 URLs
        unique_results_csv = minio_service.download_csv_from_url(unique_results_url)
        if not unique_results_csv:
            raise HTTPException(status_code=400, detail=f"Failed to download unique_results CSV from S3: {unique_results_url}")
        
        original_email_csv = minio_service.download_csv_from_url(original_email_url)
        if not original_email_csv:
            raise HTTPException(status_code=400, detail=f"Failed to download original_email CSV from S3: {original_email_url}")
        
        # Parse the original dataset to detect core type (same as _handle_email_processing)
        df, core_type = csv_service.parse_csv_and_detect_core(original_email_csv)
        if not core_type:
            raise HTTPException(status_code=400, detail="Original dataset must contain either 'occurrenceID' or 'taxonID' column")
        
        log("Debugging email processing...")

        # Parse unique results to get summary stats (same logic as _handle_email_processing)
        unique_results_df = pd.read_csv(io.StringIO(unique_results_csv), dtype=str)
        # Convert count column to numeric, handling any non-numeric values
        unique_results_df['count'] = pd.to_numeric(unique_results_df['count'], errors='coerce')
        # Replace any inf or -inf values with 0, then fill NaN with 0, then convert to int
        unique_results_df['count'] = unique_results_df['count'].replace([float('inf'), float('-inf')], 0).fillna(0).astype(int)
        summary_stats = _get_summary_stats_from_unique_results(unique_results_df, core_type, len(df))
        
        # Create mock email data for the prompt (similar to _handle_email_processing)
        mock_email_data = {
            "headers": {"from": "debug@test.com"},
            "subject": f"Debug LLM Analysis - {original_email}",
            "body": "Debug analysis request"
        }
        
        # Use custom prompt if provided, otherwise use the same prompt creation as _handle_email_processing
        if prompt_override:
            prompt = prompt_override
        else:
            # Use the same prompt creation logic as _handle_email_processing
            prompt = llm_service.create_prompt(
                mock_email_data, 
                core_type, 
                summary_stats, 
                str_snapshot(unique_results_df), 
                str_snapshot(df), 
                get_relevant_test_contexts(unique_results_df['test_id'].unique().tolist()) if 'test_id' in unique_results_df.columns else []
            )

        # Generate LLM analysis (same as _handle_email_processing)
        unique_results_csv_content = csv_service.dataframe_to_csv_string(unique_results_df)
        original_csv_content = csv_service.dataframe_to_csv_string(df)
        llm_analysis = llm_service.generate_openai_intelligent_summary(prompt, unique_results_csv_content, original_csv_content)
        
        # Create debug email data for sending
        debug_email_data = {
            "headers": {"from": "debug@bdq-service.com"},
            "threadId": "debug-thread",    # will fall back to direct send if invalid
            "to": "rukayasj@uio.no",
            "subject": f"Debug LLM Analysis Results - {original_email.split('/')[-1]}",
        }
        
        # Send analysis to debug email
        await email_service.send_reply(debug_email_data, llm_analysis, to_email="rukayasj@uio.no")
        
        return JSONResponse(
            status_code=200, 
            content={
                "status": "success", 
                "message": f"LLM analysis completed and sent to rukayasj@uio.no",
                "core_type": core_type,
                "summary_stats": summary_stats,
                "unique_results_file": unique_results,
                "original_email_file": original_email,
                "unique_results_url": unique_results_url,
                "original_email_url": original_email_url
            }
        )
        
    except Exception as e:
        log(f"Error in debug LLM analysis: {e}", "ERROR")
        log(f"Traceback: {traceback.format_exc()}", "ERROR")
        raise HTTPException(status_code=500, detail=f"Error processing files: {str(e)}")

def _get_summary_stats_from_unique_results(unique_results_df, core_type, original_dataset_length):
    """Generate summary statistics from unique results DataFrame - adapted from _get_summary_stats"""
    # Extract unique field names from actedUpon and consulted columns (same as _get_summary_stats)
    acted_upon_fields = set()
    consulted_fields = set()
    
    for acted_upon_str in unique_results_df['actedUpon'].dropna():
        if acted_upon_str:  # Skip empty strings
            # Split by | and extract field names (before =)
            for pair in acted_upon_str.split('|'):
                if '=' in pair:
                    field_name = pair.split('=')[0]
                    acted_upon_fields.add(field_name)
    
    for consulted_str in unique_results_df['consulted'].dropna():
        if consulted_str:  # Skip empty strings
            # Split by | and extract field names (before =)
            for pair in consulted_str.split('|'):
                if '=' in pair:
                    field_name = pair.split('=')[0]
                    consulted_fields.add(field_name)
    
    all_cols_tested = list(acted_upon_fields.union(consulted_fields))
    amendments = unique_results_df[unique_results_df['status'] == 'AMENDED']
    filled_in = unique_results_df[unique_results_df['status'] == 'FILLED_IN']
    issues = unique_results_df[unique_results_df['result'] == 'POTENTIAL_ISSUE']
    non_compliant_validations = unique_results_df[unique_results_df['result'] == 'NOT_COMPLIANT']

    def _get_top_grouped(df, n=15):
        """Helper to get top n grouped counts sorted descending using the count column."""
        if df.empty:
            return []
        # Fill NaN values in count column with 0 and convert to int
        df_clean = df.copy()
        # Handle any remaining NaN, inf, or -inf values in count column
        df_clean['count'] = df_clean['count'].replace([float('inf'), float('-inf')], 0)
        df_clean['count'] = df_clean['count'].fillna(0).astype(int)
        # Handle NaN values in string columns by replacing with empty string
        df_clean['actedUpon'] = df_clean['actedUpon'].fillna('')
        df_clean['consulted'] = df_clean['consulted'].fillna('')
        df_clean['test_id'] = df_clean['test_id'].fillna('')
        return (df_clean.sort_values('count', ascending=False)
                .head(n)
                [['actedUpon', 'consulted', 'test_id', 'count']]
                .to_dict('records'))

    # Helper function to safely get numeric values
    def safe_int(value, default=0):
        """Safely convert value to int, handling NaN, inf, and other edge cases"""
        if pd.isna(value) or value == float('inf') or value == float('-inf'):
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    summary = {
        'number_of_records_in_dataset': original_dataset_length,
        'list_of_all_columns_tested': all_cols_tested,
        'no_of_tests_results': safe_int(unique_results_df['count'].sum()),
        'no_of_tests_run': safe_int(unique_results_df['test_id'].nunique()),
        'no_of_non_compliant_validations': safe_int(non_compliant_validations['count'].sum()),
        'no_of_unique_non_compliant_validations': len(non_compliant_validations),
        'no_of_amendments': safe_int(amendments['count'].sum()),
        'no_of_unique_amendments': len(amendments),
        'no_of_filled_in': safe_int(filled_in['count'].sum()),
        'no_of_unique_filled_in': len(filled_in),
        'no_of_issues': safe_int(issues['count'].sum()),
        'no_of_unique_issues': len(issues),
        'top_issues': _get_top_grouped(issues),
        'top_filled_in': _get_top_grouped(filled_in),
        'top_amendments': _get_top_grouped(amendments),
        'top_non_compliant_validations': _get_top_grouped(non_compliant_validations),
    }

    log(f"Generated summary from unique results: {summary}")
    return summary


def _format_summary_stats_html(summary_stats, core_type, no_of_records):
    """Format summary statistics as a nice HTML list for the top of the email"""
    return f"""
    <div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #007bff; margin-bottom: 20px;">
        <h3 style="margin-top: 0; color: #007bff;">&#x1F4CA; BDQ Test Results Summary</h3>
        <ul style="margin: 0; padding-left: 20px;">
            <li><strong>Dataset:</strong> {core_type.title()} core with {no_of_records} records</li>
            <li><strong>Tests Run:</strong> {summary_stats['no_of_tests_results']} tests across {summary_stats['no_of_tests_run']} types of BDQ Tests</li>
            <li><strong>Possible problems found (Non-Compliant Validations):</strong> {summary_stats['no_of_non_compliant_validations']}</li>
            <li><strong>Amendments automatically applied:</strong> {summary_stats['no_of_amendments'] + summary_stats['no_of_filled_in']}</li>
            <li><strong>Other possible issues found:</strong> {summary_stats['no_of_issues']}</li>
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
            &#x1F4CA; View a Breakdown
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
