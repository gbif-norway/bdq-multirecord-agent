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
from utils.logger import setup_logging

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

# Initialize services
email_service = EmailService()
bdq_service = BDQService()
csv_service = CSVService()

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "BDQ Email Report Service is running"}

@app.get("/health")
async def health_check():
    """Detailed health check"""
    return {
        "status": "healthy",
        "service": "BDQ Email Report Service",
        "version": "1.0.0"
    }

@app.post("/email/incoming")
async def process_incoming_email(email_data: EmailPayload):
    """
    Process incoming email with CSV attachment for BDQ testing
    """
    try:
        logger.info(f"Processing email from {email_data.from_email}")
        
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
    uvicorn.run(app, host="0.0.0.0", port=port)
