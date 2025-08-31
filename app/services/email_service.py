import base64
import logging
import requests
import os
from typing import Optional, List
from models.email_models import EmailPayload, EmailAttachment, ProcessingSummary

logger = logging.getLogger(__name__)

class EmailService:
    """Service for handling email operations"""
    
    def __init__(self):
        self.gmail_send_endpoint = os.getenv("GMAIL_SEND")
    
    def extract_csv_attachment(self, email_data: EmailPayload) -> Optional[str]:
        """Extract CSV attachment from email data"""
        try:
            csv_attachments = []
            
            for attachment in email_data.attachments:
                # Check if it's a CSV file
                if (attachment.filename.lower().endswith('.csv') or 
                    'csv' in attachment.mime_type.lower()):
                    csv_attachments.append(attachment)
            
            if not csv_attachments:
                logger.warning("No CSV attachments found in email")
                return None
            
            # Use the first CSV attachment
            csv_attachment = csv_attachments[0]
            
            # Decode base64 content
            csv_content = base64.b64decode(csv_attachment.content_base64).decode('utf-8')
            
            logger.info(f"Extracted CSV attachment: {csv_attachment.filename} ({len(csv_content)} chars)")
            return csv_content
            
        except Exception as e:
            logger.error(f"Error extracting CSV attachment: {e}")
            return None
    
    async def send_error_reply(self, email_data: EmailPayload, error_message: str):
        """Send error reply email"""
        try:
            if not self.gmail_send_endpoint:
                logger.error("GMAIL_SEND endpoint not configured")
                return
            
            body_text = f"BDQ Processing Error\n\n{error_message}\n\nPlease check your CSV file and try again."
            body_html = f"<h3>BDQ Processing Error</h3><p>{error_message}</p><p>Please check your CSV file and try again.</p>"
            
            reply_data = {
                "threadId": email_data.thread_id,
                "bodyText": body_text,
                "bodyHtml": body_html,
                "attachments": []
            }
            
            response = requests.post(
                self.gmail_send_endpoint,
                json=reply_data,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            response.raise_for_status()
            
            logger.info(f"Sent error reply to {email_data.from_email}")
            
        except Exception as e:
            logger.error(f"Error sending error reply: {e}")
    
    async def send_results_reply(self, email_data: EmailPayload, summary: ProcessingSummary, 
                               raw_results_csv: str, amended_dataset_csv: str):
        """Send results reply email with attachments"""
        try:
            if not self.gmail_send_endpoint:
                logger.error("GMAIL_SEND endpoint not configured")
                return
            
            # Generate email body
            body_text = self._generate_summary_text(summary)
            body_html = self._generate_summary_html(summary)
            
            # Prepare attachments
            attachments = [
                {
                    "filename": "bdq_raw_results.csv",
                    "mimeType": "text/csv",
                    "contentBase64": base64.b64encode(raw_results_csv.encode('utf-8')).decode('utf-8')
                },
                {
                    "filename": "amended_dataset.csv",
                    "mimeType": "text/csv",
                    "contentBase64": base64.b64encode(amended_dataset_csv.encode('utf-8')).decode('utf-8')
                }
            ]
            
            reply_data = {
                "threadId": email_data.thread_id,
                "bodyText": body_text,
                "bodyHtml": body_html,
                "attachments": attachments
            }
            
            response = requests.post(
                self.gmail_send_endpoint,
                json=reply_data,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            response.raise_for_status()
            
            logger.info(f"Sent results reply to {email_data.from_email}")
            
        except Exception as e:
            logger.error(f"Error sending results reply: {e}")
    
    def _generate_summary_text(self, summary: ProcessingSummary) -> str:
        """Generate text summary for email body"""
        text = f"""BDQ Test Results Summary

Dataset Overview:
- Total records processed: {summary.total_records}
- Total tests run: {summary.total_tests_run}
- Amendments applied: {summary.amendments_applied}

Validation Results:
"""
        
        if summary.validation_failures:
            text += "Validation failures by test:\n"
            for test_id, count in summary.validation_failures.items():
                text += f"- {test_id}: {count} failures\n"
        else:
            text += "No validation failures found.\n"
        
        if summary.common_issues:
            text += "\nMost common issues:\n"
            for issue in summary.common_issues:
                text += f"- {issue}\n"
        
        text += """
Attachments:
- bdq_raw_results.csv: Detailed test results for each record
- amended_dataset.csv: Original dataset with proposed amendments applied

Note: The amended dataset applies proposed changes from Amendment-type tests.
"""
        
        return text
    
    def _generate_summary_html(self, summary: ProcessingSummary) -> str:
        """Generate HTML summary for email body"""
        html = f"""<h2>BDQ Test Results Summary</h2>

<h3>Dataset Overview</h3>
<ul>
<li><strong>Total records processed:</strong> {summary.total_records}</li>
<li><strong>Total tests run:</strong> {summary.total_tests_run}</li>
<li><strong>Amendments applied:</strong> {summary.amendments_applied}</li>
</ul>

<h3>Validation Results</h3>
"""
        
        if summary.validation_failures:
            html += "<p><strong>Validation failures by test:</strong></p><ul>"
            for test_id, count in summary.validation_failures.items():
                html += f"<li>{test_id}: {count} failures</li>"
            html += "</ul>"
        else:
            html += "<p>No validation failures found.</p>"
        
        if summary.common_issues:
            html += "<h3>Most common issues</h3><ul>"
            for issue in summary.common_issues:
                html += f"<li>{issue}</li>"
            html += "</ul>"
        
        html += """
<h3>Attachments</h3>
<ul>
<li><strong>bdq_raw_results.csv:</strong> Detailed test results for each record</li>
<li><strong>amended_dataset.csv:</strong> Original dataset with proposed amendments applied</li>
</ul>

<p><em>Note: The amended dataset applies proposed changes from Amendment-type tests.</em></p>
"""
        
        return html
