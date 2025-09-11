import base64
import requests
import os
import hmac
import hashlib
import json
from typing import Optional, List
from app.utils.helper import log

class EmailService:
    """Service for handling email operations"""
    
    def __init__(self):
        self.gmail_send_endpoint = os.getenv("GMAIL_SEND")
        self.hmac_secret = os.getenv("HMAC_SECRET")
    
    def _generate_hmac_signature(self, body: str) -> str:
        """Generate HMAC signature for request body"""
        if not self.hmac_secret:
            raise ValueError("HMAC_SECRET environment variable not set")
        
        signature = hmac.new(
            self.hmac_secret.encode(),
            body.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return f"sha256={signature}"
    
    def extract_csv_attachment(self, email_data: dict) -> tuple[Optional[str], Optional[str]]:
        """Extract CSV attachment from email data
        Returns (csv_content, original_filename)
        """
        csv_attachments = []

        for attachment in email_data['attachments']:
            fn = (attachment.get('filename') or '').lower()
            mt = (attachment.get('mimeType') or '').lower()
            if (
                fn.endswith(('.csv', '.tsv'))
                or 'csv' in mt
                or 'text/plain' in mt
                or 'tab-separated' in mt
            ):
                csv_attachments.append(attachment)

        # Try attachments in order; skip empties/undecodable
        for csv_attachment in csv_attachments:
            b64_raw = csv_attachment['contentBase64']
            b64 = b64_raw.strip()
            original_filename = csv_attachment.get('filename', 'unknown_file')

            log(
                f"CSV attachment candidate: filename={original_filename}, "
                f"mime={csv_attachment.get('mimeType')}, size={csv_attachment.get('size')}, b64_len={len(b64)}"
            )

            # Robust base64/url-safe base64 decode
            pad = (-len(b64)) % 4
            if pad:
                b64 += '=' * pad
            try:
                decoded_bytes = base64.urlsafe_b64decode(b64.encode('utf-8'))
            except Exception:
                try:
                    decoded_bytes = base64.b64decode(b64.encode('utf-8'))
                except Exception as decode_err:
                    log(
                        f"Failed to decode base64 for {original_filename}: {decode_err}"
                    )
                    continue

            try:
                csv_content = decoded_bytes.decode('utf-8', errors='replace')
            except Exception as enc_err:
                log(
                    f"Failed to decode bytes to UTF-8 for {original_filename}: {enc_err}"
                )
                continue

            log(
                f"Extracted CSV attachment: {original_filename} ({len(csv_content)} chars)"
            )
            return csv_content, original_filename

        log("All CSV-like attachments were empty or undecodable", "WARNING")
        return None, None
    
    async def send_reply(self, email_data: dict, body: str, attachments: Optional[List[dict]] = None, to_email: Optional[str] = None):
        """Send reply email with optional attachments"""
        if not self.gmail_send_endpoint:
            log("GMAIL_SEND endpoint not configured", "ERROR")
            return
        
        if not self.hmac_secret:
            log("HMAC_SECRET not configured", "ERROR")
            return
        
        reply_data = {
            "threadId": email_data.get('threadId'),
            "htmlBody": body,
            "attachments": attachments or []
        }
        
        # Add specific recipient if provided (for debug emails)
        if to_email:
            reply_data["to"] = to_email
        
        log(f"Reply data: {reply_data}")
        
        # Convert to JSON string for HMAC
        body_json = json.dumps(reply_data)
        signature = self._generate_hmac_signature(body_json)
        
        response = requests.post(
            self.gmail_send_endpoint,
            params={"X-Signature": signature, "signature": signature},  # Apps Script can't reliably read headers
            data=body_json,
            headers={
                "Content-Type": "application/json",
                "X-Signature": signature
            },
            timeout=30
        )
        # Even when Apps Script errors, it often returns HTTP 200 with an HTML error page.
        # Log more context and detect non-OK bodies to aid debugging in prod.
        response.raise_for_status()
        resp_ct = (response.headers.get('Content-Type') or '').lower()
        resp_text = (response.text or '')
        body_preview = resp_text[:500]
        log(
            f"Sent reply to {to_email or email_data.get('headers', {}).get('from')}; status={response.status_code}; content_type={resp_ct}; body={body_preview}"
        )
        # Expect plain text 'ok' from Apps Script on success
        if 'text/html' in resp_ct or not resp_text.strip().lower().startswith('ok'):
            log("Gmail send webapp returned non-ok response; likely misconfig or script error", "ERROR")
    
    async def send_error_reply(self, email_data: dict, error_message: str):
        """Send error reply email"""
        error_body = f"<p>Error processing your request:</p><p>{error_message}</p>"
        await self.send_reply(email_data, error_body)
    
    async def send_results_reply(self, email_data: dict, body: str):
        """Send results reply email without attachments (files are available via dashboard links)"""
        await self.send_reply(email_data, body)
    
