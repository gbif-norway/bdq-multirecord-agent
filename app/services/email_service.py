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
    
    def extract_csv_attachment(self, email_data: dict) -> Optional[str]:
        """Extract CSV attachment from email data"""
        try:
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

                log(
                    f"CSV attachment candidate: filename={csv_attachment.get('filename')}, "
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
                            f"Failed to decode base64 for {csv_attachment.get('filename')}: {decode_err}"
                        )
                        continue

                try:
                    csv_content = decoded_bytes.decode('utf-8', errors='replace')
                except Exception as enc_err:
                    log(
                        f"Failed to decode bytes to UTF-8 for {csv_attachment.get('filename')}: {enc_err}"
                    )
                    continue

                log(
                    f"Extracted CSV attachment: {csv_attachment.get('filename')} ({len(csv_content)} chars)"
                )
                return csv_content

            log("All CSV-like attachments were empty or undecodable", "WARNING")
            return None

        except Exception as e:
            log(f"Error extracting CSV attachment: {e}", "ERROR")
            return None
    
    async def send_reply(self, email_data: dict, body: str, attachments: Optional[List[dict]] = None):
        """Send reply email with optional attachments"""
        try:
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
            response.raise_for_status()
            log(
                f"Sent reply to {email_data.get('headers').get('from')}; status={response.status_code}; body={(response.text or '')[:200]}"
            )
            
        except Exception as e:
            try:
                status = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
                text = getattr(e.response, 'text', '') if hasattr(e, 'response') else ''
                log(f"Error sending reply: {e}; status={status}; body={(text or '')[:200]}", "ERROR")
            except Exception:
                log(f"Error sending reply: {e}", "ERROR")
    
    async def send_error_reply(self, email_data: dict, error_message: str):
        """Send error reply email"""
        error_body = f"<p>Error processing your request:</p><p>{error_message}</p>"
        await self.send_reply(email_data, error_body)
    
    async def send_results_reply(self, email_data: dict, body: str, raw_results_csv: str, amended_dataset_csv: str):
        """Send results reply email with CSV attachments"""
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
        await self.send_reply(email_data, body, attachments)
    
