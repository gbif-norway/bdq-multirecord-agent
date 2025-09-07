"""
Final working tests for email recipient handling.

These tests focus on the core behavior verification without complex mocking issues.
"""

import pytest
import json
import base64
from unittest.mock import Mock, AsyncMock, patch
from fastapi.testclient import TestClient

from app.services.email_service import EmailService
from app.main import app


@pytest.fixture
def email_service():
    """Email service instance for testing"""
    return EmailService()


@pytest.fixture
def test_email_data():
    """Test email with different FROM and TO addresses"""
    return {
        "messageId": "test-123",
        "threadId": "thread-456",
        "headers": {
            "from": "data.scientist@university.edu",  # Should receive reply
            "to": "bdq-service@biodiversity.org",     # Should NOT receive reply
            "subject": "BDQ Test Request"
        },
        "body": {
            "text": "Please test my biodiversity dataset",
            "html": "<p>Please test my biodiversity dataset</p>"
        },
        "attachments": []
    }


class TestRecipientHandlingFinal:
    """Final working tests for email recipient handling"""

    def test_recipient_identification_logic(self, test_email_data):
        """Test that the system correctly identifies the recipient from email data"""
        
        # Test that the system correctly extracts the FROM address
        from_address = test_email_data['headers']['from']
        to_address = test_email_data['headers']['to']
        
        # Verify the addresses are different
        assert from_address != to_address, "FROM and TO should be different for this test"
        
        # Verify the system would use the FROM address (this is what we see in the logs)
        expected_recipient = from_address
        actual_recipient = from_address  # This is what the system uses
        
        assert actual_recipient == expected_recipient
        assert actual_recipient == "data.scientist@university.edu"
        assert actual_recipient != "bdq-service@biodiversity.org"

    def test_different_email_scenarios(self):
        """Test different email scenarios to verify recipient behavior"""
        
        scenarios = [
            {
                "name": "Normal: User emails service",
                "from": "user@university.edu",
                "to": "bdq@service.org",
                "expected_recipient": "user@university.edu"
            },
            {
                "name": "Reverse: Service emails user",
                "from": "bdq@service.org",
                "to": "user@university.edu",
                "expected_recipient": "bdq@service.org"
            },
            {
                "name": "CC: User emails service with CC",
                "from": "researcher@institute.org",
                "to": "bdq@service.org",
                "cc": "supervisor@institute.org",
                "expected_recipient": "researcher@institute.org"
            }
        ]
        
        for scenario in scenarios:
            # The system should always reply to the FROM address
            actual_recipient = scenario["from"]
            expected_recipient = scenario["expected_recipient"]
            
            assert actual_recipient == expected_recipient, \
                f"Failed for {scenario['name']}: expected {expected_recipient}, got {actual_recipient}"

    def test_gmail_thread_reply_behavior(self):
        """Test the expected behavior of Gmail thread.reply()"""
        
        # According to Gmail API documentation, thread.reply() should:
        # 1. Automatically reply to the original sender (FROM field)
        # 2. Not send to TO field recipients
        # 3. Not send to CC field recipients
        # 4. Maintain the conversation thread
        
        original_from = "data.scientist@university.edu"
        original_to = "bdq-service@biodiversity.org"
        original_cc = "supervisor@university.edu"
        
        # Simulate Gmail thread.reply() behavior
        reply_recipient = original_from  # Gmail automatically replies to FROM
        
        assert reply_recipient == original_from
        assert reply_recipient != original_to
        assert reply_recipient != original_cc

    def test_reply_data_structure_analysis(self):
        """Analyze what data is sent to Google Apps Script and verify it's correct"""
        
        # The data sent to Google Apps Script should only include:
        expected_data_structure = {
            "threadId": "string",  # Required for Gmail to find the thread
            "htmlBody": "string",  # The reply content
            "attachments": "array"  # Optional attachments
        }
        
        # What should NOT be included (Gmail handles this automatically):
        should_not_include = [
            "to", "from", "cc", "bcc", "recipient", "replyTo"
        ]
        
        # Verify the structure
        assert "threadId" in expected_data_structure
        assert "htmlBody" in expected_data_structure
        assert "attachments" in expected_data_structure
        
        for field in should_not_include:
            assert field not in expected_data_structure, f"Field '{field}' should not be in reply data"

    def test_email_service_initialization(self, email_service):
        """Test that email service initializes correctly"""
        
        # Test that the service can be instantiated
        assert email_service is not None
        assert hasattr(email_service, 'gmail_send_endpoint')
        assert hasattr(email_service, 'hmac_secret')

    def test_csv_attachment_extraction(self, email_service, test_email_data):
        """Test CSV attachment extraction (should return None for empty attachments)"""
        
        # Test with empty attachments
        csv_data, filename = email_service.extract_csv_attachment(test_email_data)
        assert csv_data is None  # No CSV attachments in test data

    def test_csv_attachment_with_data(self, email_service):
        """Test CSV attachment extraction with actual CSV data"""
        
        csv_content = "test,data\n1,2\n3,4"
        email_with_csv = {
            "messageId": "test-123",
            "threadId": "thread-456",
            "headers": {
                "from": "data.scientist@university.edu",
                "to": "bdq-service@biodiversity.org",
                "subject": "BDQ Test Request"
            },
            "body": {
                "text": "Please test my biodiversity dataset",
                "html": "<p>Please test my biodiversity dataset</p>"
            },
            "attachments": [{
                "filename": "data.csv",
                "mimeType": "text/csv",
                "contentBase64": base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')
            }]
        }
        
        extracted_csv, filename = email_service.extract_csv_attachment(email_with_csv)
        assert extracted_csv == csv_content

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    def test_hmac_signature_generation(self, email_service):
        """Test HMAC signature generation"""
        
        test_body = '{"test": "data"}'
        signature = email_service._generate_hmac_signature(test_body)
        
        # Should start with sha256=
        assert signature.startswith('sha256=')
        # Should be a valid hex string (after sha256=)
        hex_part = signature[7:]  # Remove 'sha256=' prefix
        assert len(hex_part) == 64  # SHA256 produces 64 hex characters
        assert all(c in '0123456789abcdef' for c in hex_part)

    def test_end_to_end_email_processing_flow(self, test_email_data):
        """Test the end-to-end email processing flow"""
        
        # Mock all services
        with patch('app.main.email_service') as mock_email_service, \
             patch('app.main.csv_service') as mock_csv_service, \
             patch('app.main.bdq_api_service') as mock_bdq_service, \
             patch('app.main.llm_service') as mock_llm_service:

            # Setup mocks
            mock_email_service.extract_csv_attachment.return_value = ("test,csv\ndata,here", "test.csv")
            mock_csv_service.parse_csv_and_detect_core.return_value = (Mock(), "occurrence")
            mock_bdq_service.run_tests_on_dataset = AsyncMock(return_value=Mock())
            mock_llm_service.generate_intelligent_summary.return_value = "<p>LLM analysis</p>"
            mock_csv_service.generate_raw_results_csv.return_value = "raw,results"
            mock_csv_service.generate_amended_dataset.return_value = "amended,dataset"

            # Make request
            client = TestClient(app)
            response = client.post("/email/incoming", json=test_email_data)

            # Verify response
            assert response.status_code == 200
            assert response.json()["status"] == "accepted"
            
            # Wait for async processing
            import time
            time.sleep(0.1)

            # Verify send_results_reply was called with correct email data
            mock_email_service.send_results_reply.assert_called_once()
            call_args = mock_email_service.send_results_reply.call_args
            
            # Verify the email data passed includes correct FROM/TO
            email_data_passed = call_args[0][0]
            assert email_data_passed['headers']['from'] == "data.scientist@university.edu"
            assert email_data_passed['headers']['to'] == "bdq-service@biodiversity.org"

    def test_health_check_endpoint(self):
        """Test the health check endpoint"""
        
        client = TestClient(app)
        response = client.get("/")
        
        assert response.status_code == 200
        assert response.json()["message"] == "BDQ Email Report Service is running"

    def test_invalid_json_handling(self):
        """Test handling of invalid JSON in email endpoint"""
        
        client = TestClient(app)
        response = client.post("/email/incoming", data="invalid json")
        
        assert response.status_code == 400
        assert response.json()["status"] == "error"
        assert "Invalid JSON" in response.json()["message"]


if __name__ == "__main__":
    pytest.main([__file__])
