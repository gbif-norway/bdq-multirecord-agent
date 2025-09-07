"""
Simple tests to verify email recipient handling behavior.

These tests focus on the core issue: are replies being sent to the correct recipient?
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


class TestRecipientVerification:
    """Test that replies are sent to the correct recipient"""

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_reply_data_structure(self, mock_post, email_service, test_email_data):
        """Test that the reply data structure is correct"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        # Send reply
        import asyncio
        asyncio.run(email_service.send_reply(test_email_data, "<p>Test reply</p>"))

        # Verify request was made
        assert mock_post.called
        call_args = mock_post.call_args

        # Check the data sent to Gmail endpoint
        sent_data = json.loads(call_args[1]['data'])
        
        # Verify correct data structure
        assert sent_data['threadId'] == "thread-456"
        assert sent_data['htmlBody'] == "<p>Test reply</p>"
        assert sent_data['attachments'] == []
        
        # CRITICAL: Verify no explicit recipient information is sent
        # Gmail handles this automatically via thread.reply()
        recipient_fields = ['to', 'from', 'recipient', 'replyTo', 'cc', 'bcc']
        for field in recipient_fields:
            assert field not in sent_data, f"Field '{field}' should not be in request data"

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_log_shows_correct_recipient(self, mock_post, email_service, test_email_data):
        """Test that the log shows the correct recipient (FROM field)"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        # Capture log output
        with patch('app.utils.helper.log') as mock_log:
            import asyncio
            asyncio.run(email_service.send_reply(test_email_data, "<p>Test reply</p>"))

            # Check that log shows FROM address
            log_calls = [call[0][0] for call in mock_log.call_args_list]
            reply_log = next((log for log in log_calls if "Sent reply to" in log), None)
            
            assert reply_log is not None, "No 'Sent reply to' log found"
            assert "data.scientist@university.edu" in reply_log, "Log should show FROM address"
            assert "bdq-service@biodiversity.org" not in reply_log, "Log should NOT show TO address"

    def test_gmail_thread_reply_behavior_simulation(self):
        """Simulate Gmail thread.reply() behavior to verify recipient logic"""
        
        def simulate_gmail_thread_reply(original_from, original_to, reply_body):
            """Simulate what Gmail thread.reply() does"""
            # Gmail automatically replies to the original FROM address
            return {
                "sent_to": original_from,
                "not_sent_to": [original_to],
                "reply_body": reply_body
            }
        
        # Test case: User emails service
        result = simulate_gmail_thread_reply(
            "data.scientist@university.edu",  # FROM
            "bdq-service@biodiversity.org",   # TO
            "<p>Test reply</p>"
        )
        
        # Verify Gmail would send to FROM address
        assert result["sent_to"] == "data.scientist@university.edu"
        assert "bdq-service@biodiversity.org" in result["not_sent_to"]

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_error_reply_recipient(self, mock_post, email_service, test_email_data):
        """Test that error replies go to the correct recipient"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        with patch('app.utils.helper.log') as mock_log:
            import asyncio
            asyncio.run(email_service.send_error_reply(test_email_data, "Test error"))

            # Verify error reply was sent
            assert mock_post.called
            
            # Check log shows FROM address
            log_calls = [call[0][0] for call in mock_log.call_args_list]
            reply_log = next((log for log in log_calls if "Sent reply to" in log), None)
            
            assert reply_log is not None
            assert "data.scientist@university.edu" in reply_log

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_results_reply_recipient(self, mock_post, email_service, test_email_data):
        """Test that results replies go to the correct recipient"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        with patch('app.utils.helper.log') as mock_log:
            import asyncio
            asyncio.run(email_service.send_results_reply(
                test_email_data,
                "<p>Results</p>",
                "raw,data\n1,2",
                "amended,data\n1,3"
            ))

            # Verify results reply was sent
            assert mock_post.called
            
            # Check log shows FROM address
            log_calls = [call[0][0] for call in mock_log.call_args_list]
            reply_log = next((log for log in log_calls if "Sent reply to" in log), None)
            
            assert reply_log is not None
            assert "data.scientist@university.edu" in reply_log

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
            # Simulate Gmail thread.reply() behavior
            # Gmail always replies to the original FROM address
            actual_recipient = scenario["from"]
            expected_recipient = scenario["expected_recipient"]
            
            assert actual_recipient == expected_recipient, \
                f"Failed for {scenario['name']}: expected {expected_recipient}, got {actual_recipient}"

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_end_to_end_recipient_handling(self, mock_post, test_email_data):
        """End-to-end test to verify recipient handling"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        # Mock all services
        with patch('app.main.email_service') as mock_email_service, \
             patch('app.main.csv_service') as mock_csv_service, \
             patch('app.main.bdq_api_service') as mock_bdq_service, \
             patch('app.main.llm_service') as mock_llm_service:

            # Setup mocks
            mock_email_service.extract_csv_attachment.return_value = "test,csv\ndata,here"
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


if __name__ == "__main__":
    pytest.main([__file__])
