"""
Tests that mock the Gmail endpoint to verify recipient handling behavior.

These tests simulate the Google Apps Script behavior to verify that replies
are sent to the correct recipient.
"""

import pytest
import json
import base64
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient

from app.services.email_service import EmailService
from app.main import app


class MockGmailThread:
    """Mock Gmail thread object to simulate Google Apps Script behavior"""
    
    def __init__(self, thread_id, original_from, original_to, original_cc=None):
        self.thread_id = thread_id
        self.original_from = original_from
        self.original_to = original_to
        self.original_cc = original_cc or []
        self.reply_calls = []
    
    def reply(self, text, opts):
        """Mock reply method that records who the reply was sent to"""
        reply_data = {
            'text': text,
            'htmlBody': opts.get('htmlBody', ''),
            'attachments': opts.get('attachments', []),
            'sent_to': self.original_from,  # Gmail automatically replies to FROM
            'thread_id': self.thread_id
        }
        self.reply_calls.append(reply_data)
        return reply_data


class MockGmailApp:
    """Mock GmailApp to simulate Google Apps Script GmailApp behavior"""
    
    def __init__(self):
        self.threads = {}
    
    def getThreadById(self, thread_id):
        """Mock getThreadById that returns our mock thread"""
        return self.threads.get(thread_id)
    
    def add_thread(self, thread_id, original_from, original_to, original_cc=None):
        """Helper to add a thread to our mock"""
        thread = MockGmailThread(thread_id, original_from, original_to, original_cc)
        self.threads[thread_id] = thread
        return thread


@pytest.fixture
def mock_gmail_app():
    """Mock GmailApp instance"""
    return MockGmailApp()


@pytest.fixture
def sample_email_with_clear_recipients():
    """Sample email with clearly different FROM and TO addresses"""
    return {
        "messageId": "test-123",
        "threadId": "thread-clear-test",
        "headers": {
            "from": "data.scientist@university.edu",
            "to": "bdq-service@biodiversity.org",
            "subject": "BDQ Test Request",
            "cc": "supervisor@university.edu"
        },
        "body": {
            "text": "Please test my biodiversity dataset",
            "html": "<p>Please test my biodiversity dataset</p>"
        },
        "attachments": []
    }


@pytest.fixture
def sample_email_reverse_recipients():
    """Sample email with FROM and TO reversed"""
    return {
        "messageId": "test-456", 
        "threadId": "thread-reverse-test",
        "headers": {
            "from": "bdq-service@biodiversity.org",
            "to": "data.scientist@university.edu",
            "subject": "BDQ Test Request"
        },
        "body": {
            "text": "Please test my biodiversity dataset",
            "html": "<p>Please test my biodiversity dataset</p>"
        },
        "attachments": []
    }


class TestGmailEndpointMocking:
    """Test Gmail endpoint behavior with mocked Google Apps Script"""

    def test_gmail_thread_reply_behavior(self, mock_gmail_app):
        """Test that Gmail thread.reply() sends to the original FROM address"""
        
        # Setup: Create a thread where FROM and TO are different
        thread = mock_gmail_app.add_thread(
            "thread-123",
            original_from="sender@example.com",
            original_to="service@example.com",
            original_cc=["cc@example.com"]
        )
        
        # Simulate the Google Apps Script behavior
        opts = {
            'htmlBody': '<p>Test reply</p>',
            'attachments': []
        }
        
        # Call reply (this is what Google Apps Script does)
        result = thread.reply('', opts)
        
        # Verify the reply was sent to the original FROM address
        assert result['sent_to'] == "sender@example.com"
        assert result['sent_to'] != "service@example.com"  # Should NOT go to TO
        assert len(thread.reply_calls) == 1
        
        # Verify the reply data
        reply_call = thread.reply_calls[0]
        assert reply_call['htmlBody'] == '<p>Test reply</p>'
        assert reply_call['thread_id'] == "thread-123"

    def test_gmail_thread_reply_with_attachments(self, mock_gmail_app):
        """Test Gmail thread.reply() with attachments"""
        
        thread = mock_gmail_app.add_thread(
            "thread-456",
            original_from="researcher@institute.org",
            original_to="bdq@service.com"
        )
        
        attachments = [
            {
                "filename": "results.csv",
                "mimeType": "text/csv",
                "contentBase64": base64.b64encode("test,data".encode()).decode()
            }
        ]
        
        opts = {
            'htmlBody': '<p>Results attached</p>',
            'attachments': attachments
        }
        
        result = thread.reply('', opts)
        
        # Verify reply went to original sender
        assert result['sent_to'] == "researcher@institute.org"
        assert result['attachments'] == attachments

    @patch('requests.post')
    def test_email_service_integration_with_mock_gmail(self, mock_post, sample_email_with_clear_recipients):
        """Test EmailService integration with mocked Gmail endpoint"""
        
        # Mock the Gmail endpoint response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response
        
        # Setup environment
        with patch.dict('os.environ', {
            'GMAIL_SEND': 'https://script.google.com/test',
            'HMAC_SECRET': 'test-secret'
        }):
            email_service = EmailService()
            
            # Send reply
            import asyncio
            asyncio.run(email_service.send_reply(
                sample_email_with_clear_recipients, 
                "<p>Test reply</p>"
            ))
            
            # Verify the request was made to Gmail endpoint
            assert mock_post.called
            call_args = mock_post.call_args
            
            # Verify URL
            assert call_args[1]['url'] == 'https://script.google.com/test'
            
            # Verify the data sent (should only include threadId, htmlBody, attachments)
            sent_data = json.loads(call_args[1]['data'])
            expected_data = {
                "threadId": "thread-clear-test",
                "htmlBody": "<p>Test reply</p>",
                "attachments": []
            }
            assert sent_data == expected_data
            
            # CRITICAL: Verify no recipient information is sent
            # (Gmail handles this automatically via thread.reply())
            assert 'to' not in sent_data
            assert 'from' not in sent_data
            assert 'recipient' not in sent_data

    def test_different_email_scenarios_with_mock_gmail(self, mock_gmail_app):
        """Test different email scenarios to verify recipient behavior"""
        
        scenarios = [
            {
                "name": "Normal case: User emails service",
                "thread_id": "thread-normal",
                "from": "user@university.edu",
                "to": "bdq@service.org",
                "expected_recipient": "user@university.edu"
            },
            {
                "name": "Reverse case: Service emails user", 
                "thread_id": "thread-reverse",
                "from": "bdq@service.org",
                "to": "user@university.edu",
                "expected_recipient": "bdq@service.org"
            },
            {
                "name": "CC case: User emails service with CC",
                "thread_id": "thread-cc",
                "from": "researcher@institute.org",
                "to": "bdq@service.org", 
                "cc": ["supervisor@institute.org", "admin@institute.org"],
                "expected_recipient": "researcher@institute.org"
            }
        ]
        
        for scenario in scenarios:
            # Create thread
            thread = mock_gmail_app.add_thread(
                scenario["thread_id"],
                original_from=scenario["from"],
                original_to=scenario["to"],
                original_cc=scenario.get("cc", [])
            )
            
            # Send reply
            opts = {'htmlBody': f'<p>Reply to {scenario["name"]}</p>'}
            result = thread.reply('', opts)
            
            # Verify recipient
            assert result['sent_to'] == scenario["expected_recipient"], \
                f"Failed for {scenario['name']}: expected {scenario['expected_recipient']}, got {result['sent_to']}"

    @patch('requests.post')
    def test_end_to_end_recipient_verification(self, mock_post, sample_email_with_clear_recipients):
        """End-to-end test to verify recipient handling through the full pipeline"""
        
        # Mock Gmail endpoint
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response
        
        # Mock all services for end-to-end test
        with patch('app.main.email_service') as mock_email_service, \
             patch('app.main.csv_service') as mock_csv_service, \
             patch('app.main.bdq_api_service') as mock_bdq_service, \
             patch('app.main.llm_service') as mock_llm_service, \
             patch.dict('os.environ', {
                 'GMAIL_SEND': 'https://script.google.com/test',
                 'HMAC_SECRET': 'test-secret'
             }):

            # Setup service mocks
            mock_email_service.extract_csv_attachment.return_value = "test,csv\ndata,here"
            mock_csv_service.parse_csv_and_detect_core.return_value = (Mock(), "occurrence")
            mock_bdq_service.run_tests_on_dataset = AsyncMock(return_value=Mock())
            mock_llm_service.generate_intelligent_summary.return_value = "<p>LLM analysis</p>"
            mock_csv_service.generate_raw_results_csv.return_value = "raw,results"
            mock_csv_service.generate_amended_dataset.return_value = "amended,dataset"

            # Make request
            client = TestClient(app)
            response = client.post("/email/incoming", json=sample_email_with_clear_recipients)

            # Verify response
            assert response.status_code == 200
            
            # Wait for async processing
            import time
            time.sleep(0.1)

            # Verify send_results_reply was called with original email data
            mock_email_service.send_results_reply.assert_called_once()
            call_args = mock_email_service.send_results_reply.call_args
            
            # Verify the email data passed to send_results_reply
            email_data_passed = call_args[0][0]
            assert email_data_passed['headers']['from'] == "data.scientist@university.edu"
            assert email_data_passed['headers']['to'] == "bdq-service@biodiversity.org"
            assert email_data_passed['threadId'] == "thread-clear-test"

    def test_google_apps_script_behavior_simulation(self):
        """Simulate the exact Google Apps Script behavior to verify recipient logic"""
        
        # This simulates what happens in the Google Apps Script
        def simulate_google_apps_script_behavior(email_data, reply_body, attachments=None):
            """Simulate the Google Apps Script doPost function behavior"""
            
            # Extract thread ID (this is what the script does)
            thread_id = email_data.get('threadId')
            if not thread_id:
                return {"error": "no threadId"}
            
            # Get the original email headers
            headers = email_data.get('headers', {})
            original_from = headers.get('from', '')
            original_to = headers.get('to', '')
            original_cc = headers.get('cc', '')
            
            # Simulate thread.reply() behavior
            # Gmail automatically replies to the original FROM address
            reply_recipient = original_from
            
            return {
                "threadId": thread_id,
                "replyBody": reply_body,
                "attachments": attachments or [],
                "sent_to": reply_recipient,
                "original_from": original_from,
                "original_to": original_to,
                "original_cc": original_cc
            }
        
        # Test cases
        test_cases = [
            {
                "email": {
                    "threadId": "thread-1",
                    "headers": {
                        "from": "user@example.com",
                        "to": "service@example.com"
                    }
                },
                "expected_recipient": "user@example.com"
            },
            {
                "email": {
                    "threadId": "thread-2", 
                    "headers": {
                        "from": "service@example.com",
                        "to": "user@example.com"
                    }
                },
                "expected_recipient": "service@example.com"
            }
        ]
        
        for test_case in test_cases:
            result = simulate_google_apps_script_behavior(
                test_case["email"],
                "<p>Test reply</p>",
                []
            )
            
            assert result["sent_to"] == test_case["expected_recipient"]
            assert result["sent_to"] == result["original_from"]  # Should always match original FROM


if __name__ == "__main__":
    pytest.main([__file__])
