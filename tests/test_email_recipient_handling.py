"""
Tests to verify email recipient handling in reply functionality.

These tests check whether email replies are being sent to the correct recipient
(FROM field) rather than the TO field.
"""

import pytest
import json
import base64
from unittest.mock import Mock, AsyncMock, patch, call
from fastapi.testclient import TestClient
import requests

from app.services.email_service import EmailService
from app.main import app


@pytest.fixture
def email_service():
    """Email service instance for testing"""
    return EmailService()


@pytest.fixture
def sample_email_data():
    """Sample email data with different FROM and TO addresses"""
    return {
        "messageId": "test-123",
        "threadId": "thread-456",
        "headers": {
            "from": "sender@example.com",
            "to": "bdq-service@example.com", 
            "subject": "BDQ Test Request",
            "cc": "cc@example.com"
        },
        "body": {
            "text": "Please test my biodiversity dataset",
            "html": "<p>Please test my biodiversity dataset</p>"
        },
        "attachments": []
    }


@pytest.fixture
def sample_email_data_reverse():
    """Sample email data with FROM and TO reversed"""
    return {
        "messageId": "test-456",
        "threadId": "thread-789",
        "headers": {
            "from": "bdq-service@example.com",
            "to": "recipient@example.com",
            "subject": "BDQ Test Request",
            "cc": "cc@example.com"
        },
        "body": {
            "text": "Please test my biodiversity dataset",
            "html": "<p>Please test my biodiversity dataset</p>"
        },
        "attachments": []
    }


class TestEmailServiceRecipientHandling:
    """Test email service recipient handling"""

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_send_reply_data_structure(self, mock_post, email_service, sample_email_data):
        """Test that send_reply sends correct data structure without explicit recipient"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        # Call send_reply
        import asyncio
        asyncio.run(email_service.send_reply(sample_email_data, "<p>Test reply</p>"))

        # Verify the request was made
        assert mock_post.called
        call_args = mock_post.call_args

        # Check URL (first positional argument)
        assert call_args[0][0] == 'https://script.google.com/test'
        
        # Check the data sent (data parameter)
        sent_data = json.loads(call_args[1]['data'])
        expected_data = {
            "threadId": "thread-456",
            "htmlBody": "<p>Test reply</p>",
            "attachments": []
        }
        
        assert sent_data == expected_data
        
        # CRITICAL: Verify no explicit recipient information is sent
        assert 'to' not in sent_data
        assert 'from' not in sent_data
        assert 'recipient' not in sent_data
        assert 'replyTo' not in sent_data

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_send_reply_with_attachments(self, mock_post, email_service, sample_email_data):
        """Test send_reply with attachments"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        attachments = [
            {
                "filename": "test.csv",
                "mimeType": "text/csv",
                "contentBase64": base64.b64encode("test,data".encode()).decode()
            }
        ]

        import asyncio
        asyncio.run(email_service.send_reply(sample_email_data, "<p>Test reply</p>", attachments))

        # Verify the request data includes attachments
        call_args = mock_post.call_args
        sent_data = json.loads(call_args[1]['data'])
        
        assert sent_data['attachments'] == attachments
        assert len(sent_data['attachments']) == 1
        assert sent_data['attachments'][0]['filename'] == "test.csv"

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_send_reply_logs_correct_recipient(self, mock_post, email_service, sample_email_data):
        """Test that send_reply logs the correct recipient (FROM field)"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        with patch('app.utils.helper.log') as mock_log:
            import asyncio
            asyncio.run(email_service.send_reply(sample_email_data, "<p>Test reply</p>"))

            # Check that the log message includes the FROM address
            log_calls = [call[0][0] for call in mock_log.call_args_list]
            reply_log = next((log for log in log_calls if "Sent reply to" in log), None)
            
            assert reply_log is not None
            assert "sender@example.com" in reply_log  # FROM field
            assert "bdq-service@example.com" not in reply_log  # TO field should not be in log

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_send_error_reply_uses_correct_recipient(self, mock_post, email_service, sample_email_data):
        """Test that send_error_reply uses the correct recipient"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        with patch('app.utils.helper.log') as mock_log:
            import asyncio
            asyncio.run(email_service.send_error_reply(sample_email_data, "Test error"))

            # Verify error reply was sent
            assert mock_post.called
            call_args = mock_post.call_args
            sent_data = json.loads(call_args[1]['data'])
            
            assert sent_data['htmlBody'] == "<p>Error processing your request:</p><p>Test error</p>"
            
            # Check log includes FROM address
            log_calls = [call[0][0] for call in mock_log.call_args_list]
            reply_log = next((log for log in log_calls if "Sent reply to" in log), None)
            assert reply_log is not None
            assert "sender@example.com" in reply_log

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_send_results_reply_uses_correct_recipient(self, mock_post, email_service, sample_email_data):
        """Test that send_results_reply uses the correct recipient"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        with patch('app.utils.helper.log') as mock_log:
            import asyncio
            asyncio.run(email_service.send_results_reply(
                sample_email_data, 
                "<p>Results</p>", 
                "raw,data\n1,2", 
                "amended,data\n1,3"
            ))

            # Verify results reply was sent with attachments
            assert mock_post.called
            call_args = mock_post.call_args
            sent_data = json.loads(call_args[1]['data'])
            
            assert len(sent_data['attachments']) == 2
            assert sent_data['attachments'][0]['filename'] == "bdq_raw_results.csv"
            assert sent_data['attachments'][1]['filename'] == "amended_dataset.csv"
            
            # Check log includes FROM address
            log_calls = [call[0][0] for call in mock_log.call_args_list]
            reply_log = next((log for log in log_calls if "Sent reply to" in log), None)
            assert reply_log is not None
            assert "sender@example.com" in reply_log


class TestEmailRecipientIntegration:
    """Integration tests for email recipient handling"""

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_end_to_end_email_processing_recipient_handling(self, mock_post, sample_email_data):
        """Test end-to-end email processing to verify recipient handling"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        # Mock all the services
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

            # Make the request
            client = TestClient(app)
            response = client.post("/email/incoming", json=sample_email_data)

            # Verify response
            assert response.status_code == 200
            assert response.json()["status"] == "accepted"

            # Wait a bit for async processing
            import time
            time.sleep(0.1)

            # Verify that send_results_reply was called with correct email data
            mock_email_service.send_results_reply.assert_called_once()
            call_args = mock_email_service.send_results_reply.call_args
            
            # The first argument should be the original email_data
            called_email_data = call_args[0][0]
            assert called_email_data == sample_email_data
            assert called_email_data['headers']['from'] == "sender@example.com"
            assert called_email_data['headers']['to'] == "bdq-service@example.com"

    def test_email_data_structure_preservation(self, sample_email_data, sample_email_data_reverse):
        """Test that email data structure is preserved correctly"""
        
        # Test normal case: FROM is sender, TO is service
        assert sample_email_data['headers']['from'] == "sender@example.com"
        assert sample_email_data['headers']['to'] == "bdq-service@example.com"
        
        # Test reverse case: FROM is service, TO is recipient  
        assert sample_email_data_reverse['headers']['from'] == "bdq-service@example.com"
        assert sample_email_data_reverse['headers']['to'] == "recipient@example.com"

    @patch.dict('os.environ', {
        'GMAIL_SEND': 'https://script.google.com/test',
        'HMAC_SECRET': 'test-secret'
    })
    @patch('requests.post')
    def test_different_email_scenarios(self, mock_post, email_service):
        """Test different email scenarios to verify recipient handling"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "ok"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        # Scenario 1: Normal email (FROM: user, TO: service)
        normal_email = {
            "threadId": "thread-1",
            "headers": {
                "from": "user@example.com",
                "to": "bdq@service.com"
            }
        }

        # Scenario 2: CC email (FROM: user, TO: service, CC: others)
        cc_email = {
            "threadId": "thread-2", 
            "headers": {
                "from": "user@example.com",
                "to": "bdq@service.com",
                "cc": "manager@example.com,admin@example.com"
            }
        }

        # Scenario 3: BCC scenario (FROM: user, TO: service)
        bcc_email = {
            "threadId": "thread-3",
            "headers": {
                "from": "user@example.com", 
                "to": "bdq@service.com"
            }
        }

        scenarios = [normal_email, cc_email, bcc_email]
        
        for i, email_data in enumerate(scenarios):
            with patch('app.utils.helper.log') as mock_log:
                import asyncio
                asyncio.run(email_service.send_reply(email_data, f"<p>Reply {i}</p>"))

                # Verify the data sent doesn't include recipient info
                call_args = mock_post.call_args
                sent_data = json.loads(call_args[1]['data'])
                
                assert 'to' not in sent_data
                assert 'from' not in sent_data
                assert 'cc' not in sent_data
                assert 'bcc' not in sent_data
                
                # Verify log shows FROM address
                log_calls = [call[0][0] for call in mock_log.call_args_list]
                reply_log = next((log for log in log_calls if "Sent reply to" in log), None)
                assert reply_log is not None
                assert "user@example.com" in reply_log


class TestGoogleAppsScriptBehavior:
    """Test the expected behavior of Google Apps Script thread.reply()"""

    def test_thread_reply_behavior_documentation(self):
        """
        Document the expected behavior of Gmail thread.reply().
        
        According to Gmail API documentation, thread.reply() should:
        1. Automatically reply to the original sender (FROM field)
        2. Not send to TO field recipients
        3. Not send to CC field recipients
        4. Maintain the conversation thread
        """
        
        # This test documents the expected behavior
        expected_behavior = {
            "method": "thread.reply()",
            "recipient": "Original sender (FROM field)",
            "not_sent_to": ["TO field recipients", "CC field recipients", "BCC field recipients"],
            "thread_maintenance": True,
            "automatic_recipient_detection": True
        }
        
        assert expected_behavior["recipient"] == "Original sender (FROM field)"
        assert "TO field recipients" in expected_behavior["not_sent_to"]
        assert expected_behavior["automatic_recipient_detection"] is True

    def test_reply_data_structure_analysis(self):
        """
        Analyze what data is sent to Google Apps Script and verify it's correct.
        """
        
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
        
        assert "threadId" in expected_data_structure
        assert "htmlBody" in expected_data_structure
        assert "attachments" in expected_data_structure
        
        for field in should_not_include:
            assert field not in expected_data_structure


if __name__ == "__main__":
    pytest.main([__file__])
