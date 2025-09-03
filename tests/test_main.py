import pytest
import json
from unittest.mock import patch, Mock
from fastapi.testclient import TestClient

from app.main import app


class TestMainApp:
    """Test the main FastAPI application"""

    def test_root_endpoint(self, client):
        """Test the root health check endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        assert response.json()["message"] == "BDQ Email Report Service is running"

    def test_health_endpoint(self, client):
        """Test the detailed health check endpoint"""
        with patch('app.main.bdq_service.test_connection') as mock_test_conn:
            mock_test_conn.return_value = True
            
            response = client.get("/health")
            assert response.status_code == 200
            
            data = response.json()
            assert data["status"] == "healthy"
            assert data["service"] == "BDQ Email Report Service"
            assert data["version"] == "1.0.0"
            assert "services" in data
            assert "environment" in data
            assert data["services"]["bdq_cli_ready"] is True

    def test_health_endpoint_cli_failure(self, client):
        """Test health endpoint when CLI connection fails"""
        with patch('app.main.bdq_service.test_connection') as mock_test_conn:
            mock_test_conn.return_value = False
            
            response = client.get("/health")
            assert response.status_code == 200
            
            data = response.json()
            assert data["services"]["bdq_cli_ready"] is False

    def test_reject_get_email_incoming(self, client):
        """Test that GET requests to /email/incoming are rejected"""
        with patch('app.main.send_discord_notification') as mock_discord:
            response = client.get("/email/incoming")
            assert response.status_code == 405
            assert response.json()["detail"] == "Method Not Allowed"
            mock_discord.assert_called_once()

    def test_process_incoming_email_success(self, client):
        """Test successful email processing"""
        email_data = {
            "messageId": "test_msg_123",
            "threadId": "test_thread_456",
            "headers": {
                "from": "test@example.com",
                "to": "bdq@example.com",
                "subject": "Test Dataset"
            },
            "body": {
                "text": "Please process this dataset"
            },
            "attachments": []
        }
        
        with patch('app.main._handle_email_processing') as mock_handler:
            with patch('app.main.send_discord_notification') as mock_discord:
                response = client.post("/email/incoming", json=email_data)
                
                assert response.status_code == 200
                data = response.json()
                assert data["status"] == "accepted"
                assert data["message"] == "Email queued for processing"
                
                # Verify background task was scheduled
                mock_handler.assert_called_once()
                mock_discord.assert_called()

    def test_process_incoming_email_invalid_json(self, client):
        """Test email processing with invalid JSON"""
        with patch('app.main.send_discord_notification') as mock_discord:
            response = client.post("/email/incoming", data="invalid json")
            
            assert response.status_code == 200  # Still returns 200 to avoid blocking
            data = response.json()
            assert data["status"] == "error"
            assert "Invalid JSON payload" in data["message"]
            
            mock_discord.assert_called()

    def test_process_incoming_email_invalid_payload(self, client):
        """Test email processing with invalid email payload structure - still accepted but processed asynchronously"""
        email_data = {
            "invalid": "structure"
        }
        
        with patch('app.main.send_discord_notification') as mock_discord:
            response = client.post("/email/incoming", json=email_data)
            
            assert response.status_code == 200  # Still returns 200 to avoid blocking
            data = response.json()
            # The service accepts any valid JSON and processes it asynchronously
            # Invalid structure will be handled in background processing
            assert data["status"] == "accepted"
            assert "Email queued for processing" in data["message"]
            
            mock_discord.assert_called()

    def test_normalize_apps_script_payload(self, client):
        """Test Apps Script payload normalization"""
        from app.main import _normalize_apps_script_payload
        
        raw_data = {
            "messageId": "test_msg_123",
            "threadId": "test_thread_456",
            "headers": {
                "from": "test@example.com",
                "to": "bdq@example.com",
                "subject": "Test Dataset"
            },
            "body": {
                "text": "Please process this dataset",
                "html": "<p>Please process this dataset</p>"
            },
            "attachments": [
                {
                    "filename": "test.csv",
                    "mimeType": "text/csv",
                    "contentBase64": "dGVzdCBkYXRh",
                    "size": 100
                }
            ]
        }
        
        normalized = _normalize_apps_script_payload(raw_data)
        
        assert normalized["message_id"] == "test_msg_123"
        assert normalized["thread_id"] == "test_thread_456"
        assert normalized["from_email"] == "test@example.com"
        assert normalized["to_email"] == "bdq@example.com"
        assert normalized["subject"] == "Test Dataset"
        assert normalized["body_text"] == "Please process this dataset"
        assert normalized["body_html"] == "<p>Please process this dataset</p>"
        assert len(normalized["attachments"]) == 1
        assert normalized["attachments"][0]["filename"] == "test.csv"
        assert normalized["attachments"][0]["mime_type"] == "text/csv"

    def test_normalize_apps_script_payload_list_content(self, client):
        """Test Apps Script payload normalization with list content"""
        from app.main import _normalize_apps_script_payload
        
        raw_data = {
            "messageId": "test_msg_123",
            "headers": {},
            "body": {},
            "attachments": [
                {
                    "filename": "test.csv",
                    "mimeType": "text/csv",
                    "contentBase64": [116, 101, 115, 116, 32, 100, 97, 116, 97],  # "test data" as bytes
                    "size": 100
                }
            ]
        }
        
        normalized = _normalize_apps_script_payload(raw_data)
        
        assert len(normalized["attachments"]) == 1
        # The list content should be converted to base64 string
        assert normalized["attachments"][0]["content_base64"] == "dGVzdCBkYXRh"

    @pytest.mark.asyncio
    async def test_startup_event(self, client):
        """Test application startup event"""
        with patch('app.main.send_discord_notification') as mock_discord:
            with patch('app.main.bdq_service.test_connection') as mock_test_conn:
                mock_test_conn.return_value = True
                
                # Import and call the startup function directly
                from app.main import on_startup
                await on_startup()
                
                mock_discord.assert_called_with("Instance starting")
                mock_test_conn.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_event(self, client):
        """Test application shutdown event"""
        with patch('app.main.send_discord_notification') as mock_discord:
            # Import and call the shutdown function directly
            from app.main import on_shutdown
            await on_shutdown()
            
            mock_discord.assert_called_with("Instance shutting down")

    @pytest.mark.asyncio
    async def test_global_exception_handlers(self, client):
        """Test global exception handlers"""
        from fastapi import HTTPException
        from fastapi.exceptions import RequestValidationError
        from app.main import http_exception_handler, validation_exception_handler, unhandled_exception_handler
        
        # Test HTTP exception handler
        with patch('app.main.send_discord_notification') as mock_discord:
            request = Mock()
            exc = HTTPException(status_code=404, detail="Not found")
            
            response = await http_exception_handler(request, exc)
            assert response.status_code == 404
            assert response.body.decode() == '{"detail":"Not found"}'
            mock_discord.assert_called_with("HTTPException 404: Not found")

        # Test validation exception handler
        with patch('app.main.send_discord_notification') as mock_discord:
            request = Mock()
            exc = RequestValidationError(errors=[])
            
            response = await validation_exception_handler(request, exc)
            assert response.status_code == 422
            mock_discord.assert_called_with("Request validation error on incoming request")

        # Test unhandled exception handler
        with patch('app.main.send_discord_notification') as mock_discord:
            request = Mock()
            exc = Exception("Unexpected error")
            
            response = await unhandled_exception_handler(request, exc)
            assert response.status_code == 500
            assert response.body.decode() == '{"detail":"Internal server error"}'
            mock_discord.assert_called_with("Unhandled exception: Unexpected error")
