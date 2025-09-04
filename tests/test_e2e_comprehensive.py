"""
Comprehensive End-to-End Tests for BDQ Email Processing Service

These tests cover the complete email processing pipeline from email ingestion
through BDQ test execution to email reply generation.
"""
import pytest
import base64
import json
import tempfile
import os
import time
from pathlib import Path
from unittest.mock import patch, Mock, AsyncMock
from fastapi.testclient import TestClient

from app.main import app
from app.models.email_models import EmailPayload
from app.services.email_service import EmailService
from app.services.csv_service import CSVService
from app.services.bdq_py4j_service import BDQPy4JService


class TestEndToEndEmailProcessing:
    """Comprehensive end-to-end tests for the complete email processing pipeline"""
    
    @pytest.fixture
    def test_data_dir(self):
        """Get the test data directory"""
        return Path(__file__).parent / "data"
    
    @pytest.fixture
    def client(self):
        """Test client for FastAPI app"""
        return TestClient(app)
    
    def load_csv_file(self, test_data_dir, filename):
        """Helper to load a CSV file and return base64 encoded content"""
        file_path = test_data_dir / filename
        with open(file_path, 'r') as f:
            content = f.read()
        return base64.b64encode(content.encode()).decode()
    
    def create_email_payload_with_csv(self, test_data_dir, csv_filename, subject="Test BDQ Processing"):
        """Helper to create an email payload with CSV attachment"""
        csv_content = self.load_csv_file(test_data_dir, csv_filename)
        
        return {
            "messageId": "test_msg_123",
            "threadId": "test_thread_456",
            "headers": {
                "from": "test@example.com",
                "to": "bdq@example.com",
                "subject": subject
            },
            "body": {
                "text": f"Please process this {csv_filename} dataset"
            },
            "attachments": [{
                "filename": csv_filename,
                "mimeType": "text/csv",
                "contentBase64": csv_content,
                "size": len(csv_content)
            }]
        }
    
    @patch('app.main.send_discord_notification')
    @patch('app.services.email_service.EmailService.send_results_reply')
    @patch('app.services.email_service.EmailService.send_error_reply')
    @patch('app.services.bdq_py4j_service.BDQPy4JService.execute_tests')
    @patch('app.services.bdq_py4j_service.BDQPy4JService.filter_applicable_tests')
    def test_complete_occurrence_processing_pipeline(self, mock_filter_tests, mock_execute_tests, mock_send_error, 
                                                   mock_send_results, mock_discord, 
                                                   client, test_data_dir):
        """Test the complete pipeline for occurrence data processing"""
        # Setup mock test results
        mock_test_result = Mock()
        mock_test_result.test_id = "VALIDATION_COUNTRY_FOUND"
        mock_test_result.record_id = "occ1"
        mock_test_result.status = "RUN_HAS_RESULT"
        mock_test_result.result = "PASS"
        mock_test_result.comment = "Country found"
        mock_test_result.amendment = None
        mock_test_result.test_type = "VALIDATION"
        
        mock_execution_result = Mock()
        mock_execution_result.test_results = [mock_test_result]
        mock_execution_result.skipped_tests = []
        mock_execution_result.execution_time = 1.5
        
        # Mock filter_applicable_tests to return some tests
        mock_test = Mock()
        mock_test.test_id = "VALIDATION_COUNTRY_FOUND"
        mock_filter_tests.return_value = [mock_test]
        
        mock_execute_tests.return_value = mock_execution_result
        mock_send_results.return_value = None
        
        # Create email payload
        payload = self.create_email_payload_with_csv(test_data_dir, "simple_occurrence_dwc.csv")
        
        # Send request
        response = client.post("/email/incoming", json=payload)
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        assert "queued for processing" in data["message"]
        
        # Give some time for background processing
        time.sleep(0.2)
        
        # Verify Discord notifications were called
        assert mock_discord.call_count >= 1
        
        # Verify BDQ tests were executed
        mock_execute_tests.assert_called_once()
        
        # Verify results reply was sent
        mock_send_results.assert_called_once()
    
    @patch('app.main.send_discord_notification')
    @patch('app.services.email_service.EmailService.send_error_reply')
    def test_taxon_data_processing_pipeline(self, mock_send_error, mock_discord, client, test_data_dir):
        """Test the complete pipeline for taxon data processing"""
        payload = self.create_email_payload_with_csv(test_data_dir, "simple_taxon_dwc.csv")
        
        response = client.post("/email/incoming", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        
        # Give time for background processing
        time.sleep(0.1)
        
        # Verify Discord notifications were called
        assert mock_discord.call_count >= 1
    
    @patch('app.main.send_discord_notification')
    @patch('app.services.email_service.EmailService.send_error_reply')
    def test_no_csv_attachment_error_handling(self, mock_send_error, mock_discord, client):
        """Test error handling when no CSV attachment is provided"""
        payload = {
            "messageId": "test_msg_123",
            "threadId": "test_thread_456", 
            "headers": {
                "from": "test@example.com",
                "to": "bdq@example.com",
                "subject": "No attachment"
            },
            "body": {
                "text": "This email has no CSV attachment"
            },
            "attachments": []
        }
        
        response = client.post("/email/incoming", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        
        # Give time for background processing
        time.sleep(0.1)
        
        # Verify error reply was sent
        mock_send_error.assert_called_once()
    
    @patch('app.main.send_discord_notification')
    @patch('app.services.email_service.EmailService.send_error_reply')
    def test_invalid_csv_attachment_error_handling(self, mock_send_error, mock_discord, client):
        """Test error handling with invalid CSV data"""
        invalid_csv = base64.b64encode("This is not CSV data".encode()).decode()
        
        payload = {
            "messageId": "test_msg_123",
            "threadId": "test_thread_456",
            "headers": {
                "from": "test@example.com",
                "to": "bdq@example.com", 
                "subject": "Invalid CSV"
            },
            "body": {
                "text": "This has invalid CSV data"
            },
            "attachments": [{
                "filename": "invalid.csv",
                "mimeType": "text/csv",
                "contentBase64": invalid_csv,
                "size": len(invalid_csv)
            }]
        }
        
        response = client.post("/email/incoming", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        
        # Give time for background processing
        time.sleep(0.1)
        
        # Verify error reply was sent
        mock_send_error.assert_called_once()
    
    @patch('app.main.send_discord_notification')
    @patch('app.services.email_service.EmailService.send_error_reply')
    def test_unsupported_core_type_error_handling(self, mock_send_error, mock_discord, client):
        """Test error handling when CSV doesn't contain required core type columns"""
        # Create CSV without occurrenceID or taxonID
        csv_content = """name,description,value
test1,description1,value1
test2,description2,value2"""
        
        csv_b64 = base64.b64encode(csv_content.encode()).decode()
        
        payload = {
            "messageId": "test_msg_123",
            "threadId": "test_thread_456",
            "headers": {
                "from": "test@example.com",
                "to": "bdq@example.com",
                "subject": "Unsupported CSV"
            },
            "body": {
                "text": "This CSV doesn't have required columns"
            },
            "attachments": [{
                "filename": "unsupported.csv",
                "mimeType": "text/csv",
                "contentBase64": csv_b64,
                "size": len(csv_b64)
            }]
        }
        
        response = client.post("/email/incoming", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        
        # Give time for background processing
        time.sleep(0.1)
        
        # Verify error reply was sent
        mock_send_error.assert_called_once()
    
    def test_health_check_endpoint(self, client):
        """Test the health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "BDQ Email Report Service"
        assert "services" in data
        assert "environment" in data
    
    def test_root_endpoint(self, client):
        """Test the root endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        
        data = response.json()
        assert data["message"] == "BDQ Email Report Service is running"
    
    def test_get_email_incoming_rejection(self, client):
        """Test that GET requests to /email/incoming are properly rejected"""
        with patch('app.main.send_discord_notification') as mock_discord:
            response = client.get("/email/incoming")
            assert response.status_code == 405
            assert response.json()["detail"] == "Method Not Allowed"
            mock_discord.assert_called_once()
    
    def test_invalid_json_handling(self, client):
        """Test handling of invalid JSON in request body"""
        with patch('app.main.send_discord_notification') as mock_discord:
            response = client.post("/email/incoming", data="invalid json")
            
            assert response.status_code == 200  # Still returns 200 to avoid blocking
            data = response.json()
            assert data["status"] == "error"
            assert "Invalid JSON payload" in data["message"]
            # Discord is called twice: once for receiving request, once for JSON parse error
            assert mock_discord.call_count == 2


class TestCSVProcessingIntegration:
    """Integration tests for CSV processing components"""
    
    @pytest.fixture
    def test_data_dir(self):
        return Path(__file__).parent / "data"
    
    @pytest.fixture
    def csv_service(self):
        return CSVService()
    
    def test_csv_parsing_and_core_detection(self, csv_service, test_data_dir):
        """Test CSV parsing and core type detection with real data files"""
        # Test occurrence detection
        occ_csv_path = test_data_dir / "simple_occurrence_dwc.csv"
        with open(occ_csv_path, 'r') as f:
            occ_content = f.read()
        
        df, core_type = csv_service.parse_csv_and_detect_core(occ_content)
        assert core_type == "occurrence"
        assert len(df) > 0
        assert "occurrenceID" in df.columns
        
        # Test taxon detection  
        taxon_csv_path = test_data_dir / "simple_taxon_dwc.csv"
        with open(taxon_csv_path, 'r') as f:
            taxon_content = f.read()
            
        df, core_type = csv_service.parse_csv_and_detect_core(taxon_content)
        assert core_type == "taxon"
        assert len(df) > 0
        assert "taxonID" in df.columns
        
        # Test original occurrence.txt
        occ_txt_path = test_data_dir / "occurrence.txt"
        with open(occ_txt_path, 'r') as f:
            occ_txt_content = f.read()
            
        df, core_type = csv_service.parse_csv_and_detect_core(occ_txt_content)
        assert core_type == "occurrence"
        assert len(df) > 0
        assert "occurrenceID" in df.columns
    
    def test_csv_delimiter_detection(self, csv_service):
        """Test CSV delimiter detection with different formats"""
        # Test comma delimiter
        comma_csv = "occurrenceID,country,eventDate\nocc1,USA,2023-01-01\nocc2,Canada,2023-01-02"
        df, core_type = csv_service.parse_csv_and_detect_core(comma_csv)
        assert core_type == "occurrence"
        assert len(df) == 2
        
        # Test semicolon delimiter
        semicolon_csv = "occurrenceID;country;eventDate\nocc1;USA;2023-01-01\nocc2;Canada;2023-01-02"
        df, core_type = csv_service.parse_csv_and_detect_core(semicolon_csv)
        assert core_type == "occurrence"
        assert len(df) == 2
        
        # Test tab delimiter
        tab_csv = "occurrenceID\tcountry\teventDate\nocc1\tUSA\t2023-01-01\nocc2\tCanada\t2023-01-02"
        df, core_type = csv_service.parse_csv_and_detect_core(tab_csv)
        assert core_type == "occurrence"
        assert len(df) == 2
    
    def test_csv_with_special_characters(self, csv_service):
        """Test CSV parsing with special characters and unicode"""
        unicode_csv = """occurrenceID,country,locality
occ1,USA,San Francisco
occ2,Canada,Toronto
occ3,México,Guadalajara
occ4,España,Madrid"""
        
        df, core_type = csv_service.parse_csv_and_detect_core(unicode_csv)
        assert core_type == "occurrence"
        assert len(df) == 4
        assert df.iloc[2]["country"] == "México"
        assert df.iloc[3]["country"] == "España"
    
    def test_csv_with_quoted_fields(self, csv_service):
        """Test CSV parsing with quoted fields containing commas"""
        quoted_csv = """occurrenceID,country,locality
"occ1","USA","San Francisco, CA"
"occ2","Canada","Toronto, ON"
"occ3","Mexico","Guadalajara, JAL\""""
        
        df, core_type = csv_service.parse_csv_and_detect_core(quoted_csv)
        assert core_type == "occurrence"
        assert len(df) == 3
        assert df.iloc[0]["locality"] == "San Francisco, CA"


class TestBDQServiceIntegration:
    """Integration tests for BDQ service functionality"""
    
    def test_bdq_service_initialization(self):
        """Test BDQ service can be initialized without errors"""
        service = BDQPy4JService(skip_validation=True)
        assert service is not None
        assert len(service.test_mappings) > 0
    
    def test_test_availability(self):
        """Test that BDQ tests are available and properly loaded"""
        service = BDQPy4JService(skip_validation=True)
        tests = service.get_available_tests()
        
        assert len(tests) > 0
        
        # Check for some expected test types
        test_ids = [test.test_id for test in tests]
        
        # Should have some common validation tests
        common_tests = ["VALIDATION_COUNTRY_FOUND", "VALIDATION_COORDINATES_NOTEMPTY"]
        found_tests = [test_id for test_id in test_ids if any(common in test_id for common in common_tests)]
        
        # At least some tests should be found
        assert len(found_tests) > 0
    
    def test_column_mapping_issue_reproduction(self):
        """Test that reproduces the known column mapping issue"""
        service = BDQPy4JService(skip_validation=True)
        tests = service.get_available_tests()
        
        # Simulate columns from real-world data (without dwc: prefixes)
        real_world_columns = [
            "id", "modified", "license", "institutionID", "institutionCode", 
            "datasetName", "basisOfRecord", "dynamicProperties", "occurrenceID",
            "recordedBy", "associatedReferences", "organismID", "eventID", 
            "parentEventID", "year", "month", "samplingProtocol", "eventRemarks",
            "country", "countryCode", "stateProvince", "locality", 
            "minimumElevationInMeters", "maximumElevationInMeters", "verbatimElevation",
            "decimalLatitude", "decimalLongitude", "geodeticDatum", 
            "coordinateUncertaintyInMeters", "verbatimCoordinates", "verbatimLatitude",
            "verbatimLongitude", "verbatimCoordinateSystem", "verbatimSRS", 
            "georeferencedBy", "scientificName", "kingdom", "phylum", "class",
            "order", "family", "genus", "specificEpithet", "infraspecificEpithet",
            "taxonRank", "verbatimTaxonRank", "scientificNameAuthorship", "vernacularName"
        ]
        
        applicable_tests = service.get_applicable_tests(real_world_columns)
        
        # This demonstrates the known issue: good data gets 0 tests because of prefix mismatch
        # The current implementation requires exact column name matches
        print(f"Tests with real-world columns (no dwc: prefix): {len(applicable_tests)}")
        
        # Now test with dwc: prefixed versions of same columns
        dwc_prefixed_columns = [f"dwc:{col}" for col in real_world_columns]
        applicable_tests_prefixed = service.get_applicable_tests(dwc_prefixed_columns)
        
        print(f"Tests with dwc: prefixed columns: {len(applicable_tests_prefixed)}")
        
        # This test documents the current behavior - the fix would involve
        # normalizing column names or test mappings to handle both formats
        assert len(applicable_tests_prefixed) >= len(applicable_tests)


class TestEmailServiceIntegration:
    """Integration tests for email service functionality"""
    
    @pytest.fixture
    def test_data_dir(self):
        return Path(__file__).parent / "data"
    
    @pytest.fixture
    def email_service(self):
        with patch.dict('os.environ', {
            'GMAIL_SEND': 'https://script.google.com/macros/s/test/exec',
            'HMAC_SECRET': 'test_secret'
        }):
            return EmailService()
    
    def test_csv_attachment_extraction(self, email_service, test_data_dir):
        """Test CSV attachment extraction from email payload"""
        # Create test email with CSV
        csv_path = test_data_dir / "simple_occurrence_dwc.csv"
        with open(csv_path, 'r') as f:
            csv_content = f.read()
        csv_b64 = base64.b64encode(csv_content.encode()).decode()
        
        email_data = EmailPayload(
            message_id="test_msg",
            thread_id="test_thread",
            from_email="test@example.com",
            to_email="bdq@example.com",
            subject="Test",
            body_text="Test",
            attachments=[{
                "filename": "test.csv",
                "mime_type": "text/csv",
                "content_base64": csv_b64,
                "size": len(csv_b64)
            }],
            headers={}
        )
        
        extracted_csv = email_service.extract_csv_attachment(email_data)
        assert extracted_csv is not None
        assert "occurrenceID" in extracted_csv
    
    def test_csv_attachment_extraction_no_attachments(self, email_service):
        """Test CSV attachment extraction when no attachments exist"""
        email_data = EmailPayload(
            message_id="test_msg",
            thread_id="test_thread",
            from_email="test@example.com",
            to_email="bdq@example.com",
            subject="Test",
            body_text="Test",
            attachments=[],
            headers={}
        )
        
        extracted_csv = email_service.extract_csv_attachment(email_data)
        assert extracted_csv is None
    
    def test_csv_attachment_extraction_multiple_attachments(self, email_service):
        """Test CSV attachment extraction with multiple attachments"""
        # Create a CSV attachment inline
        csv_attachment = {
            "filename": "test_dataset.csv",
            "mime_type": "text/csv",
            "content_base64": base64.b64encode("test,data".encode('utf-8')).decode('utf-8'),
            "size": 9
        }
        
        pdf_attachment = {
            "filename": "document.pdf",
            "mime_type": "application/pdf",
            "content_base64": "dGVzdA==",
            "size": 4
        }
        
        email_data = EmailPayload(
            message_id="test_msg",
            thread_id="test_thread",
            from_email="test@example.com",
            to_email="bdq@example.com",
            subject="Test",
            body_text="Test",
            attachments=[pdf_attachment, csv_attachment],
            headers={}
        )
        
        extracted_csv = email_service.extract_csv_attachment(email_data)
        assert extracted_csv is not None
        assert "test" in extracted_csv
        assert "data" in extracted_csv


class TestErrorHandlingAndEdgeCases:
    """Test error handling and edge cases throughout the system"""
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    def test_malformed_email_payload(self, client):
        """Test handling of malformed email payloads"""
        with patch('app.main.send_discord_notification') as mock_discord:
            # Test with missing required fields
            malformed_payload = {
                "messageId": "test_msg_123",
                # Missing threadId, headers, etc.
            }
            
            response = client.post("/email/incoming", json=malformed_payload)
            
            assert response.status_code == 200  # Still returns 200 to avoid blocking
            data = response.json()
            assert data["status"] == "accepted"
            # Discord is called twice: once for receiving request, once for queuing
            assert mock_discord.call_count == 2
    
    def test_empty_email_payload(self, client):
        """Test handling of empty email payload"""
        with patch('app.main.send_discord_notification') as mock_discord:
            response = client.post("/email/incoming", json={})
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "accepted"
            # Discord is called twice: once for receiving request, once for queuing
            assert mock_discord.call_count == 2
    
    def test_very_large_csv_handling(self, client):
        """Test handling of very large CSV files"""
        # Create a large CSV (but not too large for testing)
        large_csv_data = "occurrenceID,country,eventDate\n"
        for i in range(1000):  # 1000 rows
            large_csv_data += f"occ{i},Country{i % 10},2023-01-{(i % 30) + 1:02d}\n"
        
        csv_b64 = base64.b64encode(large_csv_data.encode()).decode()
        
        payload = {
            "messageId": "test_msg_123",
            "threadId": "test_thread_456",
            "headers": {
                "from": "test@example.com",
                "to": "bdq@example.com",
                "subject": "Large CSV"
            },
            "body": {
                "text": "This is a large CSV file"
            },
            "attachments": [{
                "filename": "large.csv",
                "mimeType": "text/csv",
                "contentBase64": csv_b64,
                "size": len(csv_b64)
            }]
        }
        
        with patch('app.main.send_discord_notification') as mock_discord:
            response = client.post("/email/incoming", json=payload)
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "accepted"
            # Discord is called multiple times: receiving request, queuing, and error processing
            assert mock_discord.call_count >= 2
    
    def test_csv_with_only_headers(self, client):
        """Test handling of CSV with only headers and no data"""
        headers_only_csv = "occurrenceID,country,eventDate"
        csv_b64 = base64.b64encode(headers_only_csv.encode()).decode()
        
        payload = {
            "messageId": "test_msg_123",
            "threadId": "test_thread_456",
            "headers": {
                "from": "test@example.com",
                "to": "bdq@example.com",
                "subject": "Headers Only CSV"
            },
            "body": {
                "text": "This CSV has only headers"
            },
            "attachments": [{
                "filename": "headers_only.csv",
                "mimeType": "text/csv",
                "contentBase64": csv_b64,
                "size": len(csv_b64)
            }]
        }
        
        with patch('app.main.send_discord_notification') as mock_discord:
            response = client.post("/email/incoming", json=payload)
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "accepted"
            # Discord is called multiple times: receiving request, queuing, and error processing
            assert mock_discord.call_count >= 2
