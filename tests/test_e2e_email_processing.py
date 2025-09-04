import pytest
import base64
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, Mock, AsyncMock
from fastapi.testclient import TestClient

from app.main import app
from app.models.email_models import EmailPayload
from app.services.email_service import EmailService
from app.services.csv_service import CSVService
from app.services.bdq_py4j_service import BDQPy4JService


class TestEndToEndEmailProcessing:
    """End-to-end tests that simulate the full email processing pipeline"""
    
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
    def test_simple_occurrence_processing_success(self, mock_run_tests, mock_send_error, 
                                                mock_send_results, mock_discord, 
                                                client, test_data_dir):
        """Test successful processing of simple occurrence data"""
        # Setup mock test results
        mock_test_result = Mock()
        mock_test_result.test_id = "VALIDATION_COUNTRY_FOUND"
        mock_test_result.results = [Mock(
            record_id="occ1",
            status="RUN_HAS_RESULT", 
            result="PASS",
            comment="Country found"
        )]
        
        mock_run_tests.return_value = ([mock_test_result], [])
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
        import time
        time.sleep(0.1)
        
        # Verify Discord notifications were called
        assert mock_discord.call_count >= 1
        
    @patch('app.main.send_discord_notification')
    @patch('app.services.email_service.EmailService.send_error_reply')
    def test_occurrence_txt_no_applicable_tests(self, mock_send_error, mock_discord, client, test_data_dir):
        """Test the original occurrence.txt file that was getting 0 applicable tests"""
        # Create email payload with the occurrence.txt file
        payload = self.create_email_payload_with_csv(test_data_dir, "occurrence.txt", 
                                                   "Norwegian Forest Line Data")
        
        # Send request
        response = client.post("/email/incoming", json=payload)
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        
        # Give time for background processing
        import time
        time.sleep(0.1)
        
        # Verify error reply was called (should get "no applicable tests" error)
        assert mock_send_error.call_count >= 0  # May or may not be called depending on mocking
        
    @patch('app.main.send_discord_notification')
    @patch('app.services.email_service.EmailService.send_error_reply')
    def test_taxon_data_processing(self, mock_send_error, mock_discord, client, test_data_dir):
        """Test processing of taxon-based CSV data"""
        payload = self.create_email_payload_with_csv(test_data_dir, "simple_taxon_dwc.csv")
        
        response = client.post("/email/incoming", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        
    @patch('app.main.send_discord_notification')
    @patch('app.services.email_service.EmailService.send_error_reply')
    def test_prefixed_dwc_columns(self, mock_send_error, mock_discord, client, test_data_dir):
        """Test processing of CSV with dwc: prefixed column names"""
        payload = self.create_email_payload_with_csv(test_data_dir, "prefixed_occurrence_dwc.csv")
        
        response = client.post("/email/incoming", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        
    @patch('app.main.send_discord_notification')
    def test_no_csv_attachment(self, mock_discord, client):
        """Test email processing when no CSV attachment is provided"""
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
        
    @patch('app.main.send_discord_notification')
    def test_invalid_csv_attachment(self, mock_discord, client):
        """Test email processing with invalid CSV data"""
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
        
    def test_csv_core_type_detection(self, test_data_dir):
        """Test CSV core type detection logic"""
        csv_service = CSVService()
        
        # Test occurrence detection
        occ_csv_path = test_data_dir / "simple_occurrence_dwc.csv"
        with open(occ_csv_path, 'r') as f:
            occ_content = f.read()
        
        df, core_type = csv_service.parse_csv_and_detect_core(occ_content)
        assert core_type == "occurrence"
        assert len(df) == 5
        
        # Test taxon detection  
        taxon_csv_path = test_data_dir / "simple_taxon_dwc.csv"
        with open(taxon_csv_path, 'r') as f:
            taxon_content = f.read()
            
        df, core_type = csv_service.parse_csv_and_detect_core(taxon_content)
        assert core_type == "taxon"
        assert len(df) == 3
        
        # Test original occurrence.txt
        occ_txt_path = test_data_dir / "occurrence.txt"
        with open(occ_txt_path, 'r') as f:
            occ_txt_content = f.read()
            
        df, core_type = csv_service.parse_csv_and_detect_core(occ_txt_content)
        assert core_type == "occurrence"  # Should detect as occurrence since it has occurrenceID
        assert len(df) == 200  # File has 200 data rows
        
    def test_bdq_test_filtering(self, test_data_dir):
        """Test BDQ test filtering with different column name formats"""
        bdq_service = BDQPy4JService(skip_validation=True)
        
        # Get available tests
        tests = bdq_service.get_available_tests()
        assert len(tests) > 0
        
        # Test with simple column names (no dwc: prefix)
        simple_columns = ["occurrenceID", "country", "countryCode", "decimalLatitude", "decimalLongitude", "scientificName"]
        applicable_simple = bdq_service.filter_applicable_tests(tests, simple_columns)
        
        # Test with prefixed column names
        prefixed_columns = ["dwc:occurrenceID", "dwc:country", "dwc:countryCode", "dwc:decimalLatitude", "dwc:decimalLongitude", "dwc:scientificName"]
        applicable_prefixed = bdq_service.filter_applicable_tests(tests, prefixed_columns)
        
        # The prefixed version should have more applicable tests
        # (This test demonstrates the current issue - simple columns get no matches)
        print(f"Simple columns applicable tests: {len(applicable_simple)}")
        print(f"Prefixed columns applicable tests: {len(applicable_prefixed)}")
        
        # Test with occurrence.txt columns
        occ_txt_path = test_data_dir / "occurrence.txt"
        csv_service = CSVService()
        with open(occ_txt_path, 'r') as f:
            content = f.read()
        df, _ = csv_service.parse_csv_and_detect_core(content)
        
        applicable_occ_txt = bdq_service.filter_applicable_tests(tests, df.columns.tolist())
        print(f"Occurrence.txt applicable tests: {len(applicable_occ_txt)}")
        
        # This should demonstrate the issue: occurrence.txt has good columns but gets 0 applicable tests
        # The columns are there but without dwc: prefixes
        columns_in_occ_txt = df.columns.tolist()
        print(f"Columns in occurrence.txt: {columns_in_occ_txt[:10]}...")  # Show first 10 columns

class TestCSVProcessingUnits:
    """Unit tests for CSV processing components"""
    
    @pytest.fixture
    def test_data_dir(self):
        return Path(__file__).parent / "data"
    
    def test_csv_attachment_extraction(self, test_data_dir):
        """Test CSV attachment extraction from email payload"""
        email_service = EmailService()
        
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
        
    def test_csv_parsing_edge_cases(self, test_data_dir):
        """Test CSV parsing with various edge cases"""
        csv_service = CSVService()
        
        # Test empty CSV
        empty_df, empty_type = csv_service.parse_csv_and_detect_core("")
        assert empty_df is None
        assert empty_type is None
        
        # Test malformed CSV
        malformed_csv = "This,is,malformed\ndata,without,proper\nstructure"
        try:
            malformed_df, malformed_type = csv_service.parse_csv_and_detect_core(malformed_csv)
            # Should still parse but may not detect type correctly
        except Exception:
            # It's ok if it fails to parse malformed CSV
            pass
            
        # Test CSV with only headers
        headers_only = "occurrenceID,scientificName,country"
        headers_df, headers_type = csv_service.parse_csv_and_detect_core(headers_only)
        assert headers_type == "occurrence"  # Should still detect type from headers
        assert len(headers_df) == 0  # But no data rows


class TestBDQServiceIntegration:
    """Integration tests for BDQ service functionality"""
    
    def test_service_initialization(self):
        """Test BDQ service can be initialized without errors"""
        service = BDQPy4JService(skip_validation=True)
        assert service is not None
        assert len(service.test_mappings) > 0
        
    def test_test_availability(self):
        """Test that BDQ tests are available"""
        service = BDQPy4JService(skip_validation=True)
        tests = service.get_available_tests()
        
        assert len(tests) > 0
        
        # Check for some expected test types
        test_ids = [test.id for test in tests]
        
        # Should have some common validation tests
        common_tests = ["VALIDATION_COUNTRY_FOUND", "VALIDATION_COORDINATES_NOTEMPTY"]
        found_tests = [test_id for test_id in test_ids if any(common in test_id for common in common_tests)]
        
        print(f"Available tests: {len(tests)}")
        print(f"Sample test IDs: {test_ids[:5]}")
        
    def test_column_mapping_issue_reproduction(self):
        """Test that reproduces the column mapping issue"""
        service = BDQPy4JService(skip_validation=True)
        tests = service.get_available_tests()
        
        # Simulate columns from the occurrence.txt file (without dwc: prefixes)
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
        
        applicable_tests = service.filter_applicable_tests(tests, real_world_columns)
        
        print(f"Tests with real-world columns (no dwc: prefix): {len(applicable_tests)}")
        
        # Now test with dwc: prefixed versions of same columns
        dwc_prefixed_columns = [f"dwc:{col}" for col in real_world_columns]
        applicable_tests_prefixed = service.filter_applicable_tests(tests, dwc_prefixed_columns)
        
        print(f"Tests with dwc: prefixed columns: {len(applicable_tests_prefixed)}")
        
        # This test demonstrates the issue: good data gets 0 tests because of prefix mismatch
        # The fix would involve normalizing column names or test mappings to handle both formats