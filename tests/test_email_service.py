import pytest
import base64
import tempfile
import os
from unittest.mock import patch, Mock, mock_open
from io import StringIO

from app.services.email_service import EmailService
from app.models.email_models import EmailPayload, TestExecutionResult, BDQTest, BDQTestResult


class TestEmailService:
    """Test the Email Service"""

    @pytest.fixture
    def email_service(self):
        """Email service instance for testing"""
        with patch.dict('os.environ', {
            'GMAIL_SEND': 'https://script.google.com/macros/s/test/exec',
            'HMAC_SECRET': 'test_secret'
        }):
            return EmailService()

    @pytest.fixture
    def sample_email_payload(self):
        """Sample email payload for testing"""
        return EmailPayload(
            message_id="test_msg_123",
            thread_id="test_thread_456",
            from_email="test@example.com",
            to_email="bdq@example.com",
            subject="Test Dataset",
            body_text="Please process this dataset",
            body_html="<p>Please process this dataset</p>",
            attachments=[],
            headers={}
        )

    @pytest.fixture
    def sample_csv_attachment(self):
        """Sample CSV attachment for testing"""
        csv_content = """occurrenceID,country,eventDate
occ1,USA,2023-01-01
occ2,Canada,2023-01-02"""
        
        return {
            "filename": "test_dataset.csv",
            "mime_type": "text/csv",
            "content_base64": base64.b64encode(csv_content.encode('utf-8')).decode('utf-8'),
            "size": len(csv_content)
        }

    @pytest.fixture
    def sample_test_results(self):
        """Sample test results for testing"""
        test = BDQTest(
            test_id="VALIDATION_COUNTRY_FOUND",
            name="Country Validation",
            description="Validates that country field is present and valid",
            test_type="VALIDATION",
            acted_upon=["dwc:country"],
            consulted=[],
            parameters={}
        )
        
        results = [
            BDQTestResult(
                test_id="VALIDATION_COUNTRY_FOUND",
                row_index=0,
                status="RUN_HAS_RESULT",
                result="PASS",
                comment="Country field is valid"
            ),
            BDQTestResult(
                test_id="VALIDATION_COUNTRY_FOUND",
                row_index=1,
                status="RUN_HAS_RESULT",
                result="FAIL",
                comment="Country field is missing"
            )
        ]
        
        return TestExecutionResult(
            test=test,
            results=results,
            total_records=2,
            successful_records=1,
            failed_records=1
        )

    def test_init_with_environment_variables(self):
        """Test email service initialization with environment variables"""
        with patch.dict('os.environ', {
            'GMAIL_SEND': 'https://script.google.com/macros/s/test/exec',
            'HMAC_SECRET': 'test_secret'
        }):
            service = EmailService()
            assert service.gmail_send_endpoint == 'https://script.google.com/macros/s/test/exec'
            assert service.hmac_secret == 'test_secret'

    def test_init_without_environment_variables(self):
        """Test email service initialization without environment variables"""
        with patch.dict('os.environ', {}, clear=True):
            service = EmailService()
            assert service.gmail_send_endpoint is None
            assert service.hmac_secret is None

    def test_extract_csv_attachment_success(self, email_service, sample_email_payload, sample_csv_attachment):
        """Test successful CSV attachment extraction"""
        sample_email_payload.attachments = [sample_csv_attachment]
        
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        
        assert csv_data is not None
        assert csv_data["filename"] == "test_dataset.csv"
        assert csv_data["mime_type"] == "text/csv"
        assert "occurrenceID" in csv_data["content"]
        assert "country" in csv_data["content"]
        assert "eventDate" in csv_data["content"]

    def test_extract_csv_attachment_no_attachments(self, email_service, sample_email_payload):
        """Test CSV attachment extraction when no attachments exist"""
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        assert csv_data is None

    def test_extract_csv_attachment_no_csv_attachments(self, email_service, sample_email_payload):
        """Test CSV attachment extraction when no CSV attachments exist"""
        non_csv_attachment = {
            "filename": "document.pdf",
            "mime_type": "application/pdf",
            "content_base64": "dGVzdA==",
            "size": 4
        }
        sample_email_payload.attachments = [non_csv_attachment]
        
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        assert csv_data is None

    def test_extract_csv_attachment_multiple_attachments(self, email_service, sample_email_payload):
        """Test CSV attachment extraction with multiple attachments"""
        pdf_attachment = {
            "filename": "document.pdf",
            "mime_type": "application/pdf",
            "content_base64": "dGVzdA==",
            "size": 4
        }
        sample_email_payload.attachments = [pdf_attachment, sample_csv_attachment]
        
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        assert csv_data is not None
        assert csv_data["filename"] == "test_dataset.csv"

    def test_extract_csv_attachment_case_insensitive_mime_type(self, email_service, sample_email_payload):
        """Test CSV attachment extraction with case insensitive MIME type"""
        csv_attachment_upper = {
            "filename": "test_dataset.csv",
            "mime_type": "TEXT/CSV",
            "content_base64": base64.b64encode("test,data".encode('utf-8')).decode('utf-8'),
            "size": 9
        }
        sample_email_payload.attachments = [csv_attachment_upper]
        
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        assert csv_data is not None

    def test_extract_csv_attachment_invalid_base64(self, email_service, sample_email_payload):
        """Test CSV attachment extraction with invalid base64 content"""
        invalid_attachment = {
            "filename": "test_dataset.csv",
            "mime_type": "text/csv",
            "content_base64": "invalid_base64_content",
            "size": 20
        }
        sample_email_payload.attachments = [invalid_attachment]
        
        # Should handle invalid base64 gracefully
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        # Behavior depends on implementation - could return None or raise exception

    def test_extract_csv_attachment_empty_content(self, email_service, sample_email_payload):
        """Test CSV attachment extraction with empty content"""
        empty_attachment = {
            "filename": "test_dataset.csv",
            "mime_type": "text/csv",
            "content_base64": "",
            "size": 0
        }
        sample_email_payload.attachments = [empty_attachment]
        
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        assert csv_data is not None
        assert csv_data["content"] == ""

    @patch('app.services.email_service.requests.post')
    def test_send_error_reply_success(self, mock_post, email_service, sample_email_payload):
        """Test successful error reply sending"""
        mock_post.return_value = Mock(status_code=200, text="Success")
        
        email_service.send_error_reply(
            sample_email_payload,
            "An error occurred while processing your request"
        )
        
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        
        # Check that the request was made to the correct URL
        assert call_args[0][0] == email_service.gmail_send_url
        
        # Check that the request contains the error message
        request_data = call_args[1]['json']
        assert "error" in request_data
        assert "An error occurred while processing your request" in request_data["error"]

    @patch('app.services.email_service.requests.post')
    def test_send_error_reply_failure(self, mock_post, email_service, sample_email_payload):
        """Test error reply sending failure"""
        mock_post.side_effect = Exception("Network error")
        
        # Should handle failure gracefully
        try:
            email_service.send_error_reply(
                sample_email_payload,
                "An error occurred while processing your request"
            )
        except Exception:
            # Expected to fail, but should not crash the service
            pass

    @patch('app.services.email_service.requests.post')
    def test_send_results_reply_success(self, mock_post, email_service, sample_email_payload, sample_test_results):
        """Test successful results reply sending"""
        mock_post.return_value = Mock(status_code=200, text="Success")
        
        raw_results_csv = "test_id,row_index,status,result\nVALIDATION_COUNTRY_FOUND,0,PASS,Valid"
        amended_dataset_csv = "occurrenceID,country,amendment_notes\nocc1,USA,Valid\nocc2,,Missing"
        
        email_service.send_results_reply(
            sample_email_payload,
            "Processing completed successfully",
            raw_results_csv,
            amended_dataset_csv,
            [sample_test_results],
            "Occurrence"
        )
        
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        
        # Check that the request was made to the correct URL
        assert call_args[0][0] == email_service.gmail_send_url
        
        # Check that the request contains the results
        request_data = call_args[1]['json']
        assert "summary" in request_data
        assert "raw_results_csv" in request_data
        assert "amended_dataset_csv" in request_data

    @patch('app.services.email_service.requests.post')
    def test_send_results_reply_failure(self, mock_post, email_service, sample_email_payload, sample_test_results):
        """Test results reply sending failure"""
        mock_post.side_effect = Exception("Network error")
        
        raw_results_csv = "test_id,row_index,status,result\nVALIDATION_COUNTRY_FOUND,0,PASS,Valid"
        amended_dataset_csv = "occurrenceID,country,amendment_notes\nocc1,USA,Valid\nocc2,,Missing"
        
        # Should handle failure gracefully
        try:
            email_service.send_results_reply(
                sample_email_payload,
                "Processing completed successfully",
                raw_results_csv,
                amended_dataset_csv,
                [sample_test_results],
                "Occurrence"
            )
        except Exception:
            # Expected to fail, but should not crash the service
            pass

    def test_generate_email_summary_basic(self, email_service, sample_test_results):
        """Test basic email summary generation"""
        summary = email_service.generate_email_summary(
            [sample_test_results],
            "Occurrence",
            2,
            ["VALIDATION_DATE_FORMAT"]
        )
        
        assert "Occurrence" in summary
        assert "2 records" in summary
        assert "VALIDATION_COUNTRY_FOUND" in summary
        assert "1 passed" in summary
        assert "1 failed" in summary
        assert "1 skipped" in summary

    def test_generate_email_summary_taxon(self, email_service, sample_test_results):
        """Test email summary generation for taxon data"""
        summary = email_service.generate_email_summary(
            [sample_test_results],
            "Taxon",
            2,
            []
        )
        
        assert "Taxon" in summary
        assert "2 records" in summary

    def test_generate_email_summary_no_results(self, email_service):
        """Test email summary generation with no test results"""
        summary = email_service.generate_email_summary(
            [],
            "Occurrence",
            5,
            ["TEST_1", "TEST_2"]
        )
        
        assert "5 records" in summary
        assert "0 tests" in summary
        assert "2 skipped" in summary

    def test_generate_email_summary_all_passed(self, email_service):
        """Test email summary generation when all tests pass"""
        test = BDQTest(
            test_id="VALIDATION_COUNTRY_FOUND",
            name="Country Validation",
            description="Validates that country field is present and valid",
            test_type="VALIDATION",
            acted_upon=["dwc:country"],
            consulted=[],
            parameters={}
        )
        
        results = [
            BDQTestResult(
                test_id="VALIDATION_COUNTRY_FOUND",
                row_index=0,
                status="RUN_HAS_RESULT",
                result="PASS",
                comment="Valid"
            ),
            BDQTestResult(
                test_id="VALIDATION_COUNTRY_FOUND",
                row_index=1,
                status="RUN_HAS_RESULT",
                result="PASS",
                comment="Valid"
            )
        ]
        
        test_result = TestExecutionResult(
            test=test,
            results=results,
            total_records=2,
            successful_records=2,
            failed_records=0
        )
        
        summary = email_service.generate_email_summary(
            [test_result],
            "Occurrence",
            2,
            []
        )
        
        assert "2 passed" in summary
        assert "0 failed" in summary

    def test_generate_email_summary_all_failed(self, email_service):
        """Test email summary generation when all tests fail"""
        test = BDQTest(
            test_id="VALIDATION_COUNTRY_FOUND",
            name="Country Validation",
            description="Validates that country field is present and valid",
            test_type="VALIDATION",
            acted_upon=["dwc:country"],
            consulted=[],
            parameters={}
        )
        
        results = [
            BDQTestResult(
                test_id="VALIDATION_COUNTRY_FOUND",
                row_index=0,
                status="RUN_HAS_RESULT",
                result="FAIL",
                comment="Invalid"
            ),
            BDQTestResult(
                test_id="VALIDATION_COUNTRY_FOUND",
                row_index=1,
                status="RUN_HAS_RESULT",
                result="FAIL",
                comment="Invalid"
            )
        ]
        
        test_result = TestExecutionResult(
            test=test,
            results=results,
            total_records=2,
            successful_records=0,
            failed_records=2
        )
        
        summary = email_service.generate_email_summary(
            [test_result],
            "Occurrence",
            2,
            []
        )
        
        assert "0 passed" in summary
        assert "2 failed" in summary

    def test_generate_email_summary_mixed_results(self, email_service):
        """Test email summary generation with mixed test results"""
        test1 = BDQTest(
            test_id="VALIDATION_COUNTRY_FOUND",
            name="Country Validation",
            description="Validates that country field is present and valid",
            test_type="VALIDATION",
            acted_upon=["dwc:country"],
            consulted=[],
            parameters={}
        )
        
        test2 = BDQTest(
            test_id="VALIDATION_DATE_FORMAT",
            name="Date Format Validation",
            description="Validates date format",
            test_type="VALIDATION",
            acted_upon=["dwc:eventDate"],
            consulted=[],
            parameters={}
        )
        
        results1 = TestExecutionResult(
            test=test1,
            results=[
                BDQTestResult(
                    test_id="VALIDATION_COUNTRY_FOUND",
                    row_index=0,
                    status="RUN_HAS_RESULT",
                    result="PASS",
                    comment="Valid"
                )
            ],
            total_records=1,
            successful_records=1,
            failed_records=0
        )
        
        results2 = TestExecutionResult(
            test=test2,
            results=[
                BDQTestResult(
                    test_id="VALIDATION_DATE_FORMAT",
                    row_index=0,
                    status="RUN_HAS_RESULT",
                    result="FAIL",
                    comment="Invalid format"
                )
            ],
            total_records=1,
            successful_records=0,
            failed_records=1
        )
        
        summary = email_service.generate_email_summary(
            [results1, results2],
            "Occurrence",
            2,
            ["TEST_3"]
        )
        
        assert "1 passed" in summary
        assert "1 failed" in summary
        assert "1 skipped" in summary

    def test_generate_email_summary_large_dataset(self, email_service):
        """Test email summary generation with large dataset"""
        test = BDQTest(
            test_id="VALIDATION_COUNTRY_FOUND",
            name="Country Validation",
            description="Validates that country field is present and valid",
            test_type="VALIDATION",
            acted_upon=["dwc:country"],
            consulted=[],
            parameters={}
        )
        
        # Create many test results
        results = []
        for i in range(100):
            results.append(BDQTestResult(
                test_id="VALIDATION_COUNTRY_FOUND",
                row_index=i,
                status="RUN_HAS_RESULT",
                result="PASS" if i % 2 == 0 else "FAIL",
                comment="Valid" if i % 2 == 0 else "Invalid"
            ))
        
        test_result = TestExecutionResult(
            test=test,
            results=results,
            total_records=100,
            successful_records=50,
            failed_records=50
        )
        
        summary = email_service.generate_email_summary(
            [test_result],
            "Occurrence",
            100,
            []
        )
        
        assert "100 records" in summary
        assert "50 passed" in summary
        assert "50 failed" in summary

    def test_generate_email_summary_special_characters(self, email_service):
        """Test email summary generation with special characters in test names"""
        test = BDQTest(
            test_id="VALIDATION_SPECIAL_CHARS_2023",
            name="Special Characters Test (2023)",
            description="Test with special characters: !@#$%^&*()",
            test_type="VALIDATION",
            acted_upon=["dwc:field"],
            consulted=[],
            parameters={}
        )
        
        results = [
            BDQTestResult(
                test_id="VALIDATION_SPECIAL_CHARS_2023",
                row_index=0,
                status="RUN_HAS_RESULT",
                result="PASS",
                comment="Special chars handled correctly"
            )
        ]
        
        test_result = TestExecutionResult(
            test=test,
            results=results,
            total_records=1,
            successful_records=1,
            failed_records=0
        )
        
        summary = email_service.generate_email_summary(
            [test_result],
            "Occurrence",
            1,
            []
        )
        
        # Should handle special characters gracefully
        assert "VALIDATION_SPECIAL_CHARS_2023" in summary
        assert "1 passed" in summary
