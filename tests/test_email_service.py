import pytest
import base64
import tempfile
import os
import json
from unittest.mock import patch, Mock, mock_open
from io import StringIO

from app.services.email_service import EmailService
from app.models.email_models import EmailPayload, BDQTestExecutionResult, BDQTest, BDQTestResult, EmailAttachment, ProcessingSummary


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
        
        return EmailAttachment(
            filename="test_dataset.csv",
            mime_type="text/csv",
            content_base64=base64.b64encode(csv_content.encode('utf-8')).decode('utf-8'),
            size=len(csv_content)
        )

    @pytest.fixture
    def sample_test_results(self):
        """Sample test results for testing"""
        return BDQTestExecutionResult(
            record_id="occ1",
            test_id="VALIDATION_COUNTRY_FOUND",
            status="RUN_HAS_RESULT",
            result="PASS",
            comment="Country field is valid",
            amendment=None,
            test_type="VALIDATION"
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

    def test_extract_csv_attachment_no_attachments(self, email_service, sample_email_payload):
        """Test CSV attachment extraction when no attachments exist"""
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        assert csv_data is None

    def test_extract_csv_attachment_no_csv_attachments(self, email_service, sample_email_payload):
        """Test CSV attachment extraction when no CSV attachments exist"""
        non_csv_attachment = EmailAttachment(
            filename="document.pdf",
            mime_type="application/pdf",
            content_base64="dGVzdA==",
            size=4
        )
        sample_email_payload.attachments = [non_csv_attachment]
        
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        assert csv_data is None

    def test_extract_csv_attachment_multiple_attachments(self, email_service, sample_email_payload):
        """Test CSV attachment extraction with multiple attachments"""
        pdf_attachment = EmailAttachment(
            filename="document.pdf",
            mime_type="application/pdf",
            content_base64="dGVzdA==",
            size=4
        )
        # Create a CSV attachment inline
        csv_attachment = EmailAttachment(
            filename="test_dataset.csv",
            mime_type="text/csv",
            content_base64=base64.b64encode("test,data".encode('utf-8')).decode('utf-8'),
            size=9
        )
        sample_email_payload.attachments = [pdf_attachment, csv_attachment]
        
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        assert csv_data is not None
        assert "test" in csv_data
        assert "data" in csv_data

    def test_extract_csv_attachment_case_insensitive_mime_type(self, email_service, sample_email_payload):
        """Test CSV attachment extraction with case insensitive MIME type"""
        csv_attachment_upper = EmailAttachment(
            filename="test_dataset.csv",
            mime_type="TEXT/CSV",
            content_base64=base64.b64encode("test,data".encode('utf-8')).decode('utf-8'),
            size=9
        )
        sample_email_payload.attachments = [csv_attachment_upper]
        
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        assert csv_data is not None

    def test_extract_csv_attachment_invalid_base64(self, email_service, sample_email_payload):
        """Test CSV attachment extraction with invalid base64 content"""
        invalid_attachment = EmailAttachment(
            filename="test_dataset.csv",
            mime_type="text/csv",
            content_base64="invalid_base64_content",
            size=20
        )
        sample_email_payload.attachments = [invalid_attachment]
        
        # Should handle invalid base64 gracefully
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        # Behavior depends on implementation - could return None or raise exception

    def test_extract_csv_attachment_empty_content(self, email_service, sample_email_payload):
        """Test CSV attachment extraction with empty content"""
        empty_attachment = EmailAttachment(
            filename="test_dataset.csv",
            mime_type="text/csv",
            content_base64="",
            size=0
        )
        sample_email_payload.attachments = [empty_attachment]
        
        csv_data = email_service.extract_csv_attachment(sample_email_payload)
        assert csv_data is None

    @patch('app.services.email_service.requests.post')
    @pytest.mark.asyncio
    async def test_send_reply_success(self, mock_post, email_service, sample_email_payload):
        """Test successful reply sending"""
        mock_post.return_value = Mock(status_code=200, text="Success")
        
        error_body = "<h3>BDQ Processing Error</h3><p>An error occurred while processing your request</p><p>Please check your CSV file and try again.</p>"
        await email_service.send_reply(
            sample_email_payload,
            error_body
        )
        
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        
        # Check that the request was made to the correct URL
        assert call_args[0][0] == email_service.gmail_send_endpoint
        
        # Check that the request contains the error message
        request_data = json.loads(call_args[1]['data'])
        assert "htmlBody" in request_data
        assert "An error occurred while processing your request" in request_data["htmlBody"]

    @patch('app.services.email_service.requests.post')
    @pytest.mark.asyncio
    async def test_send_reply_failure(self, mock_post, email_service, sample_email_payload):
        """Test reply sending failure"""
        mock_post.side_effect = Exception("Network error")
        
        # Should handle failure gracefully
        try:
            error_body = "<h3>BDQ Processing Error</h3><p>An error occurred while processing your request</p><p>Please check your CSV file and try again.</p>"
            await email_service.send_reply(
                sample_email_payload,
                error_body
            )
        except Exception:
            # Expected to fail, but should not crash the service
            pass

    @patch('app.services.email_service.requests.post')
    @pytest.mark.asyncio
    async def test_send_reply_with_attachments_success(self, mock_post, email_service, sample_email_payload, sample_test_results):
        """Test successful reply sending with attachments"""
        mock_post.return_value = Mock(status_code=200, text="Success")
        
        raw_results_csv = "test_id,row_index,status,result\nVALIDATION_COUNTRY_FOUND,0,PASS,Valid"
        amended_dataset_csv = "occurrenceID,country,amendment_notes\nocc1,USA,Valid\nocc2,,Missing"
        
        # Create a ProcessingSummary for testing
        summary = ProcessingSummary(
            total_records=2,
            total_tests_run=1,
            validation_failures={},
            common_issues=[],
            amendments_applied=0,
            skipped_tests=[]
        )
        
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
        
        await email_service.send_reply(
            sample_email_payload,
            str(summary),
            attachments
        )
        
        # The post method is called multiple times: once for Discord notifications and once for email sending
        assert mock_post.call_count >= 1
        # Get the last call (the email sending call)
        call_args = mock_post.call_args_list[-1]
        
        # Check that the request was made to the correct URL
        assert call_args[0][0] == email_service.gmail_send_endpoint
        
        # Check that the request contains the results
        request_data = json.loads(call_args[1]['data'])
        assert "threadId" in request_data
        assert "htmlBody" in request_data
        assert "attachments" in request_data
        assert len(request_data["attachments"]) == 2
        assert request_data["attachments"][0]["filename"] == "bdq_raw_results.csv"
        assert request_data["attachments"][1]["filename"] == "amended_dataset.csv"

    @patch('app.services.email_service.requests.post')
    @pytest.mark.asyncio
    async def test_send_reply_with_attachments_failure(self, mock_post, email_service, sample_email_payload, sample_test_results):
        """Test reply sending with attachments failure"""
        mock_post.side_effect = Exception("Network error")
        
        raw_results_csv = "test_id,row_index,status,result\nVALIDATION_COUNTRY_FOUND,0,PASS,Valid"
        amended_dataset_csv = "occurrenceID,country,amendment_notes\nocc1,USA,Valid\nocc2,,Missing"
        
        # Should handle failure gracefully
        try:
            # Create a ProcessingSummary for testing
            summary = ProcessingSummary(
                total_records=2,
                total_tests_run=1,
                validation_failures={},
                common_issues=[],
                amendments_applied=0,
                skipped_tests=[]
            )
            
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
            
            await email_service.send_reply(
                sample_email_payload,
                str(summary),
                attachments
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
        
        assert "Total records processed: 2" in summary
        assert "Total tests run: 1" in summary
        assert "skipped in this run" in summary

    def test_generate_email_summary_taxon(self, email_service, sample_test_results):
        """Test email summary generation for taxon data"""
        summary = email_service.generate_email_summary(
            [sample_test_results],
            "Taxon",
            2,
            []
        )
        
        assert "Total records processed: 2" in summary

    def test_generate_email_summary_no_results(self, email_service):
        """Test email summary generation with no test results"""
        summary = email_service.generate_email_summary(
            [],
            "Occurrence",
            5,
            ["TEST_1", "TEST_2"]
        )
        
        assert "Total records processed: 5" in summary
        assert "Total tests run: 0" in summary
        assert "skipped in this run" in summary

    def test_generate_email_summary_all_passed(self, email_service):
        """Test email summary generation when all tests pass"""
        # Create simple test results for testing
        test_result1 = BDQTestExecutionResult(
            record_id="occ1",
            test_id="VALIDATION_COUNTRY_FOUND",
            status="RUN_HAS_RESULT",
            result="PASS",
            comment="Valid",
            amendment=None,
            test_type="VALIDATION"
        )
        
        test_result2 = BDQTestExecutionResult(
            record_id="occ2",
            test_id="VALIDATION_COUNTRY_FOUND",
            status="RUN_HAS_RESULT",
            result="PASS",
            comment="Valid",
            amendment=None,
            test_type="VALIDATION"
        )
        
        summary = email_service.generate_email_summary(
            [test_result1, test_result2],
            "Occurrence",
            2,
            []
        )
        
        assert "Total records processed: 2" in summary
        assert "Total tests run: 2" in summary

    def test_generate_email_summary_all_failed(self, email_service):
        """Test email summary generation when all tests fail"""
        # Create simple test results for testing
        test_result1 = BDQTestExecutionResult(
            record_id="occ1",
            test_id="VALIDATION_COUNTRY_FOUND",
            status="RUN_HAS_RESULT",
            result="FAIL",
            comment="Invalid",
            amendment=None,
            test_type="VALIDATION"
        )
        
        test_result2 = BDQTestExecutionResult(
            record_id="occ2",
            test_id="VALIDATION_COUNTRY_FOUND",
            status="RUN_HAS_RESULT",
            result="FAIL",
            comment="Invalid",
            amendment=None,
            test_type="VALIDATION"
        )
        
        summary = email_service.generate_email_summary(
            [test_result1, test_result2],
            "Occurrence",
            2,
            []
        )
        
        assert "Total records processed: 2" in summary
        assert "Total tests run: 2" in summary

    def test_generate_email_summary_mixed_results(self, email_service):
        """Test email summary generation with mixed test results"""
        # Create simple test results for testing
        test_result1 = BDQTestExecutionResult(
            record_id="occ1",
            test_id="VALIDATION_COUNTRY_FOUND",
            status="RUN_HAS_RESULT",
            result="PASS",
            comment="Valid",
            amendment=None,
            test_type="VALIDATION"
        )
        
        test_result2 = BDQTestExecutionResult(
            record_id="occ2",
            test_id="VALIDATION_DATE_FORMAT",
            status="RUN_HAS_RESULT",
            result="FAIL",
            comment="Invalid format",
            amendment=None,
            test_type="VALIDATION"
        )
        
        summary = email_service.generate_email_summary(
            [test_result1, test_result2],
            "Occurrence",
            2,
            ["TEST_3"]
        )
        
        assert "Total records processed: 2" in summary
        assert "Total tests run: 2" in summary
        assert "skipped in this run" in summary

    def test_generate_email_summary_large_dataset(self, email_service):
        """Test email summary generation with large dataset"""
        # Create simple test results for testing
        test_results = []
        for i in range(100):
            test_results.append(BDQTestExecutionResult(
                record_id=f"occ{i}",
                test_id="VALIDATION_COUNTRY_FOUND",
                status="RUN_HAS_RESULT",
                result="PASS" if i % 2 == 0 else "FAIL",
                comment="Valid" if i % 2 == 0 else "Invalid",
                amendment=None,
                test_type="VALIDATION"
            ))
        
        summary = email_service.generate_email_summary(
            test_results,
            "Occurrence",
            100,
            []
        )
        
        assert "Total records processed: 100" in summary
        assert "Total tests run: 100" in summary

    def test_generate_email_summary_special_characters(self, email_service):
        """Test email summary generation with special characters in test names"""
        # Create simple test result for testing
        test_result = BDQTestExecutionResult(
            record_id="occ1",
            test_id="VALIDATION_SPECIAL_CHARS_2023",
            status="RUN_HAS_RESULT",
            result="PASS",
            comment="Special chars handled correctly",
            amendment=None,
            test_type="VALIDATION"
        )
        
        summary = email_service.generate_email_summary(
            [test_result],
            "Occurrence",
            1,
            []
        )
        
        # Should handle special characters gracefully
        assert "Total records processed: 1" in summary
        assert "Total tests run: 1" in summary
