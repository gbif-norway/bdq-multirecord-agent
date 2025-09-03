import pytest
import json
from unittest.mock import patch, Mock, MagicMock
from unittest.mock import AsyncMock

from app.services.llm_service import LLMService
from app.models.email_models import BDQTestExecutionResult, BDQTest, BDQTestResult, ProcessingSummary, EmailPayload


class TestLLMService:
    """Test the LLM Service"""

    @pytest.fixture
    def llm_service(self):
        """LLM service instance for testing"""
        with patch.dict('os.environ', {
            'GOOGLE_API_KEY': 'test_api_key_123'
        }):
            return LLMService()

    @pytest.fixture
    def llm_service_no_key(self):
        """LLM service instance without API key"""
        with patch.dict('os.environ', {}, clear=True):
            return LLMService()

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

    @pytest.fixture
    def sample_email_context(self):
        """Sample email context for testing"""
        return {
            "from_email": "researcher@university.edu",
            "subject": "Biodiversity dataset for quality assessment",
            "body_text": "Please analyze this dataset for data quality issues. It contains occurrence records from our field survey.",
            "body_html": "<p>Please analyze this dataset for data quality issues. It contains occurrence records from our field survey.</p>"
        }

    def test_init_with_api_key(self):
        """Test LLM service initialization with API key"""
        with patch.dict('os.environ', {
            'GOOGLE_API_KEY': 'test_api_key_123'
        }):
            service = LLMService()
            assert service.api_key == 'test_api_key_123'
            assert service.enabled is True

    def test_init_without_api_key(self):
        """Test LLM service initialization without API key"""
        with patch.dict('os.environ', {}, clear=True):
            service = LLMService()
            assert service.api_key is None
            assert service.enabled is False

    def test_init_with_empty_api_key(self):
        """Test LLM service initialization with empty API key"""
        with patch.dict('os.environ', {
            'GOOGLE_API_KEY': ''
        }):
            service = LLMService()
            assert service.api_key == ''
            assert service.enabled is False

    def test_enabled_property(self, llm_service):
        """Test enabled property"""
        assert llm_service.enabled is True
        
        # Test with disabled service
        llm_service.api_key = None
        assert llm_service.enabled is False

    def test_generate_intelligent_summary_disabled_service(self, llm_service_no_key):
        """Test summary generation when LLM service is disabled"""
        # Create a ProcessingSummary for testing
        summary = ProcessingSummary(
            total_records=5,
            total_tests_run=0,
            validation_failures={},
            common_issues=[],
            amendments_applied=0,
            skipped_tests=[]
        )
        
        test_results = []
        email_data = EmailPayload(
            message_id="test123",
            thread_id="thread123",
            from_email="test@example.com",
            to_email="bdq@example.com",
            subject="Test Subject",
            body_text="Test body"
        )
        
        result = llm_service_no_key.generate_intelligent_summary(
            summary, test_results, email_data, "Occurrence"
        )
        
        # Should return fallback summary when disabled
        assert "text" in result
        assert "html" in result
        assert "5" in result["text"]

    @patch('app.services.llm_service.genai')
    def test_generate_summary_success(self, mock_genai, llm_service, sample_test_results, sample_email_context):
        """Test successful summary generation"""
        # Mock the Gemini model
        mock_model = Mock()
        mock_response = Mock()
        mock_response.text = "This is an intelligent summary of the data quality assessment results."
        mock_model.generate_content.return_value = mock_response
        
        # Set up the mock to work with the service
        mock_genai.GenerativeModel.return_value = mock_model
        
        # Temporarily set the model on the service
        llm_service.model = mock_model
        
        summary = llm_service.generate_summary(
            "Occurrence",
            2,
            [sample_test_results],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            sample_email_context["body_text"]
        )
        
        # Should return LLM-generated summary
        assert "This is an intelligent summary" in summary
        mock_model.generate_content.assert_called_once()

    @patch('app.services.llm_service.genai')
    def test_generate_summary_api_error(self, mock_genai, llm_service, sample_test_results, sample_email_context):
        """Test summary generation with API error"""
        # Mock the Gemini model to raise an exception
        mock_model = Mock()
        mock_model.generate_content.side_effect = Exception("API rate limit exceeded")
        
        # Set up the mock to work with the service
        mock_genai.GenerativeModel.return_value = mock_model
        
        # Temporarily set the model on the service
        llm_service.model = mock_model
        
        summary = llm_service.generate_summary(
            "Occurrence",
            2,
            [sample_test_results],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            sample_email_context["body_text"]
        )
        
        # Should fall back to basic summary
        assert "2 records" in summary
        assert "Thank you for submitting" in summary

    @patch('app.services.llm_service.genai')
    def test_generate_summary_timeout(self, mock_genai, llm_service, sample_test_results, sample_email_context):
        """Test summary generation with timeout"""
        # Mock the Gemini model to raise a timeout exception
        mock_model = Mock()
        mock_model.generate_content.side_effect = TimeoutError("Request timed out")
        
        # Set up the mock to work with the service
        mock_genai.GenerativeModel.return_value = mock_model
        
        # Temporarily set the model on the service
        llm_service.model = mock_model
        
        summary = llm_service.generate_summary(
            "Occurrence",
            2,
            [sample_test_results],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            sample_email_context["body_text"]
        )
        
        # Should fall back to basic summary
        assert "2 records" in summary
        assert "Thank you for submitting" in summary

    def test_generate_summary_no_test_results(self, llm_service, sample_email_context):
        """Test summary generation with no test results"""
        summary = llm_service.generate_summary(
            "Occurrence",
            0,
            [],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            sample_email_context["body_text"]
        )
        
        # Should handle empty results gracefully
        assert "0 records" in summary
        assert "Thank you for submitting" in summary

    def test_generate_summary_taxon_core(self, llm_service, sample_test_results, sample_email_context):
        """Test summary generation for taxon core data"""
        summary = llm_service.generate_summary(
            "Taxon",
            2,
            [sample_test_results],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            sample_email_context["body_text"]
        )
        
        assert "2 records" in summary
        assert "Thank you for submitting" in summary

    def test_generate_summary_large_dataset(self, llm_service, sample_email_context):
        """Test summary generation for large dataset"""
        # Create many test results
        test = BDQTest(
            id="VALIDATION_COUNTRY_FOUND",
            guid="test-guid-123",
            type="Validation",
            className="org.filteredpush.qc.georef.CountryFound",
            methodName="validationCountryFound",
            actedUpon=["dwc:country"],
            consulted=[],
            parameters=[]
        )
        
        results = []
        for i in range(100):
            results.append(BDQTestResult(
                test_id="VALIDATION_COUNTRY_FOUND",
                row_index=i,
                status="RUN_HAS_RESULT",
                result="PASS" if i % 2 == 0 else "FAIL",
                comment="Valid" if i % 2 == 0 else "Invalid"
            ))
        
        # Create a simple test result for testing
        test_result = BDQTestExecutionResult(
            record_id="occ1",
            test_id="VALIDATION_COUNTRY_FOUND",
            status="RUN_HAS_RESULT",
            result="PASS",
            comment="Valid",
            amendment=None,
            test_type="VALIDATION"
        )
        
        summary = llm_service.generate_summary(
            "Occurrence",
            100,
            [test_result],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            sample_email_context["body_text"]
        )
        
        assert "100 records" in summary
        assert "Thank you for submitting" in summary

    def test_generate_summary_mixed_test_types(self, llm_service, sample_email_context):
        """Test summary generation with mixed test types"""
        # Create simple test results for testing
        validation_result = BDQTestExecutionResult(
            record_id="occ1",
            test_id="VALIDATION_COUNTRY_FOUND",
            status="RUN_HAS_RESULT",
            result="PASS",
            comment="Valid",
            amendment=None,
            test_type="VALIDATION"
        )
        
        amendment_result = BDQTestExecutionResult(
            record_id="occ2",
            test_id="AMENDMENT_COUNTRY_CODE",
            status="AMENDED",
            result="AMENDED",
            comment="Country code standardized",
            amendment={"country": "USA"},
            test_type="AMENDMENT"
        )
        
        summary = llm_service.generate_summary(
            "Occurrence",
            2,
            [validation_result, amendment_result],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            sample_email_context["body_text"]
        )
        
        # Should handle both test types
        assert "2 records" in summary
        assert "Thank you for submitting" in summary

    def test_generate_summary_with_skipped_tests(self, llm_service, sample_test_results, sample_email_context):
        """Test summary generation with skipped tests"""
        summary = llm_service.generate_summary(
            "Occurrence",
            2,
            [sample_test_results],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            sample_email_context["body_text"]
        )
        
        # Should handle test results gracefully
        assert "2 records" in summary
        assert "Thank you for submitting" in summary

    def test_generate_summary_special_characters(self, llm_service, sample_email_context):
        """Test summary generation with special characters"""
        # Create a simple test result for testing
        test_result = BDQTestExecutionResult(
            record_id="occ1",
            test_id="VALIDATION_SPECIAL_CHARS_2023",
            status="RUN_HAS_RESULT",
            result="PASS",
            comment="Special chars handled correctly",
            amendment=None,
            test_type="VALIDATION"
        )
        
        summary = llm_service.generate_summary(
            "Occurrence",
            1,
            [test_result],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            sample_email_context["body_text"]
        )
        
        # Should handle special characters gracefully
        assert "1 record" in summary
        assert "Thank you for submitting" in summary

    @patch('app.services.llm_service.genai')
    def test_generate_summary_prompt_construction(self, mock_genai, llm_service, sample_test_results, sample_email_context):
        """Test that the prompt is constructed correctly"""
        # Mock the Gemini model
        mock_model = Mock()
        mock_response = Mock()
        mock_response.text = "Generated summary"
        mock_model.generate_content.return_value = mock_response
        
        # Set up the mock to work with the service
        mock_genai.GenerativeModel.return_value = mock_model
        
        # Temporarily set the model on the service
        llm_service.model = mock_model
        
        llm_service.generate_summary(
            "Occurrence",
            2,
            [sample_test_results],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            sample_email_context["body_text"]
        )
        
        # Verify the prompt was constructed with the right components
        call_args = mock_model.generate_content.call_args[0][0]
        prompt_text = str(call_args)
        
        assert "Occurrence" in prompt_text
        assert "Total records: 2" in prompt_text
        assert "No validation failures were found" in prompt_text
        assert "researcher@university.edu" in prompt_text
        assert "Biodiversity dataset for quality assessment" in prompt_text

    def test_generate_summary_edge_cases(self, llm_service, sample_email_context):
        """Test summary generation with edge cases"""
        # Test with very long email body
        long_body = "A" * 10000  # Very long body
        
        summary = llm_service.generate_summary(
            "Occurrence",
            1,
            [],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            long_body
        )
        
        assert "1 record" in summary
        assert "Thank you for submitting" in summary

        # Test with empty strings
        summary = llm_service.generate_summary(
            "Occurrence",
            0,
            [],
            "",
            "",
            ""
        )
        
        assert "0 records" in summary
        assert "Thank you for submitting" in summary

    def test_generate_summary_unicode_content(self, llm_service, sample_email_context):
        """Test summary generation with unicode content"""
        unicode_body = "Dataset contains Spanish localities: Guadalajara, México, España"
        
        summary = llm_service.generate_summary(
            "Occurrence",
            1,
            [],
            sample_email_context["from_email"],
            sample_email_context["subject"],
            unicode_body
        )
        
        assert "1 record" in summary
        assert "Thank you for submitting" in summary
