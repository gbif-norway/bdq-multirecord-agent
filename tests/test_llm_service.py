import pytest
import pandas as pd
from unittest.mock import Mock, patch
from app.services.llm_service import LLMService


class TestLLMService:
    """Test LLMService prompt generation and response handling"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.llm_service = LLMService()
        
        # Sample test results DataFrame
        self.test_results_df = pd.DataFrame([
            {
                'test_id': 'VALIDATION_COUNTRYCODE_VALID',
                'result': 'NOT_COMPLIANT',
                'status': 'RUN_HAS_RESULT',
                'comment': 'Invalid country code format'
            },
            {
                'test_id': 'VALIDATION_COUNTRYCODE_VALID',
                'result': 'COMPLIANT',
                'status': 'RUN_HAS_RESULT',
                'comment': ''
            },
            {
                'test_id': 'AMENDMENT_EVENTDATE_STANDARDIZED',
                'result': 'dwc:eventDate=2023-01-01',
                'status': 'AMENDED',
                'comment': 'Standardized date format'
            }
        ])
        
        # Sample summary stats
        self.summary_stats = {
            'total_records': 3,
            'total_tests_run': 3,
            'unique_tests': 2,
            'validation_failures': 1,
            'amendments_applied': 1,
            'compliant_results': 1,
            'success_rate_percent': 33.3,
            'failure_counts_by_test': {
                'VALIDATION_COUNTRYCODE_VALID': 1
            },
            'common_issues': {
                'Invalid country code format': 1
            }
        }
        
        # Sample email content string (preferring HTML, fallback to text)
        self.email_content = "FROM: researcher@example.com\nSUBJECT: BDQ Test Request\n<p>Please test my biodiversity dataset</p>"
    
    @patch('app.services.llm_service.genai.GenerativeModel')
    def test_generate_intelligent_summary_success(self, mock_model_class):
        """Test successful summary generation"""
        # Mock the Gemini response
        mock_model = Mock()
        mock_model_class.return_value = mock_model
        mock_response = Mock()
        mock_response.text = "Thank you for submitting your dataset. We found 1 validation issue that needs attention."
        mock_model.generate_content.return_value = mock_response
        
        # Create a new LLMService instance to use the mocked model
        llm_service = LLMService()
        llm_service.model = mock_model  # Replace with mocked model
        
        result = llm_service.generate_intelligent_summary(
            self.test_results_df, 
            self.email_content, 
            'occurrence', 
            self.summary_stats
        )
        
        # Verify the result is HTML formatted
        assert result.startswith('<p>')
        assert result.endswith('</p>')
        assert 'Thank you for submitting' in result
        
        # Verify the model was called with a proper prompt
        mock_model.generate_content.assert_called_once()
        prompt = mock_model.generate_content.call_args[0][0]
        assert 'Occurrence core' in prompt
        assert '3' in prompt  # Total Records: 3
        assert '1 validation problems' in prompt
    
    @patch('app.services.llm_service.genai.GenerativeModel')
    def test_generate_intelligent_summary_empty_response(self, mock_model_class):
        """Test handling of empty Gemini response"""
        mock_model = Mock()
        mock_model_class.return_value = mock_model
        mock_response = Mock()
        mock_response.text = None
        mock_model.generate_content.return_value = mock_response
        
        # Create a new LLMService instance to use the mocked model
        llm_service = LLMService()
        llm_service.model = mock_model  # Replace with mocked model
        
        result = llm_service.generate_intelligent_summary(
            self.test_results_df, 
            self.email_content, 
            'occurrence', 
            self.summary_stats
        )
        
        assert result == "<p>Unable to generate summary at this time.</p>"
    
    def test_create_summary_prompt_with_results(self):
        """Test prompt creation with test results"""
        prompt = self.llm_service._create_summary_prompt(
            self.test_results_df, 
            self.email_content, 
            'occurrence', 
            self.summary_stats
        )
        
        # Verify key elements are in the prompt
        assert 'Occurrence core' in prompt
        assert '3' in prompt  # Total Records: 3
        assert '2 different test types' in prompt
        assert '33.3%' in prompt
        assert '1 validation problems' in prompt
        assert '1 records enhanced' in prompt  # Changed from "automatically enhanced"
        assert '**VALIDATION_COUNTRYCODE_VALID**: 1 issues' in prompt
        assert 'Invalid country code format (1 occurrences)' in prompt
        assert 'Please test my biodiversity dataset' in prompt
        assert 'FROM: researcher@example.com' in prompt
        
        # Verify new prompt structure
        assert '## CONTEXT' in prompt
        assert '## TEST RESULTS CONTEXT' in prompt
        assert '## YOUR TASK' in prompt
        assert '## TONE & STYLE' in prompt
        assert '## FORMAT' in prompt
    
    def test_create_summary_prompt_no_failures(self):
        """Test prompt creation when no validation failures"""
        # Modify summary stats to have no failures
        no_failures_stats = self.summary_stats.copy()
        no_failures_stats.update({
            'validation_failures': 0,
            'success_rate_percent': 100.0,
            'failure_counts_by_test': {},
            'common_issues': {}
        })
        
        prompt = self.llm_service._create_summary_prompt(
            self.test_results_df, 
            self.email_content, 
            'taxon', 
            no_failures_stats
        )
        
        assert 'Taxon core' in prompt
        assert '0 validation problems' in prompt  # Changed from "Excellent news!" message
        assert '100.0%' in prompt
    
    def test_create_summary_prompt_no_amendments(self):
        """Test prompt creation when no amendments were applied"""
        no_amendments_stats = self.summary_stats.copy()
        no_amendments_stats['amendments_applied'] = 0
        
        prompt = self.llm_service._create_summary_prompt(
            self.test_results_df, 
            self.email_content, 
            'occurrence', 
            no_amendments_stats
        )
        
        # Should not mention automatic improvements
        assert 'Automatic Improvements' not in prompt
    
    def test_create_summary_prompt_empty_email_body(self):
        """Test prompt creation with empty email body"""
        empty_email_content = "FROM: test@example.com\nSUBJECT: Test\n"
        
        prompt = self.llm_service._create_summary_prompt(
            self.test_results_df, 
            empty_email_content, 
            'occurrence', 
            self.summary_stats
        )
        
        # Should include user message section even with just subject
        assert "User's Original Message" in prompt
        assert "FROM: test@example.com" in prompt
        assert "SUBJECT: Test" in prompt
    
    def test_create_empty_results_prompt(self):
        """Test prompt creation for empty results"""
        prompt = self.llm_service._create_empty_results_prompt(
            self.email_content, 
            'occurrence'
        )
        
        assert 'occurrence core dataset' in prompt
        assert 'no applicable BDQ tests could be run' in prompt
        assert 'Missing required columns' in prompt
        assert 'Please test my biodiversity dataset' in prompt
        assert 'FROM: researcher@example.com' in prompt
    
    def test_create_empty_results_prompt_no_email_body(self):
        """Test empty results prompt with no email body"""
        empty_email_content = "FROM: test@example.com\nSUBJECT: Test\n"
        
        prompt = self.llm_service._create_empty_results_prompt(
            empty_email_content, 
            'taxon'
        )
        
        assert 'taxon core dataset' in prompt
        assert "User's Message" in prompt
        assert "FROM: test@example.com" in prompt
        assert "SUBJECT: Test" in prompt
    
    def test_convert_to_html(self):
        """Test HTML conversion"""
        text = "Hello world.\n\nThis is a new paragraph.\nWith a line break."
        html = self.llm_service._convert_to_html(text)
        
        expected = "<p>Hello world.</p><p>This is a new paragraph.<br>With a line break.</p>"
        assert html == expected
    
    def test_convert_to_html_single_paragraph(self):
        """Test HTML conversion with single paragraph"""
        text = "Just one line of text."
        html = self.llm_service._convert_to_html(text)
        
        assert html == "<p>Just one line of text.</p>"
    
    def test_handle_none_test_results(self):
        """Test handling of None test results"""
        prompt = self.llm_service._create_summary_prompt(
            None, 
            self.email_content, 
            'occurrence', 
            self.summary_stats
        )
        
        # Should create empty results prompt
        assert 'no applicable BDQ tests could be run' in prompt
    
    def test_handle_empty_test_results(self):
        """Test handling of empty test results DataFrame"""
        empty_df = pd.DataFrame()
        
        prompt = self.llm_service._create_summary_prompt(
            empty_df, 
            self.email_content, 
            'occurrence', 
            self.summary_stats
        )
        
        # Should create empty results prompt
        assert 'no applicable BDQ tests could be run' in prompt
    