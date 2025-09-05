import os
import logging
import google.generativeai as genai
from typing import List, Dict, Any, Optional
from app.utils.helper import BDQTestExecutionResult

logger = logging.getLogger(__name__)

class LLMService:
    """Service for generating intelligent summaries using Google Gemini"""
    
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY")
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
    
    def generate_intelligent_summary(
        self, 
        test_results: List[BDQTestExecutionResult],
        original_email_data: Dict[str, Any],
        core_type: str,
        summary_stats: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Generate intelligent summary using Gemini LLM
        
        Returns:
            Dict with 'text' and 'html' keys containing the generated summaries
        """
        # Generate the prompt
        prompt = self._create_summary_prompt(test_results, original_email_data, core_type, summary_stats)
        
        # Call Gemini API
        response = self.model.generate_content(prompt)
        
        if response.text:
            # Parse the response and generate both text and HTML versions
            return self._convert_to_html(response.text.strip())
        else:
            logger.error("No response text from Gemini API")
            return "<p>Unable to generate summary at this time.</p>"
                
    
    def _create_summary_prompt(self, test_results: List[BDQTestExecutionResult], original_email_data: Dict[str, Any], core_type: str, summary_stats: Dict[str, Any]) -> str:
        """Create the prompt for the LLM"""
        
        # Analyze test results for validation failures
        validation_failures = [r for r in test_results if r.result in ['NOT_COMPLIANT', 'POTENTIAL_ISSUE']]
        amendments_applied = [r for r in test_results if r.status in ['AMENDED', 'FILLED_IN']]
        
        # Group failures by test type
        failures_by_test = {}
        for failure in validation_failures:
            if failure.test_id not in failures_by_test:
                failures_by_test[failure.test_id] = []
            failures_by_test[failure.test_id].append(failure.comment or 'No comment')
        
        prompt = f"""You are a biodiversity data quality expert. Generate a helpful, informative email summary for a user who has submitted a dataset for BDQ (Biodiversity Data Quality) testing.

CONTEXT:

- Dataset type: {core_type} core

- Summary statistics: {str(summary_stats)}

- USER'S ORIGINAL EMAIL: {str(original_email_data)}

DETAILED VALIDATION RESULTS:
"""
        
        if validation_failures:
            prompt += f"Found {len(validation_failures)} validation issues across {len(failures_by_test)} different test types:\n"
            for test_id, comments in list(failures_by_test.items())[:5]:  # Limit to top 5 test types
                prompt += f"- {test_id}: {len(comments)} issues\n"
                # Add a few examples
                for comment in comments[:2]:  # Limit to 2 examples per test
                    prompt += f"  • {comment}\n"
        else:
            prompt += "No validation failures were found - excellent data quality!\n"
        
        if amendments_applied:
            prompt += f"\nAMENDMENTS APPLIED:\n"
            prompt += f"{len(amendments_applied)} records were automatically improved with standardized values.\n"
        
        # TODO add common issues list (we need to generate this)
        
        prompt += """

TASK: Write a professional, helpful email summary that:
1. Acknowledges their submission and thanks them
2. Provides a clear overview of what was found
3. Explains the significance of any issues in plain language
4. Offers actionable advice for improving data quality
5. Explains what the attached files contain
6. Maintains a helpful, encouraging tone

Format the response as a professional email body. If there are issues, explain them in terms a biologist would understand, not technical jargon.

Focus on being helpful and actionable rather than just reporting numbers."""

        return prompt
    
    def _convert_to_html(self, text: str) -> str:
        """Convert plain text to basic HTML"""
        # Simple conversion - replace newlines with <br> and wrap in <p> tags
        html = text.replace('\n\n', '</p><p>').replace('\n', '<br>')
        return f"<p>{html}</p>"
    