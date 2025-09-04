import os
import logging
import google.generativeai as genai
from typing import List, Dict, Any, Optional
from app.models.email_models import ProcessingSummary, BDQTestExecutionResult, EmailPayload

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
        original_email_data: EmailPayload,
        core_type: str
    ) -> Dict[str, str]:
        """
        Generate intelligent summary using Gemini LLM
        
        Returns:
            Dict with 'text' and 'html' keys containing the generated summaries
        """
        try:
            # Generate the prompt
            prompt = self._create_summary_prompt(test_results, original_email_data, core_type)
            
            # Call Gemini API
            response = self.model.generate_content(prompt)
            
            if response.text:
                # Parse the response and generate both text and HTML versions
                llm_summary = response.text.strip()
                return {
                    'text': llm_summary,
                    'html': self._convert_to_html(llm_summary)
                }
                
        except Exception as e:
            logger.error(f"Error generating LLM summary: {e}")
    
    
    def _create_summary_prompt(self, context: Dict[str, Any]) -> str:
        """Create the prompt for the LLM"""
        
        prompt = f"""You are a biodiversity data quality expert. Generate a helpful, informative email summary for a user who has submitted a dataset for BDQ (Biodiversity Data Quality) testing.

CONTEXT:
- Dataset type: {context['core_type']} core
- Total records: {context['summary'].total_records:,}
- Total tests run: {context['summary'].total_tests_run:,}
- Data quality score: {context['data_quality_score']}%
- Amendments applied: {context['summary'].amendments_applied}

USER'S ORIGINAL EMAIL:
Subject: {context['email_context']['subject']}
From: {context['email_context']['from_email']}
Body: {context['email_context']['body_text'] or 'No text body provided'}

VALIDATION RESULTS:
"""
        
        if context['validation_failures_by_field']:
            prompt += "The following data quality issues were found:\n"
            for field, failures in context['validation_failures_by_field'].items():
                prompt += f"- {field}: {len(failures)} issues\n"
                # Add a few examples
                for failure in failures[:2]:  # Limit to 2 examples per field
                    prompt += f"  • {failure['comment']}\n"
        else:
            prompt += "No validation failures were found - excellent data quality!\n"
        
        if context['amendment_insights']:
            prompt += f"\nAMENDMENTS APPLIED:\n"
            prompt += f"{context['summary'].amendments_applied} records were automatically improved with standardized values.\n"
        
        if context['skipped_tests']:
            prompt += "\nNOTE ON TECHNICAL LIMITATIONS:\n"
            prompt += "The following tests could not be run due to a temporary technical issue and were skipped for this run. We can try these again later without you needing to resend the data.\n"
            for t in context['skipped_tests'][:10]:
                prompt += f"- {t}\n"
        
        prompt += """

TASK: Write a professional, helpful email summary that:
1. Acknowledges their submission and thanks them
2. Provides a clear overview of what was found
3. Explains the significance of any issues in plain language
4. Offers actionable advice for improving data quality
5. Explains what the attached files contain
6. Maintains a helpful, encouraging tone

Format the response as a professional email body. Be specific about the data quality score and what it means. If there are issues, explain them in terms a biologist would understand, not technical jargon.

Focus on being helpful and actionable rather than just reporting numbers."""

        return prompt
    