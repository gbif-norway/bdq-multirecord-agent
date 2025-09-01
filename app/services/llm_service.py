import os
import logging
import google.generativeai as genai
from typing import List, Dict, Any, Optional
from models.email_models import ProcessingSummary, TestExecutionResult, EmailPayload

logger = logging.getLogger(__name__)

class LLMService:
    """Service for generating intelligent summaries using Google Gemini"""
    
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            logger.warning("GOOGLE_API_KEY not set - LLM summaries will be disabled")
            logger.info("GOOGLE_API_KEY not set - LLM summaries will be disabled")
            self.enabled = False
        else:
            genai.configure(api_key=self.api_key)
            self.enabled = True
            self.model = genai.GenerativeModel('gemini-1.5-flash')
    
    async def generate_intelligent_summary(
        self, 
        summary: ProcessingSummary, 
        test_results: List[TestExecutionResult],
        email_data: EmailPayload,
        core_type: str
    ) -> Dict[str, str]:
        """
        Generate intelligent summary using Gemini LLM
        
        Returns:
            Dict with 'text' and 'html' keys containing the generated summaries
        """
        if not self.enabled:
            logger.warning("LLM service disabled - falling back to basic summary")
            return self._generate_fallback_summary(summary)
        
        try:
            # Prepare context for the LLM
            context = self._prepare_llm_context(summary, test_results, email_data, core_type)
            
            # Generate the prompt
            prompt = self._create_summary_prompt(context)
            
            # Call Gemini API
            response = self.model.generate_content(prompt)
            
            if response.text:
                # Parse the response and generate both text and HTML versions
                llm_summary = response.text.strip()
                return {
                    'text': llm_summary,
                    'html': self._convert_to_html(llm_summary)
                }
            else:
                logger.warning("LLM returned empty response - falling back to basic summary")
                return self._generate_fallback_summary(summary)
                
        except Exception as e:
            logger.error(f"Error generating LLM summary: {e}")
            logger.info("Falling back to basic summary generation")
            return self._generate_fallback_summary(summary)
    
    def _prepare_llm_context(
        self, 
        summary: ProcessingSummary, 
        test_results: List[TestExecutionResult],
        email_data: EmailPayload,
        core_type: str
    ) -> Dict[str, Any]:
        """Prepare context data for the LLM"""
        
        # Analyze test results for insights
        validation_failures_by_field = {}
        amendment_insights = []
        data_quality_score = 0
        
        for result in test_results:
            if result.result == "NOT_COMPLIANT":
                # Extract field name from test ID for better categorization
                field_name = self._extract_field_name(result.test_id)
                if field_name not in validation_failures_by_field:
                    validation_failures_by_field[field_name] = []
                validation_failures_by_field[field_name].append({
                    'test_id': result.test_id,
                    'comment': result.comment,
                    'record_id': result.record_id
                })
            elif result.status == "AMENDED":
                amendment_insights.append({
                    'test_id': result.test_id,
                    'amendment': result.amendment,
                    'comment': result.comment
                })
        
        # Calculate data quality score
        total_failures = sum(len(failures) for failures in validation_failures_by_field.values())
        if summary.total_tests_run > 0:
            data_quality_score = max(0, 100 - (total_failures / summary.total_tests_run * 100))
        
        return {
            'summary': summary,
            'validation_failures_by_field': validation_failures_by_field,
            'amendment_insights': amendment_insights,
            'data_quality_score': round(data_quality_score, 1),
            'email_context': {
                'subject': email_data.subject,
                'body_text': email_data.body_text,
                'body_html': email_data.body_html,
                'from_email': email_data.from_email
            },
            'core_type': core_type,
            'total_records': summary.total_records
        }
    
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
    
    def _extract_field_name(self, test_id: str) -> str:
        """Extract a human-readable field name from test ID"""
        # Remove common prefixes and convert to readable format
        field_mapping = {
            'COORDINATES': 'Geographic coordinates',
            'COUNTRYCODE': 'Country codes',
            'BASISOFRECORD': 'Basis of record',
            'SCIENTIFICNAME': 'Scientific names',
            'DATE': 'Date fields',
            'IDENTIFIER': 'Identifiers',
            'GEOREFERENCE': 'Georeference data'
        }
        
        for key, value in field_mapping.items():
            if key in test_id.upper():
                return value
        
        # Fallback: convert test ID to readable format
        return test_id.replace('_', ' ').replace('VALIDATION', '').replace('AMENDMENT', '').strip()
    
    def _convert_to_html(self, text_summary: str) -> str:
        """Convert plain text summary to HTML format"""
        # Simple conversion - preserve line breaks and add basic formatting
        html = text_summary.replace('\n\n', '</p><p>')
        html = html.replace('\n', '<br>')
        return f"<p>{html}</p>"
    
    def _generate_fallback_summary(self, summary: ProcessingSummary) -> Dict[str, str]:
        """Generate basic summary when LLM is not available"""
        text = f"""Thank you for submitting your biodiversity dataset for quality assessment!

We've processed {summary.total_records:,} records and run {summary.total_tests_run:,} quality tests.

"""
        
        if summary.validation_failures:
            text += "We found some data quality issues that you may want to address:\n"
            for test_id, count in summary.validation_failures.items():
                text += f"- {test_id}: {count} issues\n"
        else:
            text += "Great news! No validation failures were found in your dataset.\n"
        
        if summary.amendments_applied > 0:
            text += f"\nWe've automatically applied {summary.amendments_applied} improvements to standardize your data.\n"
        
        text += """
Please review the attached files:
- bdq_raw_results.csv: Detailed test results for each record
- amended_dataset.csv: Your dataset with proposed improvements applied

Feel free to reach out if you have any questions about the results!"""
        
        return {
            'text': text,
            'html': self._convert_to_html(text)
        }
