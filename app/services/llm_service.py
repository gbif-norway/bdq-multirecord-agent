import os
import google.generativeai as genai
from typing import List, Dict, Any, Optional
from app.utils.helper import log

class LLMService:
    """Service for generating intelligent summaries using Google Gemini"""
    
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY")
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
    
    def generate_intelligent_summary(self, test_results_df, original_email_data, core_type, summary_stats):
        """Generate intelligent summary from DataFrame-based test results"""
        log("Generating the prompt for gemini...") 
        prompt = self._create_summary_prompt(test_results_df, original_email_data, core_type, summary_stats)
        log("Prompt generated!")
        log(prompt)
        # Call Gemini API
        response = self.model.generate_content(prompt)
        log(response.text[:1000])
        
        if response.text:
            # Parse the response and generate both text and HTML versions
            return self._convert_to_html(response.text.strip())
        else:
            log("No response text from Gemini API", "ERROR")
            return "<p>Unable to generate summary at this time.</p>"
                
    
    def _create_summary_prompt(self, test_results_df, original_email_data, core_type, summary_stats):
        """Create the prompt for the LLM using pre-computed summary statistics"""
        
        # Handle empty results
        if test_results_df is None or test_results_df.empty:
            return self._create_empty_results_prompt(original_email_data, core_type)
        
        # Extract user's original message if available
        user_message = ""
        if isinstance(original_email_data, dict):
            user_message = original_email_data.get('body', '')
            if not user_message:
                user_message = original_email_data.get('text', '')
        
        # Get data from summary stats (no need to re-analyze DataFrame)
        validation_failures_count = summary_stats.get('validation_failures', 0)
        amendments_applied_count = summary_stats.get('amendments_applied', 0)
        failure_counts_by_test = summary_stats.get('failure_counts_by_test', {})
        common_issues = summary_stats.get('common_issues', {})
        
        prompt = f"""You are a biodiversity data quality expert writing a helpful email analysis for a researcher who submitted a {core_type.title()} core dataset for BDQ (Biodiversity Data Quality) testing.

## CONTEXT
The researcher has already received a summary of their test results (total records, success rate, issues found, etc.) in a formatted summary box at the top of this email. Your job is to provide the analysis and guidance below that summary.

## TEST RESULTS CONTEXT
- **Dataset Type**: {core_type.title()} core
- **Total Records**: {summary_stats.get('total_records', 0):,}
- **Tests Run**: {summary_stats.get('total_tests_run', 0):,} across {summary_stats.get('unique_tests', 0)} different test types
- **Success Rate**: {summary_stats.get('success_rate_percent', 0)}%
- **Issues Found**: {validation_failures_count:,} validation problems
- **Auto-Improvements**: {amendments_applied_count:,} records enhanced
"""
        
        # Add specific issue context for analysis
        if validation_failures_count > 0:
            prompt += f"\n**Main Issue Categories**:\n"
            for test_id, count in list(failure_counts_by_test.items())[:5]:
                prompt += f"- **{test_id}**: {count} issues\n"
        
        if common_issues:
            prompt += f"\n**Most Common Issues**:\n"
            for issue, count in list(common_issues.items())[:3]:
                prompt += f"- {issue} ({count} occurrences)\n"
        
        # Add user context if available
        if user_message and user_message.strip():
            prompt += f"\n**User's Original Message**: {user_message[:200]}{'...' if len(user_message) > 200 else ''}\n"
        
        prompt += """

## YOUR TASK
Write a professional, encouraging email analysis that:

1. **Acknowledge** their submission and thank them for using BDQ
2. **Analyze** what the test results mean for their research (focus on interpretation, not repeating numbers)
3. **Explain** the significance of any issues found and their impact on data quality
4. **Provide** specific, actionable advice for improving data quality
5. **Describe** what the attached files contain and how to use them effectively
6. **Encourage** them to resubmit after making improvements

## TONE & STYLE
- Professional but friendly
- Use language a biologist would understand
- Focus on analysis and guidance, not repeating the summary stats
- Be encouraging about data quality improvement
- Keep it concise but comprehensive

## FORMAT
Write as a complete email body that will appear below the summary stats box. Use clear paragraphs and bullet points where appropriate. Do NOT include the summary statistics - they are already displayed above."""

        return prompt
    
    def _create_empty_results_prompt(self, original_email_data, core_type):
        """Create prompt for when no test results are available"""
        user_message = ""
        if isinstance(original_email_data, dict):
            user_message = original_email_data.get('body', '')
            if not user_message:
                user_message = original_email_data.get('text', '')
        
        prompt = f"""You are a biodiversity data quality expert writing an email for a researcher who submitted a {core_type} core dataset for BDQ testing.

## CONTEXT
The researcher has already received a summary indicating no test results are available. Your job is to provide analysis and guidance below that summary.

## SITUATION
The dataset was submitted but no applicable BDQ tests could be run. This could be due to:
- Missing required columns for BDQ testing
- Dataset format issues
- No tests available for the provided data structure

## USER CONTEXT
"""
        if user_message:
            prompt += f"**User's Message**: {user_message[:200]}{'...' if len(user_message) > 200 else ''}\n"
        
        prompt += """

## YOUR TASK
Write a helpful email analysis that:
1. Acknowledges their submission and thanks them for using BDQ
2. Explains why no tests could be run in clear, non-technical language
3. Provides specific guidance on what columns/data are needed for BDQ testing
4. Suggests next steps for improving their dataset
5. Encourages them to check the BDQ documentation or resubmit with additional data
6. Maintains a helpful, professional tone

## FORMAT
Write as a complete email body that will appear below the summary stats box. Use clear paragraphs and bullet points where appropriate."""

        return prompt
    
    def _convert_to_html(self, text: str) -> str:
        """Convert plain text to basic HTML"""
        # Simple conversion - replace newlines with <br> and wrap in <p> tags
        html = text.replace('\n\n', '</p><p>').replace('\n', '<br>')
        return f"<p>{html}</p>"
    