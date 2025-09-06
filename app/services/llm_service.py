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
        # Generate the prompt
        prompt = self._create_summary_prompt(test_results_df, original_email_data, core_type, summary_stats)
        
        # Call Gemini API
        response = self.model.generate_content(prompt)
        
        if response.text:
            # Parse the response and generate both text and HTML versions
            return self._convert_to_html(response.text.strip())
        else:
            log("No response text from Gemini API", "ERROR")
            return "<p>Unable to generate summary at this time.</p>"
                
    
    def _create_summary_prompt(self, test_results_df, original_email_data, core_type, summary_stats):
        """Create the prompt for the LLM using DataFrame-based test results"""
        
        # Handle empty results
        if test_results_df is None or test_results_df.empty:
            return self._create_empty_results_prompt(original_email_data, core_type)
        
        # Analyze test results for validation failures
        validation_failures = test_results_df[
            (test_results_df['result'] == 'NOT_COMPLIANT') | 
            (test_results_df['result'] == 'POTENTIAL_ISSUE')
        ]
        
        amendments_applied = test_results_df[
            test_results_df['status'].isin(['AMENDED', 'FILLED_IN'])
        ]
        
        # Group failures by test type
        failures_by_test = {}
        if not validation_failures.empty:
            for test_id in validation_failures['test'].unique():
                test_failures = validation_failures[validation_failures['test'] == test_id]
                comments = test_failures['comment'].dropna().tolist()
                if comments:
                    failures_by_test[test_id] = comments
        
        # Get common issues from summary stats
        common_issues = summary_stats.get('common_issues', {})
        
        # Extract user's original message if available
        user_message = ""
        if isinstance(original_email_data, dict):
            user_message = original_email_data.get('body', '')
            if not user_message:
                user_message = original_email_data.get('text', '')
        
        prompt = f"""You are a biodiversity data quality expert writing a helpful email summary for a researcher who submitted a dataset for BDQ (Biodiversity Data Quality) testing.

## DATASET OVERVIEW
- **Dataset Type**: {core_type.title()} core
- **Total Records**: {summary_stats.get('total_records', 0):,}
- **Tests Run**: {summary_stats.get('total_tests_run', 0):,} across {summary_stats.get('unique_tests', 0)} different test types
- **Success Rate**: {summary_stats.get('success_rate_percent', 0)}%

## DATA QUALITY RESULTS
"""
        
        if not validation_failures.empty:
            prompt += f"**Issues Found**: {len(validation_failures):,} validation problems across {len(failures_by_test)} test categories\n\n"
            
            # Add top failure categories
            if failures_by_test:
                prompt += "**Main Issue Categories**:\n"
                for test_id, comments in list(failures_by_test.items())[:5]:
                    prompt += f"- **{test_id}**: {len(comments)} issues\n"
                    # Add 1-2 example comments
                    for comment in comments[:2]:
                        if comment and comment.strip():
                            prompt += f"  • {comment}\n"
                prompt += "\n"
        else:
            prompt += "**Excellent news!** No validation issues were found in your dataset.\n\n"
        
        if not amendments_applied.empty:
            prompt += f"**Automatic Improvements**: {len(amendments_applied):,} records were automatically enhanced with standardized values.\n\n"
        
        # Add common issues if available
        if common_issues:
            prompt += "**Most Common Issues**:\n"
            for issue, count in list(common_issues.items())[:3]:
                prompt += f"- {issue} ({count} occurrences)\n"
            prompt += "\n"
        
        # Add user context if available
        if user_message and user_message.strip():
            prompt += f"**User's Original Message**: {user_message[:200]}{'...' if len(user_message) > 200 else ''}\n\n"
        
        prompt += """## YOUR TASK
Write a professional, encouraging email that:

1. **Acknowledge** their submission and thank them for using BDQ
2. **Summarize** the key findings in clear, non-technical language
3. **Explain** what the issues mean for their research (if any)
4. **Provide** specific, actionable advice for improving data quality
5. **Describe** what the attached files contain and how to use them
6. **Encourage** them to resubmit after making improvements

## TONE & STYLE
- Professional but friendly
- Use language a biologist would understand
- Focus on being helpful, not just reporting numbers
- Be encouraging about data quality improvement
- Keep it concise but comprehensive

## FORMAT
Write as a complete email body (no subject line needed). Use clear paragraphs and bullet points where appropriate."""

        return prompt
    
    def _create_empty_results_prompt(self, original_email_data, core_type):
        """Create prompt for when no test results are available"""
        user_message = ""
        if isinstance(original_email_data, dict):
            user_message = original_email_data.get('body', '')
            if not user_message:
                user_message = original_email_data.get('text', '')
        
        prompt = f"""You are a biodiversity data quality expert writing an email for a researcher who submitted a {core_type} core dataset for BDQ testing.

## SITUATION
The dataset was submitted but no applicable BDQ tests could be run. This could be due to:
- Missing required columns for BDQ testing
- Dataset format issues
- No tests available for the provided data structure

## USER CONTEXT
"""
        if user_message:
            prompt += f"**User's Message**: {user_message[:200]}{'...' if len(user_message) > 200 else ''}\n\n"
        
        prompt += """## YOUR TASK
Write a helpful email that:
1. Acknowledges their submission
2. Explains why no tests could be run
3. Provides guidance on what columns/data are needed for BDQ testing
4. Encourages them to check the BDQ documentation or resubmit with additional data
5. Maintains a helpful, professional tone

Write as a complete email body explaining the situation and next steps."""

        return prompt
    
    def _convert_to_html(self, text: str) -> str:
        """Convert plain text to basic HTML"""
        # Simple conversion - replace newlines with <br> and wrap in <p> tags
        html = text.replace('\n\n', '</p><p>').replace('\n', '<br>')
        return f"<p>{html}</p>"
    