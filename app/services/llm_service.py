import os
import pandas as pd
import google.generativeai as genai
from typing import List, Dict, Any, Optional
from app.utils.helper import log

class LLMService:
    """Service for generating intelligent summaries using Google Gemini"""
    
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY")
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-2.5-pro')
        self.bdq_tests_df = self._load_bdq_tests()
    
    def _load_bdq_tests(self) -> pd.DataFrame:
        """Load BDQ tests information from CSV file"""
        csv_path = os.path.join(os.path.dirname(__file__), '..', 'TG2_tests.csv')
        df = pd.read_csv(csv_path, dtype=str)  # Load all as strings to avoid float conversions
        log(f"Loaded {len(df)} BDQ tests from CSV")
        return df
    
    def _get_relevant_test_info(self, test_ids: List[str]) -> str:
        """Get relevant BDQ test information for the given test IDs"""
        if not test_ids:
            return ""
        
        # Filter tests that are in our results
        relevant_tests = self.bdq_tests_df[
            self.bdq_tests_df['Label'].isin(test_ids)
        ]
        
        # Select only the columns we need
        columns_needed = [
            'Label', 'InformationElement:ActedUpon', 'InformationElement:Consulted',
            'ExpectedResponse', 'Description', 'Examples', 'Notes'
        ]
        
        # Filter to only existing columns
        available_columns = [col for col in columns_needed if col in relevant_tests.columns]
        test_info = relevant_tests[available_columns].copy()
        
        # Build informative text about the tests
        test_context = "\n## BDQ TEST CONTEXT\n"
        test_context += "The following tests were run on the dataset. Here's what each test checks:\n\n"
        
        for _, test in test_info.iterrows():
            # Safely extract and convert values to strings, handling NaN/float values
            label = str(test.get('Label', 'Unknown Test'))
            description = str(test.get('Description', 'No description available'))
            acted_upon = str(test.get('InformationElement:ActedUpon', 'N/A'))
            consulted = str(test.get('InformationElement:Consulted', 'N/A'))
            expected_response = str(test.get('ExpectedResponse', 'N/A'))
            examples = str(test.get('Examples', 'No examples available'))
            notes = str(test.get('Notes', ''))
            
            # Handle NaN values that might be converted to 'nan' string
            if notes == 'nan' or notes == 'None':
                notes = ''
            if description == 'nan' or description == 'None':
                description = 'No description available'
            if acted_upon == 'nan' or acted_upon == 'None':
                acted_upon = 'N/A'
            if consulted == 'nan' or consulted == 'None':
                consulted = 'N/A'
            if expected_response == 'nan' or expected_response == 'None':
                expected_response = 'N/A'
            if examples == 'nan' or examples == 'None':
                examples = 'No examples available'
            
            test_context += f"**{label}**:\n"
            test_context += f"- **Purpose**: {description}\n"
            test_context += f"- **Checks**: {acted_upon}\n"
            if consulted and consulted != 'N/A':
                test_context += f"- **References**: {consulted}\n"
            if expected_response and expected_response != 'N/A':
                test_context += f"- **Expected Results**: {expected_response[:200]}{'...' if len(expected_response) > 200 else ''}\n"
            if examples and examples != 'No examples available':
                test_context += f"- **Examples**: {examples[:150]}{'...' if len(examples) > 150 else ''}\n"
            if notes:
                test_context += f"- **Notes**: {notes[:100]}{'...' if len(notes) > 100 else ''}\n"
            test_context += "\n"
        
        return test_context
    
    def generate_intelligent_summary(self, test_results_df, email_content, core_type, summary_stats):
        """Generate intelligent summary from DataFrame-based test results"""
        log("Generating the prompt for gemini...") 
        prompt = self._create_summary_prompt(test_results_df, email_content, core_type, summary_stats)
        log("Prompt generated!")
        log(prompt)
        # Call Gemini API
        response = self.model.generate_content(prompt)
        
        if response.text:
            log(response.text[:1000])
            # Parse the response and generate both text and HTML versions
            return self._convert_to_html(response.text.strip())
        else:
            log("No response text from Gemini API", "ERROR")
            return "<p>Unable to generate summary at this time.</p>"
                
    
    def _create_summary_prompt(self, test_results_df, email_content, core_type, summary_stats):
        """Create the prompt for the LLM using pre-computed summary statistics"""
        
        # Handle empty results
        if test_results_df is None or test_results_df.empty:
            return self._create_empty_results_prompt(email_content, core_type)
        
        # Get data from summary stats (no need to re-analyze DataFrame)
        validation_failures_count = summary_stats.get('validation_failures', 0)
        amendments_applied_count = summary_stats.get('amendments_applied', 0)
        failure_counts_by_test = summary_stats.get('failure_counts_by_test', {})
        common_issues = summary_stats.get('common_issues', {})
        
        # Extract unique test IDs from the results for BDQ context
        unique_test_ids = []
        if 'test_id' in test_results_df.columns:
            unique_test_ids = test_results_df['test_id'].unique().tolist()
        elif 'test' in test_results_df.columns:
            unique_test_ids = test_results_df['test'].unique().tolist()
        
        # Get BDQ test context information
        bdq_test_context = self._get_relevant_test_info(unique_test_ids)
        
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
        
        # Add user context
        prompt += f"\n**User's Original Message**: {email_content[:200]}{'...' if len(email_content) > 200 else ''}\n"
        
        # Add BDQ test context if available
        if bdq_test_context:
            prompt += bdq_test_context
        
        prompt += """

## YOUR TASK
Write a professional, encouraging email analysis that:

1. **Acknowledge** their submission and thank them for using BDQ. Reply to any queries in the original email. 
2. **Analyze** what the test results mean for their research, again using their email content if there is any (focus on interpretation, not repeating numbers)
3. **Explain** the significance of any issues found and their impact on data quality, using the BDQ test context above to provide specific insights about what each test checks
4. **Provide** specific, actionable advice for improving data quality based on the test descriptions and examples
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
    
    def _create_empty_results_prompt(self, email_content, core_type):
        """Create prompt for when no test results are available"""
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
        prompt += f"**User's Message**: {email_content[:200]}{'...' if len(email_content) > 200 else ''}\n"
        
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
    