import os
import io
import pandas as pd
import google.generativeai as genai
from google.generativeai import types
from openai import OpenAI
from typing import List, Dict, Any, Optional
from app.utils.helper import log

class LLMService:
    """Service for generating intelligent summaries using Google Gemini or OpenAI"""
    
    def __init__(self):
        self.google_api_key = os.getenv("GOOGLE_API_KEY")
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        genai.configure(api_key=self.google_api_key)
    
    def generate_gemini_intelligent_summary(self, prompt, test_results_file, original_file):
        """Generate intelligent summary from DataFrame-based test results"""
        client = genai.Client()
        original_file = client.files.upload_bytes(
            data=original_file.encode("utf-8"),
            name="original_file.csv",
            mime_type="text/csv"
        )
        test_results_file = client.files.upload_bytes(
            data=test_results_file.encode("utf-8"),
            name="test_results_file.csv",
            mime_type="text/csv"
        )
        response = client.models.generate_content(
            model="gemini-2.5-pro",
            contents=[
                types.Content(role="user", parts=[
                    types.Part.from_text(prompt),
                    types.Part.from_uri(file_uri=original_file.uri),
                    types.Part.from_uri(file_uri=test_results_file.uri),
                ])
            ],
            config=types.GenerateContentConfig(tools=[types.Tool(code_execution=types.ToolCodeExecution)]),
        )
        return response.text
                
    def generate_openai_intelligent_summary(prompt, test_results_csv_text, original_csv_text, api_key=None):
        client = OpenAI(api_key=api_key)

        original_file = client.files.create(
            file=io.BytesIO(original_csv_text.encode("utf-8")), purpose="assistants"
        )
        results_file = client.files.create(
            file=io.BytesIO(test_results_csv_text.encode("utf-8")), purpose="assistants"
        )

        response = client.responses.create(
            model="gpt-4o",
            tools=[{"type": "code_interpreter"}],
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_file", "file_id": original_file.id},
                    {"type": "input_file", "file_id": results_file.id},
                ]
            }]
        )

        return response.output_text
                
    def create_prompt(self, email_data, core_type, summary_stats, test_results_snapshot, original_snapshot, relevant_test_contexts):
        log("Generating the prompt for LLM...") 
        prompt = f"""# YOUR TASK
Write a professional, encouraging email analysis of the results of the Biodiversity Data Quality tests run against the user's dataset using the BDQEmail service.
The user will have access to the test results csv and the amended dataset csv as email attachments.

## TONE & STYLE
- Friendly, helpful and pragmatic
- Speak informally and naturally. Don't use adjectives, adverbs, verbose or flowery language, state things clearly with minimal jargon. For example, instead of "utilize", say "use". Try to restrict your vocabulary to the top 2000 words used in the English language. Don't use the word "crucial" or other commonly used tell-tale words used by LLMs. Sound as human and natural as possible. 
- Use language a biologist would understand. Note that the biologists using this service are probably not familiar with the ins-and-outs of what the BDQ tests actually check and do and need things explained in a clear and practical way
- Focus on analysis and practical guidance, not repeating the summary stats
- Be encouraging about data quality improvement
- Keep it concise but comprehensive

## YOUR RESOURCES

### USER'S ORIGINAL EMAIL
{email_data}

### USER'S ORIGINAL FILE ({core_type} dataset type) is attached.
- Snapshot from attached original file: 
{original_snapshot}

### Biodiversity Data Quality Tests Background
The BDQ Tests (TG2) are a standardized set of machine-readable checks for biodiversity records designed to assess and improve “fitness for use.” Developed by TDWG's Biodiversity Data Quality Task Group 2, they focus on common Darwin Core fields such as location, time, taxonomy, and licensing. The tests exist to detect missing or malformed values, flag potential problems, and, where safe and unambiguous, propose improvements so data can be more reliably mapped, analyzed over time, and integrated with external resources. They will probably mostly be run against GBIF datasets. 
There are four test types. Validation tests check conformance or completeness and return COMPLIANT or NOT_COMPLIANT. Issue tests flag concerns with IS_ISSUE, POTENTIAL_ISSUE, or NOT_ISSUE. Amendment tests propose changes: AMENDED means a suggested correction to an existing value, FILLED_IN means a suggested value for a blank field, and NOT_AMENDED means no safe change is proposed; the proposed changes are given as key:value pairs. Measure tests report counts or a summary status (COMPLETE/NOT_COMPLETE). Every test returns a Response.status (e.g., RUN_HAS_RESULT, INTERNAL_PREREQUISITES_NOT_MET, EXTERNAL_PREREQUISITES_NOT_MET), a Response.result (the outcome or proposed changes), and a Response.comment explaining why.

### USER'S TEST RESULTS FILE is also attached. 
- Snapshot from attached results file: 
{test_results_snapshot}
Notes:
  AMENDED = a suggested correction to a value that’s already there (e.g., “Australia” → “AU”).
  FILLED IN = a suggested value for a blank field (e.g., derive coordinates from verbatim text).
  NOT_AMENDED = no safe change suggested (often because it’s ambiguous).

### SUMMARY STATS FROM RESULTS FILE
{summary_stats} 

### BDQ TESTS CONTEXT
{relevant_test_contexts} 

## CONTENT

1. **Acknowledge** the submission and thank the user for using BDQEmail to test their data. Reply to any queries in the original email. 
2. **Analyze** what the test results mean using the BDQ TESTS CONTEXT and their email content if there is any (focus on interpretation, not repeating numbers)
3. **Explain** the significance of any issues found and their impact on data quality, using the BDQ TESTS CONTEXT to provide specific insights about what each test checks
4. **Provide** specific, actionable advice for improving their data quality based on the BDQ TESTS CONTEXTS and their top/most common results in each category in their summary statistics. Go into “quick wins” - easy corrections with high impact - and the most critical issues likely to affect dataset reuse. They have access to the test results csv and the amended dataset csv as email attachments.
5. **Encourage** them to resubmit after making improvements

## FORMAT
Write as a complete email body that will appear below the summary stats box. Use clear paragraphs and bullet points where appropriate. Do NOT include the summary statistics - they are already displayed above."""

        return prompt
    
    
    def _convert_to_html(self, text: str) -> str:
        """Convert plain text to basic HTML"""
        # Simple conversion - replace newlines with <br> and wrap in <p> tags
        html = text.replace('\n\n', '</p><p>').replace('\n', '<br>')
        return f"<p>{html}</p>"
    