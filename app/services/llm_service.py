import os
import io
import pandas as pd
from google import genai
from google.genai import types
from openai import OpenAI
from typing import List, Dict, Any, Optional
from app.utils.helper import log
import base64

class LLMService:
    """Service for generating intelligent summaries using Google Gemini or OpenAI"""
    
    def __init__(self):
        self.google_api_key = os.getenv("GOOGLE_API_KEY")
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        # The new google.genai client uses environment variables automatically
    
    def generate_gemini_intelligent_summary(self, prompt, test_results_file, original_file):
        """Generate intelligent summary from DataFrame-based test results using file uploads and code execution"""
        # Use the new google.genai client with code execution
        client = genai.Client()
        
        # Create the content with file uploads using inline_data
        contents = [
            {"text": prompt},
            {
                "inline_data": {
                    "mime_type": "text/csv",
                    "data": base64.b64encode(original_file.encode('utf-8')).decode('utf-8')
                }
            },
            {
                "inline_data": {
                    "mime_type": "text/csv", 
                    "data": base64.b64encode(test_results_file.encode('utf-8')).decode('utf-8')
                }
            }
        ]
        
        # Generate content with code execution tool
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=contents,
            config=types.GenerateContentConfig(
                tools=[types.Tool(code_execution=types.ToolCodeExecution)]
            )
        )
        
        # Extract text from response
        text_parts = []
        for part in response.candidates[0].content.parts:
            if part.text is not None:
                text_parts.append(part.text)
        
        response_text = "\n".join(text_parts)
        
        # Check if response contains HTML tags, if not convert to HTML
        if not self._contains_html_tags(response_text):
            response_text = self._convert_to_html(response_text)
        
        log(f"Gemini LLM response: {response_text}")
        return response_text
                
    def generate_openai_intelligent_summary(self, prompt, test_results_csv_text, original_csv_text, api_key=None):
        client = OpenAI(api_key=api_key)

        # Upload files to OpenAI
        original_file = client.files.create(
            file=io.BytesIO(original_csv_text.encode("utf-8")), 
            purpose="assistants"
        )
        results_file = client.files.create(
            file=io.BytesIO(test_results_csv_text.encode("utf-8")), 
            purpose="assistants"
        )

        # Use the Assistants API with file attachments (Responses API requires container setup)
        assistant = client.beta.assistants.create(
            name="BDQ Data Analyst",
            instructions="You are a biodiversity data quality analyst. Analyze the provided CSV files and generate a comprehensive summary.",
            model="gpt-4o",
            tools=[{"type": "code_interpreter"}]
        )

        # Create a thread and run
        thread = client.beta.threads.create()
        
        # Attach files to the thread; prompt should be compact (we sanitize inputs)
        client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=prompt,
            attachments=[
                {"file_id": original_file.id, "tools": [{"type": "code_interpreter"}]},
                {"file_id": results_file.id, "tools": [{"type": "code_interpreter"}]}
            ]
        )

        run = client.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=assistant.id
        )

        # Wait for completion
        import time
        while run.status in ['queued', 'in_progress', 'cancelling']:
            time.sleep(1)
            run = client.beta.threads.runs.retrieve(
                thread_id=thread.id,
                run_id=run.id
            )

        if run.status == 'completed':
            messages = client.beta.threads.messages.list(thread_id=thread.id)
            # Get the assistant's response (first message should be the assistant's response)
            assistant_message = messages.data[0]
            
            # Extract text from all content blocks
            response_parts = []
            for content_block in assistant_message.content:
                if hasattr(content_block, 'text') and content_block.text:
                    response_parts.append(content_block.text.value)
                elif hasattr(content_block, 'type') and content_block.type == 'text':
                    response_parts.append(content_block.text.value)
            
            if response_parts:
                response_text = "\n".join(response_parts)
                # Check if response contains HTML tags, if not convert to HTML
                if not self._contains_html_tags(response_text):
                    response_text = self._convert_to_html(response_text)
                log(f"OpenAI LLM response: {response_text}")
            else:
                # If no text content, return a message indicating the analysis was completed
                response_text = "Analysis completed successfully. The assistant has analyzed your CSV files and generated insights using the code interpreter."
        else:
            raise Exception(f"OpenAI run failed with status: {run.status}")
        
        return response_text
    
    # (Truncation helper removed from use; we rely on sanitization instead.)
                
    def create_prompt(self, email_data, core_type, summary_stats, test_results_snapshot, original_snapshot, relevant_test_contexts):
        log("Generating the prompt for LLM...") 
        # Sanitize email metadata to avoid embedding base64 attachments or large blobs
        def _summarize_email_meta(ed):
            try:
                if not isinstance(ed, (dict,)):
                    return str(ed)
                headers = ed.get('headers', {}) if isinstance(ed.get('headers'), dict) else {}
                attachments = ed.get('attachments', []) or []
                att_summary = []
                for att in attachments:
                    try:
                        att_summary.append({
                            'filename': att.get('filename'),
                            'mimeType': att.get('mimeType'),
                            'size': len(att.get('contentBase64', ''))
                        })
                    except Exception:
                        continue
                summary = {
                    'from': headers.get('from', ed.get('from')),
                    'to': headers.get('to', ed.get('to')),
                    'subject': headers.get('subject', ed.get('subject')),
                    'threadId': ed.get('threadId'),
                    'attachments': att_summary,
                }
                return summary
            except Exception:
                return str(ed)

        email_data = _summarize_email_meta(email_data)

        prompt = f"""# YOUR TASK
You are BDQEmail, a biodiversity data quality analyst assistant. You are helping a user with their dataset by analysing the results of a set of Biodiversity Data Quality tests run against all the relevant fields that could be found in the dataset. 
Write a professional, encouraging email analysis in HTML format, the email will include a link to the dashboard after your reply and be sent to the user automatically after you have generated it.
The user will receive the body of your email with the summary stats prefixed to it, and the email body will include a link to a dashboard allowing the user to explore the test results interactively and download the raw results and amended dataset files. 

You have access to two CSV files:
1. The original biodiversity dataset (occurrence data)
2. The test results from running BDQ tests on that dataset

Use the code execution tool to explore these files as needed to understand the data and generate insights.

## TONE & STYLE
- Friendly, helpful and pragmatic
- Speak informally and naturally. Don't use adjectives, adverbs, verbose or flowery language, state things clearly with minimal jargon. For example, instead of "utilize", say "use". Try to restrict your vocabulary to the top 2000 words used in the English language. Don't use the word "crucial" or other commonly used tell-tale words used by LLMs. Sound as human and natural as possible. 
- Use language a biologist would understand. Note that the biologists using this service are probably not familiar with the ins-and-outs of what the BDQ tests actually check and do and need things explained in a clear and practical way
- Focus on analysis and practical guidance, not repeating the summary stats
- Be encouraging about data quality improvement
- Keep it concise but comprehensive
- Use HTML formatting

## YOUR RESOURCES

### USER'S ORIGINAL EMAIL (metadata)
{email_data}

### USER'S ORIGINAL FILE ({core_type} dataset type) is attached as a CSV file.
- Snapshot from attached original file (shortened): 
{original_snapshot}

### Biodiversity Data Quality Tests Background
The BDQ Tests (TG2) are a standardized set of machine-readable checks for biodiversity records designed to assess and improve "fitness for use." Developed by TDWG's Biodiversity Data Quality Task Group 2, they focus on common Darwin Core fields such as location, time, taxonomy, and licensing. The tests exist to detect missing or malformed values, flag potential problems, and, where safe and unambiguous, propose improvements so data can be more reliably mapped, analyzed over time, and integrated with external resources. They will probably mostly be run against GBIF datasets. 
There are four test types. Validation tests check conformance or completeness and return COMPLIANT or NOT_COMPLIANT. Issue tests flag concerns with IS_ISSUE, POTENTIAL_ISSUE, or NOT_ISSUE. Amendment tests propose changes: AMENDED means a suggested correction to an existing value, FILLED_IN means a suggested value for a blank field, and NOT_AMENDED means no safe change is proposed; the proposed changes are given as key:value pairs. Measure tests report counts or a summary status (COMPLETE/NOT_COMPLETE). Every test returns a Response.status (e.g., RUN_HAS_RESULT, INTERNAL_PREREQUISITES_NOT_MET, EXTERNAL_PREREQUISITES_NOT_MET), a Response.result (the outcome or proposed changes), and a Response.comment explaining why.

### USER'S TEST RESULTS FILE is also attached as a CSV file.
- Snapshot from attached results file (shortened): 
{test_results_snapshot}
Notes:
  AMENDED = a suggested correction to a value that's already there (e.g., "Australia" → "AU").
  FILLED IN = a suggested value for a blank field (e.g., derive coordinates from verbatim text).
  NOT_AMENDED = no safe change suggested (often because it's either actually correct and does not need amending, or the correction is ambiguous).

### SUMMARY STATS FROM RESULTS FILE
{summary_stats} 

### BDQ TESTS CONTEXT (shortened)
{relevant_test_contexts} 

## YOUR PROCESS
Start by acknowledging the submission and thank the user for using BDQEmail to test their data. Reply to any queries in the original email. 

Carefully read the summary stats annd BDQ tests context. 
Start with the amendments - explain that the amended dataset (available for download from the dashboard) contains all the quick wins for the user automatically applied to their dataset. Describe what amendments were made. 
Next go into the top issues - think in terms of real world practicality - which of these should the user be paying attention to? Go into detail and make suggestions.
Look at the non-compliant validations - which are the quick and easy wins there? Which are the most critical that need paying attention to? Give a detailed analysis. Explore the full test results file and the user's original data if necessary to provide practical and actionable suggestions. For this stage it is important to be practical. What gets often automatically corrected by GBIF when the dataset gets ingested?

Finally go into some guidance as to what you think fixes should be. This should be an intelligent analysis with real meat in it, you can mention general changes if you want but at the least scatter it with a few suggestions for actual fixes on their actual records. 
For example, if they have the Validation Lifestage Standard as non compliant for records where the value was C1-C3 you can say: 
```Darwin Core lifeStage expects a plain-text description, ideally using a controlled vocabulary but not strictly enforced.

For C1-C3, the best practice would be to write it out clearly so it's understandable outside the plankton/copepod context. A good normalized value would be:

copepodite stages 1-3

This keeps:

The taxon-specific term ("copepodite")

The stage range (1-3)

A human-readable format

If you want to be even more precise and interoperable, you could use the WoRMS LifeStage terms (if your dataset links to WoRMS):

http://marinespecies.org/aphia.php?p=lsid&lsid=copepodite%20stage%201-3```
Another example, if their data lots of records which got amended because of a case change (e.g. male -> Male or something), mention that they can just 

Summmarise and provide some key takeaways at the end. I want you to showcase your understanding of the BDQ tests and your ability to help the user with their data quality issues. 

## FORMAT
Write as a complete HTML email body that will appear below the summary stats box. Use clear paragraphs, bullet points and other formatting where appropriate. 
Do NOT include the summary statistics or the link to the dashboard - they are already displayed to the user above your email body."""
        # Log full prompt for debugging/traceability as requested
        log(f"LLM prompt ({len(prompt)} chars):\n{prompt}")
        return prompt
    
    
    def _contains_html_tags(self, text: str) -> bool:
        """Check if text contains HTML tags"""
        import re
        # Look for common HTML tags
        html_pattern = r'<[^>]+>'
        return bool(re.search(html_pattern, text))
    
    def _convert_to_html(self, text: str) -> str:
        """Convert plain text to basic HTML"""
        # Simple conversion - replace newlines with <br> and wrap in <p> tags
        html = text.replace('\n\n', '</p><p>').replace('\n', '<br>')
        return f"<p>{html}</p>"
    
