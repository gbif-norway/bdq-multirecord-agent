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
                
    def generate_openai_intelligent_summary(self, prompt, test_results_csv_text, original_csv_text, curated_csv_text=None, recipient_name: str = None, api_key=None):
        """Generate summary using the Responses API with code interpreter and file attachments."""
        client = OpenAI(api_key=api_key)

        # Upload files (same purpose value works for file tools)
        original_file = client.files.create(
            file=io.BytesIO(original_csv_text.encode("utf-8")),
            purpose="assistants",
        )
        results_file = client.files.create(
            file=io.BytesIO(test_results_csv_text.encode("utf-8")),
            purpose="assistants",
        )
        curated_file = None
        if curated_csv_text:
            curated_file = client.files.create(
                file=io.BytesIO(curated_csv_text.encode("utf-8")),
                purpose="assistants",
            )

        # Provide files to the code interpreter via tool_resources
        file_ids = [original_file.id, results_file.id]
        if curated_file is not None:
            file_ids.append(curated_file.id)

        # Use GPT-5 as requested; no fallback to other models
        model = "gpt-5"

        # For now, rely on prompt + embedded curated CSV; omit tools until SDK/API stabilizes
        response = client.responses.create(
            model=model,
            input=prompt,
        )

        # Prefer SDK helper, fallback to manual extraction
        response_text = None
        try:
            response_text = getattr(response, "output_text", None)
        except Exception:
            response_text = None
        if not response_text:
            try:
                parts = []
                if hasattr(response, "output") and response.output:
                    for item in response.output:
                        try:
                            for part in getattr(item, "content", []) or []:
                                txt = getattr(part, "text", None)
                                if txt:
                                    parts.append(txt)
                        except Exception:
                            continue
                response_text = "".join(parts) if parts else None
            except Exception:
                response_text = None
        if not response_text:
            response_text = str(response)

        # Normalize to HTML
        if not self._contains_html_tags(response_text):
            response_text = self._convert_to_html(response_text)

        # Enforce greeting
        stripped = response_text.lstrip().lower()
        starts_ok = stripped.startswith("thanks for your email") or stripped.startswith("thanks for reaching out")
        if not starts_ok:
            if recipient_name:
                response_text = f"<p>Thanks for your email, {recipient_name}.</p>\n" + response_text
            else:
                response_text = f"<p>Thanks for your email,</p>\n" + response_text

        log(f"OpenAI LLM response: {response_text}")
        return response_text
    
    # (Truncation helper removed from use; we rely on sanitization instead.)
                
    def create_prompt(self, email_data, core_type, summary_stats, test_results_snapshot, original_snapshot, relevant_test_contexts, curated_joined_csv_text: Optional[str] = None):
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

        has_curated = curated_joined_csv_text is not None and curated_joined_csv_text != ""
        
        # Build curated section separately to avoid backslashes in f-string
        curated_section = ""
        if has_curated:
            curated_section = (f"### CURATED FOCUS SET (untruncated)\n"
                             f"This CSV is also attached. It contains only the unique rows where status ∈ {{AMENDED, FILLED_IN}} or result ∈ {{NOT_COMPLIANT, POTENTIAL_ISSUE}}, joined with TG2 test context columns (description, notes, type, IE class, etc.).\n"
                             f"Use this set to prioritise guidance without re-deriving groupings.\n"
                             f"\n```csv\n{curated_joined_csv_text}\n```\n")

        prompt = f"""# YOUR TASK
You are BDQEmail, a biodiversity data quality analyst assistant. You are helping a user with their dataset by analysing the results of a set of Biodiversity Data Quality tests run against all the relevant fields that could be found in the dataset. 
Write a professional, encouraging email analysis in HTML format, the email will include a link to the dashboard after your reply and be sent to the user automatically after you have generated it.
The user will receive the body of your email with the summary stats prefixed to it, and the email body will include a link to a dashboard allowing the user to explore the test results interactively and download the raw results and amended dataset files. 

You have access to these CSV files:
1. The original biodiversity dataset (occurrence data)
2. The test results from running BDQ tests on that dataset
{('- 3. A curated focus set of unique rows that either (a) were AMENDED or FILLED_IN, or (b) had results of NOT_COMPLIANT or POTENTIAL_ISSUE, joined with TG2 test definitions for rich context.' ) if has_curated else ''}

Use the code execution tool to explore these files as needed to understand the data and generate insights.

## TONE & STYLE
- Friendly, helpful and pragmatic
- Speak informally and naturally. Don't use adjectives, adverbs, verbose or flowery language, state things clearly with minimal jargon. For example, instead of "utilize", say "use". Try to restrict your vocabulary to the top 2000 words used in the English language. Don't use the word "crucial" or other commonly used tell-tale words used by LLMs. Sound as human and natural as possible. 
- Use language a biologist would understand. Note that the biologists using this service are probably not familiar with the ins-and-outs of what the BDQ tests actually check and do and need things explained in a clear and practical way
- Focus on analysis and practical guidance, not repeating the summary stats
- Be encouraging about data quality improvement
- Keep it concise but comprehensive
- Use HTML formatting
 - Avoid phrases like: "To analyze", "we need to", "this analysis", "identify the key issues".

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

{curated_section if has_curated else ''}

### SUMMARY STATS FROM RESULTS FILE
{summary_stats} 

### BDQ TESTS CONTEXT (shortened)
{relevant_test_contexts} 

## YOUR PROCESS
Start with a one-line thanks (e.g., "Thanks for your email,") and, if obvious, address the sender by name. Reply to any queries in the original email. 

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
 Another example: if many records were amended for simple case normalization (e.g., "male" → "Male"), note these are safe automatic fixes. Recommend adopting consistent casing in the source to avoid repeated downstream changes.

Summmarise and provide some key takeaways at the end. I want you to showcase your understanding of the BDQ tests and your ability to help the user with their data quality issues. 

### Additional example patterns (use when relevant)
- Country normalization (validation NOT_COMPLIANT): If `dwc:country` has values like "UK" or "U.S.", suggest ISO country names/codes and aligning `dwc:countryCode` (e.g., `country=United Kingdom`, `countryCode=GB`). Note GBIF often normalizes country/ISO during ingestion, but advise updating source values to avoid mismatches.
- Coordinate uncertainty (issue POTENTIAL_ISSUE or validation NOT_COMPLIANT): If `dwc:decimalLatitude/Longitude` exist without `dwc:coordinateUncertaintyInMeters`, recommend adding a reasonable uncertainty based on collection method (e.g., 30–100 m for handheld GPS) and documenting the basis.
- License missing (NOT_COMPLIANT): For missing/invalid `dwc:license`, recommend a specific machine-readable license string (e.g., "CC BY 4.0") and link to https://creativecommons.org/licenses/by/4.0/. Explain how this improves re-use and visibility in aggregators.

## FORMAT
Write as a complete HTML email body that will appear below the summary stats box. Use clear paragraphs, bullet points and other formatting where appropriate. 
Return only the email body in HTML (no headings like "YOUR TASK" etc.).
Begin with: "Thanks for your email," or "Thanks for reaching out,".
Do not include the summary statistics or the link to the dashboard - they are already displayed to the user above your email body."""
        # Log full prompt for debugging/traceability as requested
        log(f"LLM prompt ({len(prompt)} chars):\n" + prompt)
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
    
