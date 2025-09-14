import os
from openai import OpenAI
from typing import List, Dict, Any, Optional
from app.utils.helper import log, network_retry

class LLMService:
    """Service for generating intelligent summaries using Google Gemini or OpenAI"""
    
    def __init__(self):
        self.google_api_key = os.getenv("GOOGLE_API_KEY")
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        # The new google.genai client uses environment variables automatically
    
    def generate_gemini_intelligent_summary(self, prompt, test_results_file, original_file):
        """Simplified: we no longer attach files or use code execution for Gemini in this service.
        Delegate to the OpenAI Responses path to keep a single code path.
        """
        return self.generate_openai_intelligent_summary(prompt, test_results_file, original_file)
                
    def generate_openai_intelligent_summary(self, prompt, test_results_csv_text, original_csv_text, curated_csv_text=None, recipient_name: str = None, api_key=None):
        """Generate summary using the Responses API (prompt-only; no file attachments)."""
        client = OpenAI(api_key=api_key)

        # Use GPT-5 as requested; no fallback to other models
        model = "gpt-5"

        # For now, rely on prompt + embedded curated CSV; omit tools until SDK/API stabilizes

        @network_retry()
        def _create_response():
            return client.responses.create(
                model=model,
                input=prompt,
            )

        response = _create_response()

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

        log(f"OpenAI LLM response: {response_text}")
        return response_text
    
    # (Truncation helper removed from use; we rely on sanitization instead.)
                
    def create_prompt(self, email_data, core_type, summary_stats, test_results_snapshot, original_snapshot, curated_joined_csv_text: Optional[str] = None, failed_tests: Optional[List[str]] = None):
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

        # Build curated section (include only when provided)
        curated_section = ""
        if curated_joined_csv_text:
            curated_section = (f"### CURATED FOCUS SET (untruncated)\n"
                               f"This table contains only the unique rows where status ∈ {{AMENDED, FILLED_IN}} or result ∈ {{NOT_COMPLIANT, POTENTIAL_ISSUE}}, joined with TG2 test context columns (description, notes, type, IE class, etc.).\n"
                               f"Use this set to prioritise guidance without re-deriving groupings.\n"
                               f"\n```csv\n{curated_joined_csv_text}\n```\n")

        # Optional note about tests that could not be run due to timeouts or other API limits
        failed_section = ""
        if failed_tests:
            failed_list = ", ".join(failed_tests)
            failed_section = (
                f"\n### NOTE ABOUT UNRUN TESTS\n"
                f"The following BDQ tests could not be completed due to service timeouts or limits: {failed_list}.\n"
                f"Do not attempt to infer results for them. Simply inform the user in one short paragraph that these tests could not be run this time, and that they can try again later if they are specifically interested in those tests.\n"
            )

        prompt = f"""# YOUR TASK
You are BDQEmail, a biodiversity data quality analyst assistant. You are helping a user with their dataset by analysing the results of a set of Biodiversity Data Quality tests run against all the relevant fields that could be found in the dataset. 
Write a professional, encouraging email analysis in HTML format, the email will include summary stats and a link to the dashboard at the top of the email and be sent to the user automatically after you have generated it.
The user will receive the body of your email with the summary stats prefixed to it, and the email body will include a link to a dashboard allowing the user to explore the test results interactively and download the raw results and amended dataset files. It is not possible for the user to reply to you and for you to interact further with the user, this is a one-shot email. 

You have the following context within this prompt (no external files to load):
1. A snapshot of the original biodiversity dataset
2. A snapshot of the BDQ test results
3. A curated focus set of unique rows with amendments/issues joined to TG2 definitions

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

### ORIGINAL DATASET SNAPSHOT ({core_type})
- Snapshot (shortened): 
{original_snapshot}

### Biodiversity Data Quality Tests Background
The BDQ Tests (TG2) are a standardized set of machine-readable checks for biodiversity records designed to assess and improve "fitness for use." Developed by TDWG's Biodiversity Data Quality Task Group 2, they focus on common Darwin Core fields such as location, time, taxonomy, and licensing. The tests exist to detect missing or malformed values, flag potential problems, and, where safe and unambiguous, propose improvements so data can be more reliably mapped, analyzed over time, and integrated with external resources. They will probably mostly be run against GBIF datasets. 
There are four test types. Validation tests check conformance or completeness and return COMPLIANT or NOT_COMPLIANT. Issue tests flag concerns with IS_ISSUE, POTENTIAL_ISSUE, or NOT_ISSUE. Amendment tests propose changes: AMENDED means a suggested correction to an existing value, FILLED_IN means a suggested value for a blank field, and NOT_AMENDED means no safe change is proposed; the proposed changes are given as key:value pairs. Measure tests report counts or a summary status (COMPLETE/NOT_COMPLETE). Every test returns a Response.status (e.g., RUN_HAS_RESULT, INTERNAL_PREREQUISITES_NOT_MET, EXTERNAL_PREREQUISITES_NOT_MET), a Response.result (the outcome or proposed changes), and a Response.comment explaining why.

### BDQ TEST RESULTS SNAPSHOT
- Snapshot (shortened): 
{test_results_snapshot}
Notes:
  AMENDED = a suggested correction to a value that's already there (e.g., "Australia" → "AU").
  FILLED IN = a suggested value for a blank field (e.g., derive coordinates from verbatim text).
  NOT_AMENDED = no safe change suggested (often because it's either actually correct and does not need amending, or the correction is ambiguous).

{curated_section}

{failed_section}

### SUMMARY STATS FROM RESULTS FILE
{summary_stats} 



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

Summmarise and provide some key takeaways at the end. I want you to showcase your understanding of the BDQ tests and your ability to help the user with their data quality issues. 

### Additional example patterns (use when relevant)
- Country normalization (validation NOT_COMPLIANT): If `dwc:country` has values like "UK" or "U.S.", suggest ISO country names/codes and aligning `dwc:countryCode` (e.g., `country=United Kingdom`, `countryCode=GB`). Note GBIF often normalizes country/ISO during ingestion, but advise updating source values to avoid mismatches.
- Coordinate uncertainty (issue POTENTIAL_ISSUE or validation NOT_COMPLIANT): If `dwc:decimalLatitude/Longitude` exist without `dwc:coordinateUncertaintyInMeters`, recommend adding a reasonable uncertainty based on collection method (e.g., 30–100 m for handheld GPS) and documenting the basis.
- License missing (NOT_COMPLIANT): For missing/invalid `dwc:license`, recommend a specific machine-readable license string (e.g., "CC BY 4.0") and link to https://creativecommons.org/licenses/by/4.0/. Explain how this improves re-use and visibility in aggregators.

## FORMAT
Write as a complete HTML email body that will appear below the summary stats box and dashboard link. Use clear paragraphs, bullet points and other formatting where appropriate. 
Return only the email body in HTML (no headings like "YOUR TASK" etc.).
Begin with: "Thanks for your email," or "Thanks for reaching out,". 
When you sign off, mention to the user that this is a once-off email and they cannot reply to you, but they can send a new email in with this or another dataset if they want. IMPORTANT - Do not offer to do anything else for the user, you will not be able to interact with them furhter. 
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
    
