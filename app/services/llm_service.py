import os
import io
import pandas as pd
import google.generativeai as genai
from typing import List, Dict, Any, Optional
from app.utils.helper import log

class LLMService:
    """Service for generating intelligent summaries using Google Gemini"""
    
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY")
        genai.configure(api_key=self.api_key)
        self.bdq_tests_df = self._load_bdq_tests()
    
    def _load_bdq_tests(self) -> pd.DataFrame:
        """Load BDQ tests information from CSV file"""
        csv_path = os.path.join(os.path.dirname(__file__), '..', 'TG2_tests.csv')
        df = pd.read_csv(csv_path, dtype=str).fillna('')  # Load all as strings to avoid float conversions
        log(f"Loaded {len(df)} BDQ tests from CSV")
        return df
    
    def _get_relevant_test_contexts(self, test_ids: List[str]) -> str:
        """Get relevant BDQ test contexts for the given test IDs"""
        relevant_tests = self.bdq_tests_df[self.bdq_tests_df['Label'].isin(test_ids)]
        columns_needed = ['Label', 'InformationElement:ActedUpon', 'InformationElement:Consulted', 'ExpectedResponse', 'Description', 'Examples', 'Notes', 'IE Class', 'UseCases']
        tests = relevant_tests[columns_needed].copy()
        tests = tests.rename(columns={'IE Class': 'Information Element Class'})
        return f"\n## BDQ TEST CONTEXT\nThe following tests were run on the dataset:\n\n{str(tests)}"
    
    def generate_intelligent_summary(self, email_content, core_type, summary_stats):
        """Generate intelligent summary from DataFrame-based test results"""
        log("Generating the prompt for gemini...") 
        prompt = self._create_prompt(test_results_df, email_content, core_type, summary_stats, unique_with_results_df)
        log("Prompt generated!")
        log(prompt)

        # Call Gemini API
        client = genai.Client()
        response = client.models.generate_content(
            model="gemini-2.5-pro",
            contents=prompt,
            config=types.GenerateContentConfig(tools=[types.Tool(code_execution=types.ToolCodeExecution)]),
        )
        response = self.model.generate_content(prompt)
        
        if response.text:
            log(response.text[:1000])
            return self._convert_to_html(response.text.strip())
        else:
            log("No response text from Gemini API", "ERROR")
            return "<p>Unable to generate summary at this time.</p>"
                
    def _create_prompt(self, email_content, core_type, summary_stats):
        prompt += f"""# YOUR TASK
Write a professional, encouraging email analysis. 

## TONE & STYLE
- Friendly, helpful and pragmatic
- Speak informally and naturally. Don't use adjectives, adverbs, verbose or flowery language, state things clearly with minimal jargon. For example, instead of "utilize", say "use". Try to restrict your vocabulary to the top 2000 words used in the English language. Don't use the word "crucial" or other commonly used tell-tale words used by LLMs. Sound as human and natural as possible. 
- Use language a biologist would understand. Note that the biologists using this service are probably not familiar with the ins-and-outs of what the BDQ tests actually check and do and need things explained in a clear and practical way
- Focus on analysis and practical guidance, not repeating the summary stats
- Be encouraging about data quality improvement
- Keep it concise but comprehensive

## YOUR RESOURCES

USER'S ORIGINAL FILE ({core_type} dataset type):
- URL to full file: {original_url}
- Snapshot: 
{original_snapshot}

USER'S TEST RESULTS FILE: 
- URL to full file: {test_results_url}
- Snapshot: 
{test_results_snapshot}

### BDQ TESTS CONTEXT
{self._get_relevant_test_contexts(test_results_df['test_id'].unique().tolist())} 

### USER'S EMAIL CONTENT
{email_content}

### SUMMARY STATISTICS
{summary_stats}

## CONTENT

1. **Acknowledge** the submission and thank the user for using BDQ. Reply to any queries in the original email. 

2. **Analyze** what the test results mean using the BDQ TESTS CONTEXT their email content if there is any (focus on interpretation, not repeating numbers)
Content headings can be generated on a case-by-case basis, but 
- Your Data At A Glance: DATA OVERVIEW provided and provide a summary that makes sense given the context. Examples: Coverage: records by year, by country/region, by major taxon groups. Keep it short and just use it to give a flavour of the data.
- What We Found, use Information Element Class to group the tests by theme (i.e. location, dates, taxonomy, etc.). Explain what was checked in each theme area. 
- Quick wins section
  Suggested Fixes

  - Quick wins (safe to accept automatically)
      - Standardize obvious things: country codes, month/day formats, common abbreviations.
      - Fill in blanks from other fields when unambiguous (e.g., fill coordinates from verbatim text).
  - Needs human review
      - Ambiguous dates (e.g., dd/mm vs mm/dd), coordinates near borders, names matching multiple taxa.
  - Provider/dataset actions
      - Top 3 datasets to contact and the simplest fixes they can make at source.
  - Prioritized checklist
      -
          1. Accept auto‑fixes; 2) Review “high‑impact” ambiguities; 3) Contact providers; 4) Re‑run tests.

  Impact If Fixes Are Applied

  - Expected lift: “usable now” records goes from X% → Y%.
  - By theme: +A% valid dates; +B% valid locations; +C% names recognized.
  - Where it helps: better maps, cleaner time series, more reliable species counts.

  How To Read “Amended” vs “Filled In”

  - AMENDED = a suggested correction to a value that’s already there (e.g., “Australia” → “AU”).
  - FILLED IN = a suggested value for a blank field (e.g., derive coordinates from verbatim text).
  - NOT_AMENDED = no safe change suggested (often because it’s ambiguous).

  Deliverables You Can Use

  - Summary tables (PDF/HTML):
      - Overall readiness; top issues; quick wins; provider/dataset rankings.
  - Action spreadsheets (CSV):
      - “Auto‑fix suggestions” (ready to accept).
      - “Needs review” (ambiguous/edge cases).
      - “Provider follow‑up” (issues grouped by dataset).
  - Visuals:
      - Map of suspect points; bar charts of missing fields; trend of usable dates by year.

  Method (Short Note)

  - Tests used standard checks for location, time, taxonomy, and licensing.
  - External lookups (e.g., vocabularies, gazetteers) were used when needed.
  - Some checks can’t decide automatically; those are flagged for human review.


2. **Analyzes** what the test results mean using the BDQ TESTS CONTEXT provided below and their email content if there is any (focus on interpretation, not repeating numbers)
3. **Explains** the significance of any issues found and their impact on data quality, using the BDQ TESTS CONTEXT to provide specific insights about what each test checks
4. **Provides** specific, actionable advice for improving their data quality based on the BDQ TESTS CONTEXTS and their top/most common results in each category in their summary statistics. Go into “quick wins” - easy corrections with high impact - and the most critical issues likely to affect dataset reuse.
5. **Describes** what the attached files contain and how to use them effectively
6. **Encourages** them to resubmit after making improvements



## FORMAT
Write as a complete email body that will appear below the summary stats box. Use clear paragraphs and bullet points where appropriate. Do NOT include the summary statistics - they are already displayed above."""

        return prompt
    
    
    def _convert_to_html(self, text: str) -> str:
        """Convert plain text to basic HTML"""
        # Simple conversion - replace newlines with <br> and wrap in <p> tags
        html = text.replace('\n\n', '</p><p>').replace('\n', '<br>')
        return f"<p>{html}</p>"
    