#!/usr/bin/env python3
"""
Real-world test of the LLM service Gemini summary generation.
This test uses the actual functions from main.py without mocking to generate
a real summary using the simple_occurrence_dwc.csv and _RESULTS.csv files.
"""

import os
import sys
import pandas as pd
from dotenv import load_dotenv

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from app.services.csv_service import CSVService
from app.services.llm_service import LLMService
from app.utils.helper import log, str_snapshot, get_relevant_test_contexts

def _get_summary_stats(test_results_df, coreID):
    """Generate summary statistics from test results DataFrame - copied from main.py"""
    # Extract unique field names from actedUpon and consulted columns
    # These columns contain formatted strings like "field1=value1|field2=value2"
    acted_upon_fields = set()
    consulted_fields = set()
    
    for acted_upon_str in test_results_df['actedUpon'].dropna():
        if acted_upon_str:  # Skip empty strings
            # Split by | and extract field names (before =)
            for pair in acted_upon_str.split('|'):
                if '=' in pair:
                    field_name = pair.split('=')[0]
                    acted_upon_fields.add(field_name)
    
    for consulted_str in test_results_df['consulted'].dropna():
        if consulted_str:  # Skip empty strings
            # Split by | and extract field names (before =)
            for pair in consulted_str.split('|'):
                if '=' in pair:
                    field_name = pair.split('=')[0]
                    consulted_fields.add(field_name)
    
    all_cols_tested = list(acted_upon_fields.union(consulted_fields))
    amendments = test_results_df[test_results_df['status'] == 'AMENDED']
    filled_in = test_results_df[test_results_df['status'] == 'FILLED_IN']
    issues = test_results_df[test_results_df['result'] == 'POTENTIAL_ISSUE']
    non_compliant_validations = test_results_df[test_results_df['result'] == 'NOT_COMPLIANT']

    def _get_top_grouped(df, group_cols, n=15):
        """Helper to get top n grouped counts sorted descending."""
        return (df.groupby(group_cols)
                .size()
                .reset_index(name='count')
                .sort_values('count', ascending=False)
                .head(n))

    summary = {
        'list_of_all_columns_tested': all_cols_tested,
        'no_of_tests_results': len(test_results_df),
        'no_of_tests_run': test_results_df['test_id'].nunique(),
        'no_of_non_compliant_validations': len(non_compliant_validations),
        'no_of_amendments': len(amendments),
        'no_of_filled_in': len(filled_in),
        'no_of_issues': len(issues),
        'top_issues': _get_top_grouped(issues, ['actedUpon', 'consulted', 'test_id']),
        'top_filled_in': _get_top_grouped(filled_in, ['actedUpon', 'consulted', 'test_id']),
        'top_amendments': _get_top_grouped(amendments, ['actedUpon', 'consulted', 'test_id']),
        'top_non_compliant_validations': _get_top_grouped(non_compliant_validations, ['actedUpon', 'consulted', 'test_id']),
    }

    log(f"Generated summary: {summary}")
    return summary

def main():
    """Run the real-world test of Gemini summary generation"""
    
    # Load environment variables from .env.test
    load_dotenv('.env.test')
    
    # Initialize services
    csv_service = CSVService()
    llm_service = LLMService()
    
    # Load test data
    test_data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    original_csv_path = os.path.join(test_data_dir, 'simple_occurrence_dwc.csv')
    results_csv_path = os.path.join(test_data_dir, 'simple_occurrence_dwc_RESULTS.csv')
    
    log(f"Loading original CSV from: {original_csv_path}")
    log(f"Loading results CSV from: {results_csv_path}")
    
    # Read the CSV files
    with open(original_csv_path, 'r') as f:
        original_csv_content = f.read()
    
    with open(results_csv_path, 'r') as f:
        results_csv_content = f.read()
    
    # Parse the original CSV to get DataFrame and core type
    df, core_type = csv_service.parse_csv_and_detect_core(original_csv_content)
    log(f"Parsed original CSV: {len(df)} rows, core type: {core_type}")
    
    # Parse the results CSV
    test_results = pd.read_csv(results_csv_path, dtype=str).fillna('')
    log(f"Parsed results CSV: {len(test_results)} rows")
    
    # Generate summary stats using the real function from main.py
    log("Generating summary statistics...")
    summary_stats = _get_summary_stats(test_results, core_type)
    
    # Create mock email data (simulating what would come from Gmail)
    email_data = {
        "from": "test@example.com",
        "subject": "BDQ Test Request",
        "body": "Please test my biodiversity dataset for data quality issues.",
        "attachments": []
    }
    
    # Get relevant test contexts
    log("Getting relevant test contexts...")
    test_ids = test_results['test_id'].unique().tolist()
    relevant_test_contexts = get_relevant_test_contexts(test_ids)
    
    # Create the prompt using the real LLM service
    log("Creating prompt...")
    prompt = llm_service.create_prompt(
        email_data, 
        core_type, 
        summary_stats, 
        str_snapshot(test_results), 
        str_snapshot(df), 
        relevant_test_contexts
    )
    
    # Convert DataFrames to CSV strings for LLM (following main.py exactly)
    log("Converting DataFrames to CSV strings...")
    test_results_csv_content = csv_service.dataframe_to_csv_string(test_results)
    original_csv_content = csv_service.dataframe_to_csv_string(df)
    
    # Generate the Gemini summary
    log("Generating Gemini summary...")
    try:
        llm_analysis = llm_service.generate_gemini_intelligent_summary(
            prompt, 
            test_results_csv_content, 
            original_csv_content
        )
        log("Successfully generated Gemini summary!")
        
        # Save the summary to the specified file
        output_path = os.path.join(test_data_dir, 'gemini_html_summary.html')
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(llm_analysis)
        
        log(f"Summary saved to: {output_path}")
        print(f"\n✅ SUCCESS: Gemini summary generated and saved to {output_path}")
        print(f"Summary length: {len(llm_analysis)} characters")
        
        # Also print a preview of the summary
        print(f"\n📄 Preview of generated summary:")
        print("=" * 80)
        print(llm_analysis[:500] + "..." if len(llm_analysis) > 500 else llm_analysis)
        print("=" * 80)
        
    except Exception as e:
        log(f"Error generating Gemini summary: {e}", "ERROR")
        print(f"❌ ERROR: Failed to generate Gemini summary: {e}")
        raise

if __name__ == "__main__":
    main()
