#!/usr/bin/env python3
"""
Script to run BDQ API tests on simple_occurrence_dwc.csv and save results to simple_occurrence_dwc_RESULTS.csv.

This script:
1. Loads the simple occurrence CSV data
2. Runs BDQ API tests on the data using BDQAPIService
3. Saves the results to a CSV file in the tests/data directory
"""

import asyncio
import pandas as pd
import sys
import os
from pathlib import Path

# Add the app directory to the Python path so we can import our services
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from services.bdq_api_service import BDQAPIService
from utils.helper import log


async def main():
    """Main function to run BDQ tests on simple occurrence data."""
    
    # Define file paths
    script_dir = Path(__file__).parent
    input_file = script_dir / "tests" / "data" / "simple_occurrence_dwc.csv"
    output_file = script_dir / "tests" / "data" / "simple_occurrence_dwc_RESULTS.csv"
    
    log(f"Starting BDQ API tests on simple occurrence data...")
    log(f"Input file: {input_file}")
    log(f"Output file: {output_file}")
    
    # Check if input file exists
    if not input_file.exists():
        log(f"Error: Input file {input_file} does not exist!", "ERROR")
        return 1
    
    try:
        # Load the CSV data - treat all data as strings to avoid float conversions
        log("Loading CSV data...")
        df = pd.read_csv(input_file, dtype=str)
        log(f"Loaded {len(df)} records from {input_file}")
        log(f"Columns: {list(df.columns)}")
        
        # Initialize BDQ API service
        log("Initializing BDQ API service...")
        bdq_service = BDQAPIService()
        
        # Run tests on the dataset
        log("Running BDQ API tests...")
        results_df = await bdq_service.run_tests_on_dataset(df, core_type="occurrence")
        
        if results_df.empty:
            log("No test results returned from BDQ API", "ERROR")
            return 1
        
        log(f"Received {len(results_df)} test results")
        log(f"Result columns: {list(results_df.columns)}")
        
        # Save results to CSV file
        log(f"Saving results to {output_file}...")
        results_df.to_csv(output_file, index=False)
        
        log(f"Successfully saved {len(results_df)} test results to {output_file}")
        
        # Print summary statistics
        log("\n=== Test Results Summary ===")
        log(f"Total test results: {len(results_df)}")
        log(f"Unique occurrence IDs tested: {results_df['dwc:occurrenceID'].nunique()}")
        log(f"Unique tests run: {results_df['test_id'].nunique()}")
        
        # Show test type breakdown
        test_type_counts = results_df['test_type'].value_counts()
        log(f"Test types: {dict(test_type_counts)}")
        
        # Show status breakdown
        status_counts = results_df['status'].value_counts()
        log(f"Test statuses: {dict(status_counts)}")
        
        # Show result breakdown
        result_counts = results_df['result'].value_counts()
        log(f"Test results: {dict(result_counts)}")
        
        # Show sample results
        log("\n=== Sample Results ===")
        sample_results = results_df.head(5)
        for _, row in sample_results.iterrows():
            log(f"  {row['dwc:occurrenceID']} | {row['test_id']} | {row['status']} | {row['result']}")
        
        return 0
        
    except Exception as e:
        log(f"Error running BDQ tests: {str(e)}", "ERROR")
        import traceback
        log(f"Traceback: {traceback.format_exc()}", "ERROR")
        return 1


if __name__ == "__main__":
    # Run the async main function
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
