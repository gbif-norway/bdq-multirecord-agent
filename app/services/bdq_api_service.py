import requests
import logging
import asyncio
import time
from typing import List, Dict, Any, Optional, Tuple
from utils.helper import log

from dataclasses import dataclass, field


@dataclass
class BDQTest:
    """Model for BDQ test definition"""
    id: str
    guid: str
    type: str  # "Validation" or "Amendment"
    className: str
    methodName: str
    actedUpon: List[str]
    consulted: List[str] = field(default_factory=list)
    parameters: List[Any] = field(default_factory=list)

@dataclass
class BDQTestExecutionResult:
    """Model for complete test execution result for a row"""
    record_id: str
    test_id: str
    status: str
    result: Optional[str] = None
    comment: Optional[str] = None
    amendment: Optional[Dict[str, Any]] = None
    test_type: str

class BDQAPIService:
    """Service for interacting with BDQ API"""
    
    def __init__(self):
        self.bdq_api_base = "https://bdq-api-638241344017.europe-west1.run.app"
        self.tests_endpoint = f"{self.bdq_api_base}/api/v1/tests"
        self.run_test_endpoint = f"{self.bdq_api_base}/api/v1/tests/run"
    
    def _filter_applicable_tests(self, csv_columns: List[str]) -> List[BDQTest]:
        """Filter tests that can be applied to the CSV columns"""
        all_tests_response = requests.get(self.tests_endpoint, timeout=30)
        all_tests_response.raise_for_status()
        all_tests = [BDQTest(**test) for test in all_tests_response.json()]
        
        applicable_tests = [
            test for test in all_tests
            if all(col in csv_columns for col in test.actedUpon)
        ]
        
        logger.info(f"Found {len(applicable_tests)} applicable tests out of {len(all_tests)} total tests")
        return applicable_tests
    
    async def run_tests_on_dataset(self, df, core_type: str) -> pd.DataFrame:
        """Run BDQ tests on dataset with unique value deduplication."""
        applicable_tests = self.filter_applicable_tests(df.columns.tolist())
        all_results_dfs: List[pd.DataFrame] = []

        for test in applicable_tests:
            try:
                logger.info(f"Running test: {test.id}")
                
                # Get a df which is a subset of the main df with the id column (determined by f'{core_type}ID') and actedUpon columns and consulted columns
                df_for_testing = 'addcode'
                # Get unique values in this df (drop f'{core_type}ID' column when you do this)
                unique_values_for_test_df = 'addcode'

                # Send entire test_df to a new batch endpoint /api/v1/tests/run/batch that accepts an array of { id, params }, with the test name as the id, like this e.g.:
                # [{ "id": "VALIDATION_COUNTRYCODE_VALID", "params": { "dwc:countryCode": "US" } }, { "id": "AMENDMENT_EVENTDATE_STANDARDIZED", "params": { "dwc:eventDate": "8 May 1880" } }
                # It returns a list of  in the same order as the input tests like this e.g.:
                # [{ "status": "RUN_HAS_RESULT", "result": "COMPLIANT", "comment": "..." }, { "status": "AMENDED", "result": "1880-05-08", "comment": "..." }]
                
                # Merge that back into the unique_values_for_test_df in an easy way, maybe turn that result list into a df with columns: status, result, comment and join based on index or something
                # Make sure that we keep NAs as empty strings, so if there's no comment for example (although i think there's always a comment), that should be '' in the df, not pd.None or whatever.
                
                # Map unique results back to df_for_testing and include the test id as a column VALIDATION_COUNTRYCODE_VALID (which will of course be the same for all rows)

                all_results.append(test_results)
                log(f"Completed test {test.id}: {len(test_results)} results")
                
        # Make an all_results_df and return it after this message
        log(f"Completed all tests: {len(all_results_df)} total results, with a total of {len(applicable_tests)} run")

    # def generate_summary(self, test_results: List[BDQTestExecutionResult], total_records: int, skipped_tests: Optional[List[str]] = None) -> ProcessingSummary:
    #     """Generate processing summary from test results"""
    #     try:
    #         # Count validation failures by field
    #         validation_failures = {}
    #         amendments_applied = 0
    #         common_issues = []
            
    #         # Track unique issues for common issues list
    #         issue_counts = {}
            
    #         for result in test_results:
    #             if result.result == "NOT_COMPLIANT":
    #                 # This is a validation failure
    #                 if result.test_id not in validation_failures:
    #                     validation_failures[result.test_id] = 0
    #                 validation_failures[result.test_id] += 1
                    
    #                 # Track common issues
    #                 if result.comment:
    #                     issue_key = f"{result.test_id}: {result.comment}"
    #                     issue_counts[issue_key] = issue_counts.get(issue_key, 0) + 1
                
    #             elif result.status == "AMENDED":
    #                 amendments_applied += 1
            
    #         # Get top 5 most common issues
    #         sorted_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)
    #         common_issues = [issue[0] for issue in sorted_issues[:5]]
            
    #         summary = ProcessingSummary(
    #             total_records=total_records,
    #             total_tests_run=len(test_results),
    #             validation_failures=validation_failures,
    #             common_issues=common_issues,
    #             amendments_applied=amendments_applied,
    #             skipped_tests=list(skipped_tests or [])
    #         )
            
    #         logger.info(
    #             f"Generated summary: {total_records} records, {len(test_results)} tests, "
    #             f"{len(validation_failures)} failure types, {len(summary.skipped_tests)} tests skipped"
    #         )
    #         return summary
            
    #     except Exception as e:
    #         logger.error(f"Error generating summary: {e}")
    #         raise