import requests
import logging
import asyncio
import time
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from app.utils.helper import log

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


class BDQAPIService:
    """Service for interacting with BDQ API"""
    
    def __init__(self):
        self.bdq_api_base = "https://bdq-api-638241344017.europe-west1.run.app"
        self.tests_endpoint = f"{self.bdq_api_base}/api/v1/tests"  # tests_endpoint returns an of dicts that look like BDQTest
        self.batch_endpoint = f"{self.bdq_api_base}/api/v1/tests/run/batch"  # see readme
    
    def _filter_applicable_tests(self, csv_columns: List[str]) -> List[BDQTest]:
        """Filter tests that can be applied to the CSV columns"""
        all_tests_response = requests.get(self.tests_endpoint, timeout=30)
        all_tests_response.raise_for_status()
        all_tests = [BDQTest(**test) for test in all_tests_response.json()]
        
        applicable_tests = [
            test for test in all_tests
            if (all(col in csv_columns for col in test.actedUpon) and
                all(col in csv_columns for col in test.consulted))
        ]
        
        log(f"Found {len(applicable_tests)} applicable tests out of {len(all_tests)} total tests")
        return applicable_tests
    
    async def run_tests_on_dataset(self, df, core_type):
        """Run BDQ tests on dataset with unique value deduplication."""
        start_time = time.time()
        applicable_tests = self._filter_applicable_tests(df.columns.tolist())
        all_results_dfs: List[pd.DataFrame] = []

        def _shorten_test_id(test_id):
            return test_id.replace("VALIDATION_", "V").replace("AMENDMENT_", "A")

        log(f"Running {len(applicable_tests)} tests: [{', '.join(_shorten_test_id(test.id) for test in applicable_tests)}]")

        for test in applicable_tests:
            try:
                # Get a df which is a subset of the main df with unique items for testing for this particular test (e.g. just countryCode, or decimalLatitude and decimalLongitude and countryCode)
                test_columns = test.actedUpon + test.consulted
                unique_test_candidates = (
                    df[test_columns]
                    .drop_duplicates()
                    .reset_index(drop=True)
                    .replace([np.nan, np.inf, -np.inf], "")
                    .astype(str)
                )
                unique_test_candidates_batch_request = [
                    {"id": test.id, "params": row}
                    for row in unique_test_candidates.to_dict(orient="records")
                ]
                
                # Call batch endpoint
                try:
                    api_start_time = time.time()
                    batch_response = requests.post(self.batch_endpoint, json=unique_test_candidates_batch_request, timeout=1800)
                    batch_response.raise_for_status()
                    batch_results = batch_response.json()
                    api_duration = time.time() - api_start_time
                except Exception as e:
                    log(f"ERROR: Failed to run test {test.id}: {str(e)}", "ERROR")
                    continue

                # Create results df for unique combinations
                unique_results_df = pd.DataFrame(batch_results).fillna("")
                unique_results_df['test_id'] = test.id
                unique_results_df['test_type'] = test.type
                
                # Combine unique test data with results
                unique_with_results = pd.concat([unique_test_candidates, unique_results_df], axis=1)
                
                # Merge results back to original dataframe to get one row per test per original row
                expanded_results = df.merge(
                    unique_with_results, 
                    on=test_columns, 
                    how='left'
                )
                
                # Select only the required columns: occurrenceID/taxonID, test_id, test_type, status, result, comment
                id_column = f'dwc:{core_type}ID'
                final_results = expanded_results[[id_column, 'test_id', 'test_type', 'status', 'result', 'comment']]
                
                all_results_dfs.append(final_results)
                log(f"Completed test {test.id}: {len(final_results)} results (API call took {api_duration:.2f}s)")

            except Exception as e:
                log(f"Error running test {test.id}: {str(e)}", "ERROR")
                continue
                
        # Combine all test results
        total_duration = time.time() - start_time
        if all_results_dfs:
            all_results_df = pd.concat(all_results_dfs, ignore_index=True)
            log(f"Completed all tests: {len(all_results_df)} total results, with a total of {len(applicable_tests)} tests run (total time: {total_duration:.2f}s)")
            return all_results_df
        else:
            log(f"No tests were successfully completed (total time: {total_duration:.2f}s)", "ERROR")
            return pd.DataFrame()
