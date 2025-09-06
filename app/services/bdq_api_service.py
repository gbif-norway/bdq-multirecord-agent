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
        self.batch_endpoint = f"{self.bdq_api_base}/api/v1/tests/run/batch"
        # batch_endpoint accepts an array of { id, params }, with the test name as the id, like this e.g.:
        # [{ "id": "VALIDATION_COUNTRYCODE_VALID", "params": { "dwc:countryCode": "US" } }, { "id": "AMENDMENT_EVENTDATE_STANDARDIZED", "params": { "dwc:eventDate": "8 May 1880" } }
        # It returns a list of  in the same order as the input tests like this e.g.:
        # [{ "status": "RUN_HAS_RESULT", "result": "COMPLIANT", "comment": "..." }, { "status": "AMENDED", "result": "dwc:eventDate=1880-05-08", "comment": "..." }, { "status": "NOT_AMENDED", "result": "", "comment": "..." }, { "status": "AMENDED", "result": "dwc:decimalLatitude="-25.46"|dwc:decimalLongitude="135.87"", "comment": "..." }]
        # - Single-field amendment item:
        #     - result: dwc:eventDate=1880-05-08
        # - Multi-field amendment item:
        #     - result: dwc:minimumDepthInMeters=3.048 | dwc:maximumDepthInMeters=3.048
        # - Validation item:
        #     - result: COMPLIANT (unchanged; still the label from the value)
        # - Amendment test that didn't make changes:
        #     - status: NOT_AMENDED
        #     - result: ""
        #     - comment: explanation of why no amendment was needed
        # - Failed item:
        #     - status: INTERNAL_PREREQUISITES_NOT_MET
        #     - result: ""
        #     - comment: error message (e.g., "Unknown test id or guid: …")
    
    def _filter_applicable_tests(self, csv_columns: List[str]) -> List[BDQTest]:
        """Filter tests that can be applied to the CSV columns"""
        all_tests_response = requests.get(self.tests_endpoint, timeout=30)
        all_tests_response.raise_for_status()
        all_tests = [BDQTest(**test) for test in all_tests_response.json()]
        
        applicable_tests = [
            test for test in all_tests
            if all(col in csv_columns for col in test.actedUpon)
        ]
        
        log(f"Found {len(applicable_tests)} applicable tests out of {len(all_tests)} total tests")
        return applicable_tests
    
    async def run_tests_on_dataset(self, df, core_type):
        """Run BDQ tests on dataset with unique value deduplication."""
        applicable_tests = self._filter_applicable_tests(df.columns.tolist())
        all_results_dfs: List[pd.DataFrame] = []

        for test in applicable_tests:
            try:
                log(f"Running test: {test.id}")

                # Get a df which is a subset of the main df with unique items for testing for this particular test (e.g. just countryCode, or decimalLatitude and decimalLongitude and countryCode)
                test_columns = test.actedUpon + test.consulted 
                unique_test_candidates = df[test_columns].drop_duplicates().reset_index(drop=True)
                
                # Prepare batch request for unique combinations
                # Ensure all values are JSON-serializable by converting to strings
                unique_test_candidates_batch_request = []
                for _, row in unique_test_candidates.iterrows():
                    params = {}
                    for key, value in row.to_dict().items():
                        # Convert problematic float values to strings to avoid JSON serialization errors
                        if pd.isna(value):
                            params[key] = ""
                        elif isinstance(value, float) and (np.isinf(value) or np.isnan(value)):
                            params[key] = ""
                        else:
                            params[key] = str(value)
                    unique_test_candidates_batch_request.append({"id": test.id, "params": params})

                # Call batch endpoint
                log("Calling batch endpoint with ")
                batch_response = requests.post(self.batch_endpoint, json=unique_test_candidates_batch_request, timeout=60)
                batch_response.raise_for_status()
                batch_results = batch_response.json()

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
                log(f"Completed test {test.id}: {len(final_results)} results")
                
            except Exception as e:
                log(f"Error running test {test.id}: {str(e)}")
                continue
                
        # Combine all test results
        if all_results_dfs:
            all_results_df = pd.concat(all_results_dfs, ignore_index=True)
            log(f"Completed all tests: {len(all_results_df)} total results, with a total of {len(applicable_tests)} tests run")
            return all_results_df
        else:
            log("No tests were successfully completed", "ERROR")
            return pd.DataFrame()
