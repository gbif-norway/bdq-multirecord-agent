import requests
import logging
import asyncio
import time
import pandas as pd
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
        self.tests_endpoint = f"{self.bdq_api_base}/api/v1/tests"  # tests_endpoint returns an of dicts that look like BDQTest
        self.batch_endpoint = f"{self.bdq_api_base}/api/v1/tests/run/batch""
        # batch_endpoint accepts an array of { id, params }, with the test name as the id, like this e.g.:
        # [{ "id": "VALIDATION_COUNTRYCODE_VALID", "params": { "dwc:countryCode": "US" } }, { "id": "AMENDMENT_EVENTDATE_STANDARDIZED", "params": { "dwc:eventDate": "8 May 1880" } }
        # It returns a list of  in the same order as the input tests like this e.g.:
        # [{ "status": "RUN_HAS_RESULT", "result": "COMPLIANT", "comment": "..." }, { "status": "AMENDED", "result": "1880-05-08", "comment": "..." }]
    
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
                
                # Get a df which is a subset of the main df with the id column (determined by f'{core_type}ID') and actedUpon columns and consulted columns
                id_column = f'{core_type}ID'
                relevant_columns = [id_column] + test.actedUpon + test.consulted
                df_for_testing = df[relevant_columns].copy()
                
                # Get unique values in this df (drop f'{core_type}ID' column when you do this)
                unique_values_for_test_df = df_for_testing.drop(columns=[id_column]).drop_duplicates().reset_index(drop=True)

                # Prepare batch request
                batch_requests = []
                for _, row in unique_values_for_test_df.iterrows():
                    params = {col: row[col] for col in test.actedUpon + test.consulted}
                    batch_requests.append({
                        "id": test.id,
                        "params": params
                    })
                
                # Call batch endpoint
                batch_response = requests.post(self.batch_endpoint, json=batch_requests, timeout=60)
                batch_response.raise_for_status()
                batch_results = batch_response.json()
                
                # Create results dataframe
                results_df = pd.DataFrame(batch_results)
                results_df = results_df.fillna('')  # Replace NAs with empty strings
                
                # Combine unique values with results
                unique_with_results = pd.concat([unique_values_for_test_df, results_df], axis=1)
                
                # Merge back to original dataframe with extra necessary columns
                test_results_df = df_for_testing.merge(
                    unique_with_results, 
                    on=test.actedUpon + test.consulted, 
                    how='left'
                )
                test_results_df['test_id'] = test.id
                test_results_df['test_type'] = test.type
                
                all_results_dfs.append(test_results_df)
                log(f"Completed test {test.id}: {len(test_results_df)} results")
                
            except Exception as e:
                log(f"Error running test {test.id}: {str(e)}")
                continue
                
        # Make an all_results_df and return it after this message
        if all_results_dfs:
            all_results_df = pd.concat(all_results_dfs, ignore_index=True)
            log(f"Completed all tests: {len(all_results_df)} total results, with a total of {len(applicable_tests)} tests run")
            return all_results_df
        else:
            log("No tests were successfully completed", "ERROR")
            return pd.DataFrame()
