import requests
import logging
from typing import List, Dict, Any, Optional, Tuple
from models.email_models import BDQTest, BDQTestResult, TestExecutionResult, ProcessingSummary

logger = logging.getLogger(__name__)

class BDQService:
    """Service for interacting with BDQ API"""
    
    def __init__(self):
        self.bdq_api_base = "https://bdq-api-638241344017.europe-west1.run.app"
        self.tests_endpoint = f"{self.bdq_api_base}/api/v1/tests"
        self.run_test_endpoint = f"{self.bdq_api_base}/api/v1/tests/run"
    
    async def get_available_tests(self) -> List[BDQTest]:
        """Fetch available BDQ tests from API"""
        try:
            response = requests.get(self.tests_endpoint, timeout=30)
            response.raise_for_status()
            
            tests_data = response.json()
            tests = []
            
            for test_data in tests_data:
                test = BDQTest(
                    id=test_data.get('id', ''),
                    guid=test_data.get('guid', ''),
                    type=test_data.get('type', ''),
                    className=test_data.get('className', ''),
                    methodName=test_data.get('methodName', ''),
                    actedUpon=test_data.get('actedUpon', []),
                    consulted=test_data.get('consulted', []),
                    parameters=test_data.get('parameters', [])
                )
                tests.append(test)
            
            logger.info(f"Retrieved {len(tests)} BDQ tests from API")
            return tests
            
        except Exception as e:
            logger.error(f"Error fetching BDQ tests: {e}")
            raise
    
    def filter_applicable_tests(self, tests: List[BDQTest], csv_columns: List[str]) -> List[BDQTest]:
        """Filter tests that can be applied to the CSV columns"""
        applicable_tests = []
        
        for test in tests:
            # Check if all required columns exist in CSV
            required_columns = test.actedUpon
            if all(col in csv_columns for col in required_columns):
                applicable_tests.append(test)
        
        logger.info(f"Found {len(applicable_tests)} applicable tests out of {len(tests)} total tests")
        return applicable_tests
    
    async def run_tests_on_dataset(self, df, applicable_tests: List[BDQTest], core_type: str) -> List[TestExecutionResult]:
        """Run BDQ tests on dataset with unique value deduplication"""
        from services.csv_service import CSVService
        csv_service = CSVService()
        
        all_results = []
        
        for test in applicable_tests:
            try:
                logger.info(f"Running test: {test.id}")
                
                # Get unique tuples for this test's actedUpon columns
                unique_tuples = csv_service.get_unique_tuples(df, test.actedUpon)
                
                if not unique_tuples:
                    logger.warning(f"No unique tuples found for test {test.id}")
                    continue
                
                # Run test for each unique tuple and cache results
                cached_results = {}
                for tuple_values in unique_tuples:
                    # Create parameters dict
                    params = dict(zip(test.actedUpon, tuple_values))
                    
                    # Run test
                    result = await self._run_single_test(test.id, params)
                    if result:
                        cached_results[tuple_values] = result
                
                # Map results back to all rows
                test_results = csv_service.map_results_to_rows(
                    df, cached_results, test.id, test.actedUpon, core_type
                )
                
                all_results.extend(test_results)
                logger.info(f"Completed test {test.id}: {len(test_results)} results")
                
            except Exception as e:
                logger.error(f"Error running test {test.id}: {e}")
                continue
        
        logger.info(f"Completed all tests: {len(all_results)} total results")
        return all_results
    
    async def _run_single_test(self, test_id: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Run a single BDQ test with given parameters"""
        try:
            payload = {
                "id": test_id,
                "params": params
            }
            
            response = requests.post(
                self.run_test_endpoint,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            
            # Add test type information
            result['test_type'] = self._get_test_type_from_id(test_id)
            
            return result
            
        except Exception as e:
            logger.error(f"Error running test {test_id}: {e}")
            return None
    
    def _get_test_type_from_id(self, test_id: str) -> str:
        """Determine test type from test ID"""
        if test_id.startswith("VALIDATION_"):
            return "Validation"
        elif test_id.startswith("AMENDMENT_"):
            return "Amendment"
        else:
            return "Unknown"
    
    def generate_summary(self, test_results: List[TestExecutionResult], total_records: int) -> ProcessingSummary:
        """Generate processing summary from test results"""
        try:
            # Count validation failures by field
            validation_failures = {}
            amendments_applied = 0
            common_issues = []
            
            # Track unique issues for common issues list
            issue_counts = {}
            
            for result in test_results:
                if result.result == "NOT_COMPLIANT":
                    # This is a validation failure
                    if result.test_id not in validation_failures:
                        validation_failures[result.test_id] = 0
                    validation_failures[result.test_id] += 1
                    
                    # Track common issues
                    if result.comment:
                        issue_key = f"{result.test_id}: {result.comment}"
                        issue_counts[issue_key] = issue_counts.get(issue_key, 0) + 1
                
                elif result.status == "AMENDED":
                    amendments_applied += 1
            
            # Get top 5 most common issues
            sorted_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)
            common_issues = [issue[0] for issue in sorted_issues[:5]]
            
            summary = ProcessingSummary(
                total_records=total_records,
                total_tests_run=len(test_results),
                validation_failures=validation_failures,
                common_issues=common_issues,
                amendments_applied=amendments_applied
            )
            
            logger.info(f"Generated summary: {total_records} records, {len(test_results)} tests, {len(validation_failures)} failure types")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            raise
