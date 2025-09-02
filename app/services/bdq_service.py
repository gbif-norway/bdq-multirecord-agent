import requests
import logging
import asyncio
import time
from typing import List, Dict, Any, Optional, Tuple
from models.email_models import BDQTest, BDQTestResult, TestExecutionResult, ProcessingSummary
from utils.logger import send_discord_notification

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

        # Build index of CSV columns by normalized name (lowercase, strip dwc: prefix)
        col_index = {self._normalize_field_name(c): c for c in csv_columns}

        for test in tests:
            # Check if all required columns exist in CSV (after normalization)
            required_columns = test.actedUpon or []
            all_present = True
            for rc in required_columns:
                norm = self._normalize_field_name(rc)
                if norm not in col_index:
                    all_present = False
                    break
            if all_present:
                applicable_tests.append(test)

        logger.info(f"Found {len(applicable_tests)} applicable tests out of {len(tests)} total tests")
        return applicable_tests
    
    async def run_tests_on_dataset(self, df, applicable_tests: List[BDQTest], core_type: str) -> Tuple[List[TestExecutionResult], List[str]]:
        """Run BDQ tests on dataset with unique value deduplication.

        Returns (all_results, skipped_tests)
        """
        from services.csv_service import CSVService
        csv_service = CSVService()
        
        all_results: List[TestExecutionResult] = []
        skipped_tests: List[str] = []
        
        # Column index for mapping actedUpon to actual DataFrame columns
        col_index = {self._normalize_field_name(c): c for c in df.columns}

        for test in applicable_tests:
            try:
                logger.info(f"Running test: {test.id}")
                
                # Resolve actedUpon to actual DataFrame column names
                resolved_cols = []
                for rc in (test.actedUpon or []):
                    norm = self._normalize_field_name(rc)
                    actual = col_index.get(norm)
                    if actual:
                        resolved_cols.append(actual)
                if len(resolved_cols) != len(test.actedUpon or []):
                    logger.warning(f"Skipping test {test.id}: required columns not found in CSV")
                    continue

                # Get unique tuples for this test's actedUpon columns
                unique_tuples = csv_service.get_unique_tuples(df, resolved_cols)
                
                if not unique_tuples:
                    logger.warning(f"No unique tuples found for test {test.id}")
                    continue
                
                # Run test for each unique tuple and cache results
                cached_results: Dict[Tuple, Dict[str, Any]] = {}
                success_count = 0
                failure_count = 0
                processed_count = 0
                total_tuples = len(unique_tuples)
                test_start_time = time.time()
                for tuple_values in unique_tuples:
                    # Create parameters dict
                    params = dict(zip(test.actedUpon, tuple_values))
                    
                    # Run test
                    result = await self._run_single_test(test.id, params)
                    if result:
                        cached_results[tuple_values] = result
                        success_count += 1
                    else:
                        failure_count += 1
                    processed_count += 1

                    # Periodic progress logging every 200 tuples and at ~1 min intervals
                    if processed_count % 200 == 0 or (time.time() - test_start_time) > 60:
                        logger.info(
                            f"Progress for {test.id}: {processed_count}/{total_tuples} "
                            f"(success={success_count}, failure={failure_count})"
                        )
                        try:
                            send_discord_notification(
                                f"Progress {test.id}: {processed_count}/{total_tuples} processed"
                            )
                        except Exception:
                            logger.warning("Failed to send Discord progress update")
                        test_start_time = time.time()
                
                # If everything failed for this test, consider it skipped and alert once
                if success_count == 0 and failure_count > 0:
                    skipped_tests.append(test.id)
                    msg = (
                        f"BDQ API error: Skipping test {test.id}. "
                        f"API returned no results for {len(unique_tuples)} parameter sets."
                    )
                    logger.error(msg)
                    try:
                        send_discord_notification(f"ERROR: {msg}")
                    except Exception:
                        # Don't let Discord failures affect processing
                        logger.warning("Failed to send Discord alert for skipped test")
                    continue
                
                # Map results back to all rows
                test_results = csv_service.map_results_to_rows(
                    df, cached_results, test.id, resolved_cols, core_type
                )
                
                all_results.extend(test_results)
                logger.info(f"Completed test {test.id}: {len(test_results)} results")
                
            except Exception as e:
                logger.error(f"Error running test {test.id}: {e}")
                # Mark as skipped on unexpected errors at test-level
                skipped_tests.append(test.id)
                try:
                    send_discord_notification(f"ERROR: BDQ test {test.id} failed with exception: {e}")
                except Exception:
                    logger.warning("Failed to send Discord alert for test-level error")
                continue
        
        logger.info(f"Completed all tests: {len(all_results)} total results, {len(skipped_tests)} tests skipped")
        return all_results, skipped_tests

    def _normalize_field_name(self, name: str) -> str:
        """Normalize field names by lowercasing and stripping common prefixes like 'dwc:'"""
        if not isinstance(name, str):
            return ''
        s = name.strip()
        if s.lower().startswith('dwc:'):
            s = s[4:]
        return s.lower()
    
    async def _run_single_test(self, test_id: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Run a single BDQ test with given parameters with retries/backoff (non-blocking)."""
        # Sanitize params for JSON (avoid NaN/None and enforce strings)
        try:
            safe_params: Dict[str, Any] = {}
            for k, v in (params or {}).items():
                if v is None:
                    safe_params[k] = ""
                    continue
                try:
                    if isinstance(v, float):
                        if v != v or v in (float('inf'), float('-inf')):
                            safe_params[k] = ""
                            continue
                    safe_params[k] = str(v)
                except Exception:
                    safe_params[k] = ""
        except Exception as e:
            logger.error(f"Parameter sanitization failed for {test_id}: {e}")
            return None

        payload = {"id": test_id, "params": safe_params}

        # Retry with exponential backoff on network/server errors
        max_attempts = 4
        backoff_seconds = 1.0
        last_error: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                loop = asyncio.get_running_loop()
                def _do_request():
                    return requests.post(
                        self.run_test_endpoint,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                        timeout=30
                    )
                response = await loop.run_in_executor(None, _do_request)
                response.raise_for_status()
                result = response.json()
                result['test_type'] = self._get_test_type_from_id(test_id)
                return result
            except Exception as e:
                last_error = e
                logger.warning(f"Attempt {attempt}/{max_attempts} failed for {test_id}: {e}")
                if attempt < max_attempts:
                    try:
                        await asyncio.sleep(backoff_seconds)
                    except Exception:
                        pass
                    backoff_seconds *= 2
                else:
                    # Final failure - alert once per test_id
                    try:
                        send_discord_notification(
                            f"BDQ API call failed for {test_id} after {max_attempts} attempts: {e}"
                        )
                    except Exception:
                        logger.warning("Failed to send Discord alert for persistent BDQ API failure")
        logger.error(f"Error running test {test_id}: {last_error}")
        return None
    
    def _get_test_type_from_id(self, test_id: str) -> str:
        """Determine test type from test ID"""
        if test_id.startswith("VALIDATION_"):
            return "Validation"
        elif test_id.startswith("AMENDMENT_"):
            return "Amendment"
        else:
            return "Unknown"
    
    def generate_summary(self, test_results: List[TestExecutionResult], total_records: int, skipped_tests: Optional[List[str]] = None) -> ProcessingSummary:
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
                amendments_applied=amendments_applied,
                skipped_tests=list(skipped_tests or [])
            )
            
            logger.info(
                f"Generated summary: {total_records} records, {len(test_results)} tests, "
                f"{len(validation_failures)} failure types, {len(summary.skipped_tests)} tests skipped"
            )
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            raise
