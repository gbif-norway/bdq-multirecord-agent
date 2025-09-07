import requests
import logging
import asyncio
import time
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from app.utils.helper import log

from dataclasses import dataclass, field
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import logging as _logging
import httpx


# HTTP status codes that are considered transient and safe to retry
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


# Add retry with a visible log before each sleep so Cloud logs show retries
@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError)),
    before_sleep=before_sleep_log(_logging.getLogger(__name__), _logging.WARNING),
)
def _post_with_retry(url: str, payload: Any, timeout: int) -> requests.Response:
    """POST with bounded retries for transient errors/timeouts.

    Retries on timeouts, connection errors, and retryable HTTP status codes.
    Keeps attempts low to avoid masking full API outages.
    """
    resp = requests.post(url, json=payload, timeout=timeout)
    if resp.status_code in _RETRYABLE_STATUS:
        # Raise HTTPError to trigger retry
        raise requests.exceptions.HTTPError(f"Retryable status: {resp.status_code}")
    resp.raise_for_status()
    return resp


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((httpx.ReadTimeout, httpx.ConnectError, httpx.HTTPStatusError)),
    before_sleep=before_sleep_log(_logging.getLogger(__name__), _logging.WARNING),
)
async def _apost_with_retry(client: httpx.AsyncClient, url: str, payload: Any) -> httpx.Response:
    resp = await client.post(url, json=payload)
    if resp.status_code in _RETRYABLE_STATUS:
        raise httpx.HTTPStatusError(f"Retryable status: {resp.status_code}", request=resp.request, response=resp)
    resp.raise_for_status()
    return resp


async def _bulk_with_watchdog_and_heartbeat(
    url: str,
    payload: Any,
    per_attempt_timeout: int,
    total_deadline_sec: int,
    heartbeat_interval_sec: int,
    test_id: str,
) -> httpx.Response:
    start = time.time()
    timeout = httpx.Timeout(per_attempt_timeout)
    async with httpx.AsyncClient(timeout=timeout) as client:
        task = asyncio.create_task(_apost_with_retry(client, url, payload))
        while True:
            try:
                # Wait in heartbeat intervals without cancelling the underlying task
                return await asyncio.wait_for(asyncio.shield(task), timeout=heartbeat_interval_sec)
            except asyncio.TimeoutError:
                elapsed = int(time.time() - start)
                log(f"{test_id}: BULK still running, elapsed={elapsed}s")
                if elapsed >= total_deadline_sec:
                    task.cancel()
                    raise asyncio.TimeoutError(
                        f"Bulk total deadline exceeded after {elapsed}s for {test_id}"
                    )


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
        # Chunking configuration for batch requests. Keep requests bounded to avoid long API hangs/timeouts.
        self.batch_chunk_size = 3000
        # Per-chunk timeout in seconds. Keep comfortably under Cloud Run’s max request timeout.
        self.batch_chunk_timeout_sec = 1800
        # Try a single bulk request first for speed; fallback to chunking if it fails.
        self.initial_bulk_timeout_sec = 300
        # Overall bulk watchdog deadline (end-to-end) and heartbeat interval (seconds)
        self.initial_bulk_total_deadline_sec = 420
        self.bulk_heartbeat_interval_sec = 60
    
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
                # First try a single bulk request for speed
                total_candidates = len(unique_test_candidates)
                if total_candidates == 0:
                    log(f"Skipping test {test.id}: no candidates")
                    continue

                bulk_attempted = False
                try:
                    log(f"{test.id}: attempting BULK request with {total_candidates} candidates (timeout {self.initial_bulk_timeout_sec}s)")
                    bulk_attempted = True
                    api_start_time = time.time()
                    bulk_request = [
                        {"id": test.id, "params": row}
                        for row in unique_test_candidates.to_dict(orient="records")
                    ]
                    bulk_response = await _bulk_with_watchdog_and_heartbeat(
                        url=self.batch_endpoint,
                        payload=bulk_request,
                        per_attempt_timeout=self.initial_bulk_timeout_sec,
                        total_deadline_sec=self.initial_bulk_total_deadline_sec,
                        heartbeat_interval_sec=self.bulk_heartbeat_interval_sec,
                        test_id=test.id,
                    )
                    bulk_results = bulk_response.json()
                    api_duration = time.time() - api_start_time

                    bulk_results_df = pd.DataFrame(bulk_results).fillna("")
                    bulk_results_df['test_id'] = test.id
                    bulk_results_df['test_type'] = test.type

                    unique_with_results = pd.concat([unique_test_candidates, bulk_results_df], axis=1)
                    expanded_results = df.merge(unique_with_results, on=test_columns, how='left')

                    id_column = f'dwc:{core_type}ID'
                    final_results = expanded_results[[id_column, 'test_id', 'test_type', 'status', 'result', 'comment']]
                    all_results_dfs.append(final_results)
                    log(f"Completed test {test.id}: {len(final_results)} results (bulk API {api_duration:.2f}s)")
                    continue  # next test
                except Exception as e:
                    if bulk_attempted:
                        log(f"{test.id}: BULK request failed, falling back to chunking: {str(e)}", "WARNING")

                # Process in chunks to avoid long single requests/timeouts
                chunked_unique_results: List[pd.DataFrame] = []
                processed = 0
                chunk_size = self.batch_chunk_size
                min_chunk_size = 100
                start_idx = 0
                chunk_index = 0
                consecutive_failures = 0

                est_chunks = (total_candidates + chunk_size - 1) // chunk_size
                log(
                    f"{test.id}: starting CHUNKED processing of {total_candidates} candidates in ~{est_chunks} chunks (chunk_size={chunk_size}, timeout={self.batch_chunk_timeout_sec}s)"
                )

                while start_idx < total_candidates:
                    end_idx = min(start_idx + chunk_size, total_candidates)
                    chunk_df = unique_test_candidates.iloc[start_idx:end_idx].reset_index(drop=True)
                    chunk_request = [
                        {"id": test.id, "params": row}
                        for row in chunk_df.to_dict(orient="records")
                    ]
                    chunk_index += 1

                    try:
                        log(f"{test.id}: chunk {chunk_index} START {start_idx}-{end_idx} (size={len(chunk_df)})")
                        api_start_time = time.time()
                        batch_response = _post_with_retry(
                            self.batch_endpoint,
                            payload=chunk_request,
                            timeout=self.batch_chunk_timeout_sec,
                        )
                        batch_results = batch_response.json()
                        api_duration = time.time() - api_start_time
                    except Exception as e:
                        consecutive_failures += 1
                        log(
                            f"ERROR: Failed chunk {chunk_index} for {test.id} ({start_idx}-{end_idx} of {total_candidates}): {str(e)}",
                            "ERROR",
                        )
                        # Adaptive tweak: reduce future chunk size on failure
                        if chunk_size > min_chunk_size:
                            new_size = max(chunk_size // 2, min_chunk_size)
                            if new_size < chunk_size:
                                chunk_size = new_size
                                log(f"{test.id}: reducing chunk size to {chunk_size} due to failures")
                            # Retry this window with smaller chunk size (don't advance start_idx)
                            continue

                        # At minimum size and still failing
                        if start_idx == 0 and consecutive_failures >= 2:
                            # Likely broader outage; abort this test early
                            log(f"{test.id}: repeated failures at start; aborting test (possible API outage)", "ERROR")
                            break

                        if consecutive_failures >= 2:
                            # Skip this problematic window to avoid stalling entire run
                            log(f"{test.id}: skipping window {start_idx}-{end_idx} after repeated failures", "ERROR")
                            start_idx = end_idx
                            continue

                        # Single failure at min size: try next loop iteration (retry same window once more due to tenacity already used)
                        continue

                    # Success path resets failure counter
                    consecutive_failures = 0

                    # Create results df for this chunk and tag with test info
                    chunk_results_df = pd.DataFrame(batch_results).fillna("")
                    chunk_results_df['test_id'] = test.id
                    chunk_results_df['test_type'] = test.type

                    # Combine this chunk's unique test data with results (aligns by order)
                    unique_with_results_chunk = pd.concat([chunk_df, chunk_results_df], axis=1)
                    chunked_unique_results.append(unique_with_results_chunk)

                    processed = end_idx
                    log(f"{test.id}: chunk {chunk_index} DONE {processed}/{total_candidates} (API {api_duration:.2f}s)")
                    start_idx = end_idx

                if not chunked_unique_results:
                    log(f"ERROR: No successful chunks for {test.id}", "ERROR")
                    continue

                # Merge all chunk results and expand back to original df
                unique_with_results_all = pd.concat(chunked_unique_results, ignore_index=True)
                expanded_results = df.merge(unique_with_results_all, on=test_columns, how='left')

                # Select only the required columns: occurrenceID/taxonID, test_id, test_type, status, result, comment
                id_column = f'dwc:{core_type}ID'
                final_results = expanded_results[[id_column, 'test_id', 'test_type', 'status', 'result', 'comment']]

                all_results_dfs.append(final_results)
                log(f"Completed test {test.id}: {len(final_results)} results (processed {total_candidates} unique candidates)")

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
