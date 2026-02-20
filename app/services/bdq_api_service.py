import os
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
    """Service for interacting with BDQ API."""
    
    def __init__(self):
        bdq_port = os.getenv("BDQ_API_PORT", "8081")
        self.bdq_api_base = f"http://localhost:{bdq_port}"
        self.tests_endpoint = f"{self.bdq_api_base}/api/v1/tests"
        self.batch_endpoint = f"{self.bdq_api_base}/api/v1/tests/run/batch"
        self.failed_tests: List[str] = []
    
    def _post_batch(self, payload: List[Dict[str, Any]], max_retries: int = 3) -> Tuple[bool, Optional[List[Dict[str, Any]]]]:
        """Post the payload to local BDQ API service with retry logic.

        Returns (success, results) where results is a list of dicts aligned to input order
        when success is True; otherwise (False, None).
        """
        import time
        
        for attempt in range(max_retries):
            try:
                # Local calls don't need timeout handling or chunking - the Java service handles concurrency
                resp = requests.post(self.batch_endpoint, json=payload, timeout=3600)  # 1 hour max for very large batches
                resp.raise_for_status()
                results = resp.json()
                if len(results) != len(payload):
                    log(
                        f"BDQ batch mismatch: expected {len(payload)} results, got {len(results)}",
                        "ERROR",
                    )
                    return False, None
                return True, results
            except (requests.exceptions.ConnectionError, requests.exceptions.SSLError) as e:
                # BDQ API might not be ready yet (cold start)
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                if attempt < max_retries - 1:
                    log(
                        f"BDQ API not ready (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}",
                        "WARNING",
                    )
                    time.sleep(wait_time)
                    continue
                log(
                    f"BDQ batch request failed after {max_retries} attempts: {type(e).__name__}: {e}",
                    "ERROR",
                )
                return False, None
            except requests.exceptions.RequestException as e:
                log(
                    f"BDQ batch request failed: {type(e).__name__}: {e}",
                    "ERROR",
                )
                return False, None
            except Exception as e:
                log(
                    f"BDQ batch failed: {type(e).__name__}: {e}",
                    "ERROR",
                )
                return False, None
        
        return False, None
    
    def _filter_applicable_tests(self, csv_columns: List[str], max_retries: int = 3) -> List[BDQTest]:
        """Filter tests that can be applied to the CSV columns"""
        import time
        
        all_tests: List[BDQTest] = []
        for attempt in range(max_retries):
            try:
                resp = requests.get(self.tests_endpoint, timeout=30)
                resp.raise_for_status()
                all_tests = [BDQTest(**test) for test in resp.json()]
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.SSLError) as e:
                # BDQ API might not be ready yet (cold start)
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                if attempt < max_retries - 1:
                    log(
                        f"BDQ API not ready for tests list (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}",
                        "WARNING",
                    )
                    time.sleep(wait_time)
                    continue
                log(f"Failed to fetch BDQ tests list after {max_retries} attempts: {e}", "ERROR")
                raise
            except requests.exceptions.RequestException as e:
                log(f"Failed to fetch BDQ tests list: {e}", "ERROR")
                raise

        if not all_tests:
            log("BDQ tests endpoint returned no tests", "ERROR")
            return []
        
        applicable_tests = [
            test for test in all_tests
            if (test.type != "Measure" and  # Skip measure tests
                all(col in csv_columns for col in test.actedUpon) and
                all(col in csv_columns for col in test.consulted))
        ]
        
        log(f"Found {len(applicable_tests)} applicable tests out of {len(all_tests)} total tests (excluding measure tests)")
        return applicable_tests
    
    async def run_tests_on_dataset(self, df, core_type):
        """Run BDQ tests and return unique results with counts (no per-row expansion).

        For each applicable test:
        - Build unique candidate combinations across actedUpon+consulted columns with counts
        - Send batch request preserving order
        - Combine API results back to candidates by position
        - Include: test columns (values used), status/result/comment, test id/type,
          human-readable actedUpon/consulted strings, actedUpon_cols/consulted_cols, and count
        """
        start_time = time.time()
        applicable_tests = self._filter_applicable_tests(df.columns.tolist())
        # Reset failed tests for this run
        self.failed_tests = []
        all_unique_results: List[pd.DataFrame] = []

        def _shorten_test_id(test_id):
            return test_id.replace("VALIDATION_", "V").replace("AMENDMENT_", "A")

        log(f"Running {len(applicable_tests)} tests: [{', '.join(_shorten_test_id(test.id) for test in applicable_tests)}]")

        for test in applicable_tests:
            log(
                f"Starting test {test.id}: type={test.type}, actedUpon={len(test.actedUpon)}, consulted={len(test.consulted)}"
            )
            test_columns = test.actedUpon + test.consulted
            if not test_columns:
                # Defensive, though all tests should act on at least one column
                continue

            # Normalize candidate columns for matching and API params
            norm_candidates = (
                df[test_columns]
                .replace([np.nan, np.inf, -np.inf], "")
                .astype(str)
            )

            # Compute unique combinations with counts
            unique_counts = (
                norm_candidates
                .groupby(test_columns, dropna=False, as_index=False)
                .size()
                .rename(columns={"size": "count"})
            )

            if unique_counts.empty:
                log(f"Skipping {test.id}: no candidates")
                continue

            # Prepare batch request preserving order
            unique_params = unique_counts[test_columns].to_dict(orient="records")
            unique_test_candidates_batch_request = [
                {"id": test.id, "params": params} for params in unique_params
            ]

            # Call batch endpoint (results returned in same order)
            api_start_time = time.time()
            success, batch_results = self._post_batch(unique_test_candidates_batch_request)
            api_duration = time.time() - api_start_time
            if not success or batch_results is None:
                log(
                    f"Skipping test {test.id}: batch request failed",
                    "ERROR",
                )
                # Track failed test and continue with others
                try:
                    self.failed_tests.append(test.id)
                except Exception:
                    pass
                continue

            # Build results DataFrame aligned by order
            results_df = pd.DataFrame(batch_results).fillna("")
            results_df["test_id"] = test.id
            results_df["test_type"] = test.type

            # Stitch candidates + counts + results side-by-side
            stitched = pd.concat(
                [unique_counts.reset_index(drop=True), results_df.reset_index(drop=True)], axis=1
            )

            # Add actedUpon/consulted strings and column lists
            acted_upon_cols = "|".join(test.actedUpon)
            consulted_cols = "|".join(test.consulted)

            def _pairs_str(row: pd.Series, cols: List[str]) -> str:
                pairs = []
                for c in cols:
                    v = row.get(c, "")
                    if pd.notna(v) and v != "":
                        pairs.append(f"{c}={v}")
                    else:
                        # still include empty values for transparency
                        pairs.append(f"{c}=")
                return "|".join(pairs)

            stitched["actedUpon_cols"] = acted_upon_cols
            stitched["consulted_cols"] = consulted_cols
            stitched["actedUpon"] = stitched.apply(lambda r: _pairs_str(r, test.actedUpon), axis=1)
            stitched["consulted"] = stitched.apply(lambda r: _pairs_str(r, test.consulted), axis=1)

            # Order columns for readability
            ordered_cols = (
                [*test_columns, "count", "test_id", "test_type", "status", "result", "comment", "actedUpon", "consulted", "actedUpon_cols", "consulted_cols"]
            )
            # Some tests may have no consulted columns; ensure columns exist
            for c in ordered_cols:
                if c not in stitched.columns:
                    stitched[c] = ""
            stitched = stitched[ordered_cols]

            all_unique_results.append(stitched)
            log(
                f"Completed test {test.id}: {len(stitched)} unique combos (affecting {int(stitched['count'].sum())} rows). API {api_duration:.2f}s"
            )

        total_duration = time.time() - start_time
        if all_unique_results:
            all_unique_df = pd.concat(all_unique_results, ignore_index=True)
            # Reorder columns for consistency: put core metadata first, then all raw field columns
            base_cols = [
                "test_id",
                "test_type",
                "status",
                "result",
                "comment",
                "actedUpon",
                "consulted",
                "actedUpon_cols",
                "consulted_cols",
                "count",
            ]
            raw_cols = [
                c for c in all_unique_df.columns
                if c not in base_cols and (":" in c)  # likely field columns like dwc:*, dc:type, etc.
            ]
            # Keep stable, readable order
            ordered = base_cols + sorted(raw_cols)
            # Ensure all columns present
            for c in ordered:
                if c not in all_unique_df.columns:
                    all_unique_df[c] = ""
            # Also carry through any leftover non-field columns (rare)
            leftovers = [c for c in all_unique_df.columns if c not in ordered]
            final_cols = ordered + leftovers
            all_unique_df = all_unique_df[final_cols]

            log(
                f"Completed all tests: {len(all_unique_df)} unique results across {len(applicable_tests)} tests (total {total_duration:.2f}s)"
            )
            return all_unique_df
        else:
            log(
                f"No tests were successfully completed or applicable (total time: {total_duration:.2f}s)",
                "ERROR",
            )
            return pd.DataFrame()
