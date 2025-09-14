import requests
import logging
import asyncio
import time
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from app.utils.helper import log, http_get_with_retry, http_post_with_retry

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
        self.batch_endpoint = f"{self.bdq_api_base}/api/v1/tests/run/batch"  # see readme
    
    def _filter_applicable_tests(self, csv_columns: List[str]) -> List[BDQTest]:
        """Filter tests that can be applied to the CSV columns"""
        all_tests_response = http_get_with_retry(self.tests_endpoint, timeout=30)
        all_tests = [BDQTest(**test) for test in all_tests_response.json()]
        
        applicable_tests = [
            test for test in all_tests
            if (test.type != "Measure" and  # Skip measure testsl
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
        all_unique_results: List[pd.DataFrame] = []

        def _shorten_test_id(test_id):
            return test_id.replace("VALIDATION_", "V").replace("AMENDMENT_", "A")

        log(f"Running {len(applicable_tests)} tests: [{', '.join(_shorten_test_id(test.id) for test in applicable_tests)}]")

        for test in applicable_tests:
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
            batch_response = http_post_with_retry(
                self.batch_endpoint, json=unique_test_candidates_batch_request, timeout=1800
            )
            batch_results = batch_response.json()
            api_duration = time.time() - api_start_time

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
