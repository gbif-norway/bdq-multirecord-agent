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
    
    def _post_batch_with_backoff(self, payload: List[Dict[str, Any]], timeout: int = 1800) -> Tuple[bool, Optional[List[Dict[str, Any]]]]:
        """Post the payload with escalating chunking: 1 → 2 → 4 → 8.

        Returns (success, results) where results is a list of dicts aligned to input order
        when success is True; otherwise (False, None).
        """
        def _post_chunk(chunk: List[Dict[str, Any]]):
            resp = http_post_with_retry(self.batch_endpoint, json=chunk, timeout=timeout)
            return resp.json()

        def _partition(seq: List[Any], parts: int) -> List[List[Any]]:
            n = len(seq)
            if parts <= 1 or n == 0:
                return [seq]
            base, rem = divmod(n, parts)
            chunks = []
            start = 0
            for i in range(parts):
                size = base + (1 if i < rem else 0)
                end = start + size
                chunks.append(seq[start:end])
                start = end
            return chunks

        n_total = len(payload)
        for chunks_count in (1, 2, 4, 8):
            slices = _partition(payload, chunks_count)
            t0 = time.time()
            results: List[Dict[str, Any]] = []
            had_timeout = False
            try:
                for idx, ch in enumerate(slices):
                    if not ch:
                        continue
                    cstart = time.time()
                    try:
                        r = _post_chunk(ch)
                        results.extend(r)
                        log(
                            f"BDQ sub-batch {chunks_count}/{idx+1}: {len(ch)} items, {time.time() - cstart:.2f}s"
                        )
                    except requests.Timeout:
                        had_timeout = True
                        log(
                            f"BDQ sub-batch {chunks_count}/{idx+1} timed out for {len(ch)} items",
                            "WARNING",
                        )
                        # No need to continue this stage; escalate
                        break
                if not had_timeout and len(results) == n_total:
                    if chunks_count == 1:
                        log(
                            f"BDQ batch posted in 1 chunk: {n_total} items, {time.time() - t0:.2f}s"
                        )
                    else:
                        log(
                            f"BDQ batch succeeded in {chunks_count} chunks: total {n_total} items, {time.time() - t0:.2f}s"
                        )
                    return True, results
                if had_timeout:
                    # escalate to next chunk level
                    continue
                # If we reach here without timeouts but size mismatch, it's an error
                log(
                    f"BDQ {chunks_count}-chunk mismatch: expected {n_total} results, got {len(results)}",
                    "ERROR",
                )
                return False, None
            except Exception as e:
                if isinstance(e, requests.Timeout):
                    # Should be captured above, but handle defensively
                    continue
                log(
                    f"BDQ batch failed during {chunks_count}-chunk attempt: {type(e).__name__}: {e}",
                    "ERROR",
                )
                return False, None

        log(
            "BDQ batch timed out even with 8 chunks; aborting for manual inspection",
            "ERROR",
        )
        return False, None
    
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
            success, batch_results = self._post_batch_with_backoff(
                unique_test_candidates_batch_request, timeout=1800
            )
            api_duration = time.time() - api_start_time
            if not success or batch_results is None:
                log(
                    f"Aborting on test {test.id}: batch requests timed out after split attempts",
                    "ERROR",
                )
                return pd.DataFrame()

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
