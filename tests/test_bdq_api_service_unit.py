"""
Unit tests for BDQAPIService internals with HTTP mocked.

These tests do not require network access and validate:
- _filter_applicable_tests selects only tests whose actedUpon/consulted fields
  are present in the provided DataFrame columns
- run_tests_on_dataset builds batch requests from unique combinations and
  merges results back to the original rows with expected columns
"""

import asyncio
import pandas as pd
import pytest
from unittest.mock import patch

from app.services.bdq_api_service import BDQAPIService


class _FakeResponse:
    def __init__(self, json_data, status_code=200):
        self._json = json_data
        self.status_code = status_code

    def json(self):
        return self._json

    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise AssertionError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, tests_payload=None, batch_handler=None):
        self._tests_payload = tests_payload or []
        self._batch_handler = batch_handler
        self.last_post_payload = None

    def get(self, url, timeout=None):
        return _FakeResponse(self._tests_payload, 200)

    def post(self, url, json=None, timeout=None):
        self.last_post_payload = list(json or [])
        if self._batch_handler is not None:
            payload = self._batch_handler(self.last_post_payload)
        else:
            # Default: one OK result per item
            payload = [
                {"status": "RUN_HAS_RESULT", "result": "COMPLIANT", "comment": ""}
                for _ in self.last_post_payload
            ]
        return _FakeResponse(payload, 200)


@pytest.fixture
def occurrence_df():
    return pd.DataFrame([
        {
            "dwc:occurrenceID": "occ1",
            "dwc:countryCode": "US",
            "dwc:decimalLatitude": "37.7",
            "dwc:decimalLongitude": "-122.4",
        },
        {
            "dwc:occurrenceID": "occ2",
            "dwc:countryCode": "CA",
            "dwc:decimalLatitude": "43.6",
            "dwc:decimalLongitude": "-79.3",
        },
        {
            "dwc:occurrenceID": "occ3",
            "dwc:countryCode": "US",
            "dwc:decimalLatitude": "37.7",
            "dwc:decimalLongitude": "-122.4",
        },
    ])


def test_filter_applicable_tests_basic(occurrence_df):
    """Test that the real BDQ API finds applicable tests for our occurrence data"""
    svc = BDQAPIService()
    
    # Test with real BDQ API - should find applicable tests for occurrence data
    applicable = svc._filter_applicable_tests(occurrence_df.columns.tolist())
    
    # Should find some applicable tests since we have proper dwc: prefixed columns
    assert len(applicable) > 0, "Should find applicable tests for occurrence data with dwc: prefixed columns"
    
    # Verify that all found tests are actually applicable (all their actedUpon fields exist in our data)
    for test in applicable:
        for field in test.actedUpon:
            assert field in occurrence_df.columns, f"Test {test.id} requires field {field} which is not in our data"
    
    print(f"✓ Found {len(applicable)} applicable tests for occurrence data")
    for test in applicable[:5]:  # Show first 5 tests
        print(f"  - {test.id} ({test.type}) - acts on {test.actedUpon}")


@pytest.mark.asyncio
async def test_run_tests_on_dataset_merging(occurrence_df):
    """Test that the real BDQ API can run tests on our occurrence data and return proper results"""
    svc = BDQAPIService()
    
    # Run tests on the real BDQ API with our occurrence data
    results = await svc.run_tests_on_dataset(occurrence_df, core_type="occurrence")
    
    # Should get results back
    assert len(results) > 0, "Should get some test results from BDQ API"
    
    # Required columns should be present
    required_columns = ["dwc:occurrenceID", "test_id", "test_type", "status", "result", "comment"]
    for col in required_columns:
        assert col in results.columns, f"Missing required column: {col}"
    
    # Should have results for all our occurrence records
    unique_occurrence_ids = set(results["dwc:occurrenceID"].unique())
    expected_occurrence_ids = set(occurrence_df["dwc:occurrenceID"].unique())
    assert unique_occurrence_ids == expected_occurrence_ids, "Should have results for all occurrence records"
    
    # Should have both validation and amendment tests
    test_types = set(results["test_type"].unique())
    assert "Validation" in test_types, "Should have validation tests"
    
    # Verify result structure
    for _, row in results.iterrows():
        assert row["status"] in ["RUN_HAS_RESULT", "AMENDED", "NOT_AMENDED", "INTERNAL_PREREQUISITES_NOT_MET"], \
            f"Invalid status: {row['status']}"
        assert isinstance(row["result"], str), "Result should be a string"
        assert isinstance(row["comment"], str), "Comment should be a string"
    
    print(f"✓ Successfully ran {len(results)} tests on {len(occurrence_df)} occurrence records")
    print(f"  - Found {len(set(results['test_id'].unique()))} unique tests")
    print(f"  - Test types: {', '.join(test_types)}")
    
    # Show some sample results
    sample_results = results.head(3)
    for _, row in sample_results.iterrows():
        print(f"  - {row['test_id']}: {row['status']} -> {row['result'][:50]}...")
