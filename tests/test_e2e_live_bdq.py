"""
E2E test that hits the live BDQ API to validate the end-to-end flow:
- Build a small occurrence dataset
- Run BDQ tests to get unique results with counts
- Apply amendments to produce an amended dataset

This requires external network access and the live BDQ API to be reachable.
Skip or mark as integration if your CI environment cannot access the network.
"""

import os
import pytest
import pandas as pd

from app.services.bdq_api_service import BDQAPIService
from app.services.csv_service import CSVService


@pytest.mark.integration
@pytest.mark.skipif(os.getenv('RUN_LIVE_BDQ') != '1', reason='Set RUN_LIVE_BDQ=1 to run live BDQ E2E test')
def test_e2e_live_bdq_occurrence_flow():
    # Prepare a small, realistic dataset
    df = pd.DataFrame([
        {
            "dwc:occurrenceID": "occ1",
            "dwc:eventDate": "2023-01-01",
            "dwc:countryCode": "US",
            "dwc:decimalLatitude": "37.7749",
            "dwc:decimalLongitude": "-122.4194",
            "dwc:scientificName": "Homo sapiens",
            "dwc:basisOfRecord": "HumanObservation",
        },
        {
            "dwc:occurrenceID": "occ2",
            "dwc:eventDate": "8 May 1880",
            "dwc:countryCode": "CA",
            "dwc:decimalLatitude": "43.6532",
            "dwc:decimalLongitude": "-79.3832",
            "dwc:scientificName": "Canis lupus",
            "dwc:basisOfRecord": "HumanObservation",
        },
    ])

    bdq = BDQAPIService()
    csv = CSVService()

    # Run tests (live API)
    import asyncio
    unique = asyncio.run(bdq.run_tests_on_dataset(df, core_type="occurrence"))

    # Basic assertions on shape/content
    assert isinstance(unique, pd.DataFrame)
    assert not unique.empty, "Unique results should not be empty when BDQ API is reachable"
    for col in ["count", "test_id", "test_type", "status", "result", "comment", "actedUpon", "consulted", "actedUpon_cols", "consulted_cols"]:
        assert col in unique.columns, f"Missing expected column: {col}"

    # For any single test, sum(count) should equal dataset size (with current inclusive candidate gen)
    first_test = unique["test_id"].iloc[0]
    total = int(unique[unique["test_id"] == first_test]["count"].sum())
    assert total == len(df), f"Sum of counts for a single test should equal dataset size (got {total} for {len(df)})"

    # Apply amendments; result may or may not change values, but should preserve shape
    amended = csv.generate_amended_dataset(df, unique, core_type="occurrence")
    assert isinstance(amended, pd.DataFrame)
    assert len(amended) == len(df)
