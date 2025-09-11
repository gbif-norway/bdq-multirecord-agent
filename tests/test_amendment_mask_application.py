"""
Unit test for mask-based amendment application using unique results format.
This does NOT hit the live BDQ API.
"""

import pandas as pd
from app.services.csv_service import CSVService


def test_mask_based_amendment_single_field_multi_key():
    csv_service = CSVService()

    # Original dataset: three rows, two unique key combinations
    df = pd.DataFrame([
        {"dwc:occurrenceID": "1", "dwc:countryCode": "US", "dwc:decimalLatitude": "40.0", "dwc:decimalLongitude": "-74.0"},
        {"dwc:occurrenceID": "2", "dwc:countryCode": "US", "dwc:decimalLatitude": "40.0", "dwc:decimalLongitude": "-78.0"},
        {"dwc:occurrenceID": "3", "dwc:countryCode": "US", "dwc:decimalLatitude": "40.0", "dwc:decimalLongitude": "-78.0"},
    ])

    # Unique results: only the second combination gets amended
    unique_results = pd.DataFrame([
        {
            # Raw key values as columns
            "dwc:countryCode": "US",
            "dwc:decimalLatitude": "40.0",
            "dwc:decimalLongitude": "-74.0",
            # Count of matching rows
            "count": 1,
            # Test metadata
            "test_id": "AMENDMENT_COUNTRYCODE_FROM_COORDINATES",
            "test_type": "Amendment",
            "status": "NOT_AMENDED",
            "result": "",
            "comment": "",
            # For matching
            "actedUpon": "dwc:countryCode=US",
            "consulted": "dwc:decimalLatitude=40.0|dwc:decimalLongitude=-74.0",
            "actedUpon_cols": "dwc:countryCode",
            "consulted_cols": "dwc:decimalLatitude|dwc:decimalLongitude",
        },
        {
            "dwc:countryCode": "US",
            "dwc:decimalLatitude": "40.0",
            "dwc:decimalLongitude": "-78.0",
            "count": 2,
            "test_id": "AMENDMENT_COUNTRYCODE_FROM_COORDINATES",
            "test_type": "Amendment",
            "status": "AMENDED",
            "result": "dwc:countryCode=CAN",
            "comment": "",
            "actedUpon": "dwc:countryCode=US",
            "consulted": "dwc:decimalLatitude=40.0|dwc:decimalLongitude=-78.0",
            "actedUpon_cols": "dwc:countryCode",
            "consulted_cols": "dwc:decimalLatitude|dwc:decimalLongitude",
        },
    ])

    amended = csv_service.generate_amended_dataset(df, unique_results, core_type="occurrence")

    # First row stays US
    assert amended.loc[amended["dwc:occurrenceID"] == "1", "dwc:countryCode"].iloc[0] == "US"
    # Rows 2 and 3 become CAN
    assert amended.loc[amended["dwc:occurrenceID"] == "2", "dwc:countryCode"].iloc[0] == "CAN"
    assert amended.loc[amended["dwc:occurrenceID"] == "3", "dwc:countryCode"].iloc[0] == "CAN"

