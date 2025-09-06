"""
Tests for the improved amendment functionality in CSV service.
"""

import pytest
import pandas as pd
from io import StringIO

from app.services.csv_service import CSVService


def test_amendment_mapping():
    """Test that amendment mapping works correctly"""
    csv_service = CSVService()
    
    # Create test data with amendments
    test_results = pd.DataFrame([
        {
            'occurrenceID': 'occ1',
            'test_id': 'AMENDMENT_EVENTDATE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': '2023-01-01T00:00:00',
            'comment': 'Standardized date format'
        },
        {
            'occurrenceID': 'occ2',
            'test_id': 'AMENDMENT_COUNTRYCODE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': 'US',
            'comment': 'Standardized country code'
        },
        {
            'occurrenceID': 'occ3',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'COMPLIANT',
            'comment': 'Valid country code'
        }
    ])
    
    original_df = pd.DataFrame([
        {'occurrenceID': 'occ1', 'dwc:eventDate': 'Jan 1, 2023', 'dwc:countryCode': 'USA'},
        {'occurrenceID': 'occ2', 'dwc:eventDate': '2023-01-02', 'dwc:countryCode': 'United States'},
        {'occurrenceID': 'occ3', 'dwc:eventDate': '2023-01-03', 'dwc:countryCode': 'US'}
    ])
    
    # Generate amended dataset
    amended_csv = csv_service.generate_amended_dataset(original_df, test_results, 'occurrence')
    amended_df = pd.read_csv(StringIO(amended_csv))
    
    # Verify amendments were applied
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ1', 'dwc:eventDate'].iloc[0] == '2023-01-01T00:00:00'
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ2', 'dwc:countryCode'].iloc[0] == 'US'
    # occ3 should be unchanged (validation, not amendment)
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ3', 'dwc:countryCode'].iloc[0] == 'US'


def test_coordinate_transposition():
    """Test coordinate transposition amendment"""
    csv_service = CSVService()
    
    # Create test data with coordinate transposition
    test_results = pd.DataFrame([
        {
            'occurrenceID': 'occ1',
            'test_id': 'AMENDMENT_COORDINATES_TRANSPOSED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': 'transposed',
            'comment': 'Coordinates were transposed'
        }
    ])
    
    original_df = pd.DataFrame([
        {'occurrenceID': 'occ1', 'dwc:decimalLatitude': '37.7749', 'dwc:decimalLongitude': '-122.4194'}
    ])
    
    # Generate amended dataset
    amended_csv = csv_service.generate_amended_dataset(original_df, test_results, 'occurrence')
    amended_df = pd.read_csv(StringIO(amended_csv))
    
    # Verify coordinates were swapped
    assert float(amended_df.loc[amended_df['occurrenceID'] == 'occ1', 'dwc:decimalLatitude'].iloc[0]) == -122.4194
    assert float(amended_df.loc[amended_df['occurrenceID'] == 'occ1', 'dwc:decimalLongitude'].iloc[0]) == 37.7749


def test_no_amendments():
    """Test case where no amendments need to be applied"""
    csv_service = CSVService()
    
    # Create test data with only validation results
    test_results = pd.DataFrame([
        {
            'occurrenceID': 'occ1',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'COMPLIANT',
            'comment': 'Valid country code'
        }
    ])
    
    original_df = pd.DataFrame([
        {'occurrenceID': 'occ1', 'dwc:countryCode': 'US'}
    ])
    
    # Generate amended dataset
    amended_csv = csv_service.generate_amended_dataset(original_df, test_results, 'occurrence')
    amended_df = pd.read_csv(StringIO(amended_csv))
    
    # Verify no changes were made
    assert amended_df.equals(original_df)


def test_unknown_amendment_test():
    """Test handling of unknown amendment test IDs"""
    csv_service = CSVService()
    
    # Create test data with unknown amendment test
    test_results = pd.DataFrame([
        {
            'occurrenceID': 'occ1',
            'test_id': 'UNKNOWN_AMENDMENT_TEST',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': 'some_value',
            'comment': 'Unknown amendment'
        }
    ])
    
    original_df = pd.DataFrame([
        {'occurrenceID': 'occ1', 'dwc:countryCode': 'US'}
    ])
    
    # Generate amended dataset
    amended_csv = csv_service.generate_amended_dataset(original_df, test_results, 'occurrence')
    amended_df = pd.read_csv(StringIO(amended_csv))
    
    # Verify no changes were made (unknown test should be ignored)
    assert amended_df.equals(original_df)


def test_amendment_with_missing_field():
    """Test handling of amendments for fields not in the dataset"""
    csv_service = CSVService()
    
    # Create test data with amendment for field not in dataset
    test_results = pd.DataFrame([
        {
            'occurrenceID': 'occ1',
            'test_id': 'AMENDMENT_EVENTDATE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': '2023-01-01T00:00:00',
            'comment': 'Standardized date format'
        }
    ])
    
    # Original dataset without the eventDate field
    original_df = pd.DataFrame([
        {'occurrenceID': 'occ1', 'dwc:countryCode': 'US'}
    ])
    
    # Generate amended dataset
    amended_csv = csv_service.generate_amended_dataset(original_df, test_results, 'occurrence')
    amended_df = pd.read_csv(StringIO(amended_csv))
    
    # Verify no changes were made (field not found should be ignored)
    assert amended_df.equals(original_df)


def test_multiple_amendments_same_record():
    """Test applying multiple amendments to the same record"""
    csv_service = CSVService()
    
    # Create test data with multiple amendments for same record
    test_results = pd.DataFrame([
        {
            'occurrenceID': 'occ1',
            'test_id': 'AMENDMENT_EVENTDATE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': '2023-01-01T00:00:00',
            'comment': 'Standardized date format'
        },
        {
            'occurrenceID': 'occ1',
            'test_id': 'AMENDMENT_COUNTRYCODE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': 'US',
            'comment': 'Standardized country code'
        }
    ])
    
    original_df = pd.DataFrame([
        {'occurrenceID': 'occ1', 'dwc:eventDate': 'Jan 1, 2023', 'dwc:countryCode': 'USA'}
    ])
    
    # Generate amended dataset
    amended_csv = csv_service.generate_amended_dataset(original_df, test_results, 'occurrence')
    amended_df = pd.read_csv(StringIO(amended_csv))
    
    # Verify both amendments were applied
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ1', 'dwc:eventDate'].iloc[0] == '2023-01-01T00:00:00'
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ1', 'dwc:countryCode'].iloc[0] == 'US'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
