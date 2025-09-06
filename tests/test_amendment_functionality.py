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
            'result': 'dwc:eventDate=2023-01-01T00:00:00',
            'comment': 'Standardized date format'
        },
        {
            'occurrenceID': 'occ2',
            'test_id': 'AMENDMENT_COUNTRYCODE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': 'dwc:countryCode=US',
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
            'result': 'dwc:eventDate=2023-01-01T00:00:00',
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
            'result': 'dwc:eventDate=2023-01-01T00:00:00',
            'comment': 'Standardized date format'
        },
        {
            'occurrenceID': 'occ1',
            'test_id': 'AMENDMENT_COUNTRYCODE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': 'dwc:countryCode=US',
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


def test_multi_field_amendment():
    """Test applying multi-field amendments (separated by |)"""
    csv_service = CSVService()
    
    # Create test data with multi-field amendment
    test_results = pd.DataFrame([
        {
            'occurrenceID': 'occ1',
            'test_id': 'AMENDMENT_DEPTH_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': 'dwc:minimumDepthInMeters=3.048|dwc:maximumDepthInMeters=3.048',
            'comment': 'Standardized depth values'
        }
    ])
    
    original_df = pd.DataFrame([
        {'occurrenceID': 'occ1', 'dwc:minimumDepthInMeters': '10', 'dwc:maximumDepthInMeters': '10'}
    ])
    
    # Generate amended dataset
    amended_csv = csv_service.generate_amended_dataset(original_df, test_results, 'occurrence')
    amended_df = pd.read_csv(StringIO(amended_csv))
    
    # Verify both fields were amended
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ1', 'dwc:minimumDepthInMeters'].iloc[0] == 3.048
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ1', 'dwc:maximumDepthInMeters'].iloc[0] == 3.048


def test_not_amended_status():
    """Test handling of NOT_AMENDED status from BDQ API"""
    csv_service = CSVService()
    
    # Create test data with NOT_AMENDED status
    test_results = pd.DataFrame([
        {
            'occurrenceID': 'occ1',
            'test_id': 'AMENDMENT_BASISOFRECORD_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'NOT_AMENDED',
            'result': '',
            'comment': 'Value already in correct format'
        },
        {
            'occurrenceID': 'occ2',
            'test_id': 'AMENDMENT_BASISOFRECORD_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': 'dwc:basisOfRecord=HumanObservation',
            'comment': 'Standardized basis of record'
        }
    ])
    
    original_df = pd.DataFrame([
        {'occurrenceID': 'occ1', 'dwc:basisOfRecord': 'HumanObservation'},
        {'occurrenceID': 'occ2', 'dwc:basisOfRecord': 'human observation'}
    ])
    
    # Generate amended dataset
    amended_csv = csv_service.generate_amended_dataset(original_df, test_results, 'occurrence')
    amended_df = pd.read_csv(StringIO(amended_csv))
    
    # Verify NOT_AMENDED record is unchanged
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ1', 'dwc:basisOfRecord'].iloc[0] == 'HumanObservation'
    
    # Verify AMENDED record is changed
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ2', 'dwc:basisOfRecord'].iloc[0] == 'HumanObservation'


def test_mixed_amendment_statuses():
    """Test handling of mixed AMENDED and NOT_AMENDED statuses"""
    csv_service = CSVService()
    
    # Create test data with mixed statuses
    test_results = pd.DataFrame([
        {
            'occurrenceID': 'occ1',
            'test_id': 'AMENDMENT_EVENTDATE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': 'dwc:eventDate=2023-01-01T00:00:00',
            'comment': 'Standardized date format'
        },
        {
            'occurrenceID': 'occ1',
            'test_id': 'AMENDMENT_COUNTRYCODE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'NOT_AMENDED',
            'result': '',
            'comment': 'Country code already standardized'
        },
        {
            'occurrenceID': 'occ2',
            'test_id': 'AMENDMENT_EVENTDATE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'NOT_AMENDED',
            'result': '',
            'comment': 'Date already in correct format'
        }
    ])
    
    original_df = pd.DataFrame([
        {'occurrenceID': 'occ1', 'dwc:eventDate': 'Jan 1, 2023', 'dwc:countryCode': 'US'},
        {'occurrenceID': 'occ2', 'dwc:eventDate': '2023-01-02', 'dwc:countryCode': 'CA'}
    ])
    
    # Generate amended dataset
    amended_csv = csv_service.generate_amended_dataset(original_df, test_results, 'occurrence')
    amended_df = pd.read_csv(StringIO(amended_csv))
    
    # Verify occ1: eventDate amended, countryCode unchanged
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ1', 'dwc:eventDate'].iloc[0] == '2023-01-01T00:00:00'
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ1', 'dwc:countryCode'].iloc[0] == 'US'
    
    # Verify occ2: both fields unchanged
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ2', 'dwc:eventDate'].iloc[0] == '2023-01-02'
    assert amended_df.loc[amended_df['occurrenceID'] == 'occ2', 'dwc:countryCode'].iloc[0] == 'CA'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
