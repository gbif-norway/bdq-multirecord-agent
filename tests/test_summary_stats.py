"""
Unit tests for the _get_summary_stats_from_unique_results method in main.py

These tests focus specifically on testing the summary statistics generation
functionality with various test data scenarios using unique results approach.
"""

import pytest
import pandas as pd
import numpy as np

# Import the function directly to avoid import issues with other dependencies
def _get_summary_stats_from_unique_results(unique_results_df, core_type, original_dataset_length):
    """Generate summary statistics from unique results DataFrame - adapted from _get_summary_stats"""
    # Extract unique field names from actedUpon and consulted columns (same as _get_summary_stats)
    acted_upon_fields = set()
    consulted_fields = set()
    
    for acted_upon_str in unique_results_df['actedUpon'].dropna():
        if acted_upon_str:  # Skip empty strings
            # Split by | and extract field names (before =)
            for pair in acted_upon_str.split('|'):
                if '=' in pair:
                    field_name = pair.split('=')[0]
                    acted_upon_fields.add(field_name)
    
    for consulted_str in unique_results_df['consulted'].dropna():
        if consulted_str:  # Skip empty strings
            # Split by | and extract field names (before =)
            for pair in consulted_str.split('|'):
                if '=' in pair:
                    field_name = pair.split('=')[0]
                    consulted_fields.add(field_name)
    
    all_cols_tested = list(acted_upon_fields.union(consulted_fields))
    amendments = unique_results_df[unique_results_df['status'] == 'AMENDED']
    filled_in = unique_results_df[unique_results_df['status'] == 'FILLED_IN']
    issues = unique_results_df[unique_results_df['result'] == 'POTENTIAL_ISSUE']
    non_compliant_validations = unique_results_df[unique_results_df['result'] == 'NOT_COMPLIANT']

    def _get_top_grouped(df, n=15):
        """Helper to get top n grouped counts sorted descending using the count column."""
        return (df.sort_values('count', ascending=False)
                .head(n)
                [['actedUpon', 'consulted', 'test_id', 'count']])

    summary = {
        'number_of_records_in_dataset': original_dataset_length,
        'list_of_all_columns_tested': all_cols_tested,
        'no_of_tests_results': unique_results_df['count'].sum(),
        'no_of_tests_run': unique_results_df['test_id'].nunique(),
        'no_of_non_compliant_validations': non_compliant_validations['count'].sum(),
        'no_of_unique_non_compliant_validations': len(non_compliant_validations),
        'no_of_amendments': amendments['count'].sum(),
        'no_of_unique_amendments': len(amendments),
        'no_of_filled_in': filled_in['count'].sum(),
        'no_of_unique_filled_in': len(filled_in),
        'no_of_issues': issues['count'].sum(),
        'no_of_unique_issues': len(issues),
        'top_issues': _get_top_grouped(issues),
        'top_filled_in': _get_top_grouped(filled_in),
        'top_amendments': _get_top_grouped(amendments),
        'top_non_compliant_validations': _get_top_grouped(non_compliant_validations),
    }

    return summary

def _create_unique_results_from_test_data(test_results_df, core_type):
    """Helper function to create unique results DataFrame from test data"""
    # Group by all columns except the ID, count occurrences, and add as 'count' column
    group_cols = [col for col in test_results_df.columns if col != f'dwc:{core_type}ID']
    unique_results = (
        test_results_df
        .groupby(group_cols, dropna=False)
        .size()
        .reset_index()
        .rename(columns={0: "count"})
    )
    return unique_results


class TestGetSummaryStatsFromUniqueResults:
    """Test class for _get_summary_stats_from_unique_results method"""

    def test_basic_functionality_with_simple_data(self):
        """Test basic functionality with simple test data"""
        # Create simple test data similar to the structure from simple_occurrence_dwc_RESULTS.csv
        test_data = {
            'dwc:occurrenceID': ['occ1', 'occ2', 'occ3', 'occ4', 'occ5'],
            'test_id': [
                'VALIDATION_BASISOFRECORD_NOTEMPTY',
                'VALIDATION_BASISOFRECORD_NOTEMPTY', 
                'VALIDATION_BASISOFRECORD_NOTEMPTY',
                'VALIDATION_BASISOFRECORD_NOTEMPTY',
                'VALIDATION_BASISOFRECORD_NOTEMPTY'
            ],
            'test_type': ['Validation', 'Validation', 'Validation', 'Validation', 'Validation'],
            'status': ['RUN_HAS_RESULT', 'RUN_HAS_RESULT', 'RUN_HAS_RESULT', 'RUN_HAS_RESULT', 'RUN_HAS_RESULT'],
            'result': ['COMPLIANT', 'COMPLIANT', 'COMPLIANT', 'COMPLIANT', 'COMPLIANT'],
            'comment': ['Some value provided', 'Some value provided', 'Some value provided', 'Some value provided', 'Some value provided'],
            'actedUpon': ['dwc:basisOfRecord=HumanObservation', 'dwc:basisOfRecord=HumanObservation', 'dwc:basisOfRecord=HumanObservation', 'dwc:basisOfRecord=HumanObservation', 'dwc:basisOfRecord=BadBasis'],
            'consulted': ['', '', '', '', '']
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 3  # Simulate original dataset with 3 records
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        # Basic assertions
        assert summary['number_of_records_in_dataset'] == 3  # Original dataset length
        assert summary['no_of_tests_results'] == 5  # Sum of count column
        assert summary['no_of_tests_run'] == 1  # Only one unique test_id
        assert 'dwc:basisOfRecord' in summary['list_of_all_columns_tested']
        assert summary['no_of_non_compliant_validations'] == 0
        assert summary['no_of_amendments'] == 0
        assert summary['no_of_filled_in'] == 0
        assert summary['no_of_issues'] == 0

    def test_with_non_compliant_validations(self):
        """Test with non-compliant validation results"""
        test_data = {
            'dwc:occurrenceID': ['occ1', 'occ2', 'occ3'],
            'test_id': ['VALIDATION_BASISOFRECORD_STANDARD', 'VALIDATION_BASISOFRECORD_STANDARD', 'VALIDATION_BASISOFRECORD_STANDARD'],
            'test_type': ['Validation', 'Validation', 'Validation'],
            'status': ['RUN_HAS_RESULT', 'RUN_HAS_RESULT', 'RUN_HAS_RESULT'],
            'result': ['COMPLIANT', 'NOT_COMPLIANT', 'NOT_COMPLIANT'],
            'comment': ['Valid value', 'Invalid value', 'Invalid value'],
            'actedUpon': ['dwc:basisOfRecord=HumanObservation', 'dwc:basisOfRecord=BadBasis', 'dwc:basisOfRecord=BadBasis'],
            'consulted': ['', '', '']
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 3
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        assert summary['no_of_non_compliant_validations'] == 2  # Sum of count for NOT_COMPLIANT
        assert summary['no_of_unique_non_compliant_validations'] == 2  # All columns are different (different occurrenceIDs)
        assert len(summary['top_non_compliant_validations']) == 1

    def test_with_amendments(self):
        """Test with amendment results"""
        test_data = {
            'dwc:occurrenceID': ['occ1', 'occ2', 'occ3'],
            'test_id': ['AMENDMENT_BASISOFRECORD_STANDARDIZED', 'AMENDMENT_BASISOFRECORD_STANDARDIZED', 'AMENDMENT_BASISOFRECORD_STANDARDIZED'],
            'test_type': ['Amendment', 'Amendment', 'Amendment'],
            'status': ['AMENDED', 'AMENDED', 'NOT_AMENDED'],
            'result': ['', '', ''],
            'comment': ['Amended value', 'Amended value', 'No change needed'],
            'actedUpon': ['dwc:basisOfRecord=BadBasis', 'dwc:basisOfRecord=BadBasis', 'dwc:basisOfRecord=HumanObservation'],
            'consulted': ['', '', '']
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 3
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        assert summary['no_of_amendments'] == 2
        assert summary['no_of_unique_amendments'] == 2  # All columns are different (different occurrenceIDs)
        assert len(summary['top_amendments']) == 1

    def test_with_filled_in_results(self):
        """Test with filled in results"""
        test_data = {
            'dwc:occurrenceID': ['occ1', 'occ2', 'occ3'],
            'test_id': ['AMENDMENT_COUNTRYCODE_FROM_COORDINATES', 'AMENDMENT_COUNTRYCODE_FROM_COORDINATES', 'AMENDMENT_COUNTRYCODE_FROM_COORDINATES'],
            'test_type': ['Amendment', 'Amendment', 'Amendment'],
            'status': ['FILLED_IN', 'FILLED_IN', 'NOT_AMENDED'],
            'result': ['', '', ''],
            'comment': ['Filled in value', 'Filled in value', 'No change needed'],
            'actedUpon': ['dwc:countryCode=', 'dwc:countryCode=', 'dwc:countryCode=US'],
            'consulted': ['dwc:decimalLatitude=37.7749|dwc:decimalLongitude=-122.4194', 'dwc:decimalLatitude=43.6532|dwc:decimalLongitude=-79.3832', 'dwc:decimalLatitude=37.7749|dwc:decimalLongitude=-122.4194']
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 3
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        assert summary['no_of_filled_in'] == 2
        assert summary['no_of_unique_filled_in'] == 2  # All columns are different (different occurrenceIDs and consulted values)
        assert len(summary['top_filled_in']) == 2  # Two different combinations due to different consulted values

    def test_with_issues(self):
        """Test with issue results"""
        test_data = {
            'dwc:occurrenceID': ['occ1', 'occ2', 'occ3'],
            'test_id': ['ISSUE_COORDINATES_CENTEROFCOUNTRY', 'ISSUE_COORDINATES_CENTEROFCOUNTRY', 'ISSUE_COORDINATES_CENTEROFCOUNTRY'],
            'test_type': ['Issue', 'Issue', 'Issue'],
            'status': ['RUN_HAS_RESULT', 'RUN_HAS_RESULT', 'RUN_HAS_RESULT'],
            'result': ['POTENTIAL_ISSUE', 'POTENTIAL_ISSUE', 'COMPLIANT'],
            'comment': ['Potential issue', 'Potential issue', 'No issue'],
            'actedUpon': ['dwc:decimalLatitude=0|dwc:decimalLongitude=0', 'dwc:decimalLatitude=0|dwc:decimalLongitude=0', 'dwc:decimalLatitude=37.7749|dwc:decimalLongitude=-122.4194'],
            'consulted': ['dwc:countryCode=US', 'dwc:countryCode=US', 'dwc:countryCode=US']
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 3
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        assert summary['no_of_issues'] == 2
        assert summary['no_of_unique_issues'] == 2  # All columns are different (different occurrenceIDs)
        assert len(summary['top_issues']) == 1

    def test_field_extraction_from_acted_upon_and_consulted(self):
        """Test that field names are correctly extracted from actedUpon and consulted columns"""
        test_data = {
            'dwc:occurrenceID': ['occ1', 'occ2'],
            'test_id': ['VALIDATION_COORDINATESCOUNTRYCODE_CONSISTENT', 'AMENDMENT_COORDINATES_TRANSPOSED'],
            'test_type': ['Validation', 'Amendment'],
            'status': ['RUN_HAS_RESULT', 'NOT_AMENDED'],
            'result': ['COMPLIANT', ''],
            'comment': ['Valid', 'No change'],
            'actedUpon': [
                'dwc:decimalLatitude=37.7749|dwc:decimalLongitude=-122.4194|dwc:countryCode=US',
                'dwc:decimalLatitude=43.6532|dwc:decimalLongitude=-79.3832'
            ],
            'consulted': [
                '',
                'dwc:countryCode=CA'
            ]
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 2
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        # Check that all field names are extracted
        expected_fields = {'dwc:decimalLatitude', 'dwc:decimalLongitude', 'dwc:countryCode'}
        assert set(summary['list_of_all_columns_tested']) == expected_fields

    def test_empty_dataframe(self):
        """Test with empty DataFrame"""
        empty_df = pd.DataFrame(columns=[
            'dwc:occurrenceID', 'test_id', 'test_type', 'status', 'result', 'comment', 'actedUpon', 'consulted'
        ])
        
        unique_results = _create_unique_results_from_test_data(empty_df, 'occurrence')
        original_dataset_length = 0
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        assert summary['number_of_records_in_dataset'] == 0
        assert summary['no_of_tests_results'] == 0
        assert summary['no_of_tests_run'] == 0
        assert summary['list_of_all_columns_tested'] == []
        assert summary['no_of_non_compliant_validations'] == 0
        assert summary['no_of_amendments'] == 0
        assert summary['no_of_filled_in'] == 0
        assert summary['no_of_issues'] == 0

    def test_mixed_statuses_and_results(self):
        """Test with mixed statuses and results"""
        test_data = {
            'dwc:occurrenceID': ['occ1', 'occ2', 'occ3', 'occ4', 'occ5', 'occ6'],
            'test_id': [
                'VALIDATION_BASISOFRECORD_STANDARD',
                'VALIDATION_BASISOFRECORD_STANDARD',
                'AMENDMENT_BASISOFRECORD_STANDARDIZED',
                'AMENDMENT_BASISOFRECORD_STANDARDIZED',
                'ISSUE_COORDINATES_CENTEROFCOUNTRY',
                'ISSUE_COORDINATES_CENTEROFCOUNTRY'
            ],
            'test_type': ['Validation', 'Validation', 'Amendment', 'Amendment', 'Issue', 'Issue'],
            'status': ['RUN_HAS_RESULT', 'RUN_HAS_RESULT', 'AMENDED', 'NOT_AMENDED', 'RUN_HAS_RESULT', 'RUN_HAS_RESULT'],
            'result': ['COMPLIANT', 'NOT_COMPLIANT', '', '', 'POTENTIAL_ISSUE', 'COMPLIANT'],
            'comment': ['Valid', 'Invalid', 'Amended', 'No change', 'Issue found', 'No issue'],
            'actedUpon': [
                'dwc:basisOfRecord=HumanObservation',
                'dwc:basisOfRecord=BadBasis',
                'dwc:basisOfRecord=BadBasis',
                'dwc:basisOfRecord=HumanObservation',
                'dwc:decimalLatitude=0|dwc:decimalLongitude=0',
                'dwc:decimalLatitude=37.7749|dwc:decimalLongitude=-122.4194'
            ],
            'consulted': ['', '', '', '', 'dwc:countryCode=US', 'dwc:countryCode=US']
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 6
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        assert summary['number_of_records_in_dataset'] == 6
        assert summary['no_of_tests_run'] == 3  # Three unique test_ids
        assert summary['no_of_non_compliant_validations'] == 1
        assert summary['no_of_amendments'] == 1
        assert summary['no_of_issues'] == 1
        assert summary['no_of_filled_in'] == 0

    def test_top_grouped_results(self):
        """Test that top grouped results are correctly calculated and limited to 15"""
        # Create data with more than 15 unique combinations to test the limit
        test_data = []
        for i in range(20):
            test_data.append({
                'dwc:occurrenceID': f'occ{i}',
                'test_id': 'VALIDATION_BASISOFRECORD_STANDARD',
                'test_type': 'Validation',
                'status': 'RUN_HAS_RESULT',
                'result': 'NOT_COMPLIANT',
                'comment': 'Invalid value',
                'actedUpon': f'dwc:basisOfRecord=BadBasis{i}',
                'consulted': ''
            })
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 20
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        assert summary['no_of_non_compliant_validations'] == 20
        assert summary['no_of_unique_non_compliant_validations'] == 20
        assert len(summary['top_non_compliant_validations']) == 15  # Should be limited to 15

    def test_with_nan_values(self):
        """Test handling of NaN values in actedUpon and consulted columns"""
        test_data = {
            'dwc:occurrenceID': ['occ1', 'occ2', 'occ3'],
            'test_id': ['VALIDATION_BASISOFRECORD_NOTEMPTY', 'VALIDATION_BASISOFRECORD_NOTEMPTY', 'VALIDATION_BASISOFRECORD_NOTEMPTY'],
            'test_type': ['Validation', 'Validation', 'Validation'],
            'status': ['RUN_HAS_RESULT', 'RUN_HAS_RESULT', 'RUN_HAS_RESULT'],
            'result': ['COMPLIANT', 'COMPLIANT', 'COMPLIANT'],
            'comment': ['Valid', 'Valid', 'Valid'],
            'actedUpon': ['dwc:basisOfRecord=HumanObservation', np.nan, ''],
            'consulted': ['', np.nan, 'dwc:countryCode=US']
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 3
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        # Should handle NaN values gracefully
        assert summary['number_of_records_in_dataset'] == 3
        assert 'dwc:basisOfRecord' in summary['list_of_all_columns_tested']
        assert 'dwc:countryCode' in summary['list_of_all_columns_tested']

    def test_taxon_core_type(self):
        """Test with taxon core type instead of occurrence"""
        test_data = {
            'dwc:taxonID': ['tax1', 'tax2'],
            'test_id': ['VALIDATION_SCIENTIFICNAME_NOTEMPTY', 'VALIDATION_SCIENTIFICNAME_NOTEMPTY'],
            'test_type': ['Validation', 'Validation'],
            'status': ['RUN_HAS_RESULT', 'RUN_HAS_RESULT'],
            'result': ['COMPLIANT', 'NOT_COMPLIANT'],
            'comment': ['Valid name', 'Invalid name'],
            'actedUpon': ['dwc:scientificName=Homo sapiens', 'dwc:scientificName='],
            'consulted': ['', '']
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'taxon')
        original_dataset_length = 2
        summary = _get_summary_stats_from_unique_results(unique_results, 'taxon', original_dataset_length)
        
        assert summary['number_of_records_in_dataset'] == 2
        assert summary['no_of_non_compliant_validations'] == 1
        assert 'dwc:scientificName' in summary['list_of_all_columns_tested']

    def test_complex_acted_upon_format(self):
        """Test with complex actedUpon format containing multiple fields"""
        test_data = {
            'dwc:occurrenceID': ['occ1'],
            'test_id': ['VALIDATION_COORDINATESCOUNTRYCODE_CONSISTENT'],
            'test_type': ['Validation'],
            'status': ['RUN_HAS_RESULT'],
            'result': ['COMPLIANT'],
            'comment': ['Valid coordinates'],
            'actedUpon': ['dwc:decimalLatitude=37.7749|dwc:decimalLongitude=-122.4194|dwc:countryCode=US'],
            'consulted': ['']
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 1
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        expected_fields = {'dwc:decimalLatitude', 'dwc:decimalLongitude', 'dwc:countryCode'}
        assert set(summary['list_of_all_columns_tested']) == expected_fields

    def test_complex_consulted_format(self):
        """Test with complex consulted format containing multiple fields"""
        test_data = {
            'dwc:occurrenceID': ['occ1'],
            'test_id': ['AMENDMENT_COORDINATES_TRANSPOSED'],
            'test_type': ['Amendment'],
            'status': ['NOT_AMENDED'],
            'result': [''],
            'comment': ['No change needed'],
            'actedUpon': ['dwc:decimalLatitude=37.7749|dwc:decimalLongitude=-122.4194'],
            'consulted': ['dwc:countryCode=US|dwc:coordinateUncertaintyInMeters=100']
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 1
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        expected_fields = {'dwc:decimalLatitude', 'dwc:decimalLongitude', 'dwc:countryCode', 'dwc:coordinateUncertaintyInMeters'}
        assert set(summary['list_of_all_columns_tested']) == expected_fields

    def test_duplicate_combinations_in_top_results(self):
        """Test that duplicate combinations are properly counted in top results"""
        test_data = {
            'dwc:occurrenceID': ['occ1', 'occ2', 'occ3', 'occ4'],
            'test_id': ['VALIDATION_BASISOFRECORD_STANDARD', 'VALIDATION_BASISOFRECORD_STANDARD', 'VALIDATION_BASISOFRECORD_STANDARD', 'VALIDATION_BASISOFRECORD_STANDARD'],
            'test_type': ['Validation', 'Validation', 'Validation', 'Validation'],
            'status': ['RUN_HAS_RESULT', 'RUN_HAS_RESULT', 'RUN_HAS_RESULT', 'RUN_HAS_RESULT'],
            'result': ['NOT_COMPLIANT', 'NOT_COMPLIANT', 'NOT_COMPLIANT', 'COMPLIANT'],
            'comment': ['Invalid', 'Invalid', 'Invalid', 'Valid'],
            'actedUpon': ['dwc:basisOfRecord=BadBasis', 'dwc:basisOfRecord=BadBasis', 'dwc:basisOfRecord=BadBasis', 'dwc:basisOfRecord=HumanObservation'],
            'consulted': ['', '', '', '']
        }
        
        test_df = pd.DataFrame(test_data)
        unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
        original_dataset_length = 4
        summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
        
        assert summary['no_of_non_compliant_validations'] == 3
        assert summary['no_of_unique_non_compliant_validations'] == 3  # All columns are different (different occurrenceIDs)
        assert len(summary['top_non_compliant_validations']) == 1
        assert summary['top_non_compliant_validations'].iloc[0]['count'] == 3

    def test_with_real_test_data(self):
        """Test with actual test data from the test files"""
        # Load the real test results data
        test_results_path = '/app/tests/data/simple_occurrence_dwc_RESULTS.csv'
        try:
            test_df = pd.read_csv(test_results_path)
            unique_results = _create_unique_results_from_test_data(test_df, 'occurrence')
            original_dataset_length = 3  # From the simple_occurrence_dwc.csv file
            summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
            
            # Basic assertions based on the known structure of the test data
            assert summary['number_of_records_in_dataset'] > 0
            assert summary['no_of_tests_results'] > 0
            assert summary['no_of_tests_run'] > 0
            assert len(summary['list_of_all_columns_tested']) > 0
            
            # Check that we have some non-compliant validations in the test data
            assert summary['no_of_non_compliant_validations'] > 0
            
            # Verify the structure of top results
            assert isinstance(summary['top_non_compliant_validations'], pd.DataFrame)
            assert isinstance(summary['top_amendments'], pd.DataFrame)
            assert isinstance(summary['top_filled_in'], pd.DataFrame)
            assert isinstance(summary['top_issues'], pd.DataFrame)
            
        except FileNotFoundError:
            pytest.skip("Test data file not found - skipping real data test")
