"""
Test to verify that the merge fix prevents empty test_id records.

This test specifically covers the fix in bdq_api_service.py where we added
dropna(subset=['test_id']) to prevent empty test_id records from being created
during the merge operation.
"""

import pytest
import pandas as pd
from unittest.mock import patch, Mock
from app.services.bdq_api_service import BDQAPIService


class TestMergeFixEmptyTestIds:
    """Test class for the merge fix that prevents empty test_id records"""

    @pytest.fixture
    def bdq_service(self):
        """Create BDQ API service instance"""
        return BDQAPIService()

    @pytest.fixture
    def sample_data_with_duplicates(self):
        """Sample data with duplicate combinations that could cause empty test_id records"""
        return pd.DataFrame([
            {
                'dwc:occurrenceID': 'occ1',
                'dwc:eventDate': '2023-01-01',
                'dwc:countryCode': 'US',
                'dwc:decimalLatitude': '37.7',
                'dwc:decimalLongitude': '-122.4',
            },
            {
                'dwc:occurrenceID': 'occ2',
                'dwc:eventDate': '2023-01-01',  # Same date as occ1
                'dwc:countryCode': 'US',        # Same country as occ1
                'dwc:decimalLatitude': '37.7',  # Same lat as occ1
                'dwc:decimalLongitude': '-122.4', # Same lng as occ1
            },
            {
                'dwc:occurrenceID': 'occ3',
                'dwc:eventDate': '2023-01-01',  # Same date as occ1 and occ2
                'dwc:countryCode': 'US',        # Same country as occ1 and occ2
                'dwc:decimalLatitude': '37.7',  # Same lat as occ1 and occ2
                'dwc:decimalLongitude': '-122.4', # Same lng as occ1 and occ2
            },
            {
                'dwc:occurrenceID': 'occ4',
                'dwc:eventDate': '2023-01-02',  # Different date
                'dwc:countryCode': 'CA',        # Different country
                'dwc:decimalLatitude': '43.6',  # Different lat
                'dwc:decimalLongitude': '-79.3', # Different lng
            }
        ])

    def test_no_empty_test_ids_after_merge(self, bdq_service, sample_data_with_duplicates):
        """Test that the merge operation doesn't create empty test_id records"""
        
        # Mock the API calls to return predictable results
        mock_tests_response = [
            {
                'id': 'VALIDATION_COUNTRYCODE_STANDARD',
                'guid': 'test-guid-1',
                'type': 'Validation',
                'className': 'TestClass',
                'methodName': 'testMethod',
                'actedUpon': ['dwc:countryCode'],
                'consulted': []
            }
        ]
        
        # Mock batch response - will be called with the number of unique combinations
        def mock_batch_handler():
            # Get the batch request from the mock call
            batch_request = mock_post.call_args[1]['json']
            return [
                {
                    'status': 'RUN_HAS_RESULT',
                    'result': 'COMPLIANT',
                    'comment': 'Valid country code'
                }
                for _ in batch_request
            ]
        
        with patch('requests.get') as mock_get, \
             patch('requests.post') as mock_post:
            
            # Mock the tests endpoint
            mock_get.return_value.json.return_value = mock_tests_response
            mock_get.return_value.raise_for_status.return_value = None
            
            # Mock the batch endpoint
            mock_post.return_value.json.side_effect = mock_batch_handler
            mock_post.return_value.raise_for_status.return_value = None
            
            # Run the test
            import asyncio
            results = asyncio.run(bdq_service.run_tests_on_dataset(
                sample_data_with_duplicates, 
                core_type="occurrence"
            ))
            
            # Verify results
            assert len(results) > 0, "Should have results"
            
            # CRITICAL: No empty test_id values
            assert not results['test_id'].isna().any(), "Should not have any NaN test_id values"
            assert not (results['test_id'] == '').any(), "Should not have any empty string test_id values"
            
            # All test_id values should be the same (since we only mocked one test)
            unique_test_ids = results['test_id'].unique()
            assert len(unique_test_ids) == 1, f"Should have exactly one unique test_id, got: {unique_test_ids}"
            assert unique_test_ids[0] == 'VALIDATION_COUNTRYCODE_STANDARD', "Should have the correct test_id"
            
            # Should have results for all occurrence records
            unique_occurrence_ids = set(results['dwc:occurrenceID'].unique())
            expected_occurrence_ids = set(sample_data_with_duplicates['dwc:occurrenceID'].unique())
            assert unique_occurrence_ids == expected_occurrence_ids, "Should have results for all occurrence records"
            
            print(f"✓ Successfully processed {len(results)} results with no empty test_id values")
            print(f"  - All test_ids: {unique_test_ids}")
            print(f"  - Occurrence IDs: {sorted(unique_occurrence_ids)}")

    def test_merge_behavior_with_multiple_tests(self, bdq_service, sample_data_with_duplicates):
        """Test merge behavior when multiple tests are applicable"""
        
        # Mock multiple tests
        mock_tests_response = [
            {
                'id': 'VALIDATION_COUNTRYCODE_STANDARD',
                'guid': 'test-guid-1',
                'type': 'Validation',
                'className': 'TestClass',
                'methodName': 'testMethod',
                'actedUpon': ['dwc:countryCode'],
                'consulted': []
            },
            {
                'id': 'VALIDATION_EVENTDATE_STANDARD',
                'guid': 'test-guid-2',
                'type': 'Validation',
                'className': 'TestClass',
                'methodName': 'testMethod',
                'actedUpon': ['dwc:eventDate'],
                'consulted': []
            }
        ]
        
        # Mock batch response - will be called with the number of unique combinations
        def mock_batch_handler():
            # Get the batch request from the mock call
            batch_request = mock_post.call_args[1]['json']
            return [
                {
                    'status': 'RUN_HAS_RESULT',
                    'result': 'COMPLIANT',
                    'comment': 'Valid value'
                }
                for _ in batch_request
            ]
        
        with patch('requests.get') as mock_get, \
             patch('requests.post') as mock_post:
            
            # Mock the tests endpoint
            mock_get.return_value.json.return_value = mock_tests_response
            mock_get.return_value.raise_for_status.return_value = None
            
            # Mock the batch endpoint
            mock_post.return_value.json.side_effect = mock_batch_handler
            mock_post.return_value.raise_for_status.return_value = None
            
            # Run the test
            import asyncio
            results = asyncio.run(bdq_service.run_tests_on_dataset(
                sample_data_with_duplicates, 
                core_type="occurrence"
            ))
            
            # Verify results
            assert len(results) > 0, "Should have results"
            
            # CRITICAL: No empty test_id values
            assert not results['test_id'].isna().any(), "Should not have any NaN test_id values"
            assert not (results['test_id'] == '').any(), "Should not have any empty string test_id values"
            
            # Should have results for both tests
            unique_test_ids = set(results['test_id'].unique())
            expected_test_ids = {'VALIDATION_COUNTRYCODE_STANDARD', 'VALIDATION_EVENTDATE_STANDARD'}
            assert unique_test_ids == expected_test_ids, f"Should have both test_ids, got: {unique_test_ids}"
            
            # Should have results for all occurrence records for each test
            for test_id in expected_test_ids:
                test_results = results[results['test_id'] == test_id]
                unique_occurrence_ids = set(test_results['dwc:occurrenceID'].unique())
                expected_occurrence_ids = set(sample_data_with_duplicates['dwc:occurrenceID'].unique())
                assert unique_occurrence_ids == expected_occurrence_ids, f"Test {test_id} should have results for all occurrence records"
            
            print(f"✓ Successfully processed {len(results)} results across {len(unique_test_ids)} tests")
            print(f"  - Test IDs: {sorted(unique_test_ids)}")
            print(f"  - Results per test: {dict(results['test_id'].value_counts())}")

    def test_merge_behavior_with_no_matching_combinations(self, bdq_service):
        """Test merge behavior when there are no matching combinations (edge case)"""
        
        # Create data that won't match any test requirements
        sample_data = pd.DataFrame([
            {
                'dwc:occurrenceID': 'occ1',
                'dwc:someField': 'someValue',  # Field that no test will use
            }
        ])
        
        # Mock a test that requires a different field
        mock_tests_response = [
            {
                'id': 'VALIDATION_COUNTRYCODE_STANDARD',
                'guid': 'test-guid-1',
                'type': 'Validation',
                'className': 'TestClass',
                'methodName': 'testMethod',
                'actedUpon': ['dwc:countryCode'],  # This field is not in our data
                'consulted': []
            }
        ]
        
        with patch('requests.get') as mock_get:
            # Mock the tests endpoint
            mock_get.return_value.json.return_value = mock_tests_response
            mock_get.return_value.raise_for_status.return_value = None
            
            # Run the test
            import asyncio
            results = asyncio.run(bdq_service.run_tests_on_dataset(
                sample_data, 
                core_type="occurrence"
            ))
            
            # Should return empty results (no applicable tests)
            assert len(results) == 0, "Should return empty results when no tests are applicable"
            
            print("✓ Correctly handled case with no applicable tests")

    def test_merge_behavior_with_api_errors(self, bdq_service, sample_data_with_duplicates):
        """Test merge behavior when API returns errors"""
        
        # Mock tests response
        mock_tests_response = [
            {
                'id': 'VALIDATION_COUNTRYCODE_STANDARD',
                'guid': 'test-guid-1',
                'type': 'Validation',
                'className': 'TestClass',
                'methodName': 'testMethod',
                'actedUpon': ['dwc:countryCode'],
                'consulted': []
            }
        ]
        
        # Mock batch response with some errors - will be called with the number of unique combinations
        def mock_batch_handler():
            # Get the batch request from the mock call
            batch_request = mock_post.call_args[1]['json']
            return [
                {
                    'status': 'INTERNAL_PREREQUISITES_NOT_MET',
                    'result': '',
                    'comment': 'Test failed due to missing prerequisites'
                }
                for _ in batch_request
            ]
        
        with patch('requests.get') as mock_get, \
             patch('requests.post') as mock_post:
            
            # Mock the tests endpoint
            mock_get.return_value.json.return_value = mock_tests_response
            mock_get.return_value.raise_for_status.return_value = None
            
            # Mock the batch endpoint
            mock_post.return_value.json.side_effect = mock_batch_handler
            mock_post.return_value.raise_for_status.return_value = None
            
            # Run the test
            import asyncio
            results = asyncio.run(bdq_service.run_tests_on_dataset(
                sample_data_with_duplicates, 
                core_type="occurrence"
            ))
            
            # Verify results
            assert len(results) > 0, "Should have results even with API errors"
            
            # CRITICAL: No empty test_id values even with errors
            assert not results['test_id'].isna().any(), "Should not have any NaN test_id values"
            assert not (results['test_id'] == '').any(), "Should not have any empty string test_id values"
            
            # Should have the correct test_id
            unique_test_ids = results['test_id'].unique()
            assert len(unique_test_ids) == 1, f"Should have exactly one unique test_id, got: {unique_test_ids}"
            assert unique_test_ids[0] == 'VALIDATION_COUNTRYCODE_STANDARD', "Should have the correct test_id"
            
            # Should have error status
            unique_statuses = results['status'].unique()
            assert 'INTERNAL_PREREQUISITES_NOT_MET' in unique_statuses, "Should have error status"
            
            print(f"✓ Successfully handled API errors with {len(results)} results")
            print(f"  - Test ID: {unique_test_ids[0]}")
            print(f"  - Statuses: {unique_statuses}")


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v", "-s"])
