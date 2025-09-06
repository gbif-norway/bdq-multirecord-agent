"""
Integration tests for the actual BDQ API.

These tests verify that the live BDQ API is working correctly and returning
the expected data format that our application depends on.
"""

import pytest
import pandas as pd
import requests
import json
from typing import List, Dict, Any
from app.services.bdq_api_service import BDQAPIService, BDQTest


class TestBDQAPIIntegration:
    """Test class for BDQ API integration tests"""
    
    @pytest.fixture
    def bdq_service(self):
        """Create BDQ API service instance"""
        return BDQAPIService()
    
    @pytest.fixture
    def sample_occurrence_data(self):
        """Sample occurrence data for testing"""
        return pd.DataFrame([
            {
                'occurrenceID': 'test-occ-1',
                'dwc:eventDate': '2023-01-01',
                'dwc:countryCode': 'US',
                'dwc:decimalLatitude': '37.7749',
                'dwc:decimalLongitude': '-122.4194',
                'dwc:scientificName': 'Homo sapiens',
                'dwc:basisOfRecord': 'HumanObservation'
            },
            {
                'occurrenceID': 'test-occ-2', 
                'dwc:eventDate': '2023-01-02',
                'dwc:countryCode': 'CA',
                'dwc:decimalLatitude': '43.6532',
                'dwc:decimalLongitude': '-79.3832',
                'dwc:scientificName': 'Canis lupus',
                'dwc:basisOfRecord': 'HumanObservation'
            },
            {
                'occurrenceID': 'test-occ-3',
                'dwc:eventDate': 'invalid-date',
                'dwc:countryCode': 'ZZ',  # Invalid country code
                'dwc:decimalLatitude': '91.0',  # Invalid latitude
                'dwc:decimalLongitude': '181.0',  # Invalid longitude
                'dwc:scientificName': 'InvalidName',
                'dwc:basisOfRecord': 'BadBasis'
            }
        ])
    
    @pytest.fixture
    def sample_taxon_data(self):
        """Sample taxon data for testing"""
        return pd.DataFrame([
            {
                'taxonID': 'test-tax-1',
                'dwc:scientificName': 'Homo sapiens',
                'dwc:kingdom': 'Animalia',
                'dwc:phylum': 'Chordata',
                'dwc:class': 'Mammalia',
                'dwc:order': 'Primates',
                'dwc:family': 'Hominidae',
                'dwc:genus': 'Homo',
                'dwc:specificEpithet': 'sapiens',
                'dwc:taxonRank': 'species',
                'dwc:scientificNameAuthorship': 'Linnaeus 1758'
            },
            {
                'taxonID': 'test-tax-2',
                'dwc:scientificName': 'InvalidTaxon',
                'dwc:kingdom': 'BadKingdom',
                'dwc:phylum': '',
                'dwc:class': '',
                'dwc:order': '',
                'dwc:family': '',
                'dwc:genus': '',
                'dwc:specificEpithet': '',
                'dwc:taxonRank': 'species',
                'dwc:scientificNameAuthorship': 'AuthorName'
            }
        ])

    def test_api_health_check(self, bdq_service):
        """Test that the BDQ API is accessible and responding"""
        try:
            response = requests.get(bdq_service.tests_endpoint, timeout=10)
            assert response.status_code == 200, f"API returned status {response.status_code}"
            
            data = response.json()
            assert isinstance(data, list), "API should return a list of tests"
            assert len(data) > 0, "API should return at least one test"
            
            print(f"✓ API is healthy - found {len(data)} tests available")
            
        except requests.exceptions.RequestException as e:
            pytest.fail(f"API health check failed: {e}")

    def test_tests_endpoint_structure(self, bdq_service):
        """Test that the tests endpoint returns properly structured data"""
        response = requests.get(bdq_service.tests_endpoint, timeout=10)
        response.raise_for_status()
        
        tests_data = response.json()
        assert isinstance(tests_data, list), "Tests endpoint should return a list"
        
        # Check structure of first few tests
        for i, test_data in enumerate(tests_data[:5]):  # Check first 5 tests
            assert isinstance(test_data, dict), f"Test {i} should be a dictionary"
            
            # Check required fields
            required_fields = ['id', 'guid', 'type', 'className', 'methodName', 'actedUpon']
            for field in required_fields:
                assert field in test_data, f"Test {i} missing required field: {field}"
            
            # Check field types
            assert isinstance(test_data['id'], str), f"Test {i} id should be string"
            assert isinstance(test_data['type'], str), f"Test {i} type should be string"
            assert test_data['type'] in ['Validation', 'Amendment'], f"Test {i} type should be Validation or Amendment"
            assert isinstance(test_data['actedUpon'], list), f"Test {i} actedUpon should be list"
            
            print(f"✓ Test {i}: {test_data['id']} ({test_data['type']}) - acts on {test_data['actedUpon']}")

    def test_batch_endpoint_structure(self, bdq_service):
        """Test that the batch endpoint accepts and returns properly structured data"""
        # First get available tests
        tests_response = requests.get(bdq_service.tests_endpoint, timeout=10)
        tests_response.raise_for_status()
        tests_data = tests_response.json()
        
        # Find a simple test to use
        simple_test = None
        for test in tests_data:
            if test['type'] == 'Validation' and len(test['actedUpon']) == 1:
                simple_test = test
                break
        
        if not simple_test:
            pytest.skip("No simple validation test found for batch testing")
        
        # Prepare batch request
        batch_request = [
            {
                "id": simple_test['id'],
                "params": {f"dwc:{field}": "test_value" for field in simple_test['actedUpon']}
            }
        ]
        
        # Make batch request
        response = requests.post(bdq_service.batch_endpoint, json=batch_request, timeout=30)
        assert response.status_code == 200, f"Batch endpoint returned status {response.status_code}"
        
        batch_results = response.json()
        assert isinstance(batch_results, list), "Batch endpoint should return a list"
        assert len(batch_results) == len(batch_request), "Should return same number of results as requests"
        
        # Check result structure
        result = batch_results[0]
        assert isinstance(result, dict), "Each result should be a dictionary"
        
        required_result_fields = ['status', 'result', 'comment']
        for field in required_result_fields:
            assert field in result, f"Result missing required field: {field}"
        
        print(f"✓ Batch test successful: {simple_test['id']} -> {result['status']}")

    def test_occurrence_data_processing(self, bdq_service, sample_occurrence_data):
        """Test processing occurrence data through the actual API"""
        try:
            # Get applicable tests for occurrence data
            applicable_tests = bdq_service._filter_applicable_tests(sample_occurrence_data.columns.tolist())
            assert len(applicable_tests) > 0, "Should find applicable tests for occurrence data"
            
            print(f"✓ Found {len(applicable_tests)} applicable tests for occurrence data")
            
            # Test a few specific tests
            test_results = []
            for test in applicable_tests[:3]:  # Test first 3 applicable tests
                print(f"Testing: {test.id} ({test.type})")
                
                # Get unique combinations for this test
                test_columns = test.actedUpon + test.consulted
                unique_combinations = sample_occurrence_data[test_columns].drop_duplicates().reset_index(drop=True)
                
                # Prepare batch request
                batch_request = [
                    {"id": test.id, "params": row.to_dict()}
                    for _, row in unique_combinations.iterrows()
                ]
                
                # Make API call
                response = requests.post(bdq_service.batch_endpoint, json=batch_request, timeout=30)
                response.raise_for_status()
                
                results = response.json()
                assert len(results) == len(batch_request), f"Result count mismatch for {test.id}"
                
                # Validate result structure
                for i, result in enumerate(results):
                    assert 'status' in result, f"Result {i} missing status"
                    assert 'result' in result, f"Result {i} missing result"
                    assert 'comment' in result, f"Result {i} missing comment"
                    
                    # Check status values
                    valid_statuses = ['RUN_HAS_RESULT', 'AMENDED', 'NOT_AMENDED', 'INTERNAL_PREREQUISITES_NOT_MET']
                    assert result['status'] in valid_statuses, f"Invalid status: {result['status']}"
                
                test_results.extend(results)
                print(f"  ✓ {test.id}: {len(results)} results")
            
            print(f"✓ Successfully processed {len(test_results)} test results")
            
        except Exception as e:
            pytest.fail(f"Occurrence data processing failed: {e}")

    def test_taxon_data_processing(self, bdq_service, sample_taxon_data):
        """Test processing taxon data through the actual API"""
        try:
            # Get applicable tests for taxon data
            applicable_tests = bdq_service._filter_applicable_tests(sample_taxon_data.columns.tolist())
            
            if len(applicable_tests) == 0:
                print("ℹ No applicable tests found for taxon data - this may be expected")
                return
            
            print(f"✓ Found {len(applicable_tests)} applicable tests for taxon data")
            
            # Test a few specific tests
            for test in applicable_tests[:2]:  # Test first 2 applicable tests
                print(f"Testing: {test.id} ({test.type})")
                
                # Get unique combinations for this test
                test_columns = test.actedUpon + test.consulted
                
                # Check if all required columns exist in our test data
                missing_columns = [col for col in test_columns if col not in sample_taxon_data.columns]
                if missing_columns:
                    print(f"  ⚠ Skipping {test.id} - missing columns: {missing_columns}")
                    continue
                
                unique_combinations = sample_taxon_data[test_columns].drop_duplicates().reset_index(drop=True)
                
                # Prepare batch request
                batch_request = [
                    {"id": test.id, "params": row.to_dict()}
                    for _, row in unique_combinations.iterrows()
                ]
                
                # Make API call
                response = requests.post(bdq_service.batch_endpoint, json=batch_request, timeout=30)
                response.raise_for_status()
                
                results = response.json()
                assert len(results) == len(batch_request), f"Result count mismatch for {test.id}"
                
                print(f"  ✓ {test.id}: {len(results)} results")
            
        except Exception as e:
            pytest.fail(f"Taxon data processing failed: {e}")

    def test_validation_vs_amendment_results(self, bdq_service, sample_occurrence_data):
        """Test that validation and amendment tests return different result formats"""
        applicable_tests = bdq_service._filter_applicable_tests(sample_occurrence_data.columns.tolist())
        
        validation_tests = [t for t in applicable_tests if t.type == 'Validation']
        amendment_tests = [t for t in applicable_tests if t.type == 'Amendment']
        
        if not validation_tests or not amendment_tests:
            pytest.skip("Need both validation and amendment tests to compare formats")
        
        # Test validation result format
        val_test = validation_tests[0]
        test_columns = val_test.actedUpon + val_test.consulted
        unique_combinations = sample_occurrence_data[test_columns].drop_duplicates().reset_index(drop=True)
        
        batch_request = [{"id": val_test.id, "params": row.to_dict()} for _, row in unique_combinations.iterrows()]
        response = requests.post(bdq_service.batch_endpoint, json=batch_request, timeout=30)
        response.raise_for_status()
        val_results = response.json()
        
        # Test amendment result format
        amend_test = amendment_tests[0]
        test_columns = amend_test.actedUpon + amend_test.consulted
        unique_combinations = sample_occurrence_data[test_columns].drop_duplicates().reset_index(drop=True)
        
        batch_request = [{"id": amend_test.id, "params": row.to_dict()} for _, row in unique_combinations.iterrows()]
        response = requests.post(bdq_service.batch_endpoint, json=batch_request, timeout=30)
        response.raise_for_status()
        amend_results = response.json()
        
        # Compare result formats
        for val_result in val_results:
            if val_result['status'] == 'RUN_HAS_RESULT':
                assert val_result['result'] in ['COMPLIANT', 'NOT_COMPLIANT'], \
                    f"Validation result should be COMPLIANT or NOT_COMPLIANT, got: {val_result['result']}"
        
        for amend_result in amend_results:
            if amend_result['status'] == 'AMENDED':
                assert 'dwc:' in amend_result['result'], \
                    f"Amendment result should contain 'dwc:' field, got: {amend_result['result']}"
            elif amend_result['status'] == 'NOT_AMENDED':
                # NOT_AMENDED means the amendment test ran but didn't make changes
                assert amend_result['result'] == '', \
                    f"NOT_AMENDED should have empty result, got: {amend_result['result']}"
        
        print(f"✓ Validation test {val_test.id}: {len(val_results)} results")
        print(f"✓ Amendment test {amend_test.id}: {len(amend_results)} results")

    def test_error_handling(self, bdq_service):
        """Test API error handling with invalid requests"""
        # Test with invalid test ID
        invalid_batch_request = [
            {"id": "INVALID_TEST_ID", "params": {"dwc:countryCode": "US"}}
        ]
        
        response = requests.post(bdq_service.batch_endpoint, json=invalid_batch_request, timeout=30)
        assert response.status_code == 200, "API should return 200 even for invalid test IDs"
        
        results = response.json()
        assert len(results) == 1, "Should return one result for one request"
        
        result = results[0]
        assert result['status'] == 'INTERNAL_PREREQUISITES_NOT_MET', \
            f"Invalid test should return INTERNAL_PREREQUISITES_NOT_MET, got: {result['status']}"
        assert 'Unknown test id' in result['comment'], \
            f"Error comment should mention unknown test, got: {result['comment']}"
        
        print("✓ Error handling works correctly for invalid test IDs")

    def test_api_performance(self, bdq_service, sample_occurrence_data):
        """Test API performance with reasonable load"""
        import time
        
        applicable_tests = bdq_service._filter_applicable_tests(sample_occurrence_data.columns.tolist())
        
        if not applicable_tests:
            pytest.skip("No applicable tests found for performance testing")
        
        # Test with a simple test
        test = applicable_tests[0]
        test_columns = test.actedUpon + test.consulted
        unique_combinations = sample_occurrence_data[test_columns].drop_duplicates().reset_index(drop=True)
        
        # Create a batch with multiple requests
        batch_request = [
            {"id": test.id, "params": row.to_dict()}
            for _, row in unique_combinations.iterrows()
        ]
        
        start_time = time.time()
        response = requests.post(bdq_service.batch_endpoint, json=batch_request, timeout=60)
        end_time = time.time()
        
        response.raise_for_status()
        results = response.json()
        
        duration = end_time - start_time
        requests_per_second = len(batch_request) / duration if duration > 0 else 0
        
        print(f"✓ Performance test: {len(batch_request)} requests in {duration:.2f}s ({requests_per_second:.2f} req/s)")
        
        # Basic performance expectations
        assert duration < 30, f"API should respond within 30 seconds, took {duration:.2f}s"
        assert len(results) == len(batch_request), "Should return all results"


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v", "-s"])
