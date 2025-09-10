import pytest
import pandas as pd
from io import StringIO
from unittest.mock import Mock, patch
from app.services.bdq_api_service import BDQAPIService, BDQTest


class TestPreAmendedValue:
    """Test the pre_amended_value column functionality"""
    
    @patch('app.services.bdq_api_service.requests.post')
    @patch('app.services.bdq_api_service.requests.get')
    def test_pre_amended_value_for_amended_status(self, mock_get, mock_post):
        """Test that pre_amended_value is populated for AMENDED status amendments"""
        # Mock the API responses
        mock_get.return_value.json.return_value = [
            {
                'id': 'AMENDMENT_COUNTRYCODE_STANDARDIZED',
                'guid': 'test-guid',
                'type': 'Amendment',
                'className': 'TestClass',
                'methodName': 'testMethod',
                'actedUpon': ['dwc:countryCode'],
                'consulted': []
            }
        ]
        mock_get.return_value.raise_for_status = Mock()
        
        # Mock batch response with AMENDED status
        mock_post.return_value.json.return_value = [
            {
                'status': 'AMENDED',
                'result': 'dwc:countryCode=US',
                'comment': 'Standardized country code'
            }
        ]
        mock_post.return_value.raise_for_status = Mock()
        
        # Create test data
        test_df = pd.DataFrame([
            {'dwc:occurrenceID': 'occ1', 'dwc:countryCode': 'United States'}
        ])
        
        # Run the service
        service = BDQAPIService()
        import asyncio
        results = asyncio.run(service.run_tests_on_dataset(test_df, 'occurrence'))
        
        # Verify the results
        assert 'pre_amended_value' in results.columns
        assert len(results) == 1
        
        result_row = results.iloc[0]
        assert result_row['status'] == 'AMENDED'
        assert result_row['pre_amended_value'] == 'dwc:countryCode=United States'  # Original value
        assert result_row['actedUpon'] == 'dwc:countryCode=United States'  # Current value (same as original in this case)
    
    @patch('app.services.bdq_api_service.requests.post')
    @patch('app.services.bdq_api_service.requests.get')
    def test_pre_amended_value_for_filled_in_status(self, mock_get, mock_post):
        """Test that pre_amended_value is populated for FILLED_IN status amendments"""
        # Mock the API responses
        mock_get.return_value.json.return_value = [
            {
                'id': 'AMENDMENT_COORDINATES_FROM_VERBATIM',
                'guid': 'test-guid',
                'type': 'Amendment',
                'className': 'TestClass',
                'methodName': 'testMethod',
                'actedUpon': ['dwc:decimalLatitude', 'dwc:decimalLongitude'],
                'consulted': ['dwc:verbatimLatitude', 'dwc:verbatimLongitude']
            }
        ]
        mock_get.return_value.raise_for_status = Mock()
        
        # Mock batch response with FILLED_IN status
        mock_post.return_value.json.return_value = [
            {
                'status': 'FILLED_IN',
                'result': 'dwc:decimalLatitude=-41.0526|dwc:decimalLongitude=-71.5311',
                'comment': 'Converted from verbatim'
            }
        ]
        mock_post.return_value.raise_for_status = Mock()
        
        # Create test data with empty coordinates but verbatim data
        test_df = pd.DataFrame([
            {
                'dwc:occurrenceID': 'occ1', 
                'dwc:decimalLatitude': '', 
                'dwc:decimalLongitude': '',
                'dwc:verbatimLatitude': '-41.0526',
                'dwc:verbatimLongitude': '-71.5311'
            }
        ])
        
        # Run the service
        service = BDQAPIService()
        import asyncio
        results = asyncio.run(service.run_tests_on_dataset(test_df, 'occurrence'))
        
        # Verify the results
        assert 'pre_amended_value' in results.columns
        assert len(results) == 1
        
        result_row = results.iloc[0]
        assert result_row['status'] == 'FILLED_IN'
        assert result_row['pre_amended_value'] == 'dwc:decimalLatitude=|dwc:decimalLongitude='  # Original empty values
        assert result_row['actedUpon'] == 'dwc:decimalLatitude=|dwc:decimalLongitude='  # Current values (still empty)
    
    @patch('app.services.bdq_api_service.requests.post')
    @patch('app.services.bdq_api_service.requests.get')
    def test_pre_amended_value_for_not_amended_status(self, mock_get, mock_post):
        """Test that pre_amended_value is blank for NOT_AMENDED status"""
        # Mock the API responses
        mock_get.return_value.json.return_value = [
            {
                'id': 'AMENDMENT_COUNTRYCODE_STANDARDIZED',
                'guid': 'test-guid',
                'type': 'Amendment',
                'className': 'TestClass',
                'methodName': 'testMethod',
                'actedUpon': ['dwc:countryCode'],
                'consulted': []
            }
        ]
        mock_get.return_value.raise_for_status = Mock()
        
        # Mock batch response with NOT_AMENDED status
        mock_post.return_value.json.return_value = [
            {
                'status': 'NOT_AMENDED',
                'result': '',
                'comment': 'Country code already standardized'
            }
        ]
        mock_post.return_value.raise_for_status = Mock()
        
        # Create test data
        test_df = pd.DataFrame([
            {'dwc:occurrenceID': 'occ1', 'dwc:countryCode': 'US'}
        ])
        
        # Run the service
        service = BDQAPIService()
        import asyncio
        results = asyncio.run(service.run_tests_on_dataset(test_df, 'occurrence'))
        
        # Verify the results
        assert 'pre_amended_value' in results.columns
        assert len(results) == 1
        
        result_row = results.iloc[0]
        assert result_row['status'] == 'NOT_AMENDED'
        assert result_row['pre_amended_value'] == ''  # Should be blank
    
    @patch('app.services.bdq_api_service.requests.post')
    @patch('app.services.bdq_api_service.requests.get')
    def test_pre_amended_value_for_validation_tests(self, mock_get, mock_post):
        """Test that pre_amended_value is blank for validation tests"""
        # Mock the API responses
        mock_get.return_value.json.return_value = [
            {
                'id': 'VALIDATION_COUNTRYCODE_STANDARD',
                'guid': 'test-guid',
                'type': 'Validation',
                'className': 'TestClass',
                'methodName': 'testMethod',
                'actedUpon': ['dwc:countryCode'],
                'consulted': []
            }
        ]
        mock_get.return_value.raise_for_status = Mock()
        
        # Mock batch response with validation result
        mock_post.return_value.json.return_value = [
            {
                'status': 'RUN_HAS_RESULT',
                'result': 'NOT_COMPLIANT',
                'comment': 'Invalid country code format'
            }
        ]
        mock_post.return_value.raise_for_status = Mock()
        
        # Create test data
        test_df = pd.DataFrame([
            {'dwc:occurrenceID': 'occ1', 'dwc:countryCode': 'XX'}
        ])
        
        # Run the service
        service = BDQAPIService()
        import asyncio
        results = asyncio.run(service.run_tests_on_dataset(test_df, 'occurrence'))
        
        # Verify the results
        assert 'pre_amended_value' in results.columns
        assert len(results) == 1
        
        result_row = results.iloc[0]
        assert result_row['test_type'] == 'Validation'
        assert result_row['pre_amended_value'] == ''  # Should be blank for validations


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
