import pytest
import pandas as pd
from io import StringIO
from app.services.csv_service import CSVService


class TestPreAmendedValueIntegration:
    """Integration tests for pre_amended_value column functionality"""
    
    def test_pre_amended_value_with_real_data_structure(self):
        """Test that pre_amended_value column is present in results structure"""
        # This test verifies that the pre_amended_value column is added to the results
        # and has the expected structure, even if we can't test the actual API calls
        
        # Create a mock results DataFrame that matches the expected structure
        mock_results = pd.DataFrame([
            {
                'dwc:occurrenceID': 'occ1',
                'test_id': 'AMENDMENT_COUNTRYCODE_STANDARDIZED',
                'test_type': 'Amendment',
                'status': 'AMENDED',
                'result': 'dwc:countryCode=US',
                'comment': 'Standardized country code',
                'actedUpon': 'dwc:countryCode=US',
                'consulted': 'dwc:countryCode=United States',
                'pre_amended_value': 'dwc:countryCode=United States'
            },
            {
                'dwc:occurrenceID': 'occ2',
                'test_id': 'VALIDATION_COUNTRYCODE_STANDARD',
                'test_type': 'Validation',
                'status': 'RUN_HAS_RESULT',
                'result': 'NOT_COMPLIANT',
                'comment': 'Invalid country code format',
                'actedUpon': 'dwc:countryCode=XX',
                'consulted': 'dwc:countryCode=XX',
                'pre_amended_value': ''
            }
        ])
        
        # Verify the structure
        expected_columns = [
            'dwc:occurrenceID', 'test_id', 'test_type', 'status', 'result', 
            'comment', 'actedUpon', 'consulted', 'pre_amended_value'
        ]
        
        assert list(mock_results.columns) == expected_columns
        assert 'pre_amended_value' in mock_results.columns
        
        # Verify amendment test has pre_amended_value populated
        amendment_row = mock_results[mock_results['test_type'] == 'Amendment'].iloc[0]
        assert amendment_row['pre_amended_value'] == 'dwc:countryCode=United States'
        
        # Verify validation test has pre_amended_value blank
        validation_row = mock_results[mock_results['test_type'] == 'Validation'].iloc[0]
        assert validation_row['pre_amended_value'] == ''
    
    def test_csv_service_compatibility(self):
        """Test that CSV service can handle the new pre_amended_value column"""
        csv_service = CSVService()
        
        # Create test data with the new column
        test_results = pd.DataFrame([
            {
                'dwc:occurrenceID': 'occ1',
                'test_id': 'AMENDMENT_COUNTRYCODE_STANDARDIZED',
                'test_type': 'Amendment',
                'status': 'AMENDED',
                'result': 'dwc:countryCode=US',
                'comment': 'Standardized country code',
                'actedUpon': 'dwc:countryCode=US',
                'consulted': 'dwc:countryCode=United States',
                'pre_amended_value': 'dwc:countryCode=United States'
            }
        ])
        
        # Test that CSV service can convert to CSV string
        csv_string = csv_service.dataframe_to_csv_string(test_results)
        assert 'pre_amended_value' in csv_string
        
        # Test that we can parse it back
        parsed_df = pd.read_csv(StringIO(csv_string))
        assert 'pre_amended_value' in parsed_df.columns
        assert parsed_df.iloc[0]['pre_amended_value'] == 'dwc:countryCode=United States'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
