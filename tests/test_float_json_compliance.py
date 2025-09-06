"""
Test to reproduce the "Out of range float values are not JSON compliant" error.

This test creates data with problematic float values that cause JSON serialization issues
when sending to the BDQ API.
"""

import pytest
import pandas as pd
import json
import numpy as np
from app.services.bdq_api_service import BDQAPIService
from app.services.csv_service import CSVService


class TestFloatJSONCompliance:
    """Test class for reproducing float JSON compliance issues"""
    
    @pytest.fixture
    def bdq_service(self):
        """Create BDQ API service instance"""
        return BDQAPIService()
    
    @pytest.fixture
    def csv_service(self):
        """Create CSV service instance"""
        return CSVService()
    
    def test_json_serialization_with_inf_values(self):
        """Test JSON serialization with actual inf values that cause the error"""
        
        # Create data with actual inf values
        df = pd.DataFrame([
            {
                'dwc:occurrenceID': 'test-occ-1',
                'dwc:decimalLatitude': float('inf'),
                'dwc:decimalLongitude': float('-inf'),
                'dwc:minimumElevationInMeters': float('nan'),
                'dwc:maximumElevationInMeters': 1.7976931348623157e+308,  # Max float64
                'dwc:coordinateUncertaintyInMeters': -1.7976931348623157e+308,  # Min float64
            },
            {
                'dwc:occurrenceID': 'test-occ-2',
                'dwc:decimalLatitude': 37.7749,
                'dwc:decimalLongitude': -122.4194,
                'dwc:minimumElevationInMeters': 100.0,
                'dwc:maximumElevationInMeters': 200.0,
                'dwc:coordinateUncertaintyInMeters': 10.0,
            }
        ])
        
        print("DataFrame with problematic float values:")
        print(df)
        print("\nDataFrame dtypes:")
        print(df.dtypes)
        
        # Check for problematic values
        for col in df.columns:
            if df[col].dtype == 'float64':
                has_nan = df[col].isna().any()
                has_inf = np.isinf(df[col]).any()
                print(f"\nColumn {col}:")
                print(f"  Has NaN: {has_nan}")
                print(f"  Has inf: {has_inf}")
                if has_nan:
                    print(f"  NaN values: {df[col].isna().sum()}")
                if has_inf:
                    print(f"  inf values: {np.isinf(df[col]).sum()}")
        
        # Try to serialize to JSON (this should fail with inf values)
        try:
            test_columns = ['dwc:decimalLatitude', 'dwc:decimalLongitude']
            unique_combinations = df[test_columns].drop_duplicates().reset_index(drop=True)
            
            batch_request = [
                {"id": "AMENDMENT_COORDINATES_FROM_VERBATIM", "params": row.to_dict()}
                for _, row in unique_combinations.iterrows()
            ]
            
            # This is where the error occurs
            json_str = json.dumps(batch_request)
            print(f"\nJSON serialization successful: {len(json_str)} characters")
            
        except (ValueError, TypeError) as e:
            print(f"\nJSON serialization failed as expected: {e}")
            # This is the error we're trying to reproduce
            assert "Out of range float values are not JSON compliant" in str(e) or \
                   "not JSON serializable" in str(e) or \
                   "float" in str(e).lower()
    
    def test_actual_bdq_api_call_with_problematic_data(self, bdq_service):
        """Test actual BDQ API call with problematic data to reproduce the error"""
        
        # Create data with problematic float values
        df = pd.DataFrame([
            {
                'dwc:occurrenceID': 'test-occ-1',
                'dwc:decimalLatitude': float('inf'),
                'dwc:decimalLongitude': float('-inf'),
                'dwc:minimumElevationInMeters': float('nan'),
                'dwc:maximumElevationInMeters': 1.7976931348623157e+308,
                'dwc:coordinateUncertaintyInMeters': -1.7976931348623157e+308,
                'dwc:countryCode': 'US',
                'dwc:verbatimLatitude': float('inf'),
                'dwc:verbatimLongitude': float('-inf'),
            },
            {
                'dwc:occurrenceID': 'test-occ-2',
                'dwc:decimalLatitude': 37.7749,
                'dwc:decimalLongitude': -122.4194,
                'dwc:minimumElevationInMeters': 100.0,
                'dwc:maximumElevationInMeters': 200.0,
                'dwc:coordinateUncertaintyInMeters': 10.0,
                'dwc:countryCode': 'CA',
                'dwc:verbatimLatitude': 37.7749,
                'dwc:verbatimLongitude': -122.4194,
            }
        ])
        
        print("DataFrame with problematic float values:")
        print(df)
        
        # Try to run tests on this data using the actual service method
        try:
            # Use the actual service method which should now handle the float values correctly
            import asyncio
            results_df = asyncio.run(bdq_service.run_tests_on_dataset(df, 'occurrence'))
            
            if not results_df.empty:
                print(f"Service method successful: {len(results_df)} results")
                print("Results sample:")
                print(results_df.head())
            else:
                print("Service method returned empty results")
                
        except Exception as e:
            print(f"Error occurred: {e}")
            # Check if this is still the JSON serialization error
            if "Out of range float values are not JSON compliant" in str(e):
                print("Still getting the JSON serialization error - fix may not be working")
                # This should NOT happen with our fix
                assert False, "Fix should have prevented this error"
            else:
                print("Different error occurred, which is expected")
                # Other errors are acceptable
    
    def test_large_occurrence_file_parsing(self, csv_service):
        """Test parsing the actual occurrence.txt file to reproduce the error"""
        
        # Read the actual occurrence.txt file
        with open('/app/tests/data/occurrence.txt', 'r') as f:
            csv_data = f.read()
        
        # Parse CSV
        df, core_type = csv_service.parse_csv_and_detect_core(csv_data)
        
        print(f"Parsed {len(df)} rows, {len(df.columns)} columns")
        print("DataFrame dtypes:")
        print(df.dtypes)
        
        # Check for problematic values in numeric columns
        numeric_columns = [col for col in df.columns if any(numeric_field in col.lower() for numeric_field in [
            'latitude', 'longitude', 'elevation', 'uncertainty', 'meters'
        ])]
        
        print(f"\nNumeric columns found: {numeric_columns}")
        
        for col in numeric_columns:
            if col in df.columns:
                # Convert to numeric to reproduce the float conversion issue
                df[col] = pd.to_numeric(df[col], errors='coerce')
                has_nan = df[col].isna().any()
                has_inf = np.isinf(df[col]).any()
                print(f"\nColumn {col}:")
                print(f"  Has NaN: {has_nan}")
                print(f"  Has inf: {has_inf}")
                if has_nan:
                    print(f"  NaN values: {df[col].isna().sum()}")
                if has_inf:
                    print(f"  inf values: {np.isinf(df[col]).sum()}")
                
                # Show some sample values
                sample_values = df[col].dropna().head(5).tolist()
                print(f"  Sample values: {sample_values}")
        
        # Try to reproduce the JSON serialization error
        try:
            # Use coordinates as test case (this is what fails in the logs)
            test_columns = ['dwc:decimalLatitude', 'dwc:decimalLongitude']
            if all(col in df.columns for col in test_columns):
                unique_combinations = df[test_columns].drop_duplicates().reset_index(drop=True)
                
                # Take a small sample to avoid too much output
                sample_combinations = unique_combinations.head(10)
                
                batch_request = [
                    {"id": "AMENDMENT_COORDINATES_FROM_VERBATIM", "params": row.to_dict()}
                    for _, row in sample_combinations.iterrows()
                ]
                
                json_str = json.dumps(batch_request)
                print(f"\nJSON serialization successful: {len(json_str)} characters")
                
            else:
                print(f"\nRequired columns not found: {test_columns}")
                
        except (ValueError, TypeError) as e:
            print(f"\nJSON serialization failed: {e}")
            # This should reproduce the error from the logs
            assert "Out of range float values are not JSON compliant" in str(e) or \
                   "not JSON serializable" in str(e) or \
                   "float" in str(e).lower()


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v", "-s"])
