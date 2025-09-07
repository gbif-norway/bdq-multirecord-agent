"""
Test BDQ API service filtering logic to ensure tests are properly filtered based on available columns.
"""

import pytest
import json
import pandas as pd
from unittest.mock import patch, MagicMock
from app.services.bdq_api_service import BDQAPIService, BDQTest


class TestBDQFilteringLogic:
    """Test that BDQ API service correctly filters tests based on available columns"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.bdq_service = BDQAPIService()
        
        # Load the actual BDQ tests list
        with open('tests/data/bdq_api_tests_list.json', 'r') as f:
            self.bdq_tests_data = json.load(f)
        
        # Create occurrence data columns (what's actually in occurrence.txt)
        self.occurrence_columns = [
            'dwc:id', 'dwc:modified', 'dwc:license', 'dwc:institutionID', 'dwc:institutionCode',
            'dwc:datasetName', 'dwc:basisOfRecord', 'dwc:dynamicProperties', 'dwc:occurrenceID',
            'dwc:recordedBy', 'dwc:associatedReferences', 'dwc:organismID', 'dwc:eventID',
            'dwc:parentEventID', 'dwc:year', 'dwc:month', 'dwc:samplingProtocol', 'dwc:eventRemarks',
            'dwc:country', 'dwc:countryCode', 'dwc:stateProvince', 'dwc:locality',
            'dwc:minimumElevationInMeters', 'dwc:maximumElevationInMeters', 'dwc:verbatimElevation',
            'dwc:decimalLatitude', 'dwc:decimalLongitude', 'dwc:geodeticDatum',
            'dwc:coordinateUncertaintyInMeters', 'dwc:verbatimCoordinates', 'dwc:verbatimLatitude',
            'dwc:verbatimLongitude', 'dwc:verbatimCoordinateSystem', 'dwc:verbatimSRS',
            'dwc:georeferencedBy', 'dwc:scientificName', 'dwc:kingdom', 'dwc:phylum', 'dwc:class',
            'dwc:order', 'dwc:family', 'dwc:genus', 'dwc:specificEpithet', 'dwc:infraspecificEpithet',
            'dwc:taxonRank', 'dwc:verbatimTaxonRank', 'dwc:scientificNameAuthorship', 'dwc:vernacularName'
        ]
    
    @patch('requests.get')
    def test_filter_applicable_tests_with_occurrence_data(self, mock_get):
        """Test that filtering works correctly with occurrence data columns"""
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = self.bdq_tests_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Test the filtering
        applicable_tests = self.bdq_service._filter_applicable_tests(self.occurrence_columns)
        
        # Verify that the problematic test is NOT included
        test_ids = [test.id for test in applicable_tests]
        assert 'AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID' not in test_ids, \
            "AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID should be filtered out because dwc:scientificNameID is not available"
        
        # Verify that some valid tests ARE included
        assert 'VALIDATION_COUNTRYCODE_STANDARD' in test_ids, \
            "VALIDATION_COUNTRYCODE_STANDARD should be included because dwc:countryCode is available"
        
        assert 'VALIDATION_DECIMALLATITUDE_INRANGE' in test_ids, \
            "VALIDATION_DECIMALLATITUDE_INRANGE should be included because dwc:decimalLatitude is available"
        
        assert 'VALIDATION_SCIENTIFICNAME_NOTEMPTY' in test_ids, \
            "VALIDATION_SCIENTIFICNAME_NOTEMPTY should be included because dwc:scientificName is available"
        
        print(f"✓ Found {len(applicable_tests)} applicable tests out of {len(self.bdq_tests_data)} total tests")
        print(f"✓ Correctly filtered out AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID")
    
    @patch('requests.get')
    def test_filter_applicable_tests_checks_consulted_columns(self, mock_get):
        """Test that filtering checks both actedUpon AND consulted columns"""
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = self.bdq_tests_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Test with occurrence columns (missing dwc:scientificNameID)
        applicable_tests = self.bdq_service._filter_applicable_tests(self.occurrence_columns)
        
        # Find the problematic test in the full list
        scientific_name_test = None
        for test_data in self.bdq_tests_data:
            if test_data['id'] == 'AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID':
                scientific_name_test = BDQTest(**test_data)
                break
        
        assert scientific_name_test is not None, "Should find the AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID test"
        
        # Verify it requires dwc:scientificNameID in consulted columns
        assert 'dwc:scientificNameID' in scientific_name_test.consulted, \
            "AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID should require dwc:scientificNameID in consulted columns"
        
        # Verify it's not in the applicable tests
        assert scientific_name_test not in applicable_tests, \
            "AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID should be filtered out because dwc:scientificNameID is missing"
        
        print("✓ Correctly identified that AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID requires dwc:scientificNameID")
        print("✓ Correctly filtered it out because dwc:scientificNameID is not available in occurrence data")
    
    @patch('requests.get')
    def test_filter_applicable_tests_with_scientific_name_id(self, mock_get):
        """Test that filtering includes the test when scientificNameID is available"""
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = self.bdq_tests_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Add dwc:scientificNameID to the columns
        columns_with_scientific_name_id = self.occurrence_columns + ['dwc:scientificNameID']
        
        # Test the filtering
        applicable_tests = self.bdq_service._filter_applicable_tests(columns_with_scientific_name_id)
        
        # Verify that the test IS now included
        test_ids = [test.id for test in applicable_tests]
        assert 'AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID' in test_ids, \
            "AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID should be included when dwc:scientificNameID is available"
        
        print("✓ Correctly included AMENDMENT_SCIENTIFICNAME_FROM_SCIENTIFICNAMEID when dwc:scientificNameID is available")
    
    def test_current_filtering_logic_bug(self):
        """Test that demonstrates the current bug in filtering logic"""
        # Create a test that requires both actedUpon and consulted columns
        test_data = {
            "id": "TEST_REQUIRING_CONSULTED_COLUMN",
            "guid": "test-guid",
            "type": "Amendment",
            "className": "TestClass",
            "methodName": "testMethod",
            "actedUpon": ["dwc:scientificName"],  # This column exists in occurrence data
            "consulted": ["dwc:scientificNameID"],  # This column does NOT exist in occurrence data
            "parameters": []
        }
        
        test = BDQTest(**test_data)
        
        # Current filtering logic only checks actedUpon
        current_logic_result = all(col in self.occurrence_columns for col in test.actedUpon)
        assert current_logic_result is True, "Current logic incorrectly says this test is applicable"
        
        # Correct filtering logic should check both actedUpon AND consulted
        correct_logic_result = (
            all(col in self.occurrence_columns for col in test.actedUpon) and
            all(col in self.occurrence_columns for col in test.consulted)
        )
        assert correct_logic_result is False, "Correct logic should say this test is NOT applicable"
        
        print("✓ Demonstrated the bug: current logic only checks actedUpon, not consulted columns")
        print("✓ Correct logic should check both actedUpon AND consulted columns")
