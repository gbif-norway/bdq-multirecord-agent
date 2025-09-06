"""
Unit tests for BDQ label-based test discovery functionality.

These tests verify that we can find test implementations using annotation labels
instead of GUIDs, which is more reliable and matches the actual Java implementations.
"""
import pytest
import os
import sys
from unittest.mock import patch, MagicMock

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))

from services.bdq_py4j_service import BDQPy4JService


class TestLabelBasedDiscovery:
    """Test BDQ label-based test discovery functionality with real Py4J integration."""
    
    @pytest.fixture(scope="class")
    def bdq_service(self):
        """Shared BDQ service instance for all tests in this class (no guards)."""
        gateway_jar = os.getenv('BDQ_PY4J_GATEWAY_JAR', '/opt/bdq/bdq-py4j-gateway.jar')
        assert os.path.exists(gateway_jar), (
            f"Expected BDQ gateway JAR at {gateway_jar}. Run inside the Docker image that contains the gateway."
        )

        service = BDQPy4JService()
        yield service
        service.shutdown()

    def test_service_initialization(self, bdq_service):
        """Test that BDQPy4JService can be initialized and connects to Py4J gateway."""
        # Verify gateway is connected
        assert bdq_service.gateway is not None
        assert bdq_service.gateway.entry_point is not None
        
        # Verify health check works
        health = bdq_service.gateway.entry_point.healthCheck()
        assert health is not None

    def test_get_all_available_methods(self, bdq_service):
        """Test that we can discover all available methods with annotation labels."""
        available_methods = bdq_service._get_all_available_methods()
        
        # Should find methods from all libraries
        assert len(available_methods) > 0
        
        # Check that we have methods from different libraries
        libraries = set(method['library'] for method in available_methods.values())
        expected_libraries = {'rec_occur_qc', 'geo_ref_qc', 'event_date_qc', 'sci_name_qc'}
        assert libraries.intersection(expected_libraries)
        
        # Check that we have different annotation types
        annotation_types = set(method['annotation_type'] for method in available_methods.values())
        expected_types = {'Validation', 'Amendment', 'Issue', 'Measure'}
        assert annotation_types.intersection(expected_types)
        
        print(f"Found {len(available_methods)} methods with labels:")
        for label, method_info in list(available_methods.items())[:10]:  # Show first 10
            print(f"  {label}: {method_info['library']}.{method_info['class_name']}.{method_info['method_name']} ({method_info['annotation_type']})")

    def test_find_method_by_label_validation(self, bdq_service):
        """Test finding a specific validation test by label."""
        # Test with a known validation label
        test_label = "VALIDATION_COUNTRYCODE_STANDARD"
        
        method_info = bdq_service._find_method_by_label(test_label)
        
        assert method_info is not None, f"Could not find method for label {test_label}"
        assert method_info['library'] == 'geo_ref_qc'
        assert method_info['class_name'].endswith(('DwCGeoRefDQ', 'DwCGeoRefDQDefaults'))
        assert method_info['method_name'] is not None
        assert method_info['annotation_type'] == 'Validation'
        assert method_info['annotation_label'] == test_label

    def test_find_method_by_label_amendment(self, bdq_service):
        """Test finding a specific amendment test by label."""
        # Test with a known amendment label
        test_label = "AMENDMENT_COORDINATES_FROM_VERBATIM"
        
        method_info = bdq_service._find_method_by_label(test_label)
        
        assert method_info is not None, f"Could not find method for label {test_label}"
        assert method_info['library'] == 'geo_ref_qc'
        assert method_info['class_name'].endswith(('DwCGeoRefDQ', 'DwCGeoRefDQDefaults'))
        assert method_info['method_name'] is not None
        assert method_info['annotation_type'] == 'Amendment'
        assert method_info['annotation_label'] == test_label

    def test_find_method_by_label_measure(self, bdq_service):
        """Test finding a specific measure test by label."""
        # Test with a known measure label
        test_label = "MEASURE_EVENTDATE_DURATIONINSECONDS"
        
        method_info = bdq_service._find_method_by_label(test_label)
        
        assert method_info is not None, f"Could not find method for label {test_label}"
        assert method_info['library'] == 'event_date_qc'
        assert method_info['class_name'].endswith(('DwCEventDQ', 'DwCEventDQDefaults'))
        assert method_info['method_name'] is not None
        assert method_info['annotation_type'] == 'Measure'
        assert method_info['annotation_label'] == test_label

    def test_find_method_by_label_issue(self, bdq_service):
        """Test finding a specific issue test by label."""
        # Test with a known issue label
        test_label = "ISSUE_DATAGENERALIZATIONS_NOTEMPTY"
        
        method_info = bdq_service._find_method_by_label(test_label)
        
        assert method_info is not None, f"Could not find method for label {test_label}"
        assert method_info['library'] == 'rec_occur_qc'
        assert method_info['class_name'].endswith(('DwCMetadataDQ', 'DwCMetadataDQDefaults'))
        assert method_info['method_name'] is not None
        assert method_info['annotation_type'] == 'Issue'
        assert method_info['annotation_label'] == test_label

    def test_load_test_mappings_with_labels(self, bdq_service):
        """Test that _load_test_mappings works with label-based discovery."""
        # Clear existing tests
        bdq_service.tests = {}
        
        # Load test mappings using the new label-based approach
        bdq_service._load_test_mappings()
        
        # Should have loaded some tests
        assert len(bdq_service.tests) > 0
        
        # Check that we have tests from different libraries
        libraries = set(test.library for test in bdq_service.tests.values())
        expected_libraries = {'rec_occur_qc', 'geo_ref_qc', 'event_date_qc', 'sci_name_qc'}
        assert libraries.intersection(expected_libraries)
        
        # Check that we have different test types
        test_types = set(test.test_type for test in bdq_service.tests.values())
        expected_types = {'Validation', 'Amendment', 'Issue', 'Measure'}
        assert test_types.intersection(expected_types)
        
        print(f"Loaded {len(bdq_service.tests)} tests using label-based discovery:")
        for label, test_mapping in list(bdq_service.tests.items())[:10]:  # Show first 10
            print(f"  {label}: {test_mapping.library}.{test_mapping.java_class}.{test_mapping.java_method} ({test_mapping.test_type})")

    def test_label_matching_accuracy(self, bdq_service):
        """Test that label matching is accurate by comparing with available methods."""
        # Get all available methods
        available_methods = bdq_service._get_all_available_methods()
        
        # Load test mappings
        bdq_service.tests = {}
        bdq_service._load_test_mappings()
        
        # Check that loaded tests match available methods
        matched_count = 0
        for label, test_mapping in bdq_service.tests.items():
            if label in available_methods:
                available_method = available_methods[label]
                assert test_mapping.library == available_method['library']
                assert test_mapping.java_class == available_method['class_name']
                assert test_mapping.java_method == available_method['method_name']
                matched_count += 1
        
        print(f"Matched {matched_count} tests with available methods out of {len(bdq_service.tests)} loaded tests")
        assert matched_count > 0, "Should have matched at least some tests with available methods"

    def test_get_applicable_tests_for_dataset(self, bdq_service):
        """Test that we can find applicable tests for a dataset with specific columns."""
        # Test with occurrence core columns
        occurrence_columns = ['dwc:occurrenceID', 'dwc:scientificName', 'dwc:decimalLatitude', 'dwc:decimalLongitude']
        applicable_tests = bdq_service.get_applicable_tests_for_dataset(occurrence_columns)
        
        assert len(applicable_tests) > 0
        
        # Test with taxon core columns
        taxon_columns = ['dwc:taxonID', 'dwc:scientificName', 'dwc:phylum', 'dwc:family']
        applicable_tests_taxon = bdq_service.get_applicable_tests_for_dataset(taxon_columns)
        
        assert len(applicable_tests_taxon) > 0
        
        print(f"Found {len(applicable_tests)} applicable tests for occurrence columns")
        print(f"Found {len(applicable_tests_taxon)} applicable tests for taxon columns")
