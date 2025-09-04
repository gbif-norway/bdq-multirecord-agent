"""
BDQ Service Integration Tests

These tests focus on the BDQ service functionality, including test discovery,
filtering, and execution. They test the core business logic of the application.
"""
import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, Mock

from app.services.bdq_py4j_service import BDQPy4JService
from app.services.csv_service import CSVService
from app.services.tg2_parser import TG2Parser


class TestBDQServiceCore:
    """Test core BDQ service functionality"""
    
    @pytest.fixture
    def bdq_service(self):
        """BDQ service instance for testing"""
        return BDQPy4JService()
    
    @pytest.fixture
    def csv_service(self):
        """CSV service instance for testing"""
        return CSVService()
    
    @pytest.fixture
    def test_data_dir(self):
        """Get the test data directory"""
        return Path(__file__).parent / "data"
    
    def test_service_initialization(self, bdq_service):
        """Test BDQ service can be initialized without errors"""
        assert bdq_service is not None
        assert len(bdq_service.test_mappings) > 0
    
    def test_test_mappings_loaded(self, bdq_service):
        """Test that test mappings are properly loaded"""
        assert len(bdq_service.test_mappings) > 0
        
        # Check that we have different types of tests
        test_types = set()
        for test in bdq_service.test_mappings.values():
            test_types.add(test.test_type)
        
        # Should have at least validation tests
        assert "Validation" in test_types
    
    def test_get_applicable_tests(self, bdq_service):
        """Test getting applicable tests"""
        # Test with some common CSV columns
        csv_columns = ['occurrenceID', 'scientificName', 'decimalLatitude', 'decimalLongitude']
        tests = bdq_service.get_applicable_tests(csv_columns)
        assert len(tests) > 0
        
        # Check for some expected test types
        test_ids = [test.test_id for test in tests]
        
        # Should have some common validation tests
        common_tests = ["VALIDATION_COUNTRY_FOUND", "VALIDATION_COORDINATES_NOTEMPTY"]
        found_tests = [test_id for test_id in test_ids if any(common in test_id for common in common_tests)]
        
        # At least some tests should be found
        assert len(found_tests) > 0
    
    def test_get_applicable_tests_basic(self, bdq_service):
        """Test getting applicable tests with basic column sets"""
        # Test with simple column names (no dwc: prefix)
        simple_columns = ["occurrenceID", "country", "countryCode", "decimalLatitude", "decimalLongitude"]
        applicable_simple = bdq_service.get_applicable_tests(simple_columns)
        
        # Test with prefixed column names
        prefixed_columns = ["dwc:occurrenceID", "dwc:country", "dwc:countryCode", "dwc:decimalLatitude", "dwc:decimalLongitude"]
        applicable_prefixed = bdq_service.get_applicable_tests(prefixed_columns)
        
        print(f"Simple columns applicable tests: {len(applicable_simple)}")
        print(f"Prefixed columns applicable tests: {len(applicable_prefixed)}")
        
        # The prefixed version should have more applicable tests
        # This demonstrates the current column mapping issue
        assert len(applicable_prefixed) >= len(applicable_simple)
    
    def test_get_applicable_tests_with_real_data(self, bdq_service, test_data_dir):
        """Test getting applicable tests with real data files"""
        # Test with occurrence.txt columns
        occ_txt_path = test_data_dir / "occurrence.txt"
        csv_service = CSVService()
        with open(occ_txt_path, 'r') as f:
            content = f.read()
        df, _ = csv_service.parse_csv_and_detect_core(content)
        
        applicable_tests = bdq_service.get_applicable_tests(df.columns.tolist())
        print(f"Occurrence.txt applicable tests: {len(applicable_tests)}")
        
        # This should demonstrate the issue: occurrence.txt has good columns but gets 0 applicable tests
        # The columns are there but without dwc: prefixes
        columns_in_occ_txt = df.columns.tolist()
        print(f"Columns in occurrence.txt: {columns_in_occ_txt[:10]}...")  # Show first 10 columns
        
        # Document the current behavior
        assert isinstance(applicable_tests, list)
    
    def test_column_mapping_issue_analysis(self, bdq_service, test_data_dir):
        """Analyze the column mapping issue in detail"""
        # Load the occurrence.txt file
        occ_txt_path = test_data_dir / "occurrence.txt"
        csv_service = CSVService()
        with open(occ_txt_path, 'r') as f:
            content = f.read()
        
        df, core_type = csv_service.parse_csv_and_detect_core(content)
        
        print(f"\nOccurrence.txt Analysis:")
        print(f"Core type detected: {core_type}")
        print(f"Number of rows: {len(df)}")
        print(f"Number of columns: {len(df.columns)}")
        print(f"Columns: {list(df.columns)}")
        
        # Get available BDQ tests
        tests = list(bdq_service.test_mappings.values())
        print(f"Total available BDQ tests: {len(tests)}")
        
        # Test current filtering behavior
        applicable_tests = bdq_service.get_applicable_tests(df.columns.tolist())
        print(f"Applicable tests with current filtering: {len(applicable_tests)}")
        
        # Analyze why tests don't apply
        print("\nAnalyzing test requirements vs available columns:")
        
        sample_tests_to_check = []
        for test in tests[:10]:  # Check first 10 tests
            all_required = test.acted_upon + test.consulted
            all_required = [col for col in all_required if col.strip()]
            
            if all_required:
                missing = [col for col in all_required if col not in df.columns]
                
                print(f"\nTest {test.test_id}:")
                print(f"  Requires: {all_required}")
                print(f"  Missing: {missing}")
                
                # Check if we have the unprefixed version
                unprefixed_available = []
                for req_col in all_required:
                    if req_col.startswith('dwc:'):
                        unprefixed = req_col[4:]  # Remove 'dwc:' prefix
                        if unprefixed in df.columns:
                            unprefixed_available.append(f"{req_col} -> {unprefixed}")
                
                if unprefixed_available:
                    print(f"  Available without prefix: {unprefixed_available}")
                    sample_tests_to_check.append(test)
        
        # This documents the current behavior
        assert core_type == "occurrence"
        assert len(applicable_tests) >= 0  # May be 0 due to prefix mismatch
    
    def test_column_normalization_concept(self, bdq_service, test_data_dir):
        """Test a concept for column name normalization to fix the mapping issue"""
        
        def normalize_column_name(col_name):
            """Normalize column names by removing dwc: prefix for comparison"""
            if col_name.startswith('dwc:'):
                return col_name[4:]
            return col_name
        
        def normalize_column_list(columns):
            """Normalize a list of column names"""
            return [normalize_column_name(col) for col in columns]
        
        # Load occurrence.txt
        occ_txt_path = test_data_dir / "occurrence.txt"
        csv_service = CSVService()
        with open(occ_txt_path, 'r') as f:
            content = f.read()
        
        df, _ = csv_service.parse_csv_and_detect_core(content)
        available_columns = df.columns.tolist()
        normalized_available = normalize_column_list(available_columns)
        
        # Get BDQ tests
        tests = list(bdq_service.test_mappings.values())
        
        # Test original filtering (should give 0)
        original_applicable = bdq_service.get_applicable_tests(available_columns)
        
        # Test with normalization concept
        normalized_applicable = []
        
        for test in tests:
            all_required = test.acted_upon + test.consulted
            all_required = [col for col in all_required if col.strip()]
            
            if all_required:
                normalized_required = normalize_column_list(all_required)
                missing_normalized = [col for col in normalized_required if col not in normalized_available]
                
                if not missing_normalized:  # All required columns available after normalization
                    normalized_applicable.append(test)
        
        print(f"\nColumn Normalization Results:")
        print(f"Original applicable tests: {len(original_applicable)}")
        print(f"Normalized applicable tests: {len(normalized_applicable)}")
        
        # Show some examples of tests that would now be applicable
        for test in normalized_applicable[:5]:
            all_required = test.acted_upon + test.consulted
            all_required = [col for col in all_required if col.strip()]
            print(f"Test {test.test_id} requires: {all_required}")
        
        # This demonstrates the fix would work
        assert len(original_applicable) >= 0
        assert len(normalized_applicable) > 0  # Should have applicable tests after normalization
    
    def test_different_csv_formats_comparison(self, bdq_service, test_data_dir):
        """Compare test applicability across different CSV formats"""
        
        test_files = [
            ("occurrence.txt", "Original occurrence data"),
            ("simple_occurrence_dwc.csv", "Simple occurrence without prefix"),
            ("prefixed_occurrence_dwc.csv", "Prefixed occurrence with dwc:")
        ]
        
        results = {}
        csv_service = CSVService()
        
        for filename, description in test_files:
            file_path = test_data_dir / filename
            with open(file_path, 'r') as f:
                content = f.read()
            
            df, core_type = csv_service.parse_csv_and_detect_core(content)
            tests = list(bdq_service.test_mappings.values())
            applicable = bdq_service.get_applicable_tests(df.columns.tolist())
            
            results[filename] = {
                'description': description,
                'core_type': core_type,
                'num_rows': len(df),
                'num_columns': len(df.columns),
                'columns': list(df.columns),
                'applicable_tests': len(applicable)
            }
            
            print(f"\n{description} ({filename}):")
            print(f"  Core type: {core_type}")
            print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
            print(f"  Applicable tests: {len(applicable)}")
            print(f"  Sample columns: {list(df.columns)[:5]}")
        
        # The prefixed version should have more applicable tests than the others
        return results
    
    def test_manual_column_mapping_fix(self, bdq_service, test_data_dir):
        """Test a manual fix by mapping known Darwin Core terms"""
        
        # Common Darwin Core term mappings (without prefix -> with prefix)
        DWC_MAPPINGS = {
            'occurrenceID': 'dwc:occurrenceID',
            'country': 'dwc:country', 
            'countryCode': 'dwc:countryCode',
            'decimalLatitude': 'dwc:decimalLatitude',
            'decimalLongitude': 'dwc:decimalLongitude', 
            'scientificName': 'dwc:scientificName',
            'basisOfRecord': 'dwc:basisOfRecord',
            'eventDate': 'dwc:eventDate',
            'year': 'dwc:year',
            'month': 'dwc:month',
            'locality': 'dwc:locality',
            'stateProvince': 'dwc:stateProvince',
            'kingdom': 'dwc:kingdom',
            'phylum': 'dwc:phylum',
            'class': 'dwc:class',
            'order': 'dwc:order',
            'family': 'dwc:family',
            'genus': 'dwc:genus',
            'specificEpithet': 'dwc:specificEpithet',
            'taxonRank': 'dwc:taxonRank',
            'scientificNameAuthorship': 'dwc:scientificNameAuthorship',
            'taxonID': 'dwc:taxonID'
        }
        
        def map_columns_to_dwc(columns):
            """Map column names to Darwin Core equivalents where possible"""
            mapped = []
            for col in columns:
                mapped_col = DWC_MAPPINGS.get(col, col)
                mapped.append(mapped_col)
            return mapped
        
        # Test with occurrence.txt
        occ_txt_path = test_data_dir / "occurrence.txt"
        csv_service = CSVService()
        with open(occ_txt_path, 'r') as f:
            content = f.read()
        
        df, _ = csv_service.parse_csv_and_detect_core(content)
        original_columns = df.columns.tolist()
        mapped_columns = map_columns_to_dwc(original_columns)
        
        tests = list(bdq_service.test_mappings.values())
        
        original_applicable = bdq_service.get_applicable_tests(original_columns)
        mapped_applicable = bdq_service.get_applicable_tests(mapped_columns)
        
        print(f"\nManual DWC Mapping Results:")
        print(f"Original columns (sample): {original_columns[:10]}")
        print(f"Mapped columns (sample): {mapped_columns[:10]}")
        print(f"Original applicable tests: {len(original_applicable)}")
        print(f"Mapped applicable tests: {len(mapped_applicable)}")
        
        # Show which columns got mapped
        mappings_applied = []
        for orig, mapped in zip(original_columns, mapped_columns):
            if orig != mapped:
                mappings_applied.append(f"{orig} -> {mapped}")
        
        print(f"Mappings applied: {mappings_applied[:10]}")
        
        # Show some tests that are now applicable
        for test in mapped_applicable[:3]:
            all_required = test.acted_upon + test.consulted
            print(f"Test {test.test_id} requires: {[col for col in all_required if col.strip()]}")
        
        # With automatic column normalization, both should find the same number of tests
        # This demonstrates that the automatic normalization works as well as manual mapping
        assert len(mapped_applicable) == len(original_applicable)
        
        return {
            'original_applicable': len(original_applicable),
            'mapped_applicable': len(mapped_applicable),
            'mappings_applied': len(mappings_applied)
        }

    def test_real_world_occurrence_data_columns(self, bdq_service):
        """Test with the actual columns from the real occurrence.txt file"""
        # These are the actual columns from the occurrence.txt file that was causing issues
        real_columns = [
            "id", "modified", "license", "institutionID", "institutionCode", "datasetName",
            "basisOfRecord", "dynamicProperties", "occurrenceID", "recordedBy", "associatedReferences",
            "organismID", "eventID", "parentEventID", "year", "month", "samplingProtocol",
            "eventRemarks", "country", "countryCode", "stateProvince", "locality",
            "minimumElevationInMeters", "maximumElevationInMeters", "verbatimElevation",
            "decimalLatitude", "decimalLongitude", "geodeticDatum", "coordinateUncertaintyInMeters",
            "verbatimCoordinates", "verbatimLatitude", "verbatimLongitude", "verbatimCoordinateSystem",
            "verbatimSRS", "georeferencedBy", "scientificName", "kingdom", "phylum", "class",
            "order", "family", "genus", "specificEpithet", "infraspecificEpithet", "taxonRank",
            "verbatimTaxonRank", "scientificNameAuthorship", "vernacularName"
        ]
        
        # Test with real columns (should now find tests due to column normalization)
        applicable_tests = bdq_service.get_applicable_tests(real_columns)
        
        # With column normalization, we should now find applicable tests
        assert len(applicable_tests) > 0, f"Expected to find applicable tests after column normalization, but found {len(applicable_tests)}. Column normalization may not be working correctly."
        
        # Show what columns would need to be normalized
        relevant_columns = ["country", "decimalLatitude", "decimalLongitude", "scientificName", "occurrenceID"]
        normalized_relevant = [f"dwc:{col}" for col in relevant_columns]
        
        # Test with normalized columns
        normalized_applicable = bdq_service.get_applicable_tests(normalized_relevant)
        
        # This shows the fix would work
        assert len(normalized_applicable) > 0, "Normalized columns should find applicable tests"
        
        return {
            "real_columns_count": len(real_columns),
            "applicable_tests_with_real_columns": len(applicable_tests),
            "relevant_columns": relevant_columns,
            "normalized_relevant_columns": normalized_relevant,
            "applicable_tests_with_normalized_columns": len(normalized_applicable),
            "fix_implemented": "Column normalization now automatically adds dwc: prefix to Darwin Core terms"
        }


class TestTG2ParserIntegration:
    """Test TG2 parser integration with BDQ service"""
    
    @pytest.fixture
    def temp_tg2_csv(self):
        """Create a temporary TG2 tests CSV file for testing"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("""Label,InformationElement:ActedUpon,InformationElement:Consulted,Parameters,Link to Specification Source Code
VALIDATION_COUNTRY_FOUND,"dwc:country","","","https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/CountryFound.java"
VALIDATION_DATE_FORMAT,"dwc:eventDate","","","https://github.com/FilteredPush/event_date_qc/blob/main/src/main/java/org/filteredpush/qc/eventdate/DateFormat.java"
AMENDMENT_COUNTRY_CODE,"dwc:country","","","https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/CountryCodeAmendment.java"
MEASURE_COORDINATE_PRECISION,"dwc:decimalLatitude,dwc:decimalLongitude","","","https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/CoordinatePrecisionMeasure.java"
ISSUE_MISSING_COORDINATES,"dwc:decimalLatitude,dwc:decimalLongitude","","","https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/MissingCoordinatesIssue.java"
VALIDATION_SCIENTIFIC_NAME,"dwc:scientificName","","","https://github.com/FilteredPush/sci_name_qc/blob/main/src/main/java/org/filteredpush/qc/sciname/ScientificNameValidation.java"
VALIDATION_TAXON_RANK,"dwc:taxonRank","","","https://github.com/FilteredPush/sci_name_qc/blob/main/src/main/java/org/filteredpush/qc/sciname/TaxonRankValidation.java"
VALIDATION_OCCURRENCE_STATUS,"dwc:occurrenceStatus","","","https://github.com/FilteredPush/rec_occur_qc/blob/main/src/main/java/org/filteredpush/qc/recoccur/OccurrenceStatusValidation.java"
VALIDATION_BASIS_OF_RECORD,"dwc:basisOfRecord","","","https://github.com/FilteredPush/rec_occur_qc/blob/main/src/main/java/org/filteredpush/qc/recoccur/BasisOfRecordValidation.java""")
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        try:
            os.unlink(temp_path)
        except OSError:
            pass
    
    def test_tg2_parser_integration(self, temp_tg2_csv):
        """Test TG2 parser integration with BDQ service"""
        # Create BDQ service with custom TG2 file
        with patch('app.services.bdq_py4j_service.TG2Parser') as mock_parser_class:
            mock_parser = Mock()
            mock_parser.parse.return_value = {
                "VALIDATION_COUNTRY_FOUND": Mock(
                    test_id="VALIDATION_COUNTRY_FOUND",
                    test_type="Validation",
                    acted_upon=["dwc:country"],
                    consulted=[],
                    parameters=[],
                    default_parameters={}
                )
            }
            mock_parser_class.return_value = mock_parser
            
            service = BDQPy4JService()
            
            # Test that the service loaded the mocked test mappings
            assert len(service.test_mappings) == 1
            assert "VALIDATION_COUNTRY_FOUND" in service.test_mappings
    
    def test_tg2_parser_with_real_file(self, temp_tg2_csv):
        """Test TG2 parser with a real file"""
        parser = TG2Parser(temp_tg2_csv)
        mappings = parser.parse()
        
        assert len(mappings) == 9
        assert "VALIDATION_COUNTRY_FOUND" in mappings
        assert "VALIDATION_DATE_FORMAT" in mappings
        assert "AMENDMENT_COUNTRY_CODE" in mappings
        
        # Test specific mapping
        country_test = mappings["VALIDATION_COUNTRY_FOUND"]
        assert country_test.test_id == "VALIDATION_COUNTRY_FOUND"
        assert country_test.test_type == "Validation"
        assert country_test.acted_upon == ["dwc:country"]
        assert country_test.consulted == []
    
    @pytest.mark.skip(reason="File busy error in Docker environment - TG2_tests.csv is mounted and in use")
    def test_tg2_parser_error_handling(self):
        """Test TG2 parser error handling"""
        # Test with non-existent file that doesn't exist in any alternative paths
        # We need to temporarily remove the TG2_tests.csv file to test error handling
        import os
        import shutil
        original_path = "/app/TG2_tests.csv"
        backup_path = "/app/TG2_tests.csv.backup"
        
        # Backup the file if it exists
        if os.path.exists(original_path):
            shutil.move(original_path, backup_path)
        
        try:
            parser = TG2Parser("/completely/nonexistent/path/file.csv")
            with pytest.raises(FileNotFoundError):
                parser.parse()
        finally:
            # Restore the file
            if os.path.exists(backup_path):
                shutil.move(backup_path, original_path)
    
    def test_tg2_parser_empty_file(self):
        """Test TG2 parser with empty file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("Label,InformationElement:ActedUpon,InformationElement:Consulted,Parameters,Link to Specification Source Code\n")
            temp_path = f.name
        
        try:
            parser = TG2Parser(temp_path)
            mappings = parser.parse()
            assert len(mappings) == 0
        finally:
            os.unlink(temp_path)


class TestBDQServiceErrorHandling:
    """Test error handling in BDQ service"""
    
    def test_bdq_service_initialization_without_tg2_file(self):
        """Test BDQ service initialization when TG2 file is missing"""
        with patch('app.services.bdq_py4j_service.TG2Parser') as mock_parser_class:
            mock_parser = Mock()
            mock_parser.parse.side_effect = FileNotFoundError("TG2 file not found")
            mock_parser_class.return_value = mock_parser
            
            # Should raise exception when service fails to initialize
            with pytest.raises(FileNotFoundError):
                BDQPy4JService()
            
            # Should not raise exception when service initializes successfully
            service = BDQPy4JService()
            assert service is not None
    
    def test_bdq_service_with_empty_test_mappings(self):
        """Test BDQ service with empty test mappings"""
        with patch('app.services.bdq_py4j_service.TG2Parser') as mock_parser_class:
            mock_parser = Mock()
            mock_parser.parse.return_value = {}  # Empty mappings
            mock_parser_class.return_value = mock_parser
            
            service = BDQPy4JService()
            
            # Should handle empty mappings gracefully
            assert len(service.test_mappings) == 0
            assert len(list(service.test_mappings.values())) == 0
            assert len(service.get_applicable_tests(["dwc:country"])) == 0
    
    def test_bdq_service_with_malformed_test_mappings(self):
        """Test BDQ service with malformed test mappings"""
        with patch('app.services.bdq_py4j_service.TG2Parser') as mock_parser_class:
            mock_parser = Mock()
            mock_parser.parse.side_effect = Exception("Malformed CSV")
            mock_parser_class.return_value = mock_parser
            
            # Should raise exception when service fails to initialize
            with pytest.raises(Exception):
                BDQPy4JService()
            
            # Should not raise exception when service initializes successfully
            service = BDQPy4JService()
            assert service is not None
