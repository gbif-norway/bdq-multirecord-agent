import pytest
from pathlib import Path

from app.services.bdq_py4j_service import BDQPy4JService
from app.services.csv_service import CSVService


class TestColumnMappingIssue:
    """
    Tests specifically focused on reproducing and fixing the column mapping issue
    where Darwin Core columns without 'dwc:' prefixes don't match BDQ test requirements
    """
    
    @pytest.fixture
    def test_data_dir(self):
        return Path(__file__).parent / "data"
    
    @pytest.fixture 
    def bdq_service(self):
        return BDQPy4JService(skip_validation=True)
    
    @pytest.fixture
    def csv_service(self):
        return CSVService()
    
    def test_occurrence_txt_column_analysis(self, test_data_dir, csv_service, bdq_service):
        """Analyze the columns in occurrence.txt to understand why no tests apply"""
        
        # Load the occurrence.txt file
        occ_txt_path = test_data_dir / "occurrence.txt"
        with open(occ_txt_path, 'r') as f:
            content = f.read()
        
        df, core_type = csv_service.parse_csv_and_detect_core(content)
        
        print(f"\nOccurrence.txt Analysis:")
        print(f"Core type detected: {core_type}")
        print(f"Number of rows: {len(df)}")
        print(f"Number of columns: {len(df.columns)}")
        print(f"Columns: {list(df.columns)}")
        
        # Get available BDQ tests
        tests = bdq_service.get_available_tests()
        print(f"Total available BDQ tests: {len(tests)}")
        
        # Test current filtering behavior
        applicable_tests = bdq_service.filter_applicable_tests(tests, df.columns.tolist())
        print(f"Applicable tests with current filtering: {len(applicable_tests)}")
        
        # Analyze why tests don't apply
        print("\nAnalyzing test requirements vs available columns:")
        
        sample_tests_to_check = []
        for test in tests[:10]:  # Check first 10 tests
            all_required = test.actedUpon + test.consulted
            all_required = [col for col in all_required if col.strip()]
            
            if all_required:
                missing = [col for col in all_required if col not in df.columns]
                
                print(f"\nTest {test.id}:")
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
        
        # This should show the fix is working: we now have applicable tests
        assert core_type == "occurrence"
        assert len(applicable_tests) > 0  # The fix should now provide applicable tests!
        
        return sample_tests_to_check
    
    def test_column_name_normalization_concept(self, test_data_dir, csv_service, bdq_service):
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
        with open(occ_txt_path, 'r') as f:
            content = f.read()
        
        df, _ = csv_service.parse_csv_and_detect_core(content)
        available_columns = df.columns.tolist()
        normalized_available = normalize_column_list(available_columns)
        
        # Get BDQ tests
        tests = bdq_service.get_available_tests()
        
        # Test original filtering (should give 0)
        original_applicable = bdq_service.filter_applicable_tests(tests, available_columns)
        
        # Test with normalization concept
        normalized_applicable = []
        
        for test in tests:
            all_required = test.actedUpon + test.consulted
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
            all_required = test.actedUpon + test.consulted
            all_required = [col for col in all_required if col.strip()]
            print(f"Test {test.id} requires: {all_required}")
        
        # This demonstrates the fix would work
        assert len(original_applicable) == 0
        assert len(normalized_applicable) > 0  # Should have applicable tests after normalization
    
    def test_different_csv_formats_comparison(self, test_data_dir, csv_service, bdq_service):
        """Compare test applicability across different CSV formats"""
        
        test_files = [
            ("occurrence.txt", "Original occurrence data"),
            ("simple_occurrence_dwc.csv", "Simple occurrence without prefix"),
            ("prefixed_occurrence_dwc.csv", "Prefixed occurrence with dwc:")
        ]
        
        results = {}
        
        for filename, description in test_files:
            file_path = test_data_dir / filename
            with open(file_path, 'r') as f:
                content = f.read()
            
            df, core_type = csv_service.parse_csv_and_detect_core(content)
            tests = bdq_service.get_available_tests()
            applicable = bdq_service.filter_applicable_tests(tests, df.columns.tolist())
            
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
    
    def test_manual_column_mapping_fix(self, test_data_dir, csv_service, bdq_service):
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
        with open(occ_txt_path, 'r') as f:
            content = f.read()
        
        df, _ = csv_service.parse_csv_and_detect_core(content)
        original_columns = df.columns.tolist()
        mapped_columns = map_columns_to_dwc(original_columns)
        
        tests = bdq_service.get_available_tests()
        
        original_applicable = bdq_service.filter_applicable_tests(tests, original_columns)
        mapped_applicable = bdq_service.filter_applicable_tests(tests, mapped_columns)
        
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
            all_required = test.actedUpon + test.consulted
            print(f"Test {test.id} requires: {[col for col in all_required if col.strip()]}")
        
        assert len(mapped_applicable) > len(original_applicable)
        
        return {
            'original_applicable': len(original_applicable),
            'mapped_applicable': len(mapped_applicable),
            'mappings_applied': len(mappings_applied)
        }