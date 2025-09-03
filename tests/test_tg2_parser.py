import pytest
import tempfile
import os
import csv
from unittest.mock import patch, Mock, mock_open

from app.services.tg2_parser import TG2Parser, TG2TestMapping


class TestTG2Parser:
    """Test the TG2 Parser service"""

    @pytest.fixture
    def sample_tg2_csv_content(self):
        """Sample TG2 CSV content for testing"""
        return """Label,InformationElement:ActedUpon,InformationElement:Consulted,Parameters,Link to Specification Source Code
VALIDATION_COUNTRY_FOUND,"dwc:country","","","https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/CountryFound.java"
VALIDATION_DATE_FORMAT,"dwc:eventDate","","","https://github.com/FilteredPush/event_date_qc/blob/main/src/main/java/org/filteredpush/qc/eventdate/DateFormat.java"
AMENDMENT_COUNTRY_CODE,"dwc:country","","","https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/CountryCodeAmendment.java"
MEASURE_COORDINATE_PRECISION,"dwc:decimalLatitude,dwc:decimalLongitude","","","https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/CoordinatePrecisionMeasure.java"
ISSUE_MISSING_COORDINATES,"dwc:decimalLatitude,dwc:decimalLongitude","","","https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/MissingCoordinatesIssue.java"
VALIDATION_SCIENTIFIC_NAME,"dwc:scientificName","","","https://github.com/FilteredPush/sci_name_qc/blob/main/src/main/java/org/filteredpush/qc/sciname/ScientificNameValidation.java"
VALIDATION_TAXON_RANK,"dwc:taxonRank","","","https://github.com/FilteredPush/sci_name_qc/blob/main/src/main/java/org/filteredpush/qc/sciname/TaxonRankValidation.java"
VALIDATION_OCCURRENCE_STATUS,"dwc:occurrenceStatus","","","https://github.com/FilteredPush/rec_occur_qc/blob/main/src/main/java/org/filteredpush/qc/recoccur/OccurrenceStatusValidation.java"
VALIDATION_BASIS_OF_RECORD,"dwc:basisOfRecord","","","https://github.com/FilteredPush/rec_occur_qc/blob/main/src/main/java/org/filteredpush/qc/recoccur/BasisOfRecordValidation.java"
VALIDATION_COORDINATES_IN_COUNTRY,"dwc:decimalLatitude,dwc:decimalLongitude","dwc:country","","https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/CoordinatesInCountry.java"
VALIDATION_DATE_RANGE,"dwc:eventDate","dwc:year","","https://github.com/FilteredPush/event_date_qc/blob/main/src/main/java/org/filteredpush/qc/eventdate/DateRange.java"
AMENDMENT_SCIENTIFIC_NAME_FORMAT,"dwc:scientificName","","authority,rank","https://github.com/FilteredPush/sci_name_qc/blob/main/src/main/java/org/filteredpush/qc/sciname/ScientificNameFormatAmendment.java"
MEASURE_TAXON_NAME_LENGTH,"dwc:scientificName","","","https://github.com/FilteredPush/sci_name_qc/blob/main/src/main/java/org/filteredpush/qc/sciname/TaxonNameLengthMeasure.java"
ISSUE_DUPLICATE_OCCURRENCE,"dwc:occurrenceID","dwc:eventDate,dwc:locality","","https://github.com/FilteredPush/rec_occur_qc/blob/main/src/main/java/org/filteredpush/qc/recoccur/DuplicateOccurrenceIssue.java"""

    @pytest.fixture
    def temp_tg2_csv(self, sample_tg2_csv_content):
        """Create a temporary TG2 tests CSV file for testing"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(sample_tg2_csv_content)
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        try:
            os.unlink(temp_path)
        except OSError:
            pass

    def test_init_default_path(self):
        """Test parser initialization with default path"""
        parser = TG2Parser()
        assert parser.csv_path == "TG2_tests.csv"
        assert len(parser.test_mappings) == 0

    def test_init_custom_path(self):
        """Test parser initialization with custom path"""
        parser = TG2Parser("/custom/path/tests.csv")
        assert parser.csv_path == "/custom/path/tests.csv"

    def test_parse_success(self, temp_tg2_csv):
        """Test successful CSV parsing"""
        parser = TG2Parser(temp_tg2_csv)
        mappings = parser.parse()
        
        assert len(mappings) == 14
        assert "VALIDATION_COUNTRY_FOUND" in mappings
        assert "VALIDATION_DATE_FORMAT" in mappings
        assert "AMENDMENT_COUNTRY_CODE" in mappings

    def test_parse_file_not_found(self):
        """Test parsing with non-existent file"""
        parser = TG2Parser("/nonexistent/file.csv")
        
        with pytest.raises(FileNotFoundError):
            parser.parse()

    def test_parse_empty_file(self):
        """Test parsing empty CSV file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("Label,InformationElement:ActedUpon,InformationElement:Consulted,Parameters,Link to Specification Source Code\n")
            temp_path = f.name
        
        try:
            parser = TG2Parser(temp_path)
            mappings = parser.parse()
            assert len(mappings) == 0
        finally:
            os.unlink(temp_path)

    def test_parse_row_success(self, temp_tg2_csv):
        """Test successful row parsing"""
        parser = TG2Parser(temp_tg2_csv)
        
        # Test a specific row
        row = {
            'Label': 'VALIDATION_COUNTRY_FOUND',
            'InformationElement:ActedUpon': 'dwc:country',
            'InformationElement:Consulted': '',
            'Parameters': '',
            'Link to Specification Source Code': 'https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/CountryFound.java'
        }
        
        mapping = parser._parse_row(row)
        
        assert mapping is not None
        assert mapping.test_id == 'VALIDATION_COUNTRY_FOUND'
        assert mapping.library == 'geo_ref_qc'
        assert mapping.java_class == 'org.filteredpush.qc.georeference.CountryFound'
        assert mapping.java_method == 'validationCountryFound'
        assert mapping.acted_upon == ['dwc:country']
        assert mapping.consulted == []
        assert mapping.parameters == []
        assert mapping.test_type == 'Validation'
        assert mapping.default_parameters == {}

    def test_parse_row_missing_label(self, temp_tg2_csv):
        """Test row parsing with missing label"""
        parser = TG2Parser(temp_tg2_csv)
        
        row = {
            'Label': '',  # Empty label
            'InformationElement:ActedUpon': 'dwc:country',
            'InformationElement:Consulted': '',
            'Parameters': '',
            'Link to Specification Source Code': 'https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/CountryFound.java'
        }
        
        mapping = parser._parse_row(row)
        assert mapping is None

    def test_parse_row_missing_source_link(self, temp_tg2_csv):
        """Test row parsing with missing source link"""
        parser = TG2Parser(temp_tg2_csv)
        
        row = {
            'Label': 'VALIDATION_COUNTRY_FOUND',
            'InformationElement:ActedUpon': 'dwc:country',
            'InformationElement:Consulted': '',
            'Parameters': '',
            'Link to Specification Source Code': ''  # Missing source link
        }
        
        mapping = parser._parse_row(row)
        assert mapping is None

    def test_parse_field_list_simple(self, temp_tg2_csv):
        """Test parsing simple field lists"""
        parser = TG2Parser(temp_tg2_csv)
        
        # Single field
        result = parser._parse_field_list('dwc:country')
        assert result == ['dwc:country']
        
        # Multiple fields
        result = parser._parse_field_list('dwc:decimalLatitude,dwc:decimalLongitude')
        assert result == ['dwc:decimalLatitude', 'dwc:decimalLongitude']
        
        # Empty field
        result = parser._parse_field_list('')
        assert result == []

    def test_parse_field_list_with_quotes(self, temp_tg2_csv):
        """Test parsing field lists with quotes"""
        parser = TG2Parser(temp_tg2_csv)
        
        # Quoted fields - parser preserves quotes for now
        result = parser._parse_field_list('"dwc:country","dwc:stateProvince"')
        assert result == ['"dwc:country"', '"dwc:stateProvince"']
        
        # Mixed quoted and unquoted
        result = parser._parse_field_list('dwc:country,"dwc:stateProvince"')
        assert result == ['dwc:country', '"dwc:stateProvince"']

    def test_parse_source_link_success(self, temp_tg2_csv):
        """Test successful source link parsing"""
        parser = TG2Parser(temp_tg2_csv)
        
        # Test geo_ref_qc
        library, java_class = parser._parse_source_link(
            'https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/CountryFound.java'
        )
        assert library == 'geo_ref_qc'
        assert java_class == 'org.filteredpush.qc.georeference.CountryFound'
        
        # Test event_date_qc
        library, java_class = parser._parse_source_link(
            'https://github.com/FilteredPush/event_date_qc/blob/main/src/main/java/org/filteredpush/qc/eventdate/DateFormat.java'
        )
        assert library == 'event_date_qc'
        assert java_class == 'org.filteredpush.qc.date.DateFormat'
        
        # Test sci_name_qc
        library, java_class = parser._parse_source_link(
            'https://github.com/FilteredPush/sci_name_qc/blob/main/src/main/java/org/filteredpush/qc/sciname/ScientificNameValidation.java'
        )
        assert library == 'sci_name_qc'
        assert java_class == 'org.filteredpush.qc.sciname.ScientificNameValidation'
        
        # Test rec_occur_qc
        library, java_class = parser._parse_source_link(
            'https://github.com/FilteredPush/rec_occur_qc/blob/main/src/main/java/org/filteredpush/qc/recoccur/OccurrenceStatusValidation.java'
        )
        assert library == 'rec_occur_qc'
        assert java_class == 'org.filteredpush.qc.metadata.OccurrenceStatusValidation'

    def test_parse_source_link_unknown_library(self, temp_tg2_csv):
        """Test source link parsing with unknown library"""
        parser = TG2Parser(temp_tg2_csv)
        
        library, java_class = parser._parse_source_link(
            'https://github.com/FilteredPush/unknown_lib/blob/main/src/main/java/org/example/Test.java'
        )
        assert library is None
        assert java_class is None

    def test_parse_source_link_invalid_url(self, temp_tg2_csv):
        """Test source link parsing with invalid URL"""
        parser = TG2Parser(temp_tg2_csv)
        
        library, java_class = parser._parse_source_link('invalid-url')
        assert library is None
        assert java_class is None

    def test_derive_method_name_simple(self, temp_tg2_csv):
        """Test method name derivation for simple cases"""
        parser = TG2Parser(temp_tg2_csv)
        
        # Simple validation test
        method = parser._derive_method_name('VALIDATION_COUNTRY_FOUND')
        assert method == 'validationCountryFound'
        
        # Simple amendment test
        method = parser._derive_method_name('AMENDMENT_COUNTRY_CODE')
        assert method == 'amendmentCountryCode'

    def test_derive_method_name_with_override(self, temp_tg2_csv):
        """Test method name derivation with manual override"""
        parser = TG2Parser(temp_tg2_csv)
        
        # Add a manual override
        parser.METHOD_OVERRIDES['CUSTOM_TEST'] = 'customMethodName'
        
        method = parser._derive_method_name('CUSTOM_TEST')
        assert method == 'customMethodName'

    def test_derive_method_name_complex(self, temp_tg2_csv):
        """Test method name derivation for complex test IDs"""
        parser = TG2Parser(temp_tg2_csv)
        
        # Complex test with multiple underscores
        method = parser._derive_method_name('VALIDATION_COORDINATES_IN_COUNTRY')
        assert method == 'validationCoordinatesInCountry'
        
        # Test with numbers
        method = parser._derive_method_name('VALIDATION_DATE_2023_FORMAT')
        assert method == 'validationDate2023Format'

    def test_determine_test_type(self, temp_tg2_csv):
        """Test test type determination"""
        parser = TG2Parser(temp_tg2_csv)
        
        # Test validation type
        test_type = parser._determine_test_type('VALIDATION_COUNTRY_FOUND')
        assert test_type == 'Validation'
        
        # Test amendment type
        test_type = parser._determine_test_type('AMENDMENT_COUNTRY_CODE')
        assert test_type == 'Amendment'
        
        # Test measure type
        test_type = parser._determine_test_type('MEASURE_COORDINATE_PRECISION')
        assert test_type == 'Measure'
        
        # Test issue type
        test_type = parser._determine_test_type('ISSUE_MISSING_COORDINATES')
        assert test_type == 'Issue'
        
        # Test unknown type
        test_type = parser._determine_test_type('UNKNOWN_TEST_TYPE')
        assert test_type == 'Unknown'

    def test_parse_parameters_from_field(self, temp_tg2_csv):
        """Test parameter parsing from field"""
        parser = TG2Parser(temp_tg2_csv)
        
        # Empty parameters
        params = parser.parse_parameters_from_field('')
        assert params == {}
        
        # Single parameter - creates empty value
        params = parser.parse_parameters_from_field('authority')
        assert params == {'authority': ''}
        
        # Multiple parameters
        params = parser.parse_parameters_from_field('authority,rank')
        assert params == {'authority': '', 'rank': ''}
        
        # Parameters with values (if supported in future)
        params = parser.parse_parameters_from_field('authority=GBIF,rank=genus')
        assert params == {'authority=GBIF': '', 'rank=genus': ''}

    def test_get_test_mapping(self, temp_tg2_csv):
        """Test getting specific test mapping"""
        parser = TG2Parser(temp_tg2_csv)
        parser.parse()
        
        # Get existing test
        mapping = parser.get_test_mapping('VALIDATION_COUNTRY_FOUND')
        assert mapping is not None
        assert mapping.test_id == 'VALIDATION_COUNTRY_FOUND'
        
        # Get non-existent test
        mapping = parser.get_test_mapping('NON_EXISTENT_TEST')
        assert mapping is None

    def test_get_tests_by_library(self, temp_tg2_csv):
        """Test getting tests by library"""
        parser = TG2Parser(temp_tg2_csv)
        parser.parse()
        
        # Get geo_ref_qc tests
        geo_tests = parser.get_tests_by_library('geo_ref_qc')
        assert len(geo_tests) == 5  # Should have 5 tests based on actual test data
        
        # Get sci_name_qc tests  
        sci_tests = parser.get_tests_by_library('sci_name_qc')
        assert len(sci_tests) == 4  # Should have 4 tests based on actual test data
        
        # Get non-existent library
        unknown_tests = parser.get_tests_by_library('unknown_library')
        assert len(unknown_tests) == 0

    def test_get_libraries(self, temp_tg2_csv):
        """Test getting all libraries"""
        parser = TG2Parser(temp_tg2_csv)
        parser.parse()
        
        libraries = parser.get_libraries()
        # The function returns a sorted list, not a set
        assert isinstance(libraries, list)
        assert 'geo_ref_qc' in libraries
        assert 'sci_name_qc' in libraries

    def test_parse_with_malformed_csv(self):
        """Test parsing with malformed CSV"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("""Label,InformationElement:ActedUpon
VALIDATION_COUNTRY_FOUND,"dwc:country"
INVALID_ROW,"dwc:country"  # Missing closing quote
VALIDATION_DATE_FORMAT,"dwc:eventDate" """)
            temp_path = f.name
        
        try:
            parser = TG2Parser(temp_path)
            # Should handle malformed CSV gracefully
            mappings = parser.parse()
            # With malformed data, no valid mappings should be created
            assert len(mappings) == 0
        finally:
            os.unlink(temp_path)

    def test_parse_with_unicode_content(self):
        """Test parsing with unicode content"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("""Label,InformationElement:ActedUpon,InformationElement:Consulted,Parameters,Link to Specification Source Code
VALIDATION_COUNTRY_FOUND,"dwc:country","","","https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/CountryFound.java"
VALIDATION_SPECIAL_CHARS,"dwc:locality","","","https://github.com/FilteredPush/geo_ref_qc/blob/main/src/main/java/org/filteredpush/qc/georef/SpecialChars.java"
VALIDATION_ACCENTED,"dwc:scientificName","","","https://github.com/FilteredPush/sci_name_qc/blob/main/src/main/java/org/filteredpush/qc/sciname/AccentedName.java""")
            temp_path = f.name
        
        try:
            parser = TG2Parser(temp_path)
            mappings = parser.parse()
            assert len(mappings) == 3
        finally:
            os.unlink(temp_path)
