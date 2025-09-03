import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import patch, Mock

from app.services.csv_service import CSVService
from app.models.email_models import BDQTestResult, BDQTestExecutionResult, BDQTest


class TestCSVService:
    """Test the CSV Service"""

    @pytest.fixture
    def csv_service(self):
        """CSV service instance for testing"""
        return CSVService()

    @pytest.fixture
    def sample_occurrence_df(self):
        """Sample occurrence DataFrame for testing"""
        return pd.DataFrame({
            "occurrenceID": ["occ1", "occ2", "occ3"],
            "eventDate": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "country": ["USA", "Canada", "Mexico"],
            "stateProvince": ["California", "Ontario", "Jalisco"],
            "locality": ["San Francisco", "Toronto", "Guadalajara"],
            "decimalLatitude": [37.7749, 43.6532, 20.6597],
            "decimalLongitude": [-122.4194, -79.3832, -103.3496]
        })

    @pytest.fixture
    def sample_taxon_df(self):
        """Sample taxon DataFrame for testing"""
        return pd.DataFrame({
            "taxonID": ["tax1", "tax2", "tax3"],
            "scientificName": ["Homo sapiens", "Canis lupus", "Felis catus"],
            "genus": ["Homo", "Canis", "Felis"],
            "species": ["sapiens", "lupus", "catus"],
            "taxonRank": ["species", "species", "species"],
            "family": ["Hominidae", "Canidae", "Felidae"]
        })

    @pytest.fixture
    def sample_test_results(self):
        """Sample test results for testing"""
        return BDQTestExecutionResult(
            record_id="occ1",
            test_id="VALIDATION_COUNTRY_FOUND",
            status="RUN_HAS_RESULT",
            result="PASS",
            comment="Country field is valid",
            amendment=None,
            test_type="VALIDATION"
        )

    def test_init(self, csv_service):
        """Test CSV service initialization"""
        assert csv_service is not None

    def test_parse_csv_and_detect_core_occurrence(self, csv_service, sample_occurrence_df):
        """Test CSV parsing and core detection for occurrence data"""
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_occurrence_df.to_csv(f, index=False)
            temp_path = f.name
        
        try:
            with open(temp_path, 'r') as f:
                csv_content = f.read()
            
            df, core_type = csv_service.parse_csv_and_detect_core(csv_content)
            
            assert core_type == "occurrence"
            assert len(df) == 3
            assert "occurrenceID" in df.columns
            assert "country" in df.columns
            assert "eventDate" in df.columns
            
        finally:
            os.unlink(temp_path)

    def test_parse_csv_and_detect_core_taxon(self, csv_service, sample_taxon_df):
        """Test CSV parsing and core detection for taxon data"""
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_taxon_df.to_csv(f, index=False)
            temp_path = f.name
        
        try:
            with open(temp_path, 'r') as f:
                csv_content = f.read()
            
            df, core_type = csv_service.parse_csv_and_detect_core(csv_content)
            
            assert core_type == "taxon"
            assert len(df) == 3
            assert "taxonID" in df.columns
            assert "scientificName" in df.columns
            assert "genus" in df.columns
            
        finally:
            os.unlink(temp_path)

    def test_parse_csv_and_detect_core_neither(self, csv_service):
        """Test CSV parsing when neither occurrenceID nor taxonID is present"""
        csv_content = """name,description,value
test1,description1,value1
test2,description2,value2"""
        
        df, core_type = csv_service.parse_csv_and_detect_core(csv_content)
        
        assert core_type is None
        assert len(df) == 2
        assert "name" in df.columns

    def test_parse_csv_and_detect_core_both_present(self, csv_service):
        """Test CSV parsing when both occurrenceID and taxonID are present (should default to Occurrence)"""
        csv_content = """occurrenceID,taxonID,scientificName,country
occ1,tax1,Homo sapiens,USA
occ2,tax2,Canis lupus,Canada"""
        
        df, core_type = csv_service.parse_csv_and_detect_core(csv_content)
        
        assert core_type == "occurrence"  # Should default to occurrence
        assert len(df) == 2
        assert "occurrenceID" in df.columns
        assert "taxonID" in df.columns

    def test_parse_csv_different_delimiters(self, csv_service):
        """Test CSV parsing with different delimiters"""
        # Test semicolon delimiter
        csv_content_semicolon = """occurrenceID;country;eventDate
occ1;USA;2023-01-01
occ2;Canada;2023-01-02"""
        
        df, core_type = csv_service.parse_csv_and_detect_core(csv_content_semicolon)
        assert core_type == "occurrence"
        assert len(df) == 2
        
        # Test tab delimiter
        csv_content_tab = """occurrenceID\tcountry\teventDate
occ1\tUSA\t2023-01-01
occ2\tCanada\t2023-01-02"""
        
        df, core_type = csv_service.parse_csv_and_detect_core(csv_content_tab)
        assert core_type == "occurrence"
        assert len(df) == 2
        
        # Test pipe delimiter
        csv_content_pipe = """occurrenceID|country|eventDate
occ1|USA|2023-01-01
occ2|Canada|2023-01-02"""
        
        df, core_type = csv_service.parse_csv_and_detect_core(csv_content_pipe)
        assert core_type == "occurrence"
        assert len(df) == 2

    def test_parse_csv_case_insensitive(self, csv_service):
        """Test that CSV parsing is case insensitive for core detection"""
        csv_content = """OccurrenceID,Country,EventDate
occ1,USA,2023-01-01
occ2,Canada,2023-01-02"""
        
        df, core_type = csv_service.parse_csv_and_detect_core(csv_content)
        assert core_type == "occurrence"
        assert len(df) == 2

    def test_parse_csv_with_quotes(self, csv_service):
        """Test CSV parsing with quoted fields"""
        csv_content = """occurrenceID,country,locality
"occ1","USA","San Francisco, CA"
"occ2","Canada","Toronto, ON"
"occ3","Mexico","Guadalajara, JAL\""""
        
        df, core_type = csv_service.parse_csv_and_detect_core(csv_content)
        assert core_type == "occurrence"
        assert len(df) == 3
        assert df.iloc[0]["locality"] == "San Francisco, CA"

    def test_parse_csv_with_unicode(self, csv_service):
        """Test CSV parsing with unicode characters"""
        csv_content = """occurrenceID,country,locality
occ1,USA,San Francisco
occ2,Canada,Toronto
occ3,México,Guadalajara
occ4,España,Madrid"""
        
        df, core_type = csv_service.parse_csv_and_detect_core(csv_content)
        assert core_type == "occurrence"
        assert len(df) == 4
        assert df.iloc[2]["country"] == "México"
        assert df.iloc[3]["country"] == "España"

    def test_generate_raw_results_csv_occurrence(self, csv_service, sample_test_results):
        """Test raw results CSV generation for occurrence data"""
        raw_results = csv_service.generate_raw_results_csv([sample_test_results], "occurrence")
        
        # Should contain test results with occurrence context
        assert "test_id" in raw_results
        assert "occurrenceID" in raw_results
        assert "status" in raw_results
        assert "result" in raw_results
        assert "comment" in raw_results
        assert "amendment" in raw_results
        assert "test_type" in raw_results
        
        # Check that occurrenceID is set correctly
        lines = raw_results.split('\n')
        header = lines[0]
        assert "occurrenceID" in header
        
        # Check first data row
        if len(lines) > 1:
            first_data_row = lines[1]
            assert "occ1" in first_data_row

    def test_generate_raw_results_csv_taxon(self, csv_service, sample_test_results):
        """Test raw results CSV generation for taxon data"""
        raw_results = csv_service.generate_raw_results_csv([sample_test_results], "taxon")
        
        # Check that taxonID is set correctly
        lines = raw_results.split('\n')
        header = lines[0]
        assert "taxonID" in header
        
        # Check first data row
        if len(lines) > 1:
            first_data_row = lines[1]
            assert "occ1" in first_data_row

    def test_generate_raw_results_csv_multiple_tests(self, csv_service):
        """Test raw results CSV generation with multiple tests"""
        results1 = BDQTestExecutionResult(
            record_id="occ1",
            test_id="VALIDATION_COUNTRY_FOUND",
            status="RUN_HAS_RESULT",
            result="PASS",
            comment="Valid",
            amendment=None,
            test_type="VALIDATION"
        )
        
        results2 = BDQTestExecutionResult(
            record_id="occ2",
            test_id="VALIDATION_DATE_FORMAT",
            status="RUN_HAS_RESULT",
            result="FAIL",
            comment="Invalid format",
            amendment=None,
            test_type="VALIDATION"
        )
        
        raw_results = csv_service.generate_raw_results_csv([results1, results2], "occurrence")
        
        # Should contain results from both tests
        assert "VALIDATION_COUNTRY_FOUND" in raw_results
        assert "VALIDATION_DATE_FORMAT" in raw_results
        assert "PASS" in raw_results
        assert "FAIL" in raw_results

    def test_generate_amended_dataset_occurrence(self, csv_service, sample_occurrence_df, sample_test_results):
        """Test amended dataset generation for occurrence data"""
        amended_dataset = csv_service.generate_amended_dataset(sample_occurrence_df, [sample_test_results], "occurrence")
        
        # Should contain original columns (amendments are applied to existing columns)
        lines = amended_dataset.split('\n')
        header = lines[0]
        
        # Check for original columns
        assert "occurrenceID" in header
        assert "country" in header
        assert "eventDate" in header

    def test_generate_amended_dataset_taxon(self, csv_service, sample_taxon_df, sample_test_results):
        """Test amended dataset generation for taxon data"""
        amended_dataset = csv_service.generate_amended_dataset(sample_taxon_df, [sample_test_results], "taxon")
        
        # Should contain original columns (amendments are applied to existing columns)
        lines = amended_dataset.split('\n')
        header = lines[0]
        
        # Check for original columns
        assert "taxonID" in header
        assert "scientificName" in header
        assert "genus" in header

    def test_generate_amended_dataset_no_results(self, csv_service, sample_occurrence_df):
        """Test amended dataset generation with no test results"""
        amended_dataset = csv_service.generate_amended_dataset(sample_occurrence_df, [], "occurrence")
        
        # Should still contain original data
        lines = amended_dataset.split('\n')
        assert len(lines) >= 2  # Header + at least one data row
        
        header = lines[0]
        assert "occurrenceID" in header

    def test_parse_csv_with_missing_values(self, csv_service):
        """Test CSV parsing with missing values"""
        csv_content = """occurrenceID,country,eventDate,locality
occ1,USA,2023-01-01,San Francisco
occ2,,2023-01-02,
occ3,Mexico,,Guadalajara"""
        
        df, core_type = csv_service.parse_csv_and_detect_core(csv_content)
        assert core_type == "occurrence"
        assert len(df) == 3
        
        # Check that missing values are handled properly
        assert pd.isna(df.iloc[1]["country"])
        assert pd.isna(df.iloc[1]["locality"])
        assert pd.isna(df.iloc[2]["eventDate"])

    def test_parse_csv_with_extra_whitespace(self, csv_service):
        """Test CSV parsing with extra whitespace"""
        csv_content = """  occurrenceID  ,  country  ,  eventDate  
  occ1  ,  USA  ,  2023-01-01  
  occ2  ,  Canada  ,  2023-01-02  """
        
        df, core_type = csv_service.parse_csv_and_detect_core(csv_content)
        assert core_type == "occurrence"
        assert len(df) == 2
        
        # Check that whitespace is preserved (CSV parsing doesn't trim)
        assert df.iloc[0]["occurrenceID"] == "  occ1  "
        assert df.iloc[0]["country"] == "  USA  "
        assert df.iloc[1]["country"] == "  Canada  "

    def test_parse_csv_single_row(self, csv_service):
        """Test CSV parsing with single row of data"""
        csv_content = """occurrenceID,country,eventDate
occ1,USA,2023-01-01"""
        
        df, core_type = csv_service.parse_csv_and_detect_core(csv_content)
        assert core_type == "occurrence"
        assert len(df) == 1
        assert df.iloc[0]["occurrenceID"] == "occ1"

    def test_parse_csv_large_dataset(self, csv_service):
        """Test CSV parsing with larger dataset"""
        # Create a larger dataset
        data = []
        for i in range(100):
            data.append({
                "occurrenceID": f"occ{i}",
                "country": f"Country{i % 10}",
                "eventDate": f"2023-01-{(i % 30) + 1:02d}"
            })
        
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f, index=False)
            temp_path = f.name
        
        try:
            with open(temp_path, 'r') as f:
                csv_content = f.read()
            
            parsed_df, core_type = csv_service.parse_csv_and_detect_core(csv_content)
            
            assert core_type == "occurrence"
            assert len(parsed_df) == 100
            assert "occurrenceID" in parsed_df.columns
            assert "country" in parsed_df.columns
            assert "eventDate" in parsed_df.columns
            
        finally:
            os.unlink(temp_path)
