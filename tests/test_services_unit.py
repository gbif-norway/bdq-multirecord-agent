"""
Unit tests for individual services without importing the main app
"""

import pytest
import base64
import os
from unittest.mock import patch, MagicMock
import pandas as pd


class TestCSVService:
    """Test CSV service functionality"""

    @pytest.fixture
    def csv_service(self):
        from app.services.csv_service import CSVService
        return CSVService()

    @pytest.fixture
    def sample_occurrence_csv(self):
        """Load sample occurrence CSV data"""
        csv_path = os.path.join(os.path.dirname(__file__), "data", "simple_occurrence_dwc.csv")
        with open(csv_path, 'r') as f:
            return f.read()

    @pytest.fixture
    def sample_taxon_csv(self):
        """Load sample taxon CSV data"""
        csv_path = os.path.join(os.path.dirname(__file__), "data", "simple_taxon_dwc.csv")
        with open(csv_path, 'r') as f:
            return f.read()

    @pytest.fixture
    def sample_prefixed_occurrence_csv(self):
        """Load sample prefixed occurrence CSV data"""
        csv_path = os.path.join(os.path.dirname(__file__), "data", "prefixed_occurrence_dwc.csv")
        with open(csv_path, 'r') as f:
            return f.read()

    def test_parse_occurrence_csv(self, csv_service, sample_occurrence_csv):
        """Test parsing occurrence CSV"""
        df, core_type = csv_service.parse_csv_and_detect_core(sample_occurrence_csv)
        assert core_type == "occurrence"
        assert "occurrenceID" in df.columns
        assert len(df) == 5
        assert df.iloc[0]["occurrenceID"] == "occ1"

    def test_parse_taxon_csv(self, csv_service, sample_taxon_csv):
        """Test parsing taxon CSV"""
        df, core_type = csv_service.parse_csv_and_detect_core(sample_taxon_csv)
        assert core_type == "taxon"
        assert "taxonID" in df.columns
        assert len(df) == 3
        assert df.iloc[0]["taxonID"] == "tax1"

    def test_parse_prefixed_occurrence_csv(self, csv_service, sample_prefixed_occurrence_csv):
        """Test parsing prefixed occurrence CSV"""
        df, core_type = csv_service.parse_csv_and_detect_core(sample_prefixed_occurrence_csv)
        assert core_type == "occurrence"
        assert "dwc:occurrenceID" in df.columns
        assert len(df) == 5
        assert df.iloc[0]["dwc:occurrenceID"] == "occ1"

    def test_detect_core_type_occurrence(self, csv_service):
        """Test core type detection for occurrence"""
        columns = ["occurrenceID", "scientificName", "country"]
        core_type = csv_service._detect_core_type(columns)
        assert core_type == "occurrence"

    def test_detect_core_type_taxon(self, csv_service):
        """Test core type detection for taxon"""
        columns = ["taxonID", "scientificName", "kingdom"]
        core_type = csv_service._detect_core_type(columns)
        assert core_type == "taxon"

    def test_detect_core_type_none(self, csv_service):
        """Test core type detection when neither ID is present"""
        columns = ["scientificName", "country", "kingdom"]
        core_type = csv_service._detect_core_type(columns)
        assert core_type is None

    def test_detect_delimiter_comma(self, csv_service):
        """Test delimiter detection for comma-separated values"""
        sample = "col1,col2,col3\nval1,val2,val3"
        delimiter = csv_service._detect_delimiter(sample)
        assert delimiter == ","

    def test_detect_delimiter_semicolon(self, csv_service):
        """Test delimiter detection for semicolon-separated values"""
        sample = "col1;col2;col3\nval1;val2;val3"
        delimiter = csv_service._detect_delimiter(sample)
        assert delimiter == ";"

    def test_detect_delimiter_tab(self, csv_service):
        """Test delimiter detection for tab-separated values"""
        sample = "col1\tcol2\tcol3\nval1\tval2\tval3"
        delimiter = csv_service._detect_delimiter(sample)
        assert delimiter == "\t"


class TestEmailService:
    """Test email service functionality"""

    @pytest.fixture
    def email_service(self):
        with patch.dict('os.environ', {'GMAIL_SEND': 'test', 'HMAC_SECRET': 'test'}):
            from app.services.email_service import EmailService
            return EmailService()

    @pytest.fixture
    def sample_csv_content(self):
        return "occurrenceID,scientificName\nocc1,Homo sapiens\nocc2,Canis lupus"

    def test_extract_csv_attachment_success(self, email_service, sample_csv_content):
        """Test successful CSV attachment extraction"""
        csv_b64 = base64.b64encode(sample_csv_content.encode('utf-8')).decode('utf-8')
        email_data = {
            "attachments": [
                {
                    "filename": "test.csv",
                    "mimeType": "text/csv",
                    "size": len(sample_csv_content),
                    "contentBase64": csv_b64
                }
            ]
        }
        
        extracted_csv = email_service.extract_csv_attachment(email_data)
        assert extracted_csv == sample_csv_content

    def test_extract_csv_attachment_no_csv(self, email_service):
        """Test extraction when no CSV attachment exists"""
        email_data = {
            "attachments": [
                {
                    "filename": "test.pdf",
                    "mimeType": "application/pdf",
                    "size": 100,
                    "contentBase64": base64.b64encode(b"not a csv").decode('utf-8')
                }
            ]
        }
        
        extracted_csv = email_service.extract_csv_attachment(email_data)
        assert extracted_csv is None

    def test_extract_csv_attachment_invalid_base64(self, email_service):
        """Test extraction with invalid base64 data"""
        email_data = {
            "attachments": [
                {
                    "filename": "test.csv",
                    "mimeType": "text/csv",
                    "size": 100,
                    "contentBase64": "invalid-base64-data!!!"
                }
            ]
        }
        
        extracted_csv = email_service.extract_csv_attachment(email_data)
        # The service is designed to be resilient and return decoded content even for invalid base64
        # It uses errors='replace' to handle invalid UTF-8 characters
        assert extracted_csv is not None
        assert isinstance(extracted_csv, str)

    def test_extract_csv_attachment_empty_attachments(self, email_service):
        """Test extraction with empty attachments list"""
        email_data = {"attachments": []}
        extracted_csv = email_service.extract_csv_attachment(email_data)
        assert extracted_csv is None

    def test_generate_hmac_signature(self, email_service):
        """Test HMAC signature generation"""
        body = "test body"
        signature = email_service._generate_hmac_signature(body)
        assert signature.startswith("sha256=")
        assert len(signature) > 10  # sha256= + 64 hex chars

    def test_generate_hmac_signature_no_secret(self, email_service):
        """Test HMAC signature generation without secret"""
        email_service.hmac_secret = None
        with pytest.raises(ValueError, match="HMAC_SECRET environment variable not set"):
            email_service._generate_hmac_signature("test body")


class TestHelperFunctions:
    """Test helper utility functions"""

    def test_get_unique_tuples(self):
        """Test unique tuple generation"""
        from app.utils.helper import get_unique_tuples
        
        df = pd.DataFrame({
            'col1': ['A', 'A', 'B', 'B'],
            'col2': ['X', 'Y', 'X', 'Y']
        })
        
        unique_tuples = get_unique_tuples(df, ['col1'], ['col2'])
        assert len(unique_tuples) == 4
        assert ['A', 'X'] in unique_tuples
        assert ['A', 'Y'] in unique_tuples
        assert ['B', 'X'] in unique_tuples
        assert ['B', 'Y'] in unique_tuples

    def test_generate_summary_statistics(self):
        """Test summary statistics generation"""
        from app.utils.helper import generate_summary_statistics, BDQTestExecutionResult
        
        # Create mock test results
        test_results = [
            BDQTestExecutionResult(
                record_id="occ1",
                test_id="test1",
                test_type="Validation",
                status="RUN_HAS_RESULT",
                result="COMPLIANT",
                comment="Test passed",
                amendment=None
            ),
            BDQTestExecutionResult(
                record_id="occ2",
                test_id="test1",
                test_type="Validation",
                status="RUN_HAS_RESULT",
                result="NOT_COMPLIANT",
                comment="Test failed",
                amendment=None
            )
        ]
        
        df = pd.DataFrame({
            'occurrenceID': ['occ1', 'occ2'],
            'scientificName': ['Homo sapiens', 'Canis lupus']
        })
        
        stats = generate_summary_statistics(test_results, df, "occurrence")
        assert 'total_records' in stats
        assert 'total_tests_run' in stats
        assert 'validation_failures' in stats
        assert 'amendments_applied' in stats
        assert 'common_issues' in stats
        assert stats['total_records'] == 2
        assert stats['total_tests_run'] == 2
        assert stats['validation_failures'] == 1  # One NOT_COMPLIANT result
