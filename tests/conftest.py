import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient

from app.main import app
from app.services.bdq_cli_service import BDQCLIService
from app.services.csv_service import CSVService
from app.services.email_service import EmailService
from app.services.tg2_parser import TG2Parser
from app.models.email_models import EmailPayload, BDQTest, BDQTestResult, BDQTestExecutionResult


@pytest.fixture
def client():
    """Test client for FastAPI app"""
    return TestClient(app)


@pytest.fixture
def sample_csv_data():
    """Sample CSV data for testing"""
    return """occurrenceID,eventDate,country,stateProvince,locality
occ1,2023-01-01,USA,California,San Francisco
occ2,2023-01-02,USA,New York,New York City
occ3,2023-01-03,Canada,Ontario,Toronto"""


@pytest.fixture
def sample_taxon_csv_data():
    """Sample taxon CSV data for testing"""
    return """taxonID,scientificName,genus,species
tax1,Homo sapiens,Homo,sapiens
tax2,Canis lupus,Canis,lupus
tax3,Felis catus,Felis,catus"""


@pytest.fixture
def sample_email_payload():
    """Sample email payload for testing"""
    return EmailPayload(
        message_id="test_msg_123",
        thread_id="test_thread_456",
        from_email="test@example.com",
        to_email="bdq@example.com",
        subject="Test Dataset",
        body_text="Please process this dataset",
        body_html="<p>Please process this dataset</p>",
        attachments=[],
        headers={}
    )


@pytest.fixture
def sample_bdq_test():
    """Sample BDQ test for testing"""
    return BDQTest(
        id="VALIDATION_COUNTRY_FOUND",
        guid="test-guid-123",
        type="Validation",
        className="org.filteredpush.qc.georef.CountryFound",
        methodName="validationCountryFound",
        actedUpon=["dwc:country"],
        consulted=[],
        parameters=[]
    )


@pytest.fixture
def sample_test_result():
    """Sample test result for testing"""
    return BDQTestResult(
        test_id="VALIDATION_COUNTRY_FOUND",
        row_index=0,
        status="RUN_HAS_RESULT",
        result="PASS",
        comment="Country field is valid"
    )


@pytest.fixture
def sample_test_execution_result(sample_bdq_test, sample_test_result):
    """Sample test execution result for testing"""
    return BDQTestExecutionResult(
        test=sample_bdq_test,
        results=[sample_test_result],
        total_records=1,
        successful_records=1,
        failed_records=0
    )


@pytest.fixture
def mock_tg2_parser():
    """Mock TG2 parser for testing"""
    parser = Mock(spec=TG2Parser)
    parser.parse.return_value = {
        "VALIDATION_COUNTRY_FOUND": Mock(
            test_id="VALIDATION_COUNTRY_FOUND",
            name="Country Validation",
            description="Validates that country field is present and valid",
            test_type="VALIDATION",
            acted_upon=["dwc:country"],
            consulted=[],
            parameters=[],
            default_parameters={}
        )
    }
    return parser


@pytest.fixture
def temp_tg2_csv():
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


@pytest.fixture
def mock_cli_response():
    """Mock CLI response for testing"""
    return {
        "requestId": "test-123",
        "results": {
            "VALIDATION_COUNTRY_FOUND": {
                "tupleResults": [
                    {
                        "tupleIndex": 0,
                        "status": "RUN_HAS_RESULT",
                        "result": "PASS",
                        "comment": "Country field is valid"
                    },
                    {
                        "tupleIndex": 1,
                        "status": "RUN_HAS_RESULT",
                        "result": "PASS",
                        "comment": "Country field is valid"
                    }
                ]
            }
        }
    }


@pytest.fixture
def mock_subprocess_result():
    """Mock subprocess result for testing"""
    result = Mock()
    result.returncode = 0
    result.stdout = "CLI execution successful"
    result.stderr = ""
    return result
