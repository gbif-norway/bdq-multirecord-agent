import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient

from app.main import app
from app.services.bdq_py4j_service import BDQPy4JService
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
        record_id="occ1",
        test_id="VALIDATION_COUNTRY_FOUND",
        status="RUN_HAS_RESULT",
        result="PASS",
        comment="Country field is valid",
        amendment=None,
        test_type="VALIDATION"
    )


@pytest.fixture
def sample_test_execution_result(sample_bdq_test, sample_test_result):
    """Sample test execution result for testing"""
    return BDQTestExecutionResult(
        test_results=[sample_test_result],
        skipped_tests=[],
        execution_time=1.5
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
def mock_py4j_response():
    """Mock Py4J response for testing"""
    return {
        "tuple_results": [
            {
                "tuple_index": 0,
                "status": "RUN_HAS_RESULT",
                "result": "PASS",
                "comment": "Country field is valid"
            },
            {
                "tuple_index": 1,
                "status": "RUN_HAS_RESULT",
                "result": "PASS",
                "comment": "Country field is valid"
            }
        ],
        "errors": []
    }
