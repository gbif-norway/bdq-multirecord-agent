"""
Simple functionality tests for BDQ email processing service.

These tests focus on the core functionality without complex mocking.
"""

import pytest
import json
import base64
import pandas as pd
from fastapi.testclient import TestClient
from io import StringIO

from app.main import app
from app.services.csv_service import CSVService


def test_health_check():
    """Test health check endpoint"""
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert "BDQ Email Report Service is running" in response.json()["message"]


def test_incoming_email_accepts_immediately():
    """Test that incoming email endpoint returns 200 immediately"""
    client = TestClient(app)
    
    sample_email = {
        "messageId": "test-123",
        "threadId": "thread-456",
        "headers": {"from": "test@example.com"},
        "attachments": [{
            "filename": "data.csv",
            "mimeType": "text/csv", 
            "contentBase64": base64.b64encode("test,data\n1,2".encode('utf-8')).decode('utf-8')
        }]
    }
    
    response = client.post("/email/incoming", json=sample_email)
    
    assert response.status_code == 200
    assert response.json()["status"] == "accepted"


def test_invalid_json_returns_400():
    """Test invalid JSON returns 400"""
    client = TestClient(app)
    response = client.post("/email/incoming", data="not json")
    
    assert response.status_code == 400
    assert "Invalid JSON" in response.json()["message"]


def test_csv_service_parsing():
    """Test CSV service parsing functionality"""
    csv_service = CSVService()
    
    # Test occurrence CSV
    occurrence_csv = """occurrenceID,eventDate,country,countryCode,decimalLatitude,decimalLongitude,scientificName,basisOfRecord
occ1,2023-01-01,United States,US,37.7749,-122.4194,Homo sapiens,HumanObservation
occ2,2023-01-02,Canada,CA,43.6532,-79.3832,Canis lupus,HumanObservation"""
    
    df, core_type = csv_service.parse_csv_and_detect_core(occurrence_csv)
    
    assert core_type == 'occurrence'
    assert len(df) == 2
    assert 'dwc:occurrenceID' in df.columns  # Should have dwc: prefix
    assert 'occurrenceID' not in df.columns  # Original column should be renamed
    
    # Test taxon CSV
    taxon_csv = """taxonID,scientificName,kingdom,phylum,class,order,family,genus,specificEpithet,taxonRank,scientificNameAuthorship
tax1,Homo sapiens,Animalia,Chordata,Mammalia,Primates,Hominidae,Homo,sapiens,species,Linnaeus 1758"""
    
    df, core_type = csv_service.parse_csv_and_detect_core(taxon_csv)
    
    assert core_type == 'taxon'
    assert len(df) == 1
    assert 'dwc:taxonID' in df.columns  # Should have dwc: prefix
    assert 'taxonID' not in df.columns  # Original column should be renamed
    
    # Test invalid CSV
    invalid_csv = "name,value\nJohn,25"
    df, core_type = csv_service.parse_csv_and_detect_core(invalid_csv)
    
    assert core_type is None
    assert len(df) == 1


def test_csv_service_generation():
    """Test CSV service generation functionality"""
    csv_service = CSVService()
    
    # Create test data
    test_results = pd.DataFrame([
        {
            'dwc:occurrenceID': 'occ1',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'COMPLIANT',
            'comment': 'Valid country code'
        },
        {
            'dwc:occurrenceID': 'occ2',
            'test_id': 'VALIDATION_COORDINATES_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'NOT_COMPLIANT',
            'comment': 'Invalid coordinates'
        }
    ])
    
    # Test raw results CSV generation
    raw_csv = csv_service.generate_raw_results_csv(test_results)
    raw_df = pd.read_csv(StringIO(raw_csv))
    
    assert len(raw_df) == 2
    assert 'dwc:occurrenceID' in raw_df.columns
    assert 'test_id' in raw_df.columns
    assert 'test_type' in raw_df.columns
    assert 'status' in raw_df.columns
    assert 'result' in raw_df.columns
    assert 'comment' in raw_df.columns
    
    # Test amended dataset generation
    original_df = pd.DataFrame([
        {'dwc:occurrenceID': 'occ1', 'dwc:countryCode': 'US'},
        {'dwc:occurrenceID': 'occ2', 'dwc:countryCode': 'CA'}
    ])
    
    amended_csv = csv_service.generate_amended_dataset(original_df, test_results, 'occurrence')
    amended_df = pd.read_csv(StringIO(amended_csv))
    
    assert len(amended_df) == 2
    assert 'dwc:occurrenceID' in amended_df.columns
    assert 'dwc:countryCode' in amended_df.columns


def test_csv_service_with_prefixed_columns():
    """Test CSV service with dwc: prefixed columns"""
    csv_service = CSVService()
    
    # Test with dwc: prefixed columns
    prefixed_csv = """dwc:occurrenceID,dwc:eventDate,dwc:country,dwc:countryCode,dwc:decimalLatitude,dwc:decimalLongitude,dwc:scientificName,dwc:basisOfRecord
occ1,2023-01-01,United States,US,37.7749,-122.4194,Homo sapiens,HumanObservation
occ2,2023-01-02,Canada,CA,43.6532,-79.3832,Canis lupus,HumanObservation"""
    
    df, core_type = csv_service.parse_csv_and_detect_core(prefixed_csv)
    
    assert core_type == 'occurrence'
    assert len(df) == 2
    assert 'dwc:occurrenceID' in df.columns
    # Note: The service adds dwc: prefixes but doesn't create non-prefixed versions for already prefixed columns


def test_email_service_csv_extraction():
    """Test email service CSV extraction functionality"""
    from app.services.email_service import EmailService
    
    email_service = EmailService()
    
    # Test valid CSV extraction
    csv_content = "occurrenceID,eventDate\nocc1,2023-01-01\nocc2,2023-01-02"
    email_data = {
        "attachments": [{
            "filename": "test.csv",
            "mimeType": "text/csv",
            "contentBase64": base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')
        }]
    }
    
    extracted = email_service.extract_csv_attachment(email_data)
    assert extracted == csv_content
    
    # Test no CSV attachment
    email_data_no_csv = {"attachments": []}
    extracted = email_service.extract_csv_attachment(email_data_no_csv)
    assert extracted is None
    
    # Test non-CSV attachment (text/plain is treated as CSV-like by the service)
    email_data_no_csv = {
        "attachments": [{
            "filename": "test.txt",
            "mimeType": "application/pdf",
            "contentBase64": base64.b64encode("not csv".encode('utf-8')).decode('utf-8')
        }]
    }
    extracted = email_service.extract_csv_attachment(email_data_no_csv)
    assert extracted is None


def test_summary_statistics_generation():
    """Test summary statistics generation"""
    from app.main import _get_summary_stats
    
    # Create test results
    test_results = pd.DataFrame([
        {
            'dwc:occurrenceID': 'occ1',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'COMPLIANT',
            'comment': 'Valid country code'
        },
        {
            'dwc:occurrenceID': 'occ2',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'NOT_COMPLIANT',
            'comment': 'Invalid country code'
        },
        {
            'dwc:occurrenceID': 'occ3',
            'test_id': 'AMENDMENT_EVENTDATE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': '2023-01-01T00:00:00',
            'comment': 'Standardized date'
        }
    ])
    
    summary = _get_summary_stats(test_results)
    
    assert summary['total_records'] == 3
    assert summary['total_tests_run'] == 3
    assert summary['unique_tests'] == 2
    assert summary['validation_failures'] == 1
    assert summary['amendments_applied'] == 1
    assert summary['compliant_results'] == 1
    assert summary['success_rate_percent'] == 33.3
    assert 'VALIDATION_COUNTRYCODE_VALID' in summary['failure_counts_by_test']
    assert summary['failure_counts_by_test']['VALIDATION_COUNTRYCODE_VALID'] == 1


def test_empty_summary_statistics():
    """Test summary statistics with empty results"""
    from app.main import _get_summary_stats
    
    # Test with empty DataFrame
    empty_df = pd.DataFrame()
    summary = _get_summary_stats(empty_df)
    assert summary == {}
    
    # Test with None
    summary = _get_summary_stats(None)
    assert summary == {}


def test_summary_statistics_common_issues():
    """Test summary statistics with common issues extraction"""
    from app.main import _get_summary_stats
    
    # Create test results with repeated comments
    test_results = pd.DataFrame([
        {
            'dwc:occurrenceID': 'occ1',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'NOT_COMPLIANT',
            'comment': 'Invalid country code format'
        },
        {
            'dwc:occurrenceID': 'occ2',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'NOT_COMPLIANT',
            'comment': 'Invalid country code format'
        },
        {
            'dwc:occurrenceID': 'occ3',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'NOT_COMPLIANT',
            'comment': 'Country code missing'
        },
        {
            'dwc:occurrenceID': 'occ4',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'NOT_COMPLIANT',
            'comment': ''  # Empty comment should be ignored
        }
    ])
    
    summary = _get_summary_stats(test_results)
    
    # Check common issues
    assert 'common_issues' in summary
    assert 'Invalid country code format' in summary['common_issues']
    assert summary['common_issues']['Invalid country code format'] == 2
    assert 'Country code missing' in summary['common_issues']
    assert summary['common_issues']['Country code missing'] == 1
    # Empty comment should not appear
    assert '' not in summary['common_issues']


def test_summary_statistics_no_validation_failures():
    """Test summary statistics when no validation failures occur"""
    from app.main import _get_summary_stats
    
    # Create test results with only compliant results
    test_results = pd.DataFrame([
        {
            'dwc:occurrenceID': 'occ1',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'COMPLIANT',
            'comment': 'Valid country code'
        },
        {
            'dwc:occurrenceID': 'occ2',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'COMPLIANT',
            'comment': 'Valid country code'
        }
    ])
    
    summary = _get_summary_stats(test_results)
    
    assert summary['validation_failures'] == 0
    assert summary['success_rate_percent'] == 100.0
    assert summary['failure_counts_by_test'] == {}
    assert summary['common_issues'] == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
