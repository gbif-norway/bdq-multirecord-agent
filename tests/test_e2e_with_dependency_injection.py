"""
End-to-end tests with proper dependency injection mocking.

These tests demonstrate the complete email processing flow with mocked services.
"""

import pytest
import json
import base64
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from fastapi.testclient import TestClient
from io import StringIO

from app.main import app


@pytest.fixture
def sample_email():
    """Sample email with occurrence CSV"""
    csv_content = """occurrenceID,eventDate,country,countryCode,decimalLatitude,decimalLongitude,scientificName,basisOfRecord
occ1,2023-01-01,United States,US,37.7749,-122.4194,Homo sapiens,HumanObservation
occ2,2023-01-02,Canada,CA,43.6532,-79.3832,Canis lupus,HumanObservation
occ3,2023-01-03,Mexico,MX,19.4326,-99.1332,Felis catus,HumanObservation
occ4,2023-01-04,France,FR,48.8566,2.3522,Ursus americanus,HumanObservation
occ5,2023-01-05,BadCountry,ZZ,91.0,181.0,InvalidName,BadBasis"""
    
    return {
        "messageId": "test-123",
        "threadId": "thread-456",
        "headers": {
            "from": "test@example.com",
            "subject": "BDQ Test Request"
        },
        "body": {
            "text": "Please test my biodiversity dataset",
            "html": "<p>Please test my biodiversity dataset</p>"
        },
        "attachments": [{
            "filename": "data.csv",
            "mimeType": "text/csv", 
            "contentBase64": base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')
        }]
    }


def test_health_check():
    """Test health check endpoint"""
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert "BDQ Email Report Service is running" in response.json()["message"]


def test_incoming_email_accepts_immediately(sample_email):
    """Test that incoming email endpoint returns 200 immediately"""
    client = TestClient(app)
    response = client.post("/email/incoming", json=sample_email)
    
    assert response.status_code == 200
    assert response.json()["status"] == "accepted"


@pytest.mark.asyncio
async def test_complete_email_processing_flow(sample_email):
    """Test complete email processing flow with mocked services"""
    
    # Create mock services
    mock_email_service = Mock()
    mock_email_service.extract_csv_attachment.return_value = ("""occurrenceID,eventDate,country,countryCode,decimalLatitude,decimalLongitude,scientificName,basisOfRecord
occ1,2023-01-01,United States,US,37.7749,-122.4194,Homo sapiens,HumanObservation
occ2,2023-01-02,Canada,CA,43.6532,-79.3832,Canis lupus,HumanObservation
occ3,2023-01-03,Mexico,MX,19.4326,-99.1332,Felis catus,HumanObservation
occ4,2023-01-04,France,FR,48.8566,2.3522,Ursus americanus,HumanObservation
occ5,2023-01-05,BadCountry,ZZ,91.0,181.0,InvalidName,BadBasis""", "test_dataset.csv")
    mock_email_service.send_error_reply = AsyncMock()
    mock_email_service.send_results_reply = AsyncMock()
    
    mock_bdq_service = Mock()
    mock_test_results = pd.DataFrame([
        {
            'dwc:occurrenceID': 'occ1',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'COMPLIANT',
            'comment': 'Valid country code',
            'actedUpon': 'dwc:countryCode=US',
            'consulted': 'dwc:countryCode=US'
        },
        {
            'dwc:occurrenceID': 'occ5',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'NOT_COMPLIANT',
            'comment': 'Invalid country code: ZZ',
            'actedUpon': 'dwc:countryCode=ZZ',
            'consulted': 'dwc:countryCode=ZZ'
        },
        {
            'dwc:occurrenceID': 'occ5',
            'test_id': 'VALIDATION_COORDINATES_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'NOT_COMPLIANT',
            'comment': 'Invalid coordinates: 91.0, 181.0',
            'actedUpon': 'dwc:decimalLatitude=91.0|dwc:decimalLongitude=181.0',
            'consulted': 'dwc:decimalLatitude=91.0|dwc:decimalLongitude=181.0'
        }
    ])
    mock_bdq_service.run_tests_on_dataset = AsyncMock(return_value=mock_test_results)
    
    # Use real CSV service - no mocking needed
    
    mock_llm_service = Mock()
    mock_llm_service.generate_openai_intelligent_summary.return_value = "<p>BDQ Test Results for occurrence dataset: Found 3 test results with 2 validation failures.</p>"
    mock_llm_service.create_prompt.return_value = "Test prompt"

    mock_minio_service = Mock()
    mock_minio_service.upload_dataframe.return_value = "test_results.csv"
    mock_minio_service.generate_dashboard_url.return_value = "https://example.com/dashboard"
    
    # Mock the module-level services (except CSV service)
    with patch('app.main.email_service', mock_email_service), \
         patch('app.main.bdq_api_service', mock_bdq_service), \
         patch('app.main.llm_service', mock_llm_service), \
         patch('app.main.minio_service', mock_minio_service):
        
        from app.main import _handle_email_processing
        
        # Process email
        await _handle_email_processing(sample_email)
        
        # Verify services were called correctly
        mock_email_service.extract_csv_attachment.assert_called_once_with(sample_email)
        mock_bdq_service.run_tests_on_dataset.assert_called_once()
        mock_llm_service.generate_openai_intelligent_summary.assert_called_once()
        mock_email_service.send_results_reply.assert_called_once()
        
        # Verify the email was sent with correct parameters
        call_args = mock_email_service.send_results_reply.call_args
        assert call_args[0][0] == sample_email  # email_data
        assert "occurrence dataset" in call_args[0][1]  # body


@pytest.mark.asyncio
async def test_no_csv_attachment_error():
    """Test error handling for emails without CSV"""
    
    mock_email_service = Mock()
    mock_email_service.extract_csv_attachment.return_value = (None, None)
    mock_email_service.send_error_reply = AsyncMock()
    
    with patch('app.main.email_service', mock_email_service):
        from app.main import _handle_email_processing
        
        email_data = {
            "messageId": "no-csv",
            "threadId": "thread-no-csv",
            "headers": {"from": "test@example.com"},
            "attachments": []
        }
        
        await _handle_email_processing(email_data)
        
        # Verify error email was sent
        mock_email_service.send_error_reply.assert_called_once()
        call_args = mock_email_service.send_error_reply.call_args
        assert call_args[0][0] == email_data
        assert "No CSV attachment found" in call_args[0][1]


@pytest.mark.asyncio
async def test_invalid_csv_structure_error():
    """Test error handling for CSV without occurrenceID/taxonID"""
    
    mock_email_service = Mock()
    mock_email_service.extract_csv_attachment.return_value = ("name,value\nJohn,25", "test.csv")
    mock_email_service.send_error_reply = AsyncMock()
    
    # Use real CSV service - no mocking needed
    
    with patch('app.main.email_service', mock_email_service):
        
        from app.main import _handle_email_processing
        
        email_data = {
            "messageId": "invalid-csv",
            "threadId": "thread-invalid-csv",
            "headers": {"from": "test@example.com"},
            "attachments": [{
                "filename": "invalid.csv",
                "mimeType": "text/csv",
                "contentBase64": base64.b64encode("name,value\nJohn,25".encode('utf-8')).decode('utf-8')
            }]
        }
        
        await _handle_email_processing(email_data)
        
        # Verify error email was sent
        mock_email_service.send_error_reply.assert_called_once()
        call_args = mock_email_service.send_error_reply.call_args
        assert call_args[0][0] == email_data
        assert "'occurrenceID' or 'taxonID'" in call_args[0][1]


def test_csv_output_validation():
    """Test that CSV outputs have the expected structure"""
    
    # Create realistic test data
    test_results = pd.DataFrame([
        {
            'dwc:occurrenceID': 'occ1',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'COMPLIANT',
            'comment': 'Valid country code',
            'actedUpon': 'dwc:countryCode=US',
            'consulted': 'dwc:countryCode=US'
        },
        {
            'dwc:occurrenceID': 'occ2',
            'test_id': 'VALIDATION_COORDINATES_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'NOT_COMPLIANT',
            'comment': 'Invalid coordinates',
            'actedUpon': 'dwc:decimalLatitude=91.0|dwc:decimalLongitude=181.0',
            'consulted': 'dwc:decimalLatitude=91.0|dwc:decimalLongitude=181.0'
        },
        {
            'dwc:occurrenceID': 'occ3',
            'test_id': 'AMENDMENT_EVENTDATE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': 'dwc:eventDate=2023-01-01T00:00:00',
            'comment': 'Standardized date format',
            'actedUpon': 'dwc:eventDate=2023-01-01',
            'consulted': 'dwc:eventDate=2023-01-01'
        }
    ])
    
    original_df = pd.DataFrame([
        {'dwc:occurrenceID': 'occ1', 'dwc:countryCode': 'US', 'dwc:eventDate': '2023-01-01'},
        {'dwc:occurrenceID': 'occ2', 'dwc:countryCode': 'CA', 'dwc:eventDate': '2023-01-02'},
        {'dwc:occurrenceID': 'occ3', 'dwc:countryCode': 'MX', 'dwc:eventDate': '2023-01-03'}
    ])
    
    from app.services.csv_service import CSVService
    csv_service = CSVService()
    
    # Test raw results CSV
    raw_csv = csv_service.dataframe_to_csv_string(test_results)
    raw_df = pd.read_csv(StringIO(raw_csv))
    
    # Verify structure
    expected_columns = ['dwc:occurrenceID', 'test_id', 'test_type', 'status', 'result', 'comment']
    for col in expected_columns:
        assert col in raw_df.columns, f"Missing column: {col}"
    
    # Verify content
    assert len(raw_df) == 3
    assert 'VALIDATION_COUNTRYCODE_VALID' in raw_df['test_id'].values
    assert 'VALIDATION_COORDINATES_VALID' in raw_df['test_id'].values
    assert 'AMENDMENT_EVENTDATE_STANDARDIZED' in raw_df['test_id'].values
    
    # Check for validation failures
    failures = raw_df[raw_df['result'] == 'NOT_COMPLIANT']
    assert len(failures) == 1
    assert failures.iloc[0]['test_id'] == 'VALIDATION_COORDINATES_VALID'
    
    # Check for amendments
    amendments = raw_df[raw_df['status'] == 'AMENDED']
    assert len(amendments) == 1
    assert amendments.iloc[0]['test_id'] == 'AMENDMENT_EVENTDATE_STANDARDIZED'
    
    # Test amended dataset CSV
    amended_csv = csv_service.generate_amended_dataset(original_df, test_results, 'occurrence')
    amended_df = pd.read_csv(StringIO(amended_csv))
    
    # Verify structure
    assert len(amended_df) == 3  # Same number of rows as original
    assert 'dwc:occurrenceID' in amended_df.columns
    assert 'dwc:countryCode' in amended_df.columns
    assert 'dwc:eventDate' in amended_df.columns


def test_summary_statistics():
    """Test summary statistics generation"""
    from app.main import _get_summary_stats_from_unique_results
    
    def _create_unique_results_from_test_data(test_results_df, core_type):
        """Helper function to create unique results DataFrame from test data"""
        group_cols = [col for col in test_results_df.columns if col != f'dwc:{core_type}ID']
        unique_results = (
            test_results_df
            .groupby(group_cols, dropna=False)
            .size()
            .reset_index()
            .rename(columns={0: "count"})
        )
        return unique_results
    
    test_results = pd.DataFrame([
        {
            'dwc:occurrenceID': 'occ1',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'COMPLIANT',
            'comment': 'Valid country code',
            'actedUpon': 'dwc:countryCode=US',
            'consulted': 'dwc:countryCode=US'
        },
        {
            'dwc:occurrenceID': 'occ2',
            'test_id': 'VALIDATION_COUNTRYCODE_VALID',
            'test_type': 'Validation',
            'status': 'RUN_HAS_RESULT',
            'result': 'NOT_COMPLIANT',
            'comment': 'Invalid country code',
            'actedUpon': 'dwc:countryCode=ZZ',
            'consulted': 'dwc:countryCode=ZZ'
        },
        {
            'dwc:occurrenceID': 'occ3',
            'test_id': 'AMENDMENT_EVENTDATE_STANDARDIZED',
            'test_type': 'Amendment',
            'status': 'AMENDED',
            'result': '2023-01-01T00:00:00',
            'comment': 'Standardized date',
            'actedUpon': 'dwc:eventDate=2023-01-01',
            'consulted': 'dwc:eventDate=2023-01-01'
        }
    ])
    
    unique_results = _create_unique_results_from_test_data(test_results, 'occurrence')
    original_dataset_length = 3
    summary = _get_summary_stats_from_unique_results(unique_results, 'occurrence', original_dataset_length)
    
    # Verify summary statistics
    assert summary['no_of_tests_results'] == 3
    assert summary['no_of_tests_run'] == 2
    assert summary['no_of_non_compliant_validations'] == 1
    assert summary['no_of_amendments'] == 1
    assert summary['no_of_filled_in'] == 0
    assert summary['no_of_issues'] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
