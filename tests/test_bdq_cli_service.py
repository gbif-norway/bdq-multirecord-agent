import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import patch, Mock, mock_open
from pathlib import Path

from app.services.bdq_cli_service import BDQCLIService
from app.services.tg2_parser import TG2Parser, TG2TestMapping


class TestBDQCLIService:
    """Test the BDQ CLI Service"""

    @pytest.fixture
    def mock_tg2_parser(self):
        """Mock TG2 parser"""
        parser = Mock(spec=TG2Parser)
        parser.parse.return_value = {
            "VALIDATION_COUNTRY_FOUND": Mock(
                test_id="VALIDATION_COUNTRY_FOUND",
                library="geo_ref_qc",
                java_class="org.filteredpush.qc.georef.CountryFound",
                java_method="validationCountryFound",
                test_type="VALIDATION",
                acted_upon=["dwc:country"],
                consulted=[],
                parameters=[],
                default_parameters={}
            ),
            "VALIDATION_DATE_FORMAT": Mock(
                test_id="VALIDATION_DATE_FORMAT",
                library="event_date_qc",
                java_class="org.filteredpush.qc.event.DateFormatValidator",
                java_method="validationDateFormat",
                test_type="VALIDATION",
                acted_upon=["dwc:eventDate"],
                consulted=[],
                parameters=[],
                default_parameters={}
            )
        }
        return parser

    @pytest.fixture
    def bdq_service(self, mock_tg2_parser):
        """BDQ service with mocked dependencies"""
        with patch('app.services.bdq_cli_service.TG2Parser') as mock_parser_class:
            mock_parser_class.return_value = mock_tg2_parser
            
            with patch('os.path.exists') as mock_exists:
                mock_exists.return_value = True
                
                service = BDQCLIService(cli_jar_path="/fake/path/bdq-cli.jar")
                return service

    def test_init_success(self, mock_tg2_parser):
        """Test successful service initialization"""
        with patch('app.services.bdq_cli_service.TG2Parser') as mock_parser_class:
            mock_parser_class.return_value = mock_tg2_parser
            
            with patch('os.path.exists') as mock_exists:
                mock_exists.return_value = True
                
                service = BDQCLIService(cli_jar_path="/fake/path/bdq-cli.jar")
                
                assert service.cli_jar_path == "/fake/path/bdq-cli.jar"
                assert service.java_opts == "-Xms256m -Xmx1024m"
                assert len(service.test_mappings) == 2

    def test_init_jar_not_found(self, mock_tg2_parser):
        """Test initialization failure when JAR not found"""
        with patch('app.services.bdq_cli_service.TG2Parser') as mock_parser_class:
            mock_parser_class.return_value = mock_tg2_parser
            
            with patch('os.path.exists') as mock_exists:
                mock_exists.return_value = False
                
                with pytest.raises(FileNotFoundError):
                    BDQCLIService(cli_jar_path="/nonexistent/path/bdq-cli.jar")

    def test_get_available_tests(self, bdq_service):
        """Test getting available tests"""
        tests = bdq_service.get_available_tests()
        
        assert len(tests) == 2
        test_ids = [test.id for test in tests]
        assert "VALIDATION_COUNTRY_FOUND" in test_ids
        assert "VALIDATION_DATE_FORMAT" in test_ids
        
        # Check first test structure
        country_test = next(t for t in tests if t.id == "VALIDATION_COUNTRY_FOUND")
        assert country_test.className == "org.filteredpush.qc.georef.CountryFound"
        assert country_test.type == "VALIDATION"
        assert country_test.actedUpon == ["dwc:country"]

    def test_filter_applicable_tests(self, bdq_service):
        """Test filtering applicable tests based on CSV columns"""
        csv_columns = ["occurrenceID", "dwc:country", "dwc:eventDate"]
        
        applicable_tests = bdq_service.filter_applicable_tests(
            bdq_service.get_available_tests(), 
            csv_columns
        )
        
        # Both tests should be applicable since country and eventDate are present
        assert len(applicable_tests) == 2
        
        # Test with missing columns
        csv_columns_missing = ["occurrenceID", "locality"]
        applicable_tests_missing = bdq_service.filter_applicable_tests(
            bdq_service.get_available_tests(), 
            csv_columns_missing
        )
        
        # No tests should be applicable
        assert len(applicable_tests_missing) == 0

    def test_filter_applicable_tests_case_insensitive(self, bdq_service):
        """Test that column matching is case insensitive"""
        csv_columns = ["occurrenceID", "dwc:country", "dwc:eventDate"]  # Different case
        
        applicable_tests = bdq_service.filter_applicable_tests(
            bdq_service.get_available_tests(), 
            csv_columns
        )
        
        # Both tests should still be applicable due to case-insensitive matching
        assert len(applicable_tests) == 2

    @pytest.mark.asyncio
    async def test_run_tests_on_dataset_success(self, bdq_service):
        """Test successful test execution on dataset"""
        df = pd.DataFrame({
            "occurrenceID": ["occ1", "occ2"],
            "dwc:country": ["USA", "Canada"],
            "dwc:eventDate": ["2023-01-01", "2023-01-02"]
        })
        
        tests = bdq_service.get_available_tests()
        applicable_tests = bdq_service.filter_applicable_tests(tests, df.columns.tolist())
        
        with patch.object(bdq_service, 'execute_tests') as mock_execute:
            mock_execute.return_value = {
                "results": {
                    "VALIDATION_COUNTRY_FOUND": {
                        "tupleResults": [
                            {"tupleIndex": 0, "status": "RUN_HAS_RESULT", "result": "PASS", "comment": "Valid"},
                            {"tupleIndex": 1, "status": "RUN_HAS_RESULT", "result": "PASS", "comment": "Valid"}
                        ]
                    },
                    "VALIDATION_DATE_FORMAT": {
                        "tupleResults": [
                            {"tupleIndex": 0, "status": "RUN_HAS_RESULT", "result": "PASS", "comment": "Valid"},
                            {"tupleIndex": 1, "status": "RUN_HAS_RESULT", "result": "PASS", "comment": "Valid"}
                        ]
                    }
                }
            }
            
            test_results, skipped_tests = await bdq_service.run_tests_on_dataset(
                df, applicable_tests, "Occurrence"
            )
            
            assert len(test_results) == 2
            assert len(skipped_tests) == 0
            assert test_results[0].test_id == "VALIDATION_COUNTRY_FOUND"
            assert test_results[0].status == "RUN_HAS_RESULT"
            assert test_results[1].test_id == "VALIDATION_DATE_FORMAT"
            assert test_results[1].status == "RUN_HAS_RESULT"

    @pytest.mark.asyncio
    async def test_run_tests_on_dataset_with_errors(self, bdq_service):
        """Test test execution with some errors"""
        df = pd.DataFrame({
            "occurrenceID": ["occ1", "occ2"],
            "dwc:country": ["USA", "Canada"],
            "dwc:eventDate": ["2023-01-01", "2023-01-02"]
        })
        
        tests = bdq_service.get_available_tests()
        applicable_tests = bdq_service.filter_applicable_tests(tests, df.columns.tolist())
        
        with patch.object(bdq_service, 'execute_tests') as mock_execute:
            mock_execute.side_effect = Exception("CLI execution failed")
            
            test_results, skipped_tests = await bdq_service.run_tests_on_dataset(
                df, applicable_tests, "Occurrence"
            )
            
            assert len(test_results) == 0
            assert len(skipped_tests) == 2  # Both tests failed

    def test_prepare_test_request(self, bdq_service):
        """Test preparing test request for CLI"""
        test = bdq_service.get_available_tests()[0]  # Get first test
        df = pd.DataFrame({
            "occurrenceID": ["occ1", "occ2"],
            "dwc:country": ["USA", "Canada"]
        })
        
        request = bdq_service._prepare_test_request(test, df, "Occurrence")
        
        assert request["testId"] == test.id
        assert request["actedUpon"] == test.actedUpon
        assert request["consulted"] == test.consulted
        assert len(request["tuples"]) == 2
        assert request["tuples"][0] == ["USA"]  # country value for first row
        assert request["tuples"][1] == ["Canada"]  # country value for second row

    def test_prepare_test_request_case_insensitive(self, bdq_service):
        """Test that column mapping is case insensitive"""
        test = bdq_service.get_available_tests()[0]  # Get first test
        df = pd.DataFrame({
            "occurrenceID": ["occ1"],
            "dwc:country": ["USA"]  # Different case
        })
        
        request = bdq_service._prepare_test_request(test, df, "Occurrence")
        
        assert len(request["tuples"]) == 1
        assert request["tuples"][0] == ["USA"]

    def test_prepare_test_request_missing_columns(self, bdq_service):
        """Test handling of missing columns in test request"""
        test = bdq_service.get_available_tests()[0]  # Get first test
        df = pd.DataFrame({
            "occurrenceID": ["occ1"],
            "locality": ["San Francisco"]  # Missing country column
        })
        
        request = bdq_service._prepare_test_request(test, df, "Occurrence")
        
        assert len(request["tuples"]) == 1
        assert request["tuples"][0] == [""]  # Empty string for missing column

    def test_process_cli_response(self, bdq_service):
        """Test processing CLI response into test results"""
        test = bdq_service.get_available_tests()[0]  # Get first test
        df = pd.DataFrame({
            "occurrenceID": ["occ1", "occ2"],
            "dwc:country": ["USA", "Canada"]
        })
        
        cli_result = {
            "tupleResults": [
                {"tupleIndex": 0, "status": "RUN_HAS_RESULT", "result": "PASS", "comment": "Valid"},
                {"tupleIndex": 1, "status": "RUN_HAS_RESULT", "result": "FAIL", "comment": "Invalid"}
            ]
        }
        
        result = bdq_service._process_cli_response(test, cli_result, df)
        
        assert result.test_id == test.id
        assert result.status == "RUN_HAS_RESULT"
        assert result.result == "PASS"
        assert result.comment == "Valid"

    def test_generate_summary(self, bdq_service):
        """Test summary generation"""
        test_results = [
            Mock(
                successful_records=2,
                failed_records=0
            ),
            Mock(
                successful_records=1,
                failed_records=1
            )
        ]
        skipped_tests = ["TEST_3"]
        
        summary = bdq_service.generate_summary(test_results, 3, skipped_tests)
        
        assert summary.total_records == 3
        assert summary.total_tests_run == 3
        assert summary.skipped_tests == ["TEST_3"]

    @patch('subprocess.run')
    def test_execute_tests_success(self, mock_subprocess, bdq_service):
        """Test successful test execution via CLI"""
        mock_subprocess.return_value = Mock(
            returncode=0,
            stdout="Success",
            stderr=""
        )
        
        test_requests = [{
            "testId": "VALIDATION_COUNTRY_FOUND",
            "actedUpon": ["dwc:country"],
            "consulted": [],
            "parameters": {},
            "tuples": [["USA"]]
        }]
        
        with patch('builtins.open', mock_open()) as mock_file:
            with patch('json.dump') as mock_json_dump:
                with patch('json.load') as mock_json_load:
                    mock_json_load.return_value = {
                        "results": {
                            "VALIDATION_COUNTRY_FOUND": {
                                "tupleResults": [
                                    {"tupleIndex": 0, "status": "RUN_HAS_RESULT", "result": "PASS"}
                                ]
                            }
                        }
                    }
                    
                    result = bdq_service.execute_tests(test_requests)
                    
                    assert "results" in result
                    assert "VALIDATION_COUNTRY_FOUND" in result["results"]

    @patch('subprocess.run')
    def test_execute_tests_cli_failure(self, mock_subprocess, bdq_service):
        """Test CLI execution failure"""
        mock_subprocess.return_value = Mock(
            returncode=1,
            stdout="",
            stderr="CLI error"
        )
        
        test_requests = [{
            "testId": "VALIDATION_COUNTRY_FOUND",
            "actedUpon": ["dwc:country"],
            "consulted": [],
            "parameters": {},
            "tuples": [["USA"]]
        }]
        
        with patch('builtins.open', mock_open()):
            with pytest.raises(RuntimeError, match="CLI execution failed"):
                bdq_service.execute_tests(test_requests)

    @patch('subprocess.run')
    def test_execute_tests_timeout(self, mock_subprocess, bdq_service):
        """Test CLI execution timeout"""
        mock_subprocess.side_effect = TimeoutError("Command timed out")
        
        test_requests = [{
            "testId": "VALIDATION_COUNTRY_FOUND",
            "actedUpon": ["dwc:country"],
            "consulted": [],
            "parameters": {},
            "tuples": [["USA"]]
        }]
        
        with patch('builtins.open', mock_open()):
            with pytest.raises(TimeoutError):
                bdq_service.execute_tests(test_requests)

    def test_run_cli_command(self, bdq_service):
        """Test CLI command construction"""
        with patch('subprocess.run') as mock_subprocess:
            mock_subprocess.return_value = Mock(
                returncode=0,
                stdout="Success",
                stderr=""
            )
            
            result = bdq_service._run_cli("/tmp/input.json", "/tmp/output.json")
            
            # Verify subprocess was called with correct command
            mock_subprocess.assert_called_once()
            call_args = mock_subprocess.call_args[0][0]
            
            assert call_args[0] == "java"
            assert "-jar" in call_args
            assert bdq_service.cli_jar_path in call_args
            assert "--input=/tmp/input.json" in call_args
            assert "--output=/tmp/output.json" in call_args

    def test_run_cli_with_java_opts(self, bdq_service):
        """Test CLI execution with custom Java options"""
        bdq_service.java_opts = "-Xms512m -Xmx2048m"
        
        with patch('subprocess.run') as mock_subprocess:
            mock_subprocess.return_value = Mock(
                returncode=0,
                stdout="Success",
                stderr=""
            )
            
            bdq_service._run_cli("/tmp/input.json", "/tmp/output.json")
            
            call_args = mock_subprocess.call_args[0][0]
            assert "-Xms512m" in call_args
            assert "-Xmx2048m" in call_args

    def test_test_connection_success(self, bdq_service):
        """Test successful CLI connection test"""
        with patch.object(bdq_service, 'execute_tests') as mock_execute:
            mock_execute.return_value = {
                "results": {
                    "VALIDATION_COUNTRY_FOUND": {
                        "tupleResults": [
                            {"tupleIndex": 0, "status": "RUN_HAS_RESULT", "result": "PASS"}
                        ]
                    }
                }
            }
            
            result = bdq_service.test_connection()
            assert result is True

    def test_test_connection_failure(self, bdq_service):
        """Test failed CLI connection test"""
        with patch.object(bdq_service, 'execute_tests') as mock_execute:
            mock_execute.side_effect = Exception("CLI execution failed")
    
            result = bdq_service.test_connection()
            assert result is False

    def test_test_connection_exception(self, bdq_service):
        """Test CLI connection test with exception"""
        with patch.object(bdq_service, 'execute_tests') as mock_execute:
            mock_execute.side_effect = Exception("Connection failed")
            
            result = bdq_service.test_connection()
            assert result is False

    @patch('subprocess.run')
    def test_get_version_info_success(self, mock_subprocess, bdq_service):
        """Test successful version info retrieval"""
        mock_subprocess.return_value = Mock(
            returncode=0,
            stdout="BDQ CLI v1.0.0\nUsage: java -jar bdq-cli.jar [options]",
            stderr=""
        )
        
        with patch.dict(os.environ, {"JAVA_VERSION": "21.0.1"}):
            version_info = bdq_service.get_version_info()
            
            assert version_info["cli_version"] == "1.0.0"
            assert version_info["java_version"] == "21.0.1"
            assert version_info["status"] == "available"

    @patch('subprocess.run')
    def test_get_version_info_failure(self, mock_subprocess, bdq_service):
        """Test failed version info retrieval"""
        mock_subprocess.return_value = Mock(
            returncode=1,
            stdout="",
            stderr="Command not found"
        )
        
        version_info = bdq_service.get_version_info()
        
        assert version_info["cli_version"] == "Unknown"
        assert version_info["status"] == "error"
        assert "Command not found" in version_info["error"]

    def test_get_version_info_exception(self, bdq_service):
        """Test version info retrieval with exception"""
        with patch('subprocess.run') as mock_subprocess:
            mock_subprocess.side_effect = Exception("Subprocess error")
            
            version_info = bdq_service.get_version_info()
            
            assert version_info["cli_version"] == "Unknown"
            assert version_info["status"] == "error"
            assert "Subprocess error" in version_info["error"]
