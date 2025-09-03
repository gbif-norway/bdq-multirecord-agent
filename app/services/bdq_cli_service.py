import json
import subprocess
import tempfile
import os
import logging
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from app.services.tg2_parser import TG2Parser, TG2TestMapping
from app.models.email_models import BDQTest, BDQTestResult, BDQTestExecutionResult, ProcessingSummary

logger = logging.getLogger(__name__)

class BDQCLIService:
    """
    Service for executing BDQ tests using the Java CLI application
    """
    
    def __init__(self, cli_jar_path: str = None, java_opts: str = None, skip_validation: bool = False):
        self.cli_jar_path = cli_jar_path or os.getenv('BDQ_CLI_JAR', '/opt/bdq/bdq-cli.jar')
        self.java_opts = java_opts or os.getenv('BDQ_JAVA_OPTS', '-Xms256m -Xmx1024m')
        self.test_mappings: Dict[str, TG2TestMapping] = {}
        self.skip_validation = skip_validation
        
        # Always load test mappings, even in test mode
        try:
            self._load_test_mappings()
            logger.info(f"Loaded {len(self.test_mappings)} test mappings")
        except Exception as e:
            logger.warning(f"Failed to load test mappings: {e}")
            if not skip_validation:
                raise
        
        if not skip_validation:
            # Validate CLI JAR exists
            if not os.path.exists(self.cli_jar_path):
                raise FileNotFoundError(f"BDQ CLI JAR not found at: {self.cli_jar_path}")
            logger.info(f"BDQ CLI Service initialized with JAR: {self.cli_jar_path}")
        else:
            logger.info(f"BDQ CLI Service initialized in test mode (validation skipped)")
    
    def _load_test_mappings(self):
        """Load test mappings from TG2_tests.csv"""
        try:
            parser = TG2Parser()
            self.test_mappings = parser.parse()
            logger.info(f"Loaded {len(self.test_mappings)} test mappings from TG2_tests.csv")
        except Exception as e:
            logger.error(f"Failed to load test mappings: {e}")
            raise
    
    def get_available_tests(self) -> List[BDQTest]:
        """Get list of all available BDQ tests"""
        tests = []
        for test_id, mapping in self.test_mappings.items():
            test = BDQTest(
                id=test_id,
                guid=f"guid-{test_id}",
                type=mapping.test_type,
                className=mapping.java_class,
                methodName=mapping.java_method,
                actedUpon=mapping.acted_upon,
                consulted=mapping.consulted,
                parameters=mapping.parameters or []
            )
            tests.append(test)
        return tests
    
    def filter_applicable_tests(self, tests: List[BDQTest], csv_columns: List[str]) -> List[BDQTest]:
        """Filter tests to only those applicable to the given CSV columns"""
        applicable_tests = []
        csv_columns_lower = [col.lower() for col in csv_columns]
        
        # Darwin Core term to common CSV column mapping
        dwc_mapping = {
            'dwc:countrycode': ['countrycode', 'country_code', 'countrycode'],
            'dwc:country': ['country'],
            'dwc:dateidentified': ['dateidentified', 'date_identified', 'dateidentified'],
            'dwc:phylum': ['phylum'],
            'dwc:minimumdepthinmeters': ['minimumdepthinmeters', 'min_depth', 'mindepth'],
            'dwc:maximumdepthinmeters': ['maximumdepthinmeters', 'max_depth', 'maxdepth'],
            'dwc:decimallatitude': ['decimallatitude', 'latitude', 'lat', 'decimallatitude'],
            'dwc:decimallongitude': ['decimallongitude', 'longitude', 'lon', 'decimallongitude'],
            'dwc:verbatimcoordinates': ['verbatimcoordinates', 'coordinates', 'coords'],
            'dwc:geodeticdatum': ['geodeticdatum', 'datum'],
            'dwc:scientificname': ['scientificname', 'scientific_name', 'sciname'],
            'dwc:year': ['year'],
            'dwc:month': ['month'],
            'dwc:day': ['day'],
            'dwc:eventdate': ['eventdate', 'event_date', 'date'],
            'dwc:basisofrecord': ['basisofrecord', 'basis_of_record', 'basis'],
            'dwc:occurrenceid': ['occurrenceid', 'occurrence_id', 'id'],
            'dwc:taxonid': ['taxonid', 'taxon_id', 'id']
        }
        
        for test in tests:
            # Check if test requires columns that are present in CSV
            test_columns = test.actedUpon + test.consulted
            test_columns_lower = [col.lower() for col in test_columns]
            
            # Check if all required test columns can be mapped to CSV columns
            all_columns_present = True
            for test_col in test_columns_lower:
                if test_col in dwc_mapping:
                    # Check if any of the mapped CSV columns are present
                    mapped_cols = dwc_mapping[test_col]
                    if not any(mapped_col in csv_columns_lower for mapped_col in mapped_cols):
                        all_columns_present = False
                        break
                else:
                    # Direct match if no mapping exists
                    if test_col not in csv_columns_lower:
                        all_columns_present = False
                        break
            
            if all_columns_present:
                applicable_tests.append(test)
            else:
                logger.debug(f"Skipping test {test.id} - missing columns: {test_columns}")
        
        return applicable_tests
    
    async def run_tests_on_dataset(self, df, applicable_tests: List[BDQTest], core_type: str) -> Tuple[List[BDQTestExecutionResult], List[str]]:
        """Run BDQ tests on the dataset using the CLI (batched, with tuple dedup)."""
        test_results: List[BDQTestExecutionResult] = []
        skipped_tests: List[str] = []

        # Identify record id column once
        id_col = None
        for c in df.columns:
            cl = c.lower()
            if cl == 'occurrenceid' or cl == 'taxonid':
                id_col = c
                break

        prepared_entries = []

        # Build requests for all tests with unique tuples and back-mapping to rows
        for test in applicable_tests:
            try:
                test_columns = test.actedUpon + test.consulted

                # Column mapping with dwc: fallbacks (case-insensitive)
                df_cols_lower = {c.lower(): c for c in df.columns}
                dwc_mapping = {
                    'dwc:countrycode': ['countrycode', 'country_code', 'countrycode'],
                    'dwc:country': ['country'],
                    'dwc:dateidentified': ['dateidentified', 'date_identified', 'dateidentified'],
                    'dwc:phylum': ['phylum'],
                    'dwc:minimumdepthinmeters': ['minimumdepthinmeters', 'min_depth', 'mindepth'],
                    'dwc:maximumdepthinmeters': ['maximumdepthinmeters', 'max_depth', 'maxdepth'],
                    'dwc:decimallatitude': ['decimallatitude', 'latitude', 'lat', 'decimallatitude'],
                    'dwc:decimallongitude': ['decimallongitude', 'longitude', 'lon', 'decimallongitude'],
                    'dwc:verbatimcoordinates': ['verbatimcoordinates', 'coordinates', 'coords'],
                    'dwc:geodeticdatum': ['geodeticdatum', 'datum'],
                    'dwc:scientificname': ['scientificname', 'scientific_name', 'sciname'],
                    'dwc:year': ['year'],
                    'dwc:month': ['month'],
                    'dwc:day': ['day'],
                    'dwc:eventdate': ['eventdate', 'event_date', 'date'],
                    'dwc:basisofrecord': ['basisofrecord', 'basis_of_record', 'basis'],
                    'dwc:occurrenceid': ['occurrenceid', 'occurrence_id', 'id'],
                    'dwc:taxonid': ['taxonid', 'taxon_id', 'id']
                }
                column_mapping: Dict[str, str] = {}
                for tc in test_columns:
                    tcl = tc.lower()
                    if tcl in df_cols_lower:
                        column_mapping[tc] = df_cols_lower[tcl]
                        continue
                    if tcl in dwc_mapping:
                        for alias in dwc_mapping[tcl]:
                            if alias in df_cols_lower:
                                column_mapping[tc] = df_cols_lower[alias]
                                break

                # Build unique tuples and mapping to row indices
                unique_tuples: List[List[str]] = []
                unique_keys_order: List[Tuple[str, ...]] = []
                tuple_to_rows: Dict[Tuple[str, ...], List[int]] = {}

                for idx, row in df.iterrows():
                    values: List[str] = []
                    for tc in test_columns:
                        if tc in column_mapping:
                            val = row[column_mapping[tc]]
                            values.append(str(val) if pd.notna(val) else "")
                        else:
                            values.append("")
                    key = tuple(values)
                    if key not in tuple_to_rows:
                        tuple_to_rows[key] = []
                        unique_tuples.append(list(values))
                        unique_keys_order.append(key)
                    tuple_to_rows[key].append(idx)

                test_request = {
                    "testId": test.id,
                    "actedUpon": test.actedUpon,
                    "consulted": test.consulted,
                    "parameters": test.parameters or {},
                    "tuples": unique_tuples,
                }

                prepared_entries.append({
                    "test": test,
                    "request": test_request,
                    "keys": unique_keys_order,
                    "tuple_to_rows": tuple_to_rows,
                })
            except Exception as e:
                logger.error(f"Error preparing test {test.id}: {e}")
                skipped_tests.append(test.id)

        if not prepared_entries:
            return test_results, skipped_tests

        # Execute all tests in a single CLI call
        try:
            response = self.execute_tests([e["request"] for e in prepared_entries])
        except Exception as e:
            logger.error(f"Batched CLI execution failed: {e}")
            # If batch fails, mark all tests as skipped
            skipped_tests.extend([e["test"].id for e in prepared_entries])
            return test_results, skipped_tests

        # Process response per test, expanding tuple results to all matching rows
        res_map = response.get('results', {}) if isinstance(response, dict) else {}
        for entry in prepared_entries:
            test = entry["test"]
            cli_result = res_map.get(test.id)
            if not cli_result:
                logger.warning(f"No results returned for test {test.id}")
                skipped_tests.append(test.id)
                continue

            tuple_results = cli_result.get('tupleResults') or []
            for i, tr in enumerate(tuple_results):
                if i >= len(entry["keys"]):
                    continue
                key = entry["keys"][i]
                row_indices = entry["tuple_to_rows"].get(key, [])
                for idx in row_indices:
                    try:
                        record_id = str(df.iloc[idx][id_col]) if (id_col is not None) else f"record_{idx}"
                    except Exception:
                        record_id = f"record_{idx}"
                    test_results.append(BDQTestExecutionResult(
                        record_id=record_id,
                        test_id=test.id,
                        status=tr.get('status', 'UNKNOWN'),
                        result=tr.get('result'),
                        comment=tr.get('comment', ''),
                        amendment=None,
                        test_type=test.type
                    ))

        return test_results, skipped_tests
    
    def _prepare_test_request(self, test: BDQTest, df, core_type: str) -> Dict[str, Any]:
        """Prepare a test request for the CLI"""
        # Extract unique tuples for the test
        test_columns = test.actedUpon + test.consulted
        
        # Map CSV columns to test columns (case-insensitive) with dwc: mapping fallbacks
        column_mapping: Dict[str, str] = {}
        df_cols_lower = {c.lower(): c for c in df.columns}
        dwc_mapping = {
            'dwc:countrycode': ['countrycode', 'country_code', 'countrycode'],
            'dwc:country': ['country'],
            'dwc:dateidentified': ['dateidentified', 'date_identified', 'dateidentified'],
            'dwc:phylum': ['phylum'],
            'dwc:minimumdepthinmeters': ['minimumdepthinmeters', 'min_depth', 'mindepth'],
            'dwc:maximumdepthinmeters': ['maximumdepthinmeters', 'max_depth', 'maxdepth'],
            'dwc:decimallatitude': ['decimallatitude', 'latitude', 'lat', 'decimallatitude'],
            'dwc:decimallongitude': ['decimallongitude', 'longitude', 'lon', 'decimallongitude'],
            'dwc:verbatimcoordinates': ['verbatimcoordinates', 'coordinates', 'coords'],
            'dwc:geodeticdatum': ['geodeticdatum', 'datum'],
            'dwc:scientificname': ['scientificname', 'scientific_name', 'sciname'],
            'dwc:year': ['year'],
            'dwc:month': ['month'],
            'dwc:day': ['day'],
            'dwc:eventdate': ['eventdate', 'event_date', 'date'],
            'dwc:basisofrecord': ['basisofrecord', 'basis_of_record', 'basis'],
            'dwc:occurrenceid': ['occurrenceid', 'occurrence_id', 'id'],
            'dwc:taxonid': ['taxonid', 'taxon_id', 'id']
        }
        for test_col in test_columns:
            tc_lower = test_col.lower()
            # Direct match
            if tc_lower in df_cols_lower:
                column_mapping[test_col] = df_cols_lower[tc_lower]
                continue
            # Fallback mapping
            if tc_lower in dwc_mapping:
                for alias in dwc_mapping[tc_lower]:
                    if alias in df_cols_lower:
                        column_mapping[test_col] = df_cols_lower[alias]
                        break
        
        # Extract tuples
        tuples = []
        for _, row in df.iterrows():
            tuple_data = []
            for test_col in test_columns:
                if test_col in column_mapping:
                    value = str(row[column_mapping[test_col]]) if pd.notna(row[column_mapping[test_col]]) else ""
                    tuple_data.append(value)
                else:
                    tuple_data.append("")
            tuples.append(tuple_data)
        
        return {
            "testId": test.id,
            "actedUpon": test.actedUpon,
            "consulted": test.consulted,
            "parameters": test.parameters or {},
            "tuples": tuples
        }
    
    def _process_cli_response(self, test: BDQTest, cli_result: Dict[str, Any], df) -> List[BDQTestExecutionResult]:
        """Process CLI response into a list of BDQTestExecutionResult mapped to each row"""
        results: List[BDQTestExecutionResult] = []
        tuple_results = cli_result.get('tupleResults') or []
        if not tuple_results:
            return results
        # Find the record ID column (occurrenceID/taxonID), case-insensitive
        id_col = None
        for c in df.columns:
            cl = c.lower()
            if cl == 'occurrenceid' or cl == 'taxonid':
                id_col = c
                break
        for tr in tuple_results:
            idx = tr.get('tupleIndex')
            try:
                record_id = str(df.iloc[idx][id_col]) if (id_col is not None) else f"record_{idx}"
            except Exception:
                record_id = f"record_{idx}"
            results.append(BDQTestExecutionResult(
                record_id=record_id,
                test_id=test.id,
                status=tr.get('status', 'UNKNOWN'),
                result=tr.get('result'),
                comment=tr.get('comment', ''),
                amendment=None,
                test_type=test.type
            ))
        return results
    
    def generate_summary(self, test_results: List[BDQTestExecutionResult], total_records: int, skipped_tests: List[str]) -> ProcessingSummary:
        """Generate a summary of test execution results"""
        total_tests = len(test_results) + len(skipped_tests)
        
        # Count tests by status
        successful_tests = len([r for r in test_results if r.status in ['COMPLIANT', 'AMENDED', 'FILLED_IN']])
        failed_tests = len([r for r in test_results if r.status in ['NOT_COMPLIANT', 'NOT_AMENDED']])
        amendments_applied = len([r for r in test_results if r.status in ['AMENDED', 'FILLED_IN']])
        
        return ProcessingSummary(
            total_records=total_records,
            total_tests_run=total_tests,
            validation_failures={},  # CLI doesn't provide detailed failure info
            common_issues=[],
            amendments_applied=amendments_applied,
            skipped_tests=skipped_tests
        )
    
    def execute_tests(self, test_requests: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Execute BDQ tests using the Java CLI
        
        Args:
            test_requests: List of test requests with testId, actedUpon, consulted, parameters, tuples
            
        Returns:
            Dictionary containing test results
        """
        try:
            # Create temporary input file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as input_file:
                request_data = {
                    "requestId": f"cli-{os.getpid()}-{int(os.times()[4])}",
                    "tests": test_requests
                }
                json.dump(request_data, input_file)
                input_file_path = input_file.name
            
            # Create temporary output file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as output_file:
                output_file_path = output_file.name
            
            try:
                # Execute the CLI
                result = self._run_cli(input_file_path, output_file_path)
                
                if result.returncode != 0:
                    logger.error(f"CLI execution failed with return code {result.returncode}")
                    logger.error(f"STDOUT: {result.stdout}")
                    logger.error(f"STDERR: {result.stderr}")
                    raise RuntimeError(f"CLI execution failed: {result.stderr}")
                
                # Read and parse the output
                with open(output_file_path, 'r') as f:
                    response_data = json.load(f)
                
                logger.info(f"Successfully executed {len(test_requests)} tests via CLI")
                return response_data
                
            finally:
                # Clean up temporary files
                try:
                    os.unlink(input_file_path)
                    os.unlink(output_file_path)
                except OSError:
                    pass  # Ignore cleanup errors
                    
        except Exception as e:
            logger.error(f"Error executing BDQ tests via CLI: {e}")
            raise
    
    def _run_cli(self, input_file: str, output_file: str) -> subprocess.CompletedProcess:
        """
        Run the BDQ CLI with input and output files
        
        Args:
            input_file: Path to input JSON file
            output_file: Path to output JSON file
            
        Returns:
            CompletedProcess with execution results
        """
        # Build Java command
        java_cmd = ['java']
        
        # Add Java options if specified
        if self.java_opts:
            java_cmd.extend(self.java_opts.split())
        
        # Add JAR and arguments
        java_cmd.extend([
            '-jar', self.cli_jar_path,
            f'--input={input_file}',
            f'--output={output_file}'
        ])
        
        logger.debug(f"Executing CLI command: {' '.join(java_cmd)}")
        
        # Execute the command
        result = subprocess.run(
            java_cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        return result
    
    def test_connection(self) -> bool:
        """
        Test if the CLI is working by running a simple test
        
        Returns:
            True if CLI is working, False otherwise
        """
        try:
            # Create a simple test request
            test_request = [{
                "testId": "VALIDATION_COUNTRY_FOUND",
                "actedUpon": ["dwc:country"],
                "consulted": [],
                "parameters": {},
                "tuples": [["Test Country"]]
            }]
            
            # Execute the test
            result = self.execute_tests(test_request)
            
            # Check if we got a valid response
            if result and 'results' in result:
                logger.info("CLI connection test successful")
                return True
            else:
                logger.warning("CLI connection test failed - invalid response format")
                return False
                
        except Exception as e:
            logger.error(f"CLI connection test failed: {e}")
            return False
    
    def get_version_info(self) -> Dict[str, str]:
        """
        Get version information about the CLI
        
        Returns:
            Dictionary with version information
        """
        try:
            # Run CLI with help to get version info
            result = subprocess.run(
                ['java', '-jar', self.cli_jar_path, '--help'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                return {
                    "cli_version": "1.0.0",  # This would come from the JAR manifest
                    "java_version": os.getenv('JAVA_VERSION', 'Unknown'),
                    "status": "available"
                }
            else:
                return {
                    "cli_version": "Unknown",
                    "java_version": "Unknown",
                    "status": "error",
                    "error": result.stderr
                }
                
        except Exception as e:
            return {
                "cli_version": "Unknown",
                "java_version": "Unknown",
                "status": "error",
                "error": str(e)
            }
