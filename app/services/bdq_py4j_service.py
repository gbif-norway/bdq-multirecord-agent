"""
Py4J-based BDQ Service - Subprocess Py4J gateway for fast execution
"""
import logging
import time
import subprocess
import json
import tempfile
import os
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.protocol import Py4JNetworkError

from app.services.tg2_parser import TG2Parser, TG2TestMapping
from app.models.email_models import BDQTest, BDQTestResult, BDQTestExecutionResult, ProcessingSummary
from app.utils.logger import send_discord_notification

logger = logging.getLogger(__name__)

class BDQPy4JService:
    """
    Py4J-based BDQ Service - Subprocess gateway for fast execution
    """
    
    def __init__(self, skip_validation: bool = False):
        self.gateway: Optional[JavaGateway] = None
        self.gateway_process: Optional[subprocess.Popen] = None
        self.test_mappings: Dict[str, TG2TestMapping] = {}
        self.skip_validation = skip_validation
        self._jvm_started = False
        
        # Load test mappings
        try:
            self._load_test_mappings()
            logger.info(f"Loaded {len(self.test_mappings)} test mappings")
            if len(self.test_mappings) == 0:
                logger.error("CRITICAL: Zero test mappings loaded - no tests will be available!")
                send_discord_notification("❌ CRITICAL: Zero BDQ test mappings loaded!")
        except Exception as e:
            logger.error(f"Failed to load test mappings: {e}")
            send_discord_notification(f"❌ Failed to load BDQ test mappings: {str(e)}")
            if not skip_validation:
                raise
        
        if not skip_validation:
            self._start_gateway()
    
    def _load_test_mappings(self):
        """Load BDQ test mappings from TG2 specification"""
        tg2_parser = TG2Parser()
        self.test_mappings = tg2_parser.parse()
        logger.info(f"Loaded {len(self.test_mappings)} BDQ test mappings")
    
    def _start_gateway(self):
        """Start Py4J gateway as subprocess"""
        try:
            java_opts = os.getenv('BDQ_JAVA_OPTS', '-Xms256m -Xmx1024m -XX:+UseSerialGC')
            gateway_jar = os.getenv('BDQ_PY4J_GATEWAY_JAR', '/opt/bdq/bdq-py4j-gateway.jar')
            
            # Start the Py4J gateway as a subprocess
            java_cmd = ['java'] + java_opts.split() + ['-jar', gateway_jar]
            
            logger.info(f"Starting Py4J gateway: {' '.join(java_cmd)}")
            self.gateway_process = subprocess.Popen(
                java_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Combine stderr with stdout
                text=True
            )
            
            # Wait for gateway to start and read port from output
            port = None
            for i in range(30):  # Wait up to 30 seconds
                time.sleep(1)
                if self.gateway_process.poll() is not None:
                    # Process has exited
                    stdout, _ = self.gateway_process.communicate()
                    logger.error(f"Py4J gateway process exited early: {stdout}")
                    raise RuntimeError("Py4J gateway process exited early")
                
                # Try to read output
                try:
                    line = self.gateway_process.stdout.readline()
                    if line:
                        logger.info(f"Gateway output: {line.strip()}")
                        # Look for port information in the format PY4J_GATEWAY_PORT=1234
                        if line.startswith("PY4J_GATEWAY_PORT="):
                            port = int(line.split("=")[1].strip())
                            logger.info(f"Found gateway port: {port}")
                            break
                except Exception as e:
                    logger.debug(f"Error reading gateway output: {e}")
                    pass
            
            if port is None:
                raise RuntimeError("Could not determine Py4J gateway port")
            
            # Connect to the gateway on the specific port
            self.gateway = JavaGateway(gateway_parameters=GatewayParameters(port=port))
            
            # Test the connection
            self._test_connection()
            self._jvm_started = True
            logger.info("✅ Py4J gateway started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start Py4J gateway: {e}")
            send_discord_notification(f"❌ Failed to start BDQ Py4J gateway: {str(e)}")
            if not self.skip_validation:
                raise
    
    def _test_connection(self):
        """Test Py4J connection"""
        try:
            # Test basic Java functionality
            java_system = self.gateway.jvm.System
            java_version = java_system.getProperty("java.version")
            logger.info(f"Java version: {java_version}")
            
            # Test BDQ gateway
            bdq_gateway = self.gateway.entry_point
            health = bdq_gateway.healthCheck()
            logger.info(f"BDQ Gateway health: {health}")
            
        except Exception as e:
            logger.error(f"Py4J connection test failed: {e}")
            raise
    
    def get_applicable_tests(self, csv_columns: List[str]) -> List[TG2TestMapping]:
        """Get tests that can be applied to the given CSV columns"""
        applicable_tests = []
        
        for test_id, test_mapping in self.test_mappings.items():
            # Check if all actedUpon columns exist in CSV
            if all(col in csv_columns for col in test_mapping.acted_upon):
                applicable_tests.append(test_mapping)
        
        logger.info(f"Found {len(applicable_tests)} applicable tests from {len(self.test_mappings)} total")
        return applicable_tests
    
    def filter_applicable_tests(self, tests: List[TG2TestMapping], csv_columns: List[str]) -> List[TG2TestMapping]:
        """Filter tests that can be applied to the given CSV columns (alias for get_applicable_tests)"""
        return self.get_applicable_tests(csv_columns)
    
    def get_available_tests(self) -> List[TG2TestMapping]:
        """Get all available tests (for testing compatibility)"""
        return list(self.test_mappings.values())
    
    def run_tests_on_dataset(self, df, csv_columns: List[str]) -> BDQTestExecutionResult:
        """Run tests on dataset (for testing compatibility)"""
        applicable_tests = self.get_applicable_tests(csv_columns)
        return self.execute_tests(df, applicable_tests)
    
    def execute_tests(self, df, applicable_tests: List[TG2TestMapping]) -> BDQTestExecutionResult:
        """Execute BDQ tests using Py4J"""
        if not self._jvm_started:
            raise RuntimeError("Py4J gateway not started - cannot execute tests")
        
        start_time = time.time()
        test_results = []
        skipped_tests = []
        
        logger.info(f"🧪 Starting BDQ test execution on {len(df)} records with {len(applicable_tests)} applicable tests")
        
        # Get BDQ gateway
        bdq_gateway = self.gateway.entry_point
        
        for i, test_mapping in enumerate(applicable_tests):
            try:
                logger.info(f"🔄 [{i+1}/{len(applicable_tests)}] Executing {test_mapping.test_id}...")
                
                # Get unique tuples for this test
                tuples = self._get_unique_tuples(df, test_mapping.acted_upon, test_mapping.consulted)
                
                if not tuples:
                    logger.warning(f"No tuples found for test {test_mapping.test_id}")
                    skipped_tests.append(test_mapping.test_id)
                    continue
                
                # Execute test via Py4J gateway
                result = bdq_gateway.executeTest(
                    test_mapping.test_id,
                    test_mapping.java_class,
                    test_mapping.java_method,
                    test_mapping.acted_upon,
                    test_mapping.consulted,
                    test_mapping.parameters or {},
                    tuples
                )
                
                # Convert Java Map to Python dict
                tuple_results = list(result.get("tuple_results", []))
                errors = list(result.get("errors", []))
                
                if errors:
                    logger.warning(f"Test {test_mapping.test_id} had errors: {errors}")
                
                if tuple_results:
                    # Expand results to all rows
                    row_results = self._expand_tuple_results_to_rows(df, test_mapping, tuple_results)
                    test_results.extend(row_results)
                    logger.info(f"✅ [{i+1}/{len(applicable_tests)}] {test_mapping.test_id}: {len(tuple_results)} results")
                else:
                    logger.warning(f"❌ [{i+1}/{len(applicable_tests)}] {test_mapping.test_id}: No results returned")
                    skipped_tests.append(test_mapping.test_id)
                
            except Exception as e:
                logger.error(f"Error executing test {test_mapping.test_id}: {e}")
                skipped_tests.append(test_mapping.test_id)
        
        execution_time = time.time() - start_time
        logger.info(f"🏁 Py4J test execution completed in {execution_time:.1f} seconds")
        logger.info(f"📊 Results: {len(test_results)} successful, {len(skipped_tests)} skipped")
        
        return BDQTestExecutionResult(
            test_results=test_results,
            skipped_tests=skipped_tests,
            execution_time=execution_time
        )
    
    def _get_unique_tuples(self, df, acted_upon: List[str], consulted: List[str]) -> List[List[str]]:
        """Get unique tuples for test execution"""
        # Combine acted_upon and consulted columns
        all_columns = acted_upon + consulted
        
        # Get unique combinations
        unique_df = df[all_columns].drop_duplicates()
        tuples = unique_df.values.tolist()
        
        logger.debug(f"Found {len(tuples)} unique tuples for columns: {all_columns}")
        return tuples
    
    
    def _expand_tuple_results_to_rows(self, df, test_mapping: TG2TestMapping, tuple_results: List[Dict]) -> List[BDQTestResult]:
        """Expand tuple results to individual row results"""
        row_results = []
        
        for tuple_result in tuple_results:
            tuple_index = tuple_result['tuple_index']
            
            # Find all rows that match this tuple
            all_columns = test_mapping.acted_upon + test_mapping.consulted
            matching_rows = df[df[all_columns].apply(
                lambda row: list(row.values) == tuple_results[tuple_index].get('tuple_values', []), 
                axis=1
            )]
            
            # Create BDQTestResult for each matching row
            for _, row in matching_rows.iterrows():
                bdq_result = BDQTestResult(
                    record_id=str(row.get('occurrenceID', row.get('taxonID', 'unknown'))),
                    test_id=test_mapping.test_id,
                    status=tuple_result['status'],
                    result=tuple_result['result'],
                    comment=tuple_result['comment'],
                    amendment=None  # TODO: Extract amendment if available
                )
                row_results.append(bdq_result)
        
        return row_results
    
    def generate_summary(self, test_results: List[BDQTestResult], total_records: int, skipped_tests: List[str]) -> ProcessingSummary:
        """Generate processing summary"""
        total_tests_run = len(test_results)
        successful_tests = len([r for r in test_results if r.status == 'RUN_HAS_RESULT'])
        
        return ProcessingSummary(
            total_records=total_records,
            total_tests_run=total_tests_run,
            successful_tests=successful_tests,
            skipped_tests=skipped_tests,
            total_test_results=len(test_results)
        )
    
    def shutdown(self):
        """Shutdown Py4J gateway"""
        if self.gateway:
            try:
                self.gateway.shutdown()
                logger.info("Py4J gateway connection closed")
            except Exception as e:
                logger.error(f"Error shutting down Py4J gateway connection: {e}")
            finally:
                self.gateway = None
        
        if self.gateway_process:
            try:
                self.gateway_process.terminate()
                self.gateway_process.wait(timeout=5)
                logger.info("Py4J gateway process terminated")
            except Exception as e:
                logger.error(f"Error terminating Py4J gateway process: {e}")
                try:
                    self.gateway_process.kill()
                except:
                    pass
            finally:
                self.gateway_process = None
                self._jvm_started = False
