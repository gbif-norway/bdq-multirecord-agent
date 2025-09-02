import json
import logging
import socket
import subprocess
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple, Any
import os
from pathlib import Path

from services.tg2_parser import TG2Parser, TG2TestMapping
from models.email_models import BDQTest, BDQTestResult, TestExecutionResult, ProcessingSummary
from utils.logger import send_discord_notification

logger = logging.getLogger(__name__)

class BDQJVMClient:
    """Client for communicating with the local BDQ JVM server over Unix socket"""
    
    def __init__(self, socket_path: str = "/tmp/bdq_jvm.sock", 
                 java_jar_path: str = "/opt/bdq/bdq-jvm-server.jar"):
        self.socket_path = socket_path
        self.java_jar_path = java_jar_path
        self.jvm_process: Optional[subprocess.Popen] = None
        self.test_mappings: Dict[str, TG2TestMapping] = {}
        self.is_warmed_up = False
        self.last_health_ok = False
        
        # Parse TG2 mappings on startup
        self._load_test_mappings()
        
    def _load_test_mappings(self):
        """Load test mappings from TG2_tests.csv"""
        try:
            parser = TG2Parser()
            self.test_mappings = parser.parse()
            logger.info(f"Loaded {len(self.test_mappings)} test mappings from TG2_tests.csv")
        except Exception as e:
            logger.error(f"Failed to load test mappings: {e}")
            raise
    
    def ensure_jvm_running(self):
        """Ensure the JVM server is running and warmed up"""
        if not self._is_jvm_running():
            self._start_jvm()
            time.sleep(2)  # Give JVM time to start
        
        if not self.is_warmed_up:
            self._warmup_jvm()
    
    def _is_jvm_running(self) -> bool:
        """Check if JVM process is running and socket is responsive"""
        if self.jvm_process is None or self.jvm_process.poll() is not None:
            return False
            
        try:
            # Test socket connection
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                sock.connect(self.socket_path)
                
                # Send health check
                health_request = json.dumps({"health": True}) + "\n"
                sock.send(health_request.encode())
                
                response = sock.recv(1024).decode().strip()
                ok = json.loads(response).get("ok") == True
                self.last_health_ok = ok
                return ok
                
        except Exception:
            return False
    
    def _start_jvm(self):
        """Start the JVM server process"""
        logger.info(f"Starting BDQ JVM server: {self.java_jar_path}")
        
        # Remove existing socket file
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        
        # Build Java command
        java_opts = os.getenv("BDQ_JAVA_OPTS", "-Xms256m -Xmx1024m").split()
        cmd = ["java"] + java_opts + ["-jar", self.java_jar_path, f"--socket={self.socket_path}"]
        
        try:
            self.jvm_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Start thread to monitor JVM output
            threading.Thread(target=self._monitor_jvm_output, daemon=True).start()
            
            logger.info(f"Started BDQ JVM server with PID {self.jvm_process.pid}")
            
        except Exception as e:
            logger.error(f"Failed to start JVM server: {e}")
            raise
    
    def _monitor_jvm_output(self):
        """Monitor JVM process output and log it"""
        if not self.jvm_process:
            return
            
        for line in iter(self.jvm_process.stdout.readline, ''):
            if line:
                logger.info(f"JVM: {line.strip()}")
        
        # Log any errors
        if self.jvm_process.stderr:
            for line in iter(self.jvm_process.stderr.readline, ''):
                if line:
                    logger.error(f"JVM Error: {line.strip()}")
    
    def _warmup_jvm(self):
        """Send test mappings to JVM and warm up reflective lookups"""
        logger.info("Warming up JVM server with test mappings")
        
        try:
            # Convert test mappings to the format expected by JVM
            jvm_mappings = []
            for mapping in self.test_mappings.values():
                jvm_mappings.append({
                    "testId": mapping.test_id,
                    "library": mapping.library,
                    "javaClass": mapping.java_class,
                    "javaMethod": mapping.java_method,
                    "actedUpon": mapping.acted_upon,
                    "consulted": mapping.consulted,
                    "parameters": mapping.parameters,
                    "testType": mapping.test_type
                })
            
            warmup_request = {
                "warmup": True,
                "testMappings": jvm_mappings
            }
            
            response = self._send_request(warmup_request)
            
            if response.get("warmupComplete"):
                self.is_warmed_up = True
                tests_loaded = response.get("testsLoaded", 0)
                logger.info(f"JVM warmup completed, {tests_loaded} tests loaded")
            else:
                raise Exception("Warmup failed")
                
        except Exception as e:
            logger.error(f"Failed to warm up JVM: {e}")
            raise
    
    def _send_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Send JSON request to JVM server and get response with retry/restart."""
        max_attempts = 3
        backoff = 1.0
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                    sock.settimeout(300)  # 5 minute timeout for large requests
                    sock.connect(self.socket_path)
                    # Send request
                    request_json = json.dumps(request_data) + "\n"
                    sock.send(request_json.encode())
                    # Receive response
                    response_data = b""
                    while True:
                        chunk = sock.recv(4096)
                        if not chunk:
                            break
                        response_data += chunk
                        if b"\n" in response_data:
                            break
                    response_str = response_data.decode().strip()
                    return json.loads(response_str)
            except Exception as e:
                last_exc = e
                logger.error(f"Attempt {attempt} failed sending request to JVM: {e}")
                # Restart JVM and warmup before next attempt
                try:
                    self.shutdown()
                except Exception:
                    pass
                try:
                    self.ensure_jvm_running()
                except Exception as start_err:
                    logger.error(f"Failed to restart JVM before retry: {start_err}")
                time.sleep(backoff)
                backoff *= 2
        # Exhausted attempts
        raise last_exc if last_exc else RuntimeError("Unknown JVM request failure")
    
    def get_available_tests(self) -> List[BDQTest]:
        """Get available BDQ tests based on TG2 mappings"""
        tests = []
        
        for mapping in self.test_mappings.values():
            test = BDQTest(
                id=mapping.test_id,
                guid="",  # TG2 doesn't include GUIDs in our mapping
                type=mapping.test_type,
                className=mapping.java_class,
                methodName=mapping.java_method,
                actedUpon=mapping.acted_upon.copy(),
                consulted=mapping.consulted.copy(),
                parameters=mapping.parameters.copy()
            )
            tests.append(test)
        
        logger.info(f"Returning {len(tests)} available tests from TG2 mappings")
        return tests

    def generate_summary(self, test_results: List[TestExecutionResult], total_records: int, skipped_tests: List[str]) -> ProcessingSummary:
        """Generate a ProcessingSummary from test results"""
        total_tests_run = len(test_results)
        validation_failures: Dict[str, int] = {}
        amendments_applied = 0
        for tr in test_results:
            # Count validation failures
            if tr.test_type == "Validation" and (tr.result or "").upper() == "NOT_COMPLIANT":
                validation_failures[tr.test_id] = validation_failures.get(tr.test_id, 0) + 1
            # Count amendments applied
            if tr.status in ("AMENDED", "FILLED_IN") and tr.amendment:
                amendments_applied += 1
        return ProcessingSummary(
            total_records=total_records,
            total_tests_run=total_tests_run,
            validation_failures=validation_failures,
            common_issues=[],
            amendments_applied=amendments_applied,
            skipped_tests=skipped_tests or []
        )
    
    def filter_applicable_tests(self, tests: List[BDQTest], csv_columns: List[str]) -> List[BDQTest]:
        """Filter tests that can be applied to the given CSV columns"""
        # Normalize column names for comparison (remove dwc: prefix, lowercase)
        normalized_columns = set()
        for col in csv_columns:
            normalized = col.lower()
            if normalized.startswith('dwc:'):
                normalized = normalized[4:]
            normalized_columns.add(normalized)
        
        applicable_tests = []
        
        for test in tests:
            # Check if all actedUpon columns exist in CSV
            test_applicable = True
            
            for acted_upon_field in test.actedUpon:
                # Normalize field name
                normalized_field = acted_upon_field.lower()
                if normalized_field.startswith('dwc:'):
                    normalized_field = normalized_field[4:]
                
                if normalized_field not in normalized_columns:
                    test_applicable = False
                    break
            
            if test_applicable:
                applicable_tests.append(test)
        
        logger.info(f"Found {len(applicable_tests)} applicable tests out of {len(tests)} total tests")
        return applicable_tests
    
    async def run_tests_on_dataset(self, df, applicable_tests: List[BDQTest], core_type: str) -> Tuple[List[TestExecutionResult], List[str]]:
        """Run BDQ tests on dataset using the local JVM"""
        self.ensure_jvm_running()
        
        from services.csv_service import CSVService
        csv_service = CSVService()
        
        all_results: List[TestExecutionResult] = []
        skipped_tests: List[str] = []
        
        # Column index for mapping actedUpon to actual DataFrame columns
        col_index = {self._normalize_field_name(c): c for c in df.columns}
        
        # Group tests for batched execution
        test_requests = []
        
        for test in applicable_tests:
            try:
                logger.info(f"Preparing test: {test.id}")
                
                # Resolve actedUpon to actual DataFrame column names
                resolved_cols = []
                for rc in (test.actedUpon or []):
                    norm = self._normalize_field_name(rc)
                    actual = col_index.get(norm)
                    if actual:
                        resolved_cols.append(actual)
                
                if len(resolved_cols) != len(test.actedUpon or []):
                    logger.warning(f"Skipping test {test.id}: required columns not found in CSV")
                    skipped_tests.append(test.id)
                    continue
                
                # Get unique tuples for this test's actedUpon columns
                unique_tuples = csv_service.get_unique_tuples(df, resolved_cols)
                
                if not unique_tuples:
                    logger.warning(f"No unique tuples found for test {test.id}")
                    skipped_tests.append(test.id)
                    continue
                
                # Convert tuples to string lists for JSON serialization
                tuple_lists = [[str(val) if val is not None else "" for val in tup] for tup in unique_tuples]
                
                test_requests.append({
                    "testId": test.id,
                    "actedUpon": test.actedUpon,
                    "consulted": test.consulted or [],
                    "parameters": {},  # TODO: Add parameter support
                    "tuples": tuple_lists
                })
                
            except Exception as e:
                logger.error(f"Error preparing test {test.id}: {e}")
                skipped_tests.append(test.id)
        
        if not test_requests:
            logger.warning("No tests to execute")
            return all_results, skipped_tests
        
        # Execute tests in batches (to avoid overwhelming the JVM)
        batch_size = 5  # Process 5 tests at a time
        
        for i in range(0, len(test_requests), batch_size):
            batch = test_requests[i:i + batch_size]
            
            try:
                request_id = str(uuid.uuid4())
                jvm_request = {
                    "requestId": request_id,
                    "tests": batch
                }
                
                logger.info(f"Executing batch {i//batch_size + 1} with {len(batch)} tests")
                
                # Send Discord notification for progress
                try:
                    test_names = [req["testId"] for req in batch]
                    send_discord_notification(f"Executing BDQ tests: {', '.join(test_names)}")
                except Exception:
                    pass
                
                # Execute batch
                jvm_response = self._send_request(jvm_request)
                
                # Process results
                for test_request in batch:
                    test_id = test_request["testId"]
                    
                    if test_id in jvm_response.get("results", {}):
                        tuple_results = jvm_response["results"][test_id]["tupleResults"]
                        
                        # Convert JVM results back to our format
                        cached_results = {}
                        test_tuples = test_request["tuples"]
                        # Lookup test type once
                        mapping = self.test_mappings.get(test_id)
                        test_type = mapping.test_type if mapping else "Unknown"
                        
                        for tuple_result in tuple_results:
                            tuple_index = tuple_result["tupleIndex"]
                            if tuple_index < len(test_tuples):
                                tuple_key = tuple(test_tuples[tuple_index])
                                status = tuple_result.get("status", "")
                                raw_result = tuple_result.get("result")
                                entry: Dict[str, Any] = {
                                    "status": status,
                                    "comment": tuple_result.get("comment", ""),
                                    "test_type": test_type
                                }
                                # Normalize based on test type
                                if test_type == "Amendment":
                                    # For amendments, treat map-like result as amendment
                                    if isinstance(raw_result, dict):
                                        entry["amendment"] = raw_result
                                        entry["result"] = None
                                    else:
                                        entry["result"] = None
                                else:
                                    # For validations, result should be string like COMPLIANT/NOT_COMPLIANT
                                    entry["result"] = raw_result if isinstance(raw_result, str) else None
                                cached_results[tuple_key] = entry
                        
                        # Find the original test and map results to rows
                        original_test = next((t for t in applicable_tests if t.id == test_id), None)
                        if original_test:
                            # Resolve columns again
                            resolved_cols = []
                            for rc in original_test.actedUpon:
                                norm = self._normalize_field_name(rc)
                                actual = col_index.get(norm)
                                if actual:
                                    resolved_cols.append(actual)
                            
                            test_results = csv_service.map_results_to_rows(
                                df, cached_results, test_id, resolved_cols, core_type
                            )
                            all_results.extend(test_results)
                    else:
                        logger.warning(f"No results returned for test {test_id}")
                        skipped_tests.append(test_id)
                
                # Check for errors
                for error in jvm_response.get("errors", []):
                    logger.error(f"JVM error for test {error['testId']}: {error['error']}")
                    if error["testId"] not in skipped_tests:
                        skipped_tests.append(error["testId"])
                
            except Exception as e:
                logger.error(f"Error executing test batch: {e}")
                # Add all tests in this batch to skipped
                for test_request in batch:
                    if test_request["testId"] not in skipped_tests:
                        skipped_tests.append(test_request["testId"])
        
        logger.info(f"Completed test execution: {len(all_results)} results, {len(skipped_tests)} skipped")
        return all_results, skipped_tests
    
    def _normalize_field_name(self, field_name: str) -> str:
        """Normalize field name for comparison (remove dwc: prefix, lowercase)"""
        normalized = field_name.lower().strip()
        if normalized.startswith('dwc:'):
            normalized = normalized[4:]
        return normalized
    
    def shutdown(self):
        """Shutdown the JVM server"""
        if self.jvm_process:
            logger.info("Shutting down BDQ JVM server")
            self.jvm_process.terminate()
            try:
                self.jvm_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning("JVM process did not terminate gracefully, killing it")
                self.jvm_process.kill()
            
            self.jvm_process = None
            self.is_warmed_up = False
        
        # Clean up socket file
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
