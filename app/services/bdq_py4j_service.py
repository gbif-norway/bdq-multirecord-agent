"""
Py4J-based BDQ Service - Subprocess Py4J gateway for fast execution with test discovery
"""
import time
import subprocess
import json
import tempfile
import os
import shutil
import select
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass
import pandas as pd

from py4j.java_gateway import JavaGateway, GatewayParameters, launch_gateway
from py4j.protocol import Py4JNetworkError
from app.utils.helper import log


@dataclass
class TG2TestMapping:
    """Represents a TG2Test details with corresponding java class so it can be accessed via Py4J"""
    label: str
    library: str
    java_class: str
    java_method: str
    acted_upon: List[str]
    consulted: List[str]
    test_type: str


class BDQPy4JService:
    """
    Py4J-based BDQ Service - Subprocess gateway for fast execution with test discovery
    """
    
    def __init__(self):
        self.gateway: Optional[JavaGateway] = None
        self.tests: Dict[str, TG2TestMapping] = {}
        self._start_gateway()
        self._load_test_mappings()
    
    def _start_gateway(self):
        """Start Py4J gateway as subprocess with retry and diagnostics"""
        java_opts = os.getenv('BDQ_JAVA_OPTS', '-Xms256m -Xmx1024m -XX:+UseSerialGC')
        gateway_jar = os.getenv('BDQ_PY4J_GATEWAY_JAR', '/opt/bdq/bdq-py4j-gateway.jar')
        startup_timeout = float(os.getenv('BDQ_PY4J_STARTUP_TIMEOUT', '60'))
        retry_interval = float(os.getenv('BDQ_PY4J_RETRY_INTERVAL', '0.5'))

        # Pre-flight diagnostics
        java_path = shutil.which('java')
        if not java_path:
            log("Java binary not found on PATH", "ERROR")
        else:
            log(f"Using java at: {java_path}", "DEBUG")

        if not os.path.exists(gateway_jar):
            log(f"Gateway JAR not found at {gateway_jar}", "ERROR")
        else:
            try:
                st = os.stat(gateway_jar)
                log(f"Gateway JAR present ({gateway_jar}, {st.st_size} bytes)", "DEBUG")
            except Exception:
                pass

        java_cmd = ['java'] + java_opts.split() + ['-jar', gateway_jar]
        log(f"Starting Py4J gateway: {' '.join(java_cmd)}")

        try:
            # Start Java process inheriting container stdio so logs go to platform/logs
            process = subprocess.Popen(java_cmd)

            port = 25333
            log(f"Py4J gateway expected on port: {port}")

            # Wait for gateway to become available with retries
            start_ts = time.time()
            last_err: Optional[Exception] = None
            while time.time() - start_ts < startup_timeout:
                # If the Java process exited early, fail fast
                if process.poll() is not None:
                    msg = f"Py4J gateway process exited with code {process.returncode}. Check container logs for Java stderr/stdout."
                    log(msg, "ERROR")
                    raise RuntimeError(msg)

                try:
                    self.gateway = JavaGateway(
                        gateway_parameters=GatewayParameters(port=port),
                        auto_convert=True
                    )
                    log(f"Java version: {self.gateway.jvm.System.getProperty('java.version')}")
                    log(f"BDQ Gateway health: {self.gateway.entry_point.healthCheck()}")
                    return
                except Exception as e:
                    last_err = e
                    time.sleep(retry_interval)

            # Timed out waiting for gateway
            err_msg = (
                f"Timed out after {startup_timeout}s waiting for Py4J gateway on 127.0.0.1:{port}. "
                f"Last error: {last_err}."
            )
            log(err_msg, "ERROR")
            raise RuntimeError(err_msg)

        except Exception as e:
            log(f"Failed to start Py4J gateway: {e}", "ERROR")
            raise
    
    def _load_test_mappings(self):
        """Load test mappings from TG2_tests.csv and map to Java methods via label-based reflection"""
        df = pd.read_csv("/app/TG2_tests.csv", dtype=str).fillna('')
        
        # No need to filter out measures - they have Java implementations with @Measure annotations
        log(f"Loading test mappings for {len(df)} tests from TG2_tests.csv using label-based discovery...")

        for _, row in df.iterrows():
            # Use Py4J reflection to find the method by label
            method_info = self._find_method_by_label(row['Label'])
            
            if method_info is None:
                log(f"No Java method found for label {row['Label']}", "ERROR")
                continue
                
            # Parse acted_upon and consulted columns (they can be comma-separated)
            acted_upon = [col.strip() for col in row['InformationElement:ActedUpon'].split(',') if col.strip()]
            consulted = [col.strip() for col in row['InformationElement:Consulted'].split(',') if col.strip()]
            
            mapping = TG2TestMapping(
                label=row['Label'],
                library=method_info['library'],
                java_class=method_info['class_name'],
                java_method=method_info['method_name'],
                acted_upon=acted_upon,
                consulted=consulted,
                test_type=row['Type']
            )
            self.tests[row['Label']] = mapping
            
        log(f"Loaded {len(self.tests)} tests from TG2_tests.csv using label-based discovery")


    def _find_method_by_label(self, label: str) -> Optional[Dict[str, str]]:
        """Ask the Java gateway to resolve a label to method info."""
        try:
            info = self.gateway.entry_point.findMethodByLabel(label)
            if info and info.get('class_name') and info.get('method_name'):
                return {
                    'library': info.get('library'),
                    'class_name': info.get('class_name'),
                    'method_name': info.get('method_name'),
                    'annotation_type': info.get('annotation_type'),
                    'annotation_label': info.get('annotation_label')
                }
        except Exception as e:
            log(f"Error in _find_method_by_label via gateway: {e}", "WARNING")
        return None

    def _get_all_available_methods(self) -> Dict[str, Dict[str, str]]:
        """Ask the Java gateway for all labeled methods."""
        try:
            data = self.gateway.entry_point.getAvailableMethodsByLabel()
            out: Dict[str, Dict[str, str]] = {}
            # Convert Java Map to Python dict
            for k in list(data.keys()):
                v = data.get(k)
                if v and v.get('class_name') and v.get('method_name'):
                    out[str(k)] = {
                        'library': v.get('library'),
                        'class_name': v.get('class_name'),
                        'method_name': v.get('method_name'),
                        'annotation_type': v.get('annotation_type'),
                        'annotation_label': v.get('annotation_label')
                    }
            return out
        except Exception as e:
            log(f"Error in _get_all_available_methods via gateway: {e}", "WARNING")
            return {}
    
    def get_applicable_tests_for_dataset(self, columns: List[str]) -> List[TG2TestMapping]:
        """Get tests that are applicable to the dataset based on available columns"""
        applicable_tests = []
        
        for test_label, test_mapping in self.tests.items():
            # Check if all acted_upon columns exist in the dataset
            if all(col in columns for col in test_mapping.acted_upon):
                applicable_tests.append(test_mapping)
            else:
                missing_cols = [col for col in test_mapping.acted_upon if col not in columns]
                log(f"Test {test_label} skipped - missing columns: {missing_cols}", "DEBUG")
        
        log(f"Found {len(applicable_tests)} applicable tests out of {len(self.tests)} total tests")
        return applicable_tests
    
    def execute_tests(self, java_class: str, java_method: str, acted_upon: List[str], consulted: List[str], tuples_batch: List[List[str]]) -> List[Dict[str, Any]]:
        """Execute a BDQ test for a batch of tuples and return per-tuple results.

        Args:
            java_class: Fully-qualified Java class name
            java_method: Method name on the Java class
            acted_upon: List of acted-upon column names (dwc:...)
            consulted: List of consulted column names (dwc:...)
            tuples_batch: List of tuples (each tuple is a list of strings)

        Returns:
            A list of result dicts aligned to tuples_batch order.
        """
        try:
            bdq_gateway = self.gateway.entry_point
            jvm = self.gateway.jvm

            def to_java_list(py_list: List[str]):
                j_list = jvm.java.util.ArrayList()
                for v in py_list:
                    j_list.add(v)
                return j_list

            j_acted = to_java_list(acted_upon)
            j_consulted = to_java_list(consulted)
            j_tuples = jvm.java.util.ArrayList()
            for tup in tuples_batch:
                j_tuples.add(to_java_list(tup))
            j_params = jvm.java.util.HashMap()

            result = bdq_gateway.executeTest(
                f"{java_class}.{java_method}",
                java_class,
                java_method,
                j_acted,
                j_consulted,
                j_params,
                j_tuples
            )

            tuple_results = list(result.get("tuple_results", []))
            errors = list(result.get("errors", []))

            if errors:
                log(f"Test {java_class}.{java_method} had errors: {errors}", "WARNING")
                # Propagate an error result for each tuple to preserve alignment
                return [
                    {
                        'status': 'ERROR',
                        'result': None,
                        'comment': f"Test execution error: {', '.join(errors)}",
                        'amendment': None
                    }
                    for _ in tuples_batch
                ]

            # Ensure alignment with input batch length
            if not tuple_results:
                return [
                    {
                        'status': 'NO_RESULT',
                        'result': None,
                        'comment': 'Test returned no results',
                        'amendment': None
                    }
                    for _ in tuples_batch
                ]

            # Some gateways may return fewer items; pad to match input length
            if len(tuple_results) < len(tuples_batch):
                pad = len(tuples_batch) - len(tuple_results)
                tuple_results.extend([
                    {
                        'status': 'NO_RESULT',
                        'result': None,
                        'comment': 'Test returned no result for tuple',
                        'amendment': None
                    }
                ] * pad)

            return tuple_results[: len(tuples_batch)]

        except Exception as e:
            log(f"Error executing tests {java_class}.{java_method}: {e}", "ERROR")
            return [
                {
                    'status': 'ERROR',
                    'result': None,
                    'comment': f"Test execution error: {str(e)}",
                    'amendment': None
                }
                for _ in tuples_batch
            ]
    
    def shutdown(self):
        """Shutdown Py4J gateway"""
        if self.gateway:
            try:
                self.gateway.shutdown()
                log("Py4J gateway connection closed")
            except Exception as e:
                log(f"Error shutting down Py4J gateway connection: {e}", "ERROR")
            finally:
                self.gateway = None
