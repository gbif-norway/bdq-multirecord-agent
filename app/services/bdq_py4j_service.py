"""
Py4J-based BDQ Service - Subprocess Py4J gateway for fast execution with test discovery
"""
import logging
import time
import subprocess
import json
import tempfile
import os
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
        """Start Py4J gateway as subprocess"""
        java_opts = os.getenv('BDQ_JAVA_OPTS', '-Xms256m -Xmx1024m -XX:+UseSerialGC')
        gateway_jar = os.getenv('BDQ_PY4J_GATEWAY_JAR', '/opt/bdq/bdq-py4j-gateway.jar')
        
        java_cmd = ['java'] + java_opts.split() + ['-jar', gateway_jar]
        log(f"Starting Py4J gateway: {' '.join(java_cmd)}")
        
        try:
            # Start the Java process (no need to read port from stdout)
            process = subprocess.Popen(
                java_cmd,
                stdout=subprocess.DEVNULL,  # Don't capture stdout
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Use the hardcoded port that matches the Java gateway
            port = 25333
            log(f"Py4J gateway starting on port: {port}")
            
            # Give the gateway time to fully start up
            time.sleep(10)
            
            # Connect to the gateway
            self.gateway = JavaGateway(gateway_parameters=GatewayParameters(port=port))
            log(f"Java version: {self.gateway.jvm.System.getProperty('java.version')}")
            log(f"BDQ Gateway health: {self.gateway.entry_point.healthCheck()}")
            
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
        """Use Py4J reflection to find method by annotation label"""
        # List of Java classes to scan (matching bdqtestrunner)
        java_classes = [
            'org.filteredpush.qc.metadata.DwCMetadataDQ',
            'org.filteredpush.qc.metadata.DwCMetadataDQDefaults', 
            'org.filteredpush.qc.georeference.DwCGeoRefDQ',
            'org.filteredpush.qc.georeference.DwCGeoRefDQDefaults',
            'org.filteredpush.qc.date.DwCEventDQ',
            'org.filteredpush.qc.date.DwCEventDQDefaults',
            'org.filteredpush.qc.date.DwCOtherDateDQ',
            'org.filteredpush.qc.date.DwCOtherDateDQDefaults',
            'org.filteredpush.qc.sciname.DwCSciNameDQ',
            'org.filteredpush.qc.sciname.DwCSciNameDQDefaults'
        ]
        
        # Annotation types to look for
        annotation_types = ['Validation', 'Amendment', 'Issue', 'Measure']
        
        try:
            jvm = self.gateway.jvm
            
            for class_name in java_classes:
                try:
                    java_class = jvm.Class.forName(class_name)
                    methods = java_class.getMethods()
                    
                    for method in methods:
                        annotations = method.getAnnotations()
                        
                        for annotation in annotations:
                            annotation_type = annotation.annotationType().getSimpleName()
                            
                            # Check if this is one of our target annotation types
                            if annotation_type in annotation_types:
                                try:
                                    # Get the label value from the annotation
                                    annotation_label = annotation.label()
                                    
                                    # Check if label matches
                                    if annotation_label == label:
                                        # Extract class and method names
                                        declaring_class = method.getDeclaringClass()
                                        class_simple_name = declaring_class.getSimpleName()
                                        method_name = method.getName()
                                        
                                        # Extract library name from package
                                        package_name = declaring_class.getPackage().getName()
                                        library = package_name.split('.')[-1]
                                        
                                        # Map library names to match the repo names
                                        library_map = {
                                            'metadata': 'rec_occur_qc',
                                            'georeference': 'geo_ref_qc', 
                                            'date': 'event_date_qc',
                                            'sciname': 'sci_name_qc'
                                        }
                                        library = library_map.get(library, library)
                                        log(f"Found method {class_simple_name}.{method_name} for label {label}", "DEBUG")
                                        
                                        return {
                                            'library': library,
                                            'class_name': class_simple_name,
                                            'method_name': method_name,
                                            'annotation_type': annotation_type,
                                            'annotation_label': annotation_label
                                        }
                                        
                                except Exception as e:
                                    # Some annotations might not have a label() method
                                    log(f"Error getting label from {annotation_type} annotation: {e}", "DEBUG")
                                    continue
                                    
                except Exception as e:
                    log(f"Error scanning class {class_name}: {e}", "DEBUG")
                    continue
                    
        except Exception as e:
            log(f"Error in _find_method_by_label: {e}", "WARNING")
            
        log(f"No method found for label {label}", "DEBUG")
        return None

    def _get_all_available_methods(self) -> Dict[str, Dict[str, str]]:
        """Get all available methods with their annotation labels"""
        # List of Java classes to scan
        java_classes = [
            'org.filteredpush.qc.metadata.DwCMetadataDQ',
            'org.filteredpush.qc.metadata.DwCMetadataDQDefaults', 
            'org.filteredpush.qc.georeference.DwCGeoRefDQ',
            'org.filteredpush.qc.georeference.DwCGeoRefDQDefaults',
            'org.filteredpush.qc.date.DwCEventDQ',
            'org.filteredpush.qc.date.DwCEventDQDefaults',
            'org.filteredpush.qc.date.DwCOtherDateDQ',
            'org.filteredpush.qc.date.DwCOtherDateDQDefaults',
            'org.filteredpush.qc.sciname.DwCSciNameDQ',
            'org.filteredpush.qc.sciname.DwCSciNameDQDefaults'
        ]
        
        # Annotation types to look for
        annotation_types = ['Validation', 'Amendment', 'Issue', 'Measure']
        
        available_methods = {}
        
        try:
            jvm = self.gateway.jvm
            
            for class_name in java_classes:
                try:
                    java_class = jvm.Class.forName(class_name)
                    methods = java_class.getMethods()
                    
                    for method in methods:
                        annotations = method.getAnnotations()
                        
                        for annotation in annotations:
                            annotation_type = annotation.annotationType().getSimpleName()
                            
                            # Check if this is one of our target annotation types
                            if annotation_type in annotation_types:
                                try:
                                    # Get the label value from the annotation
                                    annotation_label = annotation.label()
                                    
                                    if annotation_label:  # Only include methods with labels
                                        # Extract class and method names
                                        declaring_class = method.getDeclaringClass()
                                        class_simple_name = declaring_class.getSimpleName()
                                        method_name = method.getName()
                                        
                                        # Extract library name from package
                                        package_name = declaring_class.getPackage().getName()
                                        library = package_name.split('.')[-1]
                                        
                                        # Map library names to match the repo names
                                        library_map = {
                                            'metadata': 'rec_occur_qc',
                                            'georeference': 'geo_ref_qc', 
                                            'date': 'event_date_qc',
                                            'sciname': 'sci_name_qc'
                                        }
                                        library = library_map.get(library, library)
                                        
                                        available_methods[annotation_label] = {
                                            'library': library,
                                            'class_name': class_simple_name,
                                            'method_name': method_name,
                                            'annotation_type': annotation_type,
                                            'annotation_label': annotation_label
                                        }
                                        
                                except Exception as e:
                                    # Some annotations might not have a label() method
                                    log(f"Error getting label from {annotation_type} annotation: {e}", "DEBUG")
                                    continue
                                    
                except Exception as e:
                    log(f"Error scanning class {class_name}: {e}", "DEBUG")
                    continue
                    
        except Exception as e:
            log(f"Error in _get_all_available_methods: {e}", "WARNING")
            
        return available_methods
    
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
    
    def execute_single_test(self, java_class: str, java_method: str, acted_upon: List[str], consulted: List[str], tuple_values: List[str]) -> Dict[str, Any]:
        """Execute a single BDQ test for a specific tuple of values"""
        try:
            # Get BDQ gateway
            bdq_gateway = self.gateway.entry_point
            
            # Execute test via Py4J gateway
            result = bdq_gateway.executeTest(
                f"{java_class}.{java_method}",  # test_id
                java_class,
                java_method,
                acted_upon,
                consulted,
                {},  # parameters, we're always going to use the defaults
                [tuple_values]  # single tuple as list
            )
            
            # Convert Java Map to Python dict
            tuple_results = list(result.get("tuple_results", []))
            errors = list(result.get("errors", []))
            
            if errors:
                log(f"Test {java_class}.{java_method} had errors: {errors}", "WARNING")
                return {
                    'status': 'ERROR',
                    'result': None,
                    'comment': f"Test execution error: {', '.join(errors)}",
                    'amendment': None
                }
            
            if tuple_results and len(tuple_results) > 0:
                # Return the first (and only) result
                return tuple_results[0]
            else:
                log(f"Test {java_class}.{java_method} returned no results", "WARNING")
                return {
                    'status': 'NO_RESULT',
                    'result': None,
                    'comment': 'Test returned no results',
                    'amendment': None
                }
                
        except Exception as e:
            log(f"Error executing test {java_class}.{java_method}: {e}", "ERROR")
            return {
                'status': 'ERROR',
                'result': None,
                'comment': f"Test execution error: {str(e)}",
                'amendment': None
            }
    
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
