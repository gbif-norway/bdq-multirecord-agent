import csv
import re
import logging
import os
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import pandas as pd

logger = logging.getLogger(__name__)

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

class TG2TestMapper:
    def __init__(self, bdq_service):
        self.bdq_service = bdq_service
        self.tests = self._load_tests()
    
    def _load_tests(self):
        df = pd.read_csv("/app/TG2_tests.csv", dtype=str).fillna('')
        
        # Filter out measures (they don't have Java implementations)
        df = df[df['Type'] != 'Measure']

        tests = []
        for _, row in df.iterrows():
            # Use Py4J reflection to find the method
            method_info = self._find_method_by_guid(row['MethodGuid'])
            mapping = TG2TestMapping(
                label=row['Label'],
                library=method_info['library'],
                java_class=method_info['class_name'],
                java_method=method_info['method_name'],
                acted_upon=row['InformationElement:ActedUpon'],
                consulted=row['InformationElement:Consulted'],
                test_type=row['Type']
            )
            tests[row['Label']] = mapping
        return tests

    def _find_method_by_guid(self, guid: str) -> Optional[Dict[str, str]]:
        """Use Py4J reflection to find method by GUID"""
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
        
        try:
            jvm = self.bdq_service.gateway.jvm
            
            for class_name in java_classes:
                try:
                    java_class = jvm.Class.forName(class_name)
                    methods = java_class.getMethods()
                    
                    for method in methods:
                        annotations = method.getAnnotations()
                        
                        for annotation in annotations:
                            if annotation.annotationType().getSimpleName() == 'Provides':
                                # Get the GUID value
                                annotation_value = annotation.value()
                                
                                # Check if GUID matches (with or without urn:uuid: prefix)
                                if (annotation_value == guid or annotation_value == f"urn:uuid:{guid}"):
                                    
                                    # Extract class and method names
                                    declaring_class = method.getDeclaringClass()
                                    class_simple_name = declaring_class.getSimpleName()
                                    method_name = method.getName()
                                    
                                    # Extract library name from package
                                    package_name = declaring_class.getPackage().getName()
                                    library = package_name.split('.')[-1]  # e.g., 'georeference' -> 'geo_ref_qc'
                                    
                                    # Map library names to match the repo names
                                    library_map = {
                                        'metadata': 'rec_occur_qc',
                                        'georeference': 'geo_ref_qc', 
                                        'date': 'event_date_qc',
                                        'sciname': 'sci_name_qc'
                                    }
                                    library = library_map.get(library, library)
                                    logger.debug(f"Found method {class_simple_name}.{method_name} for GUID {guid}")
                                    
                                    return {
                                        'library': library,
                                        'class_name': class_simple_name,
                                        'method_name': method_name
                                    }
                                    
                except Exception as e:
                    logger.debug(f"Error scanning class {class_name}: {e}")
                    continue
                    
        except Exception as e:
            logger.warning(f"Error during reflection: {e}")
            
        logger.debug(f"No method found for GUID {guid}")
        return None
    
    def get_applicable_tests_for_dataset(self, columns):
