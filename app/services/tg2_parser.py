import csv
import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class TG2TestMapping:
    """Represents a parsed TG2 test mapping from CSV"""
    test_id: str
    library: str
    java_class: str
    java_method: str
    acted_upon: List[str]
    consulted: List[str]
    parameters: List[str]
    test_type: str
    default_parameters: Dict[str, str]
    

class TG2Parser:
    """Parser for TG2_tests.csv to extract test mappings for inline BDQ service"""
    
    # Library identification patterns from source code URLs
    LIBRARY_PATTERNS = {
        'geo_ref_qc': 'geo_ref_qc',
        'event_date_qc': 'event_date_qc', 
        'sci_name_qc': 'sci_name_qc',
        'rec_occur_qc': 'rec_occur_qc'
    }
    
    # Manual overrides for tests that don't follow the convention
    METHOD_OVERRIDES = {
        # Add any specific overrides here if needed during testing
    }
    
    def __init__(self, csv_path: str = "TG2_tests.csv"):
        self.csv_path = csv_path
        self.test_mappings: Dict[str, TG2TestMapping] = {}
        
    def parse(self) -> Dict[str, TG2TestMapping]:
        """Parse the TG2 CSV file and return test mappings"""
        logger.info(f"Parsing TG2 test mappings from {self.csv_path}")
        
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    mapping = self._parse_row(row)
                    if mapping:
                        self.test_mappings[mapping.test_id] = mapping
                        
            logger.info(f"Successfully parsed {len(self.test_mappings)} test mappings")
            return self.test_mappings
            
        except Exception as e:
            logger.error(f"Error parsing TG2 CSV: {e}")
            raise
    
    def _parse_row(self, row: Dict[str, str]) -> Optional[TG2TestMapping]:
        """Parse a single CSV row into a TG2TestMapping"""
        try:
            # Extract basic fields
            test_id = row.get('Label', '').strip()
            if not test_id:
                return None
                
            acted_upon = self._parse_field_list(row.get('InformationElement:ActedUpon', ''))
            consulted = self._parse_field_list(row.get('InformationElement:Consulted', ''))
            parameters = self._parse_field_list(row.get('Parameters', ''))
            
            # Extract library and class from source code link
            source_link = row.get('Link to Specification Source Code', '').strip()
            library, java_class = self._parse_source_link(source_link)
            
            if not library or not java_class:
                logger.warning(f"Could not determine library/class for test {test_id}")
                return None
                
            # Derive method name from test ID
            java_method = self._derive_method_name(test_id)
            
            # Determine test type from ID prefix
            test_type = self._determine_test_type(test_id)
            
            # Parse default parameters
            parameters_field = row.get('Parameters', '').strip()
            default_parameters = self.parse_parameters_from_field(parameters_field)
            
            return TG2TestMapping(
                test_id=test_id,
                library=library,
                java_class=java_class,
                java_method=java_method,
                acted_upon=acted_upon,
                consulted=consulted,
                parameters=parameters,
                test_type=test_type,
                default_parameters=default_parameters
            )
            
        except Exception as e:
            logger.warning(f"Error parsing row for test {test_id}: {e}")
            return None
    
    def _parse_field_list(self, field_value: str) -> List[str]:
        """Parse comma-separated field list, preserving dwc: prefixes"""
        if not field_value or field_value.strip() == '':
            return []
            
        # Split on comma, strip whitespace, filter empty
        fields = [f.strip() for f in field_value.split(',') if f.strip()]
        return fields
    
    def parse_parameters_from_field(self, field_value: str) -> Dict[str, str]:
        """Parse parameters from TG2 Parameters field into a dict of default values"""
        if not field_value or field_value.strip() == '':
            return {}
            
        params = {}
        # Split on comma and parse each parameter
        for param_str in field_value.split(','):
            param_str = param_str.strip()
            if not param_str:
                continue
                
            # Look for pattern: bdq:parameterName default = "value"
            if 'default' in param_str and '=' in param_str:
                # Extract parameter name (before 'default')
                param_name = param_str.split('default')[0].strip()
                # Extract default value (after '=' and inside quotes)
                value_part = param_str.split('=', 1)[1].strip()
                # Remove quotes and extra text
                if '"' in value_part:
                    value = value_part.split('"')[1]
                    params[param_name] = value
                elif "'" in value_part:
                    value = value_part.split("'")[1] 
                    params[param_name] = value
                else:
                    # If no quotes, just use the parameter name with empty value
                    params[param_name] = ""
            else:
                # If no default specified, just add the parameter name with empty value
                params[param_str] = ""
        
        return params
    
    def _parse_source_link(self, source_link: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract library and Java class from source code URL"""
        if not source_link:
            return None, None
            
        # Identify library from URL path
        library = None
        for lib_name, pattern in self.LIBRARY_PATTERNS.items():
            if pattern in source_link:
                library = lib_name
                break
                
        if not library:
            return None, None
            
        # Extract class name from URL (e.g., DwCGeoRefDQ.java -> DwCGeoRefDQ)
        # Pattern: .../ClassName.java or .../ClassName.java#line
        class_match = re.search(r'/([A-Z][a-zA-Z0-9_]+)\.java(?:#|$)', source_link)
        if not class_match:
            return library, None
            
        class_name = class_match.group(1)
        
        # Convert to full class name based on library patterns
        full_class_name = self._get_full_class_name(library, class_name)
        
        return library, full_class_name
    
    def _get_full_class_name(self, library: str, class_name: str) -> str:
        """Convert short class name to full qualified class name"""
        # Standard package patterns for each library
        class_packages = {
            'geo_ref_qc': 'org.filteredpush.qc.georeference',
            'event_date_qc': 'org.filteredpush.qc.date',
            'sci_name_qc': 'org.filteredpush.qc.sciname',
            'rec_occur_qc': 'org.filteredpush.qc.metadata'
        }
        
        package = class_packages.get(library, 'org.filteredpush.qc.unknown')
        
        # Use the *Defaults classes which have default parameter implementations
        # as used by the official bdqtestrunner
        if not class_name.endswith('Defaults'):
            if class_name.endswith('DQ'):
                class_name = class_name + 'Defaults'
        
        return f"{package}.{class_name}"
    
    def _derive_method_name(self, test_id: str) -> str:
        """Derive Java method name from test ID using actual BDQ convention"""
        # Check for manual override first
        if test_id in self.METHOD_OVERRIDES:
            return self.METHOD_OVERRIDES[test_id]
            
        # Based on actual method names found in *Defaults classes:
        # VALIDATION_COORDINATES_COUNTRYCODE_CONSISTENT -> validationCoordinatesCountrycodeConsistent
        # VALIDATION_COUNTRY_FOUND -> validationCountryFound
        # VALIDATION_EVENTDATE_INRANGE -> validationEventdateInrange
        
        parts = test_id.split('_')
        if len(parts) < 2:
            return test_id.lower()
        
        # First part is the type (validation, amendment, etc.) - lowercase
        method_type = parts[0].lower()
        
        # Remaining parts: handle compound words and capitalize appropriately
        test_parts = parts[1:]
        result_parts = []
        
        for part in test_parts:
            part_lower = part.lower()
            
            # Handle known compound words that should be joined
            if part_lower == 'code' and len(result_parts) > 0 and result_parts[-1].lower().endswith('country'):
                # countrycode -> CountryCode (as one word, but capitalize the Code part)
                result_parts[-1] = result_parts[-1] + part_lower.capitalize()
            elif part_lower in ['event', 'date'] and len(result_parts) > 0 and result_parts[-1].lower().endswith('event'):
                # eventdate -> Eventdate (as one word) 
                result_parts[-1] = result_parts[-1] + part_lower
            elif part_lower in ['occurrence', 'id'] and len(result_parts) > 0 and result_parts[-1].lower().endswith('occurrence'):
                # occurrenceid -> Occurrenceid (as one word)
                result_parts[-1] = result_parts[-1] + part_lower
            elif part_lower in ['state', 'province'] and len(result_parts) > 0 and result_parts[-1].lower().endswith('state'):
                # stateprovince -> Stateprovince (as one word)
                result_parts[-1] = result_parts[-1] + part_lower
            else:
                # Regular part - capitalize first letter
                result_parts.append(part_lower.capitalize())
        
        return f"{method_type}{''.join(result_parts)}"
    
    def _determine_test_type(self, test_id: str) -> str:
        """Determine test type from test ID prefix"""
        if test_id.startswith('VALIDATION_'):
            return 'Validation'
        elif test_id.startswith('AMENDMENT_'):
            return 'Amendment'
        elif test_id.startswith('MEASURE_'):
            return 'Measure'
        elif test_id.startswith('ISSUE_'):
            return 'Issue'
        else:
            return 'Unknown'
    
    def get_test_mapping(self, test_id: str) -> Optional[TG2TestMapping]:
        """Get test mapping for a specific test ID"""
        return self.test_mappings.get(test_id)
    
    def get_mappings_by_library(self, library: str) -> List[TG2TestMapping]:
        """Get all test mappings for a specific library"""
        return [mapping for mapping in self.test_mappings.values() 
                if mapping.library == library]
    
    def get_all_libraries(self) -> List[str]:
        """Get list of all libraries referenced in the mappings"""
        libraries = set(mapping.library for mapping in self.test_mappings.values())
        return sorted(libraries)
    
    def get_tests_by_library(self, library: str) -> List[TG2TestMapping]:
        """Get all test mappings for a specific library (alias for get_mappings_by_library)"""
        return self.get_mappings_by_library(library)
    
    def get_libraries(self) -> List[str]:
        """Get list of all libraries referenced in the mappings (alias for get_all_libraries)"""
        return self.get_all_libraries()
