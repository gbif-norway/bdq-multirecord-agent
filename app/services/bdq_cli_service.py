import json
import subprocess  
import tempfile
import os
import logging
import pandas as pd
import time
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from app.services.tg2_parser import TG2Parser, TG2TestMapping
from app.models.email_models import BDQTest, BDQTestResult, BDQTestExecutionResult, ProcessingSummary
from app.utils.logger import send_discord_notification

logger = logging.getLogger(__name__)

class BDQCLIService:
    """
    Clean architecture BDQ CLI Service - Python parses CSV, CLI is simple executor
    """
    
    def __init__(self, cli_jar_path: str = None, java_opts: str = None, skip_validation: bool = False):
        self.cli_jar_path = cli_jar_path or os.getenv('BDQ_CLI_JAR', '/opt/bdq/bdq-cli.jar')
        # Optimized Java settings for better performance
        default_java_opts = '-Xms512m -Xmx2048m -XX:+UseG1GC -XX:+UseStringDeduplication -XX:+OptimizeStringConcat'
        self.java_opts = java_opts or os.getenv('BDQ_JAVA_OPTS', default_java_opts)
        self.test_mappings: Dict[str, TG2TestMapping] = {}
        self.skip_validation = skip_validation
        
        # Always load test mappings, even in test mode
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
            # Validate CLI JAR exists
            if not os.path.exists(self.cli_jar_path):
                raise FileNotFoundError(f"BDQ CLI JAR not found at: {self.cli_jar_path}")
            logger.info(f"BDQ CLI Service initialized with JAR: {self.cli_jar_path}")
        else:
            logger.info(f"BDQ CLI Service initialized in test mode (validation skipped)")
    
    def _load_test_mappings(self):
        """Load test mappings from TG2_tests.csv - SINGLE SOURCE OF TRUTH"""
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
                parameters=mapping.default_parameters
            )
            tests.append(test)
        return tests
        
    async def run_tests_on_dataset(self, df, applicable_tests: List[BDQTest], core_type: str) -> Tuple[List[BDQTestExecutionResult], List[str]]:
        """Run BDQ tests individually with timing - Python controls everything, CLI is simple executor"""
        overall_start_time = time.time()
        test_results: List[BDQTestExecutionResult] = []
        skipped_tests: List[str] = []
        
        logger.info(f"🧪 Starting individual BDQ test execution on {len(df)} records with {len(applicable_tests)} applicable tests")
        send_discord_notification(f"🧪 Running {len(applicable_tests)} tests individually on {len(df):,} records with timing")
        
        for i, test in enumerate(applicable_tests):
            test_start_time = time.time()
            
            # Get the test mapping
            mapping = self.test_mappings.get(test.id)
            if not mapping:
                logger.warning(f"❌ No mapping found for test {test.id} - skipping")
                skipped_tests.append(test.id)
                continue
            
            logger.info(f"🔄 [{i+1}/{len(applicable_tests)}] Executing {test.id}...")
            
            # Prepare test data for this specific test
            tuples = self._prepare_test_tuples(df, mapping.acted_upon, mapping.consulted)
            if not tuples:
                logger.warning(f"⏭️  No valid tuples for test {test.id} - skipping")
                skipped_tests.append(test.id)
                continue
                
            # Execute single test via CLI with complete method info
            try:
                cli_result = self._execute_single_test_via_cli(
                    test_id=test.id,
                    java_class=mapping.java_class,
                    java_method=mapping.java_method,
                    acted_upon=mapping.acted_upon,
                    consulted=mapping.consulted,
                    parameters=mapping.default_parameters,
                    tuples=tuples
                )
                
                test_execution_time = time.time() - test_start_time
                
                # Process results
                if cli_result and 'results' in cli_result and test.id in cli_result['results']:
                    tuple_results = cli_result['results'][test.id].get('tupleResults', [])
                    
                    # Expand tuple results back to all matching rows
                    expanded_results = self._expand_tuple_results_to_rows(
                        df, mapping.acted_upon, mapping.consulted, tuple_results
                    )
                    
                    test_result = BDQTestExecutionResult(
                        test=test,
                        results=expanded_results,
                        execution_time_seconds=test_execution_time,
                        tuple_count=len(tuples)
                    )
                    test_results.append(test_result)
                    
                    logger.info(f"✅ [{i+1}/{len(applicable_tests)}] {test.id}: {len(expanded_results)} results in {test_execution_time:.2f}s ({test_execution_time/len(tuples):.3f}s/tuple)")
                    
                else:
                    logger.warning(f"❌ [{i+1}/{len(applicable_tests)}] {test.id}: No results returned in {test_execution_time:.2f}s")
                    skipped_tests.append(test.id)
                    
            except Exception as e:
                test_execution_time = time.time() - test_start_time  
                logger.error(f"❌ [{i+1}/{len(applicable_tests)}] {test.id}: Error in {test_execution_time:.2f}s - {str(e)}")
                skipped_tests.append(test.id)
        
        overall_time = time.time() - overall_start_time
        logger.info(f"🏁 Individual test execution completed in {overall_time:.1f} seconds")
        logger.info(f"📊 Results: {len(test_results)} successful, {len(skipped_tests)} skipped")
        send_discord_notification(f"🏁 Individual execution complete: {len(test_results)} tests in {overall_time:.1f}s (avg {overall_time/len(applicable_tests):.2f}s/test)")

        return test_results, skipped_tests
    
    def _map_dwc_columns_to_dataframe(self, dwc_columns: List[str], df) -> List[str]:
        """
        Map Darwin Core column names (with dwc: prefixes) back to actual DataFrame column names.
        This handles the reverse mapping from normalized test requirements to actual CSV columns.
        """
        mapped_columns = []
        
        for dwc_col in dwc_columns:
            if dwc_col in df.columns:
                # Direct match (column already has dwc: prefix or is not a DWC term)
                mapped_columns.append(dwc_col)
            elif dwc_col.startswith('dwc:'):
                # Try without the dwc: prefix
                unprefixed = dwc_col[4:]
                if unprefixed in df.columns:
                    mapped_columns.append(unprefixed)
                else:
                    logger.warning(f"Cannot map Darwin Core column '{dwc_col}' to any DataFrame column")
                    mapped_columns.append(dwc_col)  # Keep original, will fail later
            else:
                mapped_columns.append(dwc_col)
        
        return mapped_columns

    def _prepare_test_tuples(self, df, acted_upon: List[str], consulted: List[str]) -> List[List[str]]:
        """Prepare tuples for a specific test, mapping DWC column names to actual DataFrame columns"""
        all_dwc_columns = acted_upon + consulted
        
        # Map Darwin Core column names back to actual DataFrame column names
        all_df_columns = self._map_dwc_columns_to_dataframe(all_dwc_columns, df)
        
        # Check if all required columns exist in the DataFrame
        missing_columns = [col for col in all_df_columns if col not in df.columns]
        if missing_columns:
            logger.debug(f"Missing columns {missing_columns} - cannot prepare tuples")
            return []
        
        # Extract unique tuples (same deduplication as before)
        tuples_set = set()
        for _, row in df.iterrows():
            tuple_values = [str(row.get(col, '')) for col in all_df_columns]
            tuples_set.add(tuple(tuple_values))
        
        # Convert back to list of lists
        unique_tuples = [list(t) for t in tuples_set]
        logger.debug(f"Prepared {len(unique_tuples)} unique tuples from {len(df)} rows using columns {all_df_columns}")
        
        return unique_tuples
    
    def _execute_single_test_via_cli(self, test_id: str, java_class: str, java_method: str,
                                   acted_upon: List[str], consulted: List[str], 
                                   parameters: Dict[str, str], tuples: List[List[str]]) -> Dict:
        """Execute a single test via the simplified CLI with complete method information"""
        
        # Build CLI request with complete method information
        cli_input = {
            "requestId": f"single-test-{test_id}-{int(time.time())}",
            "tests": [{
                "testId": test_id,
                "javaClass": java_class,  # NEW: Pass complete class name
                "javaMethod": java_method,  # NEW: Pass method name  
                "actedUpon": acted_upon,
                "consulted": consulted,
                "parameters": parameters,
                "tuples": tuples
            }]
        }
        
        # Write to temp files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(cli_input, f, indent=2)
            input_file = f.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            output_file = f.name
        
        # Execute CLI
        java_cmd_parts = self.java_opts.split() + [
            '-jar', self.cli_jar_path,
            f'--input={input_file}',
            f'--output={output_file}'
        ]
        java_cmd = ['java'] + java_cmd_parts
        
        try:
            result = subprocess.run(
                java_cmd,
                capture_output=True,
                text=True,
                timeout=60  # Shorter timeout for individual tests
            )
            
            if result.returncode != 0:
                logger.error(f"CLI failed with return code {result.returncode}: {result.stderr}")
                return {}
            
            # Read result
            if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                with open(output_file, 'r') as f:
                    return json.load(f)
            else:
                logger.warning("CLI produced no output file")
                return {}
                
        except subprocess.TimeoutExpired:
            logger.error(f"CLI timed out for test {test_id}")
            return {}
        except Exception as e:
            logger.error(f"CLI execution error for test {test_id}: {e}")
            return {}
        finally:
            # Cleanup
            try:
                os.unlink(input_file)
                os.unlink(output_file) 
            except:
                pass
    
    def _expand_tuple_results_to_rows(self, df, acted_upon: List[str], consulted: List[str], 
                                    tuple_results: List[Dict]) -> List[BDQTestResult]:
        """Expand tuple results back to individual row results, handling DWC column mapping"""
        all_dwc_columns = acted_upon + consulted
        
        # Map Darwin Core column names back to DataFrame column names
        all_df_columns = self._map_dwc_columns_to_dataframe(all_dwc_columns, df)
        results = []
        
        # Create mapping from tuples back to row indices using actual DataFrame columns
        tuple_to_rows = {}
        for row_idx, row in df.iterrows():
            tuple_key = tuple(str(row.get(col, '')) for col in all_df_columns)
            if tuple_key not in tuple_to_rows:
                tuple_to_rows[tuple_key] = []
            tuple_to_rows[tuple_key].append(row_idx)
        
        # Map tuple results back to rows
        for tuple_result in tuple_results:
            tuple_idx = tuple_result.get('tupleIndex', 0)
            if tuple_idx < len(list(tuple_to_rows.keys())):
                tuple_key = list(tuple_to_rows.keys())[tuple_idx]
                matching_row_indices = tuple_to_rows[tuple_key]
                
                for row_idx in matching_row_indices:
                    row = df.iloc[row_idx]
                    record_id = row.get('occurrenceID', row.get('id', f'row-{row_idx}'))
                    
                    result = BDQTestResult(
                        record_id=record_id,
                        status=tuple_result.get('status', 'UNKNOWN'),
                        result=tuple_result.get('result', 'UNKNOWN'), 
                        comment=tuple_result.get('comment', '')
                    )
                    results.append(result)
        
        return results

    def _normalize_column_names_for_dwc(self, csv_columns: List[str]) -> List[str]:
        """
        Normalize CSV column names by adding 'dwc:' prefixes to known Darwin Core terms.
        This allows CSV files without prefixes to match BDQ test requirements.
        """
        # Common Darwin Core terms that should get dwc: prefixes
        # Based on Darwin Core standard: https://dwc.tdwg.org/terms/
        KNOWN_DWC_TERMS = {
            # Record-level terms
            'type', 'modified', 'language', 'license', 'rightsHolder', 'accessRights', 'bibliographicCitation',
            'references', 'institutionID', 'collectionID', 'datasetID', 'institutionCode', 'collectionCode',
            'datasetName', 'ownerInstitutionCode', 'basisOfRecord', 'informationWithheld', 'dataGeneralizations',
            'dynamicProperties',
            
            # Occurrence terms  
            'occurrenceID', 'catalogNumber', 'recordNumber', 'recordedBy', 'recordedByID', 'individualCount',
            'organismQuantity', 'organismQuantityType', 'sex', 'lifeStage', 'reproductiveCondition', 
            'occurrenceStatus', 'preparations', 'disposition', 'associatedReferences', 'associatedSequences',
            'associatedTaxa', 'otherCatalogNumbers', 'occurrenceRemarks', 'organismID', 'organismName',
            'organismScope', 'associatedOccurrences', 'associatedOrganisms', 'previousIdentifications',
            'organismRemarks', 'materialSampleID', 'eventID', 'parentEventID', 'fieldNumber', 'eventDate',
            'eventTime', 'startDayOfYear', 'endDayOfYear', 'year', 'month', 'day', 'verbatimEventDate',
            'habitat', 'samplingProtocol', 'samplingEffort', 'sampleSizeValue', 'sampleSizeUnit',
            'fieldNotes', 'eventRemarks',
            
            # Location terms
            'locationID', 'higherGeography', 'higherGeographyID', 'continent', 'waterBody', 'islandGroup',
            'island', 'country', 'countryCode', 'stateProvince', 'county', 'municipality', 'locality',
            'verbatimLocality', 'minimumElevationInMeters', 'maximumElevationInMeters', 'verbatimElevation',
            'minimumDepthInMeters', 'maximumDepthInMeters', 'verbatimDepth', 'minimumDistanceAboveSurfaceInMeters',
            'maximumDistanceAboveSurfaceInMeters', 'locationAccordingTo', 'locationRemarks', 'decimalLatitude',
            'decimalLongitude', 'geodeticDatum', 'coordinateUncertaintyInMeters', 'coordinatePrecision',
            'pointRadiusSpatialFit', 'verbatimCoordinates', 'verbatimLatitude', 'verbatimLongitude',
            'verbatimCoordinateSystem', 'verbatimSRS', 'footprintWKT', 'footprintSRS', 'footprintSpatialFit',
            'georeferencedBy', 'georeferencedDate', 'georeferenceProtocol', 'georeferenceSources',
            'georeferenceVerificationStatus', 'georeferenceRemarks',
            
            # Geological Context terms
            'geologicalContextID', 'earliestEonOrLowestEonothem', 'latestEonOrHighestEonothem',
            'earliestEraOrLowestErathem', 'latestEraOrHighestErathem', 'earliestPeriodOrLowestSystem',
            'latestPeriodOrHighestSystem', 'earliestEpochOrLowestSeries', 'latestEpochOrHighestSeries',
            'earliestAgeOrLowestStage', 'latestAgeOrHighestStage', 'lowestBiostratigraphicZone',
            'highestBiostratigraphicZone', 'lithostratigraphicTerms', 'group', 'formation', 'member', 'bed',
            
            # Identification terms
            'identificationID', 'verbatimIdentification', 'identificationQualifier', 'typeStatus',
            'identifiedBy', 'identifiedByID', 'dateIdentified', 'identificationReferences',
            'identificationVerificationStatus', 'identificationRemarks',
            
            # Taxon terms
            'taxonID', 'scientificNameID', 'acceptedNameUsageID', 'parentNameUsageID', 'originalNameUsageID',
            'nameAccordingToID', 'namePublishedInID', 'taxonConceptID', 'scientificName', 'acceptedNameUsage',
            'parentNameUsage', 'originalNameUsage', 'nameAccordingTo', 'namePublishedIn', 'namePublishedInYear',
            'higherClassification', 'kingdom', 'phylum', 'class', 'order', 'family', 'subfamily', 'tribe',
            'subtribe', 'genus', 'genericName', 'subgenus', 'infragenericEpithet', 'specificEpithet',
            'infraspecificEpithet', 'cultivarEpithet', 'taxonRank', 'verbatimTaxonRank', 'scientificNameAuthorship',
            'vernacularName', 'nomenclaturalCode', 'taxonomicStatus', 'nomenclaturalStatus', 'taxonRemarks',
            
            # Measurement or Fact terms
            'measurementID', 'measurementType', 'measurementValue', 'measurementAccuracy', 'measurementUnit',
            'measurementDeterminedBy', 'measurementDeterminedDate', 'measurementMethod', 'measurementRemarks',
            
            # Resource Relationship terms  
            'resourceRelationshipID', 'resourceID', 'relatedResourceID', 'relationshipOfResource',
            'relationshipAccordingTo', 'relationshipEstablishedDate', 'relationshipRemarks',
            
            # Additional common terms
            'id', 'modified', 'license'
        }
        
        normalized_columns = []
        for col in csv_columns:
            # Skip if already has dwc: prefix
            if col.startswith('dwc:'):
                normalized_columns.append(col)
            # Add dwc: prefix if it's a known Darwin Core term
            elif col.lower() in KNOWN_DWC_TERMS or col in KNOWN_DWC_TERMS:
                normalized_columns.append(f'dwc:{col}')
            else:
                # Keep non-Darwin Core columns as-is
                normalized_columns.append(col)
        
        # Log the normalization for debugging
        added_prefixes = [f"{orig} -> dwc:{orig}" for orig, norm in zip(csv_columns, normalized_columns) 
                         if not orig.startswith('dwc:') and norm.startswith('dwc:')]
        if added_prefixes:
            logger.info(f"Added dwc: prefixes to {len(added_prefixes)} columns: {added_prefixes[:5]}{'...' if len(added_prefixes) > 5 else ''}")
        
        return normalized_columns

    def filter_applicable_tests(self, tests: List[BDQTest], csv_columns: List[str]) -> List[BDQTest]:
        """Filter tests to only those applicable to the given CSV columns"""
        # Normalize CSV column names by adding dwc: prefixes where appropriate
        normalized_csv_columns = self._normalize_column_names_for_dwc(csv_columns)
        
        applicable_tests = []
        
        for test in tests:
            # Check if all required columns (acted upon + consulted) are present
            all_required_columns = test.actedUpon + test.consulted
            
            # Remove any empty strings from the required columns
            all_required_columns = [col for col in all_required_columns if col.strip()]
            
            if all_required_columns:
                missing_columns = [col for col in all_required_columns if col not in normalized_csv_columns]
                if not missing_columns:
                    applicable_tests.append(test)
                else:
                    logger.debug(f"Test {test.id} requires columns {missing_columns} which are not in normalized CSV columns")
            else:
                logger.debug(f"Test {test.id} has no required columns - skipping")
        
        logger.info(f"Filtered to {len(applicable_tests)} applicable tests from {len(tests)} total tests")
        logger.info(f"Original CSV columns: {len(csv_columns)}, Normalized columns: {len(normalized_csv_columns)}")
        return applicable_tests

    def generate_summary(self, test_results: List[BDQTestExecutionResult], total_records: int, skipped_tests: List[str]) -> ProcessingSummary:
        """Generate processing summary from test results"""
        # Count status types across all results
        status_counts = {'COMPLIANT': 0, 'NOT_COMPLIANT': 0, 'UNABLE_CURATE': 0}
        total_test_results = 0
        
        for test_exec in test_results:
            for result in test_exec.results:
                total_test_results += 1
                status = result.status
                if status in status_counts:
                    status_counts[status] += 1
                else:
                    # Handle other status values
                    logger.warning(f"Unexpected status: {status}")
        
        # Calculate percentages
        compliant_percentage = (status_counts['COMPLIANT'] / max(total_test_results, 1)) * 100
        non_compliant_percentage = (status_counts['NOT_COMPLIANT'] / max(total_test_results, 1)) * 100
        unable_curate_percentage = (status_counts['UNABLE_CURATE'] / max(total_test_results, 1)) * 100
        
        # Create summary
        summary = ProcessingSummary(
            total_records=total_records,
            tests_executed=len(test_results),
            tests_skipped=len(skipped_tests),
            compliant_percentage=compliant_percentage,
            non_compliant_percentage=non_compliant_percentage,
            unable_curate_percentage=unable_curate_percentage,
            total_test_results=total_test_results
        )
        
        return summary

    def test_connection(self) -> bool:
        """Test connection to BDQ CLI"""
        try:
            # Simple test - run CLI with minimal input to see if it responds
            test_input = {
                "requestId": "connection-test",
                "tests": []
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(test_input, f)
                input_file = f.name
            
            try:
                java_cmd = self.java_opts.split() if self.java_opts else []
                cmd = ['java'] + java_cmd + ['-jar', self.cli_jar_path, input_file]
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                return result.returncode == 0
                
            finally:
                if os.path.exists(input_file):
                    os.unlink(input_file)
                    
        except Exception as e:
            logger.error(f"CLI connection test failed: {e}")
            return False