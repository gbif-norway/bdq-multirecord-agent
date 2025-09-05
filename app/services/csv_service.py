import pandas as pd
import io
import base64
from typing import Tuple, Optional, List, Dict, Any
from app.utils.helper import BDQTestExecutionResult, log

class CSVService:
    """Service for CSV processing and manipulation"""
    
    def parse_csv_and_detect_core(self, csv_data: str) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Parse CSV data and detect core type (occurrence or taxon)
        Returns (dataframe, core_type)
        """
        try:
            # Try to detect delimiter
            sample = csv_data[:1024]  # First 1KB to detect delimiter
            delimiter = self._detect_delimiter(sample)
            
            # Parse CSV
            df = pd.read_csv(io.StringIO(csv_data), delimiter=delimiter, dtype=str)
            
            # Clean column names (remove surrounding quotes, whitespace)
            df.columns = df.columns.str.strip().str.strip("\"'")
            
            # Detect core type
            core_type = self._detect_core_type(df.columns.tolist())
            
            log(f"Parsed CSV with {len(df)} rows, {len(df.columns)} columns, core type: {core_type}")
            return df, core_type
            
        except Exception as e:
            log(f"Error parsing CSV: {e}", "ERROR")
            raise
    
    def _detect_delimiter(self, sample: str) -> str:
        """Detect CSV delimiter from sample"""
        delimiters = [',', ';', '\t', '|']
        delimiter_counts = {}
        
        for delimiter in delimiters:
            delimiter_counts[delimiter] = sample.count(delimiter)
        
        # Return delimiter with highest count
        return max(delimiter_counts, key=delimiter_counts.get)
    
    def _detect_core_type(self, columns: List[str]) -> Optional[str]:
        """Detect core type based on column presence"""
        columns_lower = [col.lower() for col in columns]
        
        if 'occurrenceid' in columns_lower:
            return 'occurrence'
        elif 'taxonid' in columns_lower:
            return 'taxon'
        else:
            return None
    
    def generate_raw_results_csv(self, test_results: List[BDQTestExecutionResult], core_type: str) -> str:
        """Generate CSV with raw BDQ test results"""
        try:
            # Convert results to DataFrame
            data = []
            for result in test_results:
                data.append({
                    f'{core_type}ID': result.record_id,
                    'test_id': result.test_id,
                    'status': result.status,
                    'result': result.result or '',
                    'comment': result.comment or '',
                    'amendment': str(result.amendment) if result.amendment else '',
                    'test_type': result.test_type
                })
            
            df = pd.DataFrame(data)
            
            # Convert to CSV string
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            return csv_buffer.getvalue()
            
        except Exception as e:
            log(f"Error generating raw results CSV: {e}", "ERROR")
            raise
    
    def generate_amended_dataset(self, original_df: pd.DataFrame, test_results: List[BDQTestExecutionResult], core_type: str) -> str:
        """Generate amended dataset with proposed changes applied"""
        try:
            # Create a copy of the original dataframe
            amended_df = original_df.copy()
            
            # Group results by record ID for efficient processing
            results_by_record = {}
            for result in test_results:
                if result.record_id not in results_by_record:
                    results_by_record[result.record_id] = []
                results_by_record[result.record_id].append(result)
            
            # Apply amendments
            amendments_applied = 0
            for record_id, results in results_by_record.items():
                for result in results:
                    if result.amendment and result.status in ("AMENDED", "FILLED_IN"):
                        # Apply the amendment
                        for field, new_value in result.amendment.items():
                            if field in amended_df.columns:
                                # Find the row with this record_id
                                mask = amended_df[f'{core_type}ID'] == record_id
                                if mask.any():
                                    amended_df.loc[mask, field] = new_value
                                    amendments_applied += 1
            
            log(f"Applied {amendments_applied} amendments to dataset")
            
            # Convert to CSV string
            csv_buffer = io.StringIO()
            amended_df.to_csv(csv_buffer, index=False)
            return csv_buffer.getvalue()
            
        except Exception as e:
            log(f"Error generating amended dataset: {e}", "ERROR")
            raise
    