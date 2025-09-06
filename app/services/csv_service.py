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
        # Parse CSV with automatic delimiter detection
        df = pd.read_csv(io.StringIO(csv_data), sep=None, engine='python', dtype=str)
        df.columns = df.columns.str.strip().str.strip("\"'")
        df = self._ensure_dwc_prefixed_columns(df)

        cols = [c.lower() for c in df.columns]]
        core_type = None
        if 'occurrenceid' in cols:
            core_type = 'occurrence'
        elif 'taxonid' in cols:
            core_type = 'taxon'
        
        log(f"Parsed CSV with {len(df)} rows, {len(df.columns)} columns, core type: {core_type}")
        return df, core_type
 
    def _ensure_dwc_prefixed_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """For each column that does not start with 'dwc:', convert to a 'dwc:' alias if missing.

        Keeps original columns intact and creates additional prefixed columns so that
        BDQ mappings that expect 'dwc:' names resolve without changing inputs.
        """
        try:
            added = 0
            for col in list(df.columns):
                if not col.startswith('dwc:'):
                    prefixed = f'dwc:{col}'
                    if prefixed not in df.columns:
                        df[prefixed] = df[col]
                        added += 1
            if added:
                log(f"Added {added} 'dwc:'-prefixed column aliases to match BDQ mappings")
            return df
        except Exception as e:
            log(f"Error ensuring dwc-prefixed columns: {e}", "WARNING")
            return df
    
    def generate_raw_results_csv(self, results_df):
        """Generate CSV with raw BDQ test results"""
        csv_buffer = io.StringIO()
        results_df.to_csv(csv_buffer, index=False)
        return csv_buffer.getvalue()
    
    def generate_amended_dataset(self, original_df, results_df, core_type):
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
    
