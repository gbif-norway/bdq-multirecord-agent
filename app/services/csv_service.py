import pandas as pd
import io
import base64
from typing import Tuple, Optional, List, Dict, Any
from app.utils.helper import log

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

        cols = [c.lower() for c in df.columns]
        core_type = None
        if 'dwc:occurrenceid' in cols:
            core_type = 'occurrence'
        elif 'dwc:taxonid' in cols:
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
        amended_df = original_df.copy()
        id_column = f'{core_type}ID'
        amendment_results = results_df[results_df['test_type'] == 'Amendment'].copy()
        
        if amendment_results.empty:
            log("No amendment results found")
            return amended_df
        
        log(f"Applying {len(amendment_results)} amendments to dataset")
        
        # Group by ID to handle multiple amendments per row
        for id_value, group in amendment_results.groupby(id_column):
            # Find the row index in the original dataframe
            row_mask = amended_df[id_column] == id_value
            if not row_mask.any():
                log(f"Warning: Could not find row with {id_column}={id_value} for amendment", "WARNING")
                continue
                
            row_idx = amended_df[row_mask].index[0]
            
            # Apply each amendment in the group
            for _, amendment in group.iterrows():
                if amendment['status'] == 'AMENDED':
                    self._apply_single_amendment(amended_df, row_idx, amendment)
        
        log(f"Applied amendments to {len(amendment_results)} records")
        return amended_df
    
    def _apply_single_amendment(self, df, row_idx, amendment):
        """Apply a single amendment to a specific row"""
        result = amendment['result']
        test_id = amendment['test']
        
        try:
            # Check if result contains key=value pairs (like "dwc:decimalLatitude=-25.46, dwc:decimalLongitude=135.87")
            if '=' in result and ',' in result:
                # Multiple field amendment
                pairs = result.split(',')
                for pair in pairs:
                    if '=' in pair:
                        key, value = pair.strip().split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"\'')  # Remove quotes
                        
                        if key in df.columns:
                            df.at[row_idx, key] = value
                            log(f"Applied amendment to {key}={value} for test {test_id}")
                        else:
                            log(f"Warning: Column {key} not found for amendment in test {test_id}", "WARNING")
            
            elif '=' in result:
                # Single field amendment
                key, value = result.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"\'')  # Remove quotes
                
                if key in df.columns:
                    df.at[row_idx, key] = value
                    log(f"Applied amendment to {key}={value} for test {test_id}")
                else:
                    log(f"Warning: Column {key} not found for amendment in test {test_id}", "WARNING")
            
            else:
                # Simple value amendment - need to determine which column to update
                # This is more complex as we need to know which column the test was acting upon
                log(f"Warning: Cannot determine target column for simple amendment '{result}' in test {test_id}", "WARNING")
                
        except Exception as e:
            log(f"Error applying amendment '{result}' for test {test_id}: {str(e)}", "ERROR")
