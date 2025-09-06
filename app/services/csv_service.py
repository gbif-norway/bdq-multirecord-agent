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
        """Rename columns to have 'dwc:' prefix if they don't already have it.

        This ensures BDQ mappings that expect 'dwc:' names resolve without duplicating data.
        """
        try:
            renamed = 0
            new_columns = []
            for col in df.columns:
                if not col.startswith('dwc:'):
                    new_columns.append(f'dwc:{col}')
                    renamed += 1
                else:
                    new_columns.append(col)
            
            if renamed:
                df.columns = new_columns
                log(f"Renamed {renamed} columns to have 'dwc:' prefix to match BDQ mappings")
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
        else:
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
        
        # Convert DataFrame to CSV string
        csv_buffer = io.StringIO()
        amended_df.to_csv(csv_buffer, index=False)
        return csv_buffer.getvalue()
    
    def _apply_single_amendment(self, df, row_idx, amendment):
        """Apply a single amendment to a specific row"""
        result = amendment['result']
        test_id = amendment['test_id']
    
        amendments = result.split('|')
        for amendment_part in amendments:
            col, amended_value = amendment_part.split('=', 1)
            amended_value = amended_value.strip().strip('"\'')
            
            if col in df.columns:
                df.at[row_idx, col] = amended_value
                log(f"Applied amendment to {col}={amended_value} for test {test_id}")
            else:
                log(f"ERROR: Column {col} not found for amendment in test {test_id}", "WARNING")
