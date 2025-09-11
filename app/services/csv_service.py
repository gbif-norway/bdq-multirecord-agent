import pandas as pd
import io
from typing import Tuple, Optional
from app.utils.helper import log

class CSVService:
    """Service for CSV processing and manipulation"""
    
    def parse_csv_and_detect_core(self, csv_data: str) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Parse CSV data and detect core type (occurrence or taxon)
        Returns (dataframe, core_type)
        """
        # Try multiple parsing strategies to handle malformed CSV data
        df = self._parse_csv_robust(csv_data)
        df.columns = df.columns.str.strip().str.strip("\"'")
        df = self._ensure_dwc_prefixed_columns(df)

        # Detect core type based on column names
        cols = [c.lower() for c in df.columns]
        core_type = None
        if 'dwc:occurrenceid' in cols:
            core_type = 'occurrence'
        elif 'dwc:taxonid' in cols:
            core_type = 'taxon'
        
        log(f"Parsed CSV with {len(df)} rows, {len(df.columns)} columns, core type: {core_type}")
        return df, core_type
    
    def _parse_csv_robust(self, csv_data: str) -> pd.DataFrame:
        """
        Parse CSV with multiple fallback strategies to handle malformed data
        """
        # Strategy 1: Standard pandas parsing with error handling
        try:
            df = pd.read_csv(io.StringIO(csv_data), sep=None, engine='python', dtype=str, quoting=1)
            log("Successfully parsed CSV using standard pandas parser")
            return df
        except Exception as e:
            log(f"Standard CSV parsing failed: {e}", "WARNING")
        
        # Strategy 2: Try with different quoting options
        try:
            df = pd.read_csv(io.StringIO(csv_data), sep=None, engine='python', dtype=str, quoting=3)
            log("Successfully parsed CSV using QUOTE_NONE quoting")
            return df
        except Exception as e:
            log(f"QUOTE_NONE parsing failed: {e}", "WARNING")
        
        # Strategy 3: Try with tab delimiter explicitly (common in biodiversity data)
        try:
            df = pd.read_csv(io.StringIO(csv_data), sep='\t', engine='python', dtype=str, quoting=3)
            log("Successfully parsed CSV using tab delimiter with QUOTE_NONE")
            return df
        except Exception as e:
            log(f"Tab delimiter parsing failed: {e}", "WARNING")
        
        # Strategy 4: Try to clean the data and parse again
        try:
            cleaned_data = self._clean_malformed_csv(csv_data)
            df = pd.read_csv(io.StringIO(cleaned_data), sep='\t', engine='python', dtype=str, quoting=3)
            log("Successfully parsed CSV after cleaning malformed quotes")
            return df
        except Exception as e:
            log(f"Cleaned CSV parsing failed: {e}", "WARNING")
        
        # Strategy 5: Last resort - try with minimal error handling
        try:
            df = pd.read_csv(io.StringIO(csv_data), sep='\t', engine='python', dtype=str, 
                           quoting=3, on_bad_lines='skip')
            log("Successfully parsed CSV with error line skipping")
            return df
        except Exception as e:
            log(f"All CSV parsing strategies failed: {e}", "ERROR")
            raise Exception(f"Unable to parse CSV data: {e}")
    
    def _clean_malformed_csv(self, csv_data: str) -> str:
        """
        Clean common CSV malformation issues
        """
        lines = csv_data.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # Fix malformed quotes like "text"moretext by adding tab separator
            # This handles cases like "G.M.Dannevig"1951 -> "G.M.Dannevig"	1951
            import re
            # Pattern to match quote followed immediately by non-whitespace
            line = re.sub(r'"([^"]*)"([^\s])', r'"\1"\t\2', line)
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
 
    def _ensure_dwc_prefixed_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns to have 'dwc:' prefix if they don't already have it.

        This ensures BDQ mappings that expect 'dwc:' names resolve without duplicating data.
        """
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
    
    def dataframe_to_csv_string(self, df: pd.DataFrame) -> str:
        """Convert DataFrame to CSV string"""
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        return csv_buffer.getvalue()
    
    def generate_amended_dataset(self, original_df: pd.DataFrame, results_df: pd.DataFrame, core_type: str) -> pd.DataFrame:
        """Generate amended dataset with proposed changes applied"""
        amended_df = original_df.copy()
        id_column = f'dwc:{core_type}ID'
        amendment_results = results_df[results_df['test_type'] == 'Amendment'].copy()
        
        if amendment_results.empty:
            log("No amendment results found")
        else:
            log(f"Applying {len(amendment_results)} amendments to dataset")
        
        # Filter to only AMENDED status amendments
        amended_only = amendment_results[amendment_results['status'] == 'AMENDED'].copy()
        
        if amended_only.empty:
            log("No amendments to apply (all amendments have status other than AMENDED)")
        else:
            # Use the original working method for now
            amendments_applied = self._apply_amendments_original(amended_df, amended_only, id_column)
            log(f"Applied {amendments_applied} amendments to dataset")
        
        return amended_df
    
    def _apply_amendments_original(self, df: pd.DataFrame, amendments_df: pd.DataFrame, id_column: str) -> int:
        """Apply amendments using optimized version of the original method"""
        amendments_applied = 0
        
        # Create a mapping of ID to row index for O(1) lookups - this is the key optimization
        id_to_index = {id_val: idx for idx, id_val in enumerate(df[id_column])}
        
        # Group by ID to handle multiple amendments per row
        for id_value, group in amendments_df.groupby(id_column):
            if id_value not in id_to_index:
                log(f"Warning: Could not find row with {id_column}={id_value} for amendment", "WARNING")
                continue
                
            row_idx = id_to_index[id_value]
            
            # Apply each amendment in the group
            for _, amendment in group.iterrows():
                if amendment['status'] == 'AMENDED':
                    amendments_applied += self._apply_single_amendment_count(df, row_idx, amendment)
        
        return amendments_applied
    
    def _apply_single_amendment_count(self, df: pd.DataFrame, row_idx: int, amendment: pd.Series) -> int:
        """Apply a single amendment and return count of fields amended"""
        result = amendment['result']
        amendments_applied = 0
    
        amendments = result.split('|')
        for amendment_part in amendments:
            col, amended_value = amendment_part.split('=', 1)
            amended_value = amended_value.strip().strip('"\'')
            
            if col in df.columns:
                df.at[row_idx, col] = amended_value
                amendments_applied += 1
            else:
                log(f"ERROR: Column {col} not found for amendment in test {amendment['test_id']}", "WARNING")
        
        return amendments_applied
    
