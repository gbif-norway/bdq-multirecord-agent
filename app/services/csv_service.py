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
    
    def generate_amended_dataset(self, original_df: pd.DataFrame, unique_results_df: pd.DataFrame, core_type: str) -> pd.DataFrame:
        """Generate amended dataset by applying AMENDED/FILLED_IN results using value-based masks.

        Expects unique results where each row represents a unique combination of actedUpon+consulted values
        with a 'count' column, and includes the raw test columns used to produce the result (one column per
        actedUpon/consulted field), plus:
          - 'status', 'result', 'test_id', 'test_type'
          - 'actedUpon_cols', 'consulted_cols' (pipe-separated column name lists)
        """
        amended_df = original_df.copy()

        if unique_results_df is None or unique_results_df.empty:
            log("No test results supplied; returning original dataset")
            return amended_df

        # Only apply actionable amendments
        actionable = unique_results_df[unique_results_df['status'].isin(['AMENDED', 'FILLED_IN'])].copy()
        if actionable.empty:
            log("No AMENDED/FILLED_IN results to apply")
            return amended_df

        # Build set of all columns that appear in any test's key set to normalize once
        def _split_cols(s: str) -> list:
            if pd.isna(s) or not s:
                return []
            return [c for c in str(s).split('|') if c]

        all_key_cols = set()
        for _, r in actionable.iterrows():
            all_key_cols.update(_split_cols(r.get('actedUpon_cols', '')))
            all_key_cols.update(_split_cols(r.get('consulted_cols', '')))
        existing_key_cols = [c for c in all_key_cols if c in amended_df.columns]

        # Precompute normalized views for matching on key columns
        normalized = {c: amended_df[c].astype(str).fillna('') if c in amended_df.columns else None for c in existing_key_cols}

        def _parse_result_pairs(result_str: str) -> list:
            pairs = []
            if not isinstance(result_str, str) or result_str == '':
                return pairs
            for part in result_str.split('|'):
                if '=' not in part:
                    continue
                col, val = part.split('=', 1)
                col = col.strip()
                val = val.strip().strip('"\'')
                pairs.append((col, val))
            return pairs

        amendments_applied = 0
        rows_touched = 0

        for _, res in actionable.iterrows():
            key_cols = _split_cols(res.get('actedUpon_cols', '')) + _split_cols(res.get('consulted_cols', ''))
            key_cols = [c for c in key_cols if c in amended_df.columns]
            if not key_cols:
                # Nothing to match on safely
                continue

            # Build mask matching the original values used in the unique test (normalized string compare)
            mask = None
            for c in key_cols:
                expected_val = str(res.get(c, '') if pd.notna(res.get(c, '')) else '')
                col_series = normalized.get(c)
                if col_series is None:
                    # Column missing in original dataset; skip
                    mask = None
                    break
                cond = (col_series == expected_val)
                mask = cond if mask is None else (mask & cond)

            if mask is None:
                continue

            target_pairs = _parse_result_pairs(res.get('result', ''))
            if not target_pairs:
                continue

            matched_count = int(mask.sum())
            if matched_count == 0:
                continue

            # Apply all target assignments
            for col, new_value in target_pairs:
                if col not in amended_df.columns:
                    log(f"WARNING: Column {col} not found in dataset for amendment from {res.get('test_id','')} ", "WARNING")
                    continue
                amended_df.loc[mask, col] = new_value
                amendments_applied += matched_count
            rows_touched += matched_count

        log(f"Applied amendments: {amendments_applied} field updates across {rows_touched} rows")
        return amended_df
    
