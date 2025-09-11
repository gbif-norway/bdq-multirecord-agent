
from typing import List, Dict, Optional, Any
import pandas as pd
import logging
import requests
import os
from pydantic import BaseModel

# Minimal root logger config so Cloud Run captures logs
_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LEVEL, logging.INFO),
    handlers=[logging.StreamHandler()],
    force=True,
)


def log(message: str, level: str = "INFO"):
    """Simple logging function that logs to console and Discord"""
    logger = logging.getLogger(__name__)
    
    # Log to console using standard Python logging
    if level.upper() == "DEBUG":
        logger.debug(message)
    elif level.upper() == "INFO":
        logger.info(message)
    elif level.upper() == "WARNING":
        logger.warning(message)
    elif level.upper() == "ERROR":
        logger.error(f"🚨 {message}")
    else:
        logger.info(message)
    
    # Send to Discord
    webhook_url = os.getenv("DISCORD_WEBHOOK")
    if webhook_url:
        try:
            requests.post(webhook_url, json={"content": message}, timeout=10)
        except Exception:
            pass  # Don't let Discord failures break logging


def get_relevant_test_contexts(test_ids: List[str]) -> str:
    """Get relevant BDQ test contexts for the given test IDs"""
    bdq_tests_csv_path = os.path.join(os.path.dirname(__file__), '..', 'TG2_tests_small.csv')
    bdq_tests_df = pd.read_csv(bdq_tests_csv_path, dtype=str).fillna('')
    relevant_tests = bdq_tests_df[bdq_tests_df['test_id'].isin(test_ids)]
    relevant_tests = relevant_tests.rename(columns={'IE Class': 'Information Element Class'})
    return f"\n## BDQ TEST CONTEXT\nThe following tests were run on the dataset:\n\n{str(relevant_tests)}"

def _snapshot_df(df_obj):
    max_rows, max_columns, max_str_len = 10, 10, 70

    # Truncate long strings in cells
    df = df_obj.apply(lambda col: col.astype(str).map(lambda x: (x[:max_str_len - 3] + '...') if len(x) > max_str_len else x))

    # Truncate columns
    if len(df.columns) > max_columns:
        left = df.iloc[:, :max_columns//2]
        right = df.iloc[:, -max_columns//2:]
        middle = pd.DataFrame({ '...': ['...']*len(df) }, index=df.index)
        df = pd.concat([left, middle, right], axis=1)

    df.fillna('', inplace=True)

    # Truncate rows
    if len(df) > max_rows:
        top = df.head(max_rows // 2)
        bottom = df.tail(max_rows // 2)
        middle = pd.DataFrame({col: ['...'] for col in df.columns}, index=[0])  # Use a temporary numeric index for middle
        df = pd.concat([top, middle, bottom], ignore_index=True)
        # df = '\n'.join([top, middle, bottom])

    return df

def str_snapshot(df):
    df = make_columns_unique(df)
    original_rows, original_cols = df.shape
    snapshot = _snapshot_df(df).to_string() + f"\n\n[{original_rows} rows x {original_cols} columns]"
    
    # Add value counts for each column with intelligent truncation
    value_counts_summary = _generate_value_counts_summary(df)
    if value_counts_summary:
        snapshot += "\n\n" + value_counts_summary
        
    return snapshot

def _generate_value_counts_summary(df, max_words=2000):
    """Generate a summary of value counts for each column with intelligent truncation."""
    import re
    
    summary_parts = []
    word_count = 0
    
    # Calculate basic stats for each column
    column_stats = []
    for col in df.columns:
        # Handle different data types and null values
        non_null_series = df[col].dropna()
        if len(non_null_series) == 0:
            unique_count = 0
            value_counts = pd.Series(dtype=object)
        else:
            # Convert to string to handle mixed types consistently
            string_series = non_null_series.astype(str)
            value_counts = string_series.value_counts()
            unique_count = len(value_counts)
        
        null_count = df[col].isna().sum()
        column_stats.append({
            'column': col,
            'unique_count': unique_count,
            'null_count': null_count,
            'value_counts': value_counts,
            'total_count': len(df[col])
        })
    
    # Sort columns by complexity (fewer unique values first, as they're often more informative)
    column_stats.sort(key=lambda x: (x['unique_count'], str(x['column'])))
    
    # Add header
    summary_parts.append("VALUE COUNTS BY COLUMN:")
    word_count += 4
    
    for col_stat in column_stats:
        if word_count >= max_words:
            summary_parts.append("... (truncated due to length)")
            break
            
        col = col_stat['column']
        unique_count = col_stat['unique_count']
        null_count = col_stat['null_count']
        value_counts = col_stat['value_counts']
        total_count = col_stat['total_count']
        
        # Column header with basic stats
        header = f"\n{col}: {unique_count} unique values"
        if null_count > 0:
            header += f", {null_count} nulls"
        header += f" (of {total_count} total)"
        
        summary_parts.append(header)
        word_count += len(header.split())
        
        if word_count >= max_words:
            break
            
        # Determine how many values to show based on remaining space and column complexity
        remaining_words = max_words - word_count
        if unique_count == 0:
            summary_parts.append("  (all null)")
            word_count += 2
        elif unique_count <= 10:
            # Show all values for simple columns
            for value, count in value_counts.items():
                value_str = str(value)[:50]  # Truncate very long values
                if len(str(value)) > 50:
                    value_str += "..."
                line = f"  {value_str}: {count}"
                line_words = len(line.split())
                if word_count + line_words >= max_words:
                    break
                summary_parts.append(line)
                word_count += line_words
        else:
            # For complex columns, show top values and maybe bottom values
            top_n = min(5, max(2, remaining_words // 10))  # Adaptive based on remaining space
            
            # Show top values
            for i, (value, count) in enumerate(value_counts.head(top_n).items()):
                value_str = str(value)[:50]
                if len(str(value)) > 50:
                    value_str += "..."
                line = f"  {value_str}: {count}"
                line_words = len(line.split())
                if word_count + line_words >= max_words:
                    break
                summary_parts.append(line)
                word_count += line_words
            
            # If there's space and many unique values, show bottom values too
            if unique_count > top_n + 2 and word_count < max_words - 20:
                bottom_n = min(2, max(1, (max_words - word_count) // 15))
                if bottom_n > 0:
                    summary_parts.append("  ...")
                    word_count += 1
                    
                    for value, count in value_counts.tail(bottom_n).items():
                        value_str = str(value)[:50]
                        if len(str(value)) > 50:
                            value_str += "..."
                        line = f"  {value_str}: {count}"
                        line_words = len(line.split())
                        if word_count + line_words >= max_words:
                            break
                        summary_parts.append(line)
                        word_count += line_words
    
    return "\n".join(summary_parts) if summary_parts and len(summary_parts) > 1 else ""

def make_columns_unique(df):
    cols = pd.Series(df.columns)
    nan_count = 0
    for i, col in enumerate(cols):
        if pd.isna(col):
            nan_count += 1
            cols[i] = f"NaN ({nan_count})"
        elif (cols == col).sum() > 1:
            dup_indices = cols[cols == col].index
            for j, idx in enumerate(dup_indices, start=1):
                if j > 1:
                    cols[idx] = f"{col} ({j})"

    df.columns = cols
    return df

