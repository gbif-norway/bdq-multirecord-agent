
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
        logger.error(message)
    else:
        logger.info(message)
    
    # Send to Discord
    webhook_url = os.getenv("DISCORD_WEBHOOK")
    if webhook_url:
        try:
            requests.post(webhook_url, json={"content": message}, timeout=10)
        except Exception:
            pass  # Don't let Discord failures break logging

class BDQTestExecutionResult(BaseModel):
    """Model for complete test execution result for a row"""
    record_id: str
    test_id: str
    status: str
    result: Optional[str] = None
    comment: Optional[str] = None
    amendment: Optional[Dict[str, Any]] = None
    test_type: str

def get_unique_tuples(df, acted_upon: List[str], consulted: List[str]) -> List[List[str]]:
    """Get unique tuples for test execution"""
    # Combine acted_upon and consulted columns
    all_columns = acted_upon + consulted
    
    # Check if all columns exist in the dataframe
    missing_columns = [col for col in all_columns if col not in df.columns]
    if missing_columns:
        log(f"Missing columns for tuple generation: {missing_columns}", "WARNING")
        return []
    
    # Get unique combinations
    unique_df = df[all_columns].drop_duplicates()
    tuples = unique_df.values.tolist()
    
    log(f"Found {len(tuples)} unique tuples for columns: {all_columns}", "DEBUG")
    return tuples
    
    
def expand_single_test_results_to_all_rows(df, test_mapping, tuple_result, tuple_values, core_type) -> List:
    """Expand tuple results to individual row results"""
    
    row_results = []
    
    # Find all rows that match this tuple
    all_columns = test_mapping.acted_upon + test_mapping.consulted
    
    # Check if all columns exist
    missing_columns = [col for col in all_columns if col not in df.columns]
    if missing_columns:
        log(f"Missing columns for result expansion: {missing_columns}", "WARNING")
        return []
    
    # Create a mask for rows that match the tuple values
    mask = pd.Series([True] * len(df))
    for i, col in enumerate(all_columns):
        if i < len(tuple_values):
            mask = mask & (df[col] == tuple_values[i])
    
    matching_rows = df[mask]
    
    # Create BDQTestExecutionResult for each matching row
    for _, row in matching_rows.iterrows():
        record_id = str(row.get('occurrenceID', row.get('taxonID', 'unknown')))
        
        bdq_result = BDQTestExecutionResult(
            record_id=record_id,
            test_id=test_mapping.label,  # Use label as test_id
            status=tuple_result.get('status', 'UNKNOWN'),
            result=tuple_result.get('result'),
            comment=tuple_result.get('comment'),
            amendment=tuple_result.get('amendment'),
            test_type=test_mapping.test_type
        )
        row_results.append(bdq_result)
    
    log(f"Expanded tuple result to {len(row_results)} row results", "DEBUG")
    return row_results


def generate_summary_statistics(test_results, df, core_type):
    """Generate summary statistics from test results"""
    if not test_results:
        return {}
    
    # Convert to DataFrame for easier analysis
    results_df = pd.DataFrame([
        {
            'record_id': r.record_id,
            'test_id': r.test_id,
            'status': r.status,
            'result': r.result,
            'comment': r.comment,
            'test_type': r.test_type
        } for r in test_results
    ])
    
    # Calculate basic stats
    total_records = len(df)
    total_tests_run = len(test_results)
    
    # Count validation failures by field
    validation_failures = results_df[
        (results_df['result'] == 'NOT_COMPLIANT') | 
        (results_df['result'] == 'POTENTIAL_ISSUE')
    ]
    
    # Count amendments applied
    amendments_applied = len(results_df[
        results_df['status'].isin(['AMENDED', 'FILLED_IN'])
    ])
    
    # Get common issues
    common_issues = validation_failures['comment'].value_counts().head(5).to_dict()
    
    return {
        'total_records': total_records,
        'total_tests_run': total_tests_run,
        'validation_failures': len(validation_failures),
        'amendments_applied': amendments_applied,
        'common_issues': common_issues
    }
