"""
Minimal summary statistics test for unique results.
"""

import pandas as pd
from app.main import _get_summary_stats_from_unique_results


def test_summary_from_unique_results_counts():
    # Unique results with counts for two tests
    unique = pd.DataFrame([
        # Test A: two combos, total 3 rows
        {"test_id": "VALIDATION_A", "test_type": "Validation", "status": "RUN_HAS_RESULT", "result": "COMPLIANT", "comment": "", "actedUpon": "dwc:countryCode=US", "consulted": "", "count": 2},
        {"test_id": "VALIDATION_A", "test_type": "Validation", "status": "RUN_HAS_RESULT", "result": "NOT_COMPLIANT", "comment": "", "actedUpon": "dwc:countryCode=ZZ", "consulted": "", "count": 1},
        # Test B: one AMENDED combo affecting 2 rows
        {"test_id": "AMENDMENT_B", "test_type": "Amendment", "status": "AMENDED", "result": "dwc:eventDate=2023-01-01", "comment": "", "actedUpon": "dwc:eventDate=01/01/2023", "consulted": "", "count": 2},
    ])

    summary = _get_summary_stats_from_unique_results(unique, core_type="occurrence", original_dataset_length=3)

    assert summary["number_of_records_in_dataset"] == 3
    assert summary["no_of_tests_run"] == 2
    assert summary["no_of_tests_results"] == 5  # 3 + 2
    assert summary["no_of_non_compliant_validations"] == 1
    assert summary["no_of_amendments"] == 2
    assert summary["no_of_filled_in"] == 0
