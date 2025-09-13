import os
import pytest
import pandas as pd
from app.services.llm_service import LLMService
from app.services.csv_service import CSVService
from app.utils.helper import str_snapshot


pytestmark = pytest.mark.skipif(
    os.getenv("OPENAI_LIVE") != "1",
    reason="Live OpenAI test disabled; set OPENAI_LIVE=1 to enable",
)


def _build_minimal_unique_results():
    # Minimal unique results CSV with a mix of statuses
    data = [
        {
            "test_id": "VALIDATION_COUNTRYCODE_VALID",
            "test_type": "VALIDATION",
            "status": "RUN_HAS_RESULT",
            "result": "NOT_COMPLIANT",
            "actedUpon": "dwc:countryCode=UK",
            "consulted": "",
            "count": 3,
        },
        {
            "test_id": "AMENDMENT_SEX_STANDARDIZED",
            "test_type": "AMENDMENT",
            "status": "AMENDED",
            "result": "dwc:sex=Male",
            "actedUpon": "dwc:sex=male",
            "consulted": "",
            "count": 2,
        },
        {
            "test_id": "ISSUE_COORDINATE_OUT_OF_RANGE",
            "test_type": "ISSUE",
            "status": "RUN_HAS_RESULT",
            "result": "POTENTIAL_ISSUE",
            "actedUpon": "dwc:decimalLatitude=123|dwc:decimalLongitude=456",
            "consulted": "",
            "count": 1,
        },
    ]
    df = pd.DataFrame(data)
    return df


def _build_minimal_original_dataset():
    data = [
        {"dwc:occurrenceID": "1", "dwc:countryCode": "UK", "dwc:sex": "male", "dwc:decimalLatitude": "123", "dwc:decimalLongitude": "456"},
        {"dwc:occurrenceID": "2", "dwc:countryCode": "", "dwc:sex": "female", "dwc:decimalLatitude": "-33.86", "dwc:decimalLongitude": "151.21"},
    ]
    df = pd.DataFrame(data)
    return df


def test_openai_live_responses_api_gpt5_generates_html_email():
    # Ensure key present
    assert os.getenv("OPENAI_API_KEY"), "OPENAI_API_KEY must be set in environment/.env.test"

    llm = LLMService()
    csv = CSVService()

    # Build inputs
    unique_df = _build_minimal_unique_results()
    original_df = _build_minimal_original_dataset()

    # Summaries (simple minimal summary)
    # Compute sums using masks, then cast to int
    mask_nc = unique_df["result"] == "NOT_COMPLIANT"
    mask_am = unique_df["status"] == "AMENDED"
    mask_pi = unique_df["result"] == "POTENTIAL_ISSUE"

    summary_stats = {
        "number_of_records_in_dataset": len(original_df),
        "list_of_all_columns_tested": ["dwc:countryCode", "dwc:sex", "dwc:decimalLatitude", "dwc:decimalLongitude"],
        "no_of_tests_results": int(unique_df["count"].sum()),
        "no_of_tests_run": unique_df["test_id"].nunique(),
        "no_of_non_compliant_validations": int(unique_df.loc[mask_nc, "count"].sum()),
        "no_of_unique_non_compliant_validations": int(mask_nc.sum()),
        "no_of_amendments": int(unique_df.loc[mask_am, "count"].sum()),
        "no_of_unique_amendments": int(mask_am.sum()),
        "no_of_filled_in": 0,
        "no_of_unique_filled_in": 0,
        "no_of_issues": int(unique_df.loc[mask_pi, "count"].sum()),
        "no_of_unique_issues": int(mask_pi.sum()),
        "top_issues": [],
        "top_filled_in": [],
        "top_amendments": [],
        "top_non_compliant_validations": [],
    }

    # Build curated joined results to attach
    curated_df = csv.build_curated_joined_results(unique_df)
    curated_csv_text = csv.dataframe_to_csv_string(curated_df)

    prompt = llm.create_prompt(
        email_data={"headers": {"from": "Test User <test@example.org>"}, "subject": "Live OpenAI test"},
        core_type="occurrence",
        summary_stats=summary_stats,
        test_results_snapshot=str_snapshot(unique_df),
        original_snapshot=str_snapshot(original_df),
        curated_joined_csv_text=curated_csv_text,
    )

    unique_csv = csv.dataframe_to_csv_string(unique_df)
    original_csv = csv.dataframe_to_csv_string(original_df)

    html = llm.generate_openai_intelligent_summary(
        prompt,
        unique_csv,
        original_csv,
        curated_csv_text=curated_csv_text,
        recipient_name="Test User",
    )

    assert isinstance(html, str) and len(html) > 0
    # Should be HTML and begin with greeting (or be enforced by post-processing)
    assert "<" in html and ">" in html
   
