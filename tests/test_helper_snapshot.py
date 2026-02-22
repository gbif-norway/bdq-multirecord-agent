import math

import pandas as pd

from app.utils.helper import str_snapshot


def test_str_snapshot_handles_floats_and_nan_without_type_error():
    df = pd.DataFrame(
        {
            "numeric_float": [1.23, math.nan, 9.87],
            "numeric_int": [1, 2, 3],
            "mixed": ["ok", None, 4.56],
        }
    )

    snapshot = str_snapshot(df)

    assert isinstance(snapshot, str)
    assert "[3 rows x 3 columns]" in snapshot


def test_str_snapshot_truncates_long_non_string_values_safely():
    df = pd.DataFrame(
        {
            "payload": [{"k": "x" * 200}],
            "amount": [42.0],
        }
    )

    snapshot = str_snapshot(df)

    assert isinstance(snapshot, str)
    assert "..." in snapshot
    assert "[1 rows x 2 columns]" in snapshot
