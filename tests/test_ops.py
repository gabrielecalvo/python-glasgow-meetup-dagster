import dagster
import pandas as pd

from windy.ops import generate_monthly_summary


def test_generate_monthly_summary(mock_api_client):
    # build the context with mocked resources
    context = dagster.build_op_context(resources={"api_client": mock_api_client})

    # run the op with the mocked context
    actual = generate_monthly_summary(context, month="2023-01-01")

    # check results
    expected_idx = pd.Series(1, name="TurbineName")
    expected = pd.Series([1000 / 6], name="Energy (kWh)", index=expected_idx)
    pd.testing.assert_series_equal(actual, expected, check_freq=False)
