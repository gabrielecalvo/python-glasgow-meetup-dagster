from unittest.mock import Mock

import pandas as pd
import pytest

from windy.io_managers import MyLocalHtmlIoManager
from windy.resources import CoolApiClient


@pytest.fixture
def mock_api_client():
    fake_data = pd.DataFrame(
        {
            "Timestamp": pd.date_range("2023-01-01", periods=3, freq="10Min"),
            "TurbineName": 1,
            "Wind speed (m/s)": [5, 2, 3],
            "Power (kW)": [500.0, 200.0, 300.0],
        }
    )
    mock_api_client = Mock(spec=CoolApiClient)
    mock_api_client.get_month_data.return_value = fake_data
    mock_api_client.get_metadata.return_value = [{"TurbineName": 1, "Latitude": 55.902502, "Longitude": -2.306389}]
    return mock_api_client


mock_resources = {
    "api_client": Mock(spec=CoolApiClient),
    "html_io_manager": Mock(spec=MyLocalHtmlIoManager),
    "slack": Mock(),
}
