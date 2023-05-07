import io
from pathlib import Path

import pandas as pd
import requests
from dagster import EnvVar
from dagster_slack import SlackResource

from windy.io_managers import MyLocalHtmlIoManager


class CoolApiClient:
    def __init__(self, root_url: str):
        self.root_url = root_url

    def get_metadata(self) -> dict:
        response = requests.get(f"{self.root_url}/metadata.json")
        response.raise_for_status()
        return response.json()

    def get_month_data(self, month: str) -> pd.DataFrame:
        response = requests.get(f"{self.root_url}/{month}.csv")
        response.raise_for_status()
        return pd.read_csv(io.BytesIO(response.content), parse_dates=["Timestamp"])


VIZ_DIRECTORY = Path(__file__).parents[1] / "data" / "output-viz"
html_io_manager = MyLocalHtmlIoManager(directory=VIZ_DIRECTORY)
local_resources = {
    "api_client": CoolApiClient(root_url="http://localhost:8000"),
    "html_io_manager": html_io_manager,
    "slack": SlackResource(token=EnvVar("SLACK_TOKEN")),
}
