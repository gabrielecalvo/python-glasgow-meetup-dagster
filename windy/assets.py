import dagster
import pandas as pd
import plotly.express as px
from dagster import MonthlyPartitionsDefinition

from windy.resources import CoolApiClient

monthly_partitions_def = MonthlyPartitionsDefinition(start_date="2023-01-01")


@dagster.asset(
    required_resource_keys={"api_client"},
    partitions_def=monthly_partitions_def,
    group_name="my_etl",
)
def monthly_summary_asset(context: dagster.OpExecutionContext) -> pd.Series:
    month = context.asset_partition_key_for_output()
    data = context.resources.api_client.get_month_data(month)
    context.log.debug(f"retrieved {data.shape[0]} rows")
    df = data.groupby("TurbineName")["Power (kW)"].sum().div(6).rename("Energy (kWh)")  # kW-10min to kW-h
    return df


@dagster.asset(
    required_resource_keys={"api_client"},
    partitions_def=monthly_partitions_def,
    io_manager_key="html_io_manager",
    group_name="my_etl",
)
def map_visualisation_asset(
    context: dagster.OpExecutionContext,
    monthly_summary_asset: pd.Series,
) -> str:
    api_client: CoolApiClient = context.resources.api_client
    metadata = api_client.get_metadata()

    # preparing data for map
    map_df = pd.concat([pd.DataFrame(metadata).set_index("TurbineName"), monthly_summary_asset], axis=1).reset_index()

    # create map
    fig = px.scatter_mapbox(
        map_df,
        lat="Latitude",
        lon="Longitude",
        hover_name="TurbineName",
        mapbox_style="open-street-map",
        zoom=12,
        size="Energy (kWh)",
        color="Energy (kWh)",
    )

    return fig.to_html()
