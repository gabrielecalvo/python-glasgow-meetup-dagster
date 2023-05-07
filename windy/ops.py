import dagster
import pandas as pd
import plotly.express as px
from dagster import Out

from windy.resources import CoolApiClient


@dagster.op(required_resource_keys={"api_client"})
def generate_monthly_summary(
    context: dagster.OpExecutionContext,
    month: str,
) -> pd.Series:
    # grab the custom resource from the context and use it to get data
    api_client: CoolApiClient = context.resources.api_client
    data = api_client.get_month_data(month)

    # the context also has access to the dagster logger
    context.log.debug(f"retrieved {data.shape[0]} rows")

    # our awesome business logic (monthly cumulative energy per turbine)
    ser = data.groupby("TurbineName")["Power (kW)"].sum().div(6).rename("Energy (kWh)")  # kW-10min to kW-h

    # returning the result directly, we'll handle storing using the `IOManager`
    return ser


@dagster.op(required_resource_keys={"api_client"}, out=Out(io_manager_key="html_io_manager"))
def generate_map_visualisation(
    context: dagster.OpExecutionContext,
    monthly_energy_by_turbine: pd.Series,
) -> str:
    api_client: CoolApiClient = context.resources.api_client
    metadata = api_client.get_metadata()

    # preparing data for map
    map_df = pd.concat(
        [pd.DataFrame(metadata).set_index("TurbineName"), monthly_energy_by_turbine], axis=1
    ).reset_index()

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


@dagster.op
def my_succeeding_op() -> str:
    return "hello"


@dagster.op
def my_failing_op(hello: str) -> None:
    _ = 1 / 0  # oops
    print(hello + "world")
