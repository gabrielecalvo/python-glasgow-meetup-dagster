from dagster import materialize_to_memory

from windy import map_visualisation_asset, monthly_summary_asset


def test_asset_materialization(mock_api_client):
    result = materialize_to_memory(
        assets=[monthly_summary_asset, map_visualisation_asset],
        partition_key="2023-01-01",
        resources={"api_client": mock_api_client},
    )
    assert result.success
    assert result.output_for_node("map_visualisation_asset").startswith("<html>")
