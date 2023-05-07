from windy.graphs import my_processing_graph
from windy.io_managers import MyLocalHtmlIoManager


def test_my_processing_graph_generates_viz_html(mock_api_client, tmp_path):
    sample_run_config = {"ops": {"generate_monthly_summary": {"inputs": {"month": "2023-01-01"}}}}
    in_mem_job = my_processing_graph.to_job(
        config=sample_run_config,
        resource_defs={
            "api_client": mock_api_client,
            "html_io_manager": MyLocalHtmlIoManager(directory=tmp_path),
        },
    )
    in_mem_job.execute_in_process()

    expected_fp = tmp_path / "viz.html"
    assert expected_fp.is_file()
