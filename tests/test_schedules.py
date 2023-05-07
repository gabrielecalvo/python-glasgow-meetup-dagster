import datetime as dt

import dagster

from tests.conftest import mock_resources
from windy.graphs import my_processing_graph
from windy.hooks import slack_message_on_failure
from windy.partition_defs import my_partitioned_config
from windy.schedules import my_processing_job_schedule


def test_configurable_job_schedule():
    test_job = my_processing_graph.to_job(
        resource_defs=mock_resources,
        config=my_partitioned_config,
        hooks={slack_message_on_failure},
    )
    context = dagster.build_schedule_context(scheduled_execution_time=dt.datetime(2020, 1, 1), resources=mock_resources)
    run_request = my_processing_job_schedule(context)
    assert dagster.validate_run_config(test_job, run_request.run_config)
