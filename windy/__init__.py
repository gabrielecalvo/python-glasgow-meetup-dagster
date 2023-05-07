from dagster import Definitions

from windy.assets import map_visualisation_asset, monthly_summary_asset
from windy.jobs import my_failing_job, my_processing_job
from windy.resources import local_resources
from windy.schedules import my_processing_job_schedule

defs = Definitions(
    assets=[monthly_summary_asset, map_visualisation_asset],
    jobs=[my_processing_job, my_failing_job],
    schedules=[my_processing_job_schedule],
    resources=local_resources,
)
