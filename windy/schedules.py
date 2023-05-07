import dagster

from windy.jobs import my_processing_job
from windy.ops import generate_monthly_summary


@dagster.schedule(job_name=my_processing_job.name, cron_schedule="0 0 * * *", execution_timezone="UTC")
def my_processing_job_schedule(context: dagster.ScheduleEvaluationContext) -> dagster.RunRequest:
    month = context.scheduled_execution_time.replace(day=1).strftime("%Y-%m-%d")
    return dagster.RunRequest(
        run_config={"ops": {generate_monthly_summary.name: {"inputs": {"month": month}}}},
        tags={"month": month},
        partition_key=month,
    )
