import datetime as dt

import dagster


@dagster.monthly_partitioned_config(start_date=dt.datetime(2023, 1, 1))
def my_partitioned_config(start: dt.datetime, _end: dt.datetime) -> dict:
    return {"ops": {"generate_monthly_summary": {"inputs": {"month": start.strftime("%Y-%m-%d")}}}}
