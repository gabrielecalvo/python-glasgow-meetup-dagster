import dagster

from windy.ops import generate_map_visualisation, generate_monthly_summary, my_failing_op, my_succeeding_op


@dagster.graph
def my_processing_graph() -> None:
    energy_df = generate_monthly_summary()
    generate_map_visualisation(monthly_energy_by_turbine=energy_df)


@dagster.graph
def my_failing_graph() -> None:
    x = my_succeeding_op()
    my_failing_op(x)
