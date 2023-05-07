from windy.graphs import my_failing_graph, my_processing_graph
from windy.hooks import slack_message_on_failure
from windy.partition_defs import my_partitioned_config

my_processing_job = my_processing_graph.to_job(
    name="my_processing_job", config=my_partitioned_config, hooks={slack_message_on_failure}
)
my_failing_job = my_failing_graph.to_job(name="my_failing_job", hooks={slack_message_on_failure})
