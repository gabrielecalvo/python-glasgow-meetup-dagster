from unittest.mock import MagicMock

import dagster

from windy.hooks import FAILURE_MESSAGE_TEMPLATE, slack_message_on_failure


def test_slack_hook():
    mock_slack = MagicMock()

    @dagster.op
    def oops_op():
        ...

    context = dagster.build_hook_context(
        resources={"slack": mock_slack}, op=oops_op, op_exception=ValueError("Everything is on file!!!")
    )
    slack_message_on_failure(context)

    assert mock_slack.get_client.return_value.chat_postMessage.called_once_with(
        channel="#demo", text=FAILURE_MESSAGE_TEMPLATE.format(context=context)
    )
