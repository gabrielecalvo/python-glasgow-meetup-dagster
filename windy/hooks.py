from dagster import HookContext, failure_hook

FAILURE_MESSAGE_TEMPLATE = (
    ":wave: Hello Python Glasgow!\nsomething went wrong in *{context.op.name}*:\n>`{context.op_exception}`"
)


@failure_hook(required_resource_keys={"slack"})
def slack_message_on_failure(context: HookContext) -> None:
    context.resources.slack.get_client().chat_postMessage(
        channel="#demo",
        text=FAILURE_MESSAGE_TEMPLATE.format(context=context),
    )
