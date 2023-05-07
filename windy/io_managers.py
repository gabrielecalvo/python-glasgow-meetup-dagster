from pathlib import Path

from dagster import InputContext, IOManager, OutputContext


class MyLocalHtmlIoManager(IOManager):
    def __init__(self, directory: Path):
        self.directory = directory

    def handle_output(self, context: OutputContext, obj: str) -> None:
        try:
            partition_suffix = f"-{context.partition_key}"
        except Exception:
            partition_suffix = ""

        fp = Path(self.directory) / f"viz{partition_suffix}.html"
        with open(fp, "w", encoding="utf8") as f:
            f.write(obj)
        context.log.info(f"HTML file stored here: {fp}")

    def load_input(self, context: InputContext) -> None:
        raise NotImplementedError()
