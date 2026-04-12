"""Scrapy pipeline, що делегує non-browser обробку сервісному процесору."""

# uppi/pipelines.py
from __future__ import annotations

from uppi.logging_config import configure_uppi_logging
from uppi.services.visura_processor import VisuraProcessor


class UppiPipeline:
    """
    Minimal glue: delegate generation-only items to VisuraProcessor service.
    """

    def __init__(self):
        """Піднімає logging foundation і створює сервіс обробки item-ів."""
        configure_uppi_logging()
        self.processor = VisuraProcessor()

    def process_item(self, item, spider):
        """Передає item у generation-only boundary без import/browser continuation."""
        return self.processor.process_generation_item(item, spider)


class UppiImportPipeline:
    """Delegate items to the import-only processor entry point."""

    def __init__(self):
        """Піднімає logging foundation і створює сервіс обробки item-ів."""
        configure_uppi_logging()
        self.processor = VisuraProcessor()

    def process_item(self, item, spider):
        """Передає item у import-only boundary без generation continuation."""
        return self.processor.process_import_item(item, spider)
