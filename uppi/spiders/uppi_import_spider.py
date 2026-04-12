"""Internal Scrapy entry point for the import-only browser pipeline."""

from __future__ import annotations

from uppi.spiders.uppi_spider import UppiSpider


class UppiImportSpider(UppiSpider):
    """Reuse the browser flow and stop at the import-only pipeline boundary."""

    name = "uppi_import"
    custom_settings = {
        # Keep the browser-critical spider flow unchanged and only switch the
        # non-browser boundary so the run stops after `ImmobileSync`.
        "ITEM_PIPELINES": {
            "uppi.pipelines.UppiImportPipeline": 300,
        },
    }
