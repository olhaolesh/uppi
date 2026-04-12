"""Production Scrapy entry point for generation-only processing."""

from __future__ import annotations

from uuid import uuid4

import scrapy

from uppi.domain.immobili_document import default_immobili_source_config, load_immobili_document
from uppi.items import UppiItem
from uppi.logging_config import configure_uppi_logging
from uppi.utils.immobili_item_mapper import map_immobili_document_to_item


class UppiSpider(scrapy.Spider):
    """Run generation only from a prepared single-client `immobili.yml` document."""

    name = "uppi"
    allowed_domains: list[str] = []

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Attach centralized logging before instantiating the production spider."""
        configure_uppi_logging()
        return super().from_crawler(crawler, *args, **kwargs)

    async def start(self):
        """Load prepared YAML, filter active immobili, and enter generation only."""
        source_config = default_immobili_source_config()
        document = load_immobili_document(source_config=source_config)
        active_immobili = [immobile for immobile in document.immobili if immobile.enabled]

        # `scrapy crawl uppi` is generation-only. Fetch/update ownership lives in
        # prepare and bulk import modes, so this spider never calls the browser path.
        self.crawl_run_id = uuid4().hex

        self.logger.info(
            "[START] Loaded prepared immobili.yml for LOCATORE_CF=%s from %s",
            document.locatore_cf,
            source_config.immobili_file,
        )
        self.logger.info(
            "[START] Active immobili for generation: %d/%d",
            len(active_immobili),
            len(document.immobili),
        )

        if not active_immobili:
            self.logger.warning(
                "[START] No active immobili found in prepared document for LOCATORE_CF=%s",
                document.locatore_cf,
            )
            return

        total = len(active_immobili)
        for index, immobile in enumerate(active_immobili, start=1):
            mapped = map_immobili_document_to_item(document, immobile)
            mapped["run_id"] = self.crawl_run_id

            self.logger.info(
                "[GENERATION %d/%d] Queueing LOCATORE_CF=%s F=%s N=%s SUB=%s",
                index,
                total,
                document.locatore_cf,
                mapped.get("foglio"),
                mapped.get("numero"),
                mapped.get("sub"),
            )
            yield UppiItem(**mapped)
