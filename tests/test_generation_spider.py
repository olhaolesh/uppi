"""Focused tests for the generation-only production spider."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

from itemadapter import ItemAdapter

import uppi.spiders.uppi_spider as spider_module
from uppi.config.immobili import ImmobileConfig, ImmobiliDocumentConfig
from uppi.items import UppiItem


async def _consume(async_iterable) -> list:
    """Collect one async generator into a deterministic in-memory list."""
    return [item async for item in async_iterable]


def test_generation_spider_reads_prepared_immobili_yaml_and_filters_disabled_records(monkeypatch, tmp_path):
    """The production spider must read `immobili.yml` and only queue active immobili."""
    source_config = SimpleNamespace(immobili_file=tmp_path / "clients" / "immobili.yml")
    load_calls = []
    document = ImmobiliDocumentConfig(
        locatore_cf="RSSMRA80A01H501Z",
        comune="PESCARA",
        tipo_catasto="F",
        ufficio_label="PESCARA Territorio",
        locatore_comune_res="Pescara",
        locatore_via="Via Roma",
        locatore_civico="10",
        immobili=(
            ImmobileConfig(
                enabled=True,
                foglio="12",
                numero="345",
                sub="7",
                contract_kind="TRANSITORIO",
                elements={"a1": "X"},
            ),
            ImmobileConfig(
                enabled=False,
                foglio="20",
                numero="100",
                sub="2",
            ),
        ),
    )

    monkeypatch.setattr(spider_module, "default_immobili_source_config", lambda: source_config)
    monkeypatch.setattr(
        spider_module,
        "load_immobili_document",
        lambda *, source_config: load_calls.append(source_config) or document,
    )

    spider = spider_module.UppiSpider()
    yielded = asyncio.run(_consume(spider.start()))

    assert load_calls == [source_config]
    assert len(yielded) == 1
    assert isinstance(yielded[0], UppiItem)

    adapter = ItemAdapter(yielded[0])
    assert adapter.get("locatore_cf") == "RSSMRA80A01H501Z"
    assert adapter.get("foglio") == "12"
    assert adapter.get("numero") == "345"
    assert adapter.get("sub") == "7"
    assert adapter.get("contract_kind") == "TRANSITORIO"
    assert adapter.get("a1") == "X"
    assert adapter.get("run_id") == spider.crawl_run_id


def test_generation_spider_yields_items_without_browser_requests(monkeypatch, tmp_path):
    """The production spider must enqueue generation items directly, not Playwright requests."""
    source_config = SimpleNamespace(immobili_file=tmp_path / "clients" / "immobili.yml")
    document = ImmobiliDocumentConfig(
        locatore_cf="RSSMRA80A01H501Z",
        immobili=(
            ImmobileConfig(enabled=True, foglio="12", numero="345", sub="7"),
        ),
    )

    monkeypatch.setattr(spider_module, "default_immobili_source_config", lambda: source_config)
    monkeypatch.setattr(spider_module, "load_immobili_document", lambda *, source_config: document)

    spider = spider_module.UppiSpider()
    yielded = asyncio.run(_consume(spider.start()))

    assert len(yielded) == 1
    assert not hasattr(yielded[0], "meta")
    assert spider.allowed_domains == []
