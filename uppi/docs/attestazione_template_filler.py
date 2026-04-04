"""Compatibility shim для DOCX template filler зі старого шляху `uppi.docs`."""

from uppi.services.attestazione_template_filler import (
    fill_attestazione_template,
    fill_underscored,
    replace_in_cell,
    replace_in_paragraph,
    underscored,
)

__all__ = [
    "fill_attestazione_template",
    "fill_underscored",
    "replace_in_cell",
    "replace_in_paragraph",
    "underscored",
]
