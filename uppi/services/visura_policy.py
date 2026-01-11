from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from uppi.services.db_repo import VisuraState


@dataclass(frozen=True)
class VisuraDecision:
    should_download: bool
    reason: str


def should_download_visura(
    *,
    force_update: bool,
    db_state: Optional[VisuraState],
    minio_exists: bool,
    now: Optional[datetime] = None,
) -> VisuraDecision:
    if force_update:
        return VisuraDecision(True, "force_update_visura")

    if db_state is None:
        return VisuraDecision(True, "missing_db_record")

    if not minio_exists:
        return VisuraDecision(True, "missing_minio_object")

    if db_state.fetched_at is None:
        return VisuraDecision(True, "missing_fetched_at")

    return VisuraDecision(False, "fresh_enough")
