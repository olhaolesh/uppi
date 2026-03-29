"""
Legacy-хелпер для повторних спроб.

У production-потоці проєкт уже використовує `tenacity`, але модуль залишено
для сумісності та локальних утиліт, які ще можуть його імпортувати.
"""

from __future__ import annotations

import time
from typing import Callable, Iterable, Optional, Type


def retry(
    func: Callable[[], object],
    *,
    exceptions: Iterable[Type[BaseException]],
    attempts: int = 3,
    base_delay: float = 0.5,
    backoff: float = 2.0,
    logger: Optional[object] = None,
    context: str = "",
):
    """Запускає callable з простою retry/backoff-логікою."""
    delay = base_delay
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except tuple(exceptions) as exc:
            if attempt >= attempts:
                raise
            if logger is not None:
                logger.warning("[RETRY] %s attempt %d failed: %s", context, attempt, exc)
            time.sleep(delay)
            delay *= backoff
    return None
