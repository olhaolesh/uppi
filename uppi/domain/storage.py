"""Utils for file storage and path management."""

from pathlib import Path
import logging

# Імпорт Immobile:
# при запуску як модуль (через пакети) працює відносний імпорт,
# при прямому запуску файлу можливі нюанси, але Immobile там не використовується.
try:
    from .immobile import Immobile
except ImportError:  # fallback, якщо хтось запустить як скрипт з іншої точки
    from immobile import Immobile  # type: ignore

logger = logging.getLogger(__name__)

DOWNLOADS_DIR = Path(__file__).resolve().parents[2] / "downloads"


def slugify_immobile(imm: Immobile) -> str:
    """
    Створює унікальний ідентифікатор для Immobile на основі його атрибутів.

    Використовується для імені файлу ATTESTAZIONE_<cf>_<slug>.docx.
    """
    parts = [
        f"F{imm.foglio}",
        f"N{imm.numero}",
    ]
    if imm.sub:
        parts.append(f"S{imm.sub}")
    if getattr(imm, "zona_censuaria", None):
        parts.append(f"Z{imm.zona_censuaria}")
    if getattr(imm, "micro_zona", None):
        parts.append(f"MZ{imm.micro_zona}")
    if imm.categoria:
        parts.append(f"CAT{imm.categoria.replace('/', '')}")
    if getattr(imm, "classe", None):
        parts.append(f"CL{imm.classe}")
    if getattr(imm, "consistenza", None):
        parts.append(f"CONS{imm.consistenza}")

    slug = "_".join(parts)
    logger.debug("[STORAGE] slugify_immobile → %s", slug)
    return slug


def get_attestazione_path(cf: str, imm: Immobile) -> Path:
    """Шлях до файлу ATTESTAZIONE_<cf>_<immobile_slug>.docx у каталозі клієнта."""
    slug = slugify_immobile(imm)
    path = DOWNLOADS_DIR / cf / f"ATTESTAZIONE_{cf}_{slug}.docx"
    logger.debug("[STORAGE] get_attestazione_path(%s, ...) → %s", cf, path)
    return path


def get_client_dir(cf: str) -> Path:
    """Повертає шлях до каталогу для заданого CF та створює його за потреби."""
    client_dir = DOWNLOADS_DIR / cf
    client_dir.mkdir(parents=True, exist_ok=True)
    logger.debug("[STORAGE] get_client_dir(%s) → %s", cf, client_dir)
    return client_dir


def get_visura_path(cf: str) -> Path:
    """Шлях до файлу VISURA_<cf>.pdf у каталозі клієнта."""
    path = get_client_dir(cf) / f"VISURA_{cf}.pdf"
    logger.debug("[STORAGE] get_visura_path(%s) → %s", cf, path)
    return path


if __name__ == "__main__":
    print(DOWNLOADS_DIR)