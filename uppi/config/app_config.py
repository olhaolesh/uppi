"""Централізовані dataclass-конфіги для low-risk bootstrap і provider-ready seams."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from decouple import config


def project_root() -> Path:
    """Повертає корінь репозиторію для побудови canonical локальних шляхів."""
    return Path(__file__).resolve().parents[2]


def _parse_bool(value: str | bool | None, *, default: bool = False) -> bool:
    """Нормалізує типові env-значення прапорців у булевий тип."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() == "true"


def _normalize_optional_path(value: str | Path | None, *, base_dir: Path | None = None) -> Path | None:
    """Нормалізує optional path-значення без зміни default fallback semantics."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute() and base_dir is not None:
        path = base_dir / path
    return path


@dataclass(frozen=True)
class DatabaseConfig:
    """Описує параметри підключення до PostgreSQL без side effects."""

    host: str
    port: int
    name: str
    user: str
    password: str
    ssl_mode: str

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Читає DB-конфіг з env, зберігаючи current defaults і provider-neutral shape."""
        return cls(
            host=config("DB_HOST", default="localhost"),
            port=int(config("DB_PORT", default="5432")),
            name=config("DB_NAME", default="uppi_db"),
            user=config("DB_USER", default="uppi_user"),
            password=config("DB_PASSWORD", default="uppi_password"),
            ssl_mode=config("DB_SSL_MODE", default="prefer"),
        )

    def connect_kwargs(self) -> dict[str, object]:
        """Повертає kwargs для psycopg2.connect у canonical shape."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.name,
            "user": self.user,
            "password": self.password,
            "sslmode": self.ssl_mode,
        }


@dataclass(frozen=True)
class ImmobiliYamlSourceConfig:
    """Canonical generation input source for single-client `immobili.yml`."""

    immobili_dir: Path
    immobili_file: Path

    @classmethod
    def from_env(
        cls,
        *,
        repo_root: Path | None = None,
        default_immobili_file: Path | None = None,
    ) -> "ImmobiliYamlSourceConfig":
        """Resolves `UPPI_IMMOBILI_YAML` or falls back to the canonical local path."""
        resolved_root = repo_root or project_root()
        fallback_file = default_immobili_file or (resolved_root / "clients" / "immobili.yml")
        env_override = _normalize_optional_path(
            config("UPPI_IMMOBILI_YAML", default=""),
            base_dir=resolved_root,
        )
        immobili_file = env_override or fallback_file
        return cls(
            immobili_dir=immobili_file.parent,
            immobili_file=immobili_file,
        )

    @classmethod
    def default(cls, *, repo_root: Path | None = None) -> "ImmobiliYamlSourceConfig":
        """Returns the canonical generation source config."""
        return cls.from_env(repo_root=repo_root)


@dataclass(frozen=True)
class ClientsCsvSourceConfig:
    """Bulk input source for `clients.csv`, kept separate from generation input."""

    clients_dir: Path
    clients_file: Path

    @classmethod
    def from_env(
        cls,
        *,
        repo_root: Path | None = None,
        default_clients_file: Path | None = None,
    ) -> "ClientsCsvSourceConfig":
        """Resolves future bulk CSV input with an additive env override."""
        resolved_root = repo_root or project_root()
        fallback_file = default_clients_file or (resolved_root / "clients" / "clients.csv")
        env_override = _normalize_optional_path(
            config("UPPI_CLIENTS_CSV", default=""),
            base_dir=resolved_root,
        )
        clients_file = env_override or fallback_file
        return cls(
            clients_dir=clients_file.parent,
            clients_file=clients_file,
        )

    @classmethod
    def default(cls, *, repo_root: Path | None = None) -> "ClientsCsvSourceConfig":
        """Returns the default bulk CSV source config."""
        return cls.from_env(repo_root=repo_root)


@dataclass(frozen=True)
class ClientsSourceConfig:
    """Legacy/transitional source for flat `clients.yml` records."""

    clients_dir: Path
    clients_file: Path
    default_comune: str
    default_tipo_catasto: str
    default_ufficio_label: str

    @classmethod
    def from_env(
        cls,
        *,
        repo_root: Path | None = None,
        default_clients_file: Path | None = None,
        default_comune: str = "PESCARA",
        default_tipo_catasto: str = "F",
        default_ufficio_label: str = "PESCARA Territorio",
    ) -> "ClientsSourceConfig":
        """Повертає clients-source з env override або canonical default fallback."""
        resolved_root = repo_root or project_root()
        fallback_file = default_clients_file or (resolved_root / "clients" / "clients.yml")
        env_override = _normalize_optional_path(
            config("UPPI_CLIENTS_YAML", default=""),
            base_dir=resolved_root,
        )
        clients_file = env_override or fallback_file
        clients_dir = clients_file.parent
        return cls(
            clients_dir=clients_dir,
            clients_file=clients_file,
            default_comune=default_comune,
            default_tipo_catasto=default_tipo_catasto,
            default_ufficio_label=default_ufficio_label,
        )

    @classmethod
    def default(cls, *, repo_root: Path | None = None) -> "ClientsSourceConfig":
        """Повертає canonical clients-source з підтримкою поточного env override."""
        return cls.from_env(repo_root=repo_root)


@dataclass(frozen=True)
class VisuraProcessorRuntimeConfig:
    """Описує non-browser runtime defaults для VisuraProcessor."""

    ae_username: str
    template_version: str
    template_path: Path
    prune_old_immobili_without_contracts: bool
    delete_local_visura_after_upload: bool

    @classmethod
    def from_env(cls, *, repo_root: Path | None = None) -> "VisuraProcessorRuntimeConfig":
        """Читає поточні runtime defaults процесора без зміни їхніх значень."""
        resolved_root = repo_root or project_root()
        return cls(
            ae_username=config("AE_USERNAME", default="").strip(),
            template_version=config("TEMPLATE_VERSION", default="pescara2018_v2").strip(),
            template_path=resolved_root / "attestazione_template" / "template_attestazione_pescara.docx",
            prune_old_immobili_without_contracts=_parse_bool(
                config("PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS", default="True"),
                default=True,
            ),
            delete_local_visura_after_upload=_parse_bool(
                config("DELETE_LOCAL_VISURA_AFTER_UPLOAD", default="False"),
                default=False,
            ),
        )


@dataclass(frozen=True)
class AppConfig:
    """Агрегує bootstrap-конфіг, який далі можна ін’єктувати в сервіси."""

    database: DatabaseConfig
    immobili: ImmobiliYamlSourceConfig
    clients_csv: ClientsCsvSourceConfig
    legacy_clients: ClientsSourceConfig
    visura_processor: VisuraProcessorRuntimeConfig

    @property
    def clients(self) -> ClientsSourceConfig:
        """Legacy alias kept for current runtime surfaces until spider rollout changes."""
        return self.legacy_clients

    @classmethod
    def from_env(cls, *, repo_root: Path | None = None) -> "AppConfig":
        """Будує повний конфіг застосунку з поточних env і canonical paths.

        На цьому етапі джерелом лишаються env/defaults, але caller-ів уже
        відокремлено від конкретного config-provider механізму через dataclass
        surface. Це готує шлях до future SSM/Secrets Manager-backed provider-а
        без зміни runtime semantics цього PR.
        """
        resolved_root = repo_root or project_root()
        return cls(
            database=DatabaseConfig.from_env(),
            immobili=ImmobiliYamlSourceConfig.from_env(repo_root=resolved_root),
            clients_csv=ClientsCsvSourceConfig.from_env(repo_root=resolved_root),
            legacy_clients=ClientsSourceConfig.from_env(repo_root=resolved_root),
            visura_processor=VisuraProcessorRuntimeConfig.from_env(repo_root=resolved_root),
        )
