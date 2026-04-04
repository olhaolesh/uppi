"""Тести для additive Config / DI foundation без зміни current defaults."""

from __future__ import annotations

from pathlib import Path

import yaml

from uppi.config.app_config import AppConfig, ClientsSourceConfig, DatabaseConfig, project_root
from uppi.domain.clients import default_clients_source_config, load_clients
from uppi.domain.db import build_pg_connection_kwargs, get_pg_connection
from uppi.domain.object_storage import ObjectStorage, ObjectStorageConfig, create_object_storage
from uppi.services.storage_minio import StorageService
from uppi.services.visura_processor import VisuraProcessor, default_visura_processor_runtime_config


class _FakeConnection:
    """Мінімальний fake-connection для перевірки DI seam у DB-хелперах."""

    def __init__(self):
        """Ініціалізує тестовий об’єкт із тим самим полем autocommit, що й у psycopg2."""
        self.autocommit = True


class _RecordingClient:
    """Мінімальний fake-клієнт для перевірки lazy client factory у storage."""

    def __init__(self, endpoint: str, *, access_key: str, secret_key: str, secure: bool):
        """Запам’ятовує параметри створення клієнта для assert-перевірок."""
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure


def test_app_config_from_env_uses_current_env_resolution_and_canonical_paths(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.setenv("DB_HOST", "db.example.test")
    monkeypatch.setenv("DB_PORT", "5544")
    monkeypatch.setenv("DB_NAME", "uppi_custom")
    monkeypatch.setenv("DB_USER", "custom_user")
    monkeypatch.setenv("DB_PASSWORD", "custom_password")
    monkeypatch.setenv("DB_SSL_MODE", "require")
    monkeypatch.setenv("AE_USERNAME", "agent.test")
    monkeypatch.setenv("TEMPLATE_VERSION", "custom_template_v1")
    monkeypatch.setenv("PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS", "False")
    monkeypatch.setenv("DELETE_LOCAL_VISURA_AFTER_UPLOAD", "True")
    monkeypatch.setenv("UPPI_CLIENTS_YAML", "")

    cfg = AppConfig.from_env()

    assert cfg.database == DatabaseConfig.from_env()
    assert cfg.database.host == "db.example.test"
    assert cfg.database.port == 5544
    assert cfg.database.name == "uppi_custom"
    assert cfg.database.user == "custom_user"
    assert cfg.database.password == "custom_password"
    assert cfg.database.ssl_mode == "require"
    assert cfg.clients.clients_file == project_root() / "clients" / "clients.yml"
    assert cfg.clients.default_comune == "PESCARA"
    assert cfg.clients.default_tipo_catasto == "F"
    assert cfg.clients.default_ufficio_label == "PESCARA Territorio"
    assert cfg.visura_processor.ae_username == "agent.test"
    assert cfg.visura_processor.template_version == "custom_template_v1"
    assert cfg.visura_processor.prune_old_immobili_without_contracts is False
    assert cfg.visura_processor.delete_local_visura_after_upload is True
    assert cfg.visura_processor.template_path == project_root() / "attestazione_template" / "template_attestazione_pescara.docx"


def test_get_pg_connection_accepts_explicit_database_config_and_connect_factory():
    """Перевіряє сценарій, описаний у назві тесту."""
    cfg = DatabaseConfig(
        host="db.example.test",
        port=5544,
        name="uppi_custom",
        user="custom_user",
        password="custom_password",
        ssl_mode="require",
    )
    calls = {}

    def fake_connect(**kwargs):
        calls["kwargs"] = kwargs
        return _FakeConnection()

    conn = get_pg_connection(cfg, connect_factory=fake_connect)

    assert build_pg_connection_kwargs(cfg) == {
        "host": "db.example.test",
        "port": 5544,
        "dbname": "uppi_custom",
        "user": "custom_user",
        "password": "custom_password",
        "sslmode": "require",
    }
    assert calls["kwargs"] == build_pg_connection_kwargs(cfg)
    assert conn.autocommit is False


def test_create_object_storage_uses_explicit_loader_and_lazy_client_factory():
    """Перевіряє сценарій, описаний у назві тесту."""
    cfg = ObjectStorageConfig(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
        visure_bucket="visure-bucket",
        attestazioni_bucket="attestazioni-bucket",
    )

    storage = create_object_storage(cfg=None, config_loader=lambda: cfg, client_factory=_RecordingClient)

    assert storage.cfg == cfg
    client = storage.client
    assert isinstance(client, _RecordingClient)
    assert client.endpoint == "localhost:9000"
    assert client.access_key == "minioadmin"
    assert client.secret_key == "minioadmin"
    assert client.secure is False


def test_load_clients_accepts_explicit_source_config_without_changing_default_contract(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.setenv("UPPI_CLIENTS_YAML", "")
    clients_dir = tmp_path / "custom-clients"
    clients_dir.mkdir()
    clients_file = clients_dir / "clients.yml"
    clients_file.write_text(
        yaml.safe_dump(
            [{"LOCATORE_CF": "ABCDEF12G34H567I"}],
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    source_config = ClientsSourceConfig(
        clients_dir=clients_dir,
        clients_file=clients_file,
        default_comune="PESCARA",
        default_tipo_catasto="F",
        default_ufficio_label="PESCARA Territorio",
    )

    rows = load_clients(source_config=source_config)

    assert default_clients_source_config().clients_file == project_root() / "clients" / "clients.yml"
    assert len(rows) == 1
    assert rows[0]["LOCATORE_CF"] == "ABCDEF12G34H567I"
    assert rows[0]["COMUNE"] == "PESCARA"
    assert rows[0]["TIPO_CATASTO"] == "F"


def test_app_config_from_env_uses_uppi_clients_yaml_override_for_clients_source(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    override_path = tmp_path / "override-clients.yml"
    monkeypatch.setenv("UPPI_CLIENTS_YAML", str(override_path))

    cfg = AppConfig.from_env()

    assert cfg.clients.clients_file == override_path
    assert cfg.clients.clients_dir == override_path.parent


def test_visura_processor_uses_injected_storage_and_runtime_config_without_changing_default_wiring():
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = ObjectStorage(
        cfg=ObjectStorageConfig(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
            visure_bucket="visure-bucket",
            attestazioni_bucket="attestazioni-bucket",
        ),
        client_factory=_RecordingClient,
    )
    runtime_config = default_visura_processor_runtime_config(
        template_path=Path("/tmp/custom-template.docx")
    )

    processor = VisuraProcessor(
        storage=storage,
        storage_service=StorageService(storage),
        connection_factory=lambda: _FakeConnection(),
        parser_factory=lambda: None,
        runtime_config=runtime_config,
    )

    assert processor.storage is storage
    assert processor.storage_service.storage is storage
    assert processor.runtime_config == runtime_config
    assert processor.template_path == Path("/tmp/custom-template.docx")
