"""Тести для workspace/local-artifacts policy з незмінними default paths."""

from __future__ import annotations

from pathlib import Path

import uppi.domain.storage as storage_module
from uppi.config.app_config import project_root
from uppi.config.workspace import (
    WorkspaceConfig,
    default_artifact_cleanup_policies,
    get_captcha_client_dir,
    get_state_json_path,
)
from uppi.domain.immobile import Immobile


def test_workspace_config_default_paths_preserve_repo_local_defaults(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.delenv("UPPI_WORKSPACE_ROOT", raising=False)

    cfg = WorkspaceConfig.default()

    assert cfg.root == project_root()
    assert cfg.downloads_dir == project_root() / "downloads"
    assert cfg.captcha_images_dir == project_root() / "captcha_images"
    assert cfg.logs_dir == project_root() / "logs"
    assert cfg.state_file == Path("state.json")
    assert get_state_json_path() == Path("state.json")


def test_workspace_config_supports_custom_root_without_moving_state_json_contract(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    custom_root = tmp_path / "custom-workspace"
    monkeypatch.setenv("UPPI_WORKSPACE_ROOT", str(custom_root))

    cfg = WorkspaceConfig.from_env()

    assert cfg.root == custom_root
    assert cfg.downloads_dir == custom_root / "downloads"
    assert cfg.captcha_images_dir == custom_root / "captcha_images"
    assert cfg.logs_dir == custom_root / "logs"
    assert cfg.state_file == Path("state.json")


def test_workspace_config_resolves_relative_override_from_repo_root(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.setenv("UPPI_WORKSPACE_ROOT", "var/workspace")

    cfg = WorkspaceConfig.from_env()

    assert cfg.root == project_root() / "var" / "workspace"


def test_storage_paths_accept_workspace_config_without_changing_current_naming_contract(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    cfg = WorkspaceConfig.from_env(workspace_root=tmp_path)
    imm = Immobile(
        foglio="12",
        numero="345",
        sub="7",
        zona_cens="2",
        micro_zona="5",
        categoria="A/2",
        classe="3",
        consistenza="5 vani",
    )

    visura_path = storage_module.get_visura_path("RSSMRA80A01H501Z", workspace_config=cfg)
    attestazione_path = storage_module.get_attestazione_path(
        "RSSMRA80A01H501Z",
        "41",
        imm,
        workspace_config=cfg,
    )
    captcha_dir = get_captcha_client_dir("RSSMRA80A01H501Z", workspace_root=tmp_path)

    assert visura_path == tmp_path / "downloads" / "RSSMRA80A01H501Z" / "VISURA_RSSMRA80A01H501Z.pdf"
    assert attestazione_path == (
        tmp_path
        / "downloads"
        / "RSSMRA80A01H501Z"
        / "ATTESTAZIONE_RSSMRA80A01H501Z_41_F12_N345_S7_Z2_MZ5_CATA2_CL3_CONS5 vani.docx"
    )
    assert captcha_dir == tmp_path / "captcha_images" / "RSSMRA80A01H501Z"


def test_default_artifact_cleanup_policies_document_current_cleanup_contract():
    """Перевіряє сценарій, описаний у назві тесту."""
    policies = default_artifact_cleanup_policies()

    assert policies["state_json"].cleanup_trigger == "fresh_session_start_and_failed_login"
    assert policies["state_json"].browser_critical is True
    assert policies["captcha_images"].cleanup_trigger == "fresh_session_start"
    assert policies["captcha_images"].auto_cleanup is True
    assert policies["local_visura_pdf"].cleanup_trigger == "post_commit_if_delete_local_visura_after_upload"
    assert policies["local_visura_pdf"].auto_cleanup is False
    assert policies["local_attestazione_docx"].cleanup_trigger == "none"
