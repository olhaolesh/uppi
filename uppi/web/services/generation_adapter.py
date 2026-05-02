"""Protected web adapter for Stage 4 generation requests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from uppi.config.app_config import project_root
from uppi.domain.exceptions import GenerationPrepareRequiredError
from uppi.domain.failure_registry import FailureRecord
from uppi.web.services.generation_yaml_builder import (
    BuiltGenerationYaml,
    GenerationYamlBuilder,
)

if TYPE_CHECKING:
    from uppi.services.generation_runner import GenerationArtifactRef, GenerationRunnerResult
    from uppi.web.schemas.attestazioni import AttestazioniGenerateRequest


class GenerationRunFailedError(RuntimeError):
    """Raised when a synchronous generation run cannot produce any artifact."""


@dataclass(frozen=True)
class GeneratedRunResult:
    """Web-facing synchronous result returned by Stage 4 generation."""

    run_id: str
    locatore_cf: str
    prepared_output_path_relative: str
    generation_output_path_relative: str
    requested_count: int
    generated_count: int
    failed_count: int
    artifacts: tuple["GenerationArtifactRef", ...]
    messages: tuple[str, ...] = ()
    failure_records: tuple[FailureRecord, ...] = ()


class GenerationAdapter:
    """Thin adapter from web payload to generation YAML builder and current runner."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        yaml_builder: GenerationYamlBuilder | None = None,
        generation_runner_factory: Callable[[], object] | None = None,
    ) -> None:
        self.repo_root = Path(repo_root) if repo_root is not None else project_root()
        self.yaml_builder = yaml_builder or GenerationYamlBuilder(repo_root=self.repo_root)
        self.generation_runner_factory = generation_runner_factory

    def generate(
        self,
        payload: "AttestazioniGenerateRequest",
        *,
        run_id: str | None = None,
    ) -> GeneratedRunResult:
        """Builds one web-run YAML and delegates execution to the current generation-only path."""
        built_yaml = self.yaml_builder.build(payload, run_id=run_id)
        generation_runner = self._build_generation_runner()
        runner_result = generation_runner.run_yaml(
            built_yaml.generation_output_path,
            run_id=built_yaml.run_id,
        )
        self._raise_if_prepare_must_be_repeated(runner_result)
        if runner_result.generated_count <= 0 and runner_result.failed_count > 0:
            raise GenerationRunFailedError(
                "Generation did not produce any artifact for the selected immobili."
            )

        return GeneratedRunResult(
            run_id=runner_result.run_id,
            locatore_cf=runner_result.locatore_cf,
            prepared_output_path_relative=self._to_relative_path(
                built_yaml.prepared_output_path
            ),
            generation_output_path_relative=self._to_relative_path(
                built_yaml.generation_output_path
            ),
            requested_count=built_yaml.requested_count,
            generated_count=runner_result.generated_count,
            failed_count=runner_result.failed_count,
            artifacts=runner_result.artifacts,
            messages=runner_result.messages,
            failure_records=runner_result.failure_records,
        )

    def _build_generation_runner(self):
        if self.generation_runner_factory is not None:
            return self.generation_runner_factory()

        from uppi.services.generation_runner import GenerationRunner

        return GenerationRunner()

    def _to_relative_path(self, path: Path) -> str:
        try:
            return path.relative_to(self.repo_root).as_posix()
        except ValueError:
            return str(path)

    @staticmethod
    def _raise_if_prepare_must_be_repeated(runner_result) -> None:
        if runner_result.generated_count > 0:
            return
        if any(
            record.error_type == "GenerationPrepareRequiredError"
            for record in runner_result.failure_records
        ):
            raise GenerationPrepareRequiredError(
                "Prepared generation input no longer matches current DB state. Run search/prepare again."
            )


__all__ = [
    "GeneratedRunResult",
    "GenerationAdapter",
    "GenerationRunFailedError",
]
