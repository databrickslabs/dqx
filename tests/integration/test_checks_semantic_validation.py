import pytest

from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.checks_semantic_validator import ChecksSemanticValidationMode

DUPLICATE_CHECKS = [
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "col1"}}},
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "col1"}}},
]

CONFLICTING_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_in_range", "arguments": {"column": "col1", "min_limit": 0, "max_limit": 100}},
    },
    {
        "criticality": "error",
        "check": {"function": "is_in_range", "arguments": {"column": "col1", "min_limit": 0, "max_limit": 50}},
    },
]


def test_validate_checks_fails_on_duplicate_rules(ws, spark):
    dq_engine = DQEngine(ws, spark)
    with pytest.raises(ValueError, match="Semantic validation failed"):
        dq_engine.validate_checks(DUPLICATE_CHECKS, semantic_validation_mode=ChecksSemanticValidationMode.FAIL)


def test_validate_checks_fails_on_conflicting_rules(ws, spark):
    dq_engine = DQEngine(ws, spark)
    with pytest.raises(ValueError, match="Semantic validation failed"):
        dq_engine.validate_checks(CONFLICTING_CHECKS, semantic_validation_mode=ChecksSemanticValidationMode.FAIL)


def test_load_checks_fails_on_duplicate_rules(ws, spark, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"

    dq_engine = DQEngine(ws, spark)
    config = WorkspaceFileChecksStorageConfig(location=checks_path)
    # Persist a ruleset with duplicates without triggering validation on save.
    dq_engine.save_checks(DUPLICATE_CHECKS, config=config, semantic_validation_mode=None)

    with pytest.raises(ValueError, match="Semantic validation failed"):
        dq_engine.load_checks(config=config, semantic_validation_mode=ChecksSemanticValidationMode.FAIL)


def test_save_checks_fails_on_duplicate_rules(ws, spark, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"

    dq_engine = DQEngine(ws, spark)
    config = WorkspaceFileChecksStorageConfig(location=checks_path)

    with pytest.raises(ValueError, match="Semantic validation failed"):
        dq_engine.save_checks(
            DUPLICATE_CHECKS, config=config, semantic_validation_mode=ChecksSemanticValidationMode.FAIL
        )


def test_save_checks_fails_on_conflicting_rules(ws, spark, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"

    dq_engine = DQEngine(ws, spark)
    config = WorkspaceFileChecksStorageConfig(location=checks_path)

    with pytest.raises(ValueError, match="Semantic validation failed"):
        dq_engine.save_checks(
            CONFLICTING_CHECKS, config=config, semantic_validation_mode=ChecksSemanticValidationMode.FAIL
        )
