from abc import ABC, abstractmethod
import logging
import os
import json
import yaml

from databricks.labs.dqx.engine_core import DQEngineCore
from databricks.sdk.errors import NotFound

logger = logging.getLogger(__name__)
COLLECT_LIMIT_WARNING = 500


class ChecksHandler(ABC):
    """Abstract base for checks I/O handlers."""

    def __init__(self, spark, ws=None):
        self.spark = spark
        self.ws = ws

    @abstractmethod
    def load(self, source: str, run_config_name: str | None = None) -> list[dict]:
        pass

    @abstractmethod
    def save(
        self, checks: list[dict], target: str, run_config_name: str | None = None, mode: str | None = None
    ) -> None:
        pass


class DeltaTableChecksHandler(ChecksHandler):
    """Handler for Delta tables."""

    def load(self, source: str, run_config_name: str | None = "default") -> list[dict]:
        df = self.spark.table(source).where(f"run_config_name = '{run_config_name}'")
        return DQEngineCore.build_quality_rules_from_dataframe(df, run_config_name)

    def save(
        self,
        checks: list[dict],
        target: str,
        run_config_name: str | None = "default",
        mode: str | None = "append",
    ) -> None:
        df = DQEngineCore.build_dataframe_from_quality_rules(self.spark, checks, run_config_name=run_config_name)
        df.write.format("delta").mode(mode).option(
            "replaceWhere", f"run_config_name = '{run_config_name}'"
        ).saveAsTable(target)


class WorkspaceFileChecksHandler(ChecksHandler):
    """Handler for Databricks workspace files (JSON/YAML). Needs ws with export/upload."""

    def load(self, source: str, run_config_name: str | None = None) -> list[dict]:
        if not self.ws:
            raise RuntimeError("Workspace client required for workspace file operations")
        content = self.ws.workspace.export(source, format="AUTO").content.decode("utf-8")
        ext = os.path.splitext(source)[-1].lower()
        if ext == ".json":
            try:
                checks = json.loads(content)
            except Exception:
                checks = [json.loads(line) for line in content.splitlines() if line.strip()]
        elif ext in {".yaml", ".yml"}:
            checks = yaml.safe_load(content)
        else:
            raise ValueError(f"Unsupported workspace file extension: {ext}")
        if not checks:
            raise ValueError(f"No valid checks found in workspace file: {source}")
        return checks

    def save(
        self, checks: list[dict], target: str, run_config_name: str | None = None, mode: str | None = None
    ) -> None:
        if not self.ws:
            raise RuntimeError("Workspace client required for workspace file operations")
        ext = os.path.splitext(target)[-1].lower()
        if ext == ".json":
            content = json.dumps(checks, indent=2).encode("utf-8")
        elif ext in {".yaml", ".yml"}:
            content = yaml.safe_dump(checks, allow_unicode=True).encode("utf-8")
        else:
            raise ValueError(f"Unsupported workspace file extension: {ext}")
        dir_path = os.path.dirname(target)
        self.ws.workspace.mkdirs(dir_path)
        self.ws.workspace.upload(target, content, format="AUTO", overwrite=True)


class UCVolumeChecksHandler(ChecksHandler):
    """Handler for UC volume files (/Volumes/...). Works with yaml or json."""

    def load(self, source: str, run_config_name: str | None = None) -> list[dict]:
        """
        Load checks (data quality rules) from a YAML or JSON file in the specified volume path.

        Supzorts both `.yaml`/`.yml` and `.json` formats. The file extension determines the format to load.
        This does not require the installation of DQX in the workspace.
        The loaded checks can be used as input for the `apply_checks_by_metadata` function.

        :param source: Path to the checks file in the volume (must start with /Volumes/).
                            The file must be in YAML or JSON format; the extension is used to infer the format.
        :param run_config_name: run_config_name
        :return: List of data quality rules (each as a dict).
        :raises ValueError: If `volume_path` is not a valid Volume path, if the file format is unsupported, or if the file contains no valid checks.
        :raises Exception: For any I/O, YAML, or JSON parsing errors.
        """
        self._is_volume_path(path=source)

        logger.info(f"Loading quality rules (checks) from {source} in UC Volume.")
        parsed_checks = self._load_checks_from_uc_volume(volume_path=source)
        return parsed_checks

    def save(
        self, checks: list[dict], target: str, run_config_name: str | None = None, mode: str | None = None
    ) -> None:
        """
        Save checks (dq rules) to a YAML file at the given volume path.

        :param checks: List of dq rules (each as a dict) to save
        :param target: Destination path in the volume (must start with /Volumes/)
        :param run_config_name: run_config_name
        :param mode: mode
        :raises ValueError: if volume_path is not a Volume path
        :raises Exception: for I/O or YAML errors
        """
        self._is_volume_path(path=target)

        logger.info(f"Saving quality rules (checks) to {target} in the Volume.")
        self._save_checks_in_uc_volume(checks=checks, volume_path=target)

    def _is_volume_path(self, path: str) -> bool:
        self._validate_volume_prefix(path=path)
        path_parts = self._get_path_parts(path=path)
        self._validate_path_depth(path_parts=path_parts)
        volume_mount = self._get_volume_mount(path_parts=path_parts)
        self._validate_volume_mount(volume_mount=volume_mount)
        return True

    def _validate_volume_prefix(self, path: str) -> None:
        if not path.startswith("/Volumes/"):
            logger.error(f"Provided path does not look like a Volume path: {path}")
            raise ValueError(f"Path must start with '/Volumes/': {path}")

    def _get_path_parts(self, path: str) -> list:
        return [part for part in path.split(os.sep) if part]

    def _validate_path_depth(self, path_parts: list) -> None:
        if len(path_parts) < 4:
            logger.error("Path must be at least '/Volumes/<catalog>/<schema>/<volume>/...'")
            raise ValueError("Path must be at least '/Volumes/<catalog>/<schema>/<volume>/...'")

    def _get_volume_mount(self, path_parts: list) -> str:
        return os.sep + os.sep.join(path_parts[:4])

    def _validate_volume_mount(self, volume_mount: str) -> None:
        if not os.path.isdir(volume_mount):
            logger.error(f"Provided Volume path does not exists: {volume_mount}")
            raise NotFound(f"Provided Volume path does not exists: {volume_mount}")

    def _load_checks_from_uc_volume(self, volume_path: str) -> list[dict]:
        filename = os.path.basename(volume_path)
        logger.info(f"Loading quality rules (checks) from {volume_path} in the Unity Catalog volume.")
        try:
            with open(volume_path, "r", encoding="utf-8") as f:
                if filename.endswith(".json"):
                    parsed_checks = json.load(f)
                elif filename.endswith((".yaml", ".yml")):
                    parsed_checks = yaml.safe_load(f)
                else:
                    raise ValueError("Checks file must be .json, .yaml, or .yml")
        except Exception as e:
            raise ValueError(f"Failed to load or parse checks file: {e}") from e
        if not parsed_checks:
            raise ValueError(f"Invalid or no checks in UC volume file: {volume_path}")
        return parsed_checks

    def _save_checks_in_uc_volume(self, checks: list[dict], volume_path: str) -> None:
        """
        Save checks (dq rules) to a YAML file at the given volume path.

        :param checks: List of dq rules (each as a dict) to save
        :param volume_path: Destination path in the volume (must start with /Volumes/)
        :raises ValueError: if volume_path is not a Volume path
        :raises Exception: for I/O or YAML errors
        """
        volume_dir = os.path.dirname(volume_path)
        os.makedirs(volume_dir, exist_ok=True)
        logger.info(f"Saving quality rules (checks) to {volume_path} in the Volume.")

        try:
            with open(volume_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(checks, f, allow_unicode=True)
            logger.info(f"Successfully saved checks to {volume_path}.")
        except Exception as e:
            logger.error(f"Failed to save checks to {volume_path}: {e}")
            raise
