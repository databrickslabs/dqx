import io
import logging
from importlib.resources import files
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

logger = logging.getLogger(__name__)


def install_skills(folder: str | None = None):
    """Deploy DQX skills to a Databricks workspace.

    Copies bundled SKILL.md files and example notebooks from package
    resources to the workspace so that Databricks Assistant can discover
    and use them.

    Args:
        folder: Target workspace folder. Defaults to
            ``/Users/{current_user}/.assistant/skills/dqx``.
    """
    ws = WorkspaceClient()

    if folder is None:
        user_name = ws.current_user.me().user_name
        folder = f"/Users/{user_name}/.assistant/skills/dqx"

    skills_root = Path(str(files("databricks.labs.dqx.resources") / "skills" / "dqx"))

    # Collect all files from the skills directory
    deploy_files = sorted(f for f in skills_root.rglob("*") if f.is_file())

    if not deploy_files:
        logger.warning("No skill files found in package resources")
        return

    logger.info(f"Deploying skills to: {folder}")

    for src_file in deploy_files:
        relative = src_file.relative_to(skills_root)
        target_path = f"{folder}/{relative}"
        target_dir = "/".join(target_path.split("/")[:-1])

        ws.workspace.mkdirs(target_dir)
        ws.workspace.upload(target_path, io.BytesIO(src_file.read_bytes()), format=ImportFormat.AUTO, overwrite=True)
        logger.info(f"  Uploaded: {relative}")

    logger.info(f"Done! {len(deploy_files)} skill files deployed to {folder}")
