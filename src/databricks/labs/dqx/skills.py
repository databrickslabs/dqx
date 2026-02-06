import io
import logging
from importlib.resources import files
from pathlib import Path

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def install_skills(folder: str | None = None):
    """Deploy DQX skills to a Databricks workspace.

    Copies bundled SKILL.md files from package resources to the workspace
    so that Databricks Assistant can discover and use them.

    Args:
        folder: Target workspace folder. Defaults to
            ``/Users/{current_user}/.assistant/skills/dqx``.
    """
    ws = WorkspaceClient()

    if folder is None:
        user_name = ws.current_user.me().user_name
        folder = f"/Users/{user_name}/.assistant/skills/dqx"

    skills_root = Path(str(files("databricks.labs.dqx.resources") / "skills" / "dqx"))
    skill_files = sorted(skills_root.rglob("SKILL.md"))

    if not skill_files:
        logger.warning("No SKILL.md files found in package resources")
        return

    logger.info(f"Deploying skills to: {folder}")

    for skill_file in skill_files:
        relative = skill_file.relative_to(skills_root)
        target_path = f"{folder}/{relative}"
        target_dir = "/".join(target_path.split("/")[:-1])

        ws.workspace.mkdirs(target_dir)
        ws.workspace.upload(target_path, io.BytesIO(skill_file.read_bytes()), overwrite=True)
        logger.info(f"  Uploaded: {relative}")

    logger.info(f"Done! {len(skill_files)} skill files deployed to {folder}")
