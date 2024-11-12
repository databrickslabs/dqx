from dataclasses import dataclass

from databricks.sdk.core import Config

__all__ = ["WorkspaceConfig"]


@dataclass
class WorkspaceConfig:
    """Configuration class for the workspace"""

    __file__ = "config.yml"
    __version__ = 2

    log_level: str | None = "INFO"
    connect: Config | None = None

    input_location: str | None = None  # input data path or a table
    output_location: str | None = None  # output data path or a table
    quarantine_location: str | None = None  # quarantined data path or a table
    curated_location: str | None = None  # curated data path or a table
    checks_file: str | None = None  # name of the file containing quality rules / checks
    profile_summary_stats_file: str | None = None  # name of the file containing profile summary statistics
