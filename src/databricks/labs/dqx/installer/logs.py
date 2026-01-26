import contextlib
import datetime as dt
import logging
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import TextIO

from databricks.labs.blueprint.logger import install_logger

from databricks.sdk.retries import retried

from databricks.labs.dqx.__about__ import __version__

logger = logging.getLogger(__name__)


@dataclass
class LogRecord:
    timestamp: int
    job_id: int
    job_name: str
    task_name: str
    job_run_id: int
    level: str
    component: str
    message: str


@dataclass
class PartialLogRecord:
    """The information found within a log file record."""

    time: dt.time
    level: str
    component: str
    message: str


def peak_multi_line_message(log: TextIO, pattern: re.Pattern) -> tuple[str, re.Match | None, str]:
    """
    A single log record message may span multiple log lines. In this case, the regex on
    subsequent lines do not match.

    Args:
         log (TextIO): The log file IO.
         pattern (re.Pattern): The regex pattern for a log line.
    """
    multi_line_message = ""
    line = log.readline()
    match = pattern.match(line)
    while len(line) > 0 and match is None:
        multi_line_message += "\n" + line.rstrip()
        line = log.readline()
        match = pattern.match(line)
    return line, match, multi_line_message


def parse_logs(log: TextIO) -> Iterator[PartialLogRecord]:
    """Parse the logs to retrieve values for PartialLogRecord fields.

    Args:
         log (TextIO): The log file IO.
    """
    time_format = "%H:%M:%S"
    # This regex matches the log format defined in databricks.labs.dqx.installer.logs.TaskLogger
    log_format = r"(\d+:\d+:\d+)\s(\w+)\s\[(.+)\]\s\{\w+\}\s(.+)"
    pattern = re.compile(log_format)

    line = log.readline()
    match = pattern.match(line)
    if match is None:
        logger.warning(f"Logs do not match expected format ({log_format}): {line}")
        return
    while len(line) > 0:
        assert match is not None
        time, *groups, message = match.groups()

        next_line, next_match, multi_line_message = peak_multi_line_message(log, pattern)

        time = dt.datetime.strptime(time, time_format).time()
        # Mypy can't determine length of regex expressions
        partial_log_record = PartialLogRecord(time, *groups, message + multi_line_message)  # type: ignore

        yield partial_log_record

        line, match = next_line, next_match


class TaskLogger(contextlib.AbstractContextManager):
    # files are available in the workspace only once their handlers are closed,
    # so we rotate files log every minute to make them available for download.
    #
    # See https://docs.python.org/3/library/logging.handlers.html#logging.handlers.TimedRotatingFileHandler
    # See https://docs.python.org/3/howto/logging-cookbook.html

    def __init__(
        self,
        install_dir: Path,
        workflow: str,
        job_id: str,
        task_name: str,
        job_run_id: str,
        log_level="INFO",
        attempt: str = "0",
    ):
        self._log_level = log_level
        self._workflow = workflow
        self._job_id = job_id
        self._job_run_id = job_run_id
        self._databricks_logger = logging.getLogger("databricks")
        self._app_logger = logging.getLogger("databricks.labs.dqx")
        self._log_path = self._get_log_path(install_dir, workflow, job_run_id, attempt)
        self.log_file = self._log_path / f"{task_name}.log"
        self._app_logger.info(f"DQX v{__version__} After workflow finishes, see debug logs at {self.log_file}")

    @classmethod
    def _get_log_path(cls, install_dir: Path, workflow: str, workflow_run_id: str | int, attempt: str | int) -> Path:
        return install_dir / "logs" / workflow / f"run-{workflow_run_id}-{attempt}"

    def __repr__(self):
        return self.log_file.as_posix()

    def __enter__(self):
        self._log_path.mkdir(parents=True, exist_ok=True)
        self._init_debug_logfile()
        self._init_run_readme()
        self._databricks_logger.setLevel(logging.DEBUG)
        self._app_logger.setLevel(logging.DEBUG)
        console_handler = install_logger(self._log_level)
        self._databricks_logger.removeHandler(console_handler)
        self._databricks_logger.addHandler(self._file_handler)
        return self

    def __exit__(self, _t, error, _tb):
        if error:
            log_file_for_cli = str(self.log_file).removeprefix("/Workspace")
            cli_command = f"databricks workspace export /{log_file_for_cli}"
            self._app_logger.error(f"Execute `{cli_command}` locally to troubleshoot with more details. {error}")
            self._databricks_logger.debug("Task crash details", exc_info=error)
        self._file_handler.flush()
        self._file_handler.close()

    def _init_debug_logfile(self):
        log_format = "%(asctime)s %(levelname)s [%(name)s] {%(threadName)s} %(message)s"
        log_formatter = logging.Formatter(fmt=log_format, datefmt="%H:%M:%S")
        self._file_handler = TimedRotatingFileHandler(self.log_file.as_posix(), when="M", interval=1)
        self._file_handler.setFormatter(log_formatter)
        self._file_handler.setLevel(logging.DEBUG)

    def _init_run_readme(self):
        log_readme = self._log_path.joinpath("README.md")
        if log_readme.exists():
            return
        # this may race when run from multiple tasks, therefore it must be multiprocess safe
        with self._exclusive_open(str(log_readme), mode="w") as f:
            f.write(f"# Logs for the DQX {self._workflow} workflow\n")
            f.write("This folder contains DQX log files.\n\n")
            f.write(f"See the [{self._workflow} workflow](/#job/{self._job_id}) and ")
            f.write(f"[run #{self._job_run_id}](/#job/{self._job_id}/run/{self._job_run_id})\n")

    @classmethod
    @contextmanager
    def _exclusive_open(cls, filename: str, **kwargs):
        """Open a file with exclusive access across multiple processes.
        Requires write access to the directory containing the file.

        Arguments are the same as the built-in open.

        Returns a context manager that closes the file and releases the lock.
        """
        lockfile_name = filename + ".lock"
        lockfile = cls._create_lock(lockfile_name)

        try:
            with open(filename, encoding="utf-8", **kwargs) as f:
                yield f
        finally:
            try:
                os.close(lockfile)
            finally:
                os.unlink(lockfile_name)

    @staticmethod
    @retried(on=[FileExistsError], timeout=timedelta(seconds=5))
    def _create_lock(lockfile_name):
        while True:  # wait until the lock file can be opened
            f = os.open(lockfile_name, os.O_CREAT | os.O_EXCL)
            break
        return f
