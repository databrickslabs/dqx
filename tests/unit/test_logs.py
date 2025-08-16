import logging
import os
import tempfile
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import TextIO
import re
from unittest.mock import create_autospec, patch

import pytest

from databricks.labs.dqx.installer.logs import TaskLogger, parse_logs, peak_multi_line_message


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_directory:
        yield Path(temp_directory)


@pytest.fixture
def task_logger(temp_dir, test_logger):
    return test_logger(
        install_dir=temp_dir,
        workflow="test_workflow",
        job_id="123",
        task_name="test_task",
        job_run_id="456",
        log_level="DEBUG",
        attempt="1",
    )


@pytest.fixture
def test_logger():
    class TestLogger(TaskLogger):
        @classmethod
        def create_lock(cls, lockfile_name):
            return cls._create_lock(lockfile_name)

        @classmethod
        def exclusive_open(cls, filename, **kwargs):
            return cls._exclusive_open(filename, **kwargs)

        def init_debug_logfile(self):
            return self._init_debug_logfile()

        def init_run_readme(self):
            return self._init_run_readme()

        def get_log_path(self):
            return self._log_path

        def get_file_handler(self):
            return self._file_handler

        def get_databricks_logger(self):
            return self._databricks_logger

        def get_app_logger(self):
            return self._app_logger

        def set_file_handler(self, file_handler):
            self._file_handler = file_handler

    return TestLogger


def test_task_logger_initialization():
    install_dir = Path("/fake/install/dir")
    workflow = "test_workflow"
    job_id = "123"
    task_name = "test_task"
    job_run_id = "456"
    log_level = "DEBUG"
    attempt = "1"

    task_log = TaskLogger(install_dir, workflow, job_id, task_name, job_run_id, log_level, attempt)

    assert task_log.log_file == install_dir / "logs" / workflow / f"run-{job_run_id}-{attempt}" / f"{task_name}.log"


def test_parse_invalid_logs():
    log_content = "invalid format"
    log_file = create_autospec(TextIO)
    log_file.readline.side_effect = log_content.splitlines(keepends=True) + ['']
    assert not list(parse_logs(log_file))


def test_parse_logs():
    log_content = """12:00:00 INFO [component] {thread} message
12:00:01 ERROR [component] {thread} another message
"""
    log_file = create_autospec(TextIO)
    log_file.readline.side_effect = log_content.splitlines(keepends=True) + ['']
    parsed_logs = list(parse_logs(log_file))

    assert len(parsed_logs) == 2
    assert parsed_logs[0].time.strftime("%H:%M:%S") == "12:00:00"
    assert parsed_logs[0].level == "INFO"
    assert parsed_logs[0].component == "component"
    assert parsed_logs[0].message == "message"
    assert parsed_logs[1].time.strftime("%H:%M:%S") == "12:00:01"
    assert parsed_logs[1].level == "ERROR"
    assert parsed_logs[1].component == "component"
    assert parsed_logs[1].message == "another message"


def test_peak_multi_line_message():
    log_content = """message part 2
12:00:00 INFO [component] {thread} message part 1
"""
    log_file = create_autospec(TextIO)
    log_file.readline.side_effect = log_content.splitlines(keepends=True) + ['']
    pattern = re.compile(r"(\d+:\d+:\d+)\s(\w+)\s\[(.+)\]\s\{\w+\}\s(.+)")

    line, match, multi_line_message = peak_multi_line_message(log_file, pattern)

    assert line == "12:00:00 INFO [component] {thread} message part 1\n"
    assert match is not None
    assert multi_line_message == "\nmessage part 2"


def test_exclusive_open(test_logger, temp_dir):
    test_file = temp_dir / "test_exclusive.txt"

    with test_logger.exclusive_open(str(test_file), mode="w") as f:
        f.write("Test exclusive write")

    with open(test_file, "r", encoding="utf-8") as f:
        contents = f.read()
        assert contents == "Test exclusive write"


def test_create_lock_success(test_logger, temp_dir):
    lockfile = None
    lockfile_name = temp_dir / "test.lock"
    try:
        lockfile = test_logger.create_lock(lockfile_name)
        assert os.path.exists(lockfile_name)
    finally:
        close_lock_file(lockfile, lockfile_name)


def test_create_lock_file_exists_error(test_logger, temp_dir):
    lockfile = None
    lockfile_name = temp_dir / "test.lock"
    try:
        # Create the lock file to simulate the FileExistsError(TimeoutError)
        lockfile = os.open(lockfile_name, os.O_CREAT | os.O_EXCL)
        with pytest.raises(TimeoutError):
            test_logger.create_lock(lockfile_name)
    finally:
        close_lock_file(lockfile, lockfile_name)


def test_init_debug_logfile(task_logger, temp_dir):
    test_file = temp_dir / "test.log"
    task_logger.log_file = test_file
    task_logger.init_debug_logfile()

    assert task_logger.get_file_handler() is not None
    assert task_logger.get_file_handler().level == logging.DEBUG
    assert task_logger.get_file_handler().baseFilename == str(test_file)


def test_log_format():
    log_format = "%(asctime)s %(levelname)s [%(name)s] {%(threadName)s} %(message)s"
    log_formatter = logging.Formatter(fmt=log_format, datefmt="%H:%M:%S")
    record = logging.LogRecord(
        name="test",
        level=logging.DEBUG,
        pathname="test_path",
        lineno=10,
        msg="test message",
        args=(),
        exc_info=None,
    )
    formatted_message = log_formatter.format(record)
    assert re.match(r"\d{2}:\d{2}:\d{2} DEBUG \[test\] \{MainThread\} test message", formatted_message)


def test_init_run_readme_not_exists(task_logger):
    # README doesn't exist (file should be created)
    task_logger.get_log_path().mkdir(parents=True, exist_ok=True)
    readme_path = task_logger.get_log_path() / "README.md"
    task_logger.init_run_readme()
    assert readme_path.exists(), "README.md should be created when it does not exist."
    with open(readme_path, "r", encoding="utf-8") as f:
        contents = f.read()
        assert "# Logs for the DQX test_workflow workflow" in contents
        assert "This folder contains DQX log files." in contents
        assert "See the [test_workflow workflow](/#job/123) and [run #456](/#job/123/run/456)" in contents


def test_init_run_readme_already_exists(task_logger):
    task_logger.get_log_path().mkdir(parents=True, exist_ok=True)
    readme_path = task_logger.get_log_path() / "README.md"
    task_logger.init_run_readme()
    # README already exists (file should not be overwritten)
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write("Existing Content")

    # Call the method again (it should skip writing)
    task_logger.init_run_readme()

    # Check the content hasn't changed
    with open(readme_path, "r", encoding="utf-8") as f:
        updated_contents = f.read()
        assert updated_contents == "Existing Content", "README should not be overwritten if it already exists."


def test_task_logger_enter(task_logger):
    with task_logger:
        assert task_logger.get_log_path().exists()
        assert task_logger.get_file_handler() is not None
        assert task_logger.get_databricks_logger().level == logging.DEBUG
        assert task_logger.get_app_logger().level == logging.DEBUG
        assert task_logger.get_file_handler() in task_logger.get_databricks_logger().handlers


def test_task_logger_repr():
    install_dir = Path("/fake/install/dir")
    workflow = "test_workflow"
    job_id = "123"
    task_name = "test_task"
    job_run_id = "456"
    log_level = "DEBUG"
    attempt = "1"

    task_log = TaskLogger(install_dir, workflow, job_id, task_name, job_run_id, log_level, attempt)
    expected_repr = (install_dir / "logs" / workflow / f"run-{job_run_id}-{attempt}" / f"{task_name}.log").as_posix()

    assert repr(task_log) == expected_repr


def test_task_logger_exit(task_logger):
    file_handler_moc = create_autospec(TimedRotatingFileHandler)
    task_logger.set_file_handler(file_handler_moc)
    with (
        patch.object(task_logger.get_app_logger(), "error") as mock_error,
        patch.object(task_logger.get_databricks_logger(), "debug") as mock_debug,
        patch("os.path.exists", return_value=True),
        patch("os.unlink"),
    ):
        error = Exception("Test error")
        task_logger.__exit__(None, error, None)

        log_file_for_cli = str(task_logger.log_file).removeprefix("/Workspace")
        cli_command = f"databricks workspace export /{log_file_for_cli}"

        mock_error.assert_called_once_with(
            f"Execute `{cli_command}` locally to troubleshoot with more details. {error}"
        )
        mock_debug.assert_called_once_with("Task crash details", exc_info=error)
        task_logger.get_file_handler().flush.assert_called_once()
        task_logger.get_file_handler().close.assert_called_once()


def close_lock_file(lockfile, lockfile_name):
    if lockfile is not None:
        os.close(lockfile)
    if os.path.exists(lockfile_name):
        os.unlink(lockfile_name)
