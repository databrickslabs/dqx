from pathlib import Path
from typing import TextIO
import re
from unittest.mock import create_autospec
from databricks.labs.dqx.installer.logs import TaskLogger, parse_logs, peak_multi_line_message


def test_task_logger_initialization():
    install_dir = Path("/fake/install/dir")
    workflow = "test_workflow"
    job_id = "123"
    task_name = "test_task"
    job_run_id = "456"
    log_level = "DEBUG"
    attempt = "1"

    task_logger = TaskLogger(install_dir, workflow, job_id, task_name, job_run_id, log_level, attempt)

    assert task_logger.log_file == install_dir / "logs" / workflow / f"run-{job_run_id}-{attempt}" / f"{task_name}.log"


def test_parse_invalie_logs():
    log_content = "invalid format"
    log_file = create_autospec(TextIO, instance=True)
    log_file.readline.side_effect = log_content.splitlines(keepends=True) + ['']
    assert list(parse_logs(log_file)) == []


def test_parse_logs():
    log_content = """12:00:00 INFO [component] {thread} message
12:00:01 ERROR [component] {thread} another message
"""
    log_file = create_autospec(TextIO, instance=True)
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
    log_file = create_autospec(TextIO, instance=True)
    log_file.readline.side_effect = log_content.splitlines(keepends=True) + ['']
    pattern = re.compile(r"(\d+:\d+:\d+)\s(\w+)\s\[(.+)\]\s\{\w+\}\s(.+)")

    line, match, multi_line_message = peak_multi_line_message(log_file, pattern)

    assert line == "12:00:00 INFO [component] {thread} message part 1\n"
    assert match is not None
    assert multi_line_message == "\nmessage part 2"
