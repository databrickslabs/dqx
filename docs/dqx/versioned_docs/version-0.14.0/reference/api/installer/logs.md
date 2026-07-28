---
sidebar_label: logs
title: databricks.labs.dqx.installer.logs
---

## PartialLogRecord Objects

```python
@dataclass
class PartialLogRecord()
```

The information found within a log file record.

#### peak\_multi\_line\_message

```python
def peak_multi_line_message(
        log: TextIO, pattern: re.Pattern) -> tuple[str, re.Match | None, str]
```

A single log record message may span multiple log lines. In this case, the regex on
subsequent lines do not match.

**Arguments**:

- `log` _TextIO_ - The log file IO.
- `pattern` _re.Pattern_ - The regex pattern for a log line.

#### parse\_logs

```python
def parse_logs(log: TextIO) -> Iterator[PartialLogRecord]
```

Parse the logs to retrieve values for PartialLogRecord fields.

**Arguments**:

- `log` _TextIO_ - The log file IO.

