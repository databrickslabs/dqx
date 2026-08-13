# databricks.labs.dqx.actions.log\_sanitize

Shared log-sanitization helper for the DQX actions subsystem.

User-supplied values (action names, conditions, destination names, table identifiers) are embedded in log messages. Newlines and other control characters in those values can forge log entries or corrupt log pipelines (CWE-117), so strip them before logging.

A single implementation lives here so every action module sanitizes identically — an earlier divergence (one module stripped only CR/LF) let control characters slip through on one code path.

### sanitize\_for\_log[​](#sanitize_for_log "Direct link to sanitize_for_log")

```python
def sanitize_for_log(text: str) -> str

```

Replace newlines and control characters in *text* with spaces (CWE-117).

**Arguments**:

* `text` - An arbitrary, possibly user-supplied string.

**Returns**:

The string with CR, LF, tab, and other C0/DEL control characters replaced by spaces.
