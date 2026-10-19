"""TEST-ONLY: the wheel entry point a coverage-enabled runner job runs instead of the real one.

The ``dev-coverage`` bundle target points ``python_wheel_task`` at this entry point. It calls the
pristine runner and then persists coverage from ordinary code, which is what makes delivery
deterministic:

* ``sys.argv`` carries the task arguments by the time this runs, so the results volume — and with it
  the upload destination — resolves. The ``.pth`` runs at interpreter start, long before that, which
  is why a timer-driven flush could only ever leave the data in /tmp and hope for a later tick.
* The flush is a normal call on ``main()``'s stack, so it does not depend on ``atexit`` firing or on
  the process outliving a checkpoint interval.

The tracer itself is still started by the ``.pth`` (see the package docstring) — that has to happen
before ``dqx_mcp_runner.runner`` is imported for its module-level lines to be measured, and an entry
point cannot be early enough for that. Production deploys contain neither this module nor the
``.pth``: the whole wheel is test-only, and only ``dev-coverage`` names this entry point.
"""

from . import flush_at_task_end


def main() -> None:
    """Run the real runner, then persist coverage on every path."""
    try:
        from dqx_mcp_runner.runner import main as runner_main

        runner_main()
    finally:
        # A failed operation's coverage is worth as much as a passing one's, and the job's own
        # result_state is unaffected: the exception continues to propagate.
        flush_at_task_end()
