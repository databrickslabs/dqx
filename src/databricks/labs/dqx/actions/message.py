"""Alert message representation and builder for DQX actions.

This module defines the canonical alert-message structure (*AlertMessage*)
that destination adapters (Slack, Teams, webhook, …) receive and render into
their own wire formats, together with *StandardMessageBuilder* — a stateless
factory that assembles an *AlertMessage* from run-time primitives.

Keeping the builder free of any reference to *ActionContext* (defined in a
later task) prevents circular imports: the evaluator in Task 12 can call
*StandardMessageBuilder.build(...)* using only primitive values already
available at evaluation time.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class AlertMessage:
    """Immutable snapshot of a triggered DQX action alert.

    Instances are created exclusively by *StandardMessageBuilder.build* and
    consumed by destination adapters that render them into Slack blocks,
    Teams cards, webhook payloads, etc.

    Attributes:
        title: Short human-readable headline, always includes the action name.
        summary: Longer human-readable description of what triggered the alert.
        condition: The condition expression that triggered this alert, or *None*
            when the action fires unconditionally.
        table: Fully-qualified table name being checked, or *None* if not
            associated with a specific table.
        observed_metrics: Mapping of metric name to its observed value at alert
            time.
        run_id: Identifier of the DQX run that produced this alert.
        run_time: Timestamp when the DQX run executed.
        severity: Alert severity level (e.g. "error", "warn").
        fields: Flat string-to-string mapping suitable for key-value rendering
            in notification payloads.  Contains one entry per observed metric
            under a key of the form *metric.NAME* (for example, *metric.error_row_count*)
            plus un-prefixed reserved entries for *condition*, *run_id*,
            *run_time*, and *table*.  The *"metric."* prefix ensures metric
            names never silently overwrite the reserved metadata keys.
    """

    title: str
    summary: str
    condition: str | None
    table: str | None
    observed_metrics: dict[str, object]
    run_id: str
    run_time: datetime
    severity: str
    fields: dict[str, str]


class StandardMessageBuilder:
    """Stateless factory that builds an *AlertMessage* from run-time primitives.

    The builder is intentionally decoupled from *ActionContext* to avoid a
    circular import.  Call it as a static method — no constructor arguments
    are needed:

    **python
    msg = StandardMessageBuilder.build(
        action_name="notify_on_errors",
        condition="error_row_count > 0",
        metrics={"error_row_count": 5},
        run_id="run-abc",
        run_time=datetime.now(timezone.utc),
        table="catalog.schema.my_table",
    )
    **
    """

    @staticmethod
    def build(
        *,
        action_name: str,
        condition: str | None,
        metrics: dict[str, object],
        run_id: str,
        run_time: datetime,
        table: str | None,
        severity: str = "error",
    ) -> AlertMessage:
        """Build an *AlertMessage* from run-time primitives.

        Composes a human-readable *title* (always contains *action_name*), a
        *summary* that mentions the *table* and *condition* (or states that the
        action fires unconditionally when *condition* is *None*), and a *fields*
        dict suitable for flat key-value rendering in notification payloads.

        Metric entries in *fields* are stored under a key of the form *metric.NAME*
        (for example, *metric.error_row_count*) so that they never collide with the
        reserved metadata keys *condition*, *run_id*, *run_time*, and *table*,
        which are always un-prefixed.  *observed_metrics* on the returned
        *AlertMessage* is always the raw, un-prefixed metrics dict.

        Args:
            action_name: Logical name of the DQX action that was triggered.
            condition: Condition expression that triggered the action, or *None*
                for unconditional actions.
            metrics: Mapping of metric name to observed value at alert time.
            run_id: Identifier of the DQX run that produced this alert.
            run_time: Timestamp when the DQX run executed.
            table: Fully-qualified table name being checked, or *None*.
            severity: Alert severity level; defaults to "error".

        Returns:
            A frozen *AlertMessage* instance populated from the supplied arguments.
        """
        condition_text = condition if condition is not None else "fired unconditionally"
        table_text = table if table is not None else "unspecified"

        title = f"DQX alert: {action_name}"
        summary = f"Action '{action_name}' triggered on table '{table_text}'. " f"Condition: {condition_text}."

        fields: dict[str, str] = {
            f"metric.{metric_name}": str(metric_value) for metric_name, metric_value in metrics.items()
        }
        fields["condition"] = condition_text
        fields["run_id"] = run_id
        fields["run_time"] = str(run_time)
        fields["table"] = table_text

        return AlertMessage(
            title=title,
            summary=summary,
            condition=condition,
            table=table,
            observed_metrics=metrics,
            run_id=run_id,
            run_time=run_time,
            severity=severity,
            fields=fields,
        )
