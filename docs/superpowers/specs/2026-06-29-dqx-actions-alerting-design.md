# DQX Actions & Alerting — Design Spec

**Date:** 2026-06-29
**Status:** Approved for implementation (design authority delegated by requester)
**Source PRD:** [PRD] DQX Data Quality Alerts (Jan 2026)
**Scope:** All **P0** and **P1** requirements.

---

## 1. Summary

Introduce an **action abstraction** to DQX. An action runs when data checked by DQX
violates a user-defined condition evaluated against the **summary metrics** produced by
`DQMetricsObserver`. The first two concrete actions are:

- **`DQAlert`** — sends a notification to one or more destinations (Slack, Microsoft
  Teams, generic webhook, Databricks SQL Alert).
- **`FailPipeline`** — raises an exception to stop the current pipeline.

Actions are passed to `DQEngine` and fire automatically on save-to-table methods (the
save is the triggering Spark action that populates the observation), or explicitly via
`engine.evaluate_actions(...)`. Action definitions can be stored/loaded from UC or
Lakebase tables, and every fired action is recorded to a UC/Lakebase **events table**
that also seeds in-memory state for frequency / status-change control.

### Decisions locked with the requester

- **Destinations built now:** Slack + Teams + generic Webhook + **Databricks SQL Alert** + `FailPipeline`. The `AlertDestination` interface stays open for PagerDuty/DQM/email later.
- **State tracking:** in-memory dedup/frequency state for the `DQEngine` lifetime, **seeded from and persisted to** a configurable UC/Lakebase events table.

### Open questions — resolved per PRD "PREFERRED" options

| Question | Resolution |
|---|---|
| Limit destinations to Databricks-managed? | No — user-supplied destination config (PRD Option 2). |
| How to track alert events for frequency? | Configurable UC/Lakebase events table + in-memory state (PRD Option 1). |
| How are conditions computed? | Python expressions over the observed-metrics dict, evaluated with a **safe AST walker** (never `eval()`) (PRD Option 2). |
| How are messages defined? | DQX standard message (condition, eval time, metrics, table, run id) (PRD Option 1). Custom templates deferred (P2). |
| Tie to UC integration? | Keep destinations extensible; do not couple now. |

---

## 2. Goals / Non-Goals

**Goals (P0/P1)**
- `DQAction` = (condition, inner `Action`); inner actions `DQAlert` and `FailPipeline`.
- `DQAlert` with ≥1 destination and a condition; Slack, Teams, webhook, DBSQL.
- Conditions over built-in **and** custom observer metrics.
- Frequency control (`ALWAYS`/`HOURLY`/`DAILY`) and notify-on-status-change (HEALTHY↔UNHEALTHY), backed by state.
- Standard message with table name, condition, observed metrics, run time/id.
- Save/load actions to UC + Lakebase tables (`DQActionManager`).
- Batch **and** streaming support.
- Validation on instantiation; retry with exponential backoff; isolated per-destination failures; raise after N retries (warning).
- Callback-function destinations (P1 "triggered by callback functions").

**Non-Goals (P2 / out of scope)**
- Email destination, DQM health-indicator destination, PagerDuty (interface left open).
- Custom user-defined message templates.
- Catalog/schema/table-scoped alert filters.

---

## 3. Module layout

New package `src/databricks/labs/dqx/actions/`:

```
action/
  __init__.py            # public exports
  base.py                # Action (ABC), DQAction, ActionContext, ActionResult, ActionStatus
  conditions.py          # ConditionEvaluator — safe AST evaluation over metrics dict
  message.py             # AlertMessage, StandardMessageBuilder
  alert.py               # DQAlert, DQAlertFrequency, NotifyOn
  fail_pipeline.py       # FailPipeline
  secrets.py             # SecretResolver (WorkspaceClient-backed) + DQSecret resolution
  delivery.py            # WebhookClient (urllib POST + retry/backoff), webhook URL SSRF validation
  destinations/
    __init__.py
    base.py              # AlertDestination (ABC)
    webhook_base.py      # WebhookAlertDestination (template method _build_payload)
    slack.py             # SlackDQAlertDestination
    teams.py             # TeamsDQAlertDestination
    webhook.py           # WebhookDQAlertDestination (generic)
    dbsql.py             # DBSQLAlertDestination (Databricks SQL Alert via WorkspaceClient)
    callback.py          # CallbackDQAlertDestination (P1 callback function)
  state.py               # ActionStateStore, AlertEvent, ActionEventStore (ABC)
  event_storage.py       # TableActionEventStore, LakebaseActionEventStore + factory
  evaluator.py           # ActionEvaluator — orchestrates condition eval + dispatch
  serializer.py          # ActionSerializer — action <-> dict round-trip (type registry)
  manager.py             # DQActionManager — save/load action definitions
  definition_storage.py  # ActionsStorageHandler (ABC), Table/Lakebase handlers + factory
```

Config dataclasses (`DQSecret`, `TableActionsStorageConfig`, `LakebaseActionsStorageConfig`,
`ActionEventsConfig`) are added to existing `config.py`. New exceptions are added to `errors.py`.

**SOLID notes**
- *SRP*: condition eval, message building, delivery, state, storage, orchestration are each isolated units behind interfaces.
- *OCP*: new destinations/actions register in the serializer's type registry without touching the evaluator.
- *LSP*: every `AlertDestination` honors `deliver(message, context, resolver)`; every `Action` honors `execute(context, services)`.
- *DIP*: `ActionEvaluator` depends on the `Action`/`AlertDestination`/`ActionEventStore`/`SecretResolver` abstractions, all injected (constructor) — enables unit tests with `create_autospec`.

---

## 4. Core abstractions

### 4.1 `ActionContext` (base.py, frozen dataclass)
Immutable snapshot passed to every action evaluation:
`metrics: dict[str, Any]`, `run_id`, `run_time: datetime`, `run_name`,
`input_location`, `output_location`, `quarantine_location`, `checks_location`,
`rule_set_fingerprint`, `user_metadata`. Built from `DQMetricsObservation` + run metadata.

### 4.2 `Action` (base.py, ABC)
```python
class Action(abc.ABC):
    name: str
    def validate(self) -> None: ...           # default no-op; raise InvalidActionError on bad config
    @abc.abstractmethod
    def execute(self, context: ActionContext, services: ActionServices) -> ActionResult: ...
```
`ActionServices` bundles injected collaborators (`SecretResolver`, `WebhookClient`,
`WorkspaceClient`, `SparkSession`) so concrete actions stay testable and free of hidden state.

**Extensibility (OCP) — adding a new action takes 2 steps, no core edits:**
1. Subclass `Action`, implement `execute(context, services) -> ActionResult` (read metrics
   from `context.metrics`; that is the single input surface every action shares).
2. Register its `type` string in the serializer registry (§9) for table round-trip.
`ActionEvaluator`, `DQEngine`, `ConditionEvaluator`, `StandardMessageBuilder`, and storage
are never modified to add an action. The evaluator (§10) dispatches every action
polymorphically via `execute(...)` — it contains **no `isinstance` branching on action
type**. An action that must abort the run raises `TerminalActionError` (base class);
`PipelineFailedError` subclasses it, and the evaluator defers all such errors until every
other action has run, then re-raises the first. This keeps "stop the run after the others
finish" a generic, reusable behavior rather than a `FailPipeline` special case.

### 4.3 `DQAction` (base.py, dataclass)
```python
@dataclass
class DQAction:
    action: Action
    condition: str | None = None      # optional; None => always fire after checks
    name: str = ""
    def __post_init__(self):
        if self.condition is not None:
            ConditionEvaluator.validate(self.condition)   # P1 validate-on-instantiation
        self.action.validate()
        if not self.name: self.name = self.action.name or <derived from condition or action type>
```
A `None` condition means the action fires unconditionally whenever actions are evaluated
(after checks are applied) — no metric gating. Field order keeps `action` required and
`condition` optional; callers use keywords per the PRD (`DQAction(condition=..., action=...)`).

### 4.4 `FailPipeline(Action)` (fail_pipeline.py)
`execute` raises `PipelineFailedError(message, context)` (a `TerminalActionError`). Default
message includes the condition and observed metrics. Because the evaluator defers
`TerminalActionError` until all other actions have executed, alerts are always delivered
before the pipeline aborts — without the evaluator knowing `FailPipeline` exists.

---

## 5. Condition evaluation (conditions.py)

`ConditionEvaluator` parses the condition string with `ast.parse(expr, mode="eval")` and
evaluates by walking a **strict allowlist** of node types:

- `BoolOp` (`and`/`or`), `UnaryOp` (`not`, unary `-`)
- `Compare` (`< <= > >= == !=`, chained)
- `BinOp` (`+ - * / // % **`)
- `Name` → resolved from `context.metrics` (missing name → `InvalidConditionError`)
- `Constant` (int/float/bool/str), `IfExp` (optional)

Any other node (calls, attribute access, comprehensions, subscripts, lambdas) → raise
`InvalidConditionError`. This is the PRD-mandated **safe subset** — `eval()`/`exec()` are
never used (satisfies the security requirement against arbitrary code execution).

`validate(condition)` compiles + structurally checks without metrics (used in
`__post_init__`). `evaluate(condition, metrics) -> bool` runs against real metrics.
Metric values arriving from the observation as strings are coerced to numbers when numeric.

---

## 6. Messages (message.py)

`AlertMessage` (frozen): `title`, `summary`, `condition`, `table` (input location),
`observed_metrics: dict`, `run_id`, `run_time`, `severity`, `fields: dict`.

`StandardMessageBuilder.build(action_name, context) -> AlertMessage` produces the standard
message required by P0 (table name, condition, observed metrics, run time/id). Destinations
render `AlertMessage` into their own wire format.

---

## 7. Destinations

### 7.1 `AlertDestination` (destinations/base.py, ABC)
```python
class AlertDestination(abc.ABC):
    name: str
    def validate(self) -> None: ...
    @abc.abstractmethod
    def deliver(self, message: AlertMessage, context: ActionContext, services: ActionServices) -> None: ...
```

### 7.2 Webhook-based (Slack/Teams/Webhook) — Template Method
`WebhookAlertDestination` resolves the (possibly `DQSecret`) webhook URL via the
`SecretResolver`, builds the payload through the abstract `_build_payload(message)`, and
POSTs via `WebhookClient`. Subclasses implement only payload shape:
- **Slack**: Block Kit JSON (`blocks` with section fields).
- **Teams**: MessageCard / Adaptive Card JSON.
- **Webhook (generic)**: DQX canonical JSON (message dict). Optional basic auth via
  `username`/`password` `DQSecret`s.

### 7.3 `DBSQLAlertDestination` (destinations/dbsql.py)
Delivers via the Databricks SQL Alerts API on the `WorkspaceClient`
(`services.ws.alerts`). On `deliver`, **idempotently** creates-or-updates a managed SQL
alert (named after the action) whose query runs against the configured metrics/checked
table with the action's condition, attaches the configured workspace
`notification_destination_ids`, and triggers an evaluation run. Requires `warehouse_id`
and a `query` (or metrics table reference). Fully unit-tested with a mocked
`WorkspaceClient`; integration test gated on a real workspace + warehouse.

### 7.4 `CallbackDQAlertDestination` (destinations/callback.py) — P1
Wraps a user `Callable[[AlertMessage, ActionContext], None]`. Not serializable (omitted
from table round-trip with a logged warning), in-process only.

### 7.5 Delivery, retry, SSRF (delivery.py)
`WebhookClient.post(url, payload, *, auth=None)`:
- stdlib `urllib.request` with a no-redirect opener (no new dependency).
- **Retry** with exponential backoff: `max_retries`, `base_delay`, capped; injected
  `sleeper: Callable[[float], None]` for deterministic tests. Raises `AlertDeliveryError`
  after the final attempt (P1: fail after N retries + warn).
- **SSRF guard** `validate_webhook_url(url, allowed_hosts)`: require `https`; block
  `localhost`, loopback, RFC1918/link-local/ULA IPs, and the cloud metadata IP
  `169.254.169.254`; Slack/Teams enforce known host suffixes. Violations raise
  `UnsafeWebhookUrlError` (satisfies CWE-918).
- Secrets are resolved at the call boundary and **never logged**.

---

## 8. State & event tracking

### 8.1 `AlertEvent` (state.py, frozen)
`action_name`, `condition`, `fired: bool`, `status: ActionStatus` (HEALTHY/UNHEALTHY),
`observed_metrics: dict`, `run_id`, `run_time`, `input_location`, `destinations: list[str]`,
`delivery_errors: list[str]`.

### 8.2 `ActionStateStore` (state.py)
- In-memory map `action_name -> (last_fired_time, last_status)`.
- `seed(events)`: load latest event per action on engine init.
- `should_fire(dq_action, context, condition_result) -> bool`:
  - `DQAlertFrequency.ALWAYS` → fire whenever condition true.
  - `HOURLY`/`DAILY` → suppress if last fire within the window.
  - `NotifyOn.STATUS_CHANGE` → fire only on HEALTHY→UNHEALTHY (and recovery) transitions.
- `record(event)`: update memory + append to the `ActionEventStore`.

### 8.3 `ActionEventStore` (event_storage.py, ABC) + impls
- `TableActionEventStore`: append events to a UC Delta table (`save_dataframe_as_table`),
  load latest-per-action via window query.
- `LakebaseActionEventStore`: mirrors the existing `LakebaseChecksStorageHandler`
  (SQLAlchemy engine, schema bootstrap).
- Factory selects by `ActionEventsConfig` (table vs Lakebase). Configured via
  `metrics_config`-style `ActionEventsConfig` or defaulting to the run config's metrics table.

---

## 9. Action-definition storage (manager.py, definition_storage.py, serializer.py)

`ActionSerializer` round-trips actions to the PRD YAML/dict shape using a **type registry**
(`alert`, `fail_pipeline`; destination types `slack`/`teams`/`webhook`/`dbsql`). `DQSecret`
serializes to `"scope/key"`. Callback destinations are skipped (warned). New action/
destination types register without modifying the serializer (OCP).

`DQActionManager(ws, spark)`:
- `save_actions(actions, config)` and `load_actions(config) -> list[DQAction]`.
- `TableActionsStorageConfig` (UC) and `LakebaseActionsStorageConfig` mirror the existing
  checks-storage handler/factory pattern (ABC `ActionsStorageHandler[T]` + factory).

---

## 10. Orchestration (evaluator.py)

`ActionEvaluator(actions, *, state_store, services, message_builder)`:
```
evaluate(context) -> list[ActionResult]:
  results, deferred = [], []
  for dq in actions:
     # condition None => fire unconditionally; else gate on the metric expression
     if dq.condition is not None and not ConditionEvaluator.evaluate(dq.condition, context.metrics):
        record(not-fired); continue
     if not state_store.should_fire(dq, context, True): record(suppressed); continue
     try:
        result = dq.action.execute(context, services)   # polymorphic — NO isinstance
        results.append(result); state_store.record(event_from(result, dq, context))
     except TerminalActionError as err:
        deferred.append(err)                              # e.g. FailPipeline
  if deferred: raise deferred[0]                          # after every other action ran
  return results
```
- The evaluator is **closed for modification**: it never names a concrete action type.
  Non-terminal actions return an `ActionResult`; terminal actions raise
  `TerminalActionError` and are deferred so notifications go out before the run aborts.
- A `DQAlert` dispatches its destinations concurrently with
  `blueprint.parallel.Threads.gather` inside its own `execute` → **isolated failures**
  (one destination error never blocks others; P1), captured in `ActionResult.destination_errors`.
- Errors are logged sanitized (no secrets/newlines per CWE-117) and recorded on the event.

---

## 11. Engine integration

- `DQEngine.__init__(..., actions: list[DQAction] | None = None)` and the same on
  `DQEngineCore`. Engine builds an `ActionEvaluator` lazily (requires an `observer`).
- **Validation**: actions provided without an `observer` → `InvalidParameterError`
  (alerts need summary metrics; matches PRD).
- **Batch save methods** (`apply_checks_and_save_in_table` /
  `apply_checks_by_metadata_and_save_in_table`): after `batch_observation.get` is
  available (where `save_summary_metrics` runs today), call the evaluator with an
  `ActionContext` built from the observed metrics + run metadata.
- **Streaming**: `StreamingMetricsListener` gains an optional evaluator callback invoked
  per micro-batch in `onQueryProgress`, using that batch's `observedMetrics`.
- **Explicit path**: public `DQEngine.evaluate_actions(observed_metrics, *, input_location=None, ...)`
  for non-save flows (`dq, obs = engine.apply_checks(...); dq.count(); engine.evaluate_actions(obs.get)`).
- `@telemetry_logger("engine", "evaluate_actions")` on the new public method.

### RunConfig extension
Add `actions_location: str | None = None` to `RunConfig` (PRD: `actions: location: ...`).
Workflows/CLI load actions via `DQActionManager` from this location when present.

---

## 12. Errors (errors.py additions)
`TerminalActionError` (base for run-aborting actions), `PipelineFailedError(TerminalActionError)`,
`InvalidConditionError`, `InvalidActionError`, `AlertDeliveryError`, `UnsafeWebhookUrlError`.
All extend the existing DQX error base.

---

## 13. Security (mandatory per AGENTS.md)
- **No `eval()`** — safe AST walker only (§5).
- **SSRF** — `validate_webhook_url` allowlist + private-range blocking (§7.5).
- **Secrets** — `DQSecret(scope, key)` resolved via WorkspaceClient secrets at call time;
  redacted at construction; never logged. DBSQL uses workspace destinations.
- **Log injection (CWE-117)** — sanitize action/destination/table names and delivery
  errors (strip newlines/control chars) before logging.
- **Untrusted parse** — YAML/JSON action loading handles parse failures gracefully without
  leaking internals.
- **DBSQL** — only operates on the authenticated workspace's own alert objects.

---

## 14. Testing

**Unit (`tests/unit/`, no Spark/workspace):**
- `ConditionEvaluator`: allowed ops, rejected nodes (calls/attrs/subscripts), missing
  metrics, numeric coercion, boolean logic, malformed expressions.
- `StandardMessageBuilder`: message content/fields.
- Destinations: payload shape per type (Slack/Teams/webhook) with a fake `WebhookClient`;
  DBSQL with `create_autospec(WorkspaceClient)`; callback invocation.
- `WebhookClient`: retry/backoff counts (injected sleeper), failure after N, SSRF rejects
  (loopback, RFC1918, metadata IP, http scheme), basic-auth header.
- `ActionStateStore`: ALWAYS/HOURLY/DAILY suppression, status-change transitions, seeding.
- `ActionSerializer`: round-trip every type incl. `DQSecret`; callback skip+warn; bad type.
- `ActionEvaluator`: condition gating, suppression, isolated destination failure,
  `FailPipeline` raises after alerts, event recording (mocked store).
- `DQAction`/`DQAlert`/`FailPipeline` validation on instantiation.
- Config dataclasses (`DQSecret`, storage configs) validation.

**Integration (`tests/integration/`, live workspace + Spark):**
- `DQActionManager` save/load round-trip on a UC table (pytester `factory` cleanup).
- `TableActionEventStore` append + latest-per-action read.
- Engine: `apply_checks_and_save_in_table` with a `FailPipeline` action raises;
  with a webhook destination pointed at a local capture (or asserts delivery attempt
  via injected client) fires once; metrics-driven condition true/false.
- Streaming: listener fires evaluator per micro-batch.
- Lakebase handlers gated behind the existing Lakebase env-skip fixtures.

**Minimums honored:** every new function/class has ≥1 positive + ≥1 negative unit test.

---

## 15. Docs
- New page `docs/docs/guide/quality_checks_apply.md` section or a dedicated
  `docs/docs/guide/actions_and_alerts.md`: defining actions, destinations (Slack/Teams/
  webhook/DBSQL), conditions, frequency/status-change, storing/loading actions, engine
  usage (auto vs `evaluate_actions`), streaming, `FailPipeline`, security/secrets notes.
- Update README feature list and reference/API docs (`__all__` exports).
- CHANGELOG entry.

---

## 16. Phasing (for the implementation plan)
1. Errors + config dataclasses (`DQSecret`, storage/events configs).
2. `conditions.py` + `message.py` (+ tests).
3. `secrets.py`, `delivery.py` (SSRF + retry) (+ tests).
4. Destinations: base, webhook base, Slack/Teams/webhook, DBSQL, callback (+ tests).
5. `base.py` (`Action`, `DQAction`, `ActionContext`, `ActionServices`), `alert.py`,
   `fail_pipeline.py` (+ tests).
6. `state.py` + `event_storage.py` (UC + Lakebase) (+ tests).
7. `serializer.py`, `definition_storage.py`, `manager.py` (+ tests).
8. `evaluator.py` (+ tests).
9. Engine + `DQEngineCore` integration, streaming listener callback, `RunConfig`
   (+ integration tests).
10. Docs, `__all__`/exports, CHANGELOG.
