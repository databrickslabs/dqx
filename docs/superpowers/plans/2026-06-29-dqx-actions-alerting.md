# DQX Actions & Alerting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an extensible action abstraction to DQX whose first actions are `DQAlert` (Slack/Teams/webhook/DBSQL notifications) and `FailPipeline`, triggered by conditions over `DQMetricsObserver` summary metrics.

**Architecture:** A new `databricks.labs.dqx.actions` package built on SOLID interfaces — `Action`/`AlertDestination`/`ActionEventStore`/`SecretResolver` abstractions, a safe AST condition evaluator (never `eval()`), webhook delivery with retry+SSRF guards, in-memory state seeded from a UC/Lakebase events table, and an `ActionEvaluator` orchestrator. `DQEngine` accepts `actions=[...]` and fires them on save-to-table methods (batch + streaming) or via `evaluate_actions(...)`.

**Tech Stack:** Python 3.10+, PySpark, databricks-sdk (~0.73), databricks-labs-blueprint (Threads), SQLAlchemy (Lakebase), PyYAML, stdlib `ast`/`urllib.request`. No new dependencies.

## Global Constraints

- Every parameter and return value annotated; `list[str]`/`str | None` generics; no `Any` outside `anomaly/`.
- Never `eval()`/`exec()`; condition eval via safe AST walker only.
- Validate every webhook URL for SSRF before any network call; raise `UnsafeWebhookUrlError`.
- Secrets via `DQSecret(scope, key)` resolved at call time; never logged. Sanitize newlines/control chars in any user-supplied value before logging (CWE-117).
- Decorate new public `DQEngine` methods with `@telemetry_logger`.
- Serialize configs via `ConfigSerializer`, not `dataclasses.asdict()`.
- Use `blueprint.parallel.Threads.gather` for concurrent dispatch, not `concurrent.futures`.
- Frozen dataclasses updated via `dataclasses.replace`.
- Unit tests: no Spark/workspace, use `create_autospec`. Real Spark/workspace ⇒ integration test under `tests/integration/`.
- `make fmt` before every commit; `make lint` must pass.
- GPG-sign commits; end messages with `Co-authored-by: Isaac`.

---

## File Structure

**New package `src/databricks/labs/dqx/actions/`:**
- `__init__.py` — public exports
- `base.py` — `ActionStatus`, `ActionContext`, `ActionResult`, `ActionServices`, `Action` (ABC), `DQAction`
- `conditions.py` — `ConditionEvaluator`
- `message.py` — `AlertMessage`, `StandardMessageBuilder`
- `secrets.py` — `SecretResolver`
- `delivery.py` — `WebhookClient`, `validate_webhook_url`, `WebhookAuth`
- `destinations/__init__.py`, `base.py`, `webhook_base.py`, `slack.py`, `teams.py`, `webhook.py`, `dbsql.py`, `callback.py`
- `alert.py` — `DQAlert`, `DQAlertFrequency`, `NotifyOn`
- `fail_pipeline.py` — `FailPipeline`
- `state.py` — `AlertEvent`, `ActionStateStore`, `ActionEventStore` (ABC)
- `event_storage.py` — `TableActionEventStore`, `LakebaseActionEventStore`, `ActionEventStoreFactory`
- `serializer.py` — `ActionSerializer`
- `definition_storage.py` — `ActionsStorageHandler` (ABC), `TableActionsStorageHandler`, `LakebaseActionsStorageHandler`, `ActionsStorageHandlerFactory`
- `manager.py` — `DQActionManager`
- `evaluator.py` — `ActionEvaluator`

**Modified:**
- `src/databricks/labs/dqx/errors.py` — new exceptions
- `src/databricks/labs/dqx/config.py` — `DQSecret`, `TableActionsStorageConfig`, `LakebaseActionsStorageConfig`, `ActionEventsConfig`; `RunConfig.actions_location`
- `src/databricks/labs/dqx/engine.py` — `actions` param, evaluator wiring, `evaluate_actions`
- `src/databricks/labs/dqx/metrics_listener.py` — optional evaluator callback

**Docs:** `docs/docs/guide/actions_and_alerts.md`, README, CHANGELOG.

---

## Task 1: Errors and config dataclasses

**Files:**
- Modify: `src/databricks/labs/dqx/errors.py`
- Modify: `src/databricks/labs/dqx/config.py`
- Test: `tests/unit/test_action_config.py`

**Interfaces produced:**
- `DQSecret(scope: str, key: str)` frozen; `as_reference() -> str` → `"scope/key"`; `DQSecret.from_reference(ref: str) -> DQSecret`.
- Exceptions (extend existing DQX base in errors.py): `PipelineFailedError`, `InvalidConditionError`, `InvalidActionError`, `AlertDeliveryError`, `UnsafeWebhookUrlError`.
- `TableActionsStorageConfig(location: str, run_config_name: str = "default", mode: str = "append")`.
- `LakebaseActionsStorageConfig(location, instance_name, client_id=None, port="5432", run_config_name="default", mode="append")` (mirror `LakebaseChecksStorageConfig` validation + `database_name`/`schema_name`/`table_name`).
- `ActionEventsConfig(location: str, mode: str = "append")` — UC table; plus optional Lakebase fields mirroring above.
- `RunConfig.actions_location: str | None = None`.

- [ ] **Step 1:** Write `tests/unit/test_action_config.py`: `DQSecret.as_reference()=="s/k"`, `from_reference("s/k")` round-trips, `from_reference("bad")` raises `InvalidParameterError`; `TableActionsStorageConfig("")` raises `InvalidConfigError`; `LakebaseActionsStorageConfig` rejects non-3-part location and empty instance; `RunConfig().actions_location is None`.
- [ ] **Step 2:** Run `pytest tests/unit/test_action_config.py -v` → FAIL (imports missing).
- [ ] **Step 3:** Add exceptions to `errors.py` (match existing base-class pattern there). Add `DQSecret` + the storage/events configs to `config.py`; add `actions_location` to `RunConfig`; export new names in `config.py` `__all__`.
- [ ] **Step 4:** Run the test → PASS. `make fmt && make lint`.
- [ ] **Step 5:** Commit `feat(actions): add action errors and config dataclasses`.

---

## Task 2: Safe condition evaluator

**Files:**
- Create: `src/databricks/labs/dqx/actions/__init__.py` (empty for now)
- Create: `src/databricks/labs/dqx/actions/conditions.py`
- Test: `tests/unit/test_action_conditions.py`

**Interfaces produced:**
- `ConditionEvaluator.validate(condition: str) -> None` (raises `InvalidConditionError` on disallowed nodes / syntax).
- `ConditionEvaluator.evaluate(condition: str, metrics: dict[str, object]) -> bool` (raises `InvalidConditionError` on unknown name).

**Design:** parse `ast.parse(condition, mode="eval")`; recursively evaluate allowing only `Expression, BoolOp(And|Or), UnaryOp(Not|USub|UAdd), BinOp(+ - * / // % **), Compare(< <= > >= == !=), Name, Constant(int|float|bool|str)`. Names resolve from `metrics`; numeric strings coerced via `float`. Any other node ⇒ `InvalidConditionError`. Result cast to `bool`.

- [ ] **Step 1:** Tests: `evaluate("error_row_count > 0", {"error_row_count": 5}) is True`; `False` when 0; `"a > 1 or b > 1"` boolean logic; `"error_row_ratio > 0.10"` with float; numeric-string coercion (`{"x": "5"}`); unknown name raises; `"__import__('os')"`/`"len(x)"`/`"x.y"`/`"x[0]"` each raise `InvalidConditionError`; `validate("1 +")` raises.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement `conditions.py`.
- [ ] **Step 4:** Run → PASS. `make fmt && make lint`.
- [ ] **Step 5:** Commit `feat(actions): add safe AST condition evaluator`.

---

## Task 3: Alert message + builder

**Files:**
- Create: `src/databricks/labs/dqx/actions/message.py`
- Test: `tests/unit/test_action_message.py`

**Interfaces consumed:** `ActionContext` is defined in Task 5; to avoid a cycle, `StandardMessageBuilder.build` takes primitives, not `ActionContext`:
- `AlertMessage` frozen: `title, summary, condition, table: str | None, observed_metrics: dict, run_id: str, run_time: datetime, severity: str, fields: dict[str, str]`.
- `StandardMessageBuilder.build(*, action_name: str, condition: str, metrics: dict, run_id: str, run_time: datetime, table: str | None, severity: str = "error") -> AlertMessage`.

- [ ] **Step 1:** Test build() includes action_name in title, condition, table, run_id, run_time, and a `fields` entry per metric.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** PASS. fmt+lint.
- [ ] **Step 5:** Commit `feat(actions): add alert message builder`.

---

## Task 4: Secret resolver

**Files:**
- Create: `src/databricks/labs/dqx/actions/secrets.py`
- Test: `tests/unit/test_action_secrets.py`

**Interfaces produced:**
- `SecretResolver(ws: WorkspaceClient)`; `resolve(value: str | DQSecret) -> str` — plain string returned as-is; `DQSecret` resolved via `ws.dbutils.secrets.get(scope, key)`. Errors wrapped in `InvalidParameterError`. The resolved value is never logged.

- [ ] **Step 1:** Test with `create_autospec(WorkspaceClient)`: `resolve("plain")=="plain"`; `resolve(DQSecret("s","k"))` calls `ws.dbutils.secrets.get("s","k")` and returns its value.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** PASS. fmt+lint.
- [ ] **Step 5:** Commit `feat(actions): add secret resolver`.

---

## Task 5: Action core types

**Files:**
- Create: `src/databricks/labs/dqx/actions/base.py`
- Test: `tests/unit/test_action_base.py`

**Interfaces consumed:** `ConditionEvaluator` (Task 2), `SecretResolver` (Task 4).
**Interfaces produced:**
- `class ActionStatus(Enum): HEALTHY="healthy"; UNHEALTHY="unhealthy"`.
- `ActionContext` frozen: `metrics: dict, run_id: str, run_time: datetime, run_name: str = "dqx", input_location: str|None=None, output_location: str|None=None, quarantine_location: str|None=None, checks_location: str|None=None, rule_set_fingerprint: str|None=None, user_metadata: dict[str,str]|None=None`.
- `ActionResult` frozen: `action_name: str, fired: bool, status: ActionStatus, destination_errors: dict[str, str] = {}`.
- `ActionServices` frozen: `secret_resolver: SecretResolver, webhook_client: WebhookClient, ws: WorkspaceClient | None = None, spark: SparkSession | None = None`. (Type-only import of `WebhookClient` via `TYPE_CHECKING` to avoid cycle.)
- `class Action(abc.ABC)`: attr `name: str`; `validate(self) -> None` default no-op; abstract `execute(self, context: ActionContext, services: ActionServices) -> ActionResult`.
- `@dataclass DQAction`: `action: Action; condition: str | None = None; name: str = ""`. `__post_init__`: if `condition is not None` ⇒ `ConditionEvaluator.validate(condition)`; `action.validate()`; derive `name` from `action.name` (or condition hash / action type) if empty. A `None` condition means the action fires unconditionally after checks. Field order puts required `action` first; callers use keywords (`DQAction(condition=..., action=...)`).

- [ ] **Step 1:** Tests: constructing `DQAction(condition="error_row_count > 0", action=DummyAction())` works and sets `name`; `DQAction(action=DummyAction())` (no condition) is valid with `condition is None`; bad condition raises `InvalidConditionError`; an action whose `validate` raises propagates `InvalidActionError`. Use a local `DummyAction(Action)` test double.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement `base.py`.
- [ ] **Step 4:** PASS. fmt+lint.
- [ ] **Step 5:** Commit `feat(actions): add Action core types and DQAction`.

---

## Task 6: Webhook delivery + SSRF guard

**Files:**
- Create: `src/databricks/labs/dqx/actions/delivery.py`
- Test: `tests/unit/test_action_delivery.py`

**Interfaces produced:**
- `WebhookAuth` frozen: `username: str, password: str` → `header() -> dict[str,str]` (Basic).
- `validate_webhook_url(url: str, allowed_host_suffixes: list[str] | None = None) -> None` — require `https`; reject loopback/`localhost`, RFC1918 (10/8,172.16/12,192.168/16), link-local 169.254/16 (incl. metadata 169.254.169.254), ULA fc00::/7, `::1`; if `allowed_host_suffixes` given, host must end with one. Raise `UnsafeWebhookUrlError`.
- `WebhookClient(*, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 30.0, timeout: float = 30.0, sleeper: Callable[[float], None] = time.sleep, opener=<no-redirect>)`; `post(url, payload: dict, *, auth: WebhookAuth | None = None, allowed_host_suffixes=None) -> None` — validates URL, POSTs JSON, retries with exponential backoff (`base_delay * 2**attempt`, capped), raises `AlertDeliveryError` after final failure.

- [ ] **Step 1:** Tests: `validate_webhook_url` rejects `http://...`, `https://localhost`, `https://10.0.0.1`, `https://169.254.169.254`, accepts `https://hooks.slack.com/x`; with suffix allowlist rejects wrong host. `WebhookClient.post` success path (inject fake opener capturing request); retries N times then raises `AlertDeliveryError` (assert `sleeper` called N times with growing delays); basic-auth header present when `auth` given.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement using `urllib.request.Request` + an injected opener (default built via `urllib.request.build_opener` with a `HTTPRedirectHandler` subclass that blocks redirects). No secrets in log lines; sanitize URL host only.
- [ ] **Step 4:** PASS. fmt+lint.
- [ ] **Step 5:** Commit `feat(actions): add webhook delivery client with retry and SSRF guard`.

---

## Task 7: Destination base + webhook destinations (Slack/Teams/generic)

**Files:**
- Create: `src/databricks/labs/dqx/actions/destinations/__init__.py`, `base.py`, `webhook_base.py`, `slack.py`, `teams.py`, `webhook.py`
- Test: `tests/unit/test_action_destinations_webhook.py`

**Interfaces consumed:** `AlertMessage` (T3), `ActionContext`/`ActionServices` (T5), `WebhookClient`/`WebhookAuth` (T6), `SecretResolver` (T4), `DQSecret` (T1).
**Interfaces produced:**
- `class AlertDestination(abc.ABC)`: attr `name: str`, `type: ClassVar[str]`; `validate(self) -> None` default no-op; abstract `deliver(self, message, context, services) -> None`.
- `@dataclass WebhookAlertDestination(AlertDestination)` (abstract): `name: str; webhook_url: str | DQSecret`; `allowed_host_suffixes: ClassVar[list[str] | None] = None`; abstract `_build_payload(message) -> dict`; concrete `deliver` resolves URL via `services.secret_resolver`, calls `services.webhook_client.post(url, self._build_payload(message), allowed_host_suffixes=self.allowed_host_suffixes)`.
- `SlackDQAlertDestination(WebhookAlertDestination)` type `"slack"`, `allowed_host_suffixes=["hooks.slack.com"]`, Block Kit payload.
- `TeamsDQAlertDestination(WebhookAlertDestination)` type `"teams"`, suffixes `["webhook.office.com","office.com"]`, MessageCard payload.
- `WebhookDQAlertDestination(WebhookAlertDestination)` type `"webhook"`, no suffix restriction, optional `username: str|DQSecret|None`, `password: str|DQSecret|None` → builds `WebhookAuth`; canonical DQX JSON payload (`message.__dict__`-style).

- [ ] **Step 1:** Tests with a fake `WebhookClient` (records url/payload/auth): each destination posts to resolved URL; Slack payload has `blocks`; Teams payload has `@type`/`sections`; generic webhook passes auth when credentials set; `DQSecret` URL resolved through a fake `SecretResolver`.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** PASS. fmt+lint.
- [ ] **Step 5:** Commit `feat(actions): add Slack/Teams/generic webhook destinations`.

---

## Task 8: DBSQL + callback destinations

**Files:**
- Create: `src/databricks/labs/dqx/actions/destinations/dbsql.py`, `callback.py`
- Test: `tests/unit/test_action_destinations_dbsql.py`, `tests/unit/test_action_destinations_callback.py`

**Interfaces produced:**
- `@dataclass DBSQLAlertDestination(AlertDestination)` type `"dbsql"`: `name: str; warehouse_id: str; query: str; notification_destination_ids: list[str] = []`. `validate` requires non-empty `warehouse_id` and `query` (run `is_sql_query_safe(query)` from `utils.py`; raise `InvalidActionError`/`UnsafeSqlQueryError` if unsafe). `deliver` idempotently creates-or-updates a SQL alert via `services.ws.alerts` (look up by display name == `self.name`; create if absent else update) and triggers a run. All `ws` calls via the injected client.
- `@dataclass(eq=False) CallbackDQAlertDestination(AlertDestination)` type `"callback"`: `name: str; callback: Callable[[AlertMessage, ActionContext], None]`. `deliver` invokes the callback. Not serializable (Task 11 skips with warning).

- [ ] **Step 1:** DBSQL tests with `create_autospec(WorkspaceClient)`: unsafe query rejected at `validate`; `deliver` creates alert when none matches name; updates when one matches; attaches notification ids. Callback test: `deliver` calls the provided callable with message+context.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement (consult `ws.alerts` surface in installed databricks-sdk; use list+filter by `display_name`).
- [ ] **Step 4:** PASS. fmt+lint.
- [ ] **Step 5:** Commit `feat(actions): add DBSQL and callback alert destinations`.

---

## Task 9: DQAlert + FailPipeline actions

**Files:**
- Create: `src/databricks/labs/dqx/actions/alert.py`, `src/databricks/labs/dqx/actions/fail_pipeline.py`
- Test: `tests/unit/test_action_alert.py`, `tests/unit/test_action_fail_pipeline.py`

**Interfaces consumed:** `Action`/`ActionContext`/`ActionResult`/`ActionServices` (T5), `AlertDestination` (T7), `StandardMessageBuilder` (T3).
**Interfaces produced:**
- `class DQAlertFrequency(Enum): ALWAYS; HOURLY; DAILY`. `class NotifyOn(Enum): EACH; STATUS_CHANGE`.
- `@dataclass DQAlert(Action)`: `destinations: list[AlertDestination]; name: str = ""; alert_frequency: DQAlertFrequency = ALWAYS; notify_on: NotifyOn = EACH; severity: str = "error"`. `validate`: non-empty destinations, each `.validate()`. `execute(context, services)`: build message via `StandardMessageBuilder`, dispatch to each destination concurrently with `Threads.gather("dqx-alert", tasks)`, collect per-destination errors into `ActionResult` (isolated failures). Sanitize names before logging.
- `@dataclass FailPipeline(Action)`: `message: str | None = None; name: str = "fail_pipeline"`. `execute` raises `PipelineFailedError(self.message or <default with condition+metrics>, context)`.

- [ ] **Step 1:** DQAlert tests: empty destinations raises at `validate`; `execute` calls every destination's `deliver`; one destination raising does not stop others and is recorded in `ActionResult.destination_errors`. FailPipeline test: `execute` raises `PipelineFailedError` with message containing metrics.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** PASS. fmt+lint.
- [ ] **Step 5:** Commit `feat(actions): add DQAlert and FailPipeline actions`.

---

## Task 10: State store + event stores

**Files:**
- Create: `src/databricks/labs/dqx/actions/state.py`, `src/databricks/labs/dqx/actions/event_storage.py`
- Test: `tests/unit/test_action_state.py`; integration: `tests/integration/test_action_event_storage.py`

**Interfaces produced:**
- `@dataclass(frozen=True) AlertEvent`: `action_name, condition, fired: bool, status: ActionStatus, observed_metrics: dict, run_id: str, run_time: datetime, input_location: str|None, destinations: list[str], delivery_errors: list[str]`.
- `class ActionEventStore(abc.ABC)`: `append(events: list[AlertEvent]) -> None`; `load_latest_per_action() -> dict[str, AlertEvent]`.
- `ActionStateStore(event_store: ActionEventStore | None = None)`: `seed() -> None` (populates from store); `should_fire(dq_action, context, condition_result: bool) -> bool` (ALWAYS/HOURLY/DAILY window + STATUS_CHANGE transition using in-memory last status/time); `record(event: AlertEvent) -> None` (update memory + `event_store.append`). `run_time` comparisons use `context.run_time`.
- `TableActionEventStore(spark, ws, config: ActionEventsConfig)` — append via `save_dataframe_as_table`; latest-per-action via window over `run_time`. Schema constant `ACTION_EVENT_TABLE_SCHEMA`.
- `LakebaseActionEventStore(...)` — mirror `LakebaseChecksStorageHandler` engine/bootstrap.
- `ActionEventStoreFactory.create(config, spark, ws) -> ActionEventStore`.

- [ ] **Step 1 (unit):** With a fake in-memory `ActionEventStore`: ALWAYS always fires; HOURLY suppresses second fire within an hour, allows after; STATUS_CHANGE fires only on HEALTHY→UNHEALTHY; `seed` loads last status so a restart respects it; `record` appends.
- [ ] **Step 2:** FAIL → implement `state.py` + `event_storage.py` → PASS. fmt+lint. Commit `feat(actions): add alert state store and event stores`.
- [ ] **Step 3 (integration):** `tests/integration/test_action_event_storage.py` using pytester `factory` to create+drop a UC table: append events then `load_latest_per_action` returns the most recent per action. Commit.

---

## Task 11: Serializer + definition storage + manager

**Files:**
- Create: `src/databricks/labs/dqx/actions/serializer.py`, `src/databricks/labs/dqx/actions/definition_storage.py`, `src/databricks/labs/dqx/actions/manager.py`
- Test: `tests/unit/test_action_serializer.py`; integration: `tests/integration/test_action_manager.py`

**Interfaces produced:**
- `ActionSerializer`: `to_dict(action: DQAction) -> dict` and `from_dict(d: dict) -> DQAction`, using a `_ACTION_TYPES`/`_DESTINATION_TYPES` registry keyed by `type`. `DQSecret` ↔ `"scope/key"`. `condition` is optional: omitted from `to_dict` when `None`, and absent in the dict ⇒ `condition=None` on load. Callback destinations skipped on `to_dict` with `logger.warning`. Unknown `type` raises `InvalidActionError`. PRD YAML shape (optional condition, action.type/name/destinations[].type...).
- `ActionsStorageHandler(ABC, Generic[T])`: `save(actions: list[DQAction], config: T)`, `load(config: T) -> list[DQAction]`.
- `TableActionsStorageHandler` (UC Delta, filter by `run_config_name`, append/overwrite) and `LakebaseActionsStorageHandler` (mirror checks Lakebase handler). `ActionsStorageHandlerFactory.create(config) -> ActionsStorageHandler`.
- `DQActionManager(ws, spark=None)`: `save_actions(actions, config)`, `load_actions(config) -> list[DQAction]`.

- [ ] **Step 1 (unit):** Round-trip a `DQAction` with each destination type (slack/teams/webhook/dbsql) and `FailPipeline`; `DQSecret` survives as `"scope/key"`; callback destination produces a warning and is omitted; unknown type raises.
- [ ] **Step 2:** FAIL → implement → PASS. fmt+lint. Commit `feat(actions): add action serializer and storage handlers`.
- [ ] **Step 3 (integration):** `DQActionManager.save_actions`/`load_actions` round-trip on a UC table (pytester `factory`). Commit.

---

## Task 12: Action evaluator

**Files:**
- Create: `src/databricks/labs/dqx/actions/evaluator.py`
- Test: `tests/unit/test_action_evaluator.py`

**Interfaces consumed:** all of the above.
**Interfaces produced:**
- `ActionEvaluator(actions: list[DQAction], *, state_store: ActionStateStore, services: ActionServices, message_builder: StandardMessageBuilder | None = None)`; `evaluate(context: ActionContext) -> list[ActionResult]`. Logic: for each action, if `condition is not None` eval it (false → record not-fired, continue); a `None` condition always passes; then if `state_store.should_fire` → run alert actions (collect results), collect `FailPipeline` actions; record events; raise the first `PipelineFailedError` **after** all alerts dispatched.

- [ ] **Step 1:** Tests with autospec'd collaborators: condition false ⇒ destination never delivered; `None` condition ⇒ fires unconditionally; condition true + should_fire ⇒ delivered + event recorded; suppression path records suppressed; a `FailPipeline` action raises `PipelineFailedError` but only after a sibling `DQAlert` delivered; destination failure isolated.
- [ ] **Step 2:** FAIL → implement → PASS. fmt+lint.
- [ ] **Step 3:** Commit `feat(actions): add action evaluator orchestrator`.

---

## Task 13: Public exports

**Files:**
- Modify: `src/databricks/labs/dqx/actions/__init__.py`
- Test: `tests/unit/test_action_exports.py`

**Interfaces produced:** `from databricks.labs.dqx.actions import DQAction, DQAlert, FailPipeline, DQAlertFrequency, NotifyOn, SlackDQAlertDestination, TeamsDQAlertDestination, WebhookDQAlertDestination, DBSQLAlertDestination, CallbackDQAlertDestination, DQActionManager, ActionContext`.

- [ ] **Step 1:** Test imports each public name. **Step 2:** FAIL. **Step 3:** populate `__all__`/exports. **Step 4:** PASS. fmt+lint. **Step 5:** Commit `feat(actions): expose public action API`.

---

## Task 14: Engine integration (batch)

**Files:**
- Modify: `src/databricks/labs/dqx/engine.py` (`DQEngineCore.__init__`, `DQEngine.__init__`, `apply_checks_and_save_in_table`, `apply_checks_by_metadata_and_save_in_table`, add `evaluate_actions`)
- Test: `tests/unit/test_engine_actions.py`; integration: `tests/integration/test_engine_actions.py`

**Interfaces produced:**
- `DQEngine.__init__(..., actions: list[DQAction] | None = None)`. If `actions` given but no `observer` ⇒ `InvalidParameterError`. Build a lazy `ActionEvaluator` (with `ActionStateStore` seeded from `ActionEventsConfig`/metrics table when available, `ActionServices` with `SecretResolver(ws)` + `WebhookClient()`).
- New `DQEngine.evaluate_actions(self, observed_metrics: dict, *, input_location: str|None=None, output_location: str|None=None, quarantine_location: str|None=None, checks_location: str|None=None, rule_set_fingerprint: str|None=None) -> list[ActionResult]` decorated `@telemetry_logger("engine", "evaluate_actions")`. Builds `ActionContext` and calls evaluator.
- In `apply_checks_and_save_in_table` (and the metadata variant), after `save_summary_metrics(...)` with `batch_observation.get`, call `self.evaluate_actions(batch_observation.get, input_location=..., ...)` when actions configured.

- [ ] **Step 1 (unit):** `DQEngine(ws, actions=[...])` with no observer raises `InvalidParameterError`; `evaluate_actions({"error_row_count": 5})` invokes the evaluator (inject a fake evaluator via constructor seam or patch the lazy property through a protected setter — prefer constructor injection of an evaluator factory).
- [ ] **Step 2:** FAIL → implement → PASS. fmt+lint. Commit `feat(actions): wire actions into DQEngine batch flow`.
- [ ] **Step 3 (integration):** real Spark: `apply_checks_and_save_in_table` with a `FailPipeline` action whose condition matches raises `PipelineFailedError`; with a `CallbackDQAlertDestination` that appends to a list, the callback fires once with populated metrics. Commit.

---

## Task 15: Streaming integration

**Files:**
- Modify: `src/databricks/labs/dqx/metrics_listener.py` (optional `action_evaluator` callback), `src/databricks/labs/dqx/engine.py` (`get_streaming_metrics_listener` passes evaluator)
- Test: `tests/unit/test_metrics_listener_actions.py`; integration: extend `tests/integration/test_engine_actions.py`

**Interfaces produced:** `StreamingMetricsListener.__init__(..., action_evaluator: ActionEvaluator | None = None)`; in `onQueryProgress`, after building metrics, if evaluator present build an `ActionContext` from `observed_metrics.asDict()` and call `evaluate`. Failures logged, never crash the stream (except `PipelineFailedError` which propagates).

- [ ] **Step 1 (unit):** construct listener with a fake evaluator + a synthetic progress event; assert `evaluate` called with metrics from the event; unrelated `target_query_id` skips.
- [ ] **Step 2:** FAIL → implement → PASS. fmt+lint. Commit `feat(actions): evaluate actions per streaming micro-batch`.
- [ ] **Step 3 (integration):** streaming run fires the callback destination. Commit.

---

## Task 16: Docs, README, CHANGELOG

**Files:**
- Create: `docs/docs/guide/actions_and_alerts.md`
- Modify: README feature list; `CHANGELOG.md`
- Verify API docs pick up `actions` package exports.

- [ ] **Step 1:** Write `actions_and_alerts.md`: concepts; defining `DQAction`/`DQAlert`/`FailPipeline`; Slack/Teams/webhook/DBSQL destinations; `DQSecret` + security/SSRF notes; conditions over metrics (built-in + custom); frequency + status-change; engine usage (auto on save vs `evaluate_actions`); streaming; storing/loading via `DQActionManager` (UC + Lakebase); declarative `run_config` `actions_location`. Google-style, no backticks around arg names (use italics), escape `{{`.
- [ ] **Step 2:** Add a README feature bullet; CHANGELOG entry under unreleased.
- [ ] **Step 3:** `make docs-build` (or at least `pydoc-markdown` check) to confirm no doc errors. Commit `docs(actions): document actions and alerting`.

---

## Self-Review

**Spec coverage:**
- P0 actions param on engine → T14; `DQAlert` w/ destinations+condition → T7-9; Slack/Teams → T7; store actions UC/Lakebase → T11; batch+streaming → T14/T15; conditions on built-in+custom metrics → T2/T12; frequency + status-change state → T10; standard message detail → T3; requires observer + `evaluate_actions` → T14; `FailPipeline` → T9; standard message → T3.
- P1 generic webhook → T7; frequency config → T10; validate-on-instantiation → T2/T5/T7/T9; performant/async + isolated failures → T9/T12 (Threads.gather); callback destinations → T8; retry/backoff + fail-after-N + warn → T6/T9.
- Security: no eval (T2), SSRF (T6), secrets (T4), log-injection sanitize (T7/T9), unsafe SQL for DBSQL (T8).
- Storage/serialize round-trip + RunConfig `actions_location` → T1/T11.

**Placeholder scan:** none — every task names exact files, interfaces, and test intents.

**Type consistency:** `ActionContext`/`ActionServices`/`ActionResult` defined T5 and consumed unchanged in T7-15; `AlertMessage` T3 used T7-9; `should_fire(dq_action, context, condition_result)` consistent T10/T12; `WebhookClient.post(...)` consistent T6/T7.
