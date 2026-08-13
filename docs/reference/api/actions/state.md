# databricks.labs.dqx.actions.state

Alert state store and event persistence interfaces for the DQX actions subsystem.

This module defines:

* *AlertEvent* — immutable record of a single action execution outcome.
* *ActionEventStore* — abstract interface for persisting and loading alert events.
* *ActionStateStore* — in-memory state manager that evaluates whether an alert should fire based on frequency windows (*HOURLY*, *DAILY*) and status-change semantics (*STATUS\_CHANGE* vs *EACH*), optionally seeded from a persistent *ActionEventStore* so that state survives process restarts.

## AlertEvent Objects[​](#alertevent-objects "Direct link to AlertEvent Objects")

```python
@dataclass(frozen=True)
class AlertEvent()

```

Immutable record of a single DQX action execution outcome.

**Attributes**:

* `action_name` - Logical name of the *DQAction* that was evaluated.
* `condition` - The condition expression string that gated the action, or *None* when the action fires unconditionally.
* `fired` - Whether the action executed (condition evaluated to *True* AND frequency/status-change checks passed).
* `status` - Aggregate *ActionStatus* of the execution.
* `observed_metrics` - Snapshot of the metrics observed during the run.
* `run_id` - Unique identifier for the DQX run that produced this event.
* `run_time` - Timestamp when the DQX run executed.
* `input_location` - Source path/URI of the data being checked, or *None*.
* `destinations` - Names of the destinations that were targeted.
* `delivery_errors` - Error messages for any destinations that failed delivery.
* `condition`0 - Run configuration the event belongs to, for auditing. Populated from the events table on load. On append the event store stamps its own run configuration (from its storage config), so this field is informational and is not consulted on write; defaults to *default* for engines that are not scoped to a run config.

## ActionEventStore Objects[​](#actioneventstore-objects "Direct link to ActionEventStore Objects")

```python
class ActionEventStore(abc.ABC)

```

Abstract interface for persisting and loading *AlertEvent* records.

Concrete implementations include *TableActionEventStore* (Delta table via Spark) and *LakebaseActionEventStore* (PostgreSQL via SQLAlchemy).

### append[​](#append "Direct link to append")

```python
@abc.abstractmethod
def append(events: list[AlertEvent]) -> None

```

Persist *events* to the backing store.

**Arguments**:

* `events` - One or more *AlertEvent* records to append.

### load\_latest\_per\_action[​](#load_latest_per_action "Direct link to load_latest_per_action")

```python
@abc.abstractmethod
def load_latest_per_action() -> dict[str, AlertEvent]

```

Load the most recent *AlertEvent* for each distinct *action\_name*.

**Returns**:

A mapping of *action\_name* to the latest *AlertEvent* for that action. Returns an empty dict when the store has no data.

### load\_last\_fired\_per\_action[​](#load_last_fired_per_action "Direct link to load_last_fired_per_action")

```python
@abc.abstractmethod
def load_last_fired_per_action() -> dict[str, datetime]

```

Load the *run\_time* of the most recent **fired** event for each *action\_name*.

Unlike *load\_latest\_per\_action* — whose latest row may be a non-fired, suppressed evaluation — this surfaces the last time each action actually fired. Seeding *\_last\_fired* from it keeps *HOURLY* / *DAILY* frequency suppression durable across a process restart even while an action stays unhealthy (and thus keeps recording non-fired events).

**Returns**:

A mapping of *action\_name* to the *run\_time* of its most recent fired event. Returns an empty dict when the store has no fired events.

## ActionStateStore Objects[​](#actionstatestore-objects "Direct link to ActionStateStore Objects")

```python
class ActionStateStore()

```

In-memory state manager for DQX alert frequency and status-change gating.

Maintains a per-action record of:

* *last\_fired\_time* — the *run\_time* of the most recent run where the action actually fired (i.e. *event.fired* was *True*).
* *last\_status* — the *ActionStatus* recorded by the most recent event, regardless of whether the action fired.

These are consulted by *should\_fire* to suppress repeated alerts within *HOURLY* / *DAILY* windows and to gate *STATUS\_CHANGE* notifications.

When an *event\_store* is provided, *seed()* hydrates the in-memory maps from persistent storage so that state survives process restarts, and *record()* propagates new events to the store in addition to updating the in-memory maps.

**Arguments**:

* `event_store` - Optional persistent event store. When *None*, state is purely in-memory and does not survive restarts.

### seed[​](#seed "Direct link to seed")

```python
def seed() -> None

```

Hydrate in-memory state from the persistent event store.

If no *event\_store* was provided, this is a no-op. Otherwise the in-memory maps are reset and repopulated:

* *\_last\_status*: set from the latest *AlertEvent* per action.
* *\_last\_fired*: set from the latest **fired** event per action, loaded independently (*load\_last\_fired\_per\_action*) so a streak of suppressed, non-fired unhealthy evaluations does not erase the durable last-fired timestamp that *HOURLY* / *DAILY* suppression relies on.

### should\_fire[​](#should_fire "Direct link to should_fire")

```python
def should_fire(dq_action: DQAction, context: ActionContext,
                condition_result: bool) -> bool

```

Decide whether *dq\_action* should fire for this run.

Decision logic:

1. If *condition\_result* is *False* → do **not** fire.
2. If *dq\_action.action* is **not** a *DQAlert* → fire (no frequency/status gating for non-alert actions).
3. If *dq\_action.action* **is** a *DQAlert*: a. **Frequency check** — based on *alert.alert\_frequency*:

* *ALWAYS*: frequency always allows fire.
* *HOURLY*: suppress if last fire was less than 1 hour ago.
* *DAILY*: suppress if last fire was less than 24 hours ago. b. **Notify-on check** — based on *alert.notify\_on*:
* *EACH*: always allows fire.
* *STATUS\_CHANGE*: fire only when the recorded *last\_status* is **not** *UNHEALTHY* (i.e., fire on transition to UNHEALTHY; suppress if already UNHEALTHY). Both checks must pass for the action to fire. The frequency window is an **absolute cap** evaluated *before* the notify-on gate: while an *HOURLY* / *DAILY* window is active the action is suppressed even when *STATUS\_CHANGE* would otherwise fire on a genuine recovery→failure transition. Use *ALWAYS* if every transition must alert.

All time comparisons use *context.run\_time*; *datetime.now()* is never called.

**Arguments**:

* `dq_action` - The bound action configuration being evaluated.
* `context` - Immutable run-time snapshot carrying *run\_time* and *metrics*.
* `condition_result` - Result of evaluating *dq\_action.condition* against *context.metrics*; *True* means the condition passed.

**Returns**:

*True* if the action should execute this run; *False* otherwise.

### try\_fire[​](#try_fire "Direct link to try_fire")

```python
def try_fire(dq_action: DQAction, context: ActionContext,
             condition_result: bool) -> bool

```

Atomically decide whether *dq\_action* should fire **and reserve the fire slot**.

This is the concurrency-safe variant used by the evaluator. *should\_fire* is a pure predicate: two threads can both call it, both get *True*, and both fire before either records — defeating *HOURLY* / *DAILY* dedup. *try\_fire* closes that check-then-act race by running the same gates and, when they pass for a *DQAlert*, stamping *\_last\_fired* to *context.run\_time* **under the same lock hold**, so a concurrent *try\_fire* for the same action observes the reservation and is suppressed.

A later *record()* for the fired event stamps the same *\_last\_fired* value (idempotent) and updates *\_last\_status*. Non-*DQAlert* actions are never frequency-gated, so nothing is reserved for them.

Trade-off: the reservation is **optimistic** — *\_last\_fired* is stamped before the action's *execute()* runs, so if delivery then fails (a non-terminal error, e.g. a webhook timeout), the slot stays reserved and the alert is suppressed for the rest of the frequency window even though it never actually delivered. This deliberately favors "never double-fire under concurrency" over "guaranteed delivery", consistent with the subsystem's best-effort model; if an alert's delivery must be retried on the next run despite a failure, use *DQAlertFrequency.ALWAYS* (no frequency window to reserve).

**Arguments**:

* `dq_action` - The bound action configuration being evaluated.
* `context` - Immutable run-time snapshot carrying *run\_time* and *metrics*.
* `condition_result` - Result of evaluating *dq\_action.condition*; *True* means it passed.

**Returns**:

*True* if the action should execute this run (and the slot is now reserved); else *False*.

### record[​](#record "Direct link to record")

```python
def record(event: AlertEvent) -> None

```

Record *event* in in-memory state and, optionally, the persistent store.

In-memory updates:

* *\_last\_fired* is updated to *event.run\_time* only when *event.fired* is *True*.
* *\_last\_status* is always updated to *event.status*.

If an *event\_store* was provided, *event\_store.append(\[event])* is called to persist the record. The persistent append runs **outside** the lock: it is remote I/O (UC/Lakebase write), and holding the lock across it would stall every concurrent *should\_fire*/*record* on this store behind the slowest network call. Only the in-memory map updates need the lock.

In-memory state intentionally leads persistence (the in-memory maps are the source of truth for suppression within a process; the store only seeds a fresh process). If the durable append fails, the in-memory update already stands, so this run's suppression state can differ from what a later *seed()* would reload. That divergence is logged at *warning* rather than raised, so a transient store outage degrades to "suppression not durable across restart" instead of crashing the (possibly streaming-listener-thread) caller.

**Arguments**:

* `event` - The *AlertEvent* to record.
