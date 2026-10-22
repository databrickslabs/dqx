# databricks.labs.dqx.actions.lineage

Lineage collection action for the DQX actions & alerting subsystem.

*CollectLineageAction* persists upstream and downstream table lineage and per-failed-column lineage for the source table of a DQX run. Each lineage edge is emitted as a row in a single Delta table (schema declared in *LINEAGE_TABLE_SCHEMA*) via the same *io.save_dataframe_as_table* helper DQX uses for failed-check output — so the write mode / format / options semantics are handled identically.

The action never fails the pipeline: collection errors are logged (via the *\_sanitize* helper, per CWE-117) and swallowed so a stale system-table read cannot mask check results.

The persisted table's *location* is emitted verbatim as the *extras* payload (`{&quot;lineage_location&quot;: &quot;&lt;full name&gt;&quot;}`); downstream actions read the table when they need the full structure.

Per-failed-column lineage is derived from the run's *output_location*: DQX writes each row's check failures into two array-of-struct columns (*\_errors* and *\_warnings*), and each struct's *columns* field lists the column(s) the check flagged. The lineage action reads that table, explodes those two arrays plus their inner *columns* field, and takes the distinct set to seed the column-lineage walks. See *docs/dqx/docs/reference/table_schemas.mdx* for the full shape.

Table-graph walks (upstream / downstream) use a single recursive CTE against *system.access.table_lineage* with an in-CTE full-path cycle guard, and require DBR 17+ for the recursive-CTE SQL feature.

## LineageSearchConfig Objects[​](#lineagesearchconfig-objects "Direct link to LineageSearchConfig Objects")

```python
class LineageSearchConfig(BaseModel)

```

Search parameters for one lineage traversal direction (upstream, downstream, or columns).

**Attributes**:

* `depth` - Optional maximum walk depth from the source table in hops. When set, must be `&gt;= 1` and acts as a **safety guardrail** capping recursion depth for both the table-lineage and column-lineage walks. Defaults to `100` — matches Spark's default *spark.sql.cteRecursionLevelLimit*, so the recursive CTE stays within the engine's own hop limit on deep graphs. Set to *None* to opt into unbounded walks (the in-CTE full-path cycle guard plus *max_nodes* still keep the traversal finite, but very deep graphs may trip Spark's recursion limit at execution time).
* `lookback_days` - How far back to consider lineage entries in *system.access.table_lineage* / *system.access.column_lineage*. Must be `&gt;= 1`.
* `max_nodes` - Guardrail row cap applied both **inside** each member of the recursive lineage CTE (bounding the frontier per hop, so intermediate expansion is capped per iteration) and once at the tail (bounding total output). Must be `&gt;= 1` (default 100). Together with *depth* (when set) this bounds the traversal's materialised row count to `O(depth * max_nodes)`; with unbounded *depth* the cap only bounds per-hop expansion and total output. This is a safety limit — it does **not** guarantee which rows survive when the graph produces more edges than the cap (no ordering contract).

## LineageActionConfig Objects[​](#lineageactionconfig-objects "Direct link to LineageActionConfig Objects")

```python
class LineageActionConfig(BaseModel)

```

Top-level configuration for *CollectLineageAction*.

Composes four independent sub-configs — *upstream*, *downstream*, *column_upstream*, *column_downstream*.

Each sub-config defaults to a fresh *LineageSearchConfig* (direction on by default); set any field to *None* to disable that direction. Table and column directions are symmetric: the four fields let a caller collect, e.g., only table-upstream + column-downstream if that is all they need.

Per-failed-column lineage seeds are read from whichever of *context.output_location* and *context.quarantine_location* are present and unioned. DQX writes failures to *quarantine_location* in split-run mode and to *output_location* in non-split mode, so the union makes the action work in either configuration without extra wiring.

*errors_column* and *warnings_column* name the two ARRAY\<STRUCT> columns DQX appends to the output / quarantine tables. Defaults match DQX's built-in names (`_errors` / `_warnings`); override them when the engine's error / warning column renames have been applied so this action can still locate the issue structs.

## CollectLineageAction Objects[​](#collectlineageaction-objects "Direct link to CollectLineageAction Objects")

```python
@register_action
class CollectLineageAction(Action)

```

Collect table / column lineage into a Delta table.

On every run, gathers upstream/downstream tables (recursive-CTE walk, optionally bounded by *depth*) and recursive per-failed-column lineage (upstream and downstream, seeded from failure records read from whichever of *output_location* and *quarantine_location* are present on the context), and writes one row per edge to the Delta table specified by *output_config*. The action never fails the pipeline — collection errors are logged and turned into *CONFIG_ERROR* results. To disable a specific direction, set the corresponding sub-config on *LineageActionConfig* (*upstream*, *downstream*, *column_upstream*, *column_downstream*) to *None*.

**Attributes**:

* `type` - Discriminator literal, always *"collect_lineage"*.
* `name` - Logical identifier for this action; derived to *"collect_lineage"* when left empty.
* `output_config` - Required output sink for the lineage table. *mode* should be set equal to *RunConfig.output_config.mode* so lineage writes stay consistent with how DQX writes the failed-checks output.
* `config` - Optional collection knobs — see *LineageActionConfig*.

### execute[​](#execute "Direct link to execute")

```python
def execute(context: ActionContext, services: ActionServices) -> ActionResult

```

Collect lineage rows and persist them; emit the target-table full name as extras.

### extract_failed_columns[​](#extract_failed_columns "Direct link to extract_failed_columns")

```python
def extract_failed_columns(
        spark: SparkSession,
        output_location: str,
        run_id: str,
        errors_column: str = _DEFAULT_ERRORS_COLUMN,
        warnings_column: str = _DEFAULT_WARNINGS_COLUMN) -> DataFrame

```

Read *output_location* and return a DataFrame of distinct column names that failed a check **for the current DQX run**.

DQX appends two ARRAY\<STRUCT> columns to the output table (default names `_errors` / `_warnings` — override via *errors_column* / *warnings_column* when the DQX engine has renamed them) and every struct exposes a *run_id* STRING plus a *columns* ARRAY\<STRING> listing the column(s) the check flagged. In append-mode writes the same table accumulates rows from many runs, so this helper explodes both arrays, keeps issues whose `issue.run_id` matches the supplied *run_id* **or is null** (treating null as "current run"), then explodes the inner *columns* field, unions the two, filters nulls, and returns the distinct set as a one-column DataFrame (schema *\_FAILED_COLUMNS_SCHEMA* with a single `col_name STRING` column). Without the run-id filter a prior run's failure could seed column-lineage rows stamped with the current *run_id*.

The DataFrame shape is deliberate: the downstream column-lineage CTE joins it via a session temp view rather than iterating one query per name, so the full pipeline stays lazy and Spark can broadcast the (small) failed-columns set.

A missing table, an issue-column name that is not present on the output, or a read failure returns an empty DataFrame with the same schema and logs a sanitised warning — the join then produces zero rows and column lineage becomes a natural no-op.

**Arguments**:

* `spark` - Active *SparkSession* used to read *output_location*.
* `output_location` - 3-level UC name of the DQX output (or quarantine) table.
* `0 - Current DQX run identifier — issue structs whose `issue.run_id\`\` equals this value or is null contribute to the returned column set.
* `3 - Name of the ARRAY&lt;STRUCT&gt; column that carries error-level issues (default `\_errors\`\`).
* `6 - Name of the ARRAY&lt;STRUCT&gt; column that carries warning-level issues (default `\_warnings\`\`).

**Returns**:

DataFrame with a single `col_name STRING` column, distinct-only, non-null-only.
