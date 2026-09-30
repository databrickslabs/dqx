# Studio SQL construction review

Ruff S608 reports SQL-looking strings assembled from other strings. It does not
trace the source of the inserted text or recognise bound parameters elsewhere
in the call. The command below is the reproducible alert inventory:

```bash
app/.venv/bin/ruff check app/src/databricks_labs_dqx_app/backend --select S608 --output-format json
```

Reviewed on 2026-09-30: **182 unsuppressed alerts in 40 files**. The branch
started this pass with 180. Four new alerts are the pending-application and
monitored-version writes: their JSON data is now bound, while their app-owned
table names still require SQL construction. Converting the two settings
SELECTs to the existing executor builder removed two alerts from the interim
184 total. The final count is still two above the original 180. Alert count
is not a count of exploitable paths.

## Boundaries used by the code

- Scalar values in app queries and writes go through Databricks
  `StatementParameterListItem` or psycopg parameters. JSON writes use the
  dialect's `json_parameter_expr(name)` around a bound JSON string.
- Settings reads use `select_rows()` with bound keys, including the fixed
  workspace-config key. Regression tests exercise both real executors with
  mocked database boundaries and verify dialect quoting and parameter values.
- Pagination markers are explicitly cast to `INT`, as required by Databricks
  `LIMIT`/`OFFSET`; other integer parameters retain their `BIGINT` binding.
- The executor's `fqn()` now quotes unusual catalog, schema, and table names.
  Other Databricks paths use `validate_fqn()` plus `quote_fqn()`, or
  `quote_object_fqn()` with a fixed app-owned object name. Dynamic column names
  pass through the executor's `q()` or an explicit fixed allowlist.
- PostgreSQL parameterized execution escapes literal percent signs in quoted
  identifiers and string constants before psycopg parses the placeholders.
  Unparameterized DDL keeps its original text. Offline regression tests use
  psycopg's real parser to cover single/double percent signs, placeholder-like
  identifiers, escaped quotes, and positional/named parameters.
- User-authored and AI-generated SQL is an intentional feature. The execution
  paths run `is_sql_query_safe()` after substitution or final assembly. This
  gate is a separate security control; binding cannot turn an authored query
  body into a scalar parameter.
- Demo and migration SQL contains shipped templates and fixed expressions.
  Numeric fragments are coerced to integers or resolved from fixed constants.

## Site inventory

The count is the number of Ruff diagnostics, not the number of queries. A file
can contain more than one construction pattern.

| Backend file | S608 | Interpolated structure and review boundary |
| --- | ---: | --- |
| `demo/datagen.py` | 1 | Internal demo UPDATE builder: known table/column expressions and integer rate threshold. Column comments use strict quote and backslash escaping. |
| `demo/redate.py` | 11 | App-owned table FQNs and timestamp expression templates; run IDs and timestamp values are bound. |
| `demo/seed_service.py` | 6 | App-owned demo/result tables; run IDs, target times, and IN-list values are bound. |
| `lowcode_compile.py` | 3 | Generated SELECT bodies from validated AST operators, slots, grouping, and strict literal escaping; consumers apply the SQL safety gate. |
| `migrations/__init__.py` | 2 | App migration metadata table; version and description values are bound. |
| `migrations/postgres.py` | 3 | Shipped DDL template with executor-quoted schema and metadata table; migration record values are bound. |
| `pg_executor.py` | 2 | Generic upsert builder: identifiers use `q()`/`fqn()`, ordinary values are bound, and only explicit `RawSql` expressions remain inline. |
| `routes/v1/dq_results.py` | 6 | Quoted app views/tables, fixed SELECT fragments and bound filters, limits, and offsets. |
| `routes/v1/dq_score.py` | 1 | Quoted metric view and bound table filter. |
| `routes/v1/metrics.py` | 2 | App metric/run tables with bound table filter and limit; joins are fixed. |
| `routes/v1/quarantine.py` | 2 | Quoted quarantine table, built WHERE clauses with bound values, and bound pagination. |
| `rule_test_sql.py` | 4 | User-authored predicate/query and manual grid SQL; the service validates the predicate and fully assembled SQL before OBO execution, and grid literals escape quotes and backslashes. |
| `run_status_manager.py` | 4 | App run table and fixed projection fragments; run IDs and UPDATE values are bound. |
| `services/ai_rules_service.py` | 1 | SQL-looking text in an AI prompt template, not an executed query. |
| `services/apply_rules_service.py` | 6 | App tables and dialect JSON expression; row, rule, and audit values are bound. The authored row filter is validated before use. |
| `services/comments_service.py` | 3 | App comments table and timestamp projection; comment and identity values are bound. |
| `services/data_product_service.py` | 4 | App product/score tables and fixed projections; product and owner values are bound. |
| `services/database_reset_service.py` | 3 | App-owned tables and fixed settings/admin-role constants in a privileged reset operation. |
| `services/discovery.py` | 1 | Quoted catalog metadata view with bound schema and table filters. |
| `services/entitlement_service.py` | 1 | Generated entitlement view DDL from quoted app object names and a fixed TTL. |
| `services/genie_space_service.py` | 14 | Curated SQL assets built from quoted app views/tables; question filters use named Genie parameters. One diagnostic covers SQL text embedded in a Genie instruction. |
| `services/job_service.py` | 2 | App run table and integer limit; placeholder write fields and read filters are bound. |
| `services/materializer.py` | 4 | App rule tables with bound write values; the substituted authored filter is validated again before materialization. |
| `services/metadata_dim_service.py` | 1 | App metadata table and bounded batched INSERT templates; row values are bound. |
| `services/monitored_table_service.py` | 9 | App tables, fixed joins/projections, and bound binding, owner, schedule, and status values. |
| `services/monitored_table_versions.py` | 2 | App version table; state JSON and row keys are bound, with JSON converted by the dialect helper. |
| `services/pending_application_service.py` | 2 | App pending table; mapping JSON and row keys are bound, with JSON converted by the dialect helper. |
| `services/permissions_service.py` | 4 | App grants/history tables, fixed column choices and projections, and bound object/principal values. |
| `services/registry_service.py` | 13 | App registry tables and fixed projections; rule/write values are bound. One diagnostic covers a validation-only wrapper around an authored filter. |
| `services/review_status_service.py` | 4 | App status/history tables, fixed projections, bound run IDs/status values, and a fixed history limit. |
| `services/role_service.py` | 5 | App role/history tables, fixed projections, and bound role, group, audit, and pagination values. |
| `services/rules_catalog_service.py` | 10 | App rule/history tables, dialect JSON expression and fixed projections; rule/write values are bound. |
| `services/run_sets.py` | 2 | App run-set/member tables and bound IDs/config values. |
| `services/schedule_config_service.py` | 6 | App schedule/history tables and fixed projections; schedule/config/audit values are bound. |
| `services/scheduler_service.py` | 16 | App tables and views, fixed SELECT/DDL/retention clauses, quoted catalog/schema names, and bound configurable filters. Retention days are coerced to bounded integers; tracker timestamp text is produced by `datetime.isoformat()`. |
| `services/score_cache_service.py` | 6 | Quoted metric view and app score tables with bound filters; cache run time is cast from a bound timestamp value. |
| `services/score_view_service.py` | 1 | Generated view DDL from quoted app view names and fixed run mode. |
| `services/table_data_service.py` | 1 | AI-generated SELECT, checked for one read-only statement with `is_sql_query_safe()` before OBO execution; outer limit is fixed. |
| `services/view_service.py` | 5 | Source table quoted before entering the sample builder; row counts, percentages, and seeds are integer-coerced. |
| `sql_executor.py` | 9 | Generic SELECT/DML/upsert builders: identifiers use `q()`/`fqn()`, values are bound, and explicit `RawSql` remains for fixed functions. |

## Remaining limitations

This is a source-level review, not a live execution test of every generated
statement. `is_sql_query_safe()` is the project's policy for authored SQL;
its accuracy is a separate review concern. The generic executors still allow
callers to supply an explicit `RawSql` expression, so new callers must reserve
it for fixed SQL functions and use parameters for runtime data. No S608
suppression was added, and the 182 structural diagnostics remain visible.
Live Databricks SQL validation has not run: the configured test profile had
no running warehouse during this pass.
