# Monitor your tables

This section is for stewards who want to put real, ongoing data-quality checks on a table. It's where the reusable rules in your [Rules Registry](/dqx/docs/studio/authoring/create-a-rule.md) stop being definitions and start doing work.

When you apply rules to a Unity Catalog table, that table becomes a **monitored table** — the single source of truth for that table's data quality. Everything about how healthy the table is, which checks run on it, and how it's trended over time lives in one place.

## Open a table to monitor[​](#open-a-table-to-monitor "Direct link to Open a table to monitor")

1. Go to **Tables**.
2. Select **Monitor a table**.
3. Browse Unity Catalog: pick a **catalog** > **schema** > **table**. You only see catalogs, schemas, and tables you already have access to — if a table you expect isn't listed, you are missing a permission.
4. The table detail opens.

Adding many tables at once

Need to bring dozens of tables under monitoring? Use bulk-register to add many tables in one step instead of opening them one at a time.

## The table detail[​](#the-table-detail "Direct link to The table detail")

A monitored table has four tabs:

* **About** — the table's schema, a preview of sample data, and an "ask a question" box for quick natural-language questions about the data.
* **Apply Rules** — where you turn rules into live checks. See [Assign rules](/dqx/docs/studio/monitoring/assign-rules.md).
* **Results** — the table's data-quality score, its trend over time, and a breakdown you can drill down to the failing rows. See [Review results](/dqx/docs/studio/running.md).
* **Profile** — a scan of the table's shape that can suggest candidate rules. See [Profiling](/dqx/docs/studio/monitoring/profiling.md).

![Monitored table About tab showing schema, a governed tag, and a sample-data preview with an ask-a-question box](/dqx/img/studio/table_detail_light.png)![Monitored table About tab showing schema, a governed tag, and a sample-data preview with an ask-a-question box](/dqx/img/studio/table_detail_dark.png)

## In this section[​](#in-this-section "Direct link to In this section")

* **[Assign rules](/dqx/docs/studio/monitoring/assign-rules.md)** — attach rules from your registry to a table's columns, tune them per table, and set [quality thresholds](/dqx/docs/studio/monitoring/assign-rules.md#quality-thresholds).
* **[Profiling](/dqx/docs/studio/monitoring/profiling.md)** — scan a table and let the DQX suggest candidate rules.
* **[Collections](/dqx/docs/studio/monitoring/collections.md)** — group related tables into a data product or domain and see quality across all of them at once.
* **[Run checks & schedule](/dqx/docs/studio/monitoring/run-and-schedule.md)** — run checks on demand or on a schedule, and see past runs.
