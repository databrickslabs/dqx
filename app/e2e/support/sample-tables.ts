// Journeys monitor tables from the Databricks `samples` catalog, which every
// workspace ships read-only. A table can only be monitored once per deployment,
// so each spec owns its own table and specs never share one.

export const SAMPLE_TABLES = {
  /** 25 rows: n_nationkey 0-24, n_name, n_regionkey 0-4, n_comment. Fast to run. */
  monitoredTablesUi: "samples.tpch.nation",
  /** 5 rows: r_regionkey 0-4, r_name, r_comment. Smallest table, used for profiling. */
  profiler: "samples.tpch.region",
  /** 10k rows: s_suppkey, s_name, s_acctbal (some negative), s_nationkey. */
  runs: "samples.tpch.supplier",
  /** 200k rows of parts; only registered, never run. */
  collections: "samples.tpch.part",
  /** 150k customers; only registered, never run. */
  schedules: "samples.tpch.customer",
  /** 1.5M orders; registered by the data-contract import, never run. */
  contractImport: "samples.tpch.orders",
} as const;

export const ALL_SAMPLE_TABLES: readonly string[] = Object.values(SAMPLE_TABLES);
