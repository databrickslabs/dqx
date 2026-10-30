// Journeys monitor tables from the Databricks `samples` catalog, which every
// workspace ships read-only. A table can only be monitored once per deployment,
// so each spec owns its own tables and specs never share one.

export const SAMPLE_TABLES = {
  /** 25 rows: n_nationkey 0-24, n_name, n_regionkey 0-4, n_comment. Fast to run. */
  monitoredTablesUi: "samples.tpch.nation",
  /** 5 rows: r_regionkey 0-4, r_name, r_comment. Smallest table, used for profiling. */
  profiler: "samples.tpch.region",
  /** 50k rows: s_suppkey, s_name, s_acctbal (~9% negative), s_nationkey. */
  runs: "samples.tpch.supplier",
  /** 200k rows of parts; only registered, never run. */
  collections: "samples.tpch.part",
  /** 150k customers; only registered, never run. */
  schedules: "samples.tpch.customer",
  /** 1.5M orders; registered by the data-contract import, never run. */
  contractImport: "samples.tpch.orders",
  /** 5 rows (w_warehouse_sk, w_warehouse_name) and 6 rows (cc_call_center_sk, cc_name); run as a collection. */
  collectionLifecycle: ["samples.tpcds_sf1.warehouse", "samples.tpcds_sf1.call_center"],
  /** 35 rows: r_reason_sk, r_reason_desc; scheduled through its collection, never fires. */
  collectionSchedule: "samples.tpcds_sf1.reason",
  /** 20 rows: sm_ship_mode_sk, sm_type; a real scheduled run fires on it. */
  scheduledRun: "samples.tpcds_sf1.ship_mode",
  /** 20 rows: ib_income_band_sk, ib_lower_bound, ib_upper_bound; runs that fail or are cancelled. */
  runFailures: "samples.tpcds_sf1.income_band",
  /** 30 rows (web_site_sk) and 60 rows (wp_web_page_sk); one follows a rule's latest version, one is pinned. */
  ruleVersions: ["samples.tpcds_sf1.web_site", "samples.tpcds_sf1.web_page"],
  /** 12 rows: s_store_sk, s_store_name, s_number_employees, s_floor_space; lowcode and SQL rules. */
  customRules: "samples.tpcds_sf1.store",
  /** 10k rows: s_suppkey, s_name, s_acctbal (~9% negative); its failed records are browsed and exported. */
  failedRecords: "samples.tpch_sf1.supplier",
  /** 300 rows: p_promo_sk, p_promo_name, p_cost; draft-run and self-approval settings. */
  approvalSettings: "samples.tpcds_sf1.promotion",
  /** 7,200 rows: hd_demo_sk, hd_income_band_sk; shared from its Permissions tab. */
  permissions: "samples.tpcds_sf1.household_demographics",
  /** 11,718 rows: cp_catalog_page_sk, cp_department; scheduled with non-default run types, never fires. */
  scheduleOptions: "samples.tpcds_sf1.catalog_page",
} as const;

/**
 * The whole samples.bakehouse schema, owned by the multi-table journey because
 * "Select all" in the table picker selects every table in a schema. The first
 * four share a franchiseID column.
 */
export const BAKEHOUSE = {
  schema: "samples.bakehouse",
  withFranchiseId: [
    "samples.bakehouse.media_customer_reviews",
    "samples.bakehouse.media_gold_reviews_chunked",
    "samples.bakehouse.sales_franchises",
    "samples.bakehouse.sales_transactions",
  ],
  others: ["samples.bakehouse.sales_customers", "samples.bakehouse.sales_suppliers"],
} as const;

export const ALL_SAMPLE_TABLES: readonly string[] = [
  ...Object.values(SAMPLE_TABLES).flat(),
  ...BAKEHOUSE.withFranchiseId,
  ...BAKEHOUSE.others,
];
