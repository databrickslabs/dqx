import { expect, type APIRequestContext, type APIResponse } from "@playwright/test";

import { E2E_PREFIX, E2E_TAG, runTag } from "./names";

// Thin client over the Studio REST API (/api/v1), used for journey setup,
// assertions on server state, and cleanup. Journeys drive the UI for the step
// under test and use this for everything around it.

export interface RegistryRule {
  rule_id: string;
  mode: string;
  definition: { body: Record<string, unknown>; slots: { name: string }[] };
  status: string;
  version: number;
  display_status?: string;
  user_metadata: Record<string, unknown>;
  created_by?: string;
}

export interface MonitoredTable {
  binding_id: string;
  table_fqn: string;
  status: string;
  created_by?: string;
}

export interface MonitoredTableDetail {
  table: MonitoredTable;
  applied_rules: { id: string; rule_id: string }[];
}

export interface Collection {
  product_id: string;
  name: string;
  status: string;
  members: { id: string; binding_id: string }[];
  created_by?: string;
}

export interface ScheduleOverviewRow {
  source_type: string;
  source_id: string;
  name: string;
  cron: string | null;
  enabled: boolean;
  paused: boolean;
  schedule_kind?: string | null;
  sample_size?: number | null;
}

export interface ReviewStatus {
  value: string;
  description: string;
  color: string;
  is_default: boolean;
}

export type GrantObjectType = "registry_rule" | "monitored_table" | "data_product";

export interface Grant {
  principal_id: string;
  principal_type: string;
  principal_name: string;
  privileges: string[];
}

export interface RunResults {
  total_rows: number;
  invalid_rows: number;
  error_summary: { error: string; count: number }[];
}

export interface RunSummary {
  run_id: string;
  source_table_fqn: string;
  /** RUNNING | SUCCESS | FAILED | CANCELED | PENDING */
  status: string;
  /** e.g. "manual" | "scheduled" */
  run_type: string | null;
  created_at: string;
}

export interface NativeRuleSpec {
  name: string;
  /** DQX check function, e.g. "is_not_null". */
  fn: string;
  /** Extra literal arguments besides the column slot, e.g. { limit: 0 }. */
  args?: Record<string, unknown>;
  /** Name of the rule's column variable (default "column"). "Apply to tables"
   *  auto-maps it to a same-named column, e.g. "franchiseID". */
  slot?: string;
  severity?: string;
  dimension?: string;
  description?: string;
}



async function json<T>(res: APIResponse, what: string): Promise<T> {
  if (!res.ok()) {
    throw new Error(`${what} failed: HTTP ${res.status()} ${(await res.text()).slice(0, 500)}`);
  }
  return (await res.json()) as T;
}

/** The request methods StudioApi uses; the *api* fixture adds auth to each call. */
export type StudioHttp = Pick<APIRequestContext, "get" | "post" | "put" | "patch" | "delete">;

export class StudioApi {
  constructor(private readonly http: StudioHttp) {}

  // ---- identity / config -------------------------------------------------

  async currentUserEmail(): Promise<string> {
    const body = await json<{ email: string }>(await this.http.get("/api/v1/current-user/role"), "current user");
    return body.email;
  }

  async approvalsMode(): Promise<string> {
    return (await json<{ mode: string }>(await this.http.get("/api/v1/config/approvals-mode"), "approvals mode")).mode;
  }

  async setApprovalsMode(mode: string): Promise<void> {
    await json(await this.http.put("/api/v1/config/approvals-mode", { data: { mode } }), `set approvals mode ${mode}`);
  }

  async labelDefinitions(): Promise<{ key: string }[]> {
    const body = await json<{ definitions: { key: string }[] }>(
      await this.http.get("/api/v1/config/label-definitions"),
      "label definitions",
    );
    return body.definitions;
  }

  /** PUT replaces the whole set; used to restore a snapshot taken with *labelDefinitions*. */
  async setLabelDefinitions(definitions: { key: string }[]): Promise<void> {
    await json(await this.http.put("/api/v1/config/label-definitions", { data: { definitions } }), "set label definitions");
  }

  // ---- registry rules ----------------------------------------------------

  async createNativeRule(spec: NativeRuleSpec): Promise<RegistryRule> {
    const body = await json<{ rule: RegistryRule }>(
      await this.http.post("/api/v1/registry-rules", {
        data: {
          mode: "dqx_native",
          definition: {
            body: { function: spec.fn, arguments: { column: `{{${spec.slot ?? "column"}}}`, ...spec.args } },
            slots: [{ name: spec.slot ?? "column", family: "any", position: 0, cardinality: "one" }],
            parameters: [],
          },
          user_metadata: {
            name: spec.name,
            description: spec.description ?? "Created by the Studio Playwright suite.",
            severity: spec.severity ?? "High",
            dimension: spec.dimension ?? "Validity",
            [E2E_TAG]: runTag(),
          },
          allow_duplicate: true,
        },
      }),
      `create rule ${spec.name}`,
    );
    return body.rule;
  }

  /**
   * Creates, submits and approves a SQL-mode rule: either a row-level
   * *predicate* with {{slot}} placeholders (e.g. "{{amount}} >= 0", slots
   * ["amount"]), or a full *sqlQuery* with no slots (a cross-table check).
   */
  async createPublishedSqlRule(spec: {
    name: string;
    predicate?: string;
    sqlQuery?: string;
    slots?: { name: string; family?: string }[];
    polarity?: "pass" | "fail";
  }): Promise<RegistryRule> {
    const body = await json<{ rule: RegistryRule }>(
      await this.http.post("/api/v1/registry-rules", {
        data: {
          mode: "sql",
          polarity: spec.polarity ?? "pass",
          definition: {
            body: spec.sqlQuery ? { sql_query: spec.sqlQuery } : { predicate: spec.predicate },
            slots: (spec.slots ?? []).map((s, i) => ({ name: s.name, family: s.family ?? "any", position: i, cardinality: "one" })),
            parameters: [],
          },
          user_metadata: { name: spec.name, severity: "High", dimension: "Validity", [E2E_TAG]: runTag() },
          allow_duplicate: true,
        },
      }),
      `create SQL rule ${spec.name}`,
    );
    await this.submitRule(body.rule.rule_id);
    if ((await this.getRule(body.rule.rule_id)).status !== "approved") {
      await json(await this.http.post(`/api/v1/registry-rules/${body.rule.rule_id}/approve`, { data: {} }), "approve rule");
    }
    return this.getRule(body.rule.rule_id);
  }

  /** Creates, submits and approves a rule so it can be applied to tables. */
  async createPublishedRule(spec: NativeRuleSpec): Promise<RegistryRule> {
    const rule = await this.createNativeRule(spec);
    await this.submitRule(rule.rule_id);
    if ((await this.getRule(rule.rule_id)).status !== "approved") {
      await json(await this.http.post(`/api/v1/registry-rules/${rule.rule_id}/approve`, { data: {} }), "approve rule");
    }
    return this.getRule(rule.rule_id);
  }

  async approveRule(ruleId: string): Promise<void> {
    if ((await this.getRule(ruleId)).status === "approved") return;
    await json(await this.http.post(`/api/v1/registry-rules/${ruleId}/approve`, { data: {} }), "approve rule");
  }

  async submitRule(ruleId: string): Promise<void> {
    await json(await this.http.post(`/api/v1/registry-rules/${ruleId}/submit`, { data: {} }), "submit rule");
  }

  async getRule(ruleId: string): Promise<RegistryRule> {
    const body = await json<{ rule: RegistryRule }>(await this.http.get(`/api/v1/registry-rules/${ruleId}`), `get rule ${ruleId}`);
    return body.rule;
  }

  async listRules(params: Record<string, string> = {}): Promise<RegistryRule[]> {
    return json<RegistryRule[]>(await this.http.get("/api/v1/registry-rules", { params }), "list rules");
  }

  async findRuleByName(name: string): Promise<RegistryRule | undefined> {
    return (await this.listRules()).find((r) => r.user_metadata?.name === name);
  }

  async deleteRule(ruleId: string): Promise<void> {
    const res = await this.http.delete(`/api/v1/registry-rules/${ruleId}`);
    if (res.status() !== 404) await json(res, `delete rule ${ruleId}`);
  }

  // ---- monitored tables --------------------------------------------------

  async monitorTable(tableFqn: string): Promise<MonitoredTable> {
    const body = await json<{ table: MonitoredTable }>(
      await this.http.post("/api/v1/monitored-tables", { data: { table_fqn: tableFqn } }),
      `monitor ${tableFqn}`,
    );
    return body.table;
  }

  async findMonitoredTable(tableFqn: string): Promise<MonitoredTable | undefined> {
    const rows = await json<{ table: MonitoredTable }[]>(
      await this.http.get("/api/v1/monitored-tables", { params: { name: tableFqn } }),
      "list monitored tables",
    );
    return rows.map((r) => r.table).find((t) => t.table_fqn === tableFqn);
  }

  /** Monitored tables whose FQN is in *schemaFqn* ("catalog.schema"). */
  async listMonitoredTablesIn(schemaFqn: string): Promise<MonitoredTable[]> {
    const [catalog, schema] = schemaFqn.split(".");
    const rows = await json<{ table: MonitoredTable }[]>(
      await this.http.get("/api/v1/monitored-tables", { params: { catalog, schema } }),
      `list monitored tables in ${schemaFqn}`,
    );
    return rows.map((r) => r.table).filter((t) => t.table_fqn.startsWith(`${schemaFqn}.`));
  }

  async getMonitoredTableDetail(bindingId: string): Promise<MonitoredTableDetail> {
    return json<MonitoredTableDetail>(
      await this.http.get(`/api/v1/monitored-tables/${bindingId}`),
      `get monitored table ${bindingId}`,
    );
  }

  async getMonitoredTable(bindingId: string): Promise<MonitoredTable> {
    return (await this.getMonitoredTableDetail(bindingId)).table;
  }

  async applyRule(bindingId: string, ruleId: string, column: string, slot = "column"): Promise<string> {
    const body = await json<{ id: string }>(
      await this.http.post(`/api/v1/monitored-tables/${bindingId}/applied-rules`, {
        data: { rule_id: ruleId, column_mapping: [{ [slot]: column }] },
      }),
      "apply rule",
    );
    return body.id;
  }

  /** Applies a rule with an explicit slot → column mapping ({} for a rule with no slots). */
  async applyRuleWithMapping(bindingId: string, ruleId: string, mapping: Record<string, string>): Promise<string> {
    const body = await json<{ id: string }>(
      await this.http.post(`/api/v1/monitored-tables/${bindingId}/applied-rules`, {
        data: { rule_id: ruleId, column_mapping: [mapping] },
      }),
      "apply rule",
    );
    return body.id;
  }

  async submitTable(bindingId: string): Promise<void> {
    await json(await this.http.post(`/api/v1/monitored-tables/${bindingId}/submit`, { data: {} }), "submit table");
  }

  async approveTable(bindingId: string): Promise<void> {
    if ((await this.getMonitoredTable(bindingId)).status === "approved") return;
    await json(await this.http.post(`/api/v1/monitored-tables/${bindingId}/approve`, { data: {} }), "approve table");
  }

  async deleteMonitoredTable(bindingId: string): Promise<void> {
    const res = await this.http.delete(`/api/v1/monitored-tables/${bindingId}`);
    if (res.status() !== 404) await json(res, `delete monitored table ${bindingId}`);
  }

  /** Monitors *tableFqn*, applies each rule to its column, and gets the table approved. */
  async approvedTable(tableFqn: string, rules: { ruleId: string; column: string }[]): Promise<MonitoredTable> {
    const table = await this.monitorTable(tableFqn);
    for (const r of rules) await this.applyRule(table.binding_id, r.ruleId, r.column);
    await this.submitTable(table.binding_id);
    await this.approveTable(table.binding_id);
    return this.getMonitoredTable(table.binding_id);
  }

  // ---- collections (data products) ---------------------------------------

  async listCollections(): Promise<Collection[]> {
    return json<Collection[]>(await this.http.get("/api/v1/data-products"), "list collections");
  }

  async getCollection(productId: string): Promise<Collection | undefined> {
    const res = await this.http.get(`/api/v1/data-products/${productId}`);
    if (res.status() === 404) return undefined;
    return json<Collection>(res, `get collection ${productId}`);
  }

  async createCollection(name: string): Promise<Collection> {
    return json<Collection>(
      await this.http.post("/api/v1/data-products", {
        data: { name, description: "Created by the Studio Playwright suite." },
      }),
      `create collection ${name}`,
    );
  }

  async addCollectionMember(productId: string, bindingId: string): Promise<void> {
    await json(
      await this.http.post(`/api/v1/data-products/${productId}/members`, { data: { binding_id: bindingId } }),
      "add collection member",
    );
  }

  async submitCollection(productId: string): Promise<void> {
    await json(await this.http.post(`/api/v1/data-products/${productId}/submit`, { data: {} }), "submit collection");
  }

  async approveCollection(productId: string): Promise<void> {
    if ((await this.getCollection(productId))?.status === "approved") return;
    await json(await this.http.post(`/api/v1/data-products/${productId}/approve`, { data: {} }), "approve collection");
  }

  async deleteCollection(productId: string): Promise<void> {
    const res = await this.http.delete(`/api/v1/data-products/${productId}`);
    if (res.status() !== 404) await json(res, `delete collection ${productId}`);
  }

  // ---- schedules ---------------------------------------------------------

  async scheduleOverview(): Promise<ScheduleOverviewRow[]> {
    return json<ScheduleOverviewRow[]>(await this.http.get("/api/v1/schedules/overview"), "schedule overview");
  }

  async tableSchedule(bindingId: string): Promise<ScheduleOverviewRow | undefined> {
    return (await this.scheduleOverview()).find((s) => s.source_type === "table" && s.source_id === bindingId);
  }

  async collectionSchedule(productId: string): Promise<ScheduleOverviewRow | undefined> {
    return (await this.scheduleOverview()).find((s) => s.source_type === "collection" && s.source_id === productId);
  }

  async clearCollectionSchedule(productId: string): Promise<void> {
    const res = await this.http.patch(`/api/v1/data-products/${productId}`, { data: { schedule_cron: null } });
    if (res.status() !== 404) await json(res, `clear schedule on collection ${productId}`);
  }

  async clearTableSchedule(bindingId: string): Promise<void> {
    await json(
      await this.http.patch(`/api/v1/monitored-tables/${bindingId}/schedule`, { data: { schedule_cron: null } }),
      `clear schedule ${bindingId}`,
    );
  }


  // ---- settings ----------------------------------------------------------

  async requireDraftRun(): Promise<boolean> {
    const body = await json<{ require_draft_run_before_submit: boolean }>(
      await this.http.get("/api/v1/config/require-draft-run"),
      "draft-run setting",
    );
    return body.require_draft_run_before_submit;
  }

  async setRequireDraftRun(value: boolean): Promise<void> {
    await json(
      await this.http.put("/api/v1/config/require-draft-run", { data: { require_draft_run_before_submit: value } }),
      "set draft-run setting",
    );
  }

  async reviewStatuses(): Promise<ReviewStatus[]> {
    return (await json<{ statuses: ReviewStatus[] }>(await this.http.get("/api/v1/config/run-review-statuses"), "review statuses"))
      .statuses;
  }

  /** PUT replaces the whole list; used to restore a snapshot taken with *reviewStatuses*. */
  async setReviewStatuses(statuses: ReviewStatus[]): Promise<void> {
    await json(await this.http.put("/api/v1/config/run-review-statuses", { data: { statuses } }), "set review statuses");
  }

  async roleMappings(): Promise<{ role: string; group_name: string }[]> {
    return json(await this.http.get("/api/v1/roles"), "role mappings");
  }

  async deleteRoleMapping(role: string, group: string): Promise<void> {
    const res = await this.http.delete(`/api/v1/roles/${encodeURIComponent(role)}/${encodeURIComponent(group)}`);
    if (res.status() !== 404) await json(res, `delete role mapping ${role} → ${group}`);
  }

  // ---- permissions -------------------------------------------------------

  async grants(objectType: GrantObjectType, objectId: string): Promise<Grant[]> {
    return (await json<{ grants: Grant[] }>(await this.http.get(`/api/v1/permissions/${objectType}/${objectId}/grants`), "grants"))
      .grants;
  }

  async revokeGrant(objectType: GrantObjectType, objectId: string, principalId: string): Promise<void> {
    const res = await this.http.delete(`/api/v1/permissions/${objectType}/${objectId}/grants/${principalId}`);
    if (res.status() !== 404) await json(res, `revoke grant for ${principalId}`);
  }

  // ---- runs --------------------------------------------------------------

  /** Starts a run of the table's approved version; returns its run id. */
  async runTable(bindingId: string): Promise<string> {
    const body = await json<{ run_id: string }>(
      await this.http.post(`/api/v1/monitored-tables/${bindingId}/run`, { data: { source: "approved" } }),
      "run table",
    );
    return body.run_id;
  }

  async runResults(runId: string): Promise<RunResults> {
    return json<RunResults>(await this.http.get(`/api/v1/dryrun/runs/${runId}/results`), `run results ${runId}`);
  }

  async cancelRun(runId: string): Promise<void> {
    const res = await this.http.post(`/api/v1/dryrun/runs/${runId}/cancel`);
    // Already finished is fine for cleanup.
    if (![200, 400, 404, 409].includes(res.status())) await json(res, `cancel run ${runId}`);
  }

  /** Polls Runs History data until *runId* is no longer RUNNING/PENDING; returns its final status. */
  async waitForRunStatus(runId: string, timeout = 10 * 60_000): Promise<string> {
    let status = "";
    await expect
      .poll(
        async () => {
          status = (await this.listRuns()).find((r) => r.run_id === runId)?.status ?? "";
          return status;
        },
        { message: `run ${runId} did not finish`, intervals: [10_000], timeout },
      )
      .toMatch(/^(SUCCESS|FAILED|CANCELED)$/);
    return status;
  }

  /** Recent validation runs (newest first), as listed in Runs History. */
  async listRuns(): Promise<RunSummary[]> {
    return json<RunSummary[]>(await this.http.get("/api/v1/dryrun/runs", { params: { summary: "true" } }), "list runs");
  }

  async runIdsFor(tableFqn: string): Promise<Set<string>> {
    return new Set((await this.listRuns()).filter((r) => r.source_table_fqn === tableFqn).map((r) => r.run_id));
  }

  // ---- sweeping ----------------------------------------------------------

  /** Registry rules left behind by any earlier run of this suite (by tag or name prefix). */
  async leftoverRules(): Promise<RegistryRule[]> {
    return (await this.listRules()).filter(
      (r) => E2E_TAG in (r.user_metadata ?? {}) || String(r.user_metadata?.name ?? "").startsWith(E2E_PREFIX),
    );
  }
}
