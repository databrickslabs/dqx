import type { APIRequestContext, APIResponse } from "@playwright/test";

import { E2E_PREFIX, E2E_TAG, runTag } from "./names";

// Thin client over the Studio REST API (/api/v1), used for journey setup,
// assertions on server state, and cleanup. Journeys drive the UI for the step
// under test and use this for everything around it.

export interface RegistryRule {
  rule_id: string;
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
}

export interface NativeRuleSpec {
  name: string;
  /** DQX check function, e.g. "is_not_null". */
  fn: string;
  /** Extra literal arguments besides the column slot, e.g. { limit: 0 }. */
  args?: Record<string, unknown>;
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

export class StudioApi {
  constructor(private readonly http: APIRequestContext) {}

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
            body: { function: spec.fn, arguments: { column: "{{column}}", ...spec.args } },
            slots: [{ name: "column", family: "any", position: 0, cardinality: "one" }],
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

  /** Creates, submits and approves a rule so it can be applied to tables. */
  async createPublishedRule(spec: NativeRuleSpec): Promise<RegistryRule> {
    const rule = await this.createNativeRule(spec);
    await this.submitRule(rule.rule_id);
    if ((await this.getRule(rule.rule_id)).status !== "approved") {
      await json(await this.http.post(`/api/v1/registry-rules/${rule.rule_id}/approve`, { data: {} }), "approve rule");
    }
    return this.getRule(rule.rule_id);
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

  async getMonitoredTableDetail(bindingId: string): Promise<MonitoredTableDetail> {
    return json<MonitoredTableDetail>(
      await this.http.get(`/api/v1/monitored-tables/${bindingId}`),
      `get monitored table ${bindingId}`,
    );
  }

  async getMonitoredTable(bindingId: string): Promise<MonitoredTable> {
    return (await this.getMonitoredTableDetail(bindingId)).table;
  }

  async applyRule(bindingId: string, ruleId: string, column: string): Promise<string> {
    const body = await json<{ id: string }>(
      await this.http.post(`/api/v1/monitored-tables/${bindingId}/applied-rules`, {
        data: { rule_id: ruleId, column_mapping: [{ column }] },
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

  async clearTableSchedule(bindingId: string): Promise<void> {
    await json(
      await this.http.patch(`/api/v1/monitored-tables/${bindingId}/schedule`, { data: { schedule_cron: null } }),
      `clear schedule ${bindingId}`,
    );
  }


  // ---- sweeping ----------------------------------------------------------

  /** Registry rules left behind by any earlier run of this suite (by tag or name prefix). */
  async leftoverRules(): Promise<RegistryRule[]> {
    return (await this.listRules()).filter(
      (r) => E2E_TAG in (r.user_metadata ?? {}) || String(r.user_metadata?.name ?? "").startsWith(E2E_PREFIX),
    );
  }
}
