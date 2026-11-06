import { describe, expect, it } from "bun:test";
import type { SavedViewPayload } from "@/lib/api";
import { DATA_PRODUCTS_COLUMN_LAYOUT } from "@/components/data-products/DataProductsTable";
import { buildCollectionsViewDefs } from "@/components/data-products/collections-view";
import { MONITORED_TABLES_COLUMN_LAYOUT } from "@/components/monitored-tables/MonitoredTablesTable";
import {
  buildMonitoredTablesViewDefs,
  TABLE_GROUP_BY_VALUES,
} from "@/components/monitored-tables/monitored-tables-view";
import { buildGlobalResultsViewDefs } from "@/components/results/global-results-view";
import { RULES_TABLE_COLUMN_LAYOUT } from "@/components/rules/RulesTable";
import { buildRegistryRulesViewDefs, RULE_GROUP_BY_VALUES } from "@/components/rules/registry-rules-view";
import { reconcileView, type ViewDefs } from "./saved-views";
// The template the demo seed reads (backend/demo/saved_views.json): the single source of truth.
import template from "../../../backend/demo/saved_views.json";

// Drift guard: every key the demo views use must exist on the page it targets,
// and the views must survive reconcileView unchanged (nothing silently dropped).

interface DemoView {
  page: string;
  name: string;
  payload: SavedViewPayload;
}

const CATALOG = "acme";
const SCHEMA = "acme_demo";

function substitute(text: string): string {
  return text.replace(/\{catalog\}|\{schema\}/g, (token) => (token === "{catalog}" ? CATALOG : SCHEMA));
}

const views: DemoView[] = JSON.parse(substitute(JSON.stringify(template.views)));

/** Page defs with the demo catalog/schema/dimension among the filter options, as after a seed. */
const DEFS: Record<string, ViewDefs<string, string>> = {
  monitored_tables: buildMonitoredTablesViewDefs(MONITORED_TABLES_COLUMN_LAYOUT, {
    catalogs: [CATALOG, "other"],
    schemas: [SCHEMA, "other"],
    owners: [],
  }),
  global_results: buildGlobalResultsViewDefs({
    catalog: [CATALOG],
    schema: [`${CATALOG}.${SCHEMA}`],
    owner: [],
    collection: [],
    table: [],
    rule: [],
    dimension: [],
    severity: [],
  }),
  // The Rules page compares the dimension against the label definition values (seeded default list).
  registry_rules: buildRegistryRulesViewDefs(RULES_TABLE_COLUMN_LAYOUT, {
    dimensions: ["Validity", "Completeness", "Accuracy", "Consistency", "Uniqueness", "Timeliness"],
    severities: ["critical", "high", "medium", "low"],
  }),
  collections: buildCollectionsViewDefs(DATA_PRODUCTS_COLUMN_LAYOUT, { owners: [] }),
};

const GROUP_BY_VALUES: Record<string, readonly string[]> = {
  monitored_tables: TABLE_GROUP_BY_VALUES,
  registry_rules: RULE_GROUP_BY_VALUES,
};

describe("demo saved views template", () => {
  it("covers the four demo pages", () => {
    expect(views.map((v) => `${v.page}:${v.name}`)).toEqual([
      "monitored_tables:Demo Tables Only",
      "global_results:Demo Tables Only",
      "registry_rules:Validity Rules by Severity",
      "collections:Collection Health",
    ]);
  });

  for (const view of views) {
    describe(`${view.page} / ${view.name}`, () => {
      const defs = DEFS[view.page];

      it("only uses column keys the page defines", () => {
        const known = defs.columns ? Object.keys(defs.columns.columns) : [];
        expect(Boolean(defs.columns)).toBe(Boolean(view.payload.columns));
        const used = [
          ...(view.payload.columns?.order ?? []),
          ...Object.keys(view.payload.columns?.visibility ?? {}),
        ];
        for (const key of used) expect(known).toContain(key);
      });

      it("only uses filter and value keys the page defines", () => {
        const known = Object.keys(defs.filters.filters);
        const used = [
          ...(view.payload.filters?.order ?? []),
          ...Object.keys(view.payload.filters?.visibility ?? {}),
          ...Object.keys(view.payload.values ?? {}),
        ];
        for (const key of used) expect(known).toContain(key);
      });

      it("uses an allowed Group by value", () => {
        const groupBy = view.payload.values?.groupBy;
        if (groupBy === undefined) return;
        expect(GROUP_BY_VALUES[view.page]).toContain(groupBy as string);
      });

      it("survives reconcileView with its layout and values intact", () => {
        const state = reconcileView(view.payload, defs);
        for (const [key, value] of Object.entries(view.payload.values ?? {})) {
          expect(state.values[key]).toEqual(value);
        }
        if (view.payload.columns && state.columns) {
          expect(state.columns.order).toEqual(view.payload.columns.order ?? []);
          for (const [key, visible] of Object.entries(view.payload.columns.visibility ?? {})) {
            expect(state.columns.visibility[key]).toBe(visible);
          }
        }
        expect(state.filters.order).toEqual(view.payload.filters?.order ?? []);
        for (const [key, visible] of Object.entries(view.payload.filters?.visibility ?? {})) {
          expect(state.filters.visibility[key]).toBe(visible);
        }
      });
    });
  }
});
