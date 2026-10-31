import { describe, expect, it } from "bun:test";
import type { DataProductOut, MonitoredTableOut } from "@/lib/api";
import { collectionTarget, matchesTargetSearch, tableTarget } from "./schedule-target";

function table(overrides: Partial<MonitoredTableOut> = {}): MonitoredTableOut {
  return {
    binding_id: "b1",
    table_fqn: "main.sales.orders",
    status: "approved",
    owner: "ana@example.com",
    ...overrides,
  } as MonitoredTableOut;
}

function product(overrides: Partial<DataProductOut> = {}): DataProductOut {
  return {
    product_id: "p1",
    name: "Sales",
    owner: "bo@example.com",
    members: [],
    ...overrides,
  } as DataProductOut;
}

describe("tableTarget", () => {
  it("uses the short table name and the FQN as detail", () => {
    const target = tableTarget(table());
    expect(target).toMatchObject({ kind: "table", id: "b1", name: "orders", detail: "main.sales.orders" });
    expect(target.tableFqns).toEqual(["main.sales.orders"]);
  });

  it("has no existing schedule without a cron", () => {
    expect(tableTarget(table()).existing).toBeNull();
  });

  it("carries the existing schedule, defaulting timezone and sample size", () => {
    const target = tableTarget(table({ schedule_cron: "0 6 * * *", schedule_kind: "dq_only" }));
    expect(target.existing).toEqual({ cron: "0 6 * * *", timezone: "UTC", kind: "dq_only", sampleSize: 0 });
  });

  it("prefers the owner display name", () => {
    expect(tableTarget(table({ owner_display_name: "Ana" })).owner).toBe("Ana");
  });
});

describe("collectionTarget", () => {
  it("preflights only real member tables, not cross-table SQL checks", () => {
    const target = collectionTarget(
      product({
        members: [{ table_fqn: "a.b.c" }, { table_fqn: "__sql_check__/dupes" }] as DataProductOut["members"],
      }),
      "2 tables",
    );
    expect(target.tableFqns).toEqual(["a.b.c"]);
    expect(target.detail).toBe("2 tables");
  });

  it("carries the existing schedule", () => {
    const target = collectionTarget(
      product({ schedule_cron: "0 * * * *", schedule_tz: "Europe/London", schedule_sample_size: 500 }),
      "0 tables",
    );
    expect(target.existing).toEqual({ cron: "0 * * * *", timezone: "Europe/London", kind: null, sampleSize: 500 });
  });
});

describe("matchesTargetSearch", () => {
  it("matches name, detail and owner case-insensitively", () => {
    const target = tableTarget(table());
    expect(matchesTargetSearch(target, "")).toBe(true);
    expect(matchesTargetSearch(target, "SALES")).toBe(true);
    expect(matchesTargetSearch(target, "ana@")).toBe(true);
    expect(matchesTargetSearch(target, "inventory")).toBe(false);
  });
});
