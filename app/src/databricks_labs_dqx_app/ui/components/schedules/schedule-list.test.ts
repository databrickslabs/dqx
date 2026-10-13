import { describe, expect, it } from "bun:test";
import { compareSortValues } from "@/components/data-table/sort";
import type { ScheduleOverviewOut } from "@/lib/api";
import {
  EMPTY_SCHEDULE_FILTERS,
  getRelativeFutureParts,
  getScheduleSortValue,
  hasActiveScheduleFilters,
  matchesScheduleFilters,
  resetScheduleFilter,
  scheduleHealth,
  scheduleOwners,
  scheduleRowKey,
  tablesScheduledByCollections,
} from "./schedule-list";

function row(overrides: Partial<ScheduleOverviewOut> = {}): ScheduleOverviewOut {
  return {
    source_type: "table",
    source_id: "b1",
    name: "orders",
    target: "main.sales.orders",
    cron: "0 6 * * *",
    timezone: "UTC",
    enabled: true,
    paused: false,
    owner: "ana@example.com",
    ...overrides,
  };
}

describe("scheduleRowKey", () => {
  it("includes the source type so a table and a collection with the same id stay distinct", () => {
    expect(scheduleRowKey(row({ source_type: "table", source_id: "x" }))).not.toBe(
      scheduleRowKey(row({ source_type: "collection", source_id: "x" })),
    );
  });
});

describe("scheduleHealth", () => {
  it("reports paused over a failed last run", () => {
    expect(scheduleHealth(row({ paused: true, run_status: "failed" }))).toBe("paused");
  });

  it("treats a disabled schedule as paused", () => {
    expect(scheduleHealth(row({ enabled: false }))).toBe("paused");
  });

  it("maps failed and partial_failure run statuses", () => {
    expect(scheduleHealth(row({ run_status: "failed" }))).toBe("failed");
    expect(scheduleHealth(row({ run_status: "partial_failure" }))).toBe("partial");
  });

  it("is active otherwise", () => {
    expect(scheduleHealth(row({ run_status: "success" }))).toBe("active");
    expect(scheduleHealth(row())).toBe("active");
  });
});

describe("matchesScheduleFilters", () => {
  it("matches everything with empty filters", () => {
    expect(matchesScheduleFilters(row(), EMPTY_SCHEDULE_FILTERS)).toBe(true);
  });

  it("filters by type and owner", () => {
    expect(matchesScheduleFilters(row(), { ...EMPTY_SCHEDULE_FILTERS, type: "collection" })).toBe(false);
    expect(matchesScheduleFilters(row(), { ...EMPTY_SCHEDULE_FILTERS, owner: "bo@example.com" })).toBe(false);
    expect(matchesScheduleFilters(row(), { ...EMPTY_SCHEDULE_FILTERS, owner: "ana@example.com" })).toBe(true);
  });

  it("groups failed and partial failures under the failing status", () => {
    const failing = { ...EMPTY_SCHEDULE_FILTERS, status: "failing" as const };
    expect(matchesScheduleFilters(row({ run_status: "failed" }), failing)).toBe(true);
    expect(matchesScheduleFilters(row({ run_status: "partial_failure" }), failing)).toBe(true);
    expect(matchesScheduleFilters(row(), failing)).toBe(false);
  });

  it("excludes paused rows from the active status", () => {
    expect(matchesScheduleFilters(row({ paused: true }), { ...EMPTY_SCHEDULE_FILTERS, status: "active" })).toBe(false);
    expect(matchesScheduleFilters(row({ paused: true }), { ...EMPTY_SCHEDULE_FILTERS, status: "paused" })).toBe(true);
  });

  it("searches name, target and owner case-insensitively", () => {
    expect(matchesScheduleFilters(row(), { ...EMPTY_SCHEDULE_FILTERS, search: "SALES" })).toBe(true);
    expect(matchesScheduleFilters(row(), { ...EMPTY_SCHEDULE_FILTERS, search: "ana@" })).toBe(true);
    expect(matchesScheduleFilters(row(), { ...EMPTY_SCHEDULE_FILTERS, search: "inventory" })).toBe(false);
  });
});

describe("hasActiveScheduleFilters", () => {
  it("ignores whitespace-only search", () => {
    expect(hasActiveScheduleFilters({ ...EMPTY_SCHEDULE_FILTERS, search: "  " })).toBe(false);
    expect(hasActiveScheduleFilters({ ...EMPTY_SCHEDULE_FILTERS, status: "paused" })).toBe(true);
  });
});

describe("scheduleOwners", () => {
  it("returns distinct sorted owners and skips missing ones", () => {
    const rows = [row({ owner: "b" }), row({ owner: "a" }), row({ owner: "b" }), row({ owner: null })];
    expect(scheduleOwners(rows)).toEqual(["a", "b"]);
  });
});

describe("getScheduleSortValue", () => {
  it("ranks failing schedules first on an ascending status sort", () => {
    const rows = [row({ paused: true }), row(), row({ run_status: "failed" }), row({ run_status: "partial_failure" })];
    const sorted = [...rows].sort((a, b) =>
      compareSortValues(getScheduleSortValue("health", a), getScheduleSortValue("health", b), "asc", false),
    );
    expect(sorted.map(scheduleHealth)).toEqual(["failed", "partial", "active", "paused"]);
  });

  it("returns null for a missing run time so it can be pinned", () => {
    expect(getScheduleSortValue("lastRun", row({ last_run_at: null }))).toBeNull();
    expect(getScheduleSortValue("nextRun", row({ next_run_at: "2026-01-01T00:00:00Z" }))).toBe(
      Date.parse("2026-01-01T00:00:00Z"),
    );
  });
});

describe("getRelativeFutureParts", () => {
  const now = Date.parse("2026-01-01T12:00:00Z");

  it("returns null without a timestamp", () => {
    expect(getRelativeFutureParts(null, now)).toBeNull();
  });

  it("is due for past or imminent times", () => {
    expect(getRelativeFutureParts("2026-01-01T11:00:00Z", now)).toEqual({ key: "dueNow" });
    expect(getRelativeFutureParts("2026-01-01T12:00:30Z", now)).toEqual({ key: "dueNow" });
  });

  it("buckets into minutes, hours and days", () => {
    expect(getRelativeFutureParts("2026-01-01T12:05:00Z", now)).toEqual({ key: "inMinutes", count: 5 });
    expect(getRelativeFutureParts("2026-01-01T15:10:00Z", now)).toEqual({ key: "inHours", count: 3 });
    expect(getRelativeFutureParts("2026-01-03T12:00:00Z", now)).toEqual({ key: "inDays", count: 2 });
  });
});

describe("tablesScheduledByCollections", () => {
  it("collects member FQNs of scheduled collections only", () => {
    const set = tablesScheduledByCollections([
      { schedule_cron: "0 6 * * *", members: [{ table_fqn: "a.b.c" }] },
      { schedule_cron: null, members: [{ table_fqn: "x.y.z" }] },
    ]);
    expect([...set]).toEqual(["a.b.c"]);
  });
});

describe("resetScheduleFilter", () => {
  it("clears only the given filter", () => {
    const filters = { search: "x", type: "table" as const, status: "paused" as const, owner: "ana" };
    expect(resetScheduleFilter(filters, "status")).toEqual({ ...filters, status: "all" });
    expect(resetScheduleFilter(filters, "search")).toEqual({ ...filters, search: "" });
  });
});
