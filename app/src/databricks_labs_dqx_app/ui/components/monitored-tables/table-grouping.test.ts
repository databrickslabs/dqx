import { describe, expect, it } from "bun:test";
import { dqScoreBucketOf } from "@/components/data-table/filter-bar";
import { sortTablesByGroup, tableGroupOf, type GroupableTable } from "./table-grouping";

const translate = (key: string) => `t:${key}`;
const row = (tableFqn: string, score: number | null = null): GroupableTable => ({ tableFqn, score });

describe("dqScoreBucketOf", () => {
  it("uses the DQ score filter's bands", () => {
    expect(dqScoreBucketOf(1)).toBe("75-100");
    expect(dqScoreBucketOf(0.75)).toBe("75-100");
    expect(dqScoreBucketOf(0.749)).toBe("50-75");
    expect(dqScoreBucketOf(0.5)).toBe("50-75");
    expect(dqScoreBucketOf(0.25)).toBe("25-50");
    expect(dqScoreBucketOf(0)).toBe("0-25");
  });

  it("puts a missing score in the No score band", () => {
    expect(dqScoreBucketOf(null)).toBe("none");
    expect(dqScoreBucketOf(undefined)).toBe("none");
  });
});

describe("tableGroupOf", () => {
  it("groups by catalog", () => {
    expect(tableGroupOf(row("main.sales.orders"), "catalog", translate)).toEqual({ key: "main", label: "main" });
  });

  it("keys schema groups by catalog.schema", () => {
    expect(tableGroupOf(row("main.sales.orders"), "schema", translate)).toEqual({
      key: "main.sales",
      label: "main.sales",
    });
    expect(tableGroupOf(row("dev.sales.orders"), "schema", translate).key).toBe("dev.sales");
  });

  it("labels DQ score groups with the score filter's labels", () => {
    expect(tableGroupOf(row("a.b.c", 0.9), "dqScore", translate)).toEqual({
      key: "75-100",
      label: "t:common.dqScoreFilter.b75_100",
    });
    expect(tableGroupOf(row("a.b.c", null), "dqScore", translate)).toEqual({
      key: "none",
      label: "t:common.dqScoreFilter.none",
    });
  });

  it("puts malformed names in an Unknown group", () => {
    expect(tableGroupOf(row(""), "catalog", translate).label).toBe("t:monitoredTables.groupUnknown");
    expect(tableGroupOf(row("main"), "schema", translate).label).toBe("t:monitoredTables.groupUnknown");
  });
});

describe("sortTablesByGroup", () => {
  const toGroupable = (r: GroupableTable) => r;

  it("leaves rows untouched when not grouping", () => {
    const rows = [row("b.x.t"), row("a.x.t")];
    expect(sortTablesByGroup(rows, "none", toGroupable)).toEqual(rows);
  });

  it("orders score groups best first, No score last", () => {
    const rows = [row("t.s.1", null), row("t.s.2", 0.1), row("t.s.3", 0.9), row("t.s.4", 0.6)];
    expect(sortTablesByGroup(rows, "dqScore", toGroupable).map((r) => r.tableFqn)).toEqual([
      "t.s.3",
      "t.s.4",
      "t.s.2",
      "t.s.1",
    ]);
  });

  it("orders name groups alphabetically, keeping row order within a group", () => {
    const rows = [row("b.s.z"), row("a.s.y"), row("b.s.a"), row("a.s.b")];
    expect(sortTablesByGroup(rows, "catalog", toGroupable).map((r) => r.tableFqn)).toEqual([
      "a.s.y",
      "a.s.b",
      "b.s.z",
      "b.s.a",
    ]);
  });

  it("keeps same-named schemas in different catalogs apart, Unknown last", () => {
    const rows = [row("orphan"), row("prod.sales.t1"), row("dev.sales.t2")];
    expect(sortTablesByGroup(rows, "schema", toGroupable).map((r) => r.tableFqn)).toEqual([
      "dev.sales.t2",
      "prod.sales.t1",
      "orphan",
    ]);
  });
});
