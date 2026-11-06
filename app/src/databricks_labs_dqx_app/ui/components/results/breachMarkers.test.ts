import { describe, expect, it } from "bun:test";
import { breachedSeriesAt, breachTimesBySeries, perTableBreachMarkers } from "./breachMarkers";

describe("perTableBreachMarkers", () => {
  const points = [
    { run_date: "2026-10-01 00:00:00", series: "c.s.orders", pass_rate: 0.8, breached: true, breach_criticality: "warn" },
    { run_date: "2026-10-02 00:00:00", series: "c.s.orders", pass_rate: 0.9, breached: false, breach_criticality: null },
    { run_date: "2026-10-01 00:00:00", series: "c.s.customers", pass_rate: 0.5, breached: true, breach_criticality: "error" },
    { run_date: "2026-10-03 00:00:00", series: "c.s.customers", pass_rate: null, breached: true, breach_criticality: "error" },
    { run_date: "2026-10-04 00:00:00", series: "c.s.customers", pass_rate: 0.6, breached: true, breach_criticality: "info" },
  ];
  it("anchors one marker per breaching table point at that table's own score", () => {
    expect(perTableBreachMarkers(points)).toEqual([
      { run_date: "2026-10-01 00:00:00", criticality: "warn", score: 80, series: "c.s.orders" },
      { run_date: "2026-10-01 00:00:00", criticality: "error", score: 50, series: "c.s.customers" },
      { run_date: "2026-10-03 00:00:00", criticality: "error", score: null, series: "c.s.customers" },
    ]);
  });
  it("returns [] for undefined input", () => {
    expect(perTableBreachMarkers(undefined)).toEqual([]);
  });
});

describe("breachedSeriesAt / breachTimesBySeries", () => {
  const markers = [
    { run_date: "2026-10-01 00:00:00", criticality: "warn" as const, score: 80, series: "a" },
    { run_date: "2026-10-01 00:00:00", criticality: "error" as const, score: 50, series: "b" },
    { run_date: "2026-10-02 00:00:00", criticality: "warn" as const, score: 70, series: "a" },
    { run_date: "2026-10-01 00:00:00", criticality: "warn" as const, score: 70 },
  ];
  it("returns the series that breached at a run date, string or epoch", () => {
    expect([...breachedSeriesAt(markers, "2026-10-01 00:00:00")].sort()).toEqual(["a", "b"]);
    expect([...breachedSeriesAt(markers, Date.parse("2026-10-02 00:00:00"))]).toEqual(["a"]);
  });
  it("is empty for unknown, null or unparseable dates", () => {
    expect(breachedSeriesAt(markers, "2026-11-01 00:00:00").size).toBe(0);
    expect(breachedSeriesAt(markers, null).size).toBe(0);
    expect(breachedSeriesAt(markers, "nope").size).toBe(0);
    expect(breachedSeriesAt(undefined, "2026-10-01 00:00:00").size).toBe(0);
  });
  it("groups run times by series", () => {
    const m = breachTimesBySeries(markers);
    expect(m.get("a")?.size).toBe(2);
    expect(m.get("b")?.has(Date.parse("2026-10-01 00:00:00"))).toBe(true);
    expect(m.size).toBe(2);
  });
});
