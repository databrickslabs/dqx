import { describe, expect, it } from "bun:test";
import { perTableBreachMarkers } from "./breachMarkers";

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
