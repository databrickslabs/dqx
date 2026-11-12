import { describe, expect, test } from "bun:test";
import {
  POLICY_REFETCH_BUDGET_MS,
  POLICY_REFETCH_FIRST_DELAY_MS,
  appendMissingRows,
  knownAppliedIds,
  newServerRows,
  nextPolicyRefetchDelay,
} from "./policy-evaluation-refetch";

type Row = { key: string; label?: string };
const key = (r: Row) => r.key;

describe("nextPolicyRefetchDelay", () => {
  test("first re-read comes shortly after a pending response", () => {
    expect(nextPolicyRefetchDelay(true, 0)).toBe(POLICY_REFETCH_FIRST_DELAY_MS);
    expect(POLICY_REFETCH_FIRST_DELAY_MS).toBeGreaterThanOrEqual(2000);
    expect(POLICY_REFETCH_FIRST_DELAY_MS).toBeLessThanOrEqual(3000);
  });

  test("stops once nothing is pending", () => {
    expect(nextPolicyRefetchDelay(false, 0)).toBeNull();
    expect(nextPolicyRefetchDelay(undefined, 0)).toBeNull();
  });

  test("backs off while pending and keeps re-reading for about a minute in total", () => {
    const delays: number[] = [];
    for (let attempt = 0; attempt < 100; attempt++) {
      const delay = nextPolicyRefetchDelay(true, attempt);
      if (delay === null) break;
      delays.push(delay);
    }
    for (let i = 1; i < delays.length; i++) expect(delays[i]).toBeGreaterThanOrEqual(delays[i - 1]);
    const total = delays.reduce((sum, d) => sum + d, 0);
    expect(total).toBeLessThanOrEqual(POLICY_REFETCH_BUDGET_MS);
    expect(total).toBeGreaterThan(POLICY_REFETCH_BUDGET_MS - 15_000);
    expect(POLICY_REFETCH_BUDGET_MS).toBe(60_000);
    expect(delays.length).toBeGreaterThan(3);
  });
});

describe("knownAppliedIds", () => {
  test("lists the persisted ids of the baseline rows", () => {
    expect(knownAppliedIds([{ id: "a" }, { id: null }, { id: undefined }, { id: "a" }, { id: "b" }])).toEqual(["a", "b"]);
  });
});

describe("newServerRows", () => {
  test("returns rows that appeared since the previous server read", () => {
    expect(newServerRows([{ key: "a" }], [{ key: "a" }, { key: "b" }], key)).toEqual([{ key: "b" }]);
  });

  test("ignores rows that disappeared or stayed", () => {
    expect(newServerRows([{ key: "a" }, { key: "b" }], [{ key: "a" }], key)).toEqual([]);
  });
});

describe("appendMissingRows", () => {
  test("appends only rows whose key is not present, keeping the same array when nothing changes", () => {
    const rows = [{ key: "a" }];
    expect(appendMissingRows(rows, [{ key: "a", label: "server" }], key)).toBe(rows);
    expect(appendMissingRows(rows, [{ key: "b" }, { key: "b" }], key)).toEqual([{ key: "a" }, { key: "b" }]);
  });
});
