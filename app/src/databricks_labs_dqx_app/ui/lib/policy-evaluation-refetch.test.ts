import { describe, expect, test } from "bun:test";
import {
  POLICY_REFETCH_DELAY_MS,
  POLICY_REFETCH_MAX_ATTEMPTS,
  appendMissingRows,
  newServerRows,
  nextPolicyRefetchDelay,
} from "./policy-evaluation-refetch";

type Row = { key: string; label?: string };
const key = (r: Row) => r.key;

describe("nextPolicyRefetchDelay", () => {
  test("refetches after a short delay while an evaluation is pending", () => {
    expect(nextPolicyRefetchDelay(true, 0)).toBe(POLICY_REFETCH_DELAY_MS);
    expect(POLICY_REFETCH_DELAY_MS).toBeGreaterThanOrEqual(2000);
    expect(POLICY_REFETCH_DELAY_MS).toBeLessThanOrEqual(3000);
  });

  test("stops once nothing is pending", () => {
    expect(nextPolicyRefetchDelay(false, 0)).toBeNull();
    expect(nextPolicyRefetchDelay(undefined, 0)).toBeNull();
  });

  test("stops after the maximum number of attempts even if still pending", () => {
    expect(POLICY_REFETCH_MAX_ATTEMPTS).toBe(3);
    expect(nextPolicyRefetchDelay(true, POLICY_REFETCH_MAX_ATTEMPTS - 1)).toBe(POLICY_REFETCH_DELAY_MS);
    expect(nextPolicyRefetchDelay(true, POLICY_REFETCH_MAX_ATTEMPTS)).toBeNull();
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
