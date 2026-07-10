import { describe, expect, test } from "bun:test";
import {
  DQ_RESULTS_PATH_PREFIX,
  DQ_SCORE_PATH_PREFIX,
  QUARANTINE_SAMPLE_PATH_PREFIX,
  buildResultsInvalidationMatcher,
  matchesResultsInvalidation,
} from "./results-invalidation";
import {
  getGetDqResultsFailedRowsQueryKey,
  getGetDqResultsRunsQueryKey,
  getGetTableResultsQueryKey,
  getListResultDimensionsQueryKey,
  getListResultSeveritiesQueryKey,
} from "./api";

const FQN = "main.sales.orders";

describe("buildResultsInvalidationMatcher", () => {
  test("without FQNs, matches every score, dq-results AND quarantine-sample path by prefix", () => {
    const matcher = buildResultsInvalidationMatcher();
    expect(matcher.pathPrefixes).toEqual([
      DQ_SCORE_PATH_PREFIX,
      DQ_RESULTS_PATH_PREFIX,
      QUARANTINE_SAMPLE_PATH_PREFIX,
    ]);
    expect(matcher.exactPaths).toEqual([]);
  });

  test("an empty FQN list behaves like no FQNs (broad quarantine invalidation)", () => {
    expect(buildResultsInvalidationMatcher([])).toEqual(buildResultsInvalidationMatcher());
  });

  test("with FQNs, narrows quarantine invalidation to exactly those tables (dq-results stays wholesale)", () => {
    const matcher = buildResultsInvalidationMatcher([FQN]);
    expect(matcher.pathPrefixes).toEqual([DQ_SCORE_PATH_PREFIX, DQ_RESULTS_PATH_PREFIX]);
    expect(matcher.exactPaths).toEqual([`/api/v1/quarantine-samples/${FQN}`]);
  });
});

describe("DQ_RESULTS_PATH_PREFIX pins the generated dq-results query keys", () => {
  // The prefix must keep matching whatever paths orval actually generates —
  // these assertions fail loudly if the backend mount point or the generated
  // key shape ever drifts away from the invalidation helper.
  test("every generated dq-results key path starts with the prefix", () => {
    const keys = [
      getGetDqResultsRunsQueryKey("b1"),
      getGetDqResultsRunsQueryKey(FQN),
      getGetTableResultsQueryKey(FQN, { axes: "trend" }),
      getGetDqResultsFailedRowsQueryKey(FQN, { limit: 200 }),
      getListResultSeveritiesQueryKey(),
      getListResultDimensionsQueryKey(),
    ];
    for (const key of keys) {
      expect(String(key[0]).startsWith(DQ_RESULTS_PATH_PREFIX)).toBe(true);
    }
  });

  test("generated dq-results keys match in BOTH broad and table-scoped modes", () => {
    const broad = buildResultsInvalidationMatcher();
    const scoped = buildResultsInvalidationMatcher([FQN]);
    const keys = [
      getGetDqResultsRunsQueryKey("b1"),
      getGetTableResultsQueryKey(FQN, { axes: "breakdown", run_id: "r1" }),
      getGetDqResultsFailedRowsQueryKey(FQN, { limit: 200 }),
      getListResultSeveritiesQueryKey(),
      getListResultDimensionsQueryKey(),
    ];
    for (const key of keys) {
      expect(matchesResultsInvalidation(key, broad)).toBe(true);
      expect(matchesResultsInvalidation(key, scoped)).toBe(true);
    }
  });
});

describe("matchesResultsInvalidation", () => {
  const scoped = buildResultsInvalidationMatcher([FQN]);
  const broad = buildResultsInvalidationMatcher();

  test("matches every dq-score key (global, table, product, rule) regardless of scope", () => {
    for (const path of [
      "/api/v1/dq-score/global",
      `/api/v1/dq-score/table/${FQN}`,
      "/api/v1/dq-score/product/p1",
      "/api/v1/dq-score/rule/r1",
    ]) {
      expect(matchesResultsInvalidation([path], scoped)).toBe(true);
      expect(matchesResultsInvalidation([path], broad)).toBe(true);
    }
  });

  test("matches the scoped table's quarantine-sample key, including param'd variants", () => {
    expect(matchesResultsInvalidation([`/api/v1/quarantine-samples/${FQN}`], scoped)).toBe(true);
    expect(matchesResultsInvalidation([`/api/v1/quarantine-samples/${FQN}`, { limit: 50 }], scoped)).toBe(true);
  });

  test("does not match another table's quarantine-sample key when scoped", () => {
    expect(matchesResultsInvalidation(["/api/v1/quarantine-samples/main.sales.other", { limit: 50 }], scoped)).toBe(
      false,
    );
    // A table whose FQN merely extends the scoped one must not match either
    // (exact path equality, not startsWith).
    expect(matchesResultsInvalidation([`/api/v1/quarantine-samples/${FQN}2`], scoped)).toBe(false);
  });

  test("matches any table's quarantine-sample key when broad", () => {
    expect(matchesResultsInvalidation(["/api/v1/quarantine-samples/main.sales.other"], broad)).toBe(true);
  });

  test("never matches unrelated queries or non-string keys", () => {
    for (const matcher of [scoped, broad]) {
      expect(matchesResultsInvalidation(["/api/v1/monitored-tables/b1"], matcher)).toBe(false);
      expect(matchesResultsInvalidation(["/api/v1/runs"], matcher)).toBe(false);
      expect(matchesResultsInvalidation([{ not: "a string" }], matcher)).toBe(false);
      expect(matchesResultsInvalidation([], matcher)).toBe(false);
    }
  });
});
