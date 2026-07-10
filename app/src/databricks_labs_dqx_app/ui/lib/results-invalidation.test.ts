import { describe, expect, test } from "bun:test";
import type { QueryClient } from "@tanstack/react-query";
import {
  DQ_RESULTS_PATH_PREFIX,
  DQ_SCORE_PATH_PREFIX,
  HOME_STATS_PATH_PREFIX,
  QUARANTINE_SAMPLE_PATH_PREFIX,
  SCORE_CACHE_LIST_PATHS,
  buildResultsInvalidationMatcher,
  buildRuleApplicationChangeMatcher,
  matchesResultsInvalidation,
  triggerScoreCacheRefresh,
} from "./results-invalidation";
import {
  getGetDqResultsFailedRowsQueryKey,
  getGetDqResultsRunsQueryKey,
  getGetHomeStatsQueryKey,
  getGetRuleResultsQueryKey,
  getGetRuleScoreQueryKey,
  getGetTableResultsQueryKey,
  getListDataProductsQueryKey,
  getListMonitoredTablesQueryKey,
  getListResultDimensionsQueryKey,
  getListResultSeveritiesQueryKey,
} from "./api";

const FQN = "main.sales.orders";

describe("buildResultsInvalidationMatcher", () => {
  test("without FQNs, matches every score, dq-results, home-stats AND quarantine-sample path by prefix", () => {
    const matcher = buildResultsInvalidationMatcher();
    expect(matcher.pathPrefixes).toEqual([
      DQ_SCORE_PATH_PREFIX,
      DQ_RESULTS_PATH_PREFIX,
      HOME_STATS_PATH_PREFIX,
      QUARANTINE_SAMPLE_PATH_PREFIX,
    ]);
    expect(matcher.exactPaths).toEqual([]);
  });

  test("an empty FQN list behaves like no FQNs (broad quarantine invalidation)", () => {
    expect(buildResultsInvalidationMatcher([])).toEqual(buildResultsInvalidationMatcher());
  });

  test("with FQNs, narrows quarantine invalidation to exactly those tables (dq-results stays wholesale)", () => {
    const matcher = buildResultsInvalidationMatcher([FQN]);
    expect(matcher.pathPrefixes).toEqual([DQ_SCORE_PATH_PREFIX, DQ_RESULTS_PATH_PREFIX, HOME_STATS_PATH_PREFIX]);
    expect(matcher.exactPaths).toEqual([`/api/v1/quarantine-samples/${FQN}`]);
  });

  test("the generated home-stats key matches in BOTH broad and table-scoped modes (P3.5)", () => {
    const key = getGetHomeStatsQueryKey();
    expect(String(key[0]).startsWith(HOME_STATS_PATH_PREFIX)).toBe(true);
    expect(matchesResultsInvalidation(key, buildResultsInvalidationMatcher())).toBe(true);
    expect(matchesResultsInvalidation(key, buildResultsInvalidationMatcher([FQN]))).toBe(true);
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

describe("buildRuleApplicationChangeMatcher (rule apply/unapply invalidation)", () => {
  const matcher = buildRuleApplicationChangeMatcher();

  test("covers the dq-score, dq-results and home-stats prefixes but NOT quarantine samples", () => {
    expect(matcher.pathPrefixes).toEqual([DQ_SCORE_PATH_PREFIX, DQ_RESULTS_PATH_PREFIX, HOME_STATS_PATH_PREFIX]);
    expect(matcher.exactPaths).toEqual([]);
  });

  test("matches the generated P1 rule-score key (the Results tab trigger's gate)", () => {
    expect(matchesResultsInvalidation(getGetRuleScoreQueryKey("r1"), matcher)).toBe(true);
  });

  test("matches the generated rule-results keys (the rule Results tab body)", () => {
    expect(matchesResultsInvalidation(getGetRuleResultsQueryKey("r1", { axes: "trend" }), matcher)).toBe(true);
    expect(matchesResultsInvalidation(getGetRuleResultsQueryKey("r1", { axes: "breakdown" }), matcher)).toBe(true);
  });

  test("does NOT match quarantine samples (failing rows only change when a run finishes)", () => {
    expect(matchesResultsInvalidation([`/api/v1/quarantine-samples/${FQN}`], matcher)).toBe(false);
  });

  test("never matches unrelated queries", () => {
    expect(matchesResultsInvalidation(["/api/v1/monitored-tables/b1"], matcher)).toBe(false);
  });
});

describe("SCORE_CACHE_LIST_PATHS pins the generated list query keys (P3.4)", () => {
  // The score-cache refresh invalidates exactly the two list queries that
  // LEFT JOIN dq_score_cache — these assertions fail loudly if the backend
  // mount points or the generated key shapes ever drift away.
  test("every score-cache-backed list key path is covered, exactly", () => {
    expect(getListMonitoredTablesQueryKey()[0]).toBe(SCORE_CACHE_LIST_PATHS[0]);
    expect(getListDataProductsQueryKey()[0]).toBe(SCORE_CACHE_LIST_PATHS[1]);
    expect(getGetHomeStatsQueryKey()[0]).toBe(SCORE_CACHE_LIST_PATHS[2]);
  });

  test("detail-page paths are NOT list paths (exact element match, not prefix)", () => {
    for (const path of SCORE_CACHE_LIST_PATHS) {
      expect(path.endsWith("/")).toBe(false);
    }
    expect(SCORE_CACHE_LIST_PATHS).not.toContain("/api/v1/monitored-tables/b1");
  });
});

describe("triggerScoreCacheRefresh", () => {
  // A throwing stub: the broad (no-FQN) path must return without touching
  // the query client OR the network — refresh-scores is a scoped
  // run-completion trigger, never a refresh-all.
  const throwingQueryClient = new Proxy({} as QueryClient, {
    get() {
      throw new Error("queryClient must not be touched on the broad path");
    },
  });

  test("no FQNs → no-op (broad invalidations never refresh-all)", () => {
    triggerScoreCacheRefresh(throwingQueryClient);
    triggerScoreCacheRefresh(throwingQueryClient, []);
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
