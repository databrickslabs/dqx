import { describe, expect, test } from "bun:test";
import type { QueryClient } from "@tanstack/react-query";
import {
  DQ_RESULTS_PATH_PREFIX,
  DQ_SCORE_PATH_PREFIX,
  HOME_STATS_PATH_PREFIX,
  RESULTS_INVALIDATION_PATH_PREFIXES,
  SCORE_CACHE_LIST_PATHS,
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

describe("RESULTS_INVALIDATION_PATH_PREFIXES", () => {
  test("covers exactly the dq-score, dq-results and home-stats prefixes", () => {
    expect(RESULTS_INVALIDATION_PATH_PREFIXES).toEqual([
      DQ_SCORE_PATH_PREFIX,
      DQ_RESULTS_PATH_PREFIX,
      HOME_STATS_PATH_PREFIX,
    ]);
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

  test("generated dq-results keys match the invalidation predicate", () => {
    const keys = [
      getGetDqResultsRunsQueryKey("b1"),
      getGetTableResultsQueryKey(FQN, { axes: "breakdown", run_id: "r1" }),
      getGetDqResultsFailedRowsQueryKey(FQN, { limit: 200 }),
      getListResultSeveritiesQueryKey(),
      getListResultDimensionsQueryKey(),
    ];
    for (const key of keys) {
      expect(matchesResultsInvalidation(key)).toBe(true);
    }
  });
});

describe("generated score / home-stats query keys", () => {
  test("the home-stats key path is pinned to its prefix and matches (P3.5)", () => {
    const key = getGetHomeStatsQueryKey();
    expect(String(key[0]).startsWith(HOME_STATS_PATH_PREFIX)).toBe(true);
    expect(matchesResultsInvalidation(key)).toBe(true);
  });

  test("matches the generated P1 rule-score key (the Results tab trigger's gate)", () => {
    expect(matchesResultsInvalidation(getGetRuleScoreQueryKey("r1"))).toBe(true);
  });

  test("matches the generated rule-results keys (the rule Results tab body)", () => {
    expect(matchesResultsInvalidation(getGetRuleResultsQueryKey("r1", { axes: "trend" }))).toBe(true);
    expect(matchesResultsInvalidation(getGetRuleResultsQueryKey("r1", { axes: "breakdown" }))).toBe(true);
  });
});

describe("SCORE_CACHE_LIST_PATHS pins the generated list query keys (P3.4)", () => {
  // The score-cache refresh invalidates exactly the list queries that
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
  test("matches every dq-score key (global, table, product, rule)", () => {
    for (const path of [
      "/api/v1/dq-score/global",
      `/api/v1/dq-score/table/${FQN}`,
      "/api/v1/dq-score/product/p1",
      "/api/v1/dq-score/rule/r1",
    ]) {
      expect(matchesResultsInvalidation([path])).toBe(true);
      expect(matchesResultsInvalidation([path, { limit: 50 }])).toBe(true);
    }
  });

  test("never matches unrelated queries or non-string keys", () => {
    expect(matchesResultsInvalidation(["/api/v1/monitored-tables/b1"])).toBe(false);
    expect(matchesResultsInvalidation(["/api/v1/runs"])).toBe(false);
    expect(matchesResultsInvalidation([{ not: "a string" }])).toBe(false);
    expect(matchesResultsInvalidation([])).toBe(false);
  });
});
