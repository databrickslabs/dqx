import { describe, expect, test } from "bun:test";
import {
  DQ_SCORE_PATH_PREFIX,
  QUARANTINE_SAMPLE_PATH_PREFIX,
  buildResultsInvalidationMatcher,
  matchesResultsInvalidation,
} from "./results-invalidation";

const FQN = "main.sales.orders";

describe("buildResultsInvalidationMatcher", () => {
  test("without FQNs, matches every score AND every quarantine-sample path by prefix", () => {
    const matcher = buildResultsInvalidationMatcher();
    expect(matcher.pathPrefixes).toEqual([DQ_SCORE_PATH_PREFIX, QUARANTINE_SAMPLE_PATH_PREFIX]);
    expect(matcher.exactPaths).toEqual([]);
  });

  test("an empty FQN list behaves like no FQNs (broad quarantine invalidation)", () => {
    expect(buildResultsInvalidationMatcher([])).toEqual(buildResultsInvalidationMatcher());
  });

  test("with FQNs, narrows quarantine invalidation to exactly those tables", () => {
    const matcher = buildResultsInvalidationMatcher([FQN]);
    expect(matcher.pathPrefixes).toEqual([DQ_SCORE_PATH_PREFIX]);
    expect(matcher.exactPaths).toEqual([`/api/v1/quarantine-samples/${FQN}`]);
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
