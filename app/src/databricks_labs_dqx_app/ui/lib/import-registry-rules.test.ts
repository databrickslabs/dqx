import { describe, expect, it } from "vitest";
import { normalizeImportedCheck, parseImportYamlText, SQL_CHECK_PREFIX } from "./import-registry-rules";

const MESSAGES = {
  yamlMustBeList: "must be list",
  commentsOnly: "comments only",
  emptyList: "empty list",
  invalidEntry: "invalid entry {{index}}",
};

describe("normalizeImportedCheck", () => {
  it("normalizes a standard nested check", () => {
    const out = normalizeImportedCheck({
      criticality: "error",
      check: {
        function: "is_not_null",
        arguments: { column: "id" },
      },
    });
    expect(out).toEqual({
      criticality: "error",
      check: { function: "is_not_null", arguments: { column: "id" } },
      user_metadata: {},
    });
  });

  it("moves weight into user_metadata", () => {
    const out = normalizeImportedCheck({
      weight: 3,
      check: { function: "is_not_null", arguments: { column: "id" } },
    });
    expect(out.user_metadata).toEqual({ weight: "3" });
    expect(out).not.toHaveProperty("weight");
  });

  it("merges top-level name into user_metadata", () => {
    const out = normalizeImportedCheck({
      name: "not_null_id",
      check: { function: "is_not_null", arguments: { column: "id" } },
    });
    expect(out.user_metadata).toEqual({ name: "not_null_id" });
  });

  it("converts __sql_check__ prefix to sql_query", () => {
    const out = normalizeImportedCheck({
      check: {
        function: `${SQL_CHECK_PREFIX}orders_total`,
        arguments: { query: "SELECT 1" },
      },
    });
    expect(out.check).toEqual({
      function: "sql_query",
      arguments: { query: "SELECT 1" },
    });
    expect(out.user_metadata).toEqual({ name: "orders_total" });
  });
});

describe("parseImportYamlText", () => {
  it("parses a list of rules", () => {
    const result = parseImportYamlText(
      `- name: order_id_not_null
  criticality: error
  check:
    function: is_not_null
    arguments:
      column: order_id`,
      MESSAGES,
    );
    expect(result.error).toBeNull();
    expect(result.checks).toHaveLength(1);
  });

  it("wraps a single rule object without a leading dash", () => {
    const result = parseImportYamlText(
      `name: order_id_not_null
criticality: error
check:
  function: is_not_null
  arguments:
    column: order_id`,
      MESSAGES,
    );
    expect(result.error).toBeNull();
    expect(result.checks).toHaveLength(1);
  });

  it("reports comments-only YAML", () => {
    const result = parseImportYamlText("# just a comment\n# another line", MESSAGES);
    expect(result.checks).toBeNull();
    expect(result.hint).toBe(MESSAGES.commentsOnly);
  });

  it("surfaces YAML syntax errors", () => {
    const result = parseImportYamlText(
      `name: broken
  bad_indent: true`,
      MESSAGES,
    );
    expect(result.checks).toBeNull();
    expect(result.error).toBeTruthy();
  });
});
